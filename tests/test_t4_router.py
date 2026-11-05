import json
import time

import httpx
import pytest

from task4_router import router as router_mod
from task4_router.app import create_app
from task4_router.limiter import TokenLimiter
from task4_router.mock_providers import create_provider
from task4_router.router import ModelRouter, Provider
from tests.servers import LiveServer

KEY = {"Authorization": "Bearer tenant-key-1"}
BODY = {"messages": [{"role": "user", "content": "hello there"}], "max_tokens": 100}
LEAKY = ("Traceback", "shard-db-7", "/srv/llm", "org-123", "127.0.0.1", "primary.internal", "sk-")


def asgi_client(app):
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://p")


@pytest.fixture
def mocks():
    return create_provider("primary"), create_provider("secondary")


@pytest.fixture
async def make(tmp_path, mocks):
    clients = []

    def _make(timeout=0.3, limit=50_000, secondary_timeout=0.3, primary_url=None):
        p_app, s_app = mocks
        pc = asgi_client(p_app) if primary_url is None else httpx.AsyncClient()
        sc = asgi_client(s_app)
        clients.extend([pc, sc])
        r = ModelRouter(
            Provider("primary", primary_url or "http://primary.internal", "sk-primary", "big-model", pc),
            Provider("secondary", "http://secondary.internal", "sk-secondary", "small-model", sc),
            timeout_s=timeout, secondary_timeout_s=secondary_timeout,
        )
        lim = TokenLimiter(tmp_path / "rl.db", limit=limit)
        app = create_app(r, lim)
        c = asgi_client(app)
        clients.append(c)
        return c, lim

    yield _make
    for c in clients:
        await c.aclose()


async def chat(c, body=BODY, headers=KEY):
    return await c.post("/v1/chat/completions", json=body, headers=headers)


def assert_clean_error(r, type_):
    j = r.json()
    assert set(j) == {"error"}
    assert set(j["error"]) == {"type", "code", "message", "request_id"}
    assert j["error"]["type"] == type_
    assert r.headers["x-request-id"] == j["error"]["request_id"]
    for s in LEAKY:
        assert s not in r.text, s


def test_default_timeout_is_3s():
    assert router_mod.DEFAULT_TIMEOUT_S == 3.0


async def test_primary_ok(make, mocks):
    c, _ = make()
    r = await chat(c)
    assert r.status_code == 200
    assert r.headers["x-gateway-provider"] == "primary"
    assert r.json()["choices"][0]["message"]["content"] == "hi from primary"
    assert mocks[1].state.calls == 0
    assert mocks[0].state.last_body["model"] == "big-model"
    assert mocks[0].state.last_auth == "Bearer sk-primary"


async def test_primary_429_fails_over(make, mocks):
    mocks[0].state.mode = "429"
    c, _ = make()
    r = await chat(c)
    assert r.status_code == 200
    assert r.headers["x-gateway-provider"] == "secondary"
    assert mocks[1].state.last_body["model"] == "small-model"


async def test_primary_timeout_fails_over_and_cancels(make, mocks):
    mocks[0].state.mode = "slow"
    c, _ = make(timeout=0.2)
    t0 = time.perf_counter()
    r = await chat(c)
    elapsed = time.perf_counter() - t0
    assert r.headers["x-gateway-provider"] == "secondary"
    assert elapsed < 1.0
    assert mocks[0].state.cancelled == 1


async def test_slow_body_counts_against_the_deadline(make, mocks):
    # real socket: headers arrive immediately, body takes ~2s. A per-read timeout would never fire.
    p_app, _ = mocks
    p_app.state.mode = "slow_body"
    p_app.state.delay = 2.0
    with LiveServer(p_app) as srv:
        c, _ = make(timeout=0.4, primary_url=srv.url)
        t0 = time.perf_counter()
        r = await chat(c)
        assert time.perf_counter() - t0 < 1.5
    assert r.headers["x-gateway-provider"] == "secondary"


async def test_primary_500_fails_over(make, mocks):
    mocks[0].state.mode = "500"
    c, _ = make()
    r = await chat(c)
    assert r.headers["x-gateway-provider"] == "secondary"


async def test_primary_non_json_fails_over(make, mocks):
    mocks[0].state.mode = "garbage"
    c, _ = make()
    r = await chat(c)
    assert r.headers["x-gateway-provider"] == "secondary"


async def test_both_fail_is_sanitized_502(make, mocks):
    mocks[0].state.mode = "500"
    mocks[1].state.mode = "429"
    c, lim = make()
    r = await chat(c)
    assert r.status_code == 502
    assert_clean_error(r, "upstream_error")
    assert r.json()["error"]["code"] == "upstream_unavailable"
    assert await lim.usage("tenant-key-1") == 0  # failed request gave its tokens back


async def test_both_timeout_is_504(make, mocks):
    mocks[0].state.mode = mocks[1].state.mode = "slow"
    c, _ = make(timeout=0.1, secondary_timeout=0.1)
    r = await chat(c)
    assert r.status_code == 504
    assert_clean_error(r, "upstream_error")
    assert r.json()["error"]["code"] == "upstream_timeout"


async def test_usage_reconciled_with_provider(make):
    c, lim = make()
    await chat(c)
    assert await lim.usage("tenant-key-1") == 20  # mock reports 12 + 8


async def test_rate_limited(make):
    c, lim = make(limit=1_000)
    big = {"messages": [{"role": "user", "content": "x"}], "max_tokens": 600}
    assert (await chat(c, big)).status_code == 200  # reserves ~600, settles at 20
    r = await lim.reserve("tenant-key-1", 980)        # fill the rest by hand
    r = await chat(c, big)
    assert r.status_code == 429
    assert_clean_error(r, "rate_limit_error")
    assert r.json()["error"]["code"] == "tokens_per_minute_exceeded"
    assert 0 < int(r.headers["retry-after"]) <= 60


async def test_request_too_big_for_any_window(make):
    c, _ = make(limit=1_000)
    r = await chat(c, {"messages": [{"role": "user", "content": "x"}], "max_tokens": 5_000})
    assert r.status_code == 429
    assert "retry-after" not in r.headers
    assert r.json()["error"]["code"] == "request_exceeds_limit"


@pytest.mark.parametrize("headers", [{}, {"Authorization": "Basic abc"}, {"Authorization": "Bearer "}])
async def test_auth_required(make, headers):
    c, _ = make()
    r = await chat(c, headers=headers)
    assert r.status_code == 401
    assert_clean_error(r, "authentication_error")


@pytest.mark.parametrize("body", [
    b"{oops", b"[]", json.dumps({"messages": []}).encode(),
    json.dumps({"messages": [{"role": "user", "content": "x"}], "max_tokens": -1}).encode(),
    json.dumps({"messages": [{"role": "user", "content": "x"}], "stream": True}).encode(),
])
async def test_bad_bodies(make, body):
    c, _ = make()
    r = await c.post("/v1/chat/completions", content=body, headers={**KEY, "content-type": "application/json"})
    assert r.status_code == 400
    assert_clean_error(r, "invalid_request_error")


async def test_unknown_route(make):
    c, _ = make()
    r = await c.get("/nope", headers=KEY)
    assert r.status_code == 404
    assert_clean_error(r, "invalid_request_error")


async def test_unexpected_exception_is_sanitized(make, monkeypatch):
    c, lim = make()

    async def boom(self, payload):
        raise RuntimeError("psycopg2 connection to 10.0.0.5 refused sk-live-123")

    monkeypatch.setattr(ModelRouter, "complete", boom)
    r = await chat(c)
    assert r.status_code == 500
    assert_clean_error(r, "internal_error")
    assert "psycopg2" not in r.text and "10.0.0.5" not in r.text
    assert await lim.usage("tenant-key-1") == 0


async def test_provider_without_api_key(tmp_path, mocks):
    p_app, s_app = mocks
    async with asgi_client(p_app) as pc, asgi_client(s_app) as sc:
        r = ModelRouter(Provider("primary", "http://p", "", "m", pc), Provider("secondary", "http://s", "", "m", sc))
        name, _ = await r.complete({"messages": [{"role": "user", "content": "x"}]})
    assert name == "primary"
    assert p_app.state.last_auth is None


async def test_auth_checked_before_body(make):
    c, _ = make()
    r = await c.post("/v1/chat/completions", json={})
    assert r.status_code == 401
    assert_clean_error(r, "authentication_error")


async def test_client_body_forwarded_as_sent(make, mocks):
    c, _ = make()
    body = {"messages": [
        {"role": "user", "content": [{"type": "text", "text": "what's the weather"}]},
        {"role": "assistant", "content": None,
         "tool_calls": [{"id": "call_1", "type": "function", "function": {"name": "wx", "arguments": "{}"}}]},
        {"role": "tool", "tool_call_id": "call_1", "content": "sunny"},
    ], "temperature": 0.2}
    r = await chat(c, body)
    assert r.status_code == 200
    sent = mocks[0].state.last_body
    assert sent == {**body, "model": "big-model"}  # no injected max_tokens/stream, nothing dropped
