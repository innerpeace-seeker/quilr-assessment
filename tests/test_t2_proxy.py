import json

import httpx
import pytest

from task2_mcp_gateway.app import create_app
from task2_mcp_gateway.auth import make_token
from task2_mcp_gateway.mock_downstream import create_mock

SECRET = "test-secret-that-is-long-enough-for-hs256"
ADMIN = {"Authorization": "Bearer " + make_token("alice", "admin", SECRET)}
VIEWER = {"Authorization": "Bearer " + make_token("bob", "viewer", SECRET)}


@pytest.fixture
async def env():
    mock = create_mock()
    downstream = httpx.AsyncClient(transport=httpx.ASGITransport(app=mock), base_url="http://downstream")
    gw = create_app("http://downstream/mcp", SECRET, client=downstream)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=gw), base_url="http://gw") as c:
        yield c, mock.state.calls
    await downstream.aclose()


def rpc(method, id_=1, **params):
    m = {"jsonrpc": "2.0", "method": method}
    if id_ is not None:
        m["id"] = id_
    if params:
        m["params"] = params
    return m


async def post(c, body, headers):
    raw = body if isinstance(body, (bytes, str)) else json.dumps(body)
    return await c.post("/mcp", content=raw, headers={**headers, "Content-Type": "application/json",
                                                       "Accept": "application/json, text/event-stream"})


async def test_tools_list_is_forwarded_untouched(env):
    c, calls = env
    body = json.dumps(rpc("tools/list", id_=11), separators=(",", ":")).encode()
    r = await post(c, body, VIEWER)
    assert r.status_code == 200
    names = [t["name"] for t in r.json()["result"]["tools"]]
    assert "admin_reset_key" in names and "get_status" in names
    assert len(calls) == 1
    assert calls[0]["body"] == body  # exact bytes, not re-serialised


async def test_viewer_cannot_call_admin_tool(env):
    c, calls = env
    r = await post(c, rpc("tools/call", id_="x-9", name="admin_reset_key", arguments={"key_id": "k1"}), VIEWER)
    assert r.status_code == 200
    j = r.json()
    assert j["id"] == "x-9"
    assert j["error"]["code"] == -32001
    assert j["error"]["message"] == "Unauthorized Tool Call"
    assert calls == []


async def test_admin_can_call_admin_tool(env):
    c, calls = env
    r = await post(c, rpc("tools/call", id_=2, name="admin_reset_key", arguments={"key_id": "k1"}), ADMIN)
    assert r.status_code == 200
    assert "reset" in r.json()["result"]["content"][0]["text"]
    assert len(calls) == 1


async def test_viewer_can_call_regular_tool(env):
    c, calls = env
    r = await post(c, rpc("tools/call", id_=3, name="get_status", arguments={}), VIEWER)
    assert r.json()["result"]["content"][0]["text"] == "all systems nominal"


async def test_auth_header_not_leaked_downstream_but_user_is(env):
    c, calls = env
    await post(c, rpc("ping"), VIEWER)
    h = calls[0]["headers"]
    assert "authorization" not in h
    assert h["x-forwarded-user"] == "bob"
    assert h["x-forwarded-role"] == "viewer"


@pytest.mark.parametrize("headers", [{}, {"Authorization": "Bearer junk"}, {"Authorization": "Basic YTpi"}])
async def test_missing_or_bad_token(env, headers):
    c, calls = env
    r = await post(c, rpc("tools/list"), headers)
    assert r.status_code == 401
    assert r.headers["www-authenticate"].lower().startswith("bearer")
    assert r.json()["error"]["code"] == -32000
    assert calls == []


async def test_malformed_json(env):
    c, calls = env
    r = await post(c, b'{"jsonrpc": "2.0", "id": 1, "method": ', VIEWER)
    assert r.status_code == 400
    assert r.json() == {"jsonrpc": "2.0", "id": None, "error": {"code": -32700, "message": "Parse error"}}
    assert calls == []


async def test_invalid_request(env):
    c, calls = env
    r = await post(c, {"id": 1, "method": "tools/list"}, VIEWER)
    assert r.status_code == 400
    assert r.json()["error"]["code"] == -32600
    assert calls == []


async def test_batch_partially_denied(env):
    c, calls = env
    batch = [rpc("tools/list", id_=1), rpc("tools/call", id_=2, name="admin_reset_key", arguments={}),
             rpc("tools/call", id_=3, name="get_status", arguments={})]
    r = await post(c, batch, VIEWER)
    assert r.status_code == 200
    by_id = {m["id"]: m for m in r.json()}
    assert set(by_id) == {1, 2, 3}
    assert by_id[2]["error"]["code"] == -32001
    assert "result" in by_id[1] and "result" in by_id[3]
    forwarded = json.loads(calls[0]["body"])
    assert [m["id"] for m in forwarded] == [1, 3]


async def test_batch_fully_denied_never_reaches_downstream(env):
    c, calls = env
    r = await post(c, [rpc("tools/call", id_=1, name="admin_a", arguments={}),
                       rpc("tools/call", id_=2, name="admin_b", arguments={})], VIEWER)
    assert [m["error"]["code"] for m in r.json()] == [-32001, -32001]
    assert calls == []


async def test_notification_passthrough(env):
    c, calls = env
    r = await post(c, rpc("notifications/initialized", id_=None), VIEWER)
    assert r.status_code == 202
    assert len(calls) == 1


async def test_denied_notification(env):
    c, calls = env
    r = await post(c, rpc("tools/call", id_=None, name="admin_reset_key"), VIEWER)
    assert r.status_code == 403
    assert calls == []


async def test_session_header_round_trip(env):
    c, calls = env
    r = await post(c, rpc("initialize", id_=1, protocolVersion="2025-06-18", capabilities={},
                          clientInfo={"name": "t", "version": "0"}), VIEWER)
    sid = r.headers["mcp-session-id"]
    await post(c, rpc("ping", id_=2), {**VIEWER, "Mcp-Session-Id": sid})
    assert calls[1]["headers"]["mcp-session-id"] == sid


async def test_sse_response_streams_through(env):
    c, calls = env
    r = await post(c, rpc("tools/call", id_=5, name="slow_report", arguments={}), VIEWER)
    assert r.headers["content-type"].startswith("text/event-stream")
    events = [line for line in r.text.splitlines() if line.startswith("data:")]
    assert len(events) == 2 and json.loads(events[-1][5:])["id"] == 5


async def test_downstream_unreachable():
    gw = create_app("http://127.0.0.1:9/mcp", SECRET)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=gw), base_url="http://gw") as c:
        r = await post(c, rpc("tools/list", id_=4), VIEWER)
    assert r.status_code == 502
    j = r.json()
    assert j["id"] == 4 and j["error"]["code"] == -32603
    assert "127.0.0.1" not in r.text


async def test_body_too_large(env):
    c, calls = env
    big = json.dumps(rpc("tools/call", id_=1, name="get_status", arguments={"x": "a" * 2_000_000}))
    r = await post(c, big, VIEWER)
    assert r.status_code == 413
    assert calls == []


async def test_clean_batch_forwards_original_bytes(env):
    c, calls = env
    raw = b'[ {"jsonrpc":"2.0","id":1,"method":"ping"} ,  {"jsonrpc":"2.0","id":2,"method":"tools/list"} ]'
    r = await post(c, raw, VIEWER)
    assert len(r.json()) == 2
    assert calls[0]["body"] == raw


async def test_duplicate_tool_name_never_reaches_downstream(env):
    c, calls = env
    raw = b'{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"admin_reset_key","name":"get_status"}}'
    r = await post(c, raw, VIEWER)
    assert r.status_code == 400
    assert r.json()["error"]["code"] == -32600
    assert calls == []
