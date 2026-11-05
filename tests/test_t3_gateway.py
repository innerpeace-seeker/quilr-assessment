import json

import httpx
import pytest

from task3_pii_stream.app import create_app
from task3_pii_stream.mock_provider import EMAIL, SSN, CARD, create_mock
from task3_pii_stream.sse import iter_events
from tests.servers import LiveServer

R = "[REDACTED]"


@pytest.fixture(scope="module")
def stack():
    mock = create_mock()
    with LiveServer(mock) as prov:
        with LiveServer(create_app(prov.url, "sk-test-upstream")) as gw:
            yield gw.url, mock


async def lines_of(text):
    for line in text.splitlines():
        yield line


async def test_sse_parser_handles_multiline_and_comments():
    raw = ": keepalive\n\ndata: {\"a\":\ndata: 1}\n\nevent: x\ndata:[DONE]\n\n"
    got = [e async for e in iter_events(lines_of(raw))]
    assert got == ['{"a":\n1}', "[DONE]"]


async def stream_chat(url, model="mock-pii", n=1):
    events = []
    async with httpx.AsyncClient(timeout=10) as c:
        async with c.stream("POST", url + "/v1/chat/completions",
                            json={"model": model, "stream": True, "n": n,
                                  "messages": [{"role": "user", "content": "hi"}]}) as r:
            assert r.status_code == 200
            assert r.headers["content-type"].startswith("text/event-stream")
            async for data in iter_events(r.aiter_lines()):
                events.append(data)
    return events


def contents(events, index=0):
    out = ""
    for e in events:
        if e == "[DONE]":
            continue
        for ch in json.loads(e)["choices"]:
            if ch["index"] == index:
                out += ch["delta"].get("content") or ""
    return out


async def test_streamed_pii_is_redacted(stack):
    url, _ = stack
    events = await stream_chat(url)
    text = contents(events)
    assert text.count(R) == 3
    for raw in (EMAIL, SSN, CARD):
        assert raw not in text
        assert all(raw not in e for e in events)
    assert "Your account email is" in text and "Anything else?" in text


async def test_events_are_valid_openai_chunks(stack):
    url, _ = stack
    events = await stream_chat(url)
    assert events[-1] == "[DONE]"
    chunks = [json.loads(e) for e in events[:-1]]
    assert all(c["object"] == "chat.completion.chunk" for c in chunks)
    assert chunks[0]["choices"][0]["delta"].get("role") == "assistant"
    assert chunks[-1]["choices"][0]["finish_reason"] == "stop"


async def test_first_token_is_not_buffered(stack):
    # the provider sends the role chunk + some text, then blocks until we tell it to go on.
    # If the gateway accumulated the response we'd never see anything before releasing it.
    url, mock = stack
    mock.state.gate.clear()
    got = []
    async with httpx.AsyncClient(timeout=10) as c:
        async with c.stream("POST", url + "/v1/chat/completions",
                            json={"model": "mock-gated", "stream": True, "messages": []}) as r:
            it = iter_events(r.aiter_lines())
            while True:
                data = await anext(it)
                got.append(data)
                if "Hello" in contents(got):
                    break
            assert not mock.state.gate.is_set()
            mock.state.gate.set()
            async for data in it:
                got.append(data)
    assert contents(got) == "Hello there, all done."


async def test_choices_redacted_independently(stack):
    url, _ = stack
    events = await stream_chat(url, n=2)
    a, b = contents(events, 0), contents(events, 1)
    assert a.count(R) == 3 and b.count(R) == 3
    assert EMAIL not in a + b


async def test_non_streaming_redacted(stack):
    url, _ = stack
    async with httpx.AsyncClient(timeout=10) as c:
        r = await c.post(url + "/v1/chat/completions", json={"model": "mock-pii", "messages": []})
    msg = r.json()["choices"][0]["message"]["content"]
    assert msg.count(R) == 3 and EMAIL not in msg


async def test_upstream_failure_is_generic(stack):
    url, _ = stack
    async with httpx.AsyncClient(timeout=10) as c:
        r = await c.post(url + "/v1/chat/completions", json={"model": "mock-error", "stream": True, "messages": []})
    assert r.status_code == 502
    assert r.json()["error"]["type"] == "upstream_error"
    assert "Traceback" not in r.text and "db-7" not in r.text


async def test_bad_request_body(stack):
    url, _ = stack
    async with httpx.AsyncClient(timeout=10) as c:
        r = await c.post(url + "/v1/chat/completions", content=b"{not json", headers={"content-type": "application/json"})
    assert r.status_code == 400
    assert r.json()["error"]["type"] == "invalid_request"


async def test_upstream_gets_gateway_key(stack):
    url, mock = stack
    await stream_chat(url)
    assert mock.state.last_auth == "Bearer sk-test-upstream"


def test_tail_lands_on_finish_chunk_with_empty_delta():
    from task3_pii_stream.app import ChoiceRedactors, rewrite_chunk
    rs = ChoiceRedactors()
    rewrite_chunk({"choices": [{"index": 0, "delta": {"content": "all done."}, "finish_reason": None}]}, rs)
    out = rewrite_chunk({"choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]}, rs)
    assert out["choices"][0]["delta"] == {"content": "done."}
    out = rewrite_chunk({"choices": [{"index": 1, "delta": None, "finish_reason": "stop"}]}, rs)
    assert out["choices"][0]["finish_reason"] == "stop"
