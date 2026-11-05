"""OpenAI-compatible fake that streams PII split across chunk boundaries.

Run: uv run uvicorn task3_pii_stream.mock_provider:app --port 9101
"""
import asyncio
import json
import threading
import time
import uuid

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, StreamingResponse
from starlette.routing import Route

EMAIL = "jane.doe@example.com"
SSN = "078-05-1120"
CARD = "4242 4242 4242 4242"
SCRIPT = f"Your account email is {EMAIL}, SSN {SSN} and card {CARD}. Anything else?"


def _pieces(text):
    # uneven sizes so PII lands across boundaries the way real tokenizers cut it
    sizes, out, i, k = [3, 5, 2, 7, 4, 1, 6], [], 0, 0
    while i < len(text):
        out.append(text[i:i + sizes[k % len(sizes)]])
        i += sizes[k % len(sizes)]
        k += 1
    return out


def _chunk(cid, model, idx, delta, finish=None):
    return {"id": cid, "object": "chat.completion.chunk", "created": int(time.time()), "model": model,
            "choices": [{"index": idx, "delta": delta, "finish_reason": finish}]}


def _sse(obj):
    return f"data: {json.dumps(obj)}\n\n"


async def chat(request: Request):
    request.app.state.last_auth = request.headers.get("authorization")
    body = await request.json()
    model = body.get("model", "mock-pii")
    n = int(body.get("n") or 1)
    cid = "chatcmpl-" + uuid.uuid4().hex[:12]

    if model == "mock-error":
        return JSONResponse({"error": "Traceback (most recent call last): db-7.internal timeout"}, status_code=500)

    if not body.get("stream"):
        return JSONResponse({
            "id": cid, "object": "chat.completion", "created": int(time.time()), "model": model,
            "choices": [{"index": i, "message": {"role": "assistant", "content": SCRIPT}, "finish_reason": "stop"}
                        for i in range(n)],
            "usage": {"prompt_tokens": 5, "completion_tokens": 30, "total_tokens": 35},
        })

    gate: threading.Event = request.app.state.gate

    async def gen():
        for i in range(n):
            yield _sse(_chunk(cid, model, i, {"role": "assistant", "content": ""}))
        if model == "mock-gated":
            yield _sse(_chunk(cid, model, 0, {"content": "Hello there, "}))
            while not gate.is_set():
                await asyncio.sleep(0.01)
            yield _sse(_chunk(cid, model, 0, {"content": "all done."}))
            yield _sse(_chunk(cid, model, 0, {}, "stop"))
        else:
            for piece in _pieces(SCRIPT):
                for i in range(n):
                    yield _sse(_chunk(cid, model, i, {"content": piece}))
                await asyncio.sleep(0.002)
            for i in range(n):
                yield _sse(_chunk(cid, model, i, {}, "stop"))
        yield "data: [DONE]\n\n"

    return StreamingResponse(gen(), media_type="text/event-stream")


def create_mock():
    app = Starlette(routes=[Route("/v1/chat/completions", chat, methods=["POST"])])
    app.state.gate = threading.Event()
    app.state.gate.set()
    app.state.last_auth = None
    return app


app = create_mock()
