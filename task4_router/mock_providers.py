"""Fake OpenAI-compatible provider whose behaviour can be switched at runtime.

app.state.mode: ok | 429 | 500 | slow | slow_body | garbage
Run two of them for a manual demo:
  uv run uvicorn task4_router.mock_providers:primary --port 9201
  uv run uvicorn task4_router.mock_providers:secondary --port 9202
"""
import asyncio
import json
import os
import time

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response, StreamingResponse
from starlette.routing import Route


def _completion(model, text, prompt_tokens=12, completion_tokens=8):
    return {
        "id": f"cmpl-{int(time.time() * 1000)}", "object": "chat.completion", "created": int(time.time()),
        "model": model,
        "choices": [{"index": 0, "message": {"role": "assistant", "content": text}, "finish_reason": "stop"}],
        "usage": {"prompt_tokens": prompt_tokens, "completion_tokens": completion_tokens,
                  "total_tokens": prompt_tokens + completion_tokens},
    }


def create_provider(name: str, mode: str = "ok", delay: float = 5.0):
    async def chat(request: Request):
        st = request.app.state
        st.calls += 1
        body = await request.json()
        st.last_body = body
        st.last_auth = request.headers.get("authorization")
        try:
            if st.mode == "429":
                return JSONResponse({"error": {"message": "Rate limit reached for org-123"}}, status_code=429)
            if st.mode == "500":
                return Response("Traceback (most recent call last):\n  File \"/srv/llm/app.py\", line 42\n"
                                "KeyError: 'shard-db-7.internal'", status_code=500)
            if st.mode == "garbage":
                return Response("<html>bad gateway</html>", status_code=200, media_type="text/html")
            if st.mode == "slow":
                await asyncio.sleep(st.delay)
            if st.mode == "slow_body":
                payload = json.dumps(_completion(body.get("model"), f"hi from {name}")).encode()

                async def drip():
                    # headers go out right away, the body trickles in
                    for i in range(0, len(payload), 8):
                        yield payload[i:i + 8]
                        await asyncio.sleep(st.delay / max(1, len(payload) // 8))
                return StreamingResponse(drip(), media_type="application/json")
            return JSONResponse(_completion(body.get("model"), f"hi from {name}"))
        except asyncio.CancelledError:
            st.cancelled += 1
            raise

    app = Starlette(routes=[Route("/v1/chat/completions", chat, methods=["POST"])])
    app.state.mode = mode
    app.state.delay = delay
    app.state.calls = 0
    app.state.cancelled = 0
    app.state.last_body = None
    app.state.last_auth = None
    return app


primary = create_provider("primary", os.environ.get("PRIMARY_MODE", "ok"))
secondary = create_provider("secondary", os.environ.get("SECONDARY_MODE", "ok"))
