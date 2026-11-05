"""LLM gateway endpoint that redacts PII out of streamed completions.

Run:
  UPSTREAM_URL=http://127.0.0.1:9101 UPSTREAM_API_KEY=sk-... \
    uv run uvicorn task3_pii_stream.app:app --port 9100
"""
import json
import logging
import os
from contextlib import asynccontextmanager

import httpx
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, StreamingResponse
from starlette.routing import Route

from .redact import StreamRedactor, redact_text
from .sse import iter_events

log = logging.getLogger("task3.gateway")


def _err(status, type_, message):
    return JSONResponse({"error": {"type": type_, "message": message}}, status_code=status)


def _sse(obj) -> str:
    return "data: " + json.dumps(obj, separators=(",", ":")) + "\n\n"


class ChoiceRedactors:
    """One redactor per choice index, created as indexes show up."""

    def __init__(self):
        self._by_index: dict[int, StreamRedactor] = {}

    def get(self, idx) -> StreamRedactor:
        r = self._by_index.get(idx)
        if r is None:
            r = self._by_index[idx] = StreamRedactor()
        return r

    def flush_all(self):
        for idx, r in self._by_index.items():
            text = r.flush()
            if text:
                yield idx, text


def rewrite_chunk(chunk: dict, redactors: ChoiceRedactors) -> dict | None:
    """Run every choice's delta through its redactor. Returns None if nothing is left to send."""
    worth_sending = not chunk.get("choices")  # e.g. the trailing usage-only chunk
    for ch in chunk.get("choices") or []:
        delta = ch.get("delta")
        if not isinstance(delta, dict):
            delta = ch["delta"] = {}
        r = redactors.get(ch.get("index", 0))
        content = delta.get("content")
        if content:
            delta["content"] = r.feed(content)
        if ch.get("finish_reason") is not None:
            tail = r.flush()
            if tail:
                delta["content"] = (delta.get("content") or "") + tail
        # keep the chunk if it carries anything besides an (now empty) content string
        if delta.get("content") or any(k != "content" for k in delta) or ch.get("finish_reason") is not None:
            worth_sending = True
    return chunk if worth_sending else None


class Gateway:
    def __init__(self, upstream_url, api_key, client=None):
        self.url = upstream_url.rstrip("/") + "/v1/chat/completions"
        self.api_key = api_key
        self._own = client is None
        # read timeout is between bytes, so a long generation is fine as long as tokens keep coming
        self.client = client or httpx.AsyncClient(timeout=httpx.Timeout(60.0, connect=5.0))

    async def chat(self, request: Request):
        try:
            body = json.loads(await request.body())
            if not isinstance(body, dict):
                raise ValueError
        except ValueError:
            return _err(400, "invalid_request", "Request body must be a JSON object")

        streaming = bool(body.get("stream"))
        req = self.client.build_request("POST", self.url, json=body,
                                        headers={"Authorization": f"Bearer {self.api_key}"})
        try:
            resp = await self.client.send(req, stream=True)
        except httpx.HTTPError as e:
            log.warning("upstream unreachable: %r", e)
            return _err(502, "upstream_error", "Upstream provider unavailable")

        if resp.status_code != 200:
            detail = (await resp.aread())[:500]
            await resp.aclose()
            log.warning("upstream returned %s: %r", resp.status_code, detail)
            if 400 <= resp.status_code < 500:
                return _err(resp.status_code, "upstream_rejected", "Upstream provider rejected the request")
            return _err(502, "upstream_error", "Upstream provider error")

        if not streaming:
            try:
                data = json.loads(await resp.aread())
            finally:
                await resp.aclose()
            for ch in data.get("choices") or []:
                msg = ch.get("message") or {}
                if isinstance(msg.get("content"), str):
                    msg["content"] = redact_text(msg["content"])
            return JSONResponse(data)

        return StreamingResponse(self._relay(resp), media_type="text/event-stream",
                                 headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

    async def _relay(self, resp: httpx.Response):
        redactors = ChoiceRedactors()
        last = None
        done = False
        try:
            async for data in iter_events(resp.aiter_lines()):
                if data == "[DONE]":
                    done = True
                    break
                try:
                    chunk = json.loads(data)
                except ValueError:
                    log.warning("dropping unparseable upstream event (%d bytes)", len(data))
                    continue
                last = chunk
                out = rewrite_chunk(chunk, redactors)
                if out is not None:
                    yield _sse(out)

            # anything still held (upstream ended without finish_reason, or cut off)
            for idx, text in redactors.flush_all():
                tail = {k: v for k, v in (last or {}).items() if k != "choices"}
                tail["choices"] = [{"index": idx, "delta": {"content": text}, "finish_reason": None}]
                yield _sse(tail)
            if done:
                yield "data: [DONE]\n\n"
        finally:
            await resp.aclose()

    async def aclose(self):
        if self._own:
            await self.client.aclose()


def create_app(upstream_url: str, api_key: str, client: httpx.AsyncClient | None = None) -> Starlette:
    gw = Gateway(upstream_url, api_key, client)

    @asynccontextmanager
    async def lifespan(app):
        yield
        await gw.aclose()

    return Starlette(routes=[Route("/v1/chat/completions", gw.chat, methods=["POST"])], lifespan=lifespan)


def __getattr__(name):
    if name == "app":
        logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
        return create_app(os.environ.get("UPSTREAM_URL", "http://127.0.0.1:9101"),
                          os.environ.get("UPSTREAM_API_KEY", ""))
    raise AttributeError(name)
