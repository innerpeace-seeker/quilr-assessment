"""MCP gateway: bearer auth + tool-level authorization in front of a downstream MCP server.

Run:
  GATEWAY_SECRET=... DOWNSTREAM_URL=http://127.0.0.1:9001/mcp \
    uv run uvicorn task2_mcp_gateway.app:app --port 9000
"""
import json
import logging
import os
from contextlib import asynccontextmanager

import httpx
from starlette.applications import Starlette
from starlette.background import BackgroundTask
from starlette.requests import Request
from starlette.responses import JSONResponse, Response, StreamingResponse
from starlette.routing import Route

from . import jsonrpc as rpc
from .auth import AuthError, role_from_header

log = logging.getLogger("task2.gateway")

MAX_BODY = 1_000_000

# request headers worth passing on; everything else (Authorization especially) stays here
FORWARD_REQ = ("content-type", "accept", "mcp-session-id", "mcp-protocol-version", "last-event-id")
HOP_BY_HOP = {"connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailer",
              "transfer-encoding", "upgrade", "content-length"}


def _json(body, status=200, headers=None):
    return JSONResponse(body, status_code=status, headers=headers)


class Gateway:
    def __init__(self, downstream_url, secret, client=None):
        self.url = downstream_url
        self.secret = secret
        self._own_client = client is None
        self.client = client or httpx.AsyncClient(timeout=httpx.Timeout(60.0, connect=5.0))

    async def aclose(self):
        if self._own_client:
            await self.client.aclose()

    def _upstream_headers(self, request, ident):
        h = {k: v for k in FORWARD_REQ if (v := request.headers.get(k)) is not None}
        h["x-forwarded-user"] = ident.sub
        h["x-forwarded-role"] = ident.role
        return h

    async def handle(self, request: Request):
        try:
            ident = role_from_header(request.headers.get("authorization"), self.secret)
        except AuthError as e:
            log.info("auth failed: %s", e)
            return _json(rpc.error(None, -32000, "Authentication required"), 401,
                         {"WWW-Authenticate": 'Bearer realm="mcp-gateway"'})

        if request.method != "POST":
            # GET opens the server->client SSE stream, DELETE ends a session; no payload to inspect
            return await self._forward(request, ident, None, None)

        if int(request.headers.get("content-length") or 0) > MAX_BODY:
            return _json(rpc.error(None, rpc.INVALID_REQUEST, "Request too large"), 413)
        raw = b""
        async for chunk in request.stream():
            raw += chunk
            if len(raw) > MAX_BODY:
                return _json(rpc.error(None, rpc.INVALID_REQUEST, "Request too large"), 413)

        try:
            body = rpc.parse_body(raw)
        except rpc.RpcParseError:
            return _json(rpc.error(None, rpc.PARSE_ERROR, "Parse error"), 400)
        except rpc.RpcInvalidRequest as e:
            log.info("invalid request from %s: %s", ident.sub, e)
            return _json(rpc.error(None, rpc.INVALID_REQUEST, "Invalid Request"), 400)

        if isinstance(body, list):
            return await self._batch(request, ident, body, raw)

        denied = rpc.check_message(body, ident.role)
        if denied is not None:
            self._log_denied(ident, body)
            if "id" not in body:
                # a notification can't receive a JSON-RPC reply, so say no at the HTTP level
                return _json(denied, 403)
            return _json(denied)
        return await self._forward(request, ident, raw, body.get("id"))

    async def _batch(self, request, ident, msgs, raw):
        errors, allowed = [], []
        for m in msgs:
            d = rpc.check_message(m, ident.role)
            if d is None:
                allowed.append(m)
            else:
                self._log_denied(ident, m)
                if "id" in m:
                    errors.append(d)

        if not allowed:
            return _json(errors) if errors else Response(status_code=202)
        if not errors:
            # nothing was removed, forward the original bytes as-is
            return await self._forward(request, ident, raw, None)

        try:
            resp = await self.client.post(self.url, content=json.dumps(allowed).encode(),
                                          headers=self._upstream_headers(request, ident))
        except httpx.HTTPError as e:
            log.warning("downstream error on batch: %r", e)
            return _json(errors + [rpc.error(m["id"], rpc.INTERNAL_ERROR, "Upstream unavailable")
                                   for m in allowed if "id" in m], 502)
        if resp.status_code == 202:
            return _json(errors)
        try:
            upstream = resp.json()
        except ValueError:
            log.warning("downstream sent non-JSON reply to a batch (%s)", resp.headers.get("content-type"))
            return _json(errors + [rpc.error(m["id"], rpc.INTERNAL_ERROR, "Bad upstream response")
                                   for m in allowed if "id" in m], 502)
        if not isinstance(upstream, list):
            upstream = [upstream]
        return _json(upstream + errors, resp.status_code)

    async def _forward(self, request, ident, raw, id_):
        req = self.client.build_request(request.method, self.url, content=raw,
                                        headers=self._upstream_headers(request, ident))
        try:
            resp = await self.client.send(req, stream=True)
        except httpx.HTTPError as e:
            log.warning("downstream unreachable: %r", e)
            return _json(rpc.error(id_, rpc.INTERNAL_ERROR, "Upstream unavailable"), 502)

        headers = {k: v for k, v in resp.headers.items() if k.lower() not in HOP_BY_HOP}
        # aiter_raw: pass bytes through as they arrive, SSE included, without decoding
        return StreamingResponse(resp.aiter_raw(), status_code=resp.status_code, headers=headers,
                                 background=BackgroundTask(resp.aclose))

    def _log_denied(self, ident, msg):
        params = msg.get("params")
        tool = params.get("name") if isinstance(params, dict) else None
        log.warning("denied tools/call %r for %s (role=%s)", tool, ident.sub, ident.role)


def create_app(downstream_url: str, secret: str, client: httpx.AsyncClient | None = None) -> Starlette:
    gw = Gateway(downstream_url, secret, client)

    @asynccontextmanager
    async def lifespan(app):
        yield
        await gw.aclose()

    app = Starlette(routes=[Route("/mcp", gw.handle, methods=["GET", "POST", "DELETE"])], lifespan=lifespan)
    app.state.gateway = gw
    return app


def _from_env():
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
    secret = os.environ.get("GATEWAY_SECRET")
    if not secret:
        raise RuntimeError("GATEWAY_SECRET is not set")
    return create_app(os.environ.get("DOWNSTREAM_URL", "http://127.0.0.1:9001/mcp"), secret)


def __getattr__(name):
    # lets `uvicorn task2_mcp_gateway.app:app` work without building an app at import time in tests
    if name == "app":
        return _from_env()
    raise AttributeError(name)
