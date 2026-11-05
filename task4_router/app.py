"""Completion gateway: per-tenant token limit + primary/secondary failover.

Run (with the two mock providers from mock_providers.py):
  PRIMARY_URL=http://127.0.0.1:9201 SECONDARY_URL=http://127.0.0.1:9202 \
    uv run uvicorn task4_router.app:app --port 9200
"""
import asyncio
import logging
import math
import os
import uuid
from contextlib import asynccontextmanager

import httpx
from fastapi import Depends, FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field, field_validator
from starlette.exceptions import HTTPException as StarletteHTTPException

from .errors import GatewayError, error_response, internal_error
from .limiter import Denied, TokenLimiter
from .router import ModelRouter, Provider

log = logging.getLogger("task4.app")


class Message(BaseModel):
    role: str
    content: str


class ChatRequest(BaseModel):
    # unknown fields (temperature, top_p, ...) are passed through to the provider
    model_config = ConfigDict(extra="allow")

    messages: list[Message] = Field(min_length=1)
    max_tokens: int = Field(default=1024, ge=1, le=32_000)
    stream: bool = False

    @field_validator("stream")
    @classmethod
    def _no_stream(cls, v):
        if v:
            raise ValueError("streaming is not supported on this endpoint")
        return v


def estimate_tokens(req: ChatRequest) -> int:
    # ~4 chars per token plus a little per-message overhead; reconciled with the real
    # count from the provider afterwards, so this only has to be in the right ballpark
    prompt = sum(len(m.content) // 4 + 4 for m in req.messages)
    return prompt + req.max_tokens


def _tenant(request: Request) -> str:
    scheme, _, key = (request.headers.get("authorization") or "").partition(" ")
    key = key.strip()
    if scheme.lower() != "bearer" or not key:
        raise GatewayError(401, "authentication_error", "missing_api_key",
                           "Provide an API key as 'Authorization: Bearer <key>'",
                           {"WWW-Authenticate": "Bearer"})
    return key


def create_app(router: ModelRouter, limiter: TokenLimiter, on_shutdown=None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app):
        yield
        if on_shutdown:
            await on_shutdown()

    app = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)

    @app.middleware("http")
    async def request_id_and_last_resort(request: Request, call_next):
        rid = uuid.uuid4().hex
        request.state.request_id = rid
        try:
            resp = await call_next(request)
        except Exception:
            # the full traceback stays in our logs, the client only gets the request id
            log.exception("unhandled error, request_id=%s", rid)
            return internal_error(rid)
        resp.headers["x-request-id"] = rid
        return resp

    @app.exception_handler(GatewayError)
    async def _gateway_error(request: Request, exc: GatewayError):
        return error_response(exc, request.state.request_id)

    @app.exception_handler(RequestValidationError)
    async def _bad_body(request: Request, exc: RequestValidationError):
        fields = sorted({".".join(str(p) for p in e["loc"][1:]) or "body" for e in exc.errors()})
        return error_response(GatewayError(400, "invalid_request_error", "invalid_body",
                                           "Invalid request body: " + ", ".join(fields)),
                              request.state.request_id)

    @app.exception_handler(StarletteHTTPException)
    async def _http_error(request: Request, exc: StarletteHTTPException):
        code = {404: "not_found", 405: "method_not_allowed"}.get(exc.status_code, "http_error")
        return error_response(GatewayError(exc.status_code, "invalid_request_error", code, code.replace("_", " ")),
                              request.state.request_id)

    @app.post("/v1/chat/completions")
    async def chat(body: ChatRequest, tenant: str = Depends(_tenant)):
        estimate = estimate_tokens(body)

        res = await limiter.reserve(tenant, estimate)
        if isinstance(res, Denied):
            if res.retry_after_s is None:
                raise GatewayError(429, "rate_limit_error", "request_exceeds_limit",
                                   f"Request needs ~{estimate} tokens, above the {res.limit} tokens/minute limit")
            raise GatewayError(429, "rate_limit_error", "tokens_per_minute_exceeded",
                               f"Token limit of {res.limit}/minute reached",
                               {"Retry-After": str(max(1, math.ceil(res.retry_after_s)))})

        try:
            provider, data = await router.complete(body.model_dump(exclude_none=True))
        except BaseException:
            # includes cancellation (client went away): the tokens were never used.
            # shield so a second cancel can't skip the cleanup.
            await asyncio.shield(limiter.release(res.id))
            raise

        actual = (data.get("usage") or {}).get("total_tokens")
        await limiter.settle(res.id, actual if isinstance(actual, int) else estimate)
        return JSONResponse(data, headers={"x-gateway-provider": provider})

    return app


def __getattr__(name):
    if name == "app":
        logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
        client = httpx.AsyncClient(timeout=httpx.Timeout(30.0, connect=3.0))
        env = os.environ.get
        router = ModelRouter(
            Provider("primary", env("PRIMARY_URL", "http://127.0.0.1:9201"), env("PRIMARY_API_KEY", ""),
                     env("PRIMARY_MODEL", "gpt-4o"), client),
            Provider("secondary", env("SECONDARY_URL", "http://127.0.0.1:9202"), env("SECONDARY_API_KEY", ""),
                     env("SECONDARY_MODEL", "gpt-4o-mini"), client),
            timeout_s=float(env("PRIMARY_TIMEOUT_S", "3.0")),
        )
        limiter = TokenLimiter(env("RATE_LIMIT_DB", "ratelimit.db"),
                               limit=int(env("TOKENS_PER_MINUTE", "50000")))
        return create_app(router, limiter, on_shutdown=client.aclose)
    raise AttributeError(name)
