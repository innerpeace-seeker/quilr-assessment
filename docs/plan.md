# Implementation plan: Quilr AI Field Developer assessment

Spec: the assessment brief (not included in this repo). This plan is the argument; the brief wins on conflict.

## Context and architecture decisions

One Python 3.13 repo managed with `uv`, one package per assessment task, shared test suite.
Python was chosen over TS because all four tasks lean on the same pieces (Pydantic, httpx,
an ASGI server, sqlite3 in the stdlib), so one toolchain covers everything.

```
task1_mcp_server/   stdio MCP server (official `mcp` SDK 2.3, low-level Server API)
task2_mcp_gateway/  Starlette reverse proxy in front of a downstream MCP-over-HTTP server
task3_pii_stream/   Starlette LLM gateway, streaming SSE passthrough with incremental redaction
task4_router/       FastAPI completion router: sqlite sliding-window limiter + failover
tests/              pytest, one file per module
```

Why each shape:

- **Task 1 — low-level `Server`, not `MCPServer`/FastMCP.** The high-level API turns
  validation failures into `isError` tool results. The spec wants protocol errors, so the
  handler validates with Pydantic and raises `MCPError(-32602, ...)`, which the SDK puts on
  the wire as a JSON-RPC error. Business failures (unknown customer, refund over balance)
  stay as `isError: true` tool results — that's what the MCP spec says tool-execution
  errors should be.
  Spike findings against mcp 2.3.0 (verified in this session):
  - `stdio_server()` diverts fd 1 while serving, but a buffered `print()` in a handler is
    flushed at exit after fd 1 is restored and lands on real stdout. So we own stdout:
    dup the real stdout for the transport and point `sys.stdout` at stderr before any
    user code runs.
  - A non-JSON line gets no reply at all (JSON-RPC 2.0 requires `-32700`, id null). So we
    write our own ~60-line stdio transport (same memory-stream contract as the SDK's) that
    answers parse errors (-32700) and non-request JSON (-32600) itself, through the single
    writer task so stdout writes never interleave.
- **Task 2 — Starlette + one shared `httpx.AsyncClient`.** Body is read once, parsed,
  authorized, then forwarded as the *original bytes* (no re-serialisation, so the downstream
  sees exactly what the client sent). Response is streamed back (works for both JSON and
  SSE replies of Streamable HTTP). Token = HS256 JWT with a `role` claim (PyJWT);
  missing/bad token → HTTP 401 (transport-level auth, as MCP's HTTP auth spec does).
  Denied `admin_*` call → HTTP 200 with JSON-RPC error `-32001`, downstream never called.
- **Task 3 — `regex` module partial matching.** Redactor keeps a small tail buffer: after
  each delta, complete matches are replaced, and everything before the earliest
  *possible* match start (a partial match at buffer end, or a full match that touches the
  end and could still grow) is emitted immediately. Tail is hard-capped (default 256
  chars) so memory is O(longest PII), never O(response). One redactor per `choices[i]`.
- **Task 4 — stdlib `sqlite3` on disk, WAL, `BEGIN IMMEDIATE`, run via `asyncio.to_thread`.**
  Check-and-reserve is one write transaction so concurrent requests (even multiple
  processes) can't both squeeze under the limit. Reserve estimated tokens
  (prompt chars/4 + max_tokens) up front, reconcile with real `usage.total_tokens` after,
  release on total failure. Eviction: rows older than the window are deleted inside the same
  transaction. Failover: `asyncio.timeout(3.0)` wraps the *whole* primary call (connect +
  body), so a slow body can't dodge the deadline; 429/timeout (plus 5xx/connect errors —
  see Ruling below) → secondary. All errors leave through one sanitiser that emits
  `{"error": {"type", "code", "message", "request_id"}}`; details go to the log only.

Style constraint from the user: code should read like a person wrote it — no docstring on
every function, no banner comments, comments only where the *why* isn't obvious.

## Global constraints

- Python >= 3.13, deps pinned in `pyproject.toml` via uv. Run everything with `uv run`.
- All tests: `uv run pytest -q`. No network: upstreams are in-process ASGI apps via
  `httpx.ASGITransport`, except Task 1 which is tested as a real subprocess.
- Task 1 logs go to stderr only. Nothing in the repo prints to stdout from task1 code.
- Mock servers live next to the code they serve (`mock_*.py`) and are runnable with uvicorn
  for manual demos.

## Rulings made while planning (provisional, spec is silent)

- `CUST-XXXXX` = `CUST-` + exactly 5 digits. Cost if wrong: one regex.
- Unknown tool name → `-32602` (what the MCP spec's own examples use).
- Task 4 also fails over on upstream 5xx and connection errors, not just 429/timeout. A
  gateway that 502s while a healthy backup exists is worse for users. Cost if wrong: two
  lines in `_should_failover`.
- Task 2: prefix check is case-insensitive (`ADMIN_reset` is also gated). Denying more is the
  safe direction. Batches are handled per-message: denied entries get -32001, the rest are
  forwarded as a smaller batch and the replies merged.

---

## Task 1: Scaffold + Task 1 input models

Files: `pyproject.toml` (pytest config: `asyncio_mode = "auto"`, `testpaths = ["tests"]`),
`task1_mcp_server/__init__.py`, `task1_mcp_server/models.py`, `task1_mcp_server/data.py`,
`tests/test_t1_models.py`, `.gitignore`.

Interfaces (produces):
- `GetCustomerArgs(customer_id: str)` — strict, `extra="forbid"`, pattern `^CUST-\d{5}$`.
- `RefundArgs(customer_id, amount: float, reason: str)` — strict; amount `> 0`, finite,
  max 2 decimal places, <= 1_000_000; reason stripped, `min_length=10`, `max_length=500`.
- `validation_error_data(exc: ValidationError) -> list[dict]` — `[{"field", "error"}]`,
  never echoes the input value.
- `data.CUSTOMERS: dict[str, dict]`, `data.issue_refund(customer_id, amount, reason) -> dict`.

Steps:
1. Write `tests/test_t1_models.py` with cases: valid id; `cust-12345`, `CUST-1234`,
   `CUST-123456`, `CUST-12A45`, ` CUST-12345`, `CUST-12345\n`, int 12345, None → rejected;
   amount `0`, `-1`, `nan`, `inf`, `"10.00"` (string), `True`, `10.005` → rejected; `10`
   (int) and `10.5` → accepted; reason `"short"`, `"         x"` (whitespace padded to
   >= 10) → rejected; extra field → rejected; error data contains field names and no input.
2. Run `uv run pytest tests/test_t1_models.py -q`. Expected: collection error (module missing).
3. Implement models + data.
4. Run again. Expected: all pass.
5. Commit `task1: input models and mock customer data`.

## Task 2: Task 1 server + strict stdio transport

Files: `task1_mcp_server/server.py`, `task1_mcp_server/transport.py`,
`task1_mcp_server/__main__.py`, `tests/test_t1_server.py`.

Interfaces:
- `transport.strict_stdio()` async context manager → `(read_stream, write_stream)`
  compatible with `Server.run`. Claims real stdout via `os.dup(1)`, rebinds `sys.stdout`
  to stderr. Bad JSON line → `{"jsonrpc":"2.0","id":null,"error":{"code":-32700,...}}`;
  JSON that isn't a JSON-RPC message → -32600 (id echoed if one was readable).
- `server.build_server() -> Server`; `python -m task1_mcp_server` runs it.

Tests (subprocess, raw newline-delimited JSON-RPC; helper sends lines and reads N responses
with a timeout):
- initialize → tools/list returns both tools with JSON schemas generated from the models.
- valid `get_customer_record` → result with text + structuredContent.
- `get_customer_record` with `CUST-1` → error -32602, data lists `customer_id`.
- `trigger_refund` amount -5 / reason short / missing field / extra field → -32602.
- unknown customer (valid format) → result with `isError: true` (not a protocol error).
- unknown tool → -32602.
- `garbage` line → -32700 with id null; `[1,2]`/`{"foo":1}` → -32600.
- unknown method → -32601.
- **stdout purity**: every stdout line parses as JSON with `jsonrpc == "2.0"`, and a
  debug env var `T1_NOISY=1` makes handlers `print()` — those lines must show up on stderr,
  never stdout.
Steps: write tests → run (Expected: fail, no module) → implement → run (Expected: pass) →
commit `task1: stdio MCP server with strict transport`.

## Task 3: Task 2 JSON-RPC inspection + auth

Files: `task2_mcp_gateway/__init__.py`, `jsonrpc.py`, `auth.py`, `tests/test_t2_policy.py`.

Interfaces:
- `auth.role_from_header(value: str | None, secret: str) -> str` raises `AuthError`;
  `auth.make_token(sub, role, secret, ttl=3600)` (used by tests and the demo script).
- `jsonrpc.parse_body(raw: bytes) -> list[dict] | dict` raises `RpcParseError` /
  `RpcInvalidRequest`.
- `jsonrpc.check_message(msg: dict, role: str) -> dict | None` → an error response if
  denied, else None. Only `tools/call` with `params.name` starting `admin_` (casefold) and
  role != admin is denied (`-32001`, "Unauthorized Tool Call"). `tools/call` with missing or
  non-string name → -32602 (gateway can't make a decision on it, so it doesn't forward).

Tests: valid admin/viewer tokens; expired, wrong secret, `Basic x`, no header, no role
claim → AuthError; parse of object, batch, empty batch (invalid), non-JSON; checks for
tools/list (allowed for viewer), viewer + admin_reset_key (denied, id preserved), admin +
admin_reset_key (allowed), viewer + `ADMIN_x` (denied), viewer + `get_status` (allowed),
notification-style tools/call (no id) denied → returns None-id marker handled by caller.
Steps: tests → fail → implement → pass → commit `task2: json-rpc parsing and role policy`.

## Task 4: Task 2 proxy app + mock downstream

Files: `task2_mcp_gateway/app.py`, `task2_mcp_gateway/mock_downstream.py`,
`tests/test_t2_proxy.py`.

Interfaces: `app.create_app(downstream_url: str, secret: str, client: httpx.AsyncClient | None = None) -> Starlette`.
Route `/mcp` POST/GET/DELETE. Forwarded request headers: content-type, accept,
mcp-session-id, mcp-protocol-version, last-event-id; plus `x-forwarded-user` (token sub).
Authorization header is *not* forwarded (the downstream shouldn't see client creds).
Hop-by-hop response headers stripped. Downstream unreachable → 502 with JSON-RPC error
-32603 and no internal detail.

Tests (mock downstream counts calls):
- tools/list for viewer → downstream response body passed through unchanged, 1 call.
- viewer calls admin_reset_key → `{"error":{"code":-32001}}`, id matches, downstream calls == 0.
- admin calls admin_reset_key → forwarded, result returned.
- no token → 401, downstream calls == 0.
- malformed JSON → -32700, downstream calls == 0.
- batch [list, admin call] as viewer → two responses merged, one denied, downstream got a
  1-element batch.
- notification (no id) → forwarded, 202 passthrough.
- downstream down → 502 sanitized.
Steps: tests → fail → implement → pass → commit `task2: gateway proxy and mock downstream`.

## Task 5: Task 3 streaming redactor

Files: `task3_pii_stream/__init__.py`, `task3_pii_stream/redact.py`, `tests/test_t3_redact.py`.

Interfaces: `StreamRedactor(max_hold=256)` with `.feed(text) -> str` and `.flush() -> str`;
`redact_text(text) -> str` (one-shot, for non-streaming responses).
Patterns: email, SSN `ddd-dd-dddd`, credit card 13–19 digits with optional single space/dash
separators, Luhn-checked (non-Luhn digit runs pass through).

Tests:
- one-shot redaction of each type; non-Luhn 16 digits untouched; phone-like `555-1234` untouched.
- every split point: for a sentence with an email, an SSN and a card, feed it in every
  possible 2-way split and also char-by-char; joined output == `redact_text(whole)` and no
  raw PII substring ever appears in any emitted piece.
- latency: feeding `"Hello there, "` emits `"Hello there, "` minus at most the trailing
  partial token immediately (not held until flush).
- memory bound: feeding 10_000 chars of `a` with no breaks keeps internal buffer <= max_hold.
- left context: `"x" + "123-45-6789"` split after `x` — lookbehind still respected (digit
  prefix like `1123-45-6789` must not be redacted as SSN).
Steps: tests → fail → implement → pass → commit `task3: incremental PII redactor`.

## Task 6: Task 3 SSE gateway + mock provider

Files: `task3_pii_stream/sse.py`, `task3_pii_stream/app.py`, `task3_pii_stream/mock_provider.py`,
`tests/test_t3_gateway.py`.

Interfaces: `sse.iter_events(lines: AsyncIterator[str]) -> AsyncIterator[str]` (yields the
joined `data:` payload per event); `app.create_app(upstream_url, api_key, client=None)`.
Route `POST /v1/chat/completions`. `stream: true` → `text/event-stream` with rewritten
chunks; `stream: false` → JSON with redacted `choices[].message.content`. Upstream non-2xx →
same status, generic error body. Forward `Authorization: Bearer <api_key>` (gateway's own key).

Tests (mock provider streams a scripted reply that splits an email/SSN/card across chunks,
with tiny sleeps):
- streamed: concatenated content has `[REDACTED]` ×3 and no raw PII in any SSE event.
- every emitted event is valid OpenAI chunk JSON and ends with `data: [DONE]`.
- TTFT: first event (role chunk) arrives before the provider finishes (provider waits on an
  `asyncio.Event` the test sets after receiving the first event — proves no buffering).
- n=2 choices redacted independently.
- non-streaming path redacted.
- upstream 500 → 502 generic body.
Steps: tests → fail → implement → pass → commit `task3: streaming guardrail gateway`.

## Task 7: Task 4 sqlite sliding-window limiter

Files: `task4_router/__init__.py`, `task4_router/limiter.py`, `tests/test_t4_limiter.py`.

Interfaces:
- `TokenLimiter(db_path, limit=50_000, window_s=60.0, clock=time.time)`
- `await reserve(tenant, tokens) -> Reservation | Denied` — `Reservation(id, tokens)`,
  `Denied(retry_after_s: float, used: int, limit: int)`.
- `await settle(reservation_id, actual_tokens)`; `await release(reservation_id)`.
- `await usage(tenant) -> int`.
Schema: `usage(id INTEGER PK, tenant TEXT, ts REAL, tokens INTEGER)`, index `(tenant, ts)`.
Tenant key is stored as sha256 hex, not raw API key.

Tests (fake clock): under limit ok; exactly at limit ok; over → Denied with retry_after equal
to when enough old tokens expire; window slides (advance clock 61s → allowed again, old rows
deleted from table); tenants isolated; settle lowers/raises usage; release frees; request
larger than limit → Denied with no retry possible (retry_after None); concurrency: 20
concurrent reserves of 5_000 against 50_000 → exactly 10 succeed; data persists across a new
limiter instance on the same file.
Steps: tests → fail → implement → pass → commit `task4: sqlite token limiter`.

## Task 8: Task 4 router, error sanitiser, app

Files: `task4_router/errors.py`, `task4_router/router.py`, `task4_router/app.py`,
`task4_router/mock_providers.py`, `tests/test_t4_router.py`.

Interfaces:
- `errors.GatewayError(status, code, message)`; `errors.error_body(code, message, request_id)`.
- `router.ModelRouter(primary: Provider, secondary: Provider, timeout_s=3.0)`;
  `await complete(payload) -> (provider_name, response_json)` raises `GatewayError`.
- `Provider(name, base_url, api_key, model, client)`.
- `app.create_app(router, limiter) -> FastAPI`; `POST /v1/completions` (and chat alias),
  tenant key from `Authorization: Bearer`. Response header `x-gateway-provider`.
  429 from limiter carries `Retry-After`.

Tests:
- primary ok → served by primary, secondary never called.
- primary 429 → secondary used.
- primary sleeps past timeout (test timeout 0.2s) → secondary used; elapsed < timeout + margin;
  primary's in-flight request was cancelled (mock sees `CancelledError`).
- primary slow *body* (headers fast, body slow) → still times out (whole-call deadline).
- primary 500 with a stack trace in body → secondary; both fail → 502 body has no
  "Traceback", no upstream URL, has request_id.
- both time out → 504 `upstream_timeout`.
- limiter: tenant over 50k → 429 `rate_limited` + Retry-After; usage reconciled to provider's
  `usage.total_tokens`; failed request releases reservation.
- missing bearer → 401 standardized body; malformed JSON body → 400 standardized (FastAPI's
  default 422 detail is replaced).
- unhandled exception inside the route → 500 standardized, no exception text.
- default timeout constant is 3.0.
Steps: tests → fail → implement → pass → commit `task4: fallback router and gateway app`.

## Task 9: README + demo entry points

Files: `README.md`, `scripts/` (demo launch notes are in README; no extra code unless needed).
README: how to run each task, design notes (short), the rulings above, known limitations.
Steps: write README; run full suite `uv run pytest -q` (Expected: all pass); manually run
the Task 1 server with a piped initialize to show it works; commit `docs: readme`.

## Review focus

- Task 1: anything that could write to fd 1 other than the transport writer.
- Task 3: redaction correctness at chunk boundaries for overlapping patterns (a card number
  next to an SSN), and whether the holdback can ever release a raw prefix of PII.
- Task 4: reservation leak paths (cancellation of the client request mid-flight must still
  release/settle), and the timeout vs. late-response race.
