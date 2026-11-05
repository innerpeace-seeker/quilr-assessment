# Quilr assessment: MCP / LLM gateway tasks

All four tasks are in one Python 3.13 project managed with [uv](https://docs.astral.sh/uv/).

```
uv sync
uv run pytest -q          # whole suite, no network needed
```

| Task | Package | What it is |
|------|---------|------------|
| 1 | `task1_mcp_server/` | stdio MCP server, `get_customer_record` + `trigger_refund` |
| 2 | `task2_mcp_gateway/` | HTTP reverse proxy for MCP with bearer auth and `admin_*` tool gating |
| 3 | `task3_pii_stream/` | LLM gateway that redacts emails / SSNs / card numbers from a streamed completion |
| 4 | `task4_router/` | completion router with a sqlite token limiter and primary -> secondary failover |

Each package has a `mock_*.py` upstream so everything runs locally. The plan I worked from is in
`docs/plan.md`.

---

## Task 1: MCP server

```
uv run python -m task1_mcp_server
```

Hook it into an MCP client (Claude Desktop, Inspector, ...) as a stdio server with command
`uv`, args `run --directory <repo> python -m task1_mcp_server`.

Notes:

- Built on the SDK's low-level `Server` (mcp 2.3). Arguments are validated with strict
  Pydantic models (`models.py`): no type coercion, unknown fields rejected,
  `customer_id` must be `CUST-` + 5 digits, amount > 0 with at most 2 decimals, and the
  reason must be 10–500 characters after trimming.
- Bad arguments raise `MCPError(-32602)`, with `data` listing `{field, error}` pairs. The
  input value is never echoed back. An unknown tool is also -32602. Business failures
  (customer not found, refund above balance) come back as a normal result with
  `isError: true`, which is how the MCP spec wants tool-execution errors reported.
- **stdout.** `transport.py` replaces the SDK's `stdio_server()`. While testing I found two
  gaps in it:
  1. A `print()` inside a handler is buffered in `sys.stdout` and gets flushed *after* the
     SDK restores fd 1, so it ends up on the wire anyway.
  2. A line that isn't JSON is dropped silently. JSON-RPC 2.0 requires a `-32700` reply.

  So at startup (before other imports) the process dups the real stdout for itself,
  points fd 1 and `sys.stdout` at stderr, and a single writer task is the only code
  that touches the wire. Parse errors get -32700 with `id: null`, and well-formed JSON that
  isn't JSON-RPC gets -32600. Logging goes to stderr.
- The tests run the server as a real subprocess. One test turns on `T1_NOISY=1` so the
  handlers `print()`, then checks that every stdout line is still JSON-RPC.

## Task 2: MCP gateway

```
export GATEWAY_SECRET=change-me-to-something-long-enough
uv run uvicorn task2_mcp_gateway.mock_downstream:app --port 9001 &
DOWNSTREAM_URL=http://127.0.0.1:9001/mcp uv run uvicorn task2_mcp_gateway.app:app --port 9000 &

TOKEN=$(uv run python -m task2_mcp_gateway viewer bob)
curl -s localhost:9000/mcp -H "Authorization: Bearer $TOKEN" -H 'content-type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"admin_reset_key","arguments":{}}}'
# {"jsonrpc":"2.0","id":1,"error":{"code":-32001,"message":"Unauthorized Tool Call",...}}
```

- Tokens are HS256 JWTs with a `role` claim (`admin` | `viewer`). The algorithm is pinned,
  so `alg: none` can't get through, and `exp` and `sub` are required. A missing or bad
  token returns HTTP 401 with `WWW-Authenticate`.
- The body is read once (1 MB cap) and parsed. `tools/call` with a name starting with
  `admin_` (case-insensitive) from a non-admin is answered with -32001 by the gateway, and
  the downstream never sees it. Everything else is forwarded **as the original bytes**,
  and the response is streamed back as-is, so JSON and SSE replies both work, and
  `Mcp-Session-Id` passes through in both directions.
- The client's `Authorization` header is not forwarded. The downstream gets
  `x-forwarded-user` / `x-forwarded-role` instead.
- Batches (allowed in 2025-03-26) are checked per message. Denied entries get -32001, the
  rest are forwarded as a smaller batch, and the replies are merged.
- Malformed JSON returns -32700 / HTTP 400, and an invalid envelope returns -32600 / 400. If
  the downstream is unreachable you get -32603 / 502 with no host details.

## Task 3: streaming PII redaction

```
uv run uvicorn task3_pii_stream.mock_provider:app --port 9101 &
UPSTREAM_URL=http://127.0.0.1:9101 uv run uvicorn task3_pii_stream.app:app --port 9100 &
curl -N localhost:9100/v1/chat/completions -H 'content-type: application/json' \
  -d '{"model":"mock-pii","stream":true,"messages":[]}'
```

The endpoint is OpenAI-compatible (`/v1/chat/completions`) and handles both `stream: true`
and non-streaming requests.

How the redactor (`redact.py`) works: it uses the `regex` module's partial matching to find
the earliest position in the buffer that *could still* be the start of an email, SSN or
card number if more text arrived. Everything before that point is final. Complete matches
in it are replaced with `[REDACTED]` and it is sent immediately. Only the tail is kept,
which in practice is the last word or digit group. The tail is capped at 256 characters,
so memory per choice stays constant however long the response is.

Some details that took care:

- A match that ends before the buffer does is still not final. `a@b.example.` matches as
  `a@b.example` but may turn into `a@b.example.com`.
- `finditer(partial=True)` reports a full match on the right before a partial match that
  starts further left. The randomized test caught this leaking the start of an email, so
  the hold point is found by its own leftmost scan.
- Card numbers are Luhn-checked, so order numbers and other long digit runs pass through
  untouched. The card regex is greedy, so `4242 4242 4242 4242 078-05-1120` first matches
  as 19 digits and fails Luhn. Shorter spans that end at a group boundary are tried next,
  so both the card and the SSN still get caught.
- The chunk after each delta is rewritten in place, with one redactor per `choices[i]`.
  When `finish_reason` arrives, whatever is held is flushed into that chunk.

Tests include every 2-way split of a sample sentence, char-by-char feeding, and 300 seeded
random chunkings, all compared against one-shot redaction. There is also a TTFT test where
the provider blocks mid-stream and the client must already have the first text. Throughput
is roughly 120k chunks/s on my machine, far more than any model's token rate.

Limitations: only `delta.content` is scanned, not tool-call arguments. Regexes will miss
PII written out in words ("four one one one ..."). Past the 256-char cap everything except the last 40
characters is released, so a card or SSN still being typed (at most 37 characters) is never
split. A single email-like run longer than that could still get through.

## Task 4: rate limiting + failover

```
uv run uvicorn task4_router.mock_providers:primary --port 9201 &
uv run uvicorn task4_router.mock_providers:secondary --port 9202 &
uv run uvicorn task4_router.app:app --port 9200 &
curl -s localhost:9200/v1/chat/completions -H 'Authorization: Bearer tenant-1' \
  -H 'content-type: application/json' -d '{"messages":[{"role":"user","content":"hi"}]}' -i
```
(`PRIMARY_MODE=slow` or `PRIMARY_MODE=429` on the first mock shows the failover.)

**Limiter (`limiter.py`).** A sqlite file in WAL mode with one row per request:
`(tenant, ts, tokens)`. The tenant is the sha256 of the API key, never the raw key. A
tenant's usage is the sum of its rows from the last 60 s.

- Each request reserves `prompt_chars/4 + max_tokens` up front (1024 if the client didn't
  set `max_tokens`) and is settled to the provider's real `usage.total_tokens` afterwards.
  If the request fails or the handler is cancelled, the reservation is released. That
  includes a cancel that lands while `reserve()` is still waiting on sqlite's write lock:
  the worker thread can't be stopped, so its row is deleted when it commits. A client that
  just disconnects doesn't cancel the handler under uvicorn: the upstream call finishes
  and is billed, which is correct because the tokens really were spent.
- The client's JSON goes upstream as sent, with only `model` replaced. Pydantic is only
  used to validate it and estimate the token cost.
- Delete-expired, sum, compare and insert all run in one `BEGIN IMMEDIATE` transaction.
  Concurrent requests, even from several processes sharing the file, therefore can't both
  squeeze under the limit. A test fires 20 concurrent 5k reservations and exactly 10 get
  through.
- `Retry-After` is computed by walking rows oldest-first until enough tokens would have
  aged out.
- Each request deletes that tenant's expired rows, and a global sweep every 200 requests
  cleans up tenants that went quiet.

**Router (`router.py`).**

- Failover from primary to secondary happens on 429, on timeout, and also on 5xx,
  transport errors or a body that isn't valid JSON.
- Other 4xx responses are not retried, because the second provider would reject the same
  request. They map to 400.
- The 3 s deadline is one `asyncio.timeout` around the *whole* call: connect, headers and
  body. httpx's own timeouts apply per socket read, so a provider dripping a byte every
  half-second would never trip them; there's a test for exactly that against a real
  socket. When the deadline fires, the in-flight request is cancelled (the test checks the
  mock saw the cancellation).
- The calls are sequential, so a late primary reply can never race the secondary's.

**Errors.** Everything leaves as
`{"error": {"type", "code", "message", "request_id"}}` with an `x-request-id` header.

- Upstream bodies, stack traces and hostnames only go to the log.
- FastAPI's 422 validation detail, its 404/405 responses and any unhandled exception are
  all mapped onto the same error shape.

## Decisions where the brief was open

- `CUST-XXXXX` means 5 digits.
- Task 2: the `admin_` prefix check is case-insensitive. A denied notification gets HTTP
  403, because there's no id to answer with a JSON-RPC error.
- Task 3: an upstream 4xx keeps its status code with a generic body, and a 5xx becomes 502.
- Task 4:
  - The secondary has its own timeout (10 s default).
  - Any non-empty bearer key counts as a tenant. A real deployment would check keys
    against a registry first.
  - `stream: true` is rejected on this endpoint.
