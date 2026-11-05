"""Tiny stand-in for a downstream MCP server speaking Streamable HTTP.

Run: uv run uvicorn task2_mcp_gateway.mock_downstream:app --port 9001
"""
import json
import uuid

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response, StreamingResponse
from starlette.routing import Route

TOOLS = [
    {"name": "get_status", "description": "Service health", "inputSchema": {"type": "object"}},
    {"name": "slow_report", "description": "Streams progress then a result", "inputSchema": {"type": "object"}},
    {"name": "admin_reset_key", "description": "Rotate an API key",
     "inputSchema": {"type": "object", "properties": {"key_id": {"type": "string"}}}},
]


def _text(t):
    return {"content": [{"type": "text", "text": t}]}


def _handle(msg):
    method, id_ = msg.get("method"), msg.get("id")
    if id_ is None:
        return None
    if method == "initialize":
        result = {"protocolVersion": msg.get("params", {}).get("protocolVersion", "2025-06-18"),
                  "capabilities": {"tools": {}}, "serverInfo": {"name": "mock-downstream", "version": "0"}}
    elif method == "ping":
        result = {}
    elif method == "tools/list":
        result = {"tools": TOOLS}
    elif method == "tools/call":
        name = msg["params"]["name"]
        if name == "get_status":
            result = _text("all systems nominal")
        elif name == "admin_reset_key":
            key = msg["params"].get("arguments", {}).get("key_id", "?")
            result = _text(f"key {key} reset")
        else:
            return {"jsonrpc": "2.0", "id": id_, "error": {"code": -32602, "message": f"Unknown tool: {name}"}}
    else:
        return {"jsonrpc": "2.0", "id": id_, "error": {"code": -32601, "message": "Method not found"}}
    return {"jsonrpc": "2.0", "id": id_, "result": result}


async def mcp_endpoint(request: Request):
    raw = await request.body()
    request.app.state.calls.append({"body": raw, "headers": dict(request.headers)})
    body = json.loads(raw)
    headers = {}
    if isinstance(body, dict) and body.get("method") == "initialize":
        headers["Mcp-Session-Id"] = uuid.uuid4().hex

    if isinstance(body, dict) and body.get("method") == "tools/call" and body["params"]["name"] == "slow_report":
        async def events():
            progress = {"jsonrpc": "2.0", "method": "notifications/progress",
                        "params": {"progressToken": 1, "progress": 50}}
            yield f"event: message\ndata: {json.dumps(progress)}\n\n"
            yield f"event: message\ndata: {json.dumps({'jsonrpc': '2.0', 'id': body['id'], 'result': _text('done')})}\n\n"
        return StreamingResponse(events(), media_type="text/event-stream")

    if isinstance(body, list):
        out = [r for r in map(_handle, body) if r is not None]
        return JSONResponse(out, headers=headers) if out else Response(status_code=202)
    out = _handle(body)
    if out is None:
        return Response(status_code=202, headers=headers)
    return JSONResponse(out, headers=headers)


def create_mock():
    app = Starlette(routes=[Route("/mcp", mcp_endpoint, methods=["POST"])])
    app.state.calls = []
    return app


app = create_mock()
