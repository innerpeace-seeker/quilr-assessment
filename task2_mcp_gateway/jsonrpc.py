import json

PARSE_ERROR = -32700
INVALID_REQUEST = -32600
INVALID_PARAMS = -32602
INTERNAL_ERROR = -32603
UNAUTHORIZED_TOOL = -32001

ADMIN_PREFIX = "admin_"


class RpcParseError(Exception):
    pass


class RpcInvalidRequest(Exception):
    pass


def error(id_, code, message, data=None):
    err = {"code": code, "message": message}
    if data is not None:
        err["data"] = data
    return {"jsonrpc": "2.0", "id": id_, "error": err}


def _valid_id(v):
    return v is None or (isinstance(v, (str, int)) and not isinstance(v, bool))


def _check_shape(msg):
    if not isinstance(msg, dict) or msg.get("jsonrpc") != "2.0":
        raise RpcInvalidRequest("not a JSON-RPC 2.0 object")
    if "id" in msg and not _valid_id(msg["id"]):
        raise RpcInvalidRequest("bad id")
    if "method" in msg:
        if not isinstance(msg["method"], str):
            raise RpcInvalidRequest("method must be a string")
    elif "id" not in msg or not ("result" in msg or "error" in msg):
        # neither a request/notification nor a response
        raise RpcInvalidRequest("no method")


def parse_body(raw: bytes):
    try:
        body = json.loads(raw)
    except (ValueError, UnicodeDecodeError):
        raise RpcParseError() from None
    if isinstance(body, list):
        if not body:
            raise RpcInvalidRequest("empty batch")
        for m in body:
            _check_shape(m)
    else:
        _check_shape(body)
    return body


def check_message(msg: dict, role: str) -> dict | None:
    """Returns an error response if the gateway refuses this message, None to forward it."""
    if msg.get("method") != "tools/call":
        return None

    id_ = msg.get("id")
    params = msg.get("params")
    name = params.get("name") if isinstance(params, dict) else None
    if not isinstance(name, str):
        # can't make a policy decision without a tool name, so don't pass it on either
        return error(id_, INVALID_PARAMS, "Invalid params: tools/call requires a string 'name'")

    # casefold: if the downstream happens to resolve names case-insensitively,
    # "ADMIN_reset_key" must not be a way around the check
    if name.casefold().startswith(ADMIN_PREFIX) and role != "admin":
        return error(id_, UNAUTHORIZED_TOOL, "Unauthorized Tool Call",
                     {"tool": name, "required_role": "admin"})
    return None
