"""stdio transport that owns fd 1.

The SDK's stdio_server() diverts fd 1 while it serves, but anything buffered in
sys.stdout gets flushed after it restores the descriptor, and lines that aren't
JSON are dropped without a reply. Both are problems for a strict JSON-RPC peer,
so this module takes the wire away from the rest of the process at startup and
answers -32700 / -32600 itself.
"""
import json
import logging
import os
import sys
from contextlib import asynccontextmanager

import anyio
import mcp_types as types
from mcp.shared._context_streams import create_context_streams
from mcp.shared.message import SessionMessage
from pydantic import ValidationError

log = logging.getLogger(__name__)

PARSE_ERROR = -32700
INVALID_REQUEST = -32600

_wire = None


def claim_stdout():
    """Keep a private handle on the real stdout and point everything else at stderr.

    Call this before importing anything that might print. Idempotent.
    """
    global _wire
    if _wire is not None:
        return _wire
    sys.stdout.flush()
    _wire = os.fdopen(os.dup(1), "wb", buffering=0)
    os.dup2(2, 1)  # C extensions / child processes writing to fd 1 now hit stderr
    if sys.platform == "win32":
        try:
            from mcp.os.win32.utilities import rebind_std_handle_to_fd
            rebind_std_handle_to_fd(1)
        except Exception:  # pywin32 missing or the slot is locked; fd-level redirect still holds
            log.debug("could not rebind win32 stdout handle", exc_info=True)
    sys.stdout = sys.stderr
    return _wire


def _error_line(id_, code, message):
    return json.dumps({"jsonrpc": "2.0", "id": id_, "error": {"code": code, "message": message}},
                      separators=(",", ":"))


def _usable_id(obj):
    if isinstance(obj, dict):
        i = obj.get("id")
        # bool is an int subclass; true/false are not valid ids
        if isinstance(i, (str, int)) and not isinstance(i, bool):
            return i
    return None


def classify_line(line: bytes):
    """Returns (SessionMessage, None) for a good message or (None, error_json) for a bad one."""
    try:
        obj = json.loads(line)
    except (ValueError, UnicodeDecodeError):
        return None, _error_line(None, PARSE_ERROR, "Parse error")
    if not isinstance(obj, dict):
        # batches aren't part of MCP since 2025-06-18
        return None, _error_line(None, INVALID_REQUEST, "Invalid Request")
    try:
        msg = types.jsonrpc_message_adapter.validate_python(obj, by_name=False)
    except ValidationError:
        return None, _error_line(_usable_id(obj), INVALID_REQUEST, "Invalid Request")
    return SessionMessage(msg), None


@asynccontextmanager
async def strict_stdio():
    wire = claim_stdout()
    stdin = anyio.wrap_file(sys.stdin.buffer)

    read_send, read_recv = create_context_streams[SessionMessage | Exception](0)
    write_send, write_recv = create_context_streams[SessionMessage](0)
    out_send, out_recv = anyio.create_memory_object_stream[str](64)

    reader_scope = anyio.CancelScope()

    async def reader(errs):
        with reader_scope:
            async with read_send, errs:
                await _read_loop(errs)

    async def _read_loop(errs):
        async for raw in stdin:
            if not raw.strip():
                continue
            msg, err = classify_line(raw)
            if err is not None:
                log.warning("rejected inbound line (%d bytes): %s", len(raw), err)
                await errs.send(err)
            else:
                await read_send.send(msg)

    async def from_server(out):
        async with write_recv, out:
            async for sm in write_recv:
                await out.send(sm.message.model_dump_json(by_alias=True, exclude_unset=True))

    async def writer():
        # the only coroutine allowed to touch the wire
        async with out_recv:
            async for line in out_recv:
                await anyio.to_thread.run_sync(wire.write, line.encode("utf-8") + b"\n")

    async with anyio.create_task_group() as tg:
        tg.start_soon(reader, out_send.clone())
        tg.start_soon(from_server, out_send.clone())
        tg.start_soon(writer)
        out_send.close()  # only the clones keep the writer alive now
        try:
            yield read_recv, write_send
        finally:
            # stop reading, but let whatever the server already produced reach the wire
            reader_scope.cancel()
            await write_send.aclose()
