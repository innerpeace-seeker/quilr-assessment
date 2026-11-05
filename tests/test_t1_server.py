"""Drives the real server as a subprocess, over raw newline-delimited JSON-RPC."""
import json
import os
import queue
import subprocess
import sys
import threading

import pytest

PROTO = "2025-06-18"


class Proc:
    def __init__(self, env_extra=None):
        env = dict(os.environ, **(env_extra or {}))
        self.p = subprocess.Popen(
            [sys.executable, "-m", "task1_mcp_server"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            env=env, cwd=os.path.dirname(os.path.dirname(__file__)),
        )
        self.out: queue.Queue = queue.Queue()
        self.raw_stdout: list[bytes] = []
        self.stderr: list[bytes] = []
        threading.Thread(target=self._pump_out, daemon=True).start()
        threading.Thread(target=self._pump_err, daemon=True).start()

    def _pump_out(self):
        for line in self.p.stdout:
            self.raw_stdout.append(line)
            self.out.put(line)

    def _pump_err(self):
        for line in self.p.stderr:
            self.stderr.append(line)

    def send_raw(self, line: str):
        self.p.stdin.write(line.encode() + b"\n")
        self.p.stdin.flush()

    def send(self, msg: dict):
        self.send_raw(json.dumps(msg))

    def recv(self, timeout=10):
        return json.loads(self.out.get(timeout=timeout))

    def request(self, id_, method, params=None):
        msg = {"jsonrpc": "2.0", "id": id_, "method": method}
        if params is not None:
            msg["params"] = params
        self.send(msg)
        return self.recv()

    def call(self, id_, name, arguments):
        return self.request(id_, "tools/call", {"name": name, "arguments": arguments})

    def close(self):
        try:
            self.p.stdin.close()
            self.p.wait(timeout=10)
        except Exception:
            self.p.kill()


def _init(proc):
    resp = proc.request(0, "initialize", {
        "protocolVersion": PROTO, "capabilities": {}, "clientInfo": {"name": "pytest", "version": "0"}})
    proc.send({"jsonrpc": "2.0", "method": "notifications/initialized"})
    return resp


@pytest.fixture
def srv():
    p = Proc()
    _init(p)
    yield p
    p.close()


def test_initialize_and_list_tools(srv):
    resp = srv.request(1, "tools/list")
    tools = {t["name"]: t for t in resp["result"]["tools"]}
    assert set(tools) == {"get_customer_record", "trigger_refund"}
    schema = tools["trigger_refund"]["inputSchema"]
    assert schema["type"] == "object"
    assert set(schema["required"]) == {"customer_id", "amount", "reason"}
    assert schema.get("additionalProperties") is False


def test_get_customer_ok(srv):
    resp = srv.call(2, "get_customer_record", {"customer_id": "CUST-00042"})
    result = resp["result"]
    assert result.get("isError") is not True
    assert result["structuredContent"]["name"] == "Ada Lovelace"
    assert "Ada Lovelace" in result["content"][0]["text"]


@pytest.mark.parametrize("args", [
    {"customer_id": "CUST-1"},
    {"customer_id": 42},
    {},
    {"customer_id": "CUST-00042", "drop_table": True},
])
def test_get_customer_bad_input_is_invalid_params(srv, args):
    resp = srv.call(3, "get_customer_record", args)
    assert resp["id"] == 3
    assert resp["error"]["code"] == -32602
    assert isinstance(resp["error"]["data"], list) and resp["error"]["data"]


def test_arguments_not_an_object(srv):
    resp = srv.request(4, "tools/call", {"name": "get_customer_record", "arguments": "CUST-00042"})
    assert resp["error"]["code"] == -32602


@pytest.mark.parametrize("args,field", [
    ({"customer_id": "CUST-00042", "amount": -5, "reason": "customer complaint"}, "amount"),
    ({"customer_id": "CUST-00042", "amount": 5, "reason": "too short"}, "reason"),
    ({"customer_id": "CUST-00042", "amount": "5", "reason": "customer complaint"}, "amount"),
    ({"customer_id": "CUST-00042", "reason": "customer complaint"}, "amount"),
])
def test_refund_bad_input(srv, args, field):
    resp = srv.call(5, "trigger_refund", args)
    assert resp["error"]["code"] == -32602
    assert field in {d["field"] for d in resp["error"]["data"]}


def test_refund_ok(srv):
    resp = srv.call(6, "trigger_refund", {"customer_id": "CUST-00042", "amount": 20.5,
                                           "reason": "Package never arrived"})
    sc = resp["result"]["structuredContent"]
    assert sc["status"] == "processed" and sc["amount"] == 20.5


def test_unknown_customer_is_tool_error_not_protocol_error(srv):
    resp = srv.call(7, "get_customer_record", {"customer_id": "CUST-99999"})
    assert "error" not in resp
    assert resp["result"]["isError"] is True


def test_refund_over_balance_is_tool_error(srv):
    resp = srv.call(8, "trigger_refund", {"customer_id": "CUST-31337", "amount": 10,
                                           "reason": "Package never arrived"})
    assert resp["result"]["isError"] is True


def test_unknown_tool(srv):
    resp = srv.call(9, "drop_database", {})
    assert resp["error"]["code"] == -32602


def test_unknown_method(srv):
    resp = srv.request(10, "tools/explode")
    assert resp["error"]["code"] == -32601


def test_garbage_line_gets_parse_error(srv):
    srv.send_raw("this is not json {")
    resp = srv.recv()
    assert resp == {"jsonrpc": "2.0", "id": None, "error": {"code": -32700, "message": "Parse error"}}
    # and the server is still alive afterwards
    assert srv.request(11, "ping")["result"] == {}


@pytest.mark.parametrize("line,expected_id", [
    ("[1, 2]", None),
    ('{"foo": 1}', None),
    ('{"jsonrpc": "1.0", "id": 12, "method": "ping"}', 12),
    ('{"jsonrpc": "2.0", "id": 13}', 13),
])
def test_invalid_request(srv, line, expected_id):
    srv.send_raw(line)
    resp = srv.recv()
    assert resp["error"]["code"] == -32600
    assert resp["id"] == expected_id


def test_stdout_is_only_jsonrpc_even_when_handlers_print():
    p = Proc({"T1_NOISY": "1"})
    try:
        _init(p)
        p.request(1, "tools/list")
        p.call(2, "get_customer_record", {"customer_id": "CUST-00042"})
        p.call(3, "trigger_refund", {"customer_id": "CUST-00042", "amount": 1, "reason": "x"})
        p.send_raw("nope")
        p.recv()
    finally:
        p.close()
    assert p.raw_stdout, "expected some output"
    for line in p.raw_stdout:
        msg = json.loads(line)
        assert msg["jsonrpc"] == "2.0"
    err = b"".join(p.stderr)
    assert b"noisy handler" in err  # the stray print went somewhere harmless
