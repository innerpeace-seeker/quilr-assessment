import time

import jwt
import pytest

from task2_mcp_gateway.auth import AuthError, make_token, role_from_header
from task2_mcp_gateway.jsonrpc import RpcInvalidRequest, RpcParseError, check_message, parse_body

SECRET = "test-secret-that-is-long-enough-for-hs256"


def bearer(tok):
    return f"Bearer {tok}"


def test_roles_from_valid_tokens():
    assert role_from_header(bearer(make_token("alice", "admin", SECRET)), SECRET).role == "admin"
    ident = role_from_header(bearer(make_token("bob", "viewer", SECRET)), SECRET)
    assert ident.role == "viewer" and ident.sub == "bob"


def test_scheme_is_case_insensitive():
    assert role_from_header("bearer " + make_token("a", "viewer", SECRET), SECRET).role == "viewer"


@pytest.mark.parametrize("header", [
    None, "", "Bearer", "Bearer ", "Basic dXNlcjpwYXNz", "Token abc", "Bearer not.a.jwt",
    bearer(make_token("a", "admin", "some-other-secret-also-long-enough")),
    bearer(make_token("a", "admin", SECRET, ttl=-10)),
    bearer(jwt.encode({"sub": "a", "exp": int(time.time()) + 60}, SECRET, algorithm="HS256")),  # no role
    bearer(jwt.encode({"sub": "a", "role": "root", "exp": int(time.time()) + 60}, SECRET, algorithm="HS256")),
    bearer(jwt.encode({"sub": "a", "role": "admin"}, SECRET, algorithm="HS256")),  # no exp
    bearer(jwt.encode({"sub": "a", "role": "admin", "exp": int(time.time()) + 60}, key=None, algorithm="none")),
])
def test_bad_auth(header):
    with pytest.raises(AuthError):
        role_from_header(header, SECRET)


def test_parse_single_and_batch():
    assert parse_body(b'{"jsonrpc":"2.0","id":1,"method":"tools/list"}')["method"] == "tools/list"
    batch = parse_body(b'[{"jsonrpc":"2.0","id":1,"method":"ping"},{"jsonrpc":"2.0","method":"notifications/x"}]')
    assert isinstance(batch, list) and len(batch) == 2


@pytest.mark.parametrize("raw", [b"", b"{nope", b"\xff\xfe", b'{"a":1'])
def test_parse_error(raw):
    with pytest.raises(RpcParseError):
        parse_body(raw)


@pytest.mark.parametrize("raw", [
    b"[]", b"42", b'"x"', b'{"id":1,"method":"ping"}', b'{"jsonrpc":"2.0","id":1}',
    b'{"jsonrpc":"2.0","id":1,"method":5}', b'{"jsonrpc":"2.0","id":{"x":1},"method":"ping"}',
    b'[{"jsonrpc":"2.0","id":1,"method":"ping"}, 3]',
])
def test_invalid_request(raw):
    with pytest.raises(RpcInvalidRequest):
        parse_body(raw)


def test_client_responses_are_valid_messages():
    # clients answer server-initiated requests (sampling, elicitation) through the same endpoint
    assert parse_body(b'{"jsonrpc":"2.0","id":"s-1","result":{}}')["id"] == "s-1"


def call(name, id_=7):
    msg = {"jsonrpc": "2.0", "method": "tools/call", "params": {"name": name, "arguments": {}}}
    if id_ is not None:
        msg["id"] = id_
    return msg


def test_tools_list_allowed_for_viewer():
    assert check_message({"jsonrpc": "2.0", "id": 1, "method": "tools/list"}, "viewer") is None


def test_viewer_blocked_from_admin_tool():
    err = check_message(call("admin_reset_key", id_="abc"), "viewer")
    assert err == {"jsonrpc": "2.0", "id": "abc",
                   "error": {"code": -32001, "message": "Unauthorized Tool Call",
                             "data": {"tool": "admin_reset_key", "required_role": "admin"}}}


def test_admin_allowed_admin_tool():
    assert check_message(call("admin_reset_key"), "admin") is None


@pytest.mark.parametrize("name", ["ADMIN_reset_key", "Admin_x", "admin_"])
def test_prefix_variants_are_gated(name):
    assert check_message(call(name), "viewer")["error"]["code"] == -32001


@pytest.mark.parametrize("name", ["get_status", "administrator_info", "my_admin_tool", "admin"])
def test_non_admin_tools_pass(name):
    assert check_message(call(name), "viewer") is None


def test_denied_notification_has_no_id():
    err = check_message(call("admin_reset_key", id_=None), "viewer")
    assert err is not None and err["id"] is None


@pytest.mark.parametrize("params", [None, {}, {"name": 5}, {"name": None}, "admin_reset_key"])
def test_tools_call_without_usable_name(params):
    msg = {"jsonrpc": "2.0", "id": 3, "method": "tools/call"}
    if params is not None:
        msg["params"] = params
    assert check_message(msg, "admin")["error"]["code"] == -32602


@pytest.mark.parametrize("raw", [
    b'{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"admin_reset_key","name":"get_status"}}',
    b'{"jsonrpc":"2.0","id":1,"method":"tools/list","method":"tools/call"}',
])
def test_duplicate_keys_rejected(raw):
    # json.loads keeps the last duplicate; a downstream that keeps the first would run something else
    with pytest.raises(RpcInvalidRequest):
        parse_body(raw)
