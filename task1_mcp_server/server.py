import json
import logging
import os

import mcp_types as types
from mcp.server.lowlevel import Server
from mcp.shared.exceptions import MCPError
from pydantic import ValidationError

from . import data
from .models import GetCustomerArgs, RefundArgs, validation_error_data

log = logging.getLogger("task1.server")

INVALID_PARAMS = -32602

TOOLS = {
    "get_customer_record": (GetCustomerArgs, "Look up a customer record by id."),
    "trigger_refund": (RefundArgs, "Issue a refund to a customer. Amount must not exceed the refundable balance."),
}


def _schema(model):
    s = model.model_json_schema()
    s.pop("title", None)
    return s


def _noisy(msg):
    # used by the stdout-purity test: a careless print() in a handler must not corrupt the wire
    if os.environ.get("T1_NOISY"):
        print("noisy handler:", msg)


def _ok(payload: dict) -> types.CallToolResult:
    return types.CallToolResult(
        content=[types.TextContent(text=json.dumps(payload))],
        structured_content=payload,
    )


def _tool_error(text: str) -> types.CallToolResult:
    # business failure: the model should see it and can react, so it's a result, not a protocol error
    return types.CallToolResult(content=[types.TextContent(text=text)], is_error=True)


async def list_tools(ctx, params):
    return types.ListToolsResult(tools=[
        types.Tool(name=name, description=desc, input_schema=_schema(model))
        for name, (model, desc) in TOOLS.items()
    ])


async def call_tool(ctx, params: types.CallToolRequestParams):
    entry = TOOLS.get(params.name)
    if entry is None:
        raise MCPError(INVALID_PARAMS, f"Unknown tool: {params.name}")
    model, _ = entry
    try:
        args = model.model_validate(params.arguments or {})
    except ValidationError as e:
        problems = validation_error_data(e)
        log.info("rejected %s call: %s", params.name, problems)
        raise MCPError(INVALID_PARAMS, f"Invalid arguments for {params.name}", data=problems) from None

    _noisy(params.name)

    if isinstance(args, GetCustomerArgs):
        rec = data.get_customer(args.customer_id)
        if rec is None:
            return _tool_error(f"No customer with id {args.customer_id}")
        return _ok(rec)

    try:
        refund = data.issue_refund(args.customer_id, args.amount, args.reason)
    except data.RefundRejected as e:
        return _tool_error(f"Refund rejected: {e}")
    log.info("refund %s issued for %s (%.2f)", refund["refund_id"], args.customer_id, args.amount)
    return _ok(refund)


def build_server() -> Server:
    return Server("customer-support", version="0.1.0",
                  on_list_tools=list_tools, on_call_tool=call_tool)
