import math

import pytest
from pydantic import ValidationError

from task1_mcp_server.models import GetCustomerArgs, RefundArgs, validation_error_data


GOOD_REASON = "Item arrived damaged"


def test_valid_customer_id():
    assert GetCustomerArgs.model_validate({"customer_id": "CUST-00042"}).customer_id == "CUST-00042"


@pytest.mark.parametrize("bad", [
    "cust-12345", "CUST-1234", "CUST-123456", "CUST-12A45", " CUST-12345",
    "CUST-12345\n", "CUST_12345", "", 12345, None, ["CUST-12345"],
])
def test_bad_customer_ids(bad):
    with pytest.raises(ValidationError):
        GetCustomerArgs.model_validate({"customer_id": bad})


def test_missing_and_extra_fields_rejected():
    with pytest.raises(ValidationError):
        GetCustomerArgs.model_validate({})
    with pytest.raises(ValidationError):
        GetCustomerArgs.model_validate({"customer_id": "CUST-00042", "admin": True})


@pytest.mark.parametrize("amount", [10, 10.5, 0.01, 999.99])
def test_good_amounts(amount):
    args = RefundArgs.model_validate({"customer_id": "CUST-00042", "amount": amount, "reason": GOOD_REASON})
    assert args.amount == amount


@pytest.mark.parametrize("amount", [0, -1, -0.01, math.nan, math.inf, "10.00", True, 10.005, 5_000_000, None])
def test_bad_amounts(amount):
    with pytest.raises(ValidationError):
        RefundArgs.model_validate({"customer_id": "CUST-00042", "amount": amount, "reason": GOOD_REASON})


@pytest.mark.parametrize("reason", ["short", "         x", "\n\n\n\n\n\n\n\n\n\n\n", "x" * 501, 1234567890])
def test_bad_reasons(reason):
    with pytest.raises(ValidationError):
        RefundArgs.model_validate({"customer_id": "CUST-00042", "amount": 5, "reason": reason})


def test_reason_is_stripped():
    args = RefundArgs.model_validate({"customer_id": "CUST-00042", "amount": 5, "reason": "  " + GOOD_REASON + "  "})
    assert args.reason == GOOD_REASON


def test_error_data_names_fields_without_echoing_input():
    secret = "CUST-9999999-hunter2"
    with pytest.raises(ValidationError) as ei:
        RefundArgs.model_validate({"customer_id": secret, "amount": -3, "extra": 1})
    data = validation_error_data(ei.value)
    fields = {d["field"] for d in data}
    assert {"customer_id", "amount", "reason", "extra"} <= fields
    assert all(set(d) == {"field", "error"} for d in data)
    assert secret not in repr(data)
