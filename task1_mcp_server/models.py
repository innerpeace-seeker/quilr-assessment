from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, ValidationError, field_validator

# [0-9], not \d: pydantic's regex engine treats \d as Unicode and lets fullwidth digits in
CustomerId = Annotated[str, StringConstraints(pattern=r"^CUST-[0-9]{5}$")]


class _Args(BaseModel):
    # strict: no "12.5" -> 12.5 or True -> 1 coercion, the model must send the right types
    model_config = ConfigDict(strict=True, extra="forbid")


class GetCustomerArgs(_Args):
    customer_id: CustomerId = Field(description="Customer id, format CUST-XXXXX (5 digits)")


class RefundArgs(_Args):
    customer_id: CustomerId = Field(description="Customer id, format CUST-XXXXX (5 digits)")
    amount: float = Field(gt=0, le=1_000_000, allow_inf_nan=False,
                          description="Refund amount, positive, at most 2 decimal places")
    reason: Annotated[str, StringConstraints(strip_whitespace=True, min_length=10, max_length=500)] = Field(
        description="Why the refund is issued (10-500 chars)")

    @field_validator("amount")
    @classmethod
    def _cents(cls, v: float) -> float:
        if round(v, 2) != v:
            raise ValueError("amount can have at most 2 decimal places")
        return v


def validation_error_data(exc: ValidationError) -> list[dict]:
    # include_input=False keeps whatever the caller sent out of the error we send back
    out = []
    for err in exc.errors(include_input=False, include_url=False, include_context=False):
        field = ".".join(str(p) for p in err["loc"]) or "arguments"
        out.append({"field": field, "error": err["msg"]})
    return out
