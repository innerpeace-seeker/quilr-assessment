import threading
import uuid
from datetime import datetime, timezone

# fake backing store, good enough for the exercise
CUSTOMERS: dict[str, dict] = {
    "CUST-00042": {"customer_id": "CUST-00042", "name": "Ada Lovelace", "email": "ada@example.com",
                   "tier": "gold", "balance": 1250.00},
    "CUST-10001": {"customer_id": "CUST-10001", "name": "Grace Hopper", "email": "grace@example.com",
                   "tier": "silver", "balance": 80.50},
    "CUST-31337": {"customer_id": "CUST-31337", "name": "Alan Turing", "email": "alan@example.com",
                   "tier": "bronze", "balance": 0.0},
}

REFUNDS: list[dict] = []
_lock = threading.Lock()


class RefundRejected(Exception):
    pass


def get_customer(customer_id: str) -> dict | None:
    c = CUSTOMERS.get(customer_id)
    return dict(c) if c else None


def issue_refund(customer_id: str, amount: float, reason: str) -> dict:
    with _lock:
        c = CUSTOMERS.get(customer_id)
        if c is None:
            raise RefundRejected(f"customer {customer_id} not found")
        if amount > c["balance"]:
            raise RefundRejected(f"amount {amount:.2f} exceeds refundable balance {c['balance']:.2f}")
        c["balance"] = round(c["balance"] - amount, 2)
        refund = {
            "refund_id": "RF-" + uuid.uuid4().hex[:10],
            "customer_id": customer_id,
            "amount": amount,
            "reason": reason,
            "status": "processed",
            "created_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "remaining_balance": c["balance"],
        }
        REFUNDS.append(refund)
        return refund
