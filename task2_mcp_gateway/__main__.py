"""Mint a test token: uv run python -m task2_mcp_gateway admin [subject]"""
import os
import sys

from .auth import ROLES, make_token

if len(sys.argv) < 2 or sys.argv[1] not in ROLES:
    sys.exit(f"usage: python -m task2_mcp_gateway {{{'|'.join(sorted(ROLES))}}} [subject]")
secret = os.environ.get("GATEWAY_SECRET")
if not secret:
    sys.exit("GATEWAY_SECRET is not set")
print(make_token(sys.argv[2] if len(sys.argv) > 2 else "dev", sys.argv[1], secret))
