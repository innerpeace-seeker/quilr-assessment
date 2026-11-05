import time
from dataclasses import dataclass

import jwt

ROLES = {"admin", "viewer"}


class AuthError(Exception):
    pass


@dataclass(frozen=True)
class Identity:
    sub: str
    role: str


def make_token(sub: str, role: str, secret: str, ttl: int = 3600) -> str:
    now = int(time.time())
    return jwt.encode({"sub": sub, "role": role, "iat": now, "exp": now + ttl}, secret, algorithm="HS256")


def role_from_header(value: str | None, secret: str) -> Identity:
    if not value:
        raise AuthError("missing Authorization header")
    scheme, _, token = value.partition(" ")
    token = token.strip()
    if scheme.lower() != "bearer" or not token:
        raise AuthError("expected a Bearer token")
    try:
        # algorithms pinned so "alg": "none" or an RS/HS confusion can't get through
        claims = jwt.decode(token, secret, algorithms=["HS256"], options={"require": ["exp", "sub"]})
    except jwt.PyJWTError as e:
        raise AuthError(f"invalid token: {e.__class__.__name__}") from None
    role = claims.get("role")
    if role not in ROLES:
        raise AuthError("token has no usable role")
    return Identity(sub=str(claims["sub"]), role=role)
