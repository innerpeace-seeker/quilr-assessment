import asyncio
import logging
from dataclasses import dataclass

import httpx

from .errors import GatewayError

log = logging.getLogger("task4.router")

DEFAULT_TIMEOUT_S = 3.0


@dataclass
class Provider:
    name: str
    base_url: str
    api_key: str
    model: str
    client: httpx.AsyncClient

    @property
    def url(self):
        return self.base_url.rstrip("/") + "/v1/chat/completions"


class _Failover(Exception):
    """Primary didn't work out in a way the secondary might fix."""

    def __init__(self, reason):
        super().__init__(reason)
        self.reason = reason


class ModelRouter:
    def __init__(self, primary: Provider, secondary: Provider,
                 timeout_s: float = DEFAULT_TIMEOUT_S, secondary_timeout_s: float = 10.0):
        self.chain = [(primary, timeout_s), (secondary, secondary_timeout_s)]

    async def complete(self, payload: dict) -> tuple[str, dict]:
        reasons = []
        for provider, timeout in self.chain:
            try:
                return provider.name, await self._call(provider, payload, timeout)
            except _Failover as f:
                log.warning("provider %s failed (%s), trying next", provider.name, f.reason)
                reasons.append(f.reason)

        if all(r == "timeout" for r in reasons):
            raise GatewayError(504, "upstream_error", "upstream_timeout",
                               "No model provider answered in time")
        raise GatewayError(502, "upstream_error", "upstream_unavailable",
                           "No model provider is available right now, try again shortly")

    async def _call(self, p: Provider, payload: dict, timeout: float) -> dict:
        body = {**payload, "model": p.model}
        try:
            # One deadline over the whole exchange: connect, headers and the full body.
            # httpx's own timeouts are per socket operation, so a provider that drips
            # a byte every second would never trip them.
            async with asyncio.timeout(timeout):
                resp = await p.client.post(p.url, json=body, headers={"Authorization": f"Bearer {p.api_key}"})
        except TimeoutError:
            raise _Failover("timeout") from None
        except httpx.HTTPError as e:
            log.info("transport error talking to %s: %r", p.name, e)
            raise _Failover("connect_error") from None

        if resp.status_code == 429:
            raise _Failover("rate_limited")
        if resp.status_code >= 500:
            log.info("%s returned %s: %.300r", p.name, resp.status_code, resp.text)
            raise _Failover(f"http_{resp.status_code}")
        if resp.status_code >= 400:
            # our request is the problem, the other provider will say the same thing
            log.info("%s rejected request with %s: %.300r", p.name, resp.status_code, resp.text)
            raise GatewayError(400, "invalid_request_error", "upstream_rejected",
                               "The model provider rejected the request")
        try:
            data = resp.json()
            if not isinstance(data, dict) or "choices" not in data:
                raise ValueError("no choices")
        except ValueError:
            log.info("%s sent an unusable body (%s)", p.name, resp.headers.get("content-type"))
            raise _Failover("bad_response") from None
        return data
