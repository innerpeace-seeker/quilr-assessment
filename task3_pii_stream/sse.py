from collections.abc import AsyncIterator


async def iter_events(lines: AsyncIterator[str]) -> AsyncIterator[str]:
    """Yield the data payload of each server-sent event.

    Only `data:` matters for chat completion streams; event/id/retry fields and
    comment lines are skipped.
    """
    data: list[str] = []
    async for line in lines:
        line = line.rstrip("\r")
        if not line:
            if data:
                yield "\n".join(data)
                data = []
            continue
        if line.startswith(":"):
            continue
        field, _, value = line.partition(":")
        if field == "data":
            data.append(value[1:] if value.startswith(" ") else value)
    if data:
        yield "\n".join(data)
