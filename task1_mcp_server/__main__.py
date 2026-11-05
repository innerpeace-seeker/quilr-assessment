from .transport import claim_stdout

# before anything else gets a chance to write to stdout
claim_stdout()

import logging  # noqa: E402
import os  # noqa: E402
import sys  # noqa: E402

import anyio  # noqa: E402

from .server import build_server  # noqa: E402
from .transport import strict_stdio  # noqa: E402

logging.basicConfig(
    stream=sys.stderr,
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)


async def main():
    server = build_server()
    async with strict_stdio() as (read, write):
        await server.run(read, write, server.create_initialization_options())


if __name__ == "__main__":
    try:
        anyio.run(main)
    except KeyboardInterrupt:
        pass
