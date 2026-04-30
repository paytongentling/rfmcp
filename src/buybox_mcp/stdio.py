from __future__ import annotations

import anyio

from buybox_mcp.config import get_settings
from buybox_mcp.runtime import ApplicationRuntime, set_runtime
from buybox_mcp.server import create_mcp_server


async def run_stdio() -> None:
    settings = get_settings()
    runtime = ApplicationRuntime(settings)
    mcp = create_mcp_server(settings)

    await runtime.start()
    set_runtime(runtime)
    try:
        await mcp.run_stdio_async()
    finally:
        set_runtime(None)
        await runtime.stop()


def main() -> None:
    anyio.run(run_stdio)


if __name__ == "__main__":
    main()
