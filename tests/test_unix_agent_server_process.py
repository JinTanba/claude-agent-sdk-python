"""
@unix_agent_server.py
Use Bash tools to execute this file and communicate with the Agent to start it.
"""

import asyncio
from datetime import datetime

from claude_agent_sdk.unix_agent_server import (
    ClaudeClientAgent,
    start_uds_io,
)


async def main() -> None:
    agent_id = datetime.now().strftime("%Y%m%d%H%M%S")+"test"
    agent = ClaudeClientAgent(agent_id)
    await start_uds_io(agent)


if __name__ == "__main__":
    asyncio.run(main())
