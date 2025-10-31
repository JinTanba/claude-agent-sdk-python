import asyncio
import json
from typing import Any

from . import create_sdk_mcp_server, tool


async def _send_uds_command(socket_path: str, payload: dict[str, Any]) -> dict[str, Any]:
    reader, writer = await asyncio.open_unix_connection(socket_path)
    try:
        writer.write((json.dumps(payload) + "\n").encode("utf-8"))
        await writer.drain()
        line = await reader.readline()
        if not line:
            return {"ok": False, "error": "no_response"}
        return json.loads(line.decode("utf-8").strip())
    finally:
        writer.close()
        await writer.wait_closed()


@tool("uds_status", "Get status of a UnixAgentServer", {"socket_path": str})
async def uds_status(args: dict[str, Any]) -> dict[str, Any]:
    res = await _send_uds_command(args["socket_path"], {"id": "mcp-status", "cmd": "status"})
    if not res.get("ok"):
        return {"content": [{"type": "text", "text": f"Error: {res.get('error')}: {res.get('detail')}"}], "is_error": True}
    result = res.get("result", {})
    text = (
        f"Agent: {result.get('agent_id')}\n"
        f"PID: {result.get('pid')}\n"
        f"Socket: {result.get('socket_path')}\n"
        f"Session: {result.get('session_id')}\n"
        f"Uptime(s): {int(result.get('uptime_seconds', 0))}"
    )
    return {"content": [{"type": "text", "text": text}]}


@tool("uds_query", "Send a query to a UnixAgentServer", {"socket_path": str, "prompt": str})
async def uds_query(args: dict[str, Any]) -> dict[str, Any]:
    res = await _send_uds_command(
        args["socket_path"], {"id": "mcp-query", "cmd": "query", "prompt": args["prompt"]}
    )
    if not res.get("ok"):
        return {"content": [{"type": "text", "text": f"Error: {res.get('error')}: {res.get('detail')}"}], "is_error": True}
    return {"content": [{"type": "text", "text": "Query accepted"}]}


@tool("uds_interrupt", "Interrupt a UnixAgentServer session", {"socket_path": str})
async def uds_interrupt(args: dict[str, Any]) -> dict[str, Any]:
    res = await _send_uds_command(args["socket_path"], {"id": "mcp-interrupt", "cmd": "interrupt"})
    if not res.get("ok"):
        return {"content": [{"type": "text", "text": f"Error: {res.get('error')}: {res.get('detail')}"}], "is_error": True}
    return {"content": [{"type": "text", "text": "Interrupted"}]}


@tool("uds_stop", "Stop a UnixAgentServer", {"socket_path": str})
async def uds_stop(args: dict[str, Any]) -> dict[str, Any]:
    res = await _send_uds_command(args["socket_path"], {"id": "mcp-stop", "cmd": "stop"})
    if not res.get("ok"):
        return {"content": [{"type": "text", "text": f"Error: {res.get('error')}: {res.get('detail')}"}], "is_error": True}
    return {"content": [{"type": "text", "text": "Stopping server"}]}


def create_unix_agent_mcp_server(name: str = "unix_agent", version: str = "1.0.0"):
    """Create an SDK MCP server exposing UnixAgentServer control tools."""
    return create_sdk_mcp_server(name=name, version=version, tools=[
        uds_status,
        uds_query,
        uds_interrupt,
        uds_stop,
    ])
