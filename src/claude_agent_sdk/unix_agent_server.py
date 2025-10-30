"""
UnixAgentServer: UDS interface for controlling a Claude client-backed agent.

Exposes a simple JSON-over-line-delimited protocol on a Unix Domain Socket
to send queries, get status, interrupt, stop, and aggregate assistant output.
"""

import asyncio
import json
import os
import signal
import sys
from collections.abc import AsyncIterator
from datetime import datetime
from pathlib import Path
from typing import Any

from .client import ClaudeSDKClient
from .types import (
    AssistantMessage,
    ClaudeAgentOptions,
    Message,
    ResultMessage,
    TextBlock,
    ToolResultBlock,
    UserMessage,
)


class ClaudeClientAgent:
    """Minimal adapter around ClaudeSDKClient to act like an Agent.

    Provides the methods used by UnixAgentServer without requiring a broader
    Agent class in this package.
    """

    def __init__(self, agent_id: str, options: ClaudeAgentOptions | None = None):
        self.agent_id = agent_id
        self.client = ClaudeSDKClient(options=options or ClaudeAgentOptions())

    async def start(self) -> None:
        await self.client.connect()

    async def query(self, prompt: str) -> None:
        await self.client.query(prompt)

    async def interrupt(self) -> None:
        await self.client.interrupt()

    async def receive(self) -> AsyncIterator[Message]:
        async for msg in self.client.receive_messages():
            yield msg

    async def close(self) -> None:
        await self.client.disconnect()


class UnixAgentServer:
    """Unix Domain Socket based server that exposes an agent over UDS."""

    @staticmethod
    def calc_socket_path(agent_id: str, base_dir: str = "/tmp") -> Path:
        return Path(base_dir) / f"{agent_id}.sock"

    def __init__(
        self,
        agent: ClaudeClientAgent,
        base_dir: str = "/tmp",
        parent_agent_uds_sock_path: str | None = None,
    ):
        self.agent = agent
        self.base_dir = Path(base_dir)
        self.socket_path = self.base_dir / f"{agent.agent_id}.sock"
        self.parent_agent_uds_sock_path = parent_agent_uds_sock_path

        self.server: asyncio.Server | None = None
        self.running = False
        self.started_at = datetime.now()
        self.monitor_task: asyncio.Task | None = None

        self.subscribed_clients: set[asyncio.StreamWriter] = set()

        # Ensure base dir exists and clean up existing socket
        self.base_dir.mkdir(parents=True, exist_ok=True)
        if self.socket_path.exists():
            self.socket_path.unlink()

    async def start(self, initial_query: str | None = None) -> None:
        self.running = True

        # Start UDS server
        self.server = await asyncio.start_unix_server(
            self._handle_connection, path=str(self.socket_path)
        )

        # Set socket permissions
        os.chmod(self.socket_path, 0o700)

        # Setup signal handlers for graceful shutdown
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, lambda: asyncio.create_task(self.stop()))
            except NotImplementedError:
                pass

        # Start Claude client
        await self.agent.start()

        # Output server_info as JSON to stdout for discovery
        server_info = self.get_server_info()
        sys.stdout.write(json.dumps(server_info) + "\n")
        sys.stdout.flush()

        # Print startup instructions to stderr
        _print_startup_instructions(server_info)

        # Start monitor loop
        self.monitor_task = asyncio.create_task(self._start_monitor(initial_query))
        await self._serve_forever()

    async def stop(self) -> None:
        self.running = False

        if self.server:
            self.server.close()
            await self.server.wait_closed()

        if self.monitor_task and not self.monitor_task.done():
            self.monitor_task.cancel()
            try:
                await self.monitor_task
            except asyncio.CancelledError:
                pass

        if self.socket_path.exists():
            self.socket_path.unlink()

        await self.agent.close()

    def get_server_info(self) -> dict[str, Any]:
        return {
            "type": "server_info",
            "agent_id": self.agent.agent_id,
            "pid": os.getpid(),
            "socket_path": str(self.socket_path),
            "log_file": None,
            "started_at": self.started_at.isoformat(),
        }

    async def _serve_forever(self) -> None:
        if self.server:
            async with self.server:
                await self.server.serve_forever()

    async def _handle_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            while True:
                line_bytes = await reader.readline()
                if not line_bytes:
                    break

                try:
                    line = line_bytes.decode("utf-8").strip()
                    if not line:
                        continue

                    request = json.loads(line)
                    await self._handle_command(request, writer)

                except json.JSONDecodeError as e:
                    await self._send_response(
                        writer,
                        {
                            "id": None,
                            "ok": False,
                            "error": "invalid_json",
                            "detail": str(e),
                        },
                    )

        except asyncio.CancelledError:
            pass
        except Exception:
            pass
        finally:
            self.subscribed_clients.discard(writer)
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    async def _start_monitor(self, query: str | None) -> None:
        try:
            # Send optional initial query
            if query is not None:
                await self.agent.query(query)

            async for message in self.agent.receive():
                await self._broadcast_log_to_clients(self._format_log(message))
                ## stdout the log
                print(json.dumps(self._format_log(message)),flush=True)

                if isinstance(message, ResultMessage):
                    # Notify parent if configured (best-effort)
                    if self.parent_agent_uds_sock_path:
                        await self._send_completion_to_parent(message)

        except Exception:
            print(
                "[UnixAgentServer] monitor loop error",
                file=sys.stderr,
                flush=True,
            )

    def _format_log(self, message: Message) -> dict[str, Any]:
        now = datetime.now().isoformat()
        # Extract human-readable content when available
        if isinstance(message, UserMessage):
            text_parts: list[str] = []
            if isinstance(message.content, list):
                for block in message.content:
                    if isinstance(block, TextBlock):
                        text_parts.append(block.text)
                    if isinstance(block, ToolResultBlock) and isinstance(
                        block.content, str
                    ):
                        text_parts.append(block.content)
            elif isinstance(message.content, str):
                text_parts.append(message.content)
            return {
                "timestamp": now,
                "role": "user",
                "text": "\n".join(text_parts),
            }

        if isinstance(message, AssistantMessage):
            text_parts = []
            for block in message.content:
                if isinstance(block, TextBlock):
                    text_parts.append(block.text)
            return {
                "timestamp": now,
                "role": "assistant",
                "text": "\n".join(text_parts),
            }

        if isinstance(message, ResultMessage):
            return {
                "timestamp": now,
                "role": "system",
                "type": "result",
                "cost": message.total_cost_usd,
                "usage": message.usage,
            }

        # Generic fallback
        return {"timestamp": now, "role": "system", "type": "event"}

    async def _send_completion_to_parent(self, result_message: ResultMessage) -> None:
        if not self.parent_agent_uds_sock_path:
            return
        try:
            reader, writer = await asyncio.open_unix_connection(
                self.parent_agent_uds_sock_path
            )
            completion_msg = {
                "type": "child_complete",
                "child_agent_id": self.agent.agent_id,
                "result": {
                    "result": result_message.result,
                    "usage": result_message.usage,
                    "timestamp": datetime.now().isoformat(),
                },
            }
            writer.write((json.dumps(completion_msg) + "\n").encode("utf-8"))
            await writer.drain()
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass

    async def _handle_command(
        self, request: dict[str, Any], writer: asyncio.StreamWriter
    ) -> None:
        request_id = request.get("id")
        command = request.get("cmd")
        try:
            if command == "query":
                prompt = request.get("prompt", "")
                await self.agent.query(prompt)
                await self._send_response(writer, {"id": request_id, "ok": True, "done": True})

            elif command == "status":
                server_info = self.get_server_info()
                response = {
                    "id": request_id,
                    "ok": True,
                    "result": {
                        **server_info,
                        "uptime_seconds": (datetime.now() - self.started_at).total_seconds(),
                    },
                }
                await self._send_response(writer, response)

            elif command == "stop":
                await self._send_response(
                    writer, {"id": request_id, "ok": True, "message": "Server stopping"}
                )
                asyncio.create_task(self.stop())

            elif command == "interrupt":
                await self.agent.interrupt()
                await self._send_response(writer, {"id": request_id, "ok": True, "done": True})

            elif command == "subscribe":
                self.subscribed_clients.add(writer)
                await self._send_response(
                    writer, {"id": request_id, "ok": True, "message": "Subscribed to log stream"}
                )

            elif command == "unsubscribe":
                self.subscribed_clients.discard(writer)
                await self._send_response(
                    writer,
                    {"id": request_id, "ok": True, "message": "Unsubscribed from log stream"},
                )

            else:
                await self._send_response(
                    writer,
                    {
                        "id": request_id,
                        "ok": False,
                        "error": "unknown_command",
                        "detail": f"Command '{command}' is not supported",
                    },
                )

        except Exception as e:
            await self._send_response(
                writer,
                {
                    "id": request_id,
                    "ok": False,
                    "error": "command_failed",
                    "detail": str(e),
                },
            )

    async def _send_response(self, writer: asyncio.StreamWriter, response: dict[str, Any]) -> None:
        try:
            writer.write((json.dumps(response) + "\n").encode("utf-8"))
            await writer.drain()
        except Exception:
            pass

    async def _broadcast_log_to_clients(self, log_dict: dict[str, Any]) -> None:
        clients = self.subscribed_clients.copy()
        disconnected: list[asyncio.StreamWriter] = []
        message = {"type": "log_stream", "data": log_dict}
        print(json.dumps(message),flush=True)
        for writer in clients:
            try:
                writer.write((json.dumps(message) + "\n").encode("utf-8"))
                await writer.drain()
            except Exception:
                disconnected.append(writer)
        for writer in disconnected:
            self.subscribed_clients.discard(writer)


def _print_startup_instructions(server_info: dict[str, Any]) -> None:
    pid = server_info["pid"]
    socket_path = server_info["socket_path"]
    info_message = f"""
+------------------------------------------------------------------+
|      UDS Agent Server Started and Ready for Action               |
+------------------------------------------------------------------+
| Agent ID:     {server_info["agent_id"]}
| Process ID:   {pid}
| Socket Path:  {socket_path}
+------------------------------------------------------------------+
| Useful Commands:                                                 |
|------------------------------------------------------------------|
| CHECK STATUS:                                                    |
|   echo '{{"id": "status-check", "cmd": "status"}}' | \
|     socat - UNIX-CONNECT:{socket_path}
|                                                                  |
| SEND A QUERY:                                                    |
|   echo '{{"id": "query-1", "cmd": "query", "prompt": "Hello!"}}' | \
|     socat - UNIX-CONNECT:{socket_path}
|                                                                  |
| SUBSCRIBE TO LOG STREAM:                                         |
|   echo '{{"id": "sub-1", "cmd": "subscribe"}}' | \
|     socat - UNIX-CONNECT:{socket_path}
|                                                                  |
| STOP SERVER (Graceful):                                          |
|   echo '{{"id": "stop-cmd", "cmd": "stop"}}' | \
|     socat - UNIX-CONNECT:{socket_path}
|                                                                  |
| STOP SERVER (Forceful):                                          |
|   kill {pid}                                                     |
+------------------------------------------------------------------+
    """
    print(info_message, file=sys.stderr, flush=True)


async def start_uds_io(
    agent: ClaudeClientAgent,
    base_dir: str = "/tmp",
    query: str | None = None,
) -> None:
    """Start a UDS-based agent server for the given agent."""
    server = UnixAgentServer(agent=agent, base_dir=base_dir)
    await server.start(initial_query=query)
