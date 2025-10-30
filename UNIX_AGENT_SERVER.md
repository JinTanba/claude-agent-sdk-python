# UnixAgentServer: Multi-Agent Orchestration via Unix Domain Sockets

## Overview

This proposal presents an implementation of a new multi-agent architecture leveraging Unix Domain Sockets (UDS). Compared to existing sub-agent implementations, it offers the following advantages:

- **Process Isolation**: Each agent operates as an independent process
- **Enhanced Reusability**: Standardized JSON-over-UDS protocol
- **Extensibility**: Language and framework-agnostic communication method

## Architecture

### Core Principle: 1 Agent = 1 Process = 1 UnixAgentServer

Each agent launches as an independent process and communicates via Unix Domain Sockets. This provides:

1. **Independence**: Minimizes interference between agents
2. **Flexibility**: Multiple agents can launch with different configurations and environments
3. **Ease of Debugging**: Process-level monitoring and control

### Role of UnixAgentServer

UnixAgentServer abstracts agent implementations such as `ClaudeSDKClient` and exposes them as servers controllable via UDS.

```python
# Basic usage example
agent = ClaudeClientAgent(agent_id="my-agent", options=options)
server = UnixAgentServer(agent=agent, base_dir="/tmp")
await server.start(initial_query="Hello!")
```

### Communication Protocol

JSON over line-delimited protocol:

```json
# Send query
{"id": "req-1", "cmd": "query", "prompt": "Hello!"}

# Check status
{"id": "req-2", "cmd": "status"}

# Subscribe to log stream
{"id": "req-3", "cmd": "subscribe"}

# Interrupt agent
{"id": "req-4", "cmd": "interrupt"}

# Stop server
{"id": "req-5", "cmd": "stop"}
```

## Operating from Parent Agent

### Method 1: Direct Operation via Bash Tool

ClaudeCode connects to UDS using tools like `socat` through the `Bash` tool:

```bash
# Send query
echo '{"id": "1", "cmd": "query", "prompt": "Task"}' | socat - UNIX-CONNECT:/tmp/agent.sock

# Check status
echo '{"id": "2", "cmd": "status"}' | socat - UNIX-CONNECT:/tmp/agent.sock
```

### Method 2: UnixAgentMCP

By implementing as an MCP server, a higher-level tool interface can be provided.

## Monitoring Progress and Status

### Monitoring via Standard Output (Recommended)

UnixAgentServer outputs all events to standard output in JSON format:

```json
{"timestamp": "2025-10-31T03:13:56", "role": "assistant", "text": "Response content"}
{"timestamp": "2025-10-31T03:13:57", "role": "system", "type": "result", "cost": 0.015}
```

This output can be monitored with the `BashOutput` tool.

### Monitoring via UDS Client

Receive log stream in real-time with the `subscribe` command:

```bash
echo '{"id": "sub", "cmd": "subscribe"}' | socat - UNIX-CONNECT:/tmp/agent.sock
```

**Note**: This is a blocking operation, so it should be executed as a background process or with a timeout configured.