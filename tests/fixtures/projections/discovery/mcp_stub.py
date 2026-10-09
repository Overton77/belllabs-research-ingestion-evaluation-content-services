"""Minimal stdio MCP server for adapter discovery recordings (FIXTURE, not a product server).

Newline-delimited JSON-RPC 2.0 over stdin/stdout, standard library only, so a provider CLI can
launch it from a projected workspace without network access. It answers ``initialize`` and
``tools/list`` with one read-only tool and ignores notifications.
"""

from __future__ import annotations

import json
import sys

TOOL = {
    "name": "discovery_echo",
    "description": "Echo the input text (discovery fixture).",
    "inputSchema": {
        "type": "object",
        "properties": {"text": {"type": "string"}},
        "required": ["text"],
    },
}


def respond(message: dict[str, object]) -> dict[str, object] | None:
    method = message.get("method")
    if "id" not in message:
        return None
    result: dict[str, object]
    if method == "initialize":
        params = message.get("params")
        version = params.get("protocolVersion") if isinstance(params, dict) else None
        result = {
            "protocolVersion": version or "2025-06-18",
            "capabilities": {"tools": {}},
            "serverInfo": {"name": "mc-discovery-stub", "version": "1.0.0"},
        }
    elif method == "tools/list":
        result = {"tools": [TOOL]}
    elif method == "tools/call":
        params = message.get("params")
        arguments = params.get("arguments", {}) if isinstance(params, dict) else {}
        text = arguments.get("text", "") if isinstance(arguments, dict) else ""
        result = {"content": [{"type": "text", "text": str(text)}]}
    else:
        return {
            "jsonrpc": "2.0",
            "id": message["id"],
            "error": {"code": -32601, "message": f"method not found: {method}"},
        }
    return {"jsonrpc": "2.0", "id": message["id"], "result": result}


def main() -> None:
    for line in sys.stdin:
        if not line.strip():
            continue
        try:
            message = json.loads(line)
        except ValueError:
            continue
        reply = respond(message) if isinstance(message, dict) else None
        if reply is not None:
            sys.stdout.write(json.dumps(reply) + "\n")
            sys.stdout.flush()


if __name__ == "__main__":
    main()
