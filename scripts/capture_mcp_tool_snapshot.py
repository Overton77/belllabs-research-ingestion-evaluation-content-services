from __future__ import annotations

import argparse
import asyncio
import json
import os
from pathlib import Path
from typing import Any

from fastmcp import Client
from fastmcp.client.transports import StdioTransport

from app.config import Settings


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Capture a canonical tools/list snapshot from an inspected stdio MCP server."
    )
    parser.add_argument("--command", required=True)
    parser.add_argument("--arg", action="append", default=[])
    parser.add_argument("--cwd", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument(
        "--credential-setting",
        choices=("firecrawl_api_key", "tavily_api_key"),
        help="Inject one typed Settings credential into the child without printing it.",
    )
    args = parser.parse_args()
    args.cwd = Path(args.cwd).resolve(strict=True)
    args.output = Path(args.output).resolve()
    return args


async def _capture(args: argparse.Namespace) -> dict[str, Any]:
    cwd: Path = args.cwd
    output: Path = args.output
    environment = {
        key: os.environ[key]
        for key in ("PATH", "PATHEXT", "SYSTEMROOT", "COMSPEC")
        if key in os.environ
    }
    environment.update({"NO_COLOR": "1", "CI": "1"})
    if args.credential_setting:
        credential = getattr(Settings(), args.credential_setting)
        if credential is None:
            raise RuntimeError(f"{args.credential_setting} is unavailable")
        environment[
            "FIRECRAWL_API_KEY"
            if args.credential_setting == "firecrawl_api_key"
            else "TAVILY_API_KEY"
        ] = credential.get_secret_value()
    transport = StdioTransport(
        command=args.command,
        args=list(args.arg),
        env=environment,
        cwd=str(cwd),
    )
    async with Client(transport, timeout=30) as client:
        tools = await client.list_tools()
    snapshot = {
        "snapshot_format": "mcp-tools-list/1",
        "tools": [
            tool.model_dump(mode="json", exclude_none=True)
            for tool in sorted(tools, key=lambda item: item.name)
        ],
    }
    await asyncio.to_thread(_write_snapshot, output, snapshot)
    return {
        "output": str(output),
        "tool_count": len(tools),
        "tool_names": [tool.name for tool in sorted(tools, key=lambda item: item.name)],
    }


def _write_snapshot(output: Path, snapshot: dict[str, Any]) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(snapshot, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )


def main() -> None:
    print(json.dumps(asyncio.run(_capture(_arguments())), sort_keys=True))


if __name__ == "__main__":
    main()
