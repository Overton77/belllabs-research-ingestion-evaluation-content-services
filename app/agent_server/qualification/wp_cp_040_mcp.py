from __future__ import annotations

from fastmcp import FastMCP

server = FastMCP(
    "belllabs-wp-cp-040",
    instructions="One deterministic read-only qualification surface.",
)


@server.tool(
    name="lookup_binding_marker",
    description="Return the exact BellLabs qualification marker for a binding code.",
)
def lookup_binding_marker(code: str) -> str:
    """Return a deterministic marker without network or mutable discovery."""

    return f"MCP-BOUND::{code}::EXACT"


def main() -> None:
    server.run(transport="stdio", show_banner=False)


if __name__ == "__main__":
    main()
