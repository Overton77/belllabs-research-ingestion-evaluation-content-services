"""Validate agent-capability seed bundles: pins parse and tool digests match their fixtures.

``make seeds-validate`` runs this. For every published MCP Server definition in the seed
bundles it checks that the Capability Pin ``<id>@<upstream version>#sha256:<digest>`` parses,
that ``tools_list_digest`` and ``schema_digest`` equal the digest of the tool list in
``tests/fixtures/mcp/<id>.tools.json``, that the snapshot ref pins the fixture bytes, that each
MCP Tool child points at its server's exact digest, and that no secret value appears.
Exit 0 when everything holds, 1 with a report otherwise.
"""

from __future__ import annotations

import hashlib
import json
import re
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from mission_control.domain.authoring.canonical import sha256_digest
from mission_control.domain.authoring.contracts import (
    DefinitionKind,
    MCPServerDefinition,
    MCPToolDefinition,
    PublishedDefinition,
)
from mission_control.domain.capabilities.pins import CapabilityPin, CapabilityPinError

ROOT = Path(__file__).resolve().parents[1]
SEEDS = ROOT / "packages" / "mission-control-db-contract" / "seeds"
FIXTURES = ROOT / "tests" / "fixtures" / "mcp"
SECRET_VALUE = re.compile(r"(tvly-|fc-[0-9a-f]{8}|sk-[A-Za-z0-9]{8}|Bearer [A-Za-z0-9])")


def _digest(payload: bytes) -> str:
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def _tools_digest(tools: list[dict[str, Any]]) -> str:
    encoded = json.dumps(tools, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return _digest(encoded.encode("utf-8"))


def published_definitions() -> Iterator[tuple[str, PublishedDefinition]]:
    for path in sorted(SEEDS.rglob("*.json")):
        bundle = json.loads(path.read_bytes())
        for record in bundle.get("records", []):
            manifest = record.get("fields", {}).get("manifest")
            if record.get("kind") != "asset_version" or not isinstance(manifest, dict):
                continue
            if "definition" not in manifest or "ref" not in manifest:
                continue
            yield path.relative_to(SEEDS).as_posix(), PublishedDefinition.model_validate(manifest)


def validate() -> list[str]:
    problems: list[str] = []
    servers: dict[str, MCPServerDefinition] = {}
    tools: list[tuple[str, MCPToolDefinition]] = []
    for source, published in published_definitions():
        definition = published.definition
        if published.ref.digest != sha256_digest(definition):
            problems.append(f"{source}: {published.ref.logical_id} digest is not exact")
        if isinstance(definition, MCPServerDefinition):
            servers[definition.logical_id] = definition
            problems += _check_server(source, published, definition)
        elif isinstance(definition, MCPToolDefinition):
            tools.append((source, definition))
    for source, tool in tools:
        server = servers.get(tool.server_ref.logical_id)
        if server is None:
            problems.append(f"{source}: {tool.logical_id} has no seeded server")
        elif tool.server_ref.digest != sha256_digest(server):
            problems.append(f"{source}: {tool.logical_id} points at a stale server digest")
        elif tool.tool_name not in {item.name for item in server.exposed_tools()}:
            problems.append(f"{source}: {tool.logical_id} is not an exposed server tool")
    for path in sorted(SEEDS.rglob("*.json")):
        if SECRET_VALUE.search(path.read_text(encoding="utf-8")):
            problems.append(f"{path.relative_to(SEEDS).as_posix()}: looks like a secret value")
    return problems


def _check_server(
    source: str, published: PublishedDefinition, server: MCPServerDefinition
) -> list[str]:
    problems: list[str] = []
    name = server.logical_id
    fixture_path = FIXTURES / f"{name}.tools.json"
    if server.package_pin is None:
        return problems  # pre-0025 server definitions carry no package pin
    if not fixture_path.is_file():
        return [f"{source}: {name} has no tools fixture {fixture_path.name}"]
    payload = fixture_path.read_bytes().replace(b"\r\n", b"\n")
    document = json.loads(payload)
    expected = _tools_digest(document["tools"])
    if server.tools_list_digest != expected or server.schema_digest != expected:
        problems.append(f"{source}: {name} tools_list_digest does not match its fixture")
    if server.schema_snapshot_ref.digest != _digest(payload):
        problems.append(f"{source}: {name} snapshot ref does not pin the fixture bytes")
    if document.get("package_pin") != server.package_pin:
        problems.append(f"{source}: {name} package pin differs from its fixture")
    version = server.source_provenance.upstream_version
    try:
        pin = CapabilityPin.parse(f"{name}@{version}#{published.ref.digest}")
    except CapabilityPinError as error:
        problems.append(f"{source}: {name} pin does not parse: {error}")
    else:
        if pin.render() != f"{name}@{version}#{published.ref.digest}":
            problems.append(f"{source}: {name} pin does not round-trip")
    if published.ref.kind is not DefinitionKind.MCP_SERVER:
        problems.append(f"{source}: {name} is not published as an MCP server")
    return problems


def main() -> int:
    problems = validate()
    for problem in problems:
        print(problem, file=sys.stderr)
    if problems:
        return 1
    count = sum(1 for _ in published_definitions())
    print(f"seeds-validate: {count} published definitions checked; pins and tool digests hold")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
