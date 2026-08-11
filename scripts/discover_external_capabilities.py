from __future__ import annotations

import argparse
import asyncio
import json
import os
from pathlib import Path
from typing import Any

from app.application.web_research.external_candidate_repository import (
    BeanieExternalCandidateRepository,
)
from app.application.web_research.external_capability_discovery import (
    ExternalCapabilityDiscoveryService,
)
from app.config import Settings
from app.integrations.mcp_registry import HttpxMCPRegistryRunner, MCPRegistryAdapter
from app.integrations.mongodb import create_mongodb
from app.integrations.npx_skills_discovery import (
    AsyncioSkillDiscoverySubprocessRunner,
    NpxSkillsDiscoveryAdapter,
)


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Discover and quarantine external MCP or Agent Skill candidates."
    )
    parser.add_argument("source", choices=("mcp", "skills"))
    parser.add_argument("--query", required=True)
    parser.add_argument("--limit", type=int, default=10)
    parser.add_argument("--owner")
    parser.add_argument("--npx-executable")
    parser.add_argument("--node-bin")
    args = parser.parse_args()
    if args.node_bin:
        node_bin = Path(args.node_bin).resolve(strict=True)
        if not (node_bin / "node.exe").is_file():
            raise ValueError("--node-bin must contain node.exe")
        args.node_bin = node_bin
    return args


async def _run(args: argparse.Namespace) -> dict[str, Any]:
    settings = Settings()
    if args.node_bin:
        node_bin: Path = args.node_bin
        os.environ["PATH"] = (
            f"{node_bin}{os.pathsep}{os.environ.get('PATH', '')}"
        )
    mongo_client, _ = await create_mongodb(settings)
    try:
        service = ExternalCapabilityDiscoveryService(
            enabled=True,
            mcp_registry=MCPRegistryAdapter(
                HttpxMCPRegistryRunner(),
                base_url=settings.mcp_registry_base_url,
                api_version=settings.mcp_registry_api_version,
                timeout_seconds=settings.external_discovery_request_timeout_seconds,
                max_response_bytes=settings.external_discovery_max_output_bytes,
                max_pages=settings.external_discovery_max_pages,
                max_retries=settings.external_discovery_max_retries,
            ),
            skills=NpxSkillsDiscoveryAdapter(
                AsyncioSkillDiscoverySubprocessRunner(),
                executable=args.npx_executable or settings.npx_skills_executable,
                package_version=settings.npx_skills_package_version,
                timeout_seconds=settings.external_discovery_command_timeout_seconds,
                max_output_bytes=settings.external_discovery_max_output_bytes,
            ),
            candidates=BeanieExternalCandidateRepository(),
            max_results=settings.external_discovery_max_results,
        )
        if args.source == "mcp":
            batch = await service.discover_mcp_servers(
                args.query,
                limit=args.limit,
            )
        else:
            batch = await service.discover_agent_skills(
                args.query,
                limit=args.limit,
                owner=args.owner,
            )
        return batch.model_dump(mode="json")
    finally:
        await mongo_client.close()


def main() -> None:
    print(json.dumps(asyncio.run(_run(_arguments())), sort_keys=True))


if __name__ == "__main__":
    main()
