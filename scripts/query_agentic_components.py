from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
from typing import Any

from mission_control.adapters.capabilities.agentic_components.filesystem_repository import (
    FilesystemAgenticComponentRepository,
)
from mission_control.application.agentic_components.materialization import MaterializationPlanner
from mission_control.domain.agentic_components.contracts import (
    AgentHost,
    AgenticComponentRelease,
    Architecture,
    ComponentKind,
    ComponentQuery,
    MaterializationPlan,
    MaterializationRequest,
    OperatingSystem,
    TrustStage,
)


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(
        description="Query exact BellLabs Agentic Component releases and compile dry-run plans."
    )
    commands = root.add_subparsers(dest="command", required=True)

    schema = commands.add_parser("schema", help="Print a JSON Schema for a harness contract.")
    schema.add_argument("contract", choices=("release", "query", "request", "plan"))

    get = commands.add_parser("get", help="Retrieve one exact release by digest.")
    get.add_argument("--catalog-root", type=Path, required=True)
    get.add_argument("--digest", required=True)

    search = commands.add_parser("search", help="Search qualified component releases.")
    search.add_argument("--catalog-root", type=Path, required=True)
    search.add_argument("--text")
    search.add_argument("--kind", action="append", choices=[item.value for item in ComponentKind])
    search.add_argument("--host", choices=[item.value for item in AgentHost])
    search.add_argument("--os", choices=[item.value for item in OperatingSystem])
    search.add_argument("--architecture", choices=[item.value for item in Architecture])
    search.add_argument("--capability", action="append")
    search.add_argument(
        "--minimum-trust-stage",
        choices=[item.value for item in TrustStage],
        default=TrustStage.REVIEWED.value,
    )
    search.add_argument("--limit", type=int, default=20)

    plan = commands.add_parser("plan", help="Compile a materialization request JSON file.")
    plan.add_argument("--catalog-root", type=Path, required=True)
    plan.add_argument("--request", type=Path, required=True)
    return root


async def run(arguments: argparse.Namespace) -> object:
    if arguments.command == "schema":
        contracts = {
            "release": AgenticComponentRelease,
            "query": ComponentQuery,
            "request": MaterializationRequest,
            "plan": MaterializationPlan,
        }
        return contracts[arguments.contract].model_json_schema()

    repository = FilesystemAgenticComponentRepository(arguments.catalog_root)
    if arguments.command == "get":
        release = await repository.get_by_digest(arguments.digest)
        if release is None:
            raise SystemExit(f"component release not found: {arguments.digest}")
        return release.model_dump(mode="json")
    if arguments.command == "search":
        query = ComponentQuery(
            text=arguments.text,
            kinds=frozenset(ComponentKind(item) for item in arguments.kind or ()),
            host=AgentHost(arguments.host) if arguments.host else None,
            operating_system=OperatingSystem(arguments.os) if arguments.os else None,
            architecture=(Architecture(arguments.architecture) if arguments.architecture else None),
            required_capabilities=frozenset(arguments.capability or ()),
            minimum_trust_stage=TrustStage(arguments.minimum_trust_stage),
            limit=arguments.limit,
        )
        results = await repository.search(query)
        return [item.model_dump(mode="json") for item in results]
    if arguments.command == "plan":
        request = MaterializationRequest.model_validate_json(arguments.request.read_bytes())
        result = await MaterializationPlanner(repository).plan(request)
        return result.model_dump(mode="json")
    raise AssertionError("unreachable")


def main() -> None:
    arguments = parser().parse_args()
    result: Any = asyncio.run(run(arguments))
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
