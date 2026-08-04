from __future__ import annotations

import argparse
import asyncio
import json
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from app.config import PROJECT_ROOT


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run Scenario B: prove an internal catalog gap, query the official MCP "
            "Registry and pinned npx skills discovery, persist candidates, reject "
            "direct execution, and produce an immutable quarantine inspection request."
        )
    )
    parser.add_argument("--tenant-scope", default="global")
    parser.add_argument(
        "--artifact",
        type=Path,
        default=Path(".artifacts/scenario-b/live-result.json"),
    )
    parser.add_argument(
        "--workspace-root",
        type=Path,
        default=Path(".scratch/scenario-b-quarantine"),
    )
    parser.add_argument("--external-limit", type=int, default=5)
    return parser


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    return build_parser().parse_args(argv)


def _project_path(path: Path, *, argument: str) -> Path:
    resolved = (PROJECT_ROOT / path).resolve() if not path.is_absolute() else path.resolve()
    if not resolved.is_relative_to(PROJECT_ROOT.resolve()):
        raise ValueError(f"{argument} must stay inside the project directory")
    return resolved


async def run(args: argparse.Namespace) -> dict[str, Any]:
    from app.application.scenario_b_live import run_scenario_b_live

    return await run_scenario_b_live(
        tenant_scope=args.tenant_scope,
        artifact_path=_project_path(args.artifact, argument="--artifact"),
        workspace_root=_project_path(
            args.workspace_root,
            argument="--workspace-root",
        ),
        external_limit=args.external_limit,
    )


def main() -> None:
    print(json.dumps(asyncio.run(run(parse_args())), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
