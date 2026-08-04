from __future__ import annotations

import argparse
import asyncio
import json
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from app.application.schema_catalog import DEFAULT_SEMANTIC_OVERLAY
from app.config import PROJECT_ROOT

DEFAULT_SOURCE_RUN = (
    PROJECT_ROOT
    / ".scratch"
    / "schema-context-selection-runs"
    / "official-catalog-v1-live-20260723-3"
)
DEFAULT_SCHEMA = PROJECT_ROOT.parent / "biotech-kg" / "src" / "schema" / (
    "neo4jbiotechschema.graphql"
)
DEFAULT_REPORT = (
    PROJECT_ROOT.parent
    / "biotech-kg"
    / "research"
    / "trudiagnostic-20260330-203619-research-mission"
    / "reports"
    / "products-labtests-biomarkers.md"
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run coordinator Scenarios A and C through exact catalog retrieval, "
            "control-plane compilation, application-PostgreSQL admission, Mongo OEBs, "
            "real dual-family Temporal workers, governed terminalization, and typed "
            "PostgreSQL result persistence."
        )
    )
    parser.add_argument("--artifact-dir", required=True, type=Path)
    parser.add_argument(
        "--artifact-bucket",
        required=True,
        help="Private versioned S3 bucket for authoritative A/C payloads.",
    )
    parser.add_argument("--schema", type=Path, default=DEFAULT_SCHEMA)
    parser.add_argument("--semantic-overlay", type=Path, default=DEFAULT_SEMANTIC_OVERLAY)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument(
        "--projection",
        type=Path,
        default=DEFAULT_SOURCE_RUN / "selection" / "operation-projection.json",
    )
    parser.add_argument(
        "--intent",
        action="append",
        type=Path,
        default=None,
        help=(
            "Projection-bound QueryExecutionIntent JSON. Repeat for multiple intents; "
            "defaults to the five contract-valid intents in the reviewed live run."
        ),
    )
    parser.add_argument("--tenant-scope", default="global")
    parser.add_argument("--request-scope", default="global")
    parser.add_argument("--actor-id", default="coordinator-schema-grounding-live")
    parser.add_argument("--task-queue")
    parser.add_argument("--model", default="gpt-5-mini")
    parser.add_argument("--database", default="neo4j")
    parser.add_argument(
        "--deployment-id",
        required=True,
        help="Exact operator-provisioned governed deployment-evidence event identity.",
    )
    return parser


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    args = build_parser().parse_args(argv)
    if args.intent is None:
        args.intent = tuple(
            DEFAULT_SOURCE_RUN / "queries" / f"{index:03d}-intent.json"
            for index in range(1, 6)
        )
    else:
        args.intent = tuple(args.intent)
    return args


def _checked_artifact_root(path: Path) -> Path:
    resolved = path.resolve()
    if not resolved.is_relative_to(PROJECT_ROOT.resolve()):
        raise ValueError("--artifact-dir must stay inside the project directory")
    resolved.mkdir(parents=True, exist_ok=True)
    return resolved


async def run(args: argparse.Namespace) -> dict[str, Any]:
    from app.application.schema_grounding_coordinator_live import (
        run_live_schema_grounding_coordinator,
    )

    return await run_live_schema_grounding_coordinator(
        args,
        artifact_root=_checked_artifact_root(args.artifact_dir),
    )


def main() -> None:
    print(json.dumps(asyncio.run(run(parse_args())), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
