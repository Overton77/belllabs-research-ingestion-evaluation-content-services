from __future__ import annotations

import argparse
import asyncio
import json
from collections.abc import Sequence
from pathlib import Path

from app.application.schema_catalog import DEFAULT_SEMANTIC_OVERLAY
from scripts.run_schema_grounding_coordinator_live import (
    DEFAULT_REPORT,
    DEFAULT_SCHEMA,
    DEFAULT_SOURCE_RUN,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Read-only A/C launch preflight. Exercises exact catalog retrieval and "
            "rehydration plus PostgreSQL, MongoDB, Neo4j, S3, and Temporal reads. "
            "It never stages objects, issues graph authority, or admits runs."
        )
    )
    parser.add_argument("--artifact-bucket", required=True)
    parser.add_argument("--schema", type=Path, default=DEFAULT_SCHEMA)
    parser.add_argument(
        "--semantic-overlay",
        type=Path,
        default=DEFAULT_SEMANTIC_OVERLAY,
    )
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
    )
    parser.add_argument("--tenant-scope", default="global")
    parser.add_argument("--database", default="neo4j")
    parser.add_argument(
        "--deployment-id",
        help=(
            "Optional exact governed deployment-evidence identity to verify. "
            "Omitting it leaves launch_ready false without creating evidence."
        ),
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


async def run(args: argparse.Namespace) -> dict[str, object]:
    from app.application.schema_grounding_live_preflight import (
        preflight_live_schema_grounding,
    )

    return await preflight_live_schema_grounding(args)


def main() -> None:
    result = asyncio.run(run(parse_args()))
    print(json.dumps(result, indent=2, sort_keys=True))
    raise SystemExit(0 if result["ok"] else 1)


if __name__ == "__main__":
    main()
