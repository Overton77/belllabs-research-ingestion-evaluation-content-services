from __future__ import annotations

import argparse
import asyncio
import json
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from app.config import PROJECT_ROOT

DEFAULT_GOAL = (
    "Find the current Viome homepage title and one supported health-intelligence "
    "product claim. Return the final normalized URL, browser-observed page title, "
    "claim text, source URL, citation, and screenshot evidence."
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run the core Viome tracer through mounted Streamable HTTP MCP, coordinator "
            "preparation, PostgreSQL "
            "admission, three immutable OEBs, Temporal StageGraph worker, terminal "
            "Run Control, and durable typed-result path."
        )
    )
    parser.add_argument("--goal", default=DEFAULT_GOAL)
    parser.add_argument("--artifact-dir", required=True, type=Path)
    parser.add_argument(
        "--artifact-bucket",
        help=(
            "Private S3 bucket for authoritative browser screenshots. Falls back "
            "to the configured S3_BUCKET setting."
        ),
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Also save the secret-free coordinator result JSON inside the project.",
    )
    parser.add_argument("--tenant-scope", default="global")
    parser.add_argument("--request-scope", default="global")
    parser.add_argument("--actor-id", default="coordinator-live")
    parser.add_argument("--task-queue")
    parser.add_argument(
        "--plan-only",
        action="store_true",
        help=(
            "Run the real FastMCP bootstrap/search/discovery/validation path and "
            "stop before compilation, admission, or Temporal submission."
        ),
    )
    parser.add_argument(
        "--skip-external-discovery",
        action="store_true",
        help=(
            "Skip the quarantined MCP Registry and pinned npx skills discovery "
            "proof; internal catalog retrieval remains mandatory."
        ),
    )
    parser.add_argument("--maximum-results", type=int, default=5)
    parser.add_argument(
        "--browser-verification-limit",
        type=int,
        default=2,
        help=(
            "Maximum independently rendered pages; Gate A defaults to the primary "
            "Viome evidence and one independent secondary source."
        ),
    )
    return parser


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    return build_parser().parse_args(argv)


def _checked_artifact_root(path: Path) -> Path:
    resolved = path.resolve()
    if not resolved.is_relative_to(PROJECT_ROOT.resolve()):
        raise ValueError("--artifact-dir must stay inside the project directory")
    resolved.mkdir(parents=True, exist_ok=True)
    return resolved


def _checked_output_path(path: Path) -> Path:
    resolved = path.resolve()
    if not resolved.is_relative_to(PROJECT_ROOT.resolve()):
        raise ValueError("--output must stay inside the project directory")
    resolved.parent.mkdir(parents=True, exist_ok=True)
    return resolved


async def run(args: argparse.Namespace) -> dict[str, Any]:
    """Compose and execute the live coordinator chain.

    Imports remain inside this function so `--help` and unit tests never initialize
    MongoDB, PostgreSQL, Temporal, embedding providers, or browser runtimes.
    """

    from app.application.web_research_coordinator_live import run_live_coordinator

    return await run_live_coordinator(args, artifact_root=_checked_artifact_root(args.artifact_dir))


def main() -> None:
    args = parse_args()
    serialized = json.dumps(asyncio.run(run(args)), indent=2, sort_keys=True)
    if args.output is not None:
        _checked_output_path(args.output).write_text(
            serialized + "\n",
            encoding="utf-8",
        )
    print(serialized)


if __name__ == "__main__":
    main()
