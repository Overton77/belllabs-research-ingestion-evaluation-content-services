from __future__ import annotations

import argparse
import asyncio
import os
import re
from datetime import UTC, datetime
from pathlib import Path

from app.application.schema_catalog import DEFAULT_SEMANTIC_OVERLAY
from app.domain.schema_context.canonicalization import write_json
from app.experiments.schema_context_selection.reconciliation_workflow import (
    DEFAULT_MODEL,
    ReconciliationRunConfig,
    ReportGraphReconciliationWorkflow,
)

_SENSITIVE_LOCATION = re.compile(
    r"(?:neo4j(?:\+s)?|bolt|https?)://[^\s'\"]+|sk-[A-Za-z0-9_-]+", re.IGNORECASE
)


def _sanitized_error_details(error: Exception) -> dict[str, object]:
    details: dict[str, object] = {
        "error_type": type(error).__name__,
        "sanitized_message": _SENSITIVE_LOCATION.sub(
            "[redacted-location]", str(error)
        )[:4000],
        "credentials_suppressed": True,
    }
    context = getattr(error, "context", None)
    if isinstance(context, dict):
        details["error_context"] = {
            str(key): _SENSITIVE_LOCATION.sub("[redacted-location]", str(value))[:1000]
            for key, value in context.items()
        }
    cause = getattr(error, "cause", None)
    if isinstance(cause, BaseException):
        details["cause_type"] = type(cause).__name__
        details["sanitized_cause"] = _SENSITIVE_LOCATION.sub(
            "[redacted-location]", str(cause)
        )[:4000]
    return details


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run schema-context selection and reconciliation")
    parser.add_argument("--schema", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--structured-candidates", type=Path)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--run-id")
    parser.add_argument("--model", default=os.getenv("SCHEMA_SELECTION_MODEL", DEFAULT_MODEL))
    parser.add_argument("--build-only", action="store_true")
    parser.add_argument("--offline", action="store_true")
    parser.add_argument("--skip-vector", action="store_true")
    parser.add_argument("--max-query-intents", type=int, default=12)
    parser.add_argument("--database", default="neo4j")
    parser.add_argument(
        "--semantic-overlay",
        type=Path,
        default=DEFAULT_SEMANTIC_OVERLAY,
        help="Governed semantic metadata JSON (defaults to the official v1 overlay).",
    )
    return parser.parse_args()


async def _main() -> int:
    args = _arguments()
    run_id = args.run_id or (
        "trudiagnostic-products-" + datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    )
    config = ReconciliationRunConfig(
        schema_path=args.schema,
        report_path=args.report,
        structured_candidates_path=args.structured_candidates,
        output_root=args.output_root,
        run_id=run_id,
        model=args.model,
        build_only=args.build_only,
        offline=args.offline,
        skip_vector=args.skip_vector,
        max_query_intents=args.max_query_intents,
        database=args.database,
        semantic_overlay_path=args.semantic_overlay,
    )
    try:
        result = await ReportGraphReconciliationWorkflow().run(config)
    except Exception as error:
        run_root = (args.output_root / run_id).resolve()
        if run_root.exists():
            write_json(
                run_root / "failure.json",
                _sanitized_error_details(error),
            )
        print(f"schema context run failed: {type(error).__name__}")
        if run_root.exists():
            print(f"failure_artifact={run_root / 'failure.json'}")
        return 1
    print(f"status={result.status}")
    print(f"artifacts={result.artifact_root}")
    return 0 if result.status in {"build_only", "offline", "completed"} else 2


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_main()))
