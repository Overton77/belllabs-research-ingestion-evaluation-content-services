from __future__ import annotations

import argparse
import json
from pathlib import Path

from app.domain.schema_context.canonicalization import write_json, write_text
from app.experiments.schema_context_selection.comparison import (
    compare_schema_context_runs,
    comparison_markdown,
)


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare schema-context workflow run artifacts")
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    return parser.parse_args()


def main() -> int:
    args = _arguments()
    comparison = compare_schema_context_runs(args.baseline, args.candidate)
    if args.output:
        args.output.mkdir(parents=True, exist_ok=True)
        write_json(args.output / "comparison.json", comparison)
        write_text(args.output / "comparison.md", comparison_markdown(comparison))
    print(json.dumps(comparison, indent=2, sort_keys=True))
    return 0 if comparison["accepted"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
