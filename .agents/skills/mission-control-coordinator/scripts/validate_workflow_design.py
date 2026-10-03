from __future__ import annotations

import json
import sys
from pathlib import Path

from jsonschema import Draft202012Validator


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: validate_workflow_design.py <design.json>", file=sys.stderr)
        return 2
    root = Path(__file__).resolve().parents[1]
    schema = json.loads(
        (root / "schemas" / "workflow-design-draft.schema.json").read_text(encoding="utf-8")
    )
    instance = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    errors = sorted(
        Draft202012Validator(schema).iter_errors(instance),
        key=lambda item: list(item.path),
    )
    if errors:
        for error in errors:
            path = ".".join(str(item) for item in error.path) or "$"
            print(f"{path}: {error.message}", file=sys.stderr)
        return 1
    print("valid workflow design")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
