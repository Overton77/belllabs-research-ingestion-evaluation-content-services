"""JSON Schema export for ``mc.context_packet.v1`` and ``mc.context_selection.v1``.

The committed files under ``contracts/schemas/`` are generated from the Pydantic models in
:mod:`mission_control.domain.context`; ``tests/unit/context`` fails when they drift.
Regenerate with ``uv run python -m mission_control.contracts.context_packet``.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

from mission_control.domain.context.packet import PACKET_SCHEMA_VERSION, ContextPacket
from mission_control.domain.context.render import (
    SELECTION_SCHEMA_VERSION,
    ContextSelectionRecord,
)

SCHEMA_DIRECTORY = Path(__file__).resolve().parent / "schemas"


def context_packet_json_schema() -> dict[str, Any]:
    schema = ContextPacket.model_json_schema()
    schema["$id"] = PACKET_SCHEMA_VERSION
    return schema


def context_selection_json_schema() -> dict[str, Any]:
    schema = ContextSelectionRecord.model_json_schema()
    schema["$id"] = SELECTION_SCHEMA_VERSION
    return schema


def context_contract_schemas() -> dict[str, dict[str, Any]]:
    return {
        PACKET_SCHEMA_VERSION: context_packet_json_schema(),
        SELECTION_SCHEMA_VERSION: context_selection_json_schema(),
    }


def schema_file_text(schema: dict[str, Any]) -> str:
    return json.dumps(schema, indent=2, sort_keys=True) + "\n"


def write_schema_files(directory: Path = SCHEMA_DIRECTORY) -> list[Path]:
    directory.mkdir(parents=True, exist_ok=True)
    written = []
    for name, schema in context_contract_schemas().items():
        path = directory / f"{name}.schema.json"
        path.write_text(schema_file_text(schema), encoding="utf-8", newline="\n")
        written.append(path)
    return written


if __name__ == "__main__":  # pragma: no cover - maintenance entry point
    for written_path in write_schema_files():
        sys.stdout.write(f"{written_path}\n")
