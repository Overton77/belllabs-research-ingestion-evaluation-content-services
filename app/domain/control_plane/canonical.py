from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel

CANONICAL_SCHEMA_VERSION = "canonical-json/1"


def _normalize(value: Any) -> Any:
    if isinstance(value, BaseModel):
        return {
            field_name: _normalize(getattr(value, field_name))
            for field_name in type(value).model_fields
        }
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("canonical datetimes must be timezone-aware")
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, dict):
        return {str(key): _normalize(item) for key, item in value.items()}
    if isinstance(value, frozenset | set):
        normalized = [_normalize(item) for item in value]
        return sorted(
            normalized,
            key=lambda item: json.dumps(
                item, sort_keys=True, separators=(",", ":"), ensure_ascii=False
            ),
        )
    if isinstance(value, tuple | list):
        return [_normalize(item) for item in value]
    return value


def canonical_data(value: Any) -> dict[str, Any]:
    return {
        "canonical_schema_version": CANONICAL_SCHEMA_VERSION,
        "payload": _normalize(value),
    }


def canonical_json(value: Any) -> bytes:
    return json.dumps(
        canonical_data(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode()


def sha256_digest(value: Any) -> str:
    return f"sha256:{hashlib.sha256(canonical_json(value)).hexdigest()}"


def verify_digest(value: Any, expected: str) -> None:
    actual = sha256_digest(value)
    if actual != expected:
        raise ValueError(f"digest mismatch: expected {expected}, got {actual}")
