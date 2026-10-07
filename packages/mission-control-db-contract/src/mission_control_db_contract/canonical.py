"""Deterministic JSON encoding and digests shared by every artifact."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from .errors import ContractError


def canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str
    ).encode("utf-8")


def sha256_hex(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def digest_value(value: Any) -> str:
    return "sha256:" + sha256_hex(canonical_bytes(value))


def pretty_bytes(value: Any) -> bytes:
    """Stable human-readable JSON (sorted keys, LF, trailing newline)."""
    return (json.dumps(value, sort_keys=True, indent=2, ensure_ascii=False) + "\n").encode("utf-8")


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(pretty_bytes(value))


def read_json_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ContractError(f"Cannot read a valid JSON document: {path.name}") from exc
    if not isinstance(value, dict):
        raise ContractError(f"JSON document must be an object: {path.name}")
    return value
