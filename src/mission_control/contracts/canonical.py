"""GENERAL canonical JSON for new versioned contracts (not legacy digest migration)."""

from __future__ import annotations

import hashlib
import json
import math
import unicodedata
from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum
from typing import Any
from uuid import UUID

from pydantic import BaseModel


def _normalized(value: Any) -> Any:
    if isinstance(value, BaseModel):
        return _normalized({name: getattr(value, name) for name in type(value).model_fields})
    if isinstance(value, Enum):
        return _normalized(value.value)
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, str):
        return unicodedata.normalize("NFC", value)
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, int):
        if not -(2**63) <= value < 2**63:
            raise ValueError("canonical integers must fit signed 64 bits")
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("nonfinite canonical number")
        return value
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("nonfinite canonical decimal")
        # Decimal.normalize() can round through the ambient decimal context. Preserve
        # every admitted digit and remove only insignificant fractional trailing zeroes.
        text = format(value, "f")
        if "." in text:
            text = text.rstrip("0").rstrip(".")
        return "0" if value.is_zero() else text
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("canonical timestamps require a timezone")
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
    if isinstance(value, dict):
        result: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise ValueError("canonical object keys must be strings")
            normalized = unicodedata.normalize("NFC", key)
            if normalized in result:
                raise ValueError("canonical keys collide after NFC normalization")
            result[normalized] = _normalized(item)
        return result
    if isinstance(value, frozenset | set):
        # Only explicitly set-typed contract fields are sorted; arrays retain order.
        return sorted((_normalized(item) for item in value), key=_encode)
    if isinstance(value, tuple | list):
        return [_normalized(item) for item in value]
    raise TypeError(f"unsupported canonical type: {type(value).__name__}")


def _encode(value: Any) -> str:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    )


def canonical_bytes(value: Any) -> bytes:
    return _encode(_normalized(value)).encode("utf-8")


def canonical_digest(value: Any) -> str:
    return "sha256:" + hashlib.sha256(canonical_bytes(value)).hexdigest()
