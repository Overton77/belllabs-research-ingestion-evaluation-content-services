from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, ValidationError
from pydantic_core import to_jsonable_python

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
        if any(not isinstance(key, str) for key in value):
            raise ValueError("canonical JSON object keys must be strings after validation")
        return {key: _normalize(item) for key, item in value.items()}
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
    ).encode("utf-8")


def sha256_digest(value: Any) -> str:
    return f"sha256:{hashlib.sha256(canonical_json(value)).hexdigest()}"


def contract_fingerprint(value: BaseModel, *, exclude: set[str] | None = None) -> str:
    """Order-stable fingerprint of a contract instance.

    `model_dump(mode="json")` turns every `frozenset`/`set` into a list in *iteration*
    order, which depends on the per-process string hash seed (`PYTHONHASHSEED`) and on how
    the set was built. Two equal contracts can therefore dump to differently ordered lists,
    so a fingerprint of the JSON dump is not an identity. Dumping in Python mode keeps sets
    as sets, and `_normalize` sorts them canonically.
    """

    return sha256_digest(value.model_dump(mode="python", exclude=exclude, warnings=False))


def _stabilize_sets(value: Any) -> Any:
    """Walk a contract into plain containers, with every set as a canonically sorted list.

    Models are walked field by field instead of dumped in Python mode because a Python-mode
    dump cannot represent a set of models (the dumped members are unhashable dicts).
    """

    if isinstance(value, BaseModel):
        model = type(value)
        content = {
            name: _stabilize_sets(getattr(value, name))
            for name, field in model.model_fields.items()
            if not field.exclude
        }
        content.update(
            {name: _stabilize_sets(getattr(value, name)) for name in model.model_computed_fields}
        )
        return content
    if isinstance(value, frozenset | set):
        items = [to_jsonable_python(_stabilize_sets(item)) for item in value]
        return sorted(
            items,
            key=lambda item: json.dumps(
                item, sort_keys=True, separators=(",", ":"), ensure_ascii=False
            ),
        )
    if isinstance(value, dict):
        return {key: _stabilize_sets(item) for key, item in value.items()}
    if isinstance(value, tuple | list):
        return [_stabilize_sets(item) for item in value]
    return value


def stable_json_dump(value: BaseModel, *, exclude: set[str] | None = None) -> Any:
    """JSON-compatible dump of a contract whose sets are in canonical (sorted) order.

    Equivalent to `value.model_dump(mode="json", exclude=exclude)` except that every
    `set`/`frozenset` is emitted sorted instead of in per-process iteration order. For a
    contract with no set-valued field the result, and therefore any digest of it, is
    identical to the plain JSON-mode dump, so existing persisted digests stay valid.
    Use this wherever a JSON-shaped dump of a contract feeds a digest or an equality proof.
    """

    content = _stabilize_sets(value)
    for name in exclude or ():
        content.pop(name, None)
    return to_jsonable_python(content)


def stable_json_digest(value: BaseModel, *, exclude: set[str] | None = None) -> str:
    """`sha256_digest` of `stable_json_dump`: order-stable and compatible with JSON-mode digests."""

    return sha256_digest(stable_json_dump(value, exclude=exclude))


def stored_payload_matches(stored: Any, expected: BaseModel) -> bool:
    """Whether a stored JSON payload is exactly `expected`, independent of set order.

    Comparing stored JSON with `expected.model_dump(mode="json")` is order-sensitive for
    set-valued fields: the stored list reflects the writer process's iteration order. Equal
    contracts compare equal as models, whatever order their sets were built in.
    """

    try:
        restored = type(expected).model_validate(stored)
    except ValidationError:
        return False
    return bool(restored == expected)


def verify_digest(value: Any, expected: str) -> None:
    actual = sha256_digest(value)
    if actual != expected:
        raise ValueError(f"digest mismatch: expected {expected}, got {actual}")
