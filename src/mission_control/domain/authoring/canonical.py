from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from enum import Enum
from typing import Any, get_args

from pydantic import BaseModel
from pydantic_core import to_jsonable_python

CANONICAL_SCHEMA_VERSION = "canonical-json/1"


def _excluded_if(field: Any, value: Any) -> bool:
    """A field declared `exclude_if` (an optional field added after contracts were recorded)
    is left out of the digest exactly when it is left out of dumps, so adding it changes no
    existing digest or fingerprint."""

    predicate = getattr(field, "exclude_if", None)
    return predicate is not None and bool(predicate(value))


def _digest_neutral_default(field: Any, item: Any) -> bool:
    """True when an additive field marked ``digest_omit_default`` still holds its default.

    Such fields are left out of the canonical form, so definitions published before the
    field existed keep their digest (ADR-0020: published bytes never change identity).
    """
    extra = field.json_schema_extra
    if not (isinstance(extra, dict) and extra.get("digest_omit_default")):
        return False
    return bool(item == field.get_default(call_default_factory=True))


def _normalize(value: Any) -> Any:
    if isinstance(value, BaseModel):
        return {
            field_name: _normalize(getattr(value, field_name))
            for field_name, field in type(value).model_fields.items()
            if not _excluded_if(field, getattr(value, field_name))
            and not _digest_neutral_default(field, getattr(value, field_name))
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

    # Walk the fields instead of calling `model_dump(mode="python")`: a Python-mode dump turns a
    # set of models into a set of dicts and fails ("unhashable type: 'dict'"). `_normalize`
    # walks nested models itself, so the digest equals the Python-mode dump's for every
    # contract that dump could represent.
    skipped = exclude or set()
    return sha256_digest(
        {
            name: getattr(value, name)
            for name, field in type(value).model_fields.items()
            if name not in skipped
            and not field.exclude
            and not _excluded_if(field, getattr(value, name))
        }
    )


def _declared_model(annotation: Any) -> type[BaseModel] | None:
    """The single model class a field annotation declares (also as a container item), if any."""

    if isinstance(annotation, type) and issubclass(annotation, BaseModel):
        return annotation
    found = {
        model
        for argument in get_args(annotation)
        if argument is not type(None) and argument is not Ellipsis
        if (model := _declared_model(argument)) is not None
    }
    return next(iter(found)) if len(found) == 1 else None


def _stabilize_sets(
    value: Any, *, in_set: bool = False, declared: type[BaseModel] | None = None
) -> Any:
    """Walk a contract into plain containers, with every set as a canonically sorted list.

    Models are walked field by field instead of dumped in Python mode because a Python-mode
    dump cannot represent a set of models (the dumped members are unhashable dicts). Like
    pydantic, a model in a field declared as a base model is walked by the declared fields.
    Aware datetimes inside a set are converted to UTC so that equal instants in different
    offsets, which a set treats as one member, dump identically whichever representative it kept.
    """

    if isinstance(value, BaseModel):
        owner = declared if declared is not None and isinstance(value, declared) else type(value)
        content = {
            name: _stabilize_sets(
                getattr(value, name), in_set=in_set, declared=_declared_model(field.annotation)
            )
            for name, field in owner.model_fields.items()
            if not field.exclude and not _digest_neutral_default(field, getattr(value, name))
        }
        content.update(
            {
                name: _stabilize_sets(getattr(value, name), in_set=in_set)
                for name in owner.model_computed_fields
            }
        )
        return content
    if isinstance(value, frozenset | set):
        items = [
            to_jsonable_python(_stabilize_sets(item, in_set=True, declared=declared))
            for item in value
        ]
        return sorted(
            items,
            key=lambda item: json.dumps(
                item, sort_keys=True, separators=(",", ":"), ensure_ascii=False
            ),
        )
    if isinstance(value, dict):
        return {
            key: _stabilize_sets(item, in_set=in_set, declared=declared)
            for key, item in value.items()
        }
    if isinstance(value, tuple | list):
        return [_stabilize_sets(item, in_set=in_set, declared=declared) for item in value]
    if in_set and isinstance(value, datetime) and value.utcoffset() is not None:
        return value.astimezone(UTC)
    return value


def stable_json_dump(value: BaseModel, *, exclude: set[str] | None = None) -> Any:
    """JSON-compatible dump of a contract whose sets are in canonical (sorted) order.

    Equivalent to `value.model_dump(mode="json", exclude=exclude)` except that every
    `set`/`frozenset` is emitted sorted instead of in per-process iteration order. For a
    contract with no set-valued field the result, and therefore any digest of it, is
    identical to the plain JSON-mode dump, so existing persisted digests stay valid.
    Use this wherever a JSON-shaped dump of a contract feeds a digest or an equality proof.

    The walk follows pydantic's declared-type rule (a subclass in a base-typed field dumps by the
    base fields), so for a set-free contract it equals `model_dump(mode="json")`. By contrast
    `_normalize` (and so `sha256_digest(model)` and `contract_fingerprint`) walks each value's
    runtime type and includes subclass fields; that is the established persisted format and is
    deliberately left unchanged. `test_digest_set_order_guard.py` pins every contract field that
    declares a base model with extending subclasses, so a new one is reviewed.
    """

    content = _stabilize_sets(value)
    for name in exclude or ():
        content.pop(name, None)
    return to_jsonable_python(content)


def stable_json_digest(value: BaseModel, *, exclude: set[str] | None = None) -> str:
    """`sha256_digest` of `stable_json_dump`: order-stable and compatible with JSON-mode digests."""

    return sha256_digest(stable_json_dump(value, exclude=exclude))


def _sorted_lists(value: Any) -> Any:
    """Sort every list canonically so set order cannot matter (tuple order is checked apart)."""

    if isinstance(value, dict):
        return {key: _sorted_lists(item) for key, item in value.items()}
    if isinstance(value, list):
        return sorted(
            (_sorted_lists(item) for item in value),
            key=lambda item: json.dumps(
                item, sort_keys=True, separators=(",", ":"), ensure_ascii=False
            ),
        )
    return value


def stored_payload_matches(stored: Any, expected: BaseModel) -> bool:
    """Whether a stored JSON payload is exactly `expected`, independent of set order.

    Comparing stored JSON with `expected.model_dump(mode="json")` is order-sensitive for
    set-valued fields: the stored list reflects the writer process's iteration order. The match
    is as strict as that comparison apart from list order:

    - the stored payload must validate as the expected type (a corrupt payload is a mismatch,
      never an exception);
    - the restored contract equals `expected` and has the same stable dump;
    - the stored payload itself equals the restored contract's JSON dump once lists are
      sorted, so a payload that omits a defaulted field or relies on lax coercion (`"5"` for
      an int) is rejected.

    Only the validation step is guarded; a programming error in the comparison propagates.
    """

    try:
        restored = type(expected).model_validate(stored)
    except Exception:
        return False
    return (
        bool(restored == expected)
        and stable_json_dump(restored) == stable_json_dump(expected)
        and _sorted_lists(stored) == _sorted_lists(stable_json_dump(restored))
    )


def verify_digest(value: Any, expected: str) -> None:
    actual = sha256_digest(value)
    if actual != expected:
        raise ValueError(f"digest mismatch: expected {expected}, got {actual}")
