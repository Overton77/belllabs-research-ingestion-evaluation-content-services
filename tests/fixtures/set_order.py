"""Shared RRM-015 regression helper: equal contracts whose sets iterate in different orders.

`model_dump(mode="json")` lists a `set`/`frozenset` in per-process iteration order. This helper
builds two *equal* copies of a contract in which every set-valued field is padded with probe
names (so hash-collision placement makes the iteration orders differ under every
`PYTHONHASHSEED`) and rebuilt with a different insertion order. A digest that is a real
identity must be equal for the two copies; a digest of a JSON-mode dump is not.
"""

from __future__ import annotations

import json
import random
import typing
from typing import Any

from pydantic import BaseModel

_PROBE_COUNT = 400


def _probe_members(items: frozenset[Any] | set[Any], prefix: str) -> list[Any]:
    """Padding members of the same kind as `items` so dumps stay type-compatible."""

    prototype = next(iter(sorted(items, key=lambda item: json.dumps(item, default=str))), None)
    if isinstance(prototype, BaseModel):
        text_field = next(
            (
                name
                for name in type(prototype).model_fields
                if isinstance(getattr(prototype, name), str)
            ),
            None,
        )
        if text_field is not None:
            return [
                prototype.model_copy(update={text_field: f"{prefix}_{index}"})
                for index in range(_PROBE_COUNT)
            ]
    return [f"{prefix}_{index}" for index in range(_PROBE_COUNT)]


def equal_sets_with_different_iteration_order(
    items: frozenset[Any] | set[Any],
    *,
    probe_prefix: str = "set_order_probe",
) -> tuple[frozenset[Any], frozenset[Any]]:
    """Two equal frozensets whose iteration orders differ (hash-collision placement).

    String hashes depend on the per-process seed, so a small set may happen to have no
    order-changing collision under some seeds. Padding the set with 400 extra names makes
    such collisions overwhelmingly likely under every seed (500+ entries in a 2048-slot
    table), so the helper is seed-independent in practice. The padded sets are equal to each
    other but not to `items`; callers compare the two results, never either to `items`.
    """

    ordered = sorted(
        {*items, *_probe_members(items, probe_prefix)},
        key=lambda item: json.dumps(item, default=str),
    )
    generator = random.Random(4)
    base = frozenset(ordered)
    for _ in range(2_000):
        shuffled = list(ordered)
        generator.shuffle(shuffled)
        candidate = frozenset(shuffled)
        if list(candidate) != list(base):
            return base, candidate
    raise AssertionError("no insertion order changed the iteration order")


def _pair(value: Any, skip: frozenset[str]) -> tuple[Any, Any]:
    """Return two equal rebuilds of `value`; every set inside is padded and re-ordered."""

    if isinstance(value, BaseModel):
        left: dict[str, Any] = {}
        right: dict[str, Any] = {}
        for name in type(value).model_fields:
            if name in skip:
                continue
            left[name], right[name] = _pair(getattr(value, name), skip)
        return value.model_copy(update=left), value.model_copy(update=right)
    if isinstance(value, frozenset | set):
        first, second = equal_sets_with_different_iteration_order(value)
        if isinstance(value, set):
            return set(first), set(second)
        return first, second
    if isinstance(value, tuple | list):
        pairs = [_pair(item, skip) for item in value]
        return type(value)(a for a, _ in pairs), type(value)(b for _, b in pairs)
    if isinstance(value, dict):
        pairs_by_key = {key: _pair(item, skip) for key, item in value.items()}
        return (
            {key: a for key, (a, _) in pairs_by_key.items()},
            {key: b for key, (_, b) in pairs_by_key.items()},
        )
    return value, value


def with_different_set_orders[ModelT: BaseModel](
    model: ModelT, *, skip: frozenset[str] = frozenset()
) -> tuple[ModelT, ModelT]:
    """Two equal copies of `model` whose set-valued fields iterate in different orders.

    `skip` names fields (at any depth) that keep their original value, for sets whose members
    are constrained by downstream validation (for example a `Literal` member type).

    Sets are padded with probe strings and rebuilt without validation (`model_copy`), so the
    copies are not necessarily valid for domain rules that constrain set members; use them for
    digest identity, not for business logic.
    """

    first, second = _pair(model, skip)
    assert first == second, "set-order copies must be equal"
    return first, second


def assert_json_dumps_differ(first: BaseModel, second: BaseModel) -> None:
    """Prove the probe is effective: the JSON-mode dumps of equal copies list sets differently."""

    assert first.model_dump(mode="json", warnings=False) != second.model_dump(
        mode="json", warnings=False
    ), "probe failed: the model holds no set-valued data, or iteration orders did not differ"


def _holds_set(annotation: Any, seen: set[type[BaseModel]]) -> bool:
    origin = typing.get_origin(annotation)
    if annotation in (set, frozenset) or origin in (set, frozenset):
        return True
    if isinstance(annotation, type) and issubclass(annotation, BaseModel):
        return model_holds_set(annotation, _seen=seen)
    return any(_holds_set(argument, seen) for argument in typing.get_args(annotation))


def _all_subclasses(model: type[BaseModel]) -> list[type[BaseModel]]:
    found: list[type[BaseModel]] = []
    for subclass in model.__subclasses__():
        found.append(subclass)
        found.extend(_all_subclasses(subclass))
    return found


def model_holds_set(
    model: type[BaseModel],
    *,
    include_subclasses: bool = True,
    _seen: set[type[BaseModel]] | None = None,
) -> bool:
    """Whether a model (transitively through field annotations and subclasses) holds a set."""

    seen = _seen if _seen is not None else set()
    if model in seen:
        return False
    seen.add(model)
    candidates = [model, *(_all_subclasses(model) if include_subclasses else [])]
    for candidate in candidates:
        for field in candidate.model_fields.values():
            if _holds_set(field.annotation, seen):
                return True
    return False
