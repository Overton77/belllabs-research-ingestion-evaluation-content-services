"""Shared constants for disposable Stage 0 policy-model tests."""

from collections.abc import Mapping

ALLOWED_STORE_PURPOSES = frozenset({"procedural_preference", "workflow_hint"})


def is_allowed_store_value(purpose: str, value: object) -> bool:
    if not isinstance(value, Mapping):
        return False
    if purpose == "procedural_preference":
        return set(value) == {"value"} and isinstance(
            value["value"],
            str | int | float | bool,
        )
    if purpose == "workflow_hint":
        return (
            set(value) == {"hint", "source_ref"}
            and isinstance(value["hint"], str)
            and isinstance(value["source_ref"], str)
        )
    return False
