"""Typed state for disposable Block C qualification graphs."""

from __future__ import annotations

import operator
from typing import Annotated, Literal, NotRequired, TypedDict


def merge_unique_strings(
    left: tuple[str, ...] | list[str] | None,
    right: tuple[str, ...] | list[str] | None,
) -> tuple[str, ...]:
    return tuple(dict.fromkeys(tuple(left or ()) + tuple(right or ())))


class QualificationState(TypedDict):
    """Shared channels for the N-compatible qualification graph family."""

    request_scope: str
    scenario: Literal["single_interrupt", "parallel_interrupts"]
    compat_version: str
    claim_tokens: Annotated[list[str], operator.add]
    decisions: Annotated[list[str], operator.add]
    events: Annotated[tuple[str, ...], merge_unique_strings]
    decision_refs: Annotated[tuple[str, ...], merge_unique_strings]


class WaitState(TypedDict):
    """Cancellable long-wait fixture with cleanup-visible typed status."""

    request_scope: str
    compat_version: str
    wait_status: Literal["idle", "waiting", "completed", "cancelled"]
    resource_open: bool
    events: Annotated[tuple[str, ...], merge_unique_strings]
    hold_seconds: NotRequired[float]


class QualificationStateN1(TypedDict):
    """Intentionally incompatible N+1 schema (renamed claim channel + version)."""

    request_scope: str
    scenario: Literal["single_interrupt"]
    compat_version: str
    claim_tokens_v2: Annotated[list[str], operator.add]
    decisions: Annotated[list[str], operator.add]
    events: Annotated[tuple[str, ...], merge_unique_strings]
    decision_refs: Annotated[tuple[str, ...], merge_unique_strings]
