from __future__ import annotations

from collections.abc import Sequence


def merge_unique_events(
    left: Sequence[str] | None,
    right: Sequence[str] | None,
) -> tuple[str, ...]:
    """Deterministically merge compact event references, never event payloads."""

    combined = tuple(left or ()) + tuple(right or ())
    return tuple(dict.fromkeys(combined))
