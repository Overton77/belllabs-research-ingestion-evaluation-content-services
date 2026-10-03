"""Deterministic identifiers derived from the parts that identify a record.

The same parts always yield the same id, so a retried write or a replayed decision
addresses the record it created the first time. The derivation is persisted
(`uuid5` over `NAMESPACE_URL` and the colon-joined parts), so it must not change.
"""

from uuid import NAMESPACE_URL, uuid5


def stable_id(*parts: str) -> str:
    return str(uuid5(NAMESPACE_URL, ":".join(parts)))
