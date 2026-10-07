"""Retired PostgreSQL sandbox snapshot store (no common-component persistence).

The transitional implementation persisted ``sandbox.snapshot/1``,
``sandbox.snapshot.clone/1`` and the two ``*-claim/1`` contracts in
the transitional immutable-documents table. G0 inventory (callers map, 2026-10-03) found no
bootstrap/composition or application service that constructs this class; only tests did.
Per the G1 rule "retire unreachable legacy records with evidence rather than blindly
recreating unused tables", the common release defines no sandbox snapshot table and this
adapter fails closed instead of silently falling back to legacy storage. Reintroducing
durable snapshots requires a reviewed support table (scoped creation-identity and
clone-target uniqueness) plus a production composition that uses it.
"""

from __future__ import annotations

from typing import NoReturn


class SandboxSnapshotPersistenceRetired(RuntimeError):
    """Raised when code tries to use the retired PostgreSQL snapshot store."""


class PostgresSandboxSnapshotRepository:
    def __init__(self, *_args: object, **_kwargs: object) -> NoReturn:
        raise SandboxSnapshotPersistenceRetired(
            "PostgreSQL sandbox snapshot persistence is retired in the common mission_control "
            "component; no production composition constructs it"
        )
