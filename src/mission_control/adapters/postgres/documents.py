"""Scoped immutable detail-document storage on the common mission_control component.

Goal, StageGraph and operation detail envelopes are immutable support
``mission_control.runtime_document`` rows: composite tenant scope, a typed contract
allowlist enforced by the table, an append-only trigger and forced row-level security.
They are detail envelopes only; lifecycle, acceptance, events and outbox stay canonical.
Other lanes may bind the same store to their own scoped immutable support table.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import asyncpg

from mission_control.adapters.postgres.scope import apply_scope
from mission_control.contracts.identities import uuid7
from mission_control.domain.authoring.canonical import sha256_digest
from mission_control.domain.policies.errors import IdempotencyConflict

DEFAULT_TABLE = "runtime_document"
_SCOPE = "installation_id = $1 AND application_id = $2 AND tenant_id = $3"
_WRITER = "mission-control-runtime/1"


@dataclass(frozen=True)
class StoredDocument:
    identity: str
    payload: dict[str, Any]
    digest: str
    recorded_at: datetime


def _document(row: asyncpg.Record) -> StoredDocument:
    payload = row["payload"]
    if isinstance(payload, str):
        payload = json.loads(payload)
    if not isinstance(payload, dict) or sha256_digest(payload) != row["digest"]:
        raise ValueError("immutable PostgreSQL document digest mismatch")
    return StoredDocument(row["identity"], payload, row["digest"], row["recorded_at"])


def _qualified(table: str) -> str:
    if re.fullmatch(r"[a-z][a-z0-9_]{0,62}", table) is None:
        raise ValueError("document table must be a fixed mission_control identifier")
    return f"mission_control.{table}"


class PostgresDocumentStore:
    def __init__(self, pool: asyncpg.Pool, *, table: str = DEFAULT_TABLE) -> None:
        self._pool = pool
        self._table = _qualified(table)

    async def put(
        self,
        *,
        request_scope: str,
        contract: str,
        identity: str,
        payload: dict[str, Any],
        recorded_at: datetime,
    ) -> StoredDocument:
        async with self._pool.acquire() as connection, connection.transaction():
            await self.set_scope(connection, request_scope)
            return await self.put_on(
                connection,
                request_scope=request_scope,
                contract=contract,
                identity=identity,
                payload=payload,
                recorded_at=recorded_at,
                table=self._table.removeprefix("mission_control."),
            )

    @staticmethod
    async def set_scope(connection: asyncpg.Connection, request_scope: str) -> None:
        """Bind the canonical composite scope for the current transaction."""

        if not request_scope:
            raise ValueError("document request scope cannot be empty")
        await apply_scope(connection, request_scope)

    @staticmethod
    async def put_on(
        connection: asyncpg.Connection,
        *,
        request_scope: str,
        contract: str,
        identity: str,
        payload: dict[str, Any],
        recorded_at: datetime,
        table: str = DEFAULT_TABLE,
    ) -> StoredDocument:
        if not request_scope or not identity or not contract:
            raise ValueError("document scope, contract and identity are required")
        if recorded_at.utcoffset() is None:
            raise ValueError("document observation time must be timezone aware")
        qualified = _qualified(table)
        scope = await apply_scope(connection, request_scope)
        args = (scope.installation_id, scope.application_id, scope.tenant_id)
        digest = sha256_digest(payload)
        # A conflicting concurrent insert is awaited by PostgreSQL. The next statement
        # sees its committed row under READ COMMITTED and compares immutable content.
        await connection.execute(
            f"""INSERT INTO {qualified}
               (installation_id, application_id, tenant_id, {table}_id, contract, identity,
                payload, digest, recorded_at, created_at, created_by_actor_ref)
               VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8, $9, $9, $10)
               ON CONFLICT (installation_id, application_id, tenant_id, contract, identity)
               DO NOTHING""",
            *args,
            uuid7(),
            contract,
            identity,
            json.dumps(payload, ensure_ascii=False, allow_nan=False),
            digest,
            recorded_at,
            _WRITER,
        )
        row = await connection.fetchrow(
            f"""SELECT identity, payload, digest, recorded_at FROM {qualified}
               WHERE {_SCOPE} AND contract = $4 AND identity = $5""",
            *args,
            contract,
            identity,
        )
        if row is None:
            raise IdempotencyConflict("immutable document is unavailable after insert")
        prior = _document(row)
        if prior.digest != digest:
            raise IdempotencyConflict("immutable document identity has conflicting content")
        return prior

    async def get(
        self,
        *,
        request_scope: str,
        contract: str,
        identity: str,
    ) -> StoredDocument | None:
        async with self._pool.acquire() as connection, connection.transaction():
            scope = await apply_scope(connection, request_scope)
            row = await connection.fetchrow(
                f"""SELECT identity, payload, digest, recorded_at FROM {self._table}
                   WHERE {_SCOPE} AND contract = $4 AND identity = $5""",
                scope.installation_id,
                scope.application_id,
                scope.tenant_id,
                contract,
                identity,
            )
            return _document(row) if row is not None else None

    async def list(self, *, request_scope: str, contract: str) -> tuple[StoredDocument, ...]:
        async with self._pool.acquire() as connection, connection.transaction():
            scope = await apply_scope(connection, request_scope)
            rows = await connection.fetch(
                f"""SELECT identity, payload, digest, recorded_at FROM {self._table}
                   WHERE {_SCOPE} AND contract = $4 ORDER BY identity""",
                scope.installation_id,
                scope.application_id,
                scope.tenant_id,
                contract,
            )
            return tuple(_document(row) for row in rows)
