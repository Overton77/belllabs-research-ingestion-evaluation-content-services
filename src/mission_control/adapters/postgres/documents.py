"""Scoped immutable document storage for the existing execution contracts.

This adapter consumes the transitional belllabs_control migration, not the future
common mission_control component. It has no Mongo fallback or history import.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import asyncpg

from mission_control.domain.authoring.canonical import sha256_digest
from mission_control.domain.policies.errors import IdempotencyConflict


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


class PostgresDocumentStore:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

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
            )

    @staticmethod
    async def set_scope(connection: asyncpg.Connection, request_scope: str) -> None:
        if not request_scope:
            raise ValueError("document request scope cannot be empty")
        await connection.execute(
            "SELECT set_config('belllabs.request_scope', $1, true)",
            request_scope,
        )

    @staticmethod
    async def put_on(
        connection: asyncpg.Connection,
        *,
        request_scope: str,
        contract: str,
        identity: str,
        payload: dict[str, Any],
        recorded_at: datetime,
    ) -> StoredDocument:
        if not request_scope or not identity or not contract:
            raise ValueError("document scope, contract and identity are required")
        if recorded_at.utcoffset() is None:
            raise ValueError("document observation time must be timezone aware")
        digest = sha256_digest(payload)
        # A conflicting concurrent insert is awaited by PostgreSQL. The next statement
        # sees its committed row under READ COMMITTED and compares immutable content.
        await connection.execute(
            """INSERT INTO belllabs_control.immutable_documents
               (request_scope, contract, identity, payload, digest, recorded_at)
               VALUES ($1, $2, $3, $4::jsonb, $5, $6)
               ON CONFLICT (request_scope, contract, identity) DO NOTHING""",
            request_scope,
            contract,
            identity,
            json.dumps(payload, ensure_ascii=False, allow_nan=False),
            digest,
            recorded_at,
        )
        row = await connection.fetchrow(
            """SELECT identity, payload, digest, recorded_at
               FROM belllabs_control.immutable_documents
               WHERE request_scope=$1 AND contract=$2 AND identity=$3""",
            request_scope,
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
            await self.set_scope(connection, request_scope)
            row = await connection.fetchrow(
                """SELECT identity, payload, digest, recorded_at
                   FROM belllabs_control.immutable_documents
                   WHERE request_scope=$1 AND contract=$2 AND identity=$3""",
                request_scope,
                contract,
                identity,
            )
            return _document(row) if row is not None else None

    async def list(self, *, request_scope: str, contract: str) -> tuple[StoredDocument, ...]:
        async with self._pool.acquire() as connection, connection.transaction():
            await self.set_scope(connection, request_scope)
            rows = await connection.fetch(
                """SELECT identity, payload, digest, recorded_at
                   FROM belllabs_control.immutable_documents
                   WHERE request_scope=$1 AND contract=$2 ORDER BY identity""",
                request_scope,
                contract,
            )
            return tuple(_document(row) for row in rows)
