"""Scoped immutable sandbox snapshot metadata, claims, and clone lineage."""

from __future__ import annotations

from datetime import datetime

import asyncpg

from mission_control.adapters.postgres.documents import PostgresDocumentStore
from mission_control.domain.authoring.canonical import stable_json_dump
from mission_control.domain.execution.contracts import SandboxSnapshot, SnapshotCloneRecord
from mission_control.domain.policies.errors import IdempotencyConflict


class PostgresSandboxSnapshotRepository:
    def __init__(self, pool: asyncpg.Pool, *, request_scope: str) -> None:
        if not request_scope.strip():
            raise ValueError("snapshot repository requires request scope")
        self._pool = pool
        self._scope = request_scope
        self._documents = PostgresDocumentStore(pool)

    async def claim_creation(
        self, snapshot_id: str, creation_identity: str, claimed_at: datetime
    ) -> bool:
        return await self._claim("creation", snapshot_id, creation_identity, claimed_at)

    async def claim_clone(
        self, request_fingerprint: str, clone_id: str, claimed_at: datetime
    ) -> bool:
        return await self._claim("clone", clone_id, request_fingerprint, claimed_at)

    async def _claim(
        self, kind: str, identity: str, fingerprint: str, claimed_at: datetime
    ) -> bool:
        contract = f"sandbox.snapshot.{kind}-claim/1"
        async with self._pool.acquire() as connection, connection.transaction():
            await self._documents.set_scope(connection, self._scope)
            await connection.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
                f"{self._scope}:{contract}:{identity}",
            )
            exists = await connection.fetchval(
                """SELECT EXISTS(SELECT 1 FROM belllabs_control.immutable_documents
                   WHERE request_scope=$1 AND contract=$2 AND identity=$3)""",
                self._scope,
                contract,
                identity,
            )
            await self._documents.put_on(
                connection,
                request_scope=self._scope,
                contract=contract,
                identity=identity,
                payload={"fingerprint": fingerprint},
                recorded_at=claimed_at,
            )
            return not exists

    async def get_snapshot(self, snapshot_id: str) -> SandboxSnapshot | None:
        document = await self._documents.get(
            request_scope=self._scope, contract="sandbox.snapshot/1", identity=snapshot_id
        )
        if document is None:
            return None
        snapshot = SandboxSnapshot.model_validate(document.payload)
        if snapshot.request_scope != self._scope or snapshot.snapshot_id != snapshot_id:
            raise ValueError("stored snapshot identity differs from scoped key")
        return snapshot

    async def create_snapshot(self, snapshot: SandboxSnapshot) -> SandboxSnapshot:
        if snapshot.request_scope != self._scope:
            raise ValueError("snapshot crosses repository request scope")
        try:
            document = await self._documents.put(
                request_scope=self._scope,
                contract="sandbox.snapshot/1",
                identity=snapshot.snapshot_id,
                payload=stable_json_dump(snapshot),
                recorded_at=snapshot.created_at,
            )
        except asyncpg.UniqueViolationError:
            raise IdempotencyConflict(
                "snapshot creation identity already belongs to another ID"
            ) from None
        return SandboxSnapshot.model_validate(document.payload)

    async def get_clone(self, clone_id: str) -> SnapshotCloneRecord | None:
        document = await self._documents.get(
            request_scope=self._scope, contract="sandbox.snapshot.clone/1", identity=clone_id
        )
        if document is None:
            return None
        clone = SnapshotCloneRecord.model_validate(document.payload)
        if clone.clone_id != clone_id:
            raise ValueError("stored snapshot clone identity differs from scoped key")
        return clone

    async def create_clone(self, clone: SnapshotCloneRecord) -> SnapshotCloneRecord:
        if await self.get_snapshot(clone.snapshot_id) is None:
            raise IdempotencyConflict("clone requires a snapshot in the current request scope")
        try:
            document = await self._documents.put(
                request_scope=self._scope,
                contract="sandbox.snapshot.clone/1",
                identity=clone.clone_id,
                payload=stable_json_dump(clone),
                recorded_at=clone.created_at,
            )
        except asyncpg.UniqueViolationError:
            raise IdempotencyConflict(
                "snapshot clone target already belongs to another clone"
            ) from None
        return SnapshotCloneRecord.model_validate(document.payload)
