"""Scoped immutable PostgreSQL candidate descriptors with object-store byte custody."""

from __future__ import annotations

import json
from hashlib import sha256

import asyncpg

from mission_control.adapters.postgres.documents import PostgresDocumentStore
from mission_control.application.artifacts.artifact_promotion import (
    ArtifactPayloadAddress,
    ArtifactPayloadPort,
)
from mission_control.domain.authoring.canonical import sha256_digest, stable_json_dump
from mission_control.domain.execution.contracts import CapturedWorkspaceCandidate
from mission_control.domain.execution.errors import UndeclaredWorkspacePath, WorkspaceDigestMismatch
from mission_control.domain.policies.errors import IdempotencyConflict


class PostgresWorkspaceCandidateContents:
    def __init__(
        self,
        pool: asyncpg.Pool,
        payloads: ArtifactPayloadPort,
        *,
        request_scope: str,
    ) -> None:
        if not request_scope.strip():
            raise ValueError("workspace candidate contents require explicit request scope")
        self._pool = pool
        self._payloads = payloads
        self._scope = request_scope

    @staticmethod
    def _descriptor(row: asyncpg.Record) -> CapturedWorkspaceCandidate:
        payload = row["descriptor"]
        if isinstance(payload, str):
            payload = json.loads(payload)
        if sha256_digest(payload) != row["descriptor_digest"]:
            raise WorkspaceDigestMismatch("workspace candidate descriptor digest mismatch")
        candidate = CapturedWorkspaceCandidate.model_validate(payload)
        if (
            candidate.candidate_id != row["candidate_id"]
            or candidate.namespace_id != row["namespace_id"]
            or candidate.workspace_id != row["workspace_id"]
            or candidate.logical_path != row["logical_path"]
            or candidate.content_digest != row["content_digest"]
            or candidate.size_bytes != row["size_bytes"]
        ):
            raise WorkspaceDigestMismatch("workspace candidate descriptor address mismatch")
        return candidate

    async def put(self, candidate: CapturedWorkspaceCandidate, content: bytes) -> None:
        if (
            candidate.content_digest != f"sha256:{sha256(content).hexdigest()}"
            or candidate.size_bytes != len(content)
        ):
            raise WorkspaceDigestMismatch("workspace candidate bytes do not match their descriptor")
        scope_digest = sha256(self._scope.encode()).hexdigest()
        address = await self._payloads.stage(
            artifact_id=f"workspace-candidate:{scope_digest}:{candidate.candidate_id}",
            content=content,
            content_digest=candidate.content_digest,
            media_type=candidate.media_type,
        )
        if (
            address.content_digest != candidate.content_digest
            or address.size_bytes != candidate.size_bytes
        ):
            raise WorkspaceDigestMismatch("workspace candidate staged address mismatch")
        payload = stable_json_dump(candidate)
        digest = sha256_digest(payload)
        async with self._pool.acquire() as connection, connection.transaction():
            await PostgresDocumentStore.set_scope(connection, self._scope)
            await connection.execute(
                """INSERT INTO belllabs_control.workspace_candidate_descriptors
                   (request_scope,candidate_id,namespace_id,workspace_id,logical_path,descriptor,
                    descriptor_digest,object_ref,content_digest,size_bytes)
                   VALUES ($1,$2,$3,$4,$5,$6::jsonb,$7,$8,$9,$10)
                   ON CONFLICT (request_scope,candidate_id) DO NOTHING""",
                self._scope,
                candidate.candidate_id,
                candidate.namespace_id,
                candidate.workspace_id,
                candidate.logical_path,
                json.dumps(payload),
                digest,
                address.object_ref,
                address.content_digest,
                address.size_bytes,
            )
            row = await connection.fetchrow(
                """SELECT * FROM belllabs_control.workspace_candidate_descriptors
                   WHERE request_scope=$1 AND candidate_id=$2""",
                self._scope,
                candidate.candidate_id,
            )
            if row is None or self._descriptor(row) != candidate:
                raise IdempotencyConflict("workspace candidate content conflict")

    async def _document(self, candidate_id: str) -> asyncpg.Record:
        async with self._pool.acquire() as connection, connection.transaction():
            await PostgresDocumentStore.set_scope(connection, self._scope)
            row = await connection.fetchrow(
                """SELECT * FROM belllabs_control.workspace_candidate_descriptors
                   WHERE request_scope=$1 AND candidate_id=$2""",
                self._scope,
                candidate_id,
            )
            if row is None:
                raise UndeclaredWorkspacePath("workspace candidate descriptor is unavailable")
            self._descriptor(row)
            return row

    async def get(self, candidate_id: str) -> bytes:
        row = await self._document(candidate_id)
        content = await self._payloads.retrieve(
            ArtifactPayloadAddress(
                object_ref=row["object_ref"],
                content_digest=row["content_digest"],
                size_bytes=row["size_bytes"],
            )
        )
        if (
            len(content) != row["size_bytes"]
            or f"sha256:{sha256(content).hexdigest()}" != row["content_digest"]
        ):
            raise WorkspaceDigestMismatch("workspace candidate stored bytes mismatch")
        return content

    async def describe(self, candidate_id: str) -> CapturedWorkspaceCandidate:
        return self._descriptor(await self._document(candidate_id))

    async def find(
        self,
        namespace_id: str,
        workspace_id: str,
        logical_path: str,
    ) -> CapturedWorkspaceCandidate | None:
        async with self._pool.acquire() as connection, connection.transaction():
            await PostgresDocumentStore.set_scope(connection, self._scope)
            row = await connection.fetchrow(
                """SELECT * FROM belllabs_control.workspace_candidate_descriptors
                   WHERE request_scope=$1 AND namespace_id=$2 AND workspace_id=$3
                     AND logical_path=$4
                   ORDER BY recorded_at DESC, candidate_id DESC LIMIT 1""",
                self._scope,
                namespace_id,
                workspace_id,
                logical_path,
            )
            return self._descriptor(row) if row is not None else None
