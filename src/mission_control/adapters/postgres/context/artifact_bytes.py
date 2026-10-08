"""Capture producer outputs for the Context Packer from PostgreSQL custody records (FT-B2).

Two registered reference forms resolve:

- ``workspace-candidate://<candidate_id>``: a captured workspace candidate
  (``mission_control.workspace_candidate_descriptor``), the custody evidence a Deep Agents
  attempt registers for each file it wrote into a writable slot.
- ``artifact://<scope>/<run>/<artifact_id>``: a promoted artifact
  (``mission_control.artifact``).

Both carry an object-store address, so the packer's ``materialize`` tier mounts them through
the existing digest-verified durable input path. Text is fetched only for textual media within
the capture limit. Anything else (for example a model-emitted string) does not resolve.
"""

from __future__ import annotations

import json
from pathlib import PurePosixPath
from typing import Any

import asyncpg

from mission_control.adapters.postgres.scope import apply_scope
from mission_control.application.artifacts.artifact_promotion import (
    ArtifactPayloadAddress,
    ArtifactPayloadPort,
)
from mission_control.application.context.pack_service import ArtifactContent
from mission_control.contracts.identities import parse_request_scope
from mission_control.domain.context.packet import is_text_media
from mission_control.domain.context.refs import (
    durable_input_locator,
    parse_artifact_ref,
    parse_workspace_candidate_ref,
)


class PostgresArtifactBytes:
    """``ArtifactBytesPort`` over candidate descriptors, artifacts and the payload store."""

    def __init__(self, pool: asyncpg.Pool, payloads: ArtifactPayloadPort) -> None:
        self._pool = pool
        self._payloads = payloads

    async def capture(
        self, source_ref: str, *, request_scope: str, max_text_bytes: int
    ) -> ArtifactContent | None:
        candidate_id = parse_workspace_candidate_ref(source_ref)
        if candidate_id is not None:
            row = await self._candidate(candidate_id, request_scope)
            if row is None:
                return None
            descriptor = _json(row["descriptor"])
            return await self._content(
                source_ref,
                object_ref=row["object_ref"],
                content_digest=row["content_digest"],
                size_bytes=int(row["size_bytes"]),
                media_type=str(descriptor.get("media_type") or "application/octet-stream"),
                logical_path=row["logical_path"],
                max_text_bytes=max_text_bytes,
            )
        parsed = parse_artifact_ref(source_ref)
        if parsed is None:
            return None
        artifact_scope, _run_id, artifact_id = parsed
        if artifact_scope != request_scope:
            return None
        row = await self._artifact(artifact_id, request_scope)
        if (
            row is None
            or row["object_key"] is None
            or _json(row["detail"]).get("durable_reference") != source_ref
        ):
            return None
        detail = _json(row["detail"])
        return await self._content(
            source_ref,
            object_ref=row["object_key"],
            content_digest=row["content_digest"],
            size_bytes=int(row["byte_size"]),
            media_type=str(row["media_type"] or "application/octet-stream"),
            logical_path=str(detail.get("logical_path") or artifact_id),
            max_text_bytes=max_text_bytes,
        )

    async def _content(
        self,
        source_ref: str,
        *,
        object_ref: str,
        content_digest: str,
        size_bytes: int,
        media_type: str,
        logical_path: str,
        max_text_bytes: int,
    ) -> ArtifactContent:
        text: str | None = None
        if is_text_media(media_type) and size_bytes <= max_text_bytes:
            content = await self._payloads.retrieve(
                ArtifactPayloadAddress(
                    object_ref=object_ref, content_digest=content_digest, size_bytes=size_bytes
                )
            )
            try:
                text = content.decode("utf-8")
            except UnicodeDecodeError:
                text = None
        return ArtifactContent(
            source_ref=source_ref,
            durable_ref=durable_input_locator(object_ref, content_digest, size_bytes),
            content_digest=content_digest,
            size_bytes=size_bytes,
            media_type=media_type,
            file_name=PurePosixPath(logical_path).name or None,
            text=text,
        )

    async def _candidate(self, candidate_id: str, request_scope: str) -> asyncpg.Record | None:
        scope = parse_request_scope(request_scope)
        async with self._pool.acquire() as connection, connection.transaction():
            await apply_scope(connection, scope)
            row: asyncpg.Record | None = await connection.fetchrow(
                """SELECT descriptor, object_ref, content_digest, size_bytes, logical_path
                   FROM mission_control.workspace_candidate_descriptor
                   WHERE installation_id=$1 AND application_id=$2 AND tenant_id=$3
                     AND candidate_key=$4""",
                scope.installation_id,
                scope.application_id,
                scope.tenant_id,
                candidate_id,
            )
            return row

    async def _artifact(self, artifact_id: str, request_scope: str) -> asyncpg.Record | None:
        scope = parse_request_scope(request_scope)
        async with self._pool.acquire() as connection, connection.transaction():
            await apply_scope(connection, scope)
            row: asyncpg.Record | None = await connection.fetchrow(
                """SELECT detail, object_key, content_digest, byte_size, media_type
                   FROM mission_control.artifact
                   WHERE installation_id=$1 AND application_id=$2 AND tenant_id=$3
                     AND artifact_key=$4""",
                scope.installation_id,
                scope.application_id,
                scope.tenant_id,
                artifact_id,
            )
            return row


def _json(value: Any) -> dict[str, Any]:
    decoded = json.loads(value) if isinstance(value, str) else value
    return decoded if isinstance(decoded, dict) else {}
