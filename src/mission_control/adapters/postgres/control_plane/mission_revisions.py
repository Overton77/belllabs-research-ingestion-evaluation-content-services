"""Mission, definition snapshot, revision and compiled program writers (FT-D2, FT-E3).

A manifest submit commits the typed ``MissionDefinition@1`` as a Revision of a mission keyed by
its manifest key (SPEC-05 "Submit and start"); the run that executes the revision is admitted
afterwards, in the same transaction, by ``canonical.insert_run_for_mission``. Everything runs
inside the caller's transaction after ``apply_scope``.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

import asyncpg

from mission_control.adapters.postgres.run_control.canonical import SCOPE, dump
from mission_control.contracts.identities import uuid7


@dataclass(frozen=True, slots=True)
class MissionHead:
    mission_id: UUID
    created: bool
    head_revision_id: UUID | None
    head_definition_digest: str | None


@dataclass(frozen=True, slots=True)
class RevisionRows:
    mission_id: UUID
    revision_id: UUID
    revision_no: int
    definition_snapshot_id: UUID
    compiled_program_id: UUID


async def lock_or_create_mission(
    connection: asyncpg.Connection,
    args: tuple[Any, ...],
    *,
    mission_key: str,
    title: str,
    actor_ref: str,
    at: datetime,
) -> MissionHead:
    """The mission named ``mission_key`` (locked), created ``draft`` when it does not exist."""

    row = await connection.fetchrow(
        f"""
        SELECT mission_id, scheduling_head_revision_id FROM mission_control.mission
        WHERE {SCOPE} AND mission_key = $4 FOR UPDATE
        """,
        *args,
        mission_key,
    )
    if row is None:
        mission_id = uuid7()
        await connection.execute(
            """
            INSERT INTO mission_control.mission (
                installation_id, application_id, tenant_id, mission_id, mission_key, title,
                owner_actor_ref, lifecycle, scheduling_head_revision_id, next_event_seq,
                version, updated_at, last_event_seq, created_at, created_by_actor_ref
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, 'draft', NULL, 1, 1, $8, 0, $8, $7)
            """,
            *args,
            mission_id,
            mission_key,
            title,
            actor_ref,
            at,
        )
        return MissionHead(mission_id, True, None, None)
    head = row["scheduling_head_revision_id"]
    digest = None
    if head is not None:
        digest = await connection.fetchval(
            """
            SELECT snapshot.definition_digest
            FROM mission_control.mission_revision AS revision
            JOIN mission_control.definition_snapshot AS snapshot
              ON snapshot.definition_snapshot_id = revision.definition_snapshot_id
             AND snapshot.installation_id = revision.installation_id
             AND snapshot.application_id = revision.application_id
             AND snapshot.tenant_id = revision.tenant_id
            WHERE revision.installation_id = $1 AND revision.application_id = $2
              AND revision.tenant_id = $3 AND revision.revision_id = $4
            """,
            *args,
            head,
        )
    return MissionHead(row["mission_id"], False, head, digest)


async def insert_revision(
    connection: asyncpg.Connection,
    args: tuple[Any, ...],
    *,
    mission_id: UUID,
    definition_contract: str,
    definition: Mapping[str, Any],
    definition_digest: str,
    compiler_version: str,
    program_schema_version: str,
    program: Mapping[str, Any],
    program_digest: str,
    policy_digest: str,
    binding_digest: str,
    actor_ref: str,
    at: datetime,
    lifecycle: str = "admitted",
) -> RevisionRows:
    """Commit the next revision of ``mission_id`` and make it the scheduling head."""

    prior = await connection.fetchrow(
        f"""
        SELECT revision_id, revision_no FROM mission_control.mission_revision
        WHERE {SCOPE} AND mission_id = $4 ORDER BY revision_no DESC LIMIT 1
        """,
        *args,
        mission_id,
    )
    revision_no = int(prior["revision_no"]) + 1 if prior is not None else 1
    snapshot_id = await connection.fetchval(
        f"""
        SELECT definition_snapshot_id FROM mission_control.definition_snapshot
        WHERE {SCOPE} AND mission_id = $4 AND definition_digest = $5
        """,
        *args,
        mission_id,
        definition_digest,
    )
    if snapshot_id is None:
        snapshot_id = uuid7()
        await connection.execute(
            """
            INSERT INTO mission_control.definition_snapshot (
                installation_id, application_id, tenant_id, definition_snapshot_id, mission_id,
                definition_contract_version, definition, definition_digest, created_at,
                created_by_actor_ref
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8, $9, $10)
            """,
            *args,
            snapshot_id,
            mission_id,
            definition_contract,
            dump(dict(definition)),
            definition_digest,
            at,
            actor_ref,
        )
    revision_id = uuid7()
    await connection.execute(
        """
        INSERT INTO mission_control.mission_revision (
            installation_id, application_id, tenant_id, revision_id, mission_id, revision_no,
            parent_revision_id, definition_snapshot_id, policy_digest, binding_digest,
            committed_at, created_at, created_by_actor_ref
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $11, $12)
        """,
        *args,
        revision_id,
        mission_id,
        revision_no,
        prior["revision_id"] if prior is not None else None,
        snapshot_id,
        policy_digest,
        binding_digest,
        at,
        actor_ref,
    )
    program_id = uuid7()
    await connection.execute(
        """
        INSERT INTO mission_control.compiled_program (
            installation_id, application_id, tenant_id, compiled_program_id, revision_id,
            compiler_version, program_schema_version, program, program_digest, created_at,
            created_by_actor_ref
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9, $10, $11)
        """,
        *args,
        program_id,
        revision_id,
        compiler_version,
        program_schema_version,
        dump(dict(program)),
        program_digest,
        at,
        actor_ref,
    )
    await connection.execute(
        f"""
        UPDATE mission_control.mission
        SET scheduling_head_revision_id = $5, version = version + 1, updated_at = $6,
            lifecycle = CASE WHEN lifecycle = 'draft' THEN $7 ELSE lifecycle END
        WHERE {SCOPE} AND mission_id = $4
        """,
        *args,
        mission_id,
        revision_id,
        at,
        lifecycle,
    )
    return RevisionRows(mission_id, revision_id, revision_no, snapshot_id, program_id)
