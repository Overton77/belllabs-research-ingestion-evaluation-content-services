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

from mission_control.adapters.postgres.run_control import canonical as mc
from mission_control.adapters.postgres.run_control.canonical import SCOPE, dump
from mission_control.application.execution.run_control_repository import AdmissionMutation
from mission_control.contracts.identities import uuid7
from mission_control.domain.authoring.canonical import sha256_digest, stable_json_dump
from mission_control.domain.authoring.manifest import Behavior
from mission_control.domain.authoring.mission_definition import DefinitionNode, MissionDefinition
from mission_control.domain.policies.errors import IdempotencyConflict


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


async def admit_run_for_revision(
    connection: asyncpg.Connection,
    args: tuple[Any, ...],
    mutation: AdmissionMutation,
    *,
    mission_id: UUID,
    revision_id: UUID,
    created_by: str,
) -> UUID:
    """Commit an accepted admission for the run of an existing mission revision.

    The writes ``RunControlRepository.commit_admission`` makes (idempotency receipt, run,
    budget account, effect ledger, admission events and transition, ledger entries), with the
    run attached to the given revision instead of a fresh run-request mission.
    """

    decision = mutation.decision
    if (
        mutation.projection is None
        or mutation.budget is None
        or mutation.effects is None
        or mutation.transition is None
    ):
        raise ValueError("only an accepted admission mutation can be committed")
    if mutation.budget.parent_account_id is not None:
        raise ValueError("a submitted mission run carries its own budget; no parent account")
    prior = await mc.receipt_row(
        connection,
        args,
        actor_ref=decision.idempotency_issuer,
        action=mc.ADMIT_ACTION,
        request_key=decision.request_id,
    )
    if prior is not None:
        raise IdempotencyConflict("the run request was already admitted")
    actor = mutation.transition.actor.actor_id
    at = decision.recorded_at
    await mc.insert_receipt(
        connection,
        args,
        actor_ref=decision.idempotency_issuer,
        action=mc.ADMIT_ACTION,
        request_key=decision.request_id,
        payload_digest=decision.request_fingerprint,
        state="completed",
        resource_ref=decision.run_id,
        result=decision.model_dump(mode="json"),
        recorded_at=at,
    )
    run_uuid = await mc.insert_run_for_mission(
        connection,
        args,
        mutation.projection,
        created_by,
        mission_id=mission_id,
        revision_id=revision_id,
    )
    await mc.insert_budget(connection, args, mutation.budget, run_uuid, at, actor)
    await mc.insert_effect_ledger(connection, args, mutation.effects, at, actor)
    commit_id = await mc.append_events(
        connection,
        args,
        run_key=mutation.projection.run_id,
        commit_key=mutation.transition.transition_id,
        expected_versions={f"run:{mutation.projection.run_id}": 0},
        events=mutation.events,
        actor_ref=actor,
        run_hooks=False,
    )
    await mc.insert_transition(connection, args, mutation.transition, commit_id)
    await mc.insert_budget_entries(connection, args, mutation.ledger_entries, actor)
    return run_uuid


_IMPORTANCE = {"primary": 100, "secondary": 50, "optional": 10}
_NODE_KINDS = {
    Behavior.STAGE_GRAPH: "stage_graph",
    Behavior.GOAL_LOOP: "goal_directed",
    Behavior.PARALLEL_SWARM: "operation",
    Behavior.EVALUATOR_OPTIMIZER: "operation",
    Behavior.AGENT_EXECUTOR: "operation",
    Behavior.DETERMINISTIC_EXECUTOR: "operation",
    Behavior.EVENT_WAIT: "wait",
    Behavior.TIMER: "wait",
    Behavior.HUMAN_GATE: "gate",
    Behavior.PROOF_GATE: "gate",
    Behavior.CHILD_MISSION_INVOCATION: "operation",
}


async def insert_definition_rows(
    connection: asyncpg.Connection,
    args: tuple[Any, ...],
    revision_id: UUID,
    definition: MissionDefinition,
    actor_ref: str,
    at: datetime,
) -> None:
    """The typed rows of ``MissionDefinition@1`` (mig/0002 ``goal``, ``objective``,
    ``success_criterion``, ``program_node``, ``node_objective``) for one revision."""

    goal_ids: dict[str, UUID] = {}
    for goal in definition.goals:
        goal_ids[goal.key] = uuid7()
        await connection.execute(
            """
            INSERT INTO mission_control.goal (installation_id, application_id, tenant_id,
                goal_id, revision_id, goal_key, description, importance, created_at,
                created_by_actor_ref)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            """,
            *args,
            goal_ids[goal.key],
            revision_id,
            goal.key,
            goal.description,
            _IMPORTANCE[goal.importance],
            at,
            actor_ref,
        )
    objective_ids: dict[str, UUID] = {}
    pending = list(definition.objectives)
    while pending:
        progressed = False
        for objective in list(pending):
            if objective.parent is not None and objective.parent not in objective_ids:
                continue
            objective_ids[objective.key] = uuid7()
            await connection.execute(
                """
                INSERT INTO mission_control.objective (installation_id, application_id,
                    tenant_id, objective_id, revision_id, objective_key, goal_id,
                    parent_objective_id, description, created_at, created_by_actor_ref)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                """,
                *args,
                objective_ids[objective.key],
                revision_id,
                objective.key,
                goal_ids[objective.goal_key],
                objective_ids.get(objective.parent) if objective.parent else None,
                objective.description,
                at,
                actor_ref,
            )
            pending.remove(objective)
            progressed = True
        if not progressed:
            raise ValueError("objective parents form a cycle or name unknown objectives")
    for criterion in definition.criteria:
        await connection.execute(
            """
            INSERT INTO mission_control.success_criterion (installation_id, application_id,
                tenant_id, criterion_id, revision_id, goal_id, criterion_key, description,
                criterion_contract, created_at, created_by_actor_ref)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10, $11)
            """,
            *args,
            uuid7(),
            revision_id,
            goal_ids[criterion.goal_key],
            f"{criterion.goal_key}.{criterion.key}",
            criterion.description,
            dump(
                {
                    "evidence": list(criterion.evidence),
                    "acceptance": criterion.acceptance.model_dump(mode="json"),
                }
            ),
            at,
            actor_ref,
        )
    policy_digest = sha256_digest(definition.policies)

    async def node(item: DefinitionNode, parent: str | None) -> None:
        node_id = uuid7()
        body = stable_json_dump(item, exclude={"nodes"})
        await connection.execute(
            """
            INSERT INTO mission_control.program_node (installation_id, application_id,
                tenant_id, program_node_id, revision_id, node_key, parent_node_key,
                behavior_kind, definition_digest, policy_digest, node_definition, created_at,
                created_by_actor_ref)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb, $12, $13)
            """,
            *args,
            node_id,
            revision_id,
            item.key,
            parent,
            _NODE_KINDS[item.behavior],
            sha256_digest(body),
            policy_digest,
            dump(body),
            at,
            actor_ref,
        )
        for key in item.objectives:
            objective_id = objective_ids.get(key)
            if objective_id is None:
                continue  # a goal key, not an objective: recorded in the node definition
            await connection.execute(
                """
                INSERT INTO mission_control.node_objective (installation_id, application_id,
                    tenant_id, node_objective_id, revision_id, program_node_id, objective_id,
                    created_at, created_by_actor_ref)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                """,
                *args,
                uuid7(),
                revision_id,
                node_id,
                objective_id,
                at,
                actor_ref,
            )
        for child in item.nodes:
            await node(child, item.key)

    await node(definition.program, None)
