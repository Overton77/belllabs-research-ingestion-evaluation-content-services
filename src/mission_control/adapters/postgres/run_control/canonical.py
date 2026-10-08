"""Shared canonical mission_control writers for the runtime-authority repositories.

Every helper runs inside the caller's explicit transaction after ``apply_scope``. Rows
carry the parsed composite scope and every lookup filters on all three scope columns,
in addition to the forced row-level security policies. Lock order follows the frozen
protocol: request receipt -> mission -> run -> activation -> budget accounts (sorted) ->
effect.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol
from uuid import UUID

import asyncpg

from mission_control.adapters.postgres.scope import apply_scope
from mission_control.contracts.identities import RequestScope, uuid7
from mission_control.domain.authoring.canonical import sha256_digest, stable_json_dump
from mission_control.domain.policies.contracts import (
    BudgetApplicability,
    BudgetLedgerEntry,
    BudgetState,
    DomainEventEnvelope,
    EffectLedgerEntry,
    EffectLedgerState,
    LifecycleTransitionRecord,
    RunOutcome,
    RunPhase,
    RunProjection,
)
from mission_control.domain.policies.errors import (
    IdempotencyConflict,
    RunControlNotFound,
    RunVersionConflict,
)

WRITER_REF = "mission-control-runtime/1"
ADMIT_ACTION = "mc.run.admit"
LIFECYCLE_ACTION = "mc.run.lifecycle_command"
PROJECTION_CONTRACT = "mc.run_projection/1"
DEFINITION_CONTRACT = "mc.run-request-definition/1"
PROGRAM_CONTRACT = "mc.admitted-run-program/1"
TRANSITION_CONTRACT = "mc.run-lifecycle-transition/1"
EFFECT_LEDGER_CONTRACT = "mc.effect-ledger/1"
EFFECT_ENTRY_CONTRACT = "mc.effect-ledger-entry/1"
EVENT_DESTINATION = "mc.domain_event"
EMPTY_DIMENSION = "_entry"

SCOPE = "installation_id = $1 AND application_id = $2 AND tenant_id = $3"

_LIFECYCLE = {
    RunPhase.PENDING: "admitted",
    RunPhase.ACTIVE: "active",
    RunPhase.WAITING: "waiting",
    RunPhase.PAUSED: "paused",
    RunPhase.CANCELLING: "cancelling",
    RunPhase.TERMINAL: "completed",
}
_OUTCOME = {
    RunOutcome.COMPLETED: "succeeded",
    RunOutcome.PARTIALLY_COMPLETED: "partially_completed",
    RunOutcome.FAILED: "failed",
    RunOutcome.CANCELLED: "cancelled",
}


def scoped(alias: str) -> str:
    """The composite scope predicate for a table alias, bound to $1..$3."""

    return (
        f"{alias}.installation_id = $1 AND {alias}.application_id = $2 AND {alias}.tenant_id = $3"
    )


async def begin(connection: asyncpg.Connection, request_scope: str) -> tuple[Any, ...]:
    """Apply transaction-local scope and return the three bound scope values."""

    scope = await apply_scope(connection, request_scope)
    return scope_args(scope)


def scope_args(scope: RequestScope) -> tuple[UUID, str, UUID]:
    return scope.installation_id, scope.application_id, scope.tenant_id


async def advisory_lock(connection: asyncpg.Connection, key: str) -> None:
    await connection.execute("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", key)


def dump(value: Any) -> str:
    if hasattr(value, "model_dump_json"):
        return str(value.model_dump_json())
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def load(value: Any) -> Any:
    return json.loads(value) if isinstance(value, str) else value


def json_key(*parts: object) -> str:
    return json.dumps([str(part) for part in parts], separators=(",", ":"), ensure_ascii=False)


def lifecycle_columns(projection: RunProjection) -> tuple[str, str, str | None]:
    outcome = projection.terminal_outcome
    return (
        projection.phase.value,
        _LIFECYCLE[projection.phase],
        _OUTCOME[outcome] if outcome is not None else None,
    )


async def run_row(
    connection: asyncpg.Connection,
    args: tuple[Any, ...],
    run_key: str,
    *,
    lock: bool = False,
) -> asyncpg.Record | None:
    return await connection.fetchrow(
        f"""
        SELECT run_id, mission_id, revision_id, run_key, version, phase, projection,
               updated_at
        FROM mission_control.mission_run
        WHERE {SCOPE} AND run_key = $4
        """
        + (" FOR UPDATE" if lock else ""),
        *args,
        run_key,
    )


async def require_run(
    connection: asyncpg.Connection, args: tuple[Any, ...], run_key: str, *, lock: bool = False
) -> asyncpg.Record:
    row = await run_row(connection, args, run_key, lock=lock)
    if row is None:
        raise RunControlNotFound(f"workflow run not found: {run_key}")
    return row


async def run_uuid(connection: asyncpg.Connection, args: tuple[Any, ...], run_key: str) -> UUID:
    value = await connection.fetchval(
        f"SELECT run_id FROM mission_control.mission_run WHERE {SCOPE} AND run_key = $4",
        *args,
        run_key,
    )
    if value is None:
        raise RunControlNotFound(f"workflow run not found: {run_key}")
    return value


async def lock_mission_for_run(
    connection: asyncpg.Connection, args: tuple[Any, ...], run_key: str
) -> asyncpg.Record:
    """Lock the run's mission row, then the run row (frozen lock order)."""

    mission_id = await connection.fetchval(
        f"SELECT mission_id FROM mission_control.mission_run WHERE {SCOPE} AND run_key = $4",
        *args,
        run_key,
    )
    if mission_id is None:
        raise RunControlNotFound(f"workflow run not found: {run_key}")
    await connection.execute(
        f"SELECT 1 FROM mission_control.mission WHERE {SCOPE} AND mission_id = $4 FOR UPDATE",
        *args,
        mission_id,
    )
    return await require_run(connection, args, run_key, lock=True)


async def update_run(
    connection: asyncpg.Connection,
    args: tuple[Any, ...],
    projection: RunProjection,
    *,
    expected_version: int,
) -> None:
    """Compare-and-swap the run projection on its persisted version."""

    phase, lifecycle, outcome = lifecycle_columns(projection)
    target = projection.execution_target
    result = await connection.execute(
        f"""
        UPDATE mission_control.mission_run
        SET version = $5, phase = $6, lifecycle = $7, terminal_outcome = $8,
            projection = $9::jsonb, updated_at = $10,
            workflow_family = COALESCE($11, workflow_family),
            execution_epoch = $12, execution_generation = $13
        WHERE {SCOPE} AND run_key = $4 AND version = $14
        """,
        *args,
        projection.run_id,
        projection.version,
        phase,
        lifecycle,
        outcome,
        dump(projection),
        projection.updated_at,
        target.family if target is not None else None,
        target.execution_epoch if target is not None else 1,
        target.execution_generation if target is not None else 1,
        expected_version,
    )
    if result != "UPDATE 1":
        raise RunVersionConflict(f"expected version {expected_version} for run {projection.run_id}")
    if projection.phase != RunPhase.PENDING:
        await connection.execute(
            f"""
            UPDATE mission_control.mission SET lifecycle = 'active', version = version + 1,
                   updated_at = $5
            WHERE {SCOPE} AND mission_id = (
                SELECT mission_id FROM mission_control.mission_run WHERE {SCOPE} AND run_key = $4
            ) AND lifecycle = 'admitted'
            """,
            *args,
            projection.run_id,
            projection.updated_at,
        )


# --- Admission: mission, definition, revision, compiled program, run ---------------------


async def insert_admitted_run(
    connection: asyncpg.Connection,
    args: tuple[Any, ...],
    projection: RunProjection,
    budget: BudgetState,
    actor_ref: str,
) -> tuple[UUID, UUID]:
    """Insert mission, definition snapshot, revision #1, compiled program and mission run.

    The definition is exactly the admitted Run Request content carried by the projection
    and budget; the program is derived deterministically from those admitted values.
    """

    at = projection.updated_at
    definition = {
        "contract": DEFINITION_CONTRACT,
        "request_scope": projection.request_scope,
        "idempotency_issuer": projection.idempotency_issuer,
        "request_id": projection.request_id,
        "workflow_type_ref": stable_json_dump(projection.workflow_type_ref),
        "effective_configuration_digest": projection.effective_configuration_digest,
        "input_manifest": stable_json_dump(projection.input_manifest),
        "obligation_revision": projection.obligation_revision,
        "required_obligation_refs": sorted(projection.required_obligation_refs),
        "budget_limits": [stable_json_dump(item) for item in budget.limits],
        "baseline_reservations": dict(sorted(budget.reserved.items())),
        "parent_budget_account": budget.parent_account_id,
    }
    definition_digest = sha256_digest(definition)
    program = {
        "contract": PROGRAM_CONTRACT,
        "definition_digest": definition_digest,
        "workflow_type_ref": definition["workflow_type_ref"],
        "effective_configuration_digest": projection.effective_configuration_digest,
        "input_manifest": definition["input_manifest"],
        "obligation_revision": projection.obligation_revision,
        "required_obligation_refs": definition["required_obligation_refs"],
        "budget_limits": definition["budget_limits"],
    }
    program_digest = sha256_digest(program)
    binding_digest = sha256_digest(
        {
            "workflow_type_ref": definition["workflow_type_ref"],
            "input_manifest": definition["input_manifest"],
        }
    )
    mission_id, snapshot_id, revision_id, program_id, run_id = (
        uuid7(),
        uuid7(),
        uuid7(),
        uuid7(),
        uuid7(),
    )
    ref = projection.workflow_type_ref
    await connection.execute(
        """
        INSERT INTO mission_control.mission (
            installation_id, application_id, tenant_id, mission_id, mission_key, title,
            owner_actor_ref, lifecycle, scheduling_head_revision_id, next_event_seq,
            version, updated_at, last_event_seq, created_at, created_by_actor_ref
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, 'admitted', $8, 1, 1, $9, 0, $9, $7)
        """,
        *args,
        mission_id,
        projection.run_id,
        f"{ref.kind}:{ref.logical_id}@{ref.revision}",
        actor_ref,
        revision_id,
        at,
    )
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
        DEFINITION_CONTRACT,
        dump(definition),
        definition_digest,
        at,
        actor_ref,
    )
    await connection.execute(
        """
        INSERT INTO mission_control.mission_revision (
            installation_id, application_id, tenant_id, revision_id, mission_id, revision_no,
            parent_revision_id, definition_snapshot_id, policy_digest, binding_digest,
            committed_at, created_at, created_by_actor_ref
        )
        VALUES ($1, $2, $3, $4, $5, 1, NULL, $6, $7, $8, $9, $9, $10)
        """,
        *args,
        revision_id,
        mission_id,
        snapshot_id,
        projection.effective_configuration_digest,
        binding_digest,
        at,
        actor_ref,
    )
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
        WRITER_REF,
        PROGRAM_CONTRACT,
        dump(program),
        program_digest,
        at,
        actor_ref,
    )
    phase, lifecycle, outcome = lifecycle_columns(projection)
    target = projection.execution_target
    manifest = projection.input_manifest
    await connection.execute(
        """
        INSERT INTO mission_control.mission_run (
            installation_id, application_id, tenant_id, run_id, run_key, mission_id,
            revision_id, scheduling_revision_id, request_key, input_manifest_ref, input_digest,
            admission_binding_ref, admission_binding_digest, workflow_family, phase, lifecycle,
            terminal_outcome, execution_epoch, execution_generation, technical_segment,
            admitted_policy_ref, admitted_build_ref, projection_contract, projection, version,
            admitted_at, updated_at, created_at, created_by_actor_ref
        )
        VALUES (
            $1, $2, $3, $4, $5, $6, $7, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17,
            $18, 1, $19, $20, $21, $22::jsonb, $23, $24, $24, $24, $25
        )
        """,
        *args,
        run_id,
        projection.run_id,
        mission_id,
        revision_id,
        json_key(projection.idempotency_issuer, projection.request_id),
        f"{manifest.manifest_id}@{manifest.revision}",
        manifest.digest,
        f"{ref.kind}:{ref.logical_id}@{ref.revision}",
        ref.digest,
        target.family if target is not None else None,
        phase,
        lifecycle,
        outcome,
        target.execution_epoch if target is not None else 1,
        target.execution_generation if target is not None else 1,
        projection.obligation_revision,
        WRITER_REF,
        PROJECTION_CONTRACT,
        dump(projection),
        projection.version,
        at,
        actor_ref,
    )
    return mission_id, run_id


async def insert_run_for_mission(
    connection: asyncpg.Connection,
    args: tuple[Any, ...],
    projection: RunProjection,
    actor_ref: str,
    *,
    mission_id: UUID,
    revision_id: UUID,
) -> UUID:
    """Insert the admitted run of an existing mission and revision (FT-D2, FT-E3).

    A manifest submit commits the mission, its definition snapshot, revision and compiled
    program first; the run that executes that revision is inserted here, in the same
    transaction as its admission (or, for a chain consumer, its link release).
    """

    run_id = uuid7()
    phase, lifecycle, outcome = lifecycle_columns(projection)
    target = projection.execution_target
    manifest = projection.input_manifest
    ref = projection.workflow_type_ref
    at = projection.updated_at
    await connection.execute(
        """
        INSERT INTO mission_control.mission_run (
            installation_id, application_id, tenant_id, run_id, run_key, mission_id,
            revision_id, scheduling_revision_id, request_key, input_manifest_ref, input_digest,
            admission_binding_ref, admission_binding_digest, workflow_family, phase, lifecycle,
            terminal_outcome, execution_epoch, execution_generation, technical_segment,
            admitted_policy_ref, admitted_build_ref, projection_contract, projection, version,
            admitted_at, updated_at, created_at, created_by_actor_ref
        )
        VALUES (
            $1, $2, $3, $4, $5, $6, $7, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17,
            $18, 1, $19, $20, $21, $22::jsonb, $23, $24, $24, $24, $25
        )
        """,
        *args,
        run_id,
        projection.run_id,
        mission_id,
        revision_id,
        json_key(projection.idempotency_issuer, projection.request_id),
        f"{manifest.manifest_id}@{manifest.revision}",
        manifest.digest,
        f"{ref.kind}:{ref.logical_id}@{ref.revision}",
        ref.digest,
        target.family if target is not None else None,
        phase,
        lifecycle,
        outcome,
        target.execution_epoch if target is not None else 1,
        target.execution_generation if target is not None else 1,
        projection.obligation_revision,
        WRITER_REF,
        PROJECTION_CONTRACT,
        dump(projection),
        projection.version,
        at,
        actor_ref,
    )
    return run_id


# --- Events: ledger commit + contiguous mission events + outbox --------------------------


async def append_events(
    connection: asyncpg.Connection,
    args: tuple[Any, ...],
    *,
    run_key: str,
    commit_key: str,
    expected_versions: dict[str, int],
    events: Sequence[DomainEventEnvelope],
    actor_ref: str,
    run_hooks: bool = True,
) -> UUID | None:
    """Append one ledger commit with contiguous mission events and their outbox rows.

    Sequences are allocated under the mission row lock; mission.next_event_seq and
    last_event_seq advance in the same statement set. Re-appending an already recorded
    envelope is accepted only when it is identical.

    FT-D2: after the rows are written, every registered post-append hook runs on the same
    connection, inside the caller's transaction (the chain reducer releases links there);
    a hook that raises rolls the whole commit back. ``run_hooks=False`` is for writes a
    hook itself makes.
    """

    if not events:
        return None
    pending: list[DomainEventEnvelope] = []
    for event in events:
        prior = await connection.fetchval(
            f"SELECT payload FROM mission_control.outbox WHERE {SCOPE} AND delivery_key = $4",
            *args,
            event.event_id,
        )
        if prior is None:
            pending.append(event)
        elif DomainEventEnvelope.model_validate(load(prior)) != event:
            raise IdempotencyConflict("outbox event collision: identity has conflicting content")
    if not pending:
        return None
    run = await connection.fetchrow(
        "SELECT run_id, mission_id FROM mission_control.mission_run "
        f"WHERE {SCOPE} AND run_key = $4",
        *args,
        run_key,
    )
    if run is None:
        raise RunControlNotFound(f"workflow run not found: {run_key}")
    next_seq = await connection.fetchval(
        f"""
        SELECT next_event_seq FROM mission_control.mission
        WHERE {SCOPE} AND mission_id = $4 FOR UPDATE
        """,
        *args,
        run["mission_id"],
    )
    first = int(next_seq)
    last = first + len(pending) - 1
    commit_id = uuid7()
    recorded = pending[-1].recorded_at
    await connection.execute(
        """
        INSERT INTO mission_control.ledger_commit (
            installation_id, application_id, tenant_id, ledger_commit_id, mission_id,
            commit_key, expected_versions, first_event_seq, last_event_seq, result_ref,
            created_at, created_by_actor_ref
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8, $9, $10, $11, $12)
        """,
        *args,
        commit_id,
        run["mission_id"],
        commit_key,
        dump(expected_versions),
        first,
        last,
        f"run:{run_key}:version:{pending[-1].aggregate_version}",
        recorded,
        actor_ref,
    )
    for offset, event in enumerate(pending):
        event_id = uuid7()
        await connection.execute(
            """
            INSERT INTO mission_control.mission_event (
                installation_id, application_id, tenant_id, event_id, mission_id, seq,
                ledger_commit_id, event_type, event_version, actor_ref, run_id, activation_id,
                happened_at, recorded_at, causation_ref, payload, payload_ref, created_at,
                created_by_actor_ref
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 1, $9, $10, NULL, $11, $12, $13,
                    $14::jsonb, NULL, $12, $9)
            """,
            *args,
            event_id,
            run["mission_id"],
            first + offset,
            commit_id,
            event.event_type,
            event.actor.actor_id,
            run["run_id"],
            event.occurred_at,
            event.recorded_at,
            event.causation_id,
            dump(event),
        )
        await connection.execute(
            """
            INSERT INTO mission_control.outbox (
                installation_id, application_id, tenant_id, outbox_id, ledger_commit_id,
                event_id, delivery_key, destination_kind, event_type, aggregate_key,
                aggregate_version, aggregate_sequence, payload, payload_ref, delivery_state,
                attempts, next_attempt_at, version, created_at, created_by_actor_ref
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13::jsonb, NULL,
                    'pending', 0, $14, 1, $14, $15)
            """,
            *args,
            uuid7(),
            commit_id,
            event_id,
            event.event_id,
            EVENT_DESTINATION,
            event.event_type,
            event.aggregate_id,
            event.aggregate_version,
            event.sequence,
            dump(event),
            event.recorded_at,
            event.actor.actor_id,
        )
    await connection.execute(
        f"""
        UPDATE mission_control.mission
        SET next_event_seq = $5, last_event_seq = $6, version = version + 1, updated_at = $7
        WHERE {SCOPE} AND mission_id = $4
        """,
        *args,
        run["mission_id"],
        last + 1,
        last,
        recorded,
    )
    if run_hooks and _POST_APPEND_HOOKS:
        appended = AppendedEvents(
            run_key=run_key,
            run_id=run["run_id"],
            mission_id=run["mission_id"],
            ledger_commit_id=commit_id,
            events=tuple(pending),
            actor_ref=actor_ref,
        )
        for hook in tuple(_POST_APPEND_HOOKS.values()):
            await hook(connection, args, appended)
    return commit_id


# --- Post-append hooks (FT-D2, SPEC-04 "Chain reducer") ----------------------------------


@dataclass(frozen=True, slots=True)
class AppendedEvents:
    """The rows one ``append_events`` call wrote, handed to post-append hooks."""

    run_key: str
    run_id: UUID
    mission_id: UUID
    ledger_commit_id: UUID
    events: tuple[DomainEventEnvelope, ...]
    actor_ref: str


class PostAppendHook(Protocol):
    async def __call__(
        self, connection: asyncpg.Connection, args: tuple[Any, ...], appended: AppendedEvents
    ) -> None: ...


_POST_APPEND_HOOKS: dict[str, PostAppendHook] = {}


def register_post_append_hook(name: str, hook: PostAppendHook) -> Callable[[], None]:
    """Register ``hook`` under ``name`` (re-registering replaces it); returns an unregister."""

    if not name:
        raise ValueError("post-append hooks are registered under a non-empty name")
    _POST_APPEND_HOOKS[name] = hook

    def unregister() -> None:
        if _POST_APPEND_HOOKS.get(name) is hook:
            del _POST_APPEND_HOOKS[name]

    return unregister


def registered_post_append_hooks() -> tuple[str, ...]:
    return tuple(sorted(_POST_APPEND_HOOKS))


async def append_mission_events(
    connection: asyncpg.Connection,
    args: tuple[Any, ...],
    *,
    mission_id: UUID,
    run_id: UUID | None,
    commit_key: str,
    events: Sequence[DomainEventEnvelope],
    actor_ref: str,
) -> UUID | None:
    """Append contiguous events to one mission's stream without outbox rows or hooks.

    Used for events that belong to several streams at once (chain events carry one envelope
    ``event_id`` in every member mission, SPEC-04 "Events"); idempotent on ``commit_key``.
    """

    if not events:
        return None
    prior = await connection.fetchval(
        f"SELECT ledger_commit_id FROM mission_control.ledger_commit "
        f"WHERE {SCOPE} AND commit_key = $4",
        *args,
        commit_key,
    )
    if prior is not None:
        return UUID(str(prior))
    next_seq = await connection.fetchval(
        f"""
        SELECT next_event_seq FROM mission_control.mission
        WHERE {SCOPE} AND mission_id = $4 FOR UPDATE
        """,
        *args,
        mission_id,
    )
    if next_seq is None:
        raise RunControlNotFound(f"mission not found: {mission_id}")
    first = int(next_seq)
    last = first + len(events) - 1
    commit_id = uuid7()
    recorded = events[-1].recorded_at
    await connection.execute(
        """
        INSERT INTO mission_control.ledger_commit (
            installation_id, application_id, tenant_id, ledger_commit_id, mission_id,
            commit_key, expected_versions, first_event_seq, last_event_seq, result_ref,
            created_at, created_by_actor_ref
        )
        VALUES ($1, $2, $3, $4, $5, $6, '{}'::jsonb, $7, $8, NULL, $9, $10)
        """,
        *args,
        commit_id,
        mission_id,
        commit_key,
        first,
        last,
        recorded,
        actor_ref,
    )
    for offset, event in enumerate(events):
        await connection.execute(
            """
            INSERT INTO mission_control.mission_event (
                installation_id, application_id, tenant_id, event_id, mission_id, seq,
                ledger_commit_id, event_type, event_version, actor_ref, run_id, activation_id,
                happened_at, recorded_at, causation_ref, payload, payload_ref, created_at,
                created_by_actor_ref
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 1, $9, $10, NULL, $11, $12, $13,
                    $14::jsonb, NULL, $12, $9)
            """,
            *args,
            uuid7(),
            mission_id,
            first + offset,
            commit_id,
            event.event_type,
            actor_ref,
            run_id,
            event.occurred_at,
            event.recorded_at,
            event.causation_id,
            dump(event),
        )
    await connection.execute(
        f"""
        UPDATE mission_control.mission
        SET next_event_seq = $5, last_event_seq = $6, version = version + 1, updated_at = $7
        WHERE {SCOPE} AND mission_id = $4
        """,
        *args,
        mission_id,
        last + 1,
        last,
        recorded,
    )
    return commit_id


async def insert_transition(
    connection: asyncpg.Connection,
    args: tuple[Any, ...],
    transition: LifecycleTransitionRecord,
    ledger_commit_id: UUID | None,
) -> None:
    await connection.execute(
        """
        INSERT INTO mission_control.run_lifecycle_transition (
            installation_id, application_id, tenant_id, run_lifecycle_transition_id,
            transition_key, run_key, command_key, prior_version, resulting_version,
            ledger_commit_id, transition_contract, transition, occurred_at, created_at,
            created_by_actor_ref
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12::jsonb, $13, $13, $14)
        """,
        *args,
        uuid7(),
        transition.transition_id,
        transition.run_id,
        transition.command_id,
        transition.prior_version,
        transition.resulting_version,
        ledger_commit_id,
        TRANSITION_CONTRACT,
        dump(transition),
        transition.occurred_at,
        transition.actor.actor_id,
    )


async def transition_payload(
    connection: asyncpg.Connection, args: tuple[Any, ...], transition_key: str
) -> Any:
    return await connection.fetchval(
        f"""
        SELECT transition FROM mission_control.run_lifecycle_transition
        WHERE {SCOPE} AND transition_key = $4
        """,
        *args,
        transition_key,
    )


# --- Budget accounts and entries ----------------------------------------------------------


def _ceilings(budget: BudgetState) -> dict[str, int]:
    return {
        limit.dimension: int(limit.hard_cap)
        for limit in budget.limits
        if limit.applicability == BudgetApplicability.BOUNDED and limit.hard_cap is not None
    }


async def insert_budget(
    connection: asyncpg.Connection,
    args: tuple[Any, ...],
    budget: BudgetState,
    run_id: UUID,
    recorded_at: datetime,
    actor_ref: str,
) -> None:
    parent_id: UUID | None = None
    if budget.parent_account_id is not None:
        parent_id = await connection.fetchval(
            f"""
            SELECT budget_account_id FROM mission_control.budget_account
            WHERE {SCOPE} AND account_key = $4
            """,
            *args,
            budget.parent_account_id,
        )
        if parent_id is None:
            raise RunControlNotFound(f"parent budget account not found: {budget.parent_account_id}")
    await connection.execute(
        """
        INSERT INTO mission_control.budget_account (
            installation_id, application_id, tenant_id, budget_account_id, account_key, run_id,
            parent_budget_account_id, currency, ceilings, reserved, settled, state, version,
            updated_at, created_at, created_by_actor_ref
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, NULL, $8::jsonb, $9::jsonb, $10::jsonb,
                $11::jsonb, 1, $12, $12, $13)
        """,
        *args,
        uuid7(),
        budget.account_id,
        run_id,
        parent_id,
        dump(_ceilings(budget)),
        dump(budget.reserved),
        dump(budget.consumed),
        dump(budget),
        recorded_at,
        actor_ref,
    )


async def budget_state(
    connection: asyncpg.Connection,
    args: tuple[Any, ...],
    *,
    account_key: str | None = None,
    run_key: str | None = None,
    lock: bool = False,
) -> BudgetState | None:
    if account_key is not None:
        raw = await connection.fetchval(
            f"SELECT state FROM mission_control.budget_account WHERE {SCOPE} AND account_key = $4"
            + (" FOR UPDATE" if lock else ""),
            *args,
            account_key,
        )
    else:
        raw = await connection.fetchval(
            f"""
            SELECT account.state FROM mission_control.budget_account account
            JOIN mission_control.mission_run run
              ON run.installation_id = account.installation_id
             AND run.application_id = account.application_id
             AND run.tenant_id = account.tenant_id AND run.run_id = account.run_id
            WHERE {scoped("account")} AND run.run_key = $4
            ORDER BY account.created_at LIMIT 1
            """
            + (" FOR UPDATE OF account" if lock else ""),
            *args,
            run_key,
        )
    return BudgetState.model_validate(load(raw)) if raw is not None else None


async def lock_budget_chain(
    connection: asyncpg.Connection, args: tuple[Any, ...], account_key: str
) -> None:
    """Lock an account and all its ancestors in sorted key order."""

    keys: list[str] = []
    current: str | None = account_key
    while current is not None and current not in keys:
        keys.append(current)
        raw = await connection.fetchval(
            f"SELECT state FROM mission_control.budget_account WHERE {SCOPE} AND account_key = $4",
            *args,
            current,
        )
        current = BudgetState.model_validate(load(raw)).parent_account_id if raw else None
    await connection.execute(
        f"""
        SELECT 1 FROM mission_control.budget_account
        WHERE {SCOPE} AND account_key = ANY($4::text[])
        ORDER BY account_key FOR UPDATE
        """,
        *args,
        sorted(keys),
    )


async def update_budget(
    connection: asyncpg.Connection,
    args: tuple[Any, ...],
    budget: BudgetState,
    updated_at: datetime,
) -> None:
    result = await connection.execute(
        f"""
        UPDATE mission_control.budget_account
        SET state = $5::jsonb, reserved = $6::jsonb, settled = $7::jsonb,
            version = version + 1, updated_at = $8
        WHERE {SCOPE} AND account_key = $4
        """,
        *args,
        budget.account_id,
        dump(budget),
        dump(budget.reserved),
        dump(budget.consumed),
        updated_at,
    )
    if result != "UPDATE 1":
        raise RunControlNotFound(f"budget account not found: {budget.account_id}")


async def budget_entry_payload(
    connection: asyncpg.Connection, args: tuple[Any, ...], entry_key: str
) -> Any:
    return await connection.fetchval(
        f"""
        SELECT entry FROM mission_control.budget_entry
        WHERE {SCOPE} AND causation_ref = $4 LIMIT 1
        """,
        *args,
        entry_key,
    )


async def insert_budget_entries(
    connection: asyncpg.Connection,
    args: tuple[Any, ...],
    entries: Iterable[BudgetLedgerEntry],
    actor_ref: str,
    *,
    replay_safe: bool = False,
) -> None:
    """One budget_entry row per dimension in integer units (a marker row when empty)."""

    for entry in entries:
        if replay_safe:
            prior = await budget_entry_payload(connection, args, entry.entry_id)
            if prior is not None:
                if load(prior) != entry.model_dump(mode="json"):
                    raise IdempotencyConflict("budget ledger entry collision")
                continue
        account = await connection.fetchrow(
            f"""
            SELECT budget_account_id FROM mission_control.budget_account
            WHERE {SCOPE} AND account_key = $4
            """,
            *args,
            entry.account_id,
        )
        if account is None:
            raise RunControlNotFound(f"budget account not found: {entry.account_id}")
        entry_run = await connection.fetchval(
            f"SELECT run_id FROM mission_control.mission_run WHERE {SCOPE} AND run_key = $4",
            *args,
            entry.run_id,
        )
        amounts = sorted(entry.amounts.items()) or [(EMPTY_DIMENSION, 0)]
        for dimension, amount in amounts:
            await connection.execute(
                """
                INSERT INTO mission_control.budget_entry (
                    installation_id, application_id, tenant_id, budget_entry_id,
                    budget_account_id, run_id, source_key, dimension, amount, entry_kind,
                    causation_ref, entry, occurred_at, created_at, created_by_actor_ref
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12::jsonb, $13, $13, $14)
                """,
                *args,
                uuid7(),
                account["budget_account_id"],
                entry_run,
                entry.idempotency_id,
                dimension,
                int(amount),
                entry.kind.value,
                entry.entry_id,
                dump(entry),
                entry.occurred_at,
                actor_ref,
            )


async def list_budget_entries(
    connection: asyncpg.Connection, args: tuple[Any, ...], run_key: str
) -> list[BudgetLedgerEntry]:
    rows = await connection.fetch(
        f"""
        SELECT DISTINCT ON (entry.causation_ref) entry.entry, entry.occurred_at,
               entry.causation_ref
        FROM mission_control.budget_entry entry
        JOIN mission_control.mission_run run
          ON run.installation_id = entry.installation_id
         AND run.application_id = entry.application_id
         AND run.tenant_id = entry.tenant_id AND run.run_id = entry.run_id
        WHERE {scoped("entry")} AND run.run_key = $4
        ORDER BY entry.causation_ref
        """,
        *args,
        run_key,
    )
    ordered = sorted(rows, key=lambda row: (row["occurred_at"], row["causation_ref"]))
    return [BudgetLedgerEntry.model_validate(load(row["entry"])) for row in ordered]


# --- Effect ledger (support) --------------------------------------------------------------


async def insert_effect_ledger(
    connection: asyncpg.Connection,
    args: tuple[Any, ...],
    effects: EffectLedgerState,
    recorded_at: datetime,
    actor_ref: str,
) -> None:
    await connection.execute(
        """
        INSERT INTO mission_control.effect_ledger (
            installation_id, application_id, tenant_id, effect_ledger_id, run_key,
            state_contract, state, version, updated_at, created_at, created_by_actor_ref
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, 1, $8, $8, $9)
        """,
        *args,
        uuid7(),
        effects.run_id,
        EFFECT_LEDGER_CONTRACT,
        dump(effects),
        recorded_at,
        actor_ref,
    )


async def effect_state(
    connection: asyncpg.Connection, args: tuple[Any, ...], run_key: str, *, lock: bool = False
) -> EffectLedgerState | None:
    raw = await connection.fetchval(
        f"SELECT state FROM mission_control.effect_ledger WHERE {SCOPE} AND run_key = $4"
        + (" FOR UPDATE" if lock else ""),
        *args,
        run_key,
    )
    return EffectLedgerState.model_validate(load(raw)) if raw is not None else None


async def update_effect_ledger(
    connection: asyncpg.Connection,
    args: tuple[Any, ...],
    effects: EffectLedgerState,
    updated_at: datetime,
) -> None:
    result = await connection.execute(
        f"""
        UPDATE mission_control.effect_ledger
        SET state = $5::jsonb, version = version + 1, updated_at = $6
        WHERE {SCOPE} AND run_key = $4
        """,
        *args,
        effects.run_id,
        dump(effects),
        updated_at,
    )
    if result != "UPDATE 1":
        raise RunControlNotFound(f"effect ledger not found for run: {effects.run_id}")


async def insert_effect_entries(
    connection: asyncpg.Connection,
    args: tuple[Any, ...],
    entries: Iterable[EffectLedgerEntry],
    actor_ref: str,
) -> None:
    for entry in entries:
        await connection.execute(
            """
            INSERT INTO mission_control.effect_ledger_entry (
                installation_id, application_id, tenant_id, effect_ledger_entry_id, entry_key,
                run_key, effect_key, kind, idempotency_key, entry_contract, entry, occurred_at,
                created_at, created_by_actor_ref
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb, $12, $12, $13)
            """,
            *args,
            uuid7(),
            entry.entry_id,
            entry.run_id,
            entry.effect_id,
            entry.kind,
            entry.idempotency_id,
            EFFECT_ENTRY_CONTRACT,
            dump(entry),
            entry.occurred_at,
            actor_ref,
        )


# --- Request receipts ---------------------------------------------------------------------


async def receipt_row(
    connection: asyncpg.Connection,
    args: tuple[Any, ...],
    *,
    actor_ref: str,
    action: str,
    request_key: str,
) -> asyncpg.Record | None:
    return await connection.fetchrow(
        f"""
        SELECT request_receipt_id, payload_digest, state, result
        FROM mission_control.request_receipt
        WHERE {SCOPE} AND actor_ref = $4 AND action = $5 AND request_key = $6
        """,
        *args,
        actor_ref,
        action,
        request_key,
    )


async def insert_receipt(
    connection: asyncpg.Connection,
    args: tuple[Any, ...],
    *,
    actor_ref: str,
    action: str,
    request_key: str,
    payload_digest: str,
    state: str,
    resource_ref: str | None,
    result: Any,
    recorded_at: datetime,
) -> UUID:
    receipt_id = uuid7()
    await connection.execute(
        """
        INSERT INTO mission_control.request_receipt (
            installation_id, application_id, tenant_id, request_receipt_id, actor_ref, action,
            request_key, payload_digest, state, resource_ref, result, error_ref, updated_at,
            created_at, created_by_actor_ref
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb, NULL, $12, $12, $5)
        """,
        *args,
        receipt_id,
        actor_ref,
        action,
        request_key,
        payload_digest,
        state,
        resource_ref,
        dump(result),
        recorded_at,
    )
    return receipt_id


def command_request_key(run_key: str, command_id: str) -> str:
    return json_key(run_key, command_id)
