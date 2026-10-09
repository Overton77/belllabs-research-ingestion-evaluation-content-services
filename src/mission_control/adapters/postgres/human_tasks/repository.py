"""Human Gate tasks on the common ``human_task`` / ``human_resolution`` rows (MP-10).

No second approval ledger: a gate task is a ``human_task`` row of kind ``human_gate:<KIND>``
whose inline request packet is the immutable ``mc.human_gate_activation.v1``; the one
attributed answer is the ``human_resolution`` row (unique per task, immutable by trigger).
Every lifecycle transition appends its canonical mission event and outbox row in the same
transaction (``canonical.append_events``), so subscriptions and the outbox relay see exactly
the committed facts. The task row lock serializes concurrent reviewers; the expected task
version and the stored request id make stale answers typed rejections and retries
idempotent.

An open task's version moves only on escalation, so a reviewer who read the task before an
escalation re-reads it (``stale_version``) before answering.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import datetime
from typing import Any
from uuid import UUID

import asyncpg

from mission_control.adapters.postgres.run_control import canonical as mc
from mission_control.adapters.postgres.run_control.canonical import SCOPE, scoped
from mission_control.contracts.identities import uuid7
from mission_control.domain.policies.contracts import ActorContext, DomainEventEnvelope
from mission_control.domain.policies.errors import IdempotencyConflict
from mission_control.domain.programs.human_gate import (
    HUMAN_GATE_TASK_KIND_PREFIX,
    HUMAN_TASK_CANCELLED,
    HUMAN_TASK_CREATED,
    HUMAN_TASK_ESCALATED,
    HUMAN_TASK_EXPIRED,
    HUMAN_TASK_RESOLVED,
    HumanGateActivation,
    HumanResolution,
    HumanTaskView,
    ResolutionCheck,
    default_resolution,
    timeout_disposition,
)

_TASK_COLUMNS = """
    task.human_task_id, task.task_key, task.kind, task.target_ref, task.lifecycle,
    task.version, task.deadline_at, task.on_timeout, task.request_packet, task.created_at,
    task.updated_at, resolution.answer
"""
_TASK_FROM = """
    FROM mission_control.human_task task
    LEFT JOIN mission_control.human_resolution resolution
      ON resolution.installation_id = task.installation_id
     AND resolution.application_id = task.application_id
     AND resolution.tenant_id = task.tenant_id
     AND resolution.human_task_id = task.human_task_id
"""


class PostgresHumanTaskRepository:
    """RLS-scoped Human Gate task persistence for one pool (runtime role)."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def open(self, activation: HumanGateActivation, *, actor_ref: str) -> HumanTaskView:
        """Persist the task and its `human_task.created` event; idempotent per identity."""

        scope = activation.request_scope
        task_id = activation.human_task_id
        packet = _dump(activation)
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, scope)
            await mc.advisory_lock(connection, f"human_task:{scope}:{activation.task_key}")
            prior = await _task_row(connection, args, task_key=activation.task_key, lock=True)
            if prior is not None:
                persisted = HumanGateActivation.model_validate(_json(prior["request_packet"]))
                if persisted != activation:
                    raise IdempotencyConflict("human gate task identity has conflicting intent")
                return _view(prior)
            await connection.execute(
                """
                INSERT INTO mission_control.human_task (
                    installation_id, application_id, tenant_id, human_task_id, task_key,
                    target_ref, kind, request_packet_ref, assignee_scope, deadline_at,
                    on_timeout, lifecycle, request_packet, version, updated_at, created_at,
                    created_by_actor_ref
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, 'open', $12::jsonb, 1,
                        $13, $13, $14)
                """,
                *args,
                task_id,
                activation.task_key,
                activation.target_ref,
                activation.kind,
                activation.request_packet_ref,
                _assignee_scope(activation),
                activation.deadline_at,
                activation.spec.on_timeout,
                packet,
                activation.opened_at,
                actor_ref,
            )
            row = await _task_row(connection, args, task_key=activation.task_key)
            assert row is not None
            view = _view(row)
            await _append(
                connection,
                args,
                view,
                event_type=HUMAN_TASK_CREATED,
                version=1,
                prior_version=0,
                at=activation.opened_at,
                actor_ref=actor_ref,
            )
            return view

    async def get(self, request_scope: str, human_task_id: UUID | str) -> HumanTaskView | None:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            row = await _task_row(connection, args, human_task_id=UUID(str(human_task_id)))
        return _view(row) if row is not None else None

    async def list(
        self,
        request_scope: str,
        *,
        run_id: str | None = None,
        lifecycle: str | None = None,
        limit: int = 100,
    ) -> tuple[HumanTaskView, ...]:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            rows = await connection.fetch(
                f"""
                SELECT {_TASK_COLUMNS} {_TASK_FROM}
                WHERE {scoped("task")} AND task.kind LIKE $4
                  AND ($5::text IS NULL OR task.target_ref LIKE $5)
                  AND ($6::text IS NULL OR task.lifecycle = $6)
                ORDER BY task.created_at, task.task_key
                LIMIT $7
                """,
                *args,
                HUMAN_GATE_TASK_KIND_PREFIX + "%",
                f"run:{_like(run_id)}/gate:%" if run_id is not None else None,
                lifecycle,
                limit,
            )
        return tuple(_view(row) for row in rows)

    async def resolve(
        self,
        request_scope: str,
        human_task_id: UUID | str,
        decide: Callable[[HumanTaskView], ResolutionCheck],
    ) -> tuple[ResolutionCheck, HumanTaskView] | None:
        """Decide under the task row lock; write the resolution and transition once."""

        task_uuid = UUID(str(human_task_id))
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            row = await _task_row(connection, args, human_task_id=task_uuid, lock=True)
            if row is None:
                return None
            view = _view(row)
            check = decide(view)
            if check.status != "accept" or check.resolution is None:
                return check, view
            resolved = await _write_resolution(connection, args, view, check.resolution)
            return check, resolved

    async def apply_timeout(
        self, request_scope: str, human_task_id: UUID | str, *, now: datetime
    ) -> HumanTaskView | None:
        """Apply the gate's timeout policy once the deadline passed (never implicit approval)."""

        task_uuid = UUID(str(human_task_id))
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            row = await _task_row(connection, args, human_task_id=task_uuid, lock=True)
            if row is None:
                return None
            view = _view(row)
            disposition = timeout_disposition(view, now)
            if disposition in {"none", "keep_waiting"}:
                return view
            if disposition == "default_answer":
                return await _write_resolution(
                    connection, args, view, default_resolution(view, now)
                )
            if disposition == "escalate":
                return await _transition(
                    connection, args, view, "open", HUMAN_TASK_ESCALATED, now, "policy:on_timeout"
                )
            return await _transition(
                connection, args, view, "expired", HUMAN_TASK_EXPIRED, now, "policy:on_timeout"
            )

    async def cancel(
        self,
        request_scope: str,
        human_task_id: UUID | str,
        *,
        now: datetime,
        actor_ref: str,
    ) -> HumanTaskView | None:
        """Cancel an open task (stop/cancel of its run); a closed task is returned as is."""

        task_uuid = UUID(str(human_task_id))
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            row = await _task_row(connection, args, human_task_id=task_uuid, lock=True)
            if row is None:
                return None
            view = _view(row)
            if view.lifecycle != "open":
                return view
            return await _transition(
                connection, args, view, "cancelled", HUMAN_TASK_CANCELLED, now, actor_ref
            )


async def _write_resolution(
    connection: asyncpg.Connection,
    args: tuple[Any, ...],
    view: HumanTaskView,
    resolution: HumanResolution,
) -> HumanTaskView:
    await connection.execute(
        """
        INSERT INTO mission_control.human_resolution (
            installation_id, application_id, tenant_id, resolution_id, human_task_id,
            actor_ref, answer, answer_ref, answer_digest, expected_task_version,
            decided_at, created_at, created_by_actor_ref
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8, $9, $10, $11, $11, $6)
        """,
        *args,
        uuid7(),
        UUID(view.human_task_id),
        resolution.actor_ref,
        _dump(resolution),
        resolution.resolution_ref,
        resolution.answer_digest,
        view.version,
        resolution.decided_at,
    )
    return await _transition(
        connection,
        args,
        view,
        "resolved",
        HUMAN_TASK_RESOLVED,
        resolution.decided_at,
        resolution.actor_ref,
    )


async def _transition(
    connection: asyncpg.Connection,
    args: tuple[Any, ...],
    view: HumanTaskView,
    lifecycle: str,
    event_type: str,
    at: datetime,
    actor_ref: str,
) -> HumanTaskView:
    updated = await connection.fetchval(
        f"""
        UPDATE mission_control.human_task
        SET lifecycle = $5, version = version + 1, updated_at = $6
        WHERE {SCOPE} AND human_task_id = $4 AND version = $7 AND lifecycle = 'open'
        RETURNING version
        """,
        *args,
        UUID(view.human_task_id),
        lifecycle,
        at,
        view.version,
    )
    if updated is None:
        raise IdempotencyConflict("human task moved under its row lock")
    row = await _task_row(connection, args, human_task_id=UUID(view.human_task_id))
    assert row is not None
    after = _view(row)
    await _append(
        connection,
        args,
        after,
        event_type=event_type,
        version=int(updated),
        prior_version=view.version,
        at=at,
        actor_ref=actor_ref,
    )
    return after


async def _append(
    connection: asyncpg.Connection,
    args: tuple[Any, ...],
    view: HumanTaskView,
    *,
    event_type: str,
    version: int,
    prior_version: int,
    at: datetime,
    actor_ref: str,
) -> None:
    activation = view.activation
    aggregate = f"human_task:{view.human_task_id}"
    payload: dict[str, object] = {
        "human_task_id": view.human_task_id,
        "task_key": view.task_key,
        "origin": "workflow_gate",
        "kind": view.kind,
        "lifecycle": view.lifecycle,
        "version": view.version,
        "run_id": activation.run_id,
        "gate_key": activation.spec.gate_key,
        "node_key": activation.spec.gate_key,
        "review_round": activation.review_round,
        "packet_digest": activation.packet_digest,
        "deadline_at": activation.deadline_at.isoformat() if activation.deadline_at else None,
        "permitted_decisions": list(activation.permitted_decisions),
    }
    if view.resolution is not None:
        payload["resolution"] = {
            "decision": view.resolution.decision,
            "resolution_action": view.resolution.resolution_action,
            "actor_ref": view.resolution.actor_ref,
            "resolution_ref": view.resolution.resolution_ref,
            "default_applied": view.resolution.default_applied,
        }
    await mc.append_events(
        connection,
        args,
        run_key=activation.run_id,
        commit_key=f"{aggregate}:v{version}:{event_type}",
        expected_versions={aggregate: prior_version},
        events=(
            DomainEventEnvelope(
                event_id=f"{event_type}:{view.human_task_id}:v{version}",
                event_type=event_type,
                aggregate_id=aggregate,
                aggregate_version=version,
                sequence=1,
                occurred_at=at,
                recorded_at=at,
                actor=ActorContext(actor_id=actor_ref),
                correlation_id=view.task_key,
                causation_id=activation.activation_key,
                payload=payload,
            ),
        ),
        actor_ref=actor_ref,
    )


async def _task_row(
    connection: asyncpg.Connection,
    args: tuple[Any, ...],
    *,
    task_key: str | None = None,
    human_task_id: UUID | None = None,
    lock: bool = False,
) -> asyncpg.Record | None:
    if lock:
        # Lock the task row first; a reviewer that waited on the lock then reads the joined
        # resolution in a fresh statement snapshot (READ COMMITTED re-checks only the locked
        # row, never the joined one).
        await connection.execute(
            f"""
            SELECT 1 FROM mission_control.human_task task
            WHERE {scoped("task")} AND task.kind LIKE $4
              AND ($5::text IS NULL OR task.task_key = $5)
              AND ($6::uuid IS NULL OR task.human_task_id = $6)
            FOR UPDATE
            """,
            *args,
            HUMAN_GATE_TASK_KIND_PREFIX + "%",
            task_key,
            human_task_id,
        )
    return await connection.fetchrow(
        f"""
        SELECT {_TASK_COLUMNS} {_TASK_FROM}
        WHERE {scoped("task")} AND task.kind LIKE $4
          AND ($5::text IS NULL OR task.task_key = $5)
          AND ($6::uuid IS NULL OR task.human_task_id = $6)
        """,
        *args,
        HUMAN_GATE_TASK_KIND_PREFIX + "%",
        task_key,
        human_task_id,
    )


def _view(row: asyncpg.Record) -> HumanTaskView:
    lifecycle = row["lifecycle"]
    version = int(row["version"])
    return HumanTaskView(
        human_task_id=str(row["human_task_id"]),
        task_key=row["task_key"],
        kind=row["kind"],
        target_ref=row["target_ref"],
        lifecycle=lifecycle,
        version=version,
        deadline_at=row["deadline_at"],
        on_timeout=row["on_timeout"],
        activation=HumanGateActivation.model_validate(_json(row["request_packet"])),
        resolution=(
            HumanResolution.model_validate(_json(row["answer"]))
            if row["answer"] is not None
            else None
        ),
        # Only an escalation moves an open task's version.
        escalated=lifecycle == "open" and version > 1,
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _assignee_scope(activation: HumanGateActivation) -> str:
    return "reviewers:" + ",".join(sorted(activation.spec.reviewers))


def _like(value: str) -> str:
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _dump(value: Any) -> str:
    return json.dumps(value.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))


def _json(value: Any) -> Any:
    return json.loads(value) if isinstance(value, str) else value


__all__ = ["PostgresHumanTaskRepository"]
