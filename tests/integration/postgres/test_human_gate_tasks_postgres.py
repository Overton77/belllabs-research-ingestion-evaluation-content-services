"""MP-10 Human Gate tasks on a disposable PostgreSQL 17 (restricted runtime login, forced RLS).

Proves on the common ``human_task`` / ``human_resolution`` rows: one deterministic task per
activation and round with its ``human_task.created`` event and outbox row in the same commit;
two concurrent reviewers produce exactly one attributed resolution; stale versions, another
packet digest and a different second answer are refused while a retry of the same request is
idempotent; timeout policies never approve implicitly; cancel closes an open task; and an
approval of one round cannot be reused by the next round's changed packet.
"""

from __future__ import annotations

import asyncio
from uuid import uuid4

import asyncpg
import pytest

from mission_control.adapters.postgres.human_tasks.repository import PostgresHumanTaskRepository
from mission_control.adapters.postgres.run_control.run_control_repository import (
    PostgresRunControlRepository,
)
from mission_control.application.human_tasks.service import HumanTaskRejected, HumanTaskService
from mission_control.application.subscriptions.aliases import PUBLIC_ALIASES
from mission_control.domain.policies.contracts import ActorContext
from mission_control.domain.policies.errors import IdempotencyConflict
from mission_control.domain.programs.human_gate import (
    HumanGateBindingError,
    HumanResolutionRequest,
    bind_outcome,
    outcome_for,
)
from tests.fixtures.mission_control_common_db import CommonDatabase
from tests.integration.postgres.runtime_common import common_db, owner_rows  # noqa: F401
from tests.unit.human_tasks.fixtures import activation, later, spec
from tests.unit.run_control.test_run_control import request, service

pytestmark = pytest.mark.common_db


async def _admitted_run(pool: asyncpg.Pool, scope: str) -> str:
    authority, _ = service(PostgresRunControlRepository(pool))  # type: ignore[arg-type]
    admission = await authority.admit(request(request_scope=scope, request_id=str(uuid4())))
    assert admission.run_id is not None
    return admission.run_id


def _answer(task_version: int, digest: str, **updates: object) -> HumanResolutionRequest:
    values: dict[str, object] = {
        "request_id": "req-1",
        "expected_task_version": task_version,
        "decision": "approve",
        "reviewed_packet_digest": digest,
    }
    values.update(updates)
    return HumanResolutionRequest.model_validate(values)


async def _events(db: CommonDatabase, task_id: str) -> list[tuple[str, int, bool]]:
    rows = await owner_rows(
        db,
        """
        SELECT e.event_type, o.aggregate_version, o.ledger_commit_id = e.ledger_commit_id AS linked
        FROM mission_control.mission_event e
        JOIN mission_control.outbox o
          ON o.installation_id = e.installation_id AND o.application_id = e.application_id
         AND o.tenant_id = e.tenant_id AND o.event_id = e.event_id
        WHERE o.aggregate_key = $1 ORDER BY e.seq
        """,
        f"human_task:{task_id}",
    )
    return [(row["event_type"], row["aggregate_version"], row["linked"]) for row in rows]


async def test_task_open_resolution_concurrency_and_idempotency(
    common_db: CommonDatabase,  # noqa: F811
) -> None:
    pool = await common_db.pool(max_size=6)
    try:
        scope = common_db.scope()
        run_id = await _admitted_run(pool, scope)
        repository = PostgresHumanTaskRepository(pool)
        item = activation(scope=scope, run_id=run_id)
        opened = await repository.open(item, actor_ref="runtime")
        again = await repository.open(item, actor_ref="runtime")
        assert opened == again and opened.lifecycle == "open" and opened.version == 1
        with pytest.raises(IdempotencyConflict):
            await repository.open(
                activation(scope=scope, run_id=run_id, gate=spec(prompt="other")).model_copy(
                    update={"activation_key": item.activation_key}
                ),
                actor_ref="runtime",
            )
        assert await _events(common_db, opened.human_task_id) == [("human_task.created", 1, True)]
        assert PUBLIC_ALIASES["human_task.created"] == "human_task.opened"

        services = [
            HumanTaskService(repository, request_scope=scope, clock=lambda: later(30))
            for _ in range(2)
        ]

        async def answer(index: int, actor: str, decision: str) -> str:
            try:
                receipt = await services[index].resolve(
                    opened.human_task_id,
                    _answer(1, item.packet_digest, request_id=f"{actor}-1", decision=decision),
                    ActorContext(actor_id=actor, permissions=frozenset({"reviewer:owner"})),
                )
            except HumanTaskRejected as rejected:
                return rejected.code
            return receipt.status

        results = await asyncio.gather(answer(0, "alice", "approve"), answer(1, "bob", "deny"))
        assert sorted(results) == ["accepted", "already_resolved"]
        rows = await owner_rows(
            common_db,
            "SELECT actor_ref, expected_task_version, answer->>'decision' AS decision "
            "FROM mission_control.human_resolution WHERE human_task_id = $1",
            opened.human_task_id,
        )
        assert len(rows) == 1 and rows[0]["expected_task_version"] == 1
        winner = rows[0]["actor_ref"]
        retry = await services[0].resolve(
            opened.human_task_id,
            _answer(
                1,
                item.packet_digest,
                request_id=f"{winner}-1",
                decision=rows[0]["decision"],
            ),
            ActorContext(actor_id=winner, permissions=frozenset({"reviewer:owner"})),
        )
        assert retry.status == "duplicate" and retry.task.version == 2
        stored = await repository.get(scope, opened.human_task_id)
        assert stored is not None and stored.lifecycle == "resolved"
        assert stored.resolution is not None and stored.resolution.actor_ref == winner
        assert await _events(common_db, opened.human_task_id) == [
            ("human_task.created", 1, True),
            ("human_task.resolved", 2, True),
        ]
        owner = await asyncpg.connect(common_db.owner_dsn)
        try:
            with pytest.raises(asyncpg.RestrictViolationError):
                await owner.execute(
                    "UPDATE mission_control.human_resolution SET actor_ref = 'forged'"
                )
        finally:
            await owner.close()
    finally:
        await pool.close()


async def test_stale_answers_changed_packets_and_rounds_cannot_reuse_an_approval(
    common_db: CommonDatabase,  # noqa: F811
) -> None:
    pool = await common_db.pool()
    try:
        scope = common_db.scope()
        run_id = await _admitted_run(pool, scope)
        repository = PostgresHumanTaskRepository(pool)
        gate = spec(remediation_target="draft", max_review_rounds=2)
        round_one = activation(scope=scope, run_id=run_id, gate=gate)
        task = await repository.open(round_one, actor_ref="runtime")
        reviewers = HumanTaskService(repository, request_scope=scope, clock=lambda: later(30))
        owner = ActorContext(actor_id="owner")
        for body, code in (
            (_answer(2, round_one.packet_digest), "stale_version"),
            (_answer(1, "sha256:" + "0" * 64), "packet_digest_mismatch"),
            (_answer(1, round_one.packet_digest, decision="request_changes"), "feedback_required"),
        ):
            with pytest.raises(HumanTaskRejected) as refused:
                await reviewers.resolve(task.human_task_id, body, owner)
            assert refused.value.code == code
        with pytest.raises(HumanTaskRejected) as intruder:
            await reviewers.resolve(
                task.human_task_id,
                _answer(1, round_one.packet_digest),
                ActorContext(actor_id="intruder"),
            )
        assert intruder.value.code == "not_reviewer"
        changes = await reviewers.resolve(
            task.human_task_id,
            _answer(
                1,
                round_one.packet_digest,
                decision="request_changes",
                feedback_artifact_refs=("artifact://notes.md",),
            ),
            owner,
        )
        first_outcome = outcome_for(changes.task)
        assert first_outcome is not None and first_outcome.status == "changes_requested"
        # Remediation produced a new artifact: round 2 is a new task over a new packet.
        round_two = activation(
            scope=scope,
            run_id=run_id,
            gate=gate,
            review_round=2,
            digest="sha256:" + "e" * 64,
        )
        second = await repository.open(round_two, actor_ref="runtime")
        assert second.human_task_id != task.human_task_id
        assert second.activation.permitted_decisions == ("approve", "deny")
        with pytest.raises(HumanTaskRejected) as reused:
            await reviewers.resolve(
                second.human_task_id, _answer(1, round_one.packet_digest), owner
            )
        assert reused.value.code == "packet_digest_mismatch"
        with pytest.raises(HumanGateBindingError):
            bind_outcome(round_two, first_outcome)
        approved = await reviewers.resolve(
            second.human_task_id, _answer(1, round_two.packet_digest, request_id="r2"), owner
        )
        assert bind_outcome(round_two, outcome_for(approved.task)).status == "accepted"  # type: ignore[arg-type]
    finally:
        await pool.close()


async def test_timeout_policies_and_cancel_never_approve(
    common_db: CommonDatabase,  # noqa: F811
) -> None:
    pool = await common_db.pool()
    try:
        scope = common_db.scope()
        run_id = await _admitted_run(pool, scope)
        repository = PostgresHumanTaskRepository(pool)

        def item(key: str, **policy: object):  # type: ignore[no-untyped-def]
            return activation(
                scope=scope, run_id=run_id, gate=spec(gate_key=key, timeout_seconds=60, **policy)
            )

        waiting = await repository.open(item("wait"), actor_ref="runtime")
        stopping = await repository.open(item("stop", on_timeout="stop"), actor_ref="runtime")
        escalating = await repository.open(
            item("escalate", on_timeout="escalate"), actor_ref="runtime"
        )
        defaulting = await repository.open(
            item("default", on_timeout="default_answer", default_decision="deny"),
            actor_ref="runtime",
        )
        early = await repository.apply_timeout(scope, stopping.human_task_id, now=later(30))
        assert early is not None and early.lifecycle == "open" and early.version == 1

        kept = await repository.apply_timeout(scope, waiting.human_task_id, now=later(61))
        assert kept is not None and kept.lifecycle == "open" and outcome_for(kept) is None
        expired = await repository.apply_timeout(scope, stopping.human_task_id, now=later(61))
        assert expired is not None and expired.lifecycle == "expired"
        assert outcome_for(expired).status == "stopped_by_policy"  # type: ignore[union-attr]
        escalated = await repository.apply_timeout(scope, escalating.human_task_id, now=later(61))
        assert escalated is not None and escalated.lifecycle == "open" and escalated.escalated
        again = await repository.apply_timeout(scope, escalating.human_task_id, now=later(120))
        assert again is not None and again.version == escalated.version  # escalates once
        defaulted = await repository.apply_timeout(scope, defaulting.human_task_id, now=later(61))
        assert defaulted is not None and defaulted.resolution is not None
        assert defaulted.resolution.default_applied
        assert defaulted.resolution.actor_ref == "policy:on_timeout"
        assert outcome_for(defaulted).status == "not_accepted"  # type: ignore[union-attr]

        cancelled = await repository.cancel(
            scope, waiting.human_task_id, now=later(70), actor_ref="run-control:cancel"
        )
        assert cancelled is not None and cancelled.lifecycle == "cancelled"
        assert outcome_for(cancelled).status == "cancelled"  # type: ignore[union-attr]
        unchanged = await repository.cancel(
            scope, expired.human_task_id, now=later(70), actor_ref="run-control:cancel"
        )
        assert unchanged is not None and unchanged.lifecycle == "expired"
        reviewers = HumanTaskService(repository, request_scope=scope, clock=lambda: later(80))
        with pytest.raises(HumanTaskRejected) as late:
            await reviewers.resolve(
                cancelled.human_task_id,
                _answer(2, cancelled.activation.packet_digest),
                ActorContext(actor_id="owner"),
            )
        assert late.value.code == "task_cancelled"
        assert [event for event, _, _ in await _events(common_db, escalating.human_task_id)] == [
            "human_task.created",
            "human_task.escalated",
        ]
        listed = await repository.list(scope, run_id=run_id, lifecycle="open")
        assert [task.activation.spec.gate_key for task in listed] == ["escalate"]
    finally:
        await pool.close()
