"""FT-F3 Stop Fence on a disposable PostgreSQL 17: persisted before the cancel, insert-only,
consulted atomically by effect admissions, under a restricted runtime login with forced RLS."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from uuid import uuid4

import asyncpg
import pytest

from mission_control.adapters.postgres.run_control.run_control_repository import (
    PostgresRunControlRepository,
)
from mission_control.adapters.postgres.run_control.stop_fence import PostgresStopFenceRepository
from mission_control.application.execution.boundary_interventions import BoundaryInterventionService
from mission_control.application.execution.stop_fence import KernelHookFenceGate
from mission_control.application.missions.service import MissionControlService
from mission_control.contracts.contracts import CancelPayload, CommandTarget, MissionCommandRequest
from mission_control.domain.policies.contracts import RunPhase, StartAction
from mission_control.domain.policies.stop_fence import STOP_FENCED, EffectAdmission, StopFence
from tests.fixtures.mission_control_common_db import CommonDatabase
from tests.integration.postgres.runtime_common import common_db  # noqa: F401
from tests.unit.run_control.test_boundary_commands import TARGET
from tests.unit.run_control.test_run_control import actor, command, request, service

pytestmark = pytest.mark.common_db


async def _run(pool: asyncpg.Pool, scope: str) -> tuple[object, str]:
    authority, _ = service(PostgresRunControlRepository(pool))  # type: ignore[arg-type]
    admission = await authority.admit(request(request_scope=scope, request_id=str(uuid4())))
    assert admission.run_id is not None
    await authority.execute(
        command(admission.run_id, 1, "start", StartAction(execution_target=TARGET)).model_copy(
            update={"request_scope": scope}
        )
    )
    return authority, admission.run_id


def _fence(scope: str, run_id: str, generation: int = 1, command_id: str = "cancel-1") -> StopFence:
    return StopFence(
        request_scope=scope,
        run_id=run_id,
        generation=generation,
        command_id=command_id,
        reason="wrong repo",
        requested_at=datetime.now(UTC),
    )


def _admission(scope: str, run_id: str, effect_ref: str, generation: int = 1) -> EffectAdmission:
    return EffectAdmission(
        request_scope=scope,
        run_id=run_id,
        generation=generation,
        effect_ref=effect_ref,
        effect_kind="shell",
        lane_profile="cursor_local",
    )


async def test_fence_is_insert_only_first_wins_and_gates_admissions(
    common_db: CommonDatabase,  # noqa: F811
) -> None:
    pool = await common_db.pool(max_size=4)
    try:
        scope = common_db.scope()
        _authority, run_id = await _run(pool, scope)
        fences = PostgresStopFenceRepository(pool)
        gate = KernelHookFenceGate(fences)
        assert await fences.get(scope, run_id) is None
        before = await gate.before_effect(_admission(scope, run_id, "tool_use:before"))
        assert before.allowed
        stored = await fences.persist(_fence(scope, run_id))
        assert stored.fenced_at is not None and stored.fenced_at >= stored.requested_at
        again = await fences.persist(_fence(scope, run_id, command_id="cancel-2"))
        assert again == stored  # first fence of the generation wins; nothing overwritten
        after = await gate.before_effect(_admission(scope, run_id, "tool_use:after"))
        assert not after.allowed and after.verdict.reason_code == STOP_FENCED
        assert after.frame is not None and after.frame["reason"] == "fenced"
        # Recorded decisions are stable per effect id.
        assert (await gate.before_effect(_admission(scope, run_id, "tool_use:before"))).allowed
        assert not (await gate.before_effect(_admission(scope, run_id, "tool_use:after"))).allowed
        async with pool.acquire() as connection:
            for statement in (
                "UPDATE mission_control.stop_fence SET reason = 'changed'",
                "DELETE FROM mission_control.stop_fence",
                "DELETE FROM mission_control.stop_fence_effect_admission",
            ):
                with pytest.raises(asyncpg.InsufficientPrivilegeError):
                    async with connection.transaction():
                        await connection.execute(statement)
        owner = await asyncpg.connect(common_db.owner_dsn)
        try:
            with pytest.raises(asyncpg.RestrictViolationError):
                await owner.execute("DELETE FROM mission_control.stop_fence")
            assert await owner.fetchval("SELECT count(*) FROM mission_control.stop_fence") == 1
            rows = await owner.fetch(
                "SELECT effect_ref, decision, reason_code FROM "
                "mission_control.stop_fence_effect_admission ORDER BY decided_at"
            )
        finally:
            await owner.close()
        assert [tuple(row) for row in rows] == [
            ("tool_use:before", "allow", None),
            ("tool_use:after", "deny", STOP_FENCED),
        ]
        # Delivery Report milestones.
        await fences.record_milestone(scope, run_id, None, "provider_acknowledged", unit_key="u1")
        await fences.record_milestone(scope, run_id, 1, "settled", unit_key="u1")
        await fences.record_milestone(scope, run_id, 1, "settled", unit_key="u1")  # idempotent
        report = await fences.report(scope, run_id)
        assert report is not None and report.state == "settled"
        assert report.command_id == "cancel-1"
        assert report.requested_at <= report.fence_persisted_at
        assert report.provider_acknowledged_at is not None
        assert report.settled_at is not None and report.settled_at >= report.fence_persisted_at
    finally:
        await pool.close()


async def test_a_fence_write_and_an_effect_admission_never_both_win(
    common_db: CommonDatabase,  # noqa: F811
) -> None:
    pool = await common_db.pool(max_size=8)
    try:
        scope = common_db.scope()
        _authority, run_id = await _run(pool, scope)
        fences = PostgresStopFenceRepository(pool)
        rounds = 25
        for generation in range(1, rounds + 1):
            # Each generation races one fence write against three admissions of new effects.
            await asyncio.gather(
                *(
                    fences.admit_effect(
                        _admission(scope, run_id, f"effect:{generation}:{index}", generation)
                    )
                    for index in range(3)
                ),
                fences.persist(_fence(scope, run_id, generation, f"cancel-{generation}")),
            )
        owner = await asyncpg.connect(common_db.owner_dsn)
        try:
            rows = await owner.fetch(
                """
                SELECT a.generation, a.decision, a.decided_at, f.fenced_at
                FROM mission_control.stop_fence_effect_admission AS a
                JOIN mission_control.stop_fence AS f
                  ON f.run_id = a.run_id AND f.generation = a.generation
                """
            )
        finally:
            await owner.close()
        assert len(rows) == rounds * 3
        for row in rows:
            if row["decision"] == "allow":
                assert row["decided_at"] < row["fenced_at"], "admitted after the fence"
            else:
                assert row["decided_at"] > row["fenced_at"], "denied before any fence"
        decisions = [row["decision"] for row in rows]
        print(f"race outcomes: allow={decisions.count('allow')} deny={decisions.count('deny')}")
        assert set(decisions) <= {"allow", "deny"}
    finally:
        await pool.close()


async def test_immediate_cancel_through_the_postgres_composition_fences_first(
    common_db: CommonDatabase,  # noqa: F811
) -> None:
    pool = await common_db.pool(max_size=4)
    try:
        scope = common_db.scope()
        authority, run_id = await _run(pool, scope)
        fences = PostgresStopFenceRepository(pool)
        facade = MissionControlService(
            authority,  # type: ignore[arg-type]
            BoundaryInterventionService(authority),  # type: ignore[arg-type]
            request_scope=scope,
            stop_fences=fences,
        )
        operator = actor().model_copy(
            update={
                "permissions": actor().permissions | {"workflow_run.read", "workflow_run.admin"}
            }
        )
        version = (await facade.inspect(run_id, operator)).version
        cancel = MissionCommandRequest(
            request_id=uuid4(),
            expected_version=version,
            expected_generation=1,
            target=CommandTarget(id=run_id),
            kind="cancel",
            payload=CancelPayload(urgency="immediate"),
            reason="wrong repo",
        )
        receipt = await facade.command(run_id, cancel, operator)
        assert receipt.admission.status == "accepted"
        replay = await facade.command(run_id, cancel, operator)
        assert replay.replay and replay.admission == receipt.admission
        fence = await fences.get(scope, run_id)
        assert fence is not None and fence.command_id == str(cancel.request_id)
        assert (await facade.inspect(run_id, operator)).projection.phase == RunPhase.CANCELLING
        denied = await KernelHookFenceGate(fences).before_effect(
            _admission(scope, run_id, "tool_use:late")
        )
        assert not denied.allowed
        report = await facade.stop_fence_report(run_id, operator)
        assert report is not None and report.state == "fence_persisted"
    finally:
        await pool.close()
