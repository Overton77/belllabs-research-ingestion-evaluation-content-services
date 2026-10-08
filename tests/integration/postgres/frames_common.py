"""Shared setup for Native Event Store proofs on the common component (SPEC-03)."""

from __future__ import annotations

from dataclasses import dataclass
from uuid import uuid4

import asyncpg

from mission_control.adapters.postgres.operations.checkpoint_lineage import (
    PostgresCheckpointLineageRepository,
)
from mission_control.adapters.postgres.run_control.run_control_repository import (
    PostgresRunControlRepository,
)
from mission_control.application.execution.service import RunControlService
from mission_control.domain.frames.contracts import HarnessExecutionStart, LaneProfile
from mission_control.domain.graph_runtime.identities import RuntimeUnitIdentity
from mission_control.domain.policies.contracts import CommandStatus, StartAction
from tests.fixtures.checkpoint_lineage import (
    LINEAGE_NOW,
    activity_attempt,
    namespace_claim,
    stage_unit,
)
from tests.fixtures.mission_control_common_db import CommonDatabase
from tests.fixtures.provider_frames import BINDING_DIGEST
from tests.integration.postgres.runtime_common import scoped_command, scoped_request
from tests.unit.run_control.test_run_control import service as run_control_service


@dataclass(frozen=True)
class AdmittedUnit:
    run_key: str
    unit: RuntimeUnitIdentity
    run_control: RunControlService

    def start(
        self,
        *,
        generation: int = 1,
        lane: LaneProfile = LaneProfile.DEEP_AGENTS,
        native_session_ref: str | None = None,
    ) -> HarnessExecutionStart:
        from mission_control.application.frames.sink import harness_execution_id

        return HarnessExecutionStart(
            harness_execution_id=harness_execution_id(
                request_scope=self.unit.request_scope,
                run_key=self.run_key,
                activation_key=self.unit.unit_key,
                attempt_no=1,
                lane=lane.value,
            ),
            request_scope=self.unit.request_scope,
            run_key=self.run_key,
            activation_key=self.unit.unit_key,
            attempt_no=1,
            generation=generation,
            lane_profile=lane,
            native_session_ref=native_session_ref or f"thread:{self.unit.unit_key}",
            runtime_kind="deep_agent",
            provider_kind="langgraph",
            placement_kind="local_in_worker",
            intended_binding_digest=BINDING_DIGEST,
            launch_key="launch-1",
        )


async def admit_unit_attempt(
    pool: asyncpg.Pool, db: CommonDatabase, *, tenant: str = "tenant-1", start: bool = True
) -> AdmittedUnit:
    """Admit (and start) a run, then record one runtime-unit attempt (activation + attempt)."""

    run_control, _ = run_control_service(PostgresRunControlRepository(pool))  # type: ignore[arg-type]
    decision = await run_control.admit(scoped_request(db, tenant, request_id=f"frames-{uuid4()}"))
    assert decision.run_id is not None
    if start:
        started = await run_control.execute(
            scoped_command(db, decision.run_id, 1, f"start-{uuid4()}", StartAction(), tenant)
        )
        assert started.status == CommandStatus.ACCEPTED
    unit = stage_unit(
        request_scope=db.scope(tenant), run_id=decision.run_id, operation_id="stage:collect"
    )
    claim = namespace_claim(unit)
    await PostgresCheckpointLineageRepository(pool).record_attempt(
        unit=unit,
        execution_generation=1,
        attempt=activity_attempt(1, workflow_id=f"operation/{unit.unit_key}"),
        binding_id=f"binding:{unit.unit_key}:1",
        binding_digest=claim.binding_digest,
        namespace=claim,
        dispatching=True,
        observed_at=LINEAGE_NOW,
    )
    return AdmittedUnit(run_key=decision.run_id, unit=unit, run_control=run_control)
