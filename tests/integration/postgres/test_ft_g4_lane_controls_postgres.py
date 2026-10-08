"""FT-G4 on a disposable PostgreSQL 17 (migrations through 0030): the lane controls' durable
records through the runtime role under forced RLS.

- A `cancel_and_replace` replacement turn and a continuation's fresh agent move the harness
  execution's write-once native identity only by an explicit supersession naming the identity
  they replace (compare-and-set inside the row lock); a stale or missing guard is refused.
- The `missing_output_policy` follow-up is given at most once per generation: the Kernel Hook
  callback records it in `hook_effect_intent` before answering, so a second `stop` (or a
  replayed one) gets none.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import timedelta
from typing import Any

import asyncpg
import pytest

from mission_control.adapters.cursor.hooks_callback import CursorHookMapper
from mission_control.adapters.postgres.frames.repository import PostgresFrameRepository
from mission_control.adapters.postgres.lanes.execution_state import PostgresLaneExecutionStateStore
from mission_control.adapters.postgres.lanes.hook_tokens import (
    PostgresHookIntentLedger,
    PostgresHookTokenStore,
)
from mission_control.application.execution.harness.hook_callbacks import (
    STOP_FOLLOWUP_EFFECT,
    HookCallbackService,
    HookTokenContext,
    KernelHookCall,
)
from mission_control.application.execution.harness.state import (
    LaneExecutionUpdate,
    NativeIdentityConflict,
)
from mission_control.domain.capabilities.hooks import HookEvent
from mission_control.domain.frames.contracts import LaneProfile
from tests.fixtures.mission_control_common_db import CommonDatabase
from tests.integration.postgres.frames_common import admit_unit_attempt
from tests.integration.postgres.runtime_common import common_db  # noqa: F401

pytestmark = pytest.mark.common_db


async def test_native_identity_moves_only_by_an_explicit_supersession(
    common_db: CommonDatabase,  # noqa: F811
) -> None:
    pool = await common_db.pool("mission_control_runtime")
    try:
        admitted = await admit_unit_attempt(pool, common_db)
        start = admitted.start(lane=LaneProfile.CURSOR_LOCAL, native_session_ref="agent-1")
        await PostgresFrameRepository(pool).open_execution(start)
        states = PostgresLaneExecutionStateStore(pool)
        scope, heid = start.request_scope, start.harness_execution_id
        await states.record(
            scope, heid, LaneExecutionUpdate(native_session_ref="agent-1", native_turn_ref="run-1")
        )
        with pytest.raises(NativeIdentityConflict):
            await states.record(scope, heid, LaneExecutionUpdate(native_turn_ref="run-2"))
        with pytest.raises(NativeIdentityConflict):
            await states.record(
                scope,
                heid,
                LaneExecutionUpdate(native_turn_ref="run-2", supersedes_turn_ref="run-0"),
            )
        # cancel_and_replace: the replacement run of the same agent.
        await states.record(
            scope, heid, LaneExecutionUpdate(native_turn_ref="run-2", supersedes_turn_ref="run-1")
        )
        # request_continuation: a fresh agent and its first run.
        await states.record(
            scope,
            heid,
            LaneExecutionUpdate(
                native_session_ref="agent-2",
                supersedes_session_ref="agent-1",
                native_turn_ref="run-3",
                supersedes_turn_ref="run-2",
            ),
        )
        state = await states.load(scope, heid)
        assert state is not None
        assert (state.native_session_ref, state.native_turn_ref) == ("agent-2", "run-3")
        # A replayed supersession is idempotent; the old identity cannot come back.
        await states.record(
            scope, heid, LaneExecutionUpdate(native_turn_ref="run-3", supersedes_turn_ref="run-2")
        )
        with pytest.raises(NativeIdentityConflict):
            await states.record(scope, heid, LaneExecutionUpdate(native_session_ref="agent-1"))
    finally:
        await pool.close()


class _MissingSummary:
    async def stop_followup(
        self, context: HookTokenContext, payload: Mapping[str, Any]
    ) -> str | None:
        return "Write outputs/summary.md, then stop."


async def test_the_missing_output_follow_up_is_given_once_per_generation(
    common_db: CommonDatabase,  # noqa: F811
) -> None:
    pool = await common_db.pool("mission_control_runtime")
    try:
        admitted = await admit_unit_attempt(pool, common_db)
        start = admitted.start(lane=LaneProfile.CURSOR_LOCAL, native_session_ref="agent-9")
        frames = PostgresFrameRepository(pool)
        await frames.open_execution(start)
        scope = start.request_scope
        intents = PostgresHookIntentLedger(pool)
        service = HookCallbackService(
            tokens=PostgresHookTokenStore(pool),
            intents=intents,
            mapper=CursorHookMapper(),
            frames=frames,
            stop_policy=_MissingSummary(),
        )
        context = HookTokenContext(
            request_scope=scope,
            run_id=admitted.run_key,
            attempt_no=1,
            generation=1,
            harness_execution_id=start.harness_execution_id,
            lane_profile="cursor_local",
            execution_start=start,
            workspace_root="/lease",
        )
        token = await service.issue(context, ttl=timedelta(hours=1))
        parts = scope.split("/")
        scope_block = {
            "installation_id": parts[1],
            "application_id": parts[2],
            "tenant_id": parts[3],
        }

        def stop(loop_count: int) -> KernelHookCall:
            return KernelHookCall(
                kernel_hook_id="mc.usage",
                event=HookEvent.STOP,
                scope=scope_block,  # type: ignore[arg-type]
                harness_execution_id=str(start.harness_execution_id),
                generation=1,
                payload={"status": "completed", "loop_count": loop_count},
            )

        first = await service.handle(stop(0), token)
        assert first.followup_message == "Write outputs/summary.md, then stop."
        # The provider's loop counter is 0 again (a replayed hook): the ledger still says no.
        replayed = await service.handle(stop(0), token)
        assert replayed.followup_message is None
        later = await service.handle(stop(1), token)
        assert later.followup_message is None
        owner = await asyncpg.connect(common_db.owner_dsn)
        try:
            rows = await owner.fetch(
                "SELECT effect_ref, effect_kind FROM mission_control.hook_effect_intent "
                "WHERE effect_ref = $1",
                STOP_FOLLOWUP_EFFECT,
            )
        finally:
            await owner.close()
        assert [(row["effect_ref"], row["effect_kind"]) for row in rows] == [
            (STOP_FOLLOWUP_EFFECT, "model")
        ]
    finally:
        await pool.close()
