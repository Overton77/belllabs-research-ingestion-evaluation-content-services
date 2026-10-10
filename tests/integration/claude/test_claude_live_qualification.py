"""MP-07 live qualification drill for `claude_agent_sdk` (paid, owner-run; skipped unless enabled).

Runs only with every precondition of `live_drill.missing_preconditions()`:
`MC_LIVE_CLAUDE_QUALIFICATION=1`, a finite owner-approved `MC_PAID_BUDGET_USD`, an explicit
`MC_CLAUDE_AUTH_ROUTE` (`api_key` or `owner_cli_login`; no default), the owner's auth profile
document and profile id (`MISSION_CONTROL_AUTH_PROFILES_PATH`, `MC_CLAUDE_AUTH_PROFILE`), the
owner's attestation for the policy-restricted login route, and a Linux / WSL 2 / macOS worker.
`make lane-qualify PROFILE=claude_agent_sdk LIVE=1` sets the flag; CI never does.

It drives the real lane through the production composition (`compose.compose_claude_local`,
the production `SdkClientFactory` behind a recording tee, MP-05 admission, MP-11 broker):

1. one session, one turn through `LaneTurnService`: start, the client-stamped turn uuid, a
   Task subagent whose Bash call reaches `can_use_tool` and is bound to an approval Human Task
   (the drill's reviewer is the owner's scripted decision, recorded as such), subordinate
   lifecycle frames, settled tokens and the SDK's estimated cost;
2. a second session: interrupt a long turn and replace it, the drained `aborted_*` terminal
   reason, no leftover in the replacement, `get_context_usage` occupancy;
3. process death: a new harness resumes that session by id (`resume=`) after MP-11
   `recover`, and one more turn runs on the resumed connection.

It stops before each step once the estimated spend reaches the budget, records the CLI's raw
stream (scrubbed) and writes `docs/qualification/lanes/claude_agent_sdk-<date>.md` with the
auth route and the observed capability matrix. An ambiguous paid effect is `unknown` and the
drill never qualifies then.
"""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from typing import Any

import pytest

from mission_control.adapters.claude.approvals import BrokerPermissionBinding
from mission_control.adapters.claude.compose import broker_permissions, compose_claude_local
from mission_control.adapters.claude.harness import (
    ClaudeAgentSdkHarness,
    ClaudeLaneSettings,
    StaticAuthAdmitter,
    turn_reference,
)
from mission_control.adapters.claude.transport import SdkClientFactory
from mission_control.adapters.cursor.projection import (
    RenderedProjectionSource,
    operating_contract,
    static_rows,
)
from mission_control.application.agentic_components.materialization import projection_digest
from mission_control.application.agentic_components.projections import render_host_files
from mission_control.application.execution.approvals_broker import ApprovalBroker
from mission_control.application.execution.approvals_memory import (
    InMemoryApprovalStore,
    StaticApprovalContext,
)
from mission_control.application.execution.auth_admission import AuthAdmission
from mission_control.application.execution.harness.hook_callbacks import InMemoryHookIntentLedger
from mission_control.application.execution.harness.inject import cancel_and_replace_turn
from mission_control.application.execution.harness.lane_turns import (
    LaneExecutionIdentity,
    harness_scope,
)
from mission_control.application.execution.stop_fence import InMemoryStopFenceRepository
from mission_control.application.frames.sink import InMemoryFrameStore
from mission_control.application.human_tasks.memory import InMemoryHumanTaskRepository
from mission_control.application.human_tasks.service import HumanTaskService
from mission_control.bootstrap.provider_auth import (
    compose_auth_admission,
    provider_child_environment,
)
from mission_control.bootstrap.settings import Settings
from mission_control.domain.execution.contracts import (
    OperationExecutionRequest,
    OperationExecutionResult,
)
from mission_control.domain.execution.lane_turns import LaneSegmentBounds, LaneTurnRequest
from mission_control.domain.execution.lanes import (
    CancelTurnRequest,
    EndSessionRequest,
    ObserveRequest,
    PrepareRequest,
    ReattachRequest,
    SendTurnRequest,
    SessionHandle,
    StartRequest,
    TurnHandle,
)
from mission_control.domain.frames.contracts import FrameKind
from mission_control.domain.policies.contracts import ActorContext
from tests.fixtures.lane_turns import RecordingSignals, lane_stack
from tests.integration.claude.live_drill import (
    DrillOutcome,
    RecordingClientFactory,
    Spend,
    Stopwatch,
    auth_route,
    budget_usd,
    live_enabled,
    missing_preconditions,
    write_record,
    write_recording,
)
from tests.unit.approvals.fixtures import answer
from tests.unit.claude.fixtures import (
    PROFILE,
    FixtureWorkspace,
    base_operation,
    claude_binding,
    claude_operation,
)

DRILL_REVIEWER = "owner:drill"
SEGMENT = LaneSegmentBounds(
    max_frames=500,
    max_duration_s=600,
    start_to_close_s=900,
    heartbeat_timeout_s=60,
    status_poll_limit=10,
    status_poll_interval_s=2,
    busy_wait_s=120,
)
TURN_ONE = (
    "Mission Control lane qualification drill. Use the Task tool exactly once to start one "
    "subagent; the subagent runs the Bash command `ls` once in the current directory and "
    "reports the file names. Then reply with the single line DRILL-OK and nothing else. Do "
    "not edit any file."
)
LONG_TURN = "Write the numbers from 1 to 600, one per line, and nothing else. Do not use tools."
REPLACEMENT = "Ignore the previous request. Reply with the single line REPLACED-OK."
RESUMED = "Reply with the single line RESUMED-OK."


def _rows() -> tuple[Any, ...]:
    from tests.fixtures.projections.rows import skill_row

    # One skill only: no hook script, no MCP server and no executable is projected.
    return (skill_row(),)


def _operation(admission: AuthAdmission, *, key: str) -> OperationExecutionRequest:
    base = base_operation()
    digest = projection_digest(render_host_files(_rows(), PROFILE, operating_contract(base), None))
    binding = claude_binding(
        materialization_digest=digest,
        model={
            "profile": "frontier.default",
            "model_id": os.environ.get("MC_CLAUDE_DRILL_MODEL", "claude-sonnet-4-5"),
        },
        auth={"profile": admission.profile_id, "billing_mode": admission.billing_mode},
        provider_options={
            "provider": PROFILE,
            "permission_mode": "default",
            # Bash is not pre-allowed: its call must reach `can_use_tool`.
            "allowed_tools": ["Read", "Task"],
            "max_turns": 8,
        },
    )
    operation = claude_operation(binding=binding)
    payload = operation.model_dump(mode="python")
    identity = payload["identity"]
    payload["identity"] = {**dict(identity), "operation_id": f"claude-drill-{key}"}
    payload["idempotency_key"] = f"claude-drill:{key}"
    return OperationExecutionRequest.model_validate(payload)


def _fields(operation: OperationExecutionRequest, key: str) -> dict[str, Any]:
    identity = LaneExecutionIdentity.of(operation, PROFILE, 1)
    assert operation.provider_binding is not None
    return {
        "scope": harness_scope(identity.request_scope),
        "lane_profile": PROFILE,
        "harness_execution_id": str(identity.harness_execution_id),
        "binding_digest": operation.provider_binding.binding_digest,
        "idempotency_key": f"{identity.harness_execution_id}:1:{key}",
        "generation": 1,
    }


async def _admitted(outcome: DrillOutcome) -> AuthAdmission:
    route = auth_route()
    assert route is not None
    settings = Settings(
        _env_file=None,
        mission_control_auth_profiles_path=Path(os.environ["MISSION_CONTROL_AUTH_PROFILES_PATH"]),
    )
    service = compose_auth_admission(settings)
    assert service is not None, "the owner's auth profile document is required"
    admission = await service.admit("claude_agent_sdk", os.environ["MC_CLAUDE_AUTH_PROFILE"])
    # The explicit flag must name the route MP-05 admitted: no silent route substitution.
    assert admission.route == route, f"admitted {admission.route}, the flag says {route}"
    if route == "owner_cli_login":
        attestation = os.environ["MC_CLAUDE_OWNER_ATTESTATION_REF"]
        assert admission.owner_attestation_ref == attestation, (
            "the policy-restricted login route needs the owner's recorded attestation"
        )
    outcome.auth = {
        "route": admission.route,
        "route_support": admission.route_support,
        "billing_mode": admission.billing_mode,
        "profile_id": admission.profile_id,
        "credential_ref": admission.credential_ref or "none (the CLI's own login)",
        "owner_attestation_ref": admission.owner_attestation_ref or "none",
        "env_unset": ",".join(admission.env_unset) or "none",
        "config_dir_relocated": str(route != "owner_cli_login").lower(),
        "observation_source": admission.observation_source,
    }
    return admission


class _Approvals:
    """The MP-11 broker over in-memory stores and the drill's scripted reviewer."""

    def __init__(self, admission_digest: str, *, connection_ref: str) -> None:
        self.store = InMemoryApprovalStore()
        self.broker = ApprovalBroker(
            self.store,
            self.store,
            probe=StaticApprovalContext(1, admission_digest),
            connection_ref=connection_ref,
            poll_seconds=0.2,
        )
        self.port: BrokerPermissionBinding = broker_permissions(
            self.broker, wait_seconds=120, reviewers=(DRILL_REVIEWER,), segment_budget_s=600
        )
        self.decided: list[str] = []

    async def review_forever(self, scope: str) -> None:
        service = HumanTaskService(
            InMemoryHumanTaskRepository(),
            request_scope=scope,
            approvals=self.store,
            approval_wake=self.broker.hub,
        )
        while True:
            for task in await self.store.list_tasks(scope, lifecycle="open"):
                await service.resolve(
                    task.human_task_id,
                    answer(task, request_id=f"drill-{task.human_task_id}"),
                    ActorContext(actor_id=DRILL_REVIEWER),
                )
                self.decided.append(task.human_task_id)
            await asyncio.sleep(0.2)


def _harness(
    admission: AuthAdmission,
    factory: RecordingClientFactory,
    workspace: FixtureWorkspace,
    approvals: _Approvals,
) -> ClaudeAgentSdkHarness:
    return compose_claude_local(
        workspaces=workspace,
        projections=RenderedProjectionSource(static_rows(_rows())),
        auth=StaticAuthAdmitter(admission),
        child_environment=provider_child_environment,
        environ=os.environ,
        fences=InMemoryStopFenceRepository(),
        intents=InMemoryHookIntentLedger(),
        settings=ClaudeLaneSettings(require_executables=False, drain_timeout_s=60.0),
        permissions=approvals.port,
        clients=factory,
    )


@pytest.mark.skipif(
    not live_enabled(),
    reason="paid live drill; missing: " + "; ".join(missing_preconditions()),
)
async def test_claude_agent_sdk_live_qualification(tmp_path: Path) -> None:
    spend = Spend(budget_usd=budget_usd())
    outcome = DrillOutcome(spend, approval_url=os.environ.get("MC_LANE_DRILL_APPROVAL_URL"))
    admission = await _admitted(outcome)
    route = outcome.auth["route"]
    factory = RecordingClientFactory(SdkClientFactory(), spend, route)
    workspace = FixtureWorkspace(tmp_path / "leases")
    try:
        await _one_turn_with_subagent_and_permission(admission, factory, workspace, outcome)
        if not spend.exhausted:
            await _interrupt_replace_occupancy_and_resume(admission, factory, workspace, outcome)
        else:
            outcome.measurements["stopped_before"] = "interrupt/replace (budget reached)"
    finally:
        for name, records in factory.recordings.items():
            outcome.recordings.append(str(write_recording(name, records)))
        if spend.sessions and len(factory.recordings) > len(spend.sessions):
            spend.unknown.append("a client was created without a recorded session id")
        record = write_record(outcome)
    assert record.is_file()
    assert not spend.unknown, f"ambiguous paid effects: {spend.unknown}"


async def _one_turn_with_subagent_and_permission(
    admission: AuthAdmission,
    factory: RecordingClientFactory,
    workspace: FixtureWorkspace,
    outcome: DrillOutcome,
) -> None:
    operation = _operation(admission, key="turn")
    approvals = _Approvals(
        operation.effective_configuration_digest, connection_ref="claude-drill#1"
    )
    harness = _harness(admission, factory, workspace, approvals)
    frames = InMemoryFrameStore()
    lanes = lane_stack(harness, frames=frames, operation=operation)
    identity = LaneExecutionIdentity.of(operation, PROFILE, 1)
    heid = str(identity.harness_execution_id)
    harness.stage(heid, operation)
    harness.stage_turn(heid, "drill:turn:1", TURN_ONE)
    reviewer = asyncio.create_task(approvals.review_forever(identity.request_scope))
    watch = Stopwatch()
    try:
        result = await lanes.service.turn(
            LaneTurnRequest(
                operation=operation,
                lane_profile=PROFILE,
                generation=1,
                segment=SEGMENT,
                instruction_ref="drill:turn:1",
            ),
            RecordingSignals(frames, identity.harness_execution_id),
        )
    finally:
        reviewer.cancel()
    outcome.measurements["full_turn_seconds"] = watch.seconds()
    spend = outcome.spend
    spend.turns.append(turn_reference(f"{heid}:1:turn:1"))
    stored = sorted(
        frames._executions[identity.harness_execution_id].frames.values(),
        key=lambda frame: frame.arrival_ordinal,
    )
    kinds = [frame.kind for frame in stored]
    settled = OperationExecutionResult.model_validate(result.operation_result)
    outcome.checks["full_turn_settles_from_closing_facts"] = result.done and settled.status in {
        "completed",
        "failed",
    }
    outcome.observe(
        "start",
        "observed" if FrameKind.SESSION_INIT in kinds else "refuted",
        "system/init session id",
    )
    outcome.observe("send_turn", "observed", "one streamed user message")
    outcome.observe(
        "observe", "observed" if FrameKind.RUN_RESULT in kinds else "refuted", "ResultMessage"
    )
    outcome.observe("end_session", "observed", "disconnect + custody + release after settle")
    requested = [frame for frame in stored if frame.kind is FrameKind.APPROVAL_REQUESTED]
    outcome.observe(
        "approval_suspension",
        "observed" if requested and approvals.decided else "not_exercised",
        f"{len(requested)} can_use_tool request(s) bound; {len(approvals.decided)} decided",
    )
    outcome.settle(
        "can_use_tool_reaches_the_lane",
        "verified" if requested else "open",
        "Bash outside allowed_tools in default permission mode"
        + ("" if requested else " (the model never called Bash)"),
    )
    tasks = [frame for frame in stored if frame.raw_kind.startswith("system.task_")]
    subordinate = [frame for frame in stored if getattr(frame, "subordinate_ref", None)]
    outcome.observe(
        "subordinate_lineage",
        "observed" if tasks else "not_exercised",
        f"{len(tasks)} task lifecycle frame(s); {len(subordinate)} with a subordinate ref",
    )
    outcome.settle(
        "subagent_lifecycle_frames",
        "verified" if tasks else "open",
        "TaskStarted/Progress/Notification messages with tool_use_id",
    )
    facts = result.closing_facts
    usage = facts.usage if facts is not None else None
    outcome.observe(
        "usage",
        "observed" if usage is not None and usage.disposition == "settled" else "refuted",
        f"tokens {usage.total_tokens if usage else 'unknown'}; cost "
        f"{facts.cost_disposition if facts else 'unknown'}",
    )
    outcome.settle(
        "cost_is_an_estimate",
        "open",
        f"total_cost_usd summed {round(spend.estimated_cost_usd, 6)}; compare with the Console",
    )
    sent_uuid = turn_reference(f"{heid}:1:turn:1")
    recorded = json.dumps(list(factory.recordings.values()))
    outcome.settle(
        "client_uuid_is_turn_identity",
        "verified" if sent_uuid in recorded else "open",
        "the stamped user-message uuid "
        + ("appears" if sent_uuid in recorded else "does not appear")
        + " in the recorded stream",
    )


async def _interrupt_replace_occupancy_and_resume(
    admission: AuthAdmission,
    factory: RecordingClientFactory,
    workspace: FixtureWorkspace,
    outcome: DrillOutcome,
) -> None:
    operation = _operation(admission, key="interrupt")
    approvals = _Approvals(
        operation.effective_configuration_digest, connection_ref="claude-drill#2"
    )
    harness = _harness(admission, factory, workspace, approvals)
    identity = LaneExecutionIdentity.of(operation, PROFILE, 1)
    heid = str(identity.harness_execution_id)
    harness.stage(heid, operation)
    prepared = await harness.prepare(
        PrepareRequest(
            **_fields(operation, "prepare"),
            run_id=operation.identity.run_id,
            operation_id=operation.identity.operation_id,
            attempt_no=1,
        )
    )
    session = await harness.start(StartRequest(**_fields(operation, "turn:1"), prepared=prepared))
    harness.stage_turn(heid, "drill:long", LONG_TURN)
    harness.stage_turn(heid, "drill:replace", REPLACEMENT)
    first = await harness.send_turn(
        SendTurnRequest(
            **_fields(operation, "turn:1"), session=session, turn_no=1, instruction_ref="drill:long"
        )
    )
    outcome.spend.turns.append(str(first.native_turn_ref))
    await asyncio.sleep(3)
    drained: list[Any] = []

    async def on_frame(frame: Any) -> None:
        drained.append(frame)

    async def unsettled() -> tuple[str, ...]:
        return ()

    watch = Stopwatch()
    replaced = await cancel_and_replace_turn(
        harness,
        cancel=CancelTurnRequest(
            **_fields(operation, "cancel"), turn=first, reason="interrupt_and_inject"
        ),
        replacement=SendTurnRequest(
            **_fields(operation, "replace:1"),
            session=session,
            turn_no=2,
            instruction_ref="drill:replace",
        ),
        unsettled=unsettled,
        on_frame=on_frame,
    )
    outcome.measurements["interrupt_to_replacement_seconds"] = watch.seconds()
    outcome.spend.turns.append(str(replaced.handle.native_turn_ref))
    terminal = drained[-1].body if drained and isinstance(drained[-1].body, dict) else {}
    reason = str(terminal.get("terminal_reason"))
    outcome.observe(
        "cancel_turn",
        "observed" if reason.startswith("aborted") else "refuted",
        f"drained terminal_reason={reason}",
    )
    outcome.settle(
        "interrupt_terminal_reason",
        "verified" if reason.startswith("aborted") else "refuted",
        f"the interrupted turn's ResultMessage.terminal_reason was {reason}",
    )
    turn2 = TurnHandle(session=session, turn_no=2, native_turn_ref=replaced.handle.native_turn_ref)
    final = [
        frame
        async for frame in harness.observe(
            ObserveRequest(**_fields(operation, "observe"), turn=turn2)
        )
    ]
    facts = harness.closing_facts(turn2, final[-1])
    clean = all("1\n2\n3" not in json.dumps(frame.body) for frame in final)
    outcome.checks["replacement_reads_the_intended_response"] = (
        "REPLACED-OK" in facts.result_excerpt and clean
    )
    occupancy = await harness.context_occupancy(heid, session, turn2)
    known = occupancy is not None and occupancy.known
    outcome.observe(
        "context_occupancy",
        "observed" if known else "refuted",
        f"get_context_usage: {occupancy.used_tokens}/{occupancy.window_tokens}"
        if known and occupancy is not None
        else f"unknown ({occupancy.reason if occupancy is not None else 'none'})",
    )
    outcome.settle(
        "context_usage_control_request",
        "verified" if known else "refuted",
        "totalTokens against rawMaxTokens at the turn boundary",
    )
    if outcome.spend.exhausted:
        outcome.measurements["stopped_before"] = "resume after process death (budget reached)"
        await harness.end_session(
            EndSessionRequest(**_fields(operation, "end"), session=session, reason="drill")
        )
        return
    # Simulated process death: the SDK subprocess goes away, the lease and the mirrored
    # history under it stay (no end_session, no release).
    execution = harness.execution_view(heid)
    assert execution is not None and execution.session is not None
    await execution.session.close()
    await _resume_after_process_death(admission, factory, workspace, operation, session, outcome)


async def _resume_after_process_death(
    admission: AuthAdmission,
    factory: RecordingClientFactory,
    workspace: FixtureWorkspace,
    operation: OperationExecutionRequest,
    session: SessionHandle,
    outcome: DrillOutcome,
) -> None:
    """A new harness (a new process's composition) over the same lease registry: MP-11
    `recover` runs, then the session resumes by id (`resume=`) as a new connection."""

    approvals = _Approvals(
        operation.effective_configuration_digest, connection_ref="claude-drill#3"
    )
    second = _harness(admission, factory, workspace, approvals)
    identity = LaneExecutionIdentity.of(operation, PROFILE, 1)
    heid = str(identity.harness_execution_id)
    second.stage(heid, operation)
    handle = await second.reattach(
        ReattachRequest(
            **_fields(operation, "turn:3"), native_session_ref=session.native_session_ref
        )
    )
    resumed = handle.native_session_ref == session.native_session_ref
    second.stage_turn(heid, "drill:resumed", RESUMED)
    turn = await second.send_turn(
        SendTurnRequest(
            **_fields(operation, "turn:3"),
            session=handle,
            turn_no=3,
            instruction_ref="drill:resumed",
        )
    )
    outcome.spend.turns.append(str(turn.native_turn_ref))
    frames = [
        frame
        async for frame in second.observe(
            ObserveRequest(**_fields(operation, "observe"), turn=turn)
        )
    ]
    facts = second.closing_facts(turn, frames[-1])
    ok = resumed and "RESUMED-OK" in facts.result_excerpt
    outcome.observe(
        "reattach",
        "observed" if ok else "refuted",
        "resume=<session_id> into a new connection after a simulated process death",
    )
    outcome.settle(
        "resume_after_process_death",
        "verified" if ok else "refuted",
        "the mirrored history under the lease resumed the session; a new generation",
    )
    outcome.checks["resume_after_process_death"] = ok
    await second.end_session(
        EndSessionRequest(**_fields(operation, "end"), session=handle, reason="drill")
    )
