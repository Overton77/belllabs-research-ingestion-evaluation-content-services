"""MP-08 live qualification drill for the `codex` lane (paid, owner-run; skipped unless enabled).

Runs only with every precondition of `live_drill.missing_preconditions()`:
`MC_LIVE_CODEX_QUALIFICATION=1`, a finite owner-approved `MC_PAID_BUDGET_USD`, an explicit
`MC_CODEX_AUTH_ROUTE` (`api_key` or `owner_cli_login`; no default), the owner's auth profile
document and profile id (`MISSION_CONTROL_AUTH_PROFILES_PATH`, `MC_CODEX_AUTH_PROFILE`), for
the login route the owner's `CODEX_HOME` (`MC_CODEX_OWNER_HOME`), and a Linux/WSL worker with
the pinned `codex-cli 0.162.0` (`MC_CODEX_BINARY` when it is not `codex`). `make lane-qualify
PROFILE=codex LIVE=1` sets the flag; CI never does. NOT RUN by MP-08 (no paid call was
authorized); it is the runner the owner executes.

It drives the real lane through the production composition (`compose_codex_local`, the
production `SubprocessAppServerLauncher` with its version pin behind a recording tee, MP-05
admission, MP-11 `ApprovalBroker` over the in-memory stores with the owner's scripted
decisions, recorded as such), at most `MAX_TURNS` paid turns on a disposable repository:

1. session A, one turn through `LaneTurnService` with `approvalPolicy=untrusted`: start,
   approval requests bound before any answer (one declined, then approved), a project
   subagent (`.codex/agents/`) whose child thread must surface as `subordinate_ref` frames,
   settled usage (README drill steps 1-3);
2. session B, direct harness calls: a long turn steered with the exact `expectedTurnId`, then
   interrupted with the terminal completion observed; a late steer's refusal shape; the
   occupancy against `modelContextWindow`; an explicit `thread/compact/start` and its report
   (step 5);
3. session B, restart classification: SIGTERM to the app-server mid-turn, `reattach`
   (relaunch + MP-11 `recover` + `thread/resume`), what Codex did with the turn, and whether
   `UserMessageThreadItem.clientId` echoes `clientUserMessageId` (step 4).

It stops before a turn once `MAX_TURNS` were started, records every app-server event
(scrubbed) and writes `docs/qualification/lanes/codex-<date>.md` with the observed capability
matrix. An ambiguous paid effect is `unknown` and the drill never qualifies then.
"""

from __future__ import annotations

import asyncio
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from mission_control.adapters.codex.compose import AdmissionServiceSource, compose_codex_local
from mission_control.adapters.codex.harness import CodexLocalHarness
from mission_control.adapters.codex.launcher import SubprocessAppServerLauncher
from mission_control.adapters.cursor.workspace import GitWorktreeLeaser
from mission_control.application.agentic_components.materialization import projection_digest
from mission_control.application.context.lane_support import CompactionRequest
from mission_control.application.execution.harness.dispatch import DispatchRecord
from mission_control.application.execution.harness.lane_turns import (
    LaneExecutionIdentity,
    harness_scope,
)
from mission_control.application.execution.harness.leases import InMemoryWorkspaceLeaseStore
from mission_control.bootstrap.provider_auth import (
    compose_auth_admission,
    provider_child_environment,
)
from mission_control.bootstrap.settings import Settings
from mission_control.domain.agentic_components.projection import (
    HostProjection,
    ProjectedFile,
    ProjectionReport,
)
from mission_control.domain.execution.contracts import (
    OperationAttemptIdentity,
    OperationExecutionRequest,
)
from mission_control.domain.execution.lane_turns import LaneTurnRequest
from mission_control.domain.execution.lanes import (
    CancelTurnRequest,
    EndSessionRequest,
    LaneFrame,
    ObserveRequest,
    PrepareRequest,
    ReattachRequest,
    SendTurnRequest,
    SessionHandle,
    StartRequest,
    StatusRequest,
    TurnHandle,
)
from mission_control.domain.frames.contracts import FrameKind, LaneProfile
from tests.fixtures.lane_turns import RecordingSignals, lane_stack
from tests.integration.codex.live_drill import (
    DrillOutcome,
    RecordingLauncher,
    Spend,
    Stopwatch,
    auth_route,
    budget_usd,
    codex_binary,
    disposable_repository,
    live_enabled,
    missing_preconditions,
    write_record,
    write_recording,
)
from tests.unit.codex.support import (
    MemoryArtifacts,
    StaticProjectionSource,
    approval_rig,
    codex_binding,
    codex_operation,
)

MODEL = os.environ.get("MC_CODEX_MODEL", "") or "gpt-5-codex"
TURN_A = (
    "Run the shell command `sh check.sh`, then create the file outputs/drill.txt containing "
    "the word ok. Then ask the `mc-drill-helper` agent to summarize README.md in one line. "
    "Keep your final answer to one sentence."
)
TURN_B_LONG = (
    "Run this exact shell command and report the last number it printed: "
    "for i in $(seq 1 120); do echo $i; sleep 1; done"
)
STEER = "Change of plan: stop as soon as you can and reply with the single word STEERED."
TURN_B_RESTART = (
    "Run this exact shell command: for i in $(seq 1 30); do echo $i >> outputs/count.txt; "
    "sleep 1; done. Then say DONE."
)


def drill_projection() -> HostProjection:
    """AGENTS.md, a project config and one project subagent (the drill's own files)."""

    return HostProjection(
        profile=LaneProfile.CODEX,
        files=(
            ProjectedFile(
                path="AGENTS.md",
                content=b"# Mission (qualification drill)\n\nKeep every answer short.\n",
            ),
            ProjectedFile(
                path=".codex/config.toml",
                content=b"# Generated by Mission Control Host Projection (drill).\n",
            ),
            ProjectedFile(
                path=".codex/agents/mc-drill-helper.toml",
                content=(
                    b'name = "mc-drill-helper"\n'
                    b'description = "Summarizes one file in one line."\n'
                    b'developer_instructions = "Summarize the named file in one line."\n'
                ),
            ),
        ),
        report=ProjectionReport(requires_trust=("codex.project_agents",)),
        send_options={},
    )


async def _admission(outcome: DrillOutcome) -> AdmissionServiceSource:
    route = auth_route()
    assert route is not None
    settings = Settings(
        _env_file=None,
        mission_control_auth_profiles_path=Path(os.environ["MISSION_CONTROL_AUTH_PROFILES_PATH"]),
    )
    service = compose_auth_admission(settings)
    assert service is not None, "the owner's auth profile document is required"
    admission = await service.admit("codex", os.environ["MC_CODEX_AUTH_PROFILE"])
    # The explicit flag must name the route MP-05 admitted: no silent route substitution.
    assert admission.route == route, f"admitted {admission.route}, the flag says {route}"
    outcome.auth = {
        "route": admission.route,
        "route_support": admission.route_support,
        "billing_mode": admission.billing_mode,
        "profile_id": admission.profile_id,
        "credential_ref": admission.credential_ref or "none (the CLI's own login)",
        "env_unset": ",".join(admission.env_unset) or "none",
        "observation_source": admission.observation_source,
    }
    return AdmissionServiceSource(service)


def _operation(key: str, prompt: str, billing_mode: str) -> OperationExecutionRequest:
    binding = codex_binding(
        model={"profile": "codex.default", "model_id": MODEL},
        auth={"profile": os.environ["MC_CODEX_AUTH_PROFILE"], "billing_mode": billing_mode},
        materialization_digest=projection_digest(drill_projection()),
        provider_options={
            "provider": "codex_app_server",
            "approval_policy": "untrusted",
            "sandbox_mode": "workspace-write",
            "app_server_schema_version": "v2@0.162.0",
        },
    )
    base = codex_operation(binding, prompt=prompt)
    workspace = base.workspace.model_copy(update={"exclusive_write_paths": ()})
    return codex_operation(
        binding,
        prompt=prompt,
        identity=OperationAttemptIdentity(
            run_id="run-codex-drill", operation_id=f"codex-drill-{key}", operation_attempt=1
        ),
        workspace=workspace.model_dump(mode="python"),
    )


def _fields(operation: OperationExecutionRequest, key: str) -> dict[str, Any]:
    identity = LaneExecutionIdentity.of(operation, "codex", 1)
    return {
        "scope": harness_scope(operation.request_scope),
        "lane_profile": "codex",
        "harness_execution_id": str(identity.harness_execution_id),
        "binding_digest": operation.effective_configuration_digest,
        "idempotency_key": f"{identity.harness_execution_id}:1:{key}",
        "generation": 1,
    }


async def _session(harness: CodexLocalHarness, operation: OperationExecutionRequest) -> Any:
    fields = _fields(operation, "drill")
    harness.stage(fields["harness_execution_id"], operation)
    prepared = await harness.prepare(
        PrepareRequest(
            **fields,
            run_id=operation.identity.run_id,
            operation_id=operation.identity.operation_id,
            attempt_no=1,
        )
    )
    return await harness.start(StartRequest(**fields, prepared=prepared))


async def _turn(
    harness: CodexLocalHarness,
    operation: OperationExecutionRequest,
    session: SessionHandle,
    key: str,
    text: str,
    spend: Spend,
    turn_no: int,
) -> TurnHandle | None:
    if spend.exhausted:
        return None
    fields = _fields(operation, key)
    ref = f"drill:{key}"
    harness.stage_turn(fields["harness_execution_id"], ref, text)
    turn = await harness.send_turn(
        SendTurnRequest(**fields, session=session, turn_no=turn_no, instruction_ref=ref)
    )
    if turn.native_turn_ref is not None:
        spend.turns_started.append(turn.native_turn_ref)
    return turn


async def _observe(
    harness: CodexLocalHarness,
    operation: OperationExecutionRequest,
    turn: TurnHandle,
    *,
    after: str | None = None,
    until: Any = None,
) -> list[LaneFrame]:
    frames: list[LaneFrame] = []
    request = ObserveRequest(**_fields(operation, "observe"), turn=turn, after=after)
    async for frame in harness.observe(request):
        frames.append(frame)
        if frame.terminal or (until is not None and until(frame)):
            break
    return frames


@pytest.mark.skipif(
    not live_enabled(),
    reason="paid live drill (owner-run): " + "; ".join(missing_preconditions()),
)
async def test_codex_live_qualification(tmp_path: Path) -> None:
    spend = Spend(budget_usd=budget_usd())
    outcome = DrillOutcome(spend)
    auth = await _admission(outcome)
    repository = disposable_repository(tmp_path / "repo")
    lease_root = tmp_path / "leases"
    launcher = RecordingLauncher(SubprocessAppServerLauncher(codex_binary()))
    operation_a = _operation("a", TURN_A, outcome.auth["billing_mode"])
    rig = approval_rig(
        operation_a.effective_configuration_digest,
        decisions=["deny", *(["approve"] * 12)],
    )
    owner_home = os.environ.get("MC_CODEX_OWNER_HOME")
    harness = compose_codex_local(
        leaser=GitWorktreeLeaser(InMemoryWorkspaceLeaseStore(), lease_root=lease_root),
        projections=StaticProjectionSource(drill_projection()),
        artifacts=MemoryArtifacts(),
        auth=auth,
        lease_root=lease_root,
        launcher=launcher,
        codex_home_mode="owner" if outcome.auth["route"] == "owner_cli_login" else "isolated",
        owner_codex_home=Path(owner_home) if owner_home else None,
        default_repository=str(repository),
        child_environment=provider_child_environment,
        broker=rig.broker,
        approval_wait_seconds=120.0,
    )
    try:
        async with asyncio.timeout(1_800):
            await _session_a(harness, operation_a, outcome, spend)
            _collab_fields(launcher, outcome)
            await _session_b(harness, outcome, spend)
    finally:
        outcome.recordings.append(str(write_recording("drill", launcher.records())))
        record = write_record(outcome)
    assert record.is_file()
    assert not spend.unknown, f"ambiguous paid effects: {spend.unknown}"


async def _session_a(
    harness: CodexLocalHarness,
    operation: OperationExecutionRequest,
    outcome: DrillOutcome,
    spend: Spend,
) -> None:
    """One full turn through `LaneTurnService`: start, approvals, subagent, usage."""

    lanes = lane_stack(harness, operation=operation)
    watch = Stopwatch()
    result = await lanes.service.turn(
        LaneTurnRequest(operation=operation, lane_profile="codex", generation=1),
        RecordingSignals(lanes.frames),
    )
    while not result.done:
        result = await lanes.service.turn(
            LaneTurnRequest(
                operation=operation,
                lane_profile="codex",
                generation=1,
                phase="resume",
                cursor=result.cursor,
                segment_no=result.segment_no + 1,
            ),
            RecordingSignals(lanes.frames),
        )
    outcome.measurements["turn_a_seconds"] = watch.seconds()
    if result.native.turn_ref is not None:
        spend.turns_started.append(result.native.turn_ref)
    else:
        spend.unknown.append("turn A ran without a recorded native turn id")
    frames = [
        frame
        for execution in lanes.frames._executions.values()
        for frame in execution.frames.values()
    ]
    outcome.checks["turn_a_settles_from_closing_facts"] = result.done
    outcome.observe("start", "native", f"thread {result.native.session_ref}")
    outcome.observe("send_turn", "native", f"turn {result.native.turn_ref}")
    outcome.observe("observe", "native", f"{len(frames)} frames stored")
    facts = result.closing_facts
    if facts is not None and facts.usage.disposition != "unknown":
        spend.estimated_tokens += facts.usage.total_tokens
        outcome.observe("usage", "native", f"last-call tokens {facts.usage.total_tokens}")
    served = harness.approvals.served
    statuses = [item.status for record in served for item in record.outcomes]
    bound_first = all(record.bound for record in served if record.refused is None)
    outcome.checks["approvals_bound_before_answer"] = bool(served) and bound_first
    outcome.observe(
        "approval_suspension",
        "native" if {"denied", "approved"} <= set(statuses) else "failed",
        f"{len(served)} server requests; outcomes {sorted(set(statuses))}",
    )
    outcome.settle(
        "approval_rpc_coverage",
        "verified" if served else "refuted",
        "methods: " + ", ".join(sorted({record.method for record in served}) or ["none"]),
    )
    children = {frame.subordinate_ref for frame in frames if frame.subordinate_ref is not None}
    outcome.observe(
        "subordinate_visibility",
        "native" if children else "failed",
        f"child threads: {sorted(children)[:5]}",
    )
    outcome.settle(
        "project_trust",
        "verified" if children else "open",
        "the project subagent from .codex/agents/ ran"
        if children
        else "no project subagent observed (trust or agent loading not shown)",
    )
    outcome.checks["approval_frames_observed"] = any(
        frame.kind == FrameKind.APPROVAL_RESOLVED for frame in frames
    )


def _collab_fields(launcher: RecordingLauncher, outcome: DrillOutcome) -> None:
    """MP-13 unresolved 3: the `collabAgentToolCall` item's field names on this pin."""

    items = [
        row["params"]["item"]
        for row in launcher.records()
        if row.get("method") in {"item/started", "item/completed"}
        and isinstance(row.get("params", {}).get("item"), dict)
        and row["params"]["item"].get("type") == "collabAgentToolCall"
    ]
    names = sorted({str(key) for item in items for key in item})
    outcome.settle(
        "collab_agent_tool_call_fields",
        "verified"
        if items and {"senderThreadId", "receiverThreadIds"} <= set(names)
        else ("refuted" if items else "open"),
        f"collabAgentToolCall fields: {names[:12]}",
    )


async def _session_b(harness: CodexLocalHarness, outcome: DrillOutcome, spend: Spend) -> None:
    """Steer, interrupt, occupancy, compaction and restart classification (direct calls)."""

    operation = _operation("b", TURN_B_LONG, outcome.auth["billing_mode"])
    session = await _session(harness, operation)
    heid = _fields(operation, "b")["harness_execution_id"]
    long_turn = await _turn(harness, operation, session, "long", TURN_B_LONG, spend, 1)
    if long_turn is None or long_turn.native_turn_ref is None:
        spend.unknown.append("the long turn was not started (busy or budget)")
        return
    started = await _observe(
        harness,
        operation,
        long_turn,
        until=lambda frame: (
            frame.raw_kind == "item/started"
            and isinstance(frame.body, dict)
            and (frame.body.get("item") or {}).get("type") == "commandExecution"
        ),
    )
    harness.stage_turn(heid, "drill:steer", STEER)
    steered = await harness.steer(long_turn, instruction_ref="drill:steer")
    spend.steers += 1
    outcome.observe("steer", "native" if steered.outcome == "applied" else "failed", steered.detail)
    watch = Stopwatch()
    receipt = await harness.cancel_turn(
        CancelTurnRequest(**_fields(operation, "cancel"), turn=long_turn, reason="command")
    )
    outcome.measurements["interrupt_to_terminal_seconds"] = watch.seconds()
    outcome.observe(
        "cancel_turn",
        "native" if receipt.native_status == "interrupted" else "failed",
        f"acknowledged={receipt.acknowledged} native_status={receipt.native_status}",
    )
    rest = await _observe(
        harness, operation, long_turn, after=started[-1].cursor if started else None
    )
    outcome.checks["interrupted_turn_reaches_terminal"] = bool(rest) and rest[-1].terminal
    late = await harness.steer(long_turn, instruction_ref="drill:steer")
    outcome.settle(
        "steer_refusal_shape",
        "verified" if late.outcome == "stale_target" else "refuted",
        f"steer after completion: {late.outcome}: {late.detail}",
    )

    occupancy = await harness.context_occupancy(heid, session, long_turn)
    staged = harness._sessions[heid]
    last = staged.last_usage
    outcome.observe(
        "context_occupancy",
        "native" if occupancy is not None and occupancy.known else "failed",
        f"{occupancy}"[:200],
    )
    outcome.settle(
        "occupancy_reading",
        "open",
        (
            f"last.totalTokens={last.last.total_tokens} "
            f"total.totalTokens={last.total.total_tokens} "
            f"modelContextWindow={last.model_context_window}; compare with the CLI's meter"
            if last is not None
            else "no thread/tokenUsage/updated observed"
        ),
    )
    compaction = await harness.compact(
        CompactionRequest(harness_execution_id=heid, session=session, turn=long_turn, epoch=1)
    )
    spend.compactions += 1
    outcome.observe(
        "compaction",
        "native" if compaction.completed else "failed",
        f"{compaction.native_ref}: {compaction.detail}",
    )
    outcome.settle(
        "compaction_report",
        "verified" if compaction.completed else "refuted",
        compaction.detail,
    )

    restart_turn = await _turn(harness, operation, session, "restart", TURN_B_RESTART, spend, 2)
    if restart_turn is None or restart_turn.native_turn_ref is None:
        spend.unknown.append("the restart turn was not started (busy or budget)")
        return
    await asyncio.sleep(5)
    launched = staged.launched
    assert launched is not None and launched.process is not None
    launched.process.terminate()  # SIGTERM to the app-server mid-turn
    await asyncio.sleep(2)
    fields = _fields(operation, "reattach")
    reattached = await harness.reattach(
        ReattachRequest(
            **fields,
            native_session_ref=session.native_session_ref,
            native_turn_ref=restart_turn.native_turn_ref,
        )
    )
    outcome.observe(
        "reattach",
        "emulated",
        f"relaunch + thread/resume; recovered correlations {len(harness.approvals.recovered)}",
    )
    status = await harness.status(StatusRequest(**fields, session=reattached, turn=restart_turn))
    outcome.measurements["restart_turn_status_after_resume"] = status.status
    outcome.settle(
        "history_after_restart",
        "verified",
        f"thread/read after the relaunch has turn {restart_turn.native_turn_ref}: {status.status}",
    )
    send_key = _fields(operation, "restart")["idempotency_key"]
    staged.sent.pop(send_key, None)  # force the history lookup (the drill's only poke)
    lookup = await harness.reconcile_dispatch(
        DispatchRecord(
            kind="send",
            idempotency_key=send_key,
            expected_generation=1,
            instruction_digest="sha256:" + "0" * 64,
            owner_ref="codex-drill",
            owner_epoch=1,
            intended_at=datetime.now(UTC),
        ),
        session=reattached,
    )
    outcome.observe(
        "reconcile_dispatch",
        "native" if lookup.outcome == "found" else "failed",
        lookup.detail or lookup.outcome,
    )
    outcome.settle(
        "client_user_message_id_echo",
        "verified" if lookup.outcome == "found" else "refuted",
        f"history lookup by clientId: {lookup.outcome} {lookup.detail}",
    )
    outcome.settle(
        "limit_error_data",
        "open",
        "the drill never provokes a limit; settled only by an observed refusal",
    )
    await harness.end_session(EndSessionRequest(**fields, session=reattached, reason="drill"))


def test_the_drill_preconditions_and_record_offline(tmp_path: Path) -> None:
    """Offline (no provider): the refusal names every missing precondition, and a record
    without checks is never `qualified`."""

    missing = missing_preconditions({})
    assert "MC_LIVE_CODEX_QUALIFICATION=1" in missing
    assert any(item.startswith("MC_PAID_BUDGET_USD") for item in missing)
    assert any(item.startswith("MC_CODEX_AUTH_ROUTE") for item in missing)
    outcome = DrillOutcome(Spend(budget_usd=1.0))
    outcome.observe("start", "native", "fixture evidence only")
    outcome.settle("compaction_report", "open", "not exercised")
    record = write_record(outcome, root=tmp_path)
    text = record.read_text(encoding="utf-8")
    assert "outcome: not_qualified" in text and "evidence: live_drill" in text
    assert "| start | native | fixture evidence only |" in text
    assert "| compaction_report | open | not exercised |" in text
    with pytest.raises(ValueError):
        outcome.observe("fork", "native", "not a matrix control")
