"""C2: fact derivation from closing frames, 05 section 3.5 mapping, usage dispositions,
determinism, and the reducer's `apply_frame_facts` action (SPEC-03 lifecycle synthesis)."""

from __future__ import annotations

import json
import random
from typing import Any
from uuid import UUID

import pytest

from mission_control.application.frames.kinds import classify
from mission_control.application.frames.reducer import (
    CompletionEvidence,
    DeriveContext,
    FrameFactProjector,
    FrameFactTarget,
    derive,
    map_execution_outcome,
)
from mission_control.application.frames.sink import InMemoryFrameStore
from mission_control.domain.frames.contracts import FrameKind, LaneProfile, native_event_ref
from mission_control.domain.frames.facts import (
    ApprovalRequestedFact,
    AttemptOutcome,
    ExecutionOutcomeFact,
    FailureClass,
    SessionStartedFact,
    ToolEffectFact,
    TurnCompletedFact,
    TurnStartedFact,
    UsageSettledFact,
    fact_events,
)
from mission_control.domain.frames.usage import UsageDisposition, render_dimension, usage_report
from mission_control.domain.policies.contracts import (
    ApplyFrameFactsAction,
    CommandStatus,
    LifecycleCommand,
    StartAction,
)
from mission_control.domain.policies.reducer import (
    ReductionRejected,
    reduce_lifecycle,
    required_action_permissions,
)
from tests.fixtures.provider_frames import (
    FIXTURE_ACTIVATION,
    FIXTURE_HARNESS,
    SCOPE,
    StepClock,
    deep_agents_turn_frames,
    harness_start,
    in_memory_store,
    provider_frame,
    turn_observations,
)
from tests.unit.run_control.test_run_control import ALL_PERMISSIONS, actor, command, request
from tests.unit.run_control.test_run_control import service as run_control_service

K = FrameKind
DA = DeriveContext(lane=LaneProfile.DEEP_AGENTS, current_generation=1)
COVERED = CompletionEvidence(
    declared_outputs=frozenset({"sources"}), registered_outputs=frozenset({"sources"})
)


def kinds(facts: Any) -> list[str]:
    return [fact.fact for fact in facts]


def test_derive_reads_only_closing_frames_and_deltas_change_nothing() -> None:
    frames = deep_agents_turn_frames()
    closing_only = [frame for frame in frames if frame.closing]
    noise = [frame for frame in frames if not frame.closing]
    extra = [
        provider_frame(100 + index, kind, {"noise": index})
        for index, kind in enumerate(
            [K.MESSAGE_DELTA, K.THINKING_DELTA, K.TOOL_CALL_DELTA, K.STATUS, K.HEARTBEAT, K.UNKNOWN]
        )
    ]
    baseline = derive(closing_only, DA)
    shuffled = [*frames, *extra]
    random.Random(7).shuffle(shuffled)
    assert derive(shuffled, DA) == baseline
    assert derive([*noise, *extra], DA) == ()
    assert kinds(baseline) == [
        "session_started",
        "turn_started",
        "tool_effect",
        "turn_completed",
        "execution_outcome",
        "session_ended",
    ]


def test_deep_agents_rows_of_the_lifecycle_synthesis_table() -> None:
    context = DeriveContext(lane=LaneProfile.DEEP_AGENTS, current_generation=1, completion=COVERED)
    facts = derive(deep_agents_turn_frames(), context)
    by_kind = {fact.fact: fact for fact in facts}
    started = by_kind["session_started"]
    assert isinstance(started, SessionStartedFact) and started.arrival_ordinal == 6
    turn = by_kind["turn_started"]
    assert isinstance(turn, TurnStartedFact) and turn.turn_ordinal == 1
    tool = by_kind["tool_effect"]
    assert isinstance(tool, ToolEffectFact)
    assert (tool.tool_call_ref, tool.status, tool.name) == ("call-1", "completed", "read_file")
    assert tool.result_digest == "sha256:" + "1" * 64
    completed = by_kind["turn_completed"]
    assert isinstance(completed, TurnCompletedFact)
    assert completed.usage.value("input_tokens") == 8
    assert completed.usage.dimensions["output_tokens"].disposition == UsageDisposition.SETTLED
    assert completed.usage.dimensions["cost_micros"].disposition == UsageDisposition.UNKNOWN
    assert completed.stop_reason == "end_turn" and len(completed.usage_frame_ids) == 2
    outcome = by_kind["execution_outcome"]
    assert isinstance(outcome, ExecutionOutcomeFact)
    assert outcome.outcome == AttemptOutcome.SUCCEEDED
    assert kinds(facts)[-1] == "session_ended"
    events = [
        event_type
        for fact in facts
        for event_type, _payload in fact_events(
            fact, lane_profile=LaneProfile.DEEP_AGENTS, activation_id="a", attempt_no=1
        )
    ]
    assert events == [
        "session.started",
        "session.turn_started",
        "tool_call.completed",
        "session.turn_completed",
        "attempt.completed",
        "session.ended",
    ]


def test_tool_failures_denials_approvals_and_compaction_map_per_the_table() -> None:
    frames = [
        provider_frame(
            1,
            K.TOOL_CALL_FAILED,
            {"tool_call_id": "c1", "status": "error", "name": "shell", "exit_code": 2},
            tool_call_ref="c1",
        ),
        provider_frame(
            2,
            K.HOOK_RESULT,
            {"decision": "deny", "event": "preToolUse", "tool_call_id": "c2", "tool_name": "rm"},
        ),
        provider_frame(3, K.SESSION_STATE, {"state": "requires_action", "request_ref": "int-1"}),
        provider_frame(4, K.APPROVAL_RESOLVED, {"request_id": "int-1", "decision": "approve"}),
        provider_frame(5, K.AFTER_COMPACTION, {"summary_digest": "sha256:" + "5" * 64}),
        provider_frame(6, K.HOOK_RESULT, {"decision": "allow", "tool_call_id": "c3"}),
        provider_frame(
            7,
            K.TOOL_CALL_COMPLETED,
            {"tool_call_id": "c1", "status": "success"},
            tool_call_ref="c1",
        ),
        provider_frame(8, K.ERROR, {"unknown_state": True, "reason": "stream_gap"}),
    ]
    facts = derive(frames, DA)
    tools = [fact for fact in facts if isinstance(fact, ToolEffectFact)]
    assert [(fact.tool_call_ref, fact.status) for fact in tools] == [
        ("c1", "failed"),
        ("c2", "denied"),
    ], "a later duplicate completion of c1 is ignored; an allow decision is no effect"
    assert tools[0].exit_code == 2
    approval = next(fact for fact in facts if isinstance(fact, ApprovalRequestedFact))
    assert approval.request_ref == "int-1"
    assert kinds(facts) == [
        "session_started",
        "turn_started",
        "tool_effect",
        "tool_effect",
        "approval_requested",
        "approval_resolved",
        "compaction_observed",
        "unit_in_doubt",
    ]
    events = [
        (event_type, payload.get("phase"), payload.get("status"))
        for fact in facts
        for event_type, payload in fact_events(
            fact, lane_profile=LaneProfile.DEEP_AGENTS, activation_id="a", attempt_no=1
        )
    ]
    assert ("activation.phase_changed", "awaiting_human", None) in events
    assert ("activation.phase_changed", "executing", None) in events
    assert ("tool_call.completed", None, "denied") in events
    assert ("session.compaction_observed", None, None) in events
    assert ("activation.lifecycle_changed", None, None) in events


def _cursor(lane: LaneProfile, ordinal: int, raw_kind: str, body: dict[str, Any], **kw: Any) -> Any:
    kind = classify(lane, raw_kind, body, counter=None).kind
    return provider_frame(ordinal, kind, body, raw_kind=raw_kind, lane=lane, turn=None, **kw)


@pytest.mark.parametrize("lane", [LaneProfile.CURSOR_LOCAL, LaneProfile.CURSOR_CLOUD])
def test_cursor_frame_shapes_derive_without_reducer_changes(lane: LaneProfile) -> None:
    started = "send.accepted" if lane == LaneProfile.CURSOR_LOCAL else "run.created"
    turn_end = "TurnEndedUpdate" if lane == LaneProfile.CURSOR_LOCAL else "interaction_update"
    frames = [
        _cursor(lane, 1, "agent.created", {"agentId": "bc-1"}),
        _cursor(lane, 2, started, {"runId": "run-1"}),
        _cursor(lane, 3, "assistant", {"text": "On it"}),
        _cursor(lane, 4, "tool_call", {"callId": "t1", "name": "edit", "status": "running"}),
        _cursor(
            lane,
            5,
            "tool_call",
            {"callId": "t1", "name": "edit", "status": "completed", "result": "ok"},
        ),
        _cursor(lane, 6, "session_state", {"state": "requires_action", "request_id": "r-9"}),
        _cursor(
            lane,
            7,
            turn_end,
            {"type": "turn-ended", "inputTokens": 120, "outputTokens": 30, "cost_micros": 4200},
        ),
        _cursor(
            lane,
            8,
            "result" if lane == LaneProfile.CURSOR_CLOUD else "RunResult",
            {"status": "finished"},
        ),
        _cursor(lane, 9, "usage", {"inputTokens": 120, "outputTokens": 30, "charged_cents": 1}),
    ]
    context = DeriveContext(lane=lane, current_generation=1, completion=COVERED)
    facts = derive(frames, context)
    assert kinds(facts) == [
        "session_started",
        "turn_started",
        "tool_effect",
        "approval_requested",
        "turn_completed",
        "execution_outcome",
        "usage_settled",
    ]
    completed = next(fact for fact in facts if isinstance(fact, TurnCompletedFact))
    assert completed.usage.value("input_tokens") == 120
    cost = completed.usage.dimensions["cost_micros"]
    assert cost.disposition == UsageDisposition.ESTIMATED and cost.value == 4200
    settled = next(fact for fact in facts if isinstance(fact, UsageSettledFact))
    assert settled.turn_ordinal == 1
    assert settled.usage.dimensions["cost_micros"].disposition == UsageDisposition.SETTLED
    assert settled.usage.value("cost_micros") == 10_000
    events = [
        event_type
        for fact in facts
        for event_type, _payload in fact_events(
            fact, lane_profile=lane, activation_id="a", attempt_no=1
        )
    ]
    assert events[:2] == ["session.started", "activation.phase_changed"], "Cursor: executing"
    assert "session.ended" not in events, "a Cursor run result does not end the agent session"


@pytest.mark.parametrize(
    ("status", "code", "context_updates", "expected"),
    [
        ("finished", "", {"completion": COVERED}, (AttemptOutcome.SUCCEEDED, None, False)),
        ("FINISHED", "", {}, (None, None, True)),
        (
            "finished",
            "",
            {"missing_output_policy": "not_accepted"},
            (AttemptOutcome.FAILED, FailureClass.OUTPUTS_MISSING, True),
        ),
        (
            "finished",
            "",
            {
                "completion": CompletionEvidence(
                    declared_outputs=frozenset({"a", "b"}), registered_outputs=frozenset({"a"})
                )
            },
            (None, None, True),
        ),
        ("finished", "", {"completion_policy": "deferred"}, (None, None, False)),
        ("error", "", {}, (AttemptOutcome.FAILED, FailureClass.PROVIDER_ERROR, False)),
        ("error", "agent_busy", {}, (AttemptOutcome.FAILED, FailureClass.CAPACITY, False)),
        (
            "error",
            "SESSION_TOKEN_LIMIT_REACHED",
            {},
            (AttemptOutcome.FAILED, FailureClass.CAPACITY, False),
        ),
        ("expired", "", {}, (AttemptOutcome.FAILED, FailureClass.TIMEOUT, False)),
        (
            "cancelled",
            "",
            {"cancel_command_admitted": True},
            (AttemptOutcome.CANCELLED, FailureClass.CANCELLED_BY_COMMAND, False),
        ),
        ("cancelled", "", {}, (AttemptOutcome.FAILED, FailureClass.PROVIDER_ERROR, False)),
        ("mystery", "", {}, (None, None, False)),
    ],
)
def test_section_3_5_outcome_mapping(
    status: str, code: str, context_updates: dict[str, Any], expected: tuple[Any, ...]
) -> None:
    context = DeriveContext(lane=LaneProfile.CURSOR_CLOUD, current_generation=1, **context_updates)
    outcome, failure, missing, _policy = map_execution_outcome(
        status, error_code=code.lower(), context=context
    )
    assert (outcome, failure, missing) == expected


def test_finished_without_a_completion_candidate_is_never_succeeded() -> None:
    facts = derive(deep_agents_turn_frames(), DA)
    outcome = next(fact for fact in facts if isinstance(fact, ExecutionOutcomeFact))
    assert outcome.outcome is None and outcome.missing_outputs
    events = fact_events(
        outcome, lane_profile=LaneProfile.DEEP_AGENTS, activation_id="a", attempt_no=1
    )
    assert [event for event, _ in events] == ["activation.phase_changed"]
    assert events[0][1]["phase"] == "follow_up_turn"
    unknown = derive([provider_frame(1, K.RUN_RESULT, {"status": "mystery"})], DA)
    assert kinds(unknown) == ["session_started", "unit_in_doubt", "session_ended"]


def test_usage_dispositions_unknown_is_never_zero() -> None:
    report = usage_report(LaneProfile.DEEP_AGENTS, [("f1", {"input_tokens": 3})])
    assert report.value("output_tokens") is None
    assert report.dimensions["output_tokens"].disposition == UsageDisposition.UNKNOWN
    assert render_dimension(report.dimensions["cost_micros"]) == "unknown"
    assert render_dimension(report.dimensions["input_tokens"]) == "3"
    cursor = usage_report(
        LaneProfile.CURSOR_CLOUD,
        [("f1", {"inputTokens": 10, "cost_micros": 500}), ("f2", {"charged_cents": 2})],
    )
    assert cursor.dimensions["cost_micros"].disposition == UsageDisposition.SETTLED
    assert cursor.value("cost_micros") == 20_000
    estimated = usage_report(LaneProfile.CURSOR_LOCAL, [("f1", {"cost_micros": 500})])
    assert render_dimension(estimated.dimensions["cost_micros"]) == "500 (estimated)"
    claude = usage_report(LaneProfile.CLAUDE_AGENT_SDK, [("f1", {"total_cost_usd": 0.0123})])
    assert claude.dimensions["cost_micros"].disposition == UsageDisposition.ESTIMATED


def test_frames_of_a_non_current_generation_produce_no_facts() -> None:
    old = deep_agents_turn_frames(generation=1)
    new = deep_agents_turn_frames(generation=2)
    context = DeriveContext(lane=LaneProfile.DEEP_AGENTS, current_generation=2)
    assert derive(old, context) == ()
    assert {fact.generation for fact in derive([*old, *new], context)} == {2}


def test_derivation_and_event_payloads_are_deterministic_on_replay() -> None:
    first = derive(deep_agents_turn_frames(), DA)
    second = derive(list(reversed(deep_agents_turn_frames())), DA)
    assert first == second

    def payloads(facts: Any) -> list[str]:
        return [
            json.dumps(payload, sort_keys=True)
            for fact in facts
            for _type, payload in fact_events(
                fact, lane_profile=LaneProfile.DEEP_AGENTS, activation_id="a", attempt_no=1
            )
        ]

    assert payloads(first) == payloads(second)
    for fact in first:
        for _event, payload in fact_events(
            fact, lane_profile=LaneProfile.DEEP_AGENTS, activation_id="a", attempt_no=1
        ):
            assert payload["source"] == {
                "kind": "adapter",
                "native_event_ref": native_event_ref(fact.frame_id),
            }
            assert "body" not in json.dumps(payload) or "body_digest" in json.dumps(payload)


# --- Reducer action ----------------------------------------------------------------------


def _apply_action(facts: Any, *, through: int, generation: int = 1) -> ApplyFrameFactsAction:
    return ApplyFrameFactsAction(
        harness_execution_id=str(FIXTURE_HARNESS),
        generation=generation,
        lane_profile="deep_agents",
        activation_id=str(FIXTURE_ACTIVATION),
        attempt_no=1,
        through_ordinal=through,
        facts=tuple(facts),
    )


@pytest.mark.asyncio
async def test_reducer_applies_frame_facts_once_with_native_event_refs() -> None:
    action = _apply_action(derive(deep_agents_turn_frames(), DA), through=11)
    assert required_action_permissions(action) == frozenset({"workflow_run.apply_frame_facts"})
    service, _repository = run_control_service()
    admitted = await service.admit(request())
    assert admitted.run_id is not None
    run_id = admitted.run_id
    started = await service.execute(command(run_id, 1, "start", StartAction()))
    assert started.status == CommandStatus.ACCEPTED
    permitted = actor().model_copy(
        update={"permissions": ALL_PERMISSIONS | {"workflow_run.apply_frame_facts"}}
    )
    facts = derive(deep_agents_turn_frames(), DA)
    apply = command(run_id, 2, "facts-1", _apply_action(facts, through=11)).model_copy(
        update={"actor": permitted}
    )
    result = await service.execute(apply)
    assert result.status == CommandStatus.ACCEPTED and result.phase.value == "active"
    run = await service.get_run("tenant-1", run_id)
    assert [(item.through_ordinal, item.generation) for item in run.frame_fact_cursors] == [(11, 1)]
    assert run.phase == started.phase, "frames never move the run phase"
    # Replaying the same facts at the next version is rejected by the cursor.
    replay = command(run_id, 3, "facts-2", _apply_action(facts, through=11)).model_copy(
        update={"actor": permitted}
    )
    rejected = await service.execute(replay)
    assert rejected.status == CommandStatus.REJECTED
    assert rejected.reason_code == "frame_facts_already_applied"
    # Without the permission the action is refused.
    with pytest.raises(Exception, match="apply_frame_facts"):
        await service.execute(command(run_id, 3, "facts-3", _apply_action(facts, through=11)))


@pytest.mark.asyncio
async def test_reduction_writes_one_event_per_fact_and_fences_stale_generations() -> None:
    service, repository = run_control_service()
    admitted = await service.admit(request(request_id="reduction"))
    assert admitted.run_id is not None
    await service.execute(command(admitted.run_id, 1, "start", StartAction()))
    projection = await repository.get_run("tenant-1", admitted.run_id)
    budget = await repository.get_budget("tenant-1", admitted.run_id)
    effects = await repository.get_effects("tenant-1", admitted.run_id)
    permitted = actor().model_copy(
        update={"permissions": ALL_PERMISSIONS | {"workflow_run.apply_frame_facts"}}
    )
    second_generation = DeriveContext(lane=LaneProfile.DEEP_AGENTS, current_generation=2)
    facts = derive(deep_agents_turn_frames(generation=2), second_generation)
    lifecycle = LifecycleCommand.model_validate(
        {
            **command(projection.run_id, projection.version, "facts", StartAction()).model_dump(),
            "action": _apply_action(facts, through=11, generation=2),
            "actor": permitted,
        }
    )
    reduction = reduce_lifecycle(projection, budget, effects, lifecycle, "sha256:" + "f" * 64)
    types = [event.event_type for event in reduction.events]
    assert types == [
        "session.started",
        "session.turn_started",
        "tool_call.completed",
        "session.turn_completed",
        "activation.phase_changed",
        "session.ended",
    ]
    assert [event.sequence for event in reduction.events] == list(range(1, 7))
    assert [event.is_version_final for event in reduction.events] == [False] * 5 + [True]
    assert reduction.projection.phase == projection.phase
    refs = {event.payload["source"]["native_event_ref"] for event in reduction.events}  # type: ignore[index]
    assert refs <= {native_event_ref(frame.frame_id) for frame in deep_agents_turn_frames(2)}
    old_facts = derive(deep_agents_turn_frames(generation=1), DA)
    older = lifecycle.model_copy(
        update={
            "expected_run_version": reduction.projection.version,
            "action": _apply_action(old_facts, through=11, generation=1),
        }
    )
    with pytest.raises(ReductionRejected) as rejected:
        reduce_lifecycle(reduction.projection, budget, effects, older, "sha256:" + "f" * 64)
    assert rejected.value.code == "stale_generation"


def test_action_rejects_facts_of_another_execution_or_out_of_order() -> None:
    facts = derive(deep_agents_turn_frames(), DA)
    with pytest.raises(ValueError, match="arrival order"):
        _apply_action(tuple(reversed(facts)), through=11)
    with pytest.raises(ValueError, match="covered by the cursor"):
        _apply_action(facts, through=3)
    with pytest.raises(ValueError, match="one execution generation"):
        ApplyFrameFactsAction(
            harness_execution_id=str(UUID(int=1)),
            generation=1,
            lane_profile="deep_agents",
            activation_id="a",
            attempt_no=1,
            through_ordinal=11,
            facts=facts,
        )


# --- Projector ------------------------------------------------------------------------------


class _RunControl:
    """In-memory run control wrapper granting the projector's permission."""

    def __init__(self, service: Any) -> None:
        self.service = service
        self.commands: list[LifecycleCommand] = []

    async def execute(self, lifecycle: LifecycleCommand) -> Any:
        self.commands.append(lifecycle)
        return await self.service.execute(lifecycle)

    async def get_run(self, request_scope: str, run_id: str) -> Any:
        return await self.service.get_run(request_scope, run_id)


@pytest.mark.asyncio
async def test_projector_applies_unapplied_facts_in_ordinal_aligned_batches() -> None:
    service, _repository = run_control_service()
    admitted = await service.admit(request(request_scope=SCOPE))
    assert admitted.run_id is not None
    run_id = admitted.run_id
    await service.execute(
        command(run_id, 1, "start", StartAction()).model_copy(update={"request_scope": SCOPE})
    )
    store, _ = in_memory_store(run_key=run_id)
    handle = await store.open_execution(harness_start(run_key=run_id))
    from mission_control.application.frames.writer import FrameWriter

    writer = FrameWriter(store, handle, clock=StepClock())
    await writer.write(turn_observations())
    control = _RunControl(service)
    permitted = actor().model_copy(
        update={"permissions": ALL_PERMISSIONS | {"workflow_run.apply_frame_facts"}}
    )
    projector = FrameFactProjector(store, control, actor=permitted, batch_size=2)
    target = FrameFactTarget.from_handle(handle, run_key=run_id)
    first = await projector.project(target, completion=COVERED)
    assert first.applied == first.derived == 6
    assert first.through_ordinal == 9
    batches = [len(item.action.facts) for item in control.commands]  # type: ignore[union-attr]
    assert batches == [2, 2, 2], "session_started + turn_started share frame 5"
    again = await projector.project(target, completion=COVERED)
    assert again.applied == 0 and again.derived == 6
    run = await service.get_run(SCOPE, run_id)
    assert run.frame_fact_cursors[0].through_ordinal == 9
    unopened = await projector.project(
        FrameFactTarget(
            request_scope=SCOPE,
            run_key=run_id,
            harness_execution_id=UUID(int=9),
            activation_id=UUID(int=9),
            attempt_no=1,
            lane=LaneProfile.DEEP_AGENTS,
        )
    )
    assert unopened.generation is None and unopened.applied == 0
    assert isinstance(store, InMemoryFrameStore)
