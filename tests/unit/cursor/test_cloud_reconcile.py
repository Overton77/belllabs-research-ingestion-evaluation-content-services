"""MP-09: `cursor_cloud` is a `DispatchReconcilingLane` (FIXTURE Cloud API).

An ambiguous create is looked up by its deterministic client `agentId` (`404` is the
provider's authoritative "not received"); an ambiguous send by the agent's `latestRunId`
against the runs this lane acknowledged. What the lane cannot anchor stays `unknown`, so the
service parks it `in_doubt` instead of guessing (MP-06 V07).
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import pytest

from mission_control.adapters.cursor.cloud import client_agent_id, parse_dispatch_key
from mission_control.application.execution.harness.dispatch import DispatchReconcilingLane
from mission_control.domain.execution.lanes import ObserveRequest, ReattachRequest, TurnHandle
from tests.fixtures.cursor_cloud import cloud_stack
from tests.fixtures.cursor_controls import harness_fields, started_session
from tests.unit.cursor.support import (
    dispatch_record,
    fresh_cloud_harness,
    heid_of,
    identity,
    lanes_for,
    send_key,
    send_request,
    session_handle,
    signals,
    turn,
)

PROFILE = "cursor_cloud"


def test_dispatch_keys_name_the_execution_generation_and_turn() -> None:
    parts = parse_dispatch_key("heid-1:2:turn:3")
    assert parts is not None and (parts.generation, parts.kind, parts.turn_no) == (2, "turn", 3)
    continuation = parse_dispatch_key("heid-1:1:continuation:transfer-9")
    assert continuation is not None and continuation.kind == "continuation"
    assert continuation.turn_no is None
    assert parse_dispatch_key("k1") is None


async def test_a_journaled_create_is_found_by_the_client_agent_id_or_authoritatively_absent(
    tmp_path: Path,
) -> None:
    stack = cloud_stack(tmp_path)
    assert isinstance(stack.harness, DispatchReconcilingLane)
    heid = heid_of(stack, PROFILE)
    stack.harness.stage(heid, stack.operation)
    record = dispatch_record(stack, kind="create", key=send_key(heid, 1))
    absent = await stack.harness.reconcile_dispatch(record, session=None)
    assert absent.outcome == "not_received", "no agent under our deterministic id: nothing accepted"
    # The earlier attempt's create reached the provider but its response was lost.
    agent_ref = client_agent_id(heid, 1)
    await stack.client.create_agent(
        {"agentId": agent_ref, "prompt": {"text": "x"}, "repos": [{"url": "u"}]}
    )
    found = await stack.harness.reconcile_dispatch(record, session=None)
    assert found.outcome == "found" and found.native_ref == agent_ref
    assert len(stack.api.agents) == 1


async def test_a_create_with_an_idempotency_key_cannot_be_read_back_and_stays_unknown(
    tmp_path: Path,
) -> None:
    stack = cloud_stack(
        tmp_path,
        cloud_changes={"env_vars_ref": "vault:mission/cloud-env", "client_agent_id": False},
        env_vars={"MC_MODE": "fixture"},
    )
    heid = heid_of(stack, PROFILE)
    stack.harness.stage(heid, stack.operation)
    lookup = await stack.harness.reconcile_dispatch(
        dispatch_record(stack, kind="create", key=send_key(heid, 1)), session=None
    )
    assert lookup.outcome == "unknown" and "Idempotency-Key" in lookup.detail


async def test_a_lost_turn_1_receipt_is_reconciled_from_the_create_run_and_never_resent(
    tmp_path: Path,
) -> None:
    """V07 through `LaneTurnService`: the send of turn 1 was journaled `intended` by a lost
    attempt; the retry reconciles it to the run the create enqueued (one agent, no POST
    /runs, the journal acknowledged with attempts 1)."""

    stack = cloud_stack(tmp_path)
    lanes = lanes_for(stack)
    ident = identity(stack, PROFILE)
    scope, heid = ident.request_scope, ident.harness_execution_id
    now = datetime.now(UTC)
    owner = await lanes.states.claim_owner(
        scope,
        heid,
        owner_ref=lanes.service.sessions.owner_ref,
        generation=1,
        now=now,
        lease=timedelta(seconds=60),
    )
    pending = dispatch_record(
        stack,
        kind="send",
        key=send_key(str(heid), 1),
        owner=owner,
        instruction_ref=f"operation:{stack.operation.identity.semantic_key}:turn:1",
    )
    claim = await lanes.states.intend_dispatch(scope, heid, pending, owner=owner)
    assert claim.fresh

    result = await lanes.service.turn(turn(stack, PROFILE), signals(lanes, stack, PROFILE))

    assert result.done and result.operation_result is not None
    assert result.operation_result["status"] == "completed"
    assert len(stack.api.creates) == 1 and stack.api.created_runs == [stack.api.run_id]
    state = await lanes.states.load(scope, heid)
    assert state is not None and state.native_turn_ref == stack.api.run_id
    acked = state.dispatch("send", send_key(str(heid), 1))
    assert acked is not None and acked.phase == "acknowledged" and acked.attempts == 1
    assert acked.native_ref == stack.api.run_id


async def _live_after_turn_1(stack, tmp_path: Path):  # type: ignore[no-untyped-def]
    """Stage, prepare, start and send turn 1 on the harness, then observe it to its result
    (the fake finishes the run when the stream reads the result), without settling: the
    session stays live, as it is while a queued instruction waits for the boundary."""

    heid, handle = await started_session(stack)
    assert handle.native_session_ref is not None
    agent_id = handle.native_session_ref
    first = await stack.harness.send_turn(send_request(stack, PROFILE, heid, agent_id, 1))
    assert first.native_turn_ref == stack.api.run_id
    session = session_handle(stack, PROFILE, heid, agent_id)
    frames = [
        frame
        async for frame in stack.harness.observe(
            ObserveRequest(
                **harness_fields(stack.operation, heid, PROFILE),
                turn=TurnHandle(session=session, turn_no=1, native_turn_ref=stack.api.run_id),
            )
        )
    ]
    assert frames[-1].terminal
    return heid, agent_id, session


async def test_a_lost_later_send_is_found_by_the_latest_run_or_authoritatively_not_received(
    tmp_path: Path,
) -> None:
    stack = cloud_stack(tmp_path)
    heid, agent_id, handle = await _live_after_turn_1(stack, tmp_path)
    # The provider accepts turn 2's run; the response is lost before the journal is written.
    stack.api.lose_run_responses = 1
    with pytest.raises(httpx.ReadError):
        await stack.harness.send_turn(send_request(stack, PROFILE, heid, agent_id, 2))
    assert len(stack.api.created_runs) == 2
    accepted = stack.api.created_runs[1]

    found = await stack.harness.reconcile_dispatch(
        dispatch_record(stack, kind="send", key=send_key(heid, 2), turn_no=2), session=handle
    )
    assert found.outcome == "found" and found.native_ref == accepted
    # A send that never reached the provider: the newest run is one we acknowledged.
    refused = await stack.harness.reconcile_dispatch(
        dispatch_record(stack, kind="send", key=send_key(heid, 3), turn_no=3), session=handle
    )
    assert refused.outcome == "not_received"
    assert len(stack.api.created_runs) == 2, "reconciliation reads; it never sends"


async def test_a_fresh_process_anchors_on_the_journaled_run_or_stays_unknown(
    tmp_path: Path,
) -> None:
    stack = cloud_stack(tmp_path)
    heid, agent_id, _handle = await _live_after_turn_1(stack, tmp_path)
    fields = harness_fields(stack.operation, heid, PROFILE)

    # A worker that knows no acknowledged run cannot tell a lost send from an earlier turn.
    unanchored = fresh_cloud_harness(stack, tmp_path / "m2")
    unanchored.stage(heid, stack.operation)
    await unanchored.reattach(ReattachRequest(**fields, native_session_ref=agent_id))
    lookup = await unanchored.reconcile_dispatch(
        dispatch_record(stack, kind="send", key=send_key(heid, 2), turn_no=2),
        session=session_handle(stack, PROFILE, heid, agent_id),
    )
    assert lookup.outcome == "unknown" and "anchor" in lookup.detail

    # The journal's last acknowledged run (`ReattachRequest.native_turn_ref`) is the anchor.
    anchored = fresh_cloud_harness(stack, tmp_path / "m3")
    anchored.stage(heid, stack.operation)
    await anchored.reattach(
        ReattachRequest(**fields, native_session_ref=agent_id, native_turn_ref=stack.api.run_id)
    )
    stack.api.lose_run_responses = 1
    with pytest.raises(httpx.ReadError):
        await anchored.send_turn(send_request(stack, PROFILE, heid, agent_id, 2))
    found = await anchored.reconcile_dispatch(
        dispatch_record(stack, kind="send", key=send_key(heid, 2), turn_no=2),
        session=session_handle(stack, PROFILE, heid, agent_id),
    )
    assert found.outcome == "found" and found.native_ref == stack.api.created_runs[-1]
    assert found.native_ref != stack.api.run_id


async def test_a_continuation_send_is_found_from_the_hydrated_agents_first_run(
    tmp_path: Path,
) -> None:
    stack = cloud_stack(tmp_path)
    heid = heid_of(stack, PROFILE)
    stack.harness.stage(heid, stack.operation)
    session = stack.harness._sessions[heid]
    session.first_runs = {"bc-continuation": "run-continuation-1"}
    lookup = await stack.harness.reconcile_dispatch(
        dispatch_record(
            stack, kind="send", key=f"{heid}:1:continuation:t-1", instruction_ref="continuation:t-1"
        ),
        session=session_handle(stack, PROFILE, heid, "bc-continuation"),
    )
    assert lookup.outcome == "found" and lookup.native_ref == "run-continuation-1"
