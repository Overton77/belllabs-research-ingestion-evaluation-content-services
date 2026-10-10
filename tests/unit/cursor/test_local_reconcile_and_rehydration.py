"""MP-09: `cursor_local` reconciles only what the process still holds and re-supplies the
options `Agent.resume` does not persist (FIXTURE replaying bridge, real git worktree).

The pinned SDK offers no read-back of a send by idempotency key, and a process death ends
the local agent loop with the run, so a send lost after a restart answers `unknown`
(the service parks it `in_doubt`, never resends).
"""

from __future__ import annotations

from pathlib import Path

from mission_control.adapters.cursor.local import CursorLocalHarness
from mission_control.application.execution.harness.dispatch import DispatchReconcilingLane
from mission_control.domain.execution.lanes import ReattachRequest
from tests.fixtures.cursor_controls import harness_fields, started_session
from tests.fixtures.cursor_local import local_stack
from tests.unit.cursor.support import dispatch_record, send_key, send_request, session_handle

PROFILE = "cursor_local"


def _fresh(stack) -> CursorLocalHarness:  # type: ignore[no-untyped-def]
    """A harness in a 'new process' over the same launcher, leases, hooks and artifacts."""

    source = stack.harness
    return CursorLocalHarness(
        launcher=source._launcher,
        leaser=source._leaser,
        projections=source._projections,
        hooks=source._hooks,
        artifacts=source._artifacts,
        settings=source._settings,
    )


async def test_a_send_is_found_in_process_and_unknown_after_a_restart(tmp_path: Path) -> None:
    stack = local_stack(tmp_path)
    assert isinstance(stack.harness, DispatchReconcilingLane)
    heid, handle = await started_session(stack)
    assert handle.native_session_ref is not None
    agent_id = handle.native_session_ref
    sent = await stack.harness.send_turn(send_request(stack, PROFILE, heid, agent_id, 1))
    assert sent.native_turn_ref is not None
    session = session_handle(stack, PROFILE, heid, agent_id)

    found = await stack.harness.reconcile_dispatch(
        dispatch_record(stack, kind="send", key=send_key(heid, 1)), session=session
    )
    assert found.outcome == "found" and found.native_ref == sent.native_turn_ref
    other = await stack.harness.reconcile_dispatch(
        dispatch_record(stack, kind="send", key=send_key(heid, 2), turn_no=2), session=session
    )
    assert other.outcome == "unknown" and "idempotency" in other.detail
    create = await stack.harness.reconcile_dispatch(
        dispatch_record(stack, kind="create", key=send_key(heid, 1)), session=None
    )
    assert create.outcome == "found" and create.native_ref == agent_id

    fresh = _fresh(stack)
    fresh.stage(heid, stack.operation)
    after_restart = await fresh.reconcile_dispatch(
        dispatch_record(stack, kind="send", key=send_key(heid, 1)), session=session
    )
    assert after_restart.outcome == "unknown", "no guess after the agent loop died"
    lost_create = await fresh.reconcile_dispatch(
        dispatch_record(stack, kind="create", key=send_key(heid, 1)), session=None
    )
    assert lost_create.outcome == "unknown"
    assert len(stack.launcher.sends) == 1, "reconciliation never sends"


async def test_reattach_re_supplies_the_pinned_options_and_records_what_it_reapplied(
    tmp_path: Path,
) -> None:
    stack = local_stack(tmp_path)
    heid, handle = await started_session(stack)
    assert handle.native_session_ref is not None
    agent_id = handle.native_session_ref
    sent = await stack.harness.send_turn(send_request(stack, PROFILE, heid, agent_id, 1))
    assert "rehydrated" not in handle.native_details

    fresh = _fresh(stack)
    fresh.stage(heid, stack.operation)
    resumed = await fresh.reattach(
        ReattachRequest(
            **harness_fields(stack.operation, heid, PROFILE),
            native_session_ref=agent_id,
            native_turn_ref=sent.native_turn_ref,
        )
    )
    (created_spec,) = stack.launcher.created
    (resumed_agent, resumed_spec) = stack.launcher.resumed[-1]
    assert resumed_agent == agent_id and resumed_spec == created_spec, (
        "tools, subagents, setting sources and sandbox are re-supplied verbatim"
    )
    receipt = fresh.rehydration_receipt(heid)
    # Subagents reach cursor_local as `.cursor/agents/*.md` files, not inline options.
    assert {"bridge", "projection", "setting_sources", "mode", "sandbox"} <= set(receipt)
    assert "agents" not in receipt and "disallowed_tools" not in receipt
    assert resumed.native_details["rehydrated"] == ",".join(receipt)
    assert resumed.native_session_ref == agent_id
    assert len(stack.launcher.launches) == 2, "a bridge was relaunched over the same lease"
    assert stack.launcher.launches[0] == stack.launcher.launches[1]
