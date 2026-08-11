"""Live Block C drills against a persistent ``langgraph up`` qualification server.

Requires ``AGENT_SERVER_ENDPOINT`` and RSA JWT env vars (see fixtures module).
Process-restart and provider fail-open N/N+1 phases are env-gated markers.
"""

from __future__ import annotations

import os
from pathlib import Path
from uuid import uuid4

import httpx
import pytest
from langgraph_sdk.errors import APIStatusError, NotFoundError

from app.agent_server.block_c_qualification.compat import (
    ASSEMBLY_ROLE_N,
    ASSEMBLY_ROLE_N1,
    COMPAT_VERSION_N,
    COMPAT_VERSION_N1,
    GRAPH_ID_N,
    GRAPH_ID_N1,
)
from app.agent_server.block_c_qualification.compat_route import (
    IncompatibleResumeRouteError,
)
from app.agent_server.block_c_qualification.guarded_resume import (
    guarded_deployment_runs_wait,
    guarded_runs_wait,
)
from tests.fixtures.agent_server_block_c import (
    capture_tenant_introspection_snapshot,
    copy_thread_strict,
    get_thread_status,
    interrupt_payloads,
    is_missing_n_graph_on_n1_error,
    read_restart_state,
    wait_run_status,
    wait_thread_status,
    wait_thread_values,
    write_restart_state,
)

pytestmark = pytest.mark.block_c_live


def _run_input(*, scenario: str, request_scope: str = "tenant-a") -> dict[str, object]:
    return {
        "request_scope": request_scope,
        "scenario": scenario,
        "compat_version": "",
        "claim_tokens": [],
        "decisions": [],
        "events": (),
        "decision_refs": (),
    }


def _wait_input(*, hold_seconds: float, request_scope: str = "tenant-a") -> dict[str, object]:
    return {
        "request_scope": request_scope,
        "compat_version": "",
        "wait_status": "idle",
        "resource_open": False,
        "events": (),
        "hold_seconds": hold_seconds,
    }


@pytest.mark.asyncio
async def test_unauthenticated_requests_are_rejected(block_c_endpoint: str) -> None:
    async with httpx.AsyncClient(base_url=block_c_endpoint, timeout=20) as client:
        ok = await client.get("/ok")
        assert ok.status_code == 200
        assistants = await client.post("/assistants/search", json={})
        assert assistants.status_code == 401


@pytest.mark.asyncio
async def test_introspection_has_no_store_or_mutation_side_effects(
    tenant_a_client,
    qualification_assistant_id: str,
) -> None:
    """Repeated tenant-scoped reads must not create runs/history/checkpoints/assistants/store."""

    thread = await tenant_a_client.threads.create(
        metadata={"request_scope": "tenant-a", "belllabs_run_id": f"qual-introspect-{uuid4().hex}"},
    )
    thread_id = str(thread["thread_id"])
    await tenant_a_client.runs.wait(
        thread_id,
        qualification_assistant_id,
        input=_run_input(scenario="single_interrupt"),
    )
    await wait_thread_status(tenant_a_client, thread_id, statuses={"interrupted"})

    baseline = await capture_tenant_introspection_snapshot(
        tenant_a_client,
        thread_id=thread_id,
        request_scope="tenant-a",
    )
    # Non-vacuous baseline: real interrupted checkpoint with history and runs.
    assert baseline["run_count"] >= 1
    assert baseline["history_count"] >= 1
    assert baseline["state_checkpoint_ref"]
    assert "claim_tokens" in baseline["state_value_keys"]
    assert baseline["assistant_count"] >= 1
    assert qualification_assistant_id in baseline["assistant_ids"]

    schemas = await tenant_a_client.assistants.get_schemas(qualification_assistant_id)
    graph = await tenant_a_client.assistants.get_graph(qualification_assistant_id)
    assert schemas is not None
    assert isinstance(graph, dict)
    # Repeated reads of state/history/thread (the introspection surface under test).
    for _ in range(3):
        await tenant_a_client.threads.get(thread_id)
        await tenant_a_client.threads.get_state(thread_id)
        await tenant_a_client.threads.get_history(thread_id, limit=50)
        await tenant_a_client.runs.list(thread_id, limit=50)
        await tenant_a_client.assistants.search(limit=50)
        await tenant_a_client.assistants.get_schemas(qualification_assistant_id)
        await tenant_a_client.assistants.get_graph(qualification_assistant_id)

    after = await capture_tenant_introspection_snapshot(
        tenant_a_client,
        thread_id=thread_id,
        request_scope="tenant-a",
    )
    assert after["assistant_count"] == baseline["assistant_count"]
    assert after["assistant_ids"] == baseline["assistant_ids"]
    assert after["assistant_ids_digest"] == baseline["assistant_ids_digest"]
    assert after["thread_count_scope"] == baseline["thread_count_scope"]
    assert after["thread_status"] == baseline["thread_status"] == "interrupted"
    assert after["state_checkpoint_ref"] == baseline["state_checkpoint_ref"]
    assert after["state_values_digest"] == baseline["state_values_digest"]
    assert after["state_value_keys"] == baseline["state_value_keys"]
    assert after["history_count"] == baseline["history_count"]
    assert after["history_checkpoint_digest"] == baseline["history_checkpoint_digest"]
    assert after["run_count"] == baseline["run_count"]
    assert after["run_digest"] == baseline["run_digest"]
    assert after["run_pairs"] == baseline["run_pairs"]
    assert after["store_item_count"] == baseline["store_item_count"]
    assert after["store_denied"] == baseline["store_denied"]

    # SDK 0.4.2: namespace is positional-only; store mutation stays denied.
    with pytest.raises(APIStatusError):
        await tenant_a_client.store.put_item(
            ("tenant-a", "development", "runtime_projection"),
            key="should-fail",
            value={"x": 1},
        )
    post_store = await capture_tenant_introspection_snapshot(
        tenant_a_client,
        thread_id=thread_id,
        request_scope="tenant-a",
    )
    assert post_store["store_item_count"] == baseline["store_item_count"]
    assert post_store["run_digest"] == baseline["run_digest"]
    assert post_store["history_checkpoint_digest"] == baseline["history_checkpoint_digest"]
    assert post_store["assistant_ids_digest"] == baseline["assistant_ids_digest"]


@pytest.mark.asyncio
async def test_tenant_isolation_denies_cross_scope_thread_access(
    tenant_a_client,
    tenant_b_client,
    qualification_assistant_id: str,
) -> None:
    thread = await tenant_a_client.threads.create(
        metadata={"request_scope": "tenant-a", "belllabs_run_id": f"qual-{uuid4().hex}"},
    )
    thread_id = str(thread["thread_id"])
    await tenant_a_client.runs.wait(
        thread_id,
        qualification_assistant_id,
        input=_run_input(scenario="single_interrupt"),
        multitask_strategy="reject",
    )
    state = await tenant_a_client.threads.get_state(thread_id)
    assert state is not None
    assert await get_thread_status(tenant_a_client, thread_id) == "interrupted"
    with pytest.raises(APIStatusError):
        await tenant_b_client.threads.get_state(thread_id)
    with pytest.raises(APIStatusError):
        await tenant_b_client.threads.get_history(thread_id)
    with pytest.raises(APIStatusError):
        await tenant_b_client.threads.copy(thread_id)
    with pytest.raises(APIStatusError):
        await tenant_b_client.threads.create(
            metadata={"request_scope": "tenant-a"},
        )


@pytest.mark.asyncio
async def test_single_interrupt_resume_and_duplicate_resume_behavior(
    tenant_a_client,
    qualification_assistant_id: str,
) -> None:
    thread = await tenant_a_client.threads.create(
        metadata={"request_scope": "tenant-a", "belllabs_run_id": f"qual-{uuid4().hex}"},
    )
    thread_id = str(thread["thread_id"])
    await tenant_a_client.runs.wait(
        thread_id,
        qualification_assistant_id,
        input=_run_input(scenario="single_interrupt"),
    )
    await wait_thread_status(tenant_a_client, thread_id, statuses={"interrupted"})
    state = await tenant_a_client.threads.get_state(thread_id)
    values = state.get("values") or {}
    assert "stable-claim:block-c-single" in (values.get("claim_tokens") or [])
    assert interrupt_payloads(state)
    history_before = await tenant_a_client.threads.get_history(thread_id, limit=20)

    await tenant_a_client.runs.wait(
        thread_id,
        qualification_assistant_id,
        command={"resume": "approved-from-authority"},
    )
    await wait_thread_status(tenant_a_client, thread_id, statuses={"idle"})
    final_values = (await tenant_a_client.threads.get_state(thread_id)).get("values") or {}
    assert final_values.get("decisions") == ["approved-from-authority"]
    assert (final_values.get("claim_tokens") or []).count("stable-claim:block-c-single") == 1
    history_after_resume = await tenant_a_client.threads.get_history(thread_id, limit=50)
    assert len(history_after_resume) >= len(history_before)

    decisions_before_dup = list(final_values.get("decisions") or [])
    history_len_before_dup = len(history_after_resume)
    duplicate_error: APIStatusError | None = None
    try:
        await tenant_a_client.runs.wait(
            thread_id,
            qualification_assistant_id,
            command={"resume": "approved-from-authority"},
        )
    except APIStatusError as error:
        duplicate_error = error
    after_state = await tenant_a_client.threads.get_state(thread_id)
    after_values = after_state.get("values") or {}
    after_status = await get_thread_status(tenant_a_client, thread_id)
    assert (after_values.get("claim_tokens") or []).count("stable-claim:block-c-single") == 1
    assert after_values.get("decisions") == decisions_before_dup
    assert after_values.get("decisions") == ["approved-from-authority"]
    history_after_dup = await tenant_a_client.threads.get_history(thread_id, limit=50)
    # No-op success is allowed only when semantic state is unchanged; otherwise
    # the API must fail closed with a status error (not a silent second approval).
    if duplicate_error is None:
        assert after_status in {"idle", "interrupted", "error"}
        assert len(history_after_dup) >= history_len_before_dup
    else:
        assert duplicate_error.status_code >= 400
        assert after_values.get("decisions") == decisions_before_dup


@pytest.mark.asyncio
async def test_parallel_interrupt_ids_map_to_distinct_decision_refs(
    tenant_a_client,
    qualification_assistant_id: str,
) -> None:
    thread = await tenant_a_client.threads.create(
        metadata={"request_scope": "tenant-a", "belllabs_run_id": f"qual-{uuid4().hex}"},
    )
    thread_id = str(thread["thread_id"])
    await tenant_a_client.runs.wait(
        thread_id,
        qualification_assistant_id,
        input=_run_input(scenario="parallel_interrupts"),
    )
    await wait_thread_status(tenant_a_client, thread_id, statuses={"interrupted"})
    state = await tenant_a_client.threads.get_state(thread_id)
    payloads = interrupt_payloads(state)
    assert len(payloads) >= 2
    ids = {str(item.get("id")) for item in payloads if item.get("id")}
    refs = {
        str((item.get("value") or {}).get("decision_ref"))
        for item in payloads
        if isinstance(item.get("value"), dict)
    }
    assert len(ids) >= 2
    assert "decision:block-c-parallel-a" in refs
    assert "decision:block-c-parallel-b" in refs
    resume_map = {
        str(item["id"]): f"ok-{(item.get('value') or {}).get('lane')}"
        for item in payloads
        if item.get("id")
    }
    await tenant_a_client.runs.wait(
        thread_id,
        qualification_assistant_id,
        command={"resume": resume_map},
    )
    await wait_thread_status(tenant_a_client, thread_id, statuses={"idle"})
    final_values = (await tenant_a_client.threads.get_state(thread_id)).get("values") or {}
    assert sorted(final_values.get("decisions") or []) == ["a:ok-a", "b:ok-b"]


@pytest.mark.asyncio
async def test_multitask_strategy_reject_and_enqueue(
    tenant_a_client,
    wait_assistant_id: str,
) -> None:
    """Observe reject/enqueue against a genuinely active wait run (not interrupt)."""

    reject_thread = await tenant_a_client.threads.create(
        metadata={"request_scope": "tenant-a", "belllabs_run_id": f"qual-{uuid4().hex}"},
    )
    reject_thread_id = str(reject_thread["thread_id"])
    active = await tenant_a_client.runs.create(
        reject_thread_id,
        wait_assistant_id,
        input=_wait_input(hold_seconds=60),
        multitask_strategy="reject",
    )
    active_id = str(active["run_id"])
    await wait_run_status(
        tenant_a_client,
        reject_thread_id,
        active_id,
        statuses={"running"},
        timeout_seconds=30,
    )
    # Barrier: enter_wait must have opened the resource while hold is still active.
    await wait_thread_values(
        tenant_a_client,
        reject_thread_id,
        predicate=lambda values, _state: (
            values.get("wait_status") == "waiting" and values.get("resource_open") is True
        ),
        timeout_seconds=30,
    )
    still_active = await tenant_a_client.runs.get(reject_thread_id, active_id)
    assert str(still_active.get("status")) == "running"

    rejected: Exception | None = None
    try:
        await tenant_a_client.runs.create(
            reject_thread_id,
            wait_assistant_id,
            input=_wait_input(hold_seconds=1),
            multitask_strategy="reject",
        )
    except Exception as error:  # noqa: BLE001 - capture provider rejection shape
        rejected = error
    assert rejected is not None, "reject strategy must deny a second run while one is active"
    active_after_reject = await tenant_a_client.runs.get(reject_thread_id, active_id)
    assert str(active_after_reject.get("status")) == "running"

    await tenant_a_client.runs.cancel(
        reject_thread_id,
        active_id,
        wait=True,
        action="interrupt",
    )
    cancelled_reject = await wait_run_status(
        tenant_a_client,
        reject_thread_id,
        active_id,
        statuses={"interrupted", "success", "error"},
        timeout_seconds=30,
    )
    assert str(cancelled_reject.get("status")) in {"interrupted", "success", "error"}

    enqueue_thread = await tenant_a_client.threads.create(
        metadata={"request_scope": "tenant-a", "belllabs_run_id": f"qual-{uuid4().hex}"},
    )
    enqueue_thread_id = str(enqueue_thread["thread_id"])
    run_a = await tenant_a_client.runs.create(
        enqueue_thread_id,
        wait_assistant_id,
        input=_wait_input(hold_seconds=30),
        multitask_strategy="enqueue",
    )
    run_a_id = str(run_a["run_id"])
    await wait_run_status(
        tenant_a_client,
        enqueue_thread_id,
        run_a_id,
        statuses={"running"},
        timeout_seconds=30,
    )
    run_b = await tenant_a_client.runs.create(
        enqueue_thread_id,
        wait_assistant_id,
        input=_wait_input(hold_seconds=1),
        multitask_strategy="enqueue",
    )
    run_b_id = str(run_b["run_id"])
    listed = await tenant_a_client.runs.list(enqueue_thread_id, limit=10)
    by_id = {str(item["run_id"]): item for item in listed}
    assert run_a_id in by_id and run_b_id in by_id
    assert str(by_id[run_a_id].get("status")) == "running"
    # Enqueued second run should not be running concurrently under enqueue.
    assert str(by_id[run_b_id].get("status")) in {"pending", "running"}
    if str(by_id[run_b_id].get("status")) == "running":
        # If the provider starts it immediately after A finishes, A must no longer run.
        refreshed_a = await tenant_a_client.runs.get(enqueue_thread_id, run_a_id)
        assert str(refreshed_a.get("status")) != "running"

    for run in listed:
        status = str(run.get("status") or "")
        if status in {"pending", "running"}:
            await tenant_a_client.runs.cancel(
                enqueue_thread_id,
                str(run["run_id"]),
                wait=False,
                action="interrupt",
            )


@pytest.mark.asyncio
@pytest.mark.parametrize("strategy", ["interrupt", "rollback"])
async def test_multitask_replacement_strategies_stop_the_active_run(
    tenant_a_client,
    wait_assistant_id: str,
    strategy: str,
) -> None:
    thread = await tenant_a_client.threads.create(
        metadata={
            "request_scope": "tenant-a",
            "belllabs_run_id": f"qual-multitask-{strategy}-{uuid4().hex}",
        },
    )
    thread_id = str(thread["thread_id"])
    active = await tenant_a_client.runs.create(
        thread_id,
        wait_assistant_id,
        input=_wait_input(hold_seconds=120),
        multitask_strategy="reject",
    )
    active_id = str(active["run_id"])
    await wait_run_status(
        tenant_a_client,
        thread_id,
        active_id,
        statuses={"running"},
        timeout_seconds=30,
    )
    await wait_thread_values(
        tenant_a_client,
        thread_id,
        predicate=lambda values, _state: (
            values.get("wait_status") == "waiting" and values.get("resource_open") is True
        ),
        timeout_seconds=30,
    )
    active_state = await tenant_a_client.threads.get_state(thread_id)
    active_checkpoint = active_state.get("checkpoint") or {}
    active_checkpoint_id = str(
        active_checkpoint.get("checkpoint_id")
        or active_state.get("checkpoint_id")
        or ""
    )
    assert active_checkpoint_id

    replacement = await tenant_a_client.runs.create(
        thread_id,
        wait_assistant_id,
        input=_wait_input(hold_seconds=0.01),
        multitask_strategy=strategy,
    )
    replacement_id = str(replacement["run_id"])
    terminal_replacement = await wait_run_status(
        tenant_a_client,
        thread_id,
        replacement_id,
        statuses={"success", "error", "interrupted"},
        timeout_seconds=30,
    )
    assert str(terminal_replacement.get("status")) == "success"
    replaced = await tenant_a_client.runs.get(thread_id, active_id)
    assert str(replaced.get("status")) != "running"
    final = await tenant_a_client.threads.get_state(thread_id)
    final_values = final.get("values") or {}
    assert final_values.get("resource_open") is False
    assert final_values.get("wait_status") == "completed"
    final_events = final_values.get("events") or ()
    assert "wait-resource-opened" in final_events
    assert "wait-resource-closed-completed" in final_events
    final_history = await tenant_a_client.threads.get_history(thread_id, limit=50)
    final_checkpoint_ids = {
        str((item.get("checkpoint") or {}).get("checkpoint_id") or item.get("checkpoint_id") or "")
        for item in final_history
    }
    if strategy == "rollback":
        assert active_checkpoint_id not in final_checkpoint_ids
    else:
        assert active_checkpoint_id in final_checkpoint_ids


@pytest.mark.asyncio
@pytest.mark.parametrize("placement", ["before", "after"])
async def test_operation_level_interrupt_before_and_after_are_resumable(
    tenant_a_client,
    wait_assistant_id: str,
    placement: str,
) -> None:
    thread = await tenant_a_client.threads.create(
        metadata={
            "request_scope": "tenant-a",
            "belllabs_run_id": f"qual-breakpoint-{placement}-{uuid4().hex}",
        },
    )
    thread_id = str(thread["thread_id"])
    interrupt_kwargs = (
        {"interrupt_before": ["enter_wait"]}
        if placement == "before"
        else {"interrupt_after": ["enter_wait"]}
    )
    await tenant_a_client.runs.wait(
        thread_id,
        wait_assistant_id,
        input=_wait_input(hold_seconds=0.01),
        **interrupt_kwargs,
    )
    await wait_thread_status(tenant_a_client, thread_id, statuses={"interrupted"})
    interrupted = await tenant_a_client.threads.get_state(thread_id)
    interrupted_values = interrupted.get("values") or {}
    if placement == "before":
        assert interrupted_values.get("resource_open") is False
        assert interrupted_values.get("wait_status") == "idle"
    else:
        assert interrupted_values.get("resource_open") is True
        assert interrupted_values.get("wait_status") == "waiting"

    await tenant_a_client.runs.wait(
        thread_id,
        wait_assistant_id,
        command={"resume": None},
    )
    await wait_thread_status(tenant_a_client, thread_id, statuses={"idle"})
    final_values = (await tenant_a_client.threads.get_state(thread_id)).get("values") or {}
    assert final_values.get("resource_open") is False
    assert final_values.get("wait_status") == "completed"


@pytest.mark.asyncio
async def test_thread_copy_fork_does_not_mutate_parent(
    tenant_a_client,
    qualification_assistant_id: str,
) -> None:
    lineage_token = f"qual-fork-{uuid4().hex}"
    thread = await tenant_a_client.threads.create(
        metadata={
            "request_scope": "tenant-a",
            "belllabs_run_id": lineage_token,
            "block_c_lineage": lineage_token,
        },
    )
    parent_id = str(thread["thread_id"])
    await tenant_a_client.runs.wait(
        parent_id,
        qualification_assistant_id,
        input=_run_input(scenario="single_interrupt"),
    )
    await wait_thread_status(tenant_a_client, parent_id, statuses={"interrupted"})
    parent_before = await tenant_a_client.threads.get_state(parent_id)
    parent_values_before = dict(parent_before.get("values") or {})
    parent_checkpoint = parent_before.get("checkpoint") or parent_before.get(
        "checkpoint_id"
    )
    parent_history_before = await tenant_a_client.threads.get_history(parent_id, limit=20)
    assert "stable-claim:block-c-single" in (parent_values_before.get("claim_tokens") or [])
    assert await get_thread_status(tenant_a_client, parent_id) == "interrupted"

    child_id = await copy_thread_strict(tenant_a_client, parent_id)
    assert child_id != parent_id
    child_before = await tenant_a_client.threads.get_state(child_id)
    child_values_before = dict(child_before.get("values") or {})
    # Correlated lineage: child must share the parent's interrupted checkpoint values.
    assert child_values_before.get("claim_tokens") == parent_values_before.get(
        "claim_tokens"
    )
    assert await get_thread_status(tenant_a_client, child_id) == "interrupted"
    assert interrupt_payloads(child_before)
    child_meta = (await tenant_a_client.threads.get(child_id)).get("metadata") or {}
    if "block_c_lineage" in child_meta:
        assert child_meta["block_c_lineage"] == lineage_token

    await tenant_a_client.runs.wait(
        child_id,
        qualification_assistant_id,
        command={"resume": "child-approval"},
    )
    await wait_thread_status(tenant_a_client, child_id, statuses={"idle"})
    parent_after = await tenant_a_client.threads.get_state(parent_id)
    parent_history_after = await tenant_a_client.threads.get_history(parent_id, limit=20)
    assert (parent_after.get("values") or {}) == parent_values_before
    assert (parent_after.get("checkpoint") or parent_after.get("checkpoint_id")) == (
        parent_checkpoint
    )
    assert len(parent_history_after) == len(parent_history_before)
    assert await get_thread_status(tenant_a_client, parent_id) == "interrupted"
    child_values = (await tenant_a_client.threads.get_state(child_id)).get("values") or {}
    assert child_values.get("decisions") == ["child-approval"]
    assert "child-approval" not in (parent_values_before.get("decisions") or [])


@pytest.mark.asyncio
async def test_cancellation_leaves_typed_cancelled_cleanup_state(
    tenant_a_client,
    wait_assistant_id: str,
) -> None:
    thread = await tenant_a_client.threads.create(
        metadata={"request_scope": "tenant-a", "belllabs_run_id": f"qual-{uuid4().hex}"},
    )
    thread_id = str(thread["thread_id"])
    run = await tenant_a_client.runs.create(
        thread_id,
        wait_assistant_id,
        input=_wait_input(hold_seconds=120),
        multitask_strategy="reject",
    )
    run_id = str(run["run_id"])
    # Deterministic precondition: enter_wait must checkpoint an open resource.
    mid = await wait_thread_values(
        tenant_a_client,
        thread_id,
        predicate=lambda values, _state: (
            values.get("wait_status") == "waiting" and values.get("resource_open") is True
        ),
        timeout_seconds=30,
    )
    assert "wait-resource-opened" in (mid["values"].get("events") or ())
    active = await tenant_a_client.runs.get(thread_id, run_id)
    assert str(active.get("status")) == "running"

    await tenant_a_client.runs.cancel(thread_id, run_id, wait=True, action="interrupt")
    final_run = await wait_run_status(
        tenant_a_client,
        thread_id,
        run_id,
        # Node-swallowed CancelledError can finish as success while still writing
        # cancelled cleanup; UserInterrupt path finishes as interrupted.
        statuses={"interrupted", "success"},
        timeout_seconds=30,
    )
    assert str(final_run.get("status")) in {"interrupted", "success"}

    cleaned = await wait_thread_values(
        tenant_a_client,
        thread_id,
        predicate=lambda values, _state: (
            values.get("wait_status") == "cancelled"
            and values.get("resource_open") is False
        ),
        timeout_seconds=30,
    )
    assert "wait-resource-closed-cancelled" in (cleaned["values"].get("events") or ())
    assert cleaned["values"].get("wait_status") == "cancelled"
    assert cleaned["values"].get("resource_open") is False


@pytest.mark.asyncio
async def test_rollback_cancellation_removes_active_run_state_and_completed_run_is_immutable(
    tenant_a_client,
    wait_assistant_id: str,
) -> None:
    thread = await tenant_a_client.threads.create(
        metadata={
            "request_scope": "tenant-a",
            "belllabs_run_id": f"qual-cancel-rollback-{uuid4().hex}",
        },
    )
    thread_id = str(thread["thread_id"])
    pre_run_state = await tenant_a_client.threads.get_state(thread_id)
    pre_run_values = dict(pre_run_state.get("values") or {})
    pre_run_history = await tenant_a_client.threads.get_history(thread_id, limit=50)
    active = await tenant_a_client.runs.create(
        thread_id,
        wait_assistant_id,
        input=_wait_input(hold_seconds=120),
        multitask_strategy="reject",
    )
    active_id = str(active["run_id"])
    await wait_thread_values(
        tenant_a_client,
        thread_id,
        predicate=lambda values, _state: (
            values.get("wait_status") == "waiting" and values.get("resource_open") is True
        ),
        timeout_seconds=30,
    )
    await tenant_a_client.runs.cancel(
        thread_id,
        active_id,
        wait=True,
        action="rollback",
    )
    rolled_back = await tenant_a_client.threads.get_state(thread_id)
    rolled_back_values = rolled_back.get("values") or {}
    assert rolled_back_values == pre_run_values
    assert await tenant_a_client.threads.get_history(
        thread_id, limit=50
    ) == pre_run_history

    completed = await tenant_a_client.runs.create(
        thread_id,
        wait_assistant_id,
        input=_wait_input(hold_seconds=0.01),
        multitask_strategy="reject",
    )
    completed_id = str(completed["run_id"])
    completed_run = await wait_run_status(
        tenant_a_client,
        thread_id,
        completed_id,
        statuses={"success", "error", "interrupted"},
        timeout_seconds=30,
    )
    assert str(completed_run.get("status")) == "success"
    completed_state = await tenant_a_client.threads.get_state(thread_id)
    completed_history = await tenant_a_client.threads.get_history(thread_id, limit=50)
    with pytest.raises(APIStatusError):
        await tenant_a_client.runs.cancel(
            thread_id,
            completed_id,
            wait=True,
            action="rollback",
        )
    assert await tenant_a_client.threads.get_state(thread_id) == completed_state
    assert await tenant_a_client.threads.get_history(
        thread_id, limit=50
    ) == completed_history


@pytest.mark.asyncio
@pytest.mark.block_c_nn1
async def test_nn1_compatible_n_on_n_resume_succeeds(
    tenant_a_client,
    qualification_assistant_id: str,
) -> None:
    """Accepted path: guarded facade allows exact N-on-N then invokes runs.wait."""

    thread = await tenant_a_client.threads.create(
        metadata={"request_scope": "tenant-a", "belllabs_run_id": f"qual-{uuid4().hex}"},
    )
    thread_id = str(thread["thread_id"])
    await tenant_a_client.runs.wait(
        thread_id,
        qualification_assistant_id,
        input=_run_input(scenario="single_interrupt"),
    )
    await wait_thread_status(tenant_a_client, thread_id, statuses={"interrupted"})
    before = await tenant_a_client.threads.get_state(thread_id)
    before_values = dict(before.get("values") or {})
    assert "stable-claim:block-c-single" in (before_values.get("claim_tokens") or [])

    result = await guarded_runs_wait(
        source_graph_id=GRAPH_ID_N,
        source_compat_version=COMPAT_VERSION_N,
        target_graph_id=GRAPH_ID_N,
        target_compat_version=COMPAT_VERSION_N,
        runs_wait=tenant_a_client.runs.wait,
        thread_id=thread_id,
        assistant_id=qualification_assistant_id,
        command={"resume": "n-ok"},
    )
    assert result.decision.allowed is True
    assert result.provider_result is not None
    await wait_thread_status(tenant_a_client, thread_id, statuses={"idle"})
    final_values = (await tenant_a_client.threads.get_state(thread_id)).get("values") or {}
    assert final_values.get("decisions") == ["n-ok"]
    assert (final_values.get("claim_tokens") or []).count("stable-claim:block-c-single") == 1
    assert "claim_tokens_v2" not in final_values


@pytest.mark.asyncio
@pytest.mark.block_c_nn1
async def test_nn1_pre_dispatch_guard_rejects_n_to_n1_and_keeps_parent_immutable(
    tenant_a_client,
    qualification_assistant_id: str,
    n1_assistant_id: str,
) -> None:
    """BellLabs policy via guarded facade: N→N1 never reaches Agent Server."""

    assert n1_assistant_id  # N1 must be deployed; BellLabs policy forbids invoking it here.
    thread = await tenant_a_client.threads.create(
        metadata={"request_scope": "tenant-a", "belllabs_run_id": f"qual-{uuid4().hex}"},
    )
    thread_id = str(thread["thread_id"])
    await tenant_a_client.runs.wait(
        thread_id,
        qualification_assistant_id,
        input=_run_input(scenario="single_interrupt"),
    )
    await wait_thread_status(tenant_a_client, thread_id, statuses={"interrupted"})
    before_state = await tenant_a_client.threads.get_state(thread_id)
    before_values = dict(before_state.get("values") or {})
    before_checkpoint = before_state.get("checkpoint") or before_state.get("checkpoint_id")
    before_history = await tenant_a_client.threads.get_history(thread_id, limit=20)
    before_status = await get_thread_status(tenant_a_client, thread_id)
    assert before_status == "interrupted"
    assert "stable-claim:block-c-single" in (before_values.get("claim_tokens") or [])

    provider_calls = {"n": 0}

    async def tracked_wait(*args: object, **kwargs: object) -> object:
        provider_calls["n"] += 1
        return await tenant_a_client.runs.wait(*args, **kwargs)

    with pytest.raises(IncompatibleResumeRouteError) as raised:
        await guarded_runs_wait(
            source_graph_id=GRAPH_ID_N,
            source_compat_version=COMPAT_VERSION_N,
            target_graph_id=GRAPH_ID_N1,
            target_compat_version=COMPAT_VERSION_N1,
            runs_wait=tracked_wait,
            thread_id=thread_id,
            assistant_id=n1_assistant_id,
            command={"resume": "should-not-apply"},
        )
    assert raised.value.decision.allowed is False
    assert provider_calls["n"] == 0

    after_state = await tenant_a_client.threads.get_state(thread_id)
    after_values = dict(after_state.get("values") or {})
    after_history = await tenant_a_client.threads.get_history(thread_id, limit=20)
    assert after_values == before_values
    assert (after_state.get("checkpoint") or after_state.get("checkpoint_id")) == (
        before_checkpoint
    )
    assert len(after_history) == len(before_history)
    assert await get_thread_status(tenant_a_client, thread_id) == "interrupted"
    assert "claim_tokens_v2" not in after_values
    assert "should-not-apply" not in (after_values.get("decisions") or [])

    # Compatible guarded N-on-N resume still works on the untouched parent.
    result = await guarded_runs_wait(
        source_graph_id=GRAPH_ID_N,
        source_compat_version=COMPAT_VERSION_N,
        target_graph_id=GRAPH_ID_N,
        target_compat_version=COMPAT_VERSION_N,
        runs_wait=tracked_wait,
        thread_id=thread_id,
        assistant_id=qualification_assistant_id,
        command={"resume": "n-after-guard"},
    )
    assert result.decision.allowed is True
    assert provider_calls["n"] == 1
    await wait_thread_status(tenant_a_client, thread_id, statuses={"idle"})
    final_values = (await tenant_a_client.threads.get_state(thread_id)).get("values") or {}
    assert final_values.get("decisions") == ["n-after-guard"]
    assert (final_values.get("claim_tokens") or []).count("stable-claim:block-c-single") == 1


@pytest.mark.asyncio
@pytest.mark.block_c_nn1
async def test_nn1_provider_fail_open_cross_assistant_resume_is_unsafe(
    tenant_a_client,
    qualification_assistant_id: str,
    n1_assistant_id: str,
) -> None:
    """Regression evidence: direct N→N1 resume can mutate schema (provider fail-open).

    Isolated disposable thread only — never reuse for accepted policy proofs.
    Do not weaken: pinned Agent Server 0.12.0 was observed to accept the resume,
    expose ``claim_tokens_v2``, and lose ``claim_tokens``.
    """

    if os.getenv("BLOCK_C_RUN_NN1_PHASE") != "1":
        pytest.skip(
            "Set BLOCK_C_RUN_NN1_PHASE=1 to capture disposable provider fail-open evidence"
        )

    contaminated = await tenant_a_client.threads.create(
        metadata={
            "request_scope": "tenant-a",
            "belllabs_run_id": f"qual-nn1-unsafe-{uuid4().hex}",
            "block_c_contamination": "nn1-fail-open-evidence",
        },
    )
    thread_id = str(contaminated["thread_id"])
    await tenant_a_client.runs.wait(
        thread_id,
        qualification_assistant_id,
        input=_run_input(scenario="single_interrupt"),
    )
    await wait_thread_status(tenant_a_client, thread_id, statuses={"interrupted"})
    before_values = dict(
        (await tenant_a_client.threads.get_state(thread_id)).get("values") or {}
    )
    assert "stable-claim:block-c-single" in (before_values.get("claim_tokens") or [])
    assert "claim_tokens_v2" not in before_values

    # Intentionally bypass the BellLabs pre-dispatch guard to document provider behavior.
    provider_error: APIStatusError | None = None
    try:
        await tenant_a_client.runs.wait(
            thread_id,
            n1_assistant_id,
            command={"resume": "should-not-apply"},
        )
    except APIStatusError as error:
        provider_error = error

    after_state = await tenant_a_client.threads.get_state(thread_id)
    after_values = dict(after_state.get("values") or {})
    after_status = await get_thread_status(tenant_a_client, thread_id)

    if provider_error is not None:
        # Fail-closed would be safer; still record that decisions did not apply.
        assert provider_error.status_code >= 400
        assert "should-not-apply" not in (after_values.get("decisions") or [])
        assert after_values.get("claim_tokens") == before_values.get("claim_tokens")
        pytest.fail(
            "Provider unexpectedly fail-closed on N→N1 resume; update evidence if this "
            "becomes the pinned runtime behavior. "
            f"status={after_status} error={provider_error.status_code}"
        )

    # Observed unsafe fail-open on Agent Server 0.12.0 / SDK 0.4.2:
    # schema shifts toward N+1 channels and N claim_tokens are lost.
    assert "claim_tokens_v2" in after_values, (
        "expected provider fail-open to expose N+1 channel claim_tokens_v2; "
        f"keys={sorted(after_values)} status={after_status}"
    )
    assert not after_values.get("claim_tokens"), (
        "expected provider fail-open to lose N claim_tokens; "
        f"claim_tokens={after_values.get('claim_tokens')!r} status={after_status}"
    )
    # Contaminated thread must not be used for further accepted N-on-N proof.
    assert after_values.get("claim_tokens") != before_values.get("claim_tokens")


@pytest.mark.asyncio
@pytest.mark.block_c_nn1
@pytest.mark.block_c_nn1_deploy
async def test_nn1_deployment_inspect_from_n1_and_guarded_resume_on_n(
    tenant_a_client,
    tenant_a_client_n1,
    qualification_assistant_id: str,
    n1_deployment_assistant_id: str,
) -> None:
    """Two-endpoint drill: N+1 sees thread id but fail-closes state; resume on N only."""

    if os.getenv("BLOCK_C_RUN_NN1_DEPLOYMENT") != "1":
        pytest.skip(
            "Set BLOCK_C_RUN_NN1_DEPLOYMENT=1 with AGENT_SERVER_ENDPOINT and "
            "AGENT_SERVER_ENDPOINT_N1 for two-endpoint deployment qualification"
        )

    thread = await tenant_a_client.threads.create(
        metadata={
            "request_scope": "tenant-a",
            "belllabs_run_id": f"qual-nn1-deploy-{uuid4().hex}",
            "block_c_assembly": "n",
        },
    )
    thread_id = str(thread["thread_id"])
    await tenant_a_client.runs.wait(
        thread_id,
        qualification_assistant_id,
        input=_run_input(scenario="single_interrupt"),
    )
    await wait_thread_status(tenant_a_client, thread_id, statuses={"interrupted"})
    parent_before = await tenant_a_client.threads.get_state(thread_id)
    parent_values = dict(parent_before.get("values") or {})
    parent_checkpoint = parent_before.get("checkpoint") or parent_before.get(
        "checkpoint_id"
    )
    parent_history = await tenant_a_client.threads.get_history(thread_id, limit=20)
    assert "stable-claim:block-c-single" in (parent_values.get("claim_tokens") or [])

    # Shared Postgres: thread row is visible on N+1…
    inspected = await tenant_a_client_n1.threads.get(thread_id)
    assert str(inspected.get("thread_id") or inspected.get("id") or "") == thread_id
    assert await get_thread_status(tenant_a_client_n1, thread_id) == "interrupted"
    # …but checkpoint/state inspection fail-closes without the N graph registered.
    with pytest.raises(NotFoundError) as missing_state:
        await tenant_a_client_n1.threads.get_state(thread_id)
    assert is_missing_n_graph_on_n1_error(missing_state.value)

    # Failed N+1 inspection must leave the N parent immutable. The accepted
    # path never submits a resume to N+1; the disposable test below records
    # the provider's direct cross-assembly behavior separately.
    mid_state = await tenant_a_client.threads.get_state(thread_id)
    mid_values = dict(mid_state.get("values") or {})
    assert mid_values == parent_values
    assert (mid_state.get("checkpoint") or mid_state.get("checkpoint_id")) == (
        parent_checkpoint
    )
    assert len(await tenant_a_client.threads.get_history(thread_id, limit=20)) == len(
        parent_history
    )
    assert await get_thread_status(tenant_a_client, thread_id) == "interrupted"
    assert "should-not-apply" not in (mid_values.get("decisions") or [])
    assert "claim_tokens_v2" not in mid_values

    n1_only = await tenant_a_client_n1.assistants.search(limit=20)
    n1_graph_ids = {str(item.get("graph_id") or "") for item in n1_only}
    assert GRAPH_ID_N1 in n1_graph_ids
    assert GRAPH_ID_N not in n1_graph_ids
    assert n1_deployment_assistant_id

    n_calls = {"n": 0}
    n1_calls = {"n": 0}

    async def wait_n(*args: object, **kwargs: object) -> object:
        n_calls["n"] += 1
        return await tenant_a_client.runs.wait(*args, **kwargs)

    async def wait_n1(*args: object, **kwargs: object) -> object:
        n1_calls["n"] += 1
        return await tenant_a_client_n1.runs.wait(*args, **kwargs)

    result = await guarded_deployment_runs_wait(
        source_graph_id=GRAPH_ID_N,
        source_compat_version=COMPAT_VERSION_N,
        inspection_assembly_role=ASSEMBLY_ROLE_N1,
        runs_wait_by_role={
            ASSEMBLY_ROLE_N: wait_n,
            ASSEMBLY_ROLE_N1: wait_n1,
        },
        assistant_id_by_graph={GRAPH_ID_N: qualification_assistant_id},
        thread_id=thread_id,
        command={"resume": "deploy-n-ok"},
    )
    assert result.decision.allowed is True
    assert result.decision.resume_assembly_role == ASSEMBLY_ROLE_N
    assert result.decision.resume_graph_id == GRAPH_ID_N
    assert result.dispatched_assembly_role == ASSEMBLY_ROLE_N
    assert n_calls["n"] == 1
    assert n1_calls["n"] == 0

    await wait_thread_status(tenant_a_client, thread_id, statuses={"idle"})
    final_values = (await tenant_a_client.threads.get_state(thread_id)).get("values") or {}
    assert final_values.get("decisions") == ["deploy-n-ok"]
    assert (final_values.get("claim_tokens") or []).count("stable-claim:block-c-single") == 1
    assert "claim_tokens_v2" not in final_values
    assert len(await tenant_a_client.threads.get_history(thread_id, limit=50)) >= len(
        parent_history
    )
    assert parent_checkpoint is not None


@pytest.mark.asyncio
@pytest.mark.block_c_nn1
@pytest.mark.block_c_nn1_deploy
async def test_nn1_deployment_direct_resume_on_n1_is_fail_closed_or_isolated(
    tenant_a_client,
    tenant_a_client_n1,
    qualification_assistant_id: str,
    n1_deployment_assistant_id: str,
) -> None:
    """Disposable separate-deployment evidence for direct N→N1 resume attempts.

    Preferred pinned behavior: fail-closed NotFound/API error (N graph absent on
    N+1) with N state unchanged. If a future pin fail-opens, assert isolated
    schema mutation by reading final state from whichever endpoint can.
    """

    if os.getenv("BLOCK_C_RUN_NN1_DEPLOYMENT") != "1":
        pytest.skip(
            "Set BLOCK_C_RUN_NN1_DEPLOYMENT=1 for disposable two-endpoint resume evidence"
        )

    contaminated = await tenant_a_client.threads.create(
        metadata={
            "request_scope": "tenant-a",
            "belllabs_run_id": f"qual-nn1-deploy-direct-{uuid4().hex}",
            "block_c_contamination": "nn1-deployment-direct-resume",
        },
    )
    thread_id = str(contaminated["thread_id"])
    await tenant_a_client.runs.wait(
        thread_id,
        qualification_assistant_id,
        input=_run_input(scenario="single_interrupt"),
    )
    await wait_thread_status(tenant_a_client, thread_id, statuses={"interrupted"})
    before_state = await tenant_a_client.threads.get_state(thread_id)
    before_values = dict(before_state.get("values") or {})
    before_checkpoint = before_state.get("checkpoint") or before_state.get("checkpoint_id")
    assert "stable-claim:block-c-single" in (before_values.get("claim_tokens") or [])

    # Do not require N+1 get_state (fail-closed without N graph). Attempt direct run.
    provider_error: APIStatusError | None = None
    try:
        await tenant_a_client_n1.runs.wait(
            thread_id,
            n1_deployment_assistant_id,
            command={"resume": "should-not-apply"},
        )
    except APIStatusError as error:
        provider_error = error

    if provider_error is not None:
        # Deterministic separate-deployment outcome: fail-closed, N parent unchanged.
        assert provider_error.status_code >= 400
        assert (
            is_missing_n_graph_on_n1_error(provider_error)
            or isinstance(provider_error, NotFoundError)
            or provider_error.status_code in {404, 400, 409, 422}
        )
        after_state = await tenant_a_client.threads.get_state(thread_id)
        unchanged_values = dict(after_state.get("values") or {})
        assert unchanged_values == before_values
        assert (after_state.get("checkpoint") or after_state.get("checkpoint_id")) == (
            before_checkpoint
        )
        assert await get_thread_status(tenant_a_client, thread_id) == "interrupted"
        assert "should-not-apply" not in (unchanged_values.get("decisions") or [])
        assert "claim_tokens_v2" not in unchanged_values
        return

    # Fail-open path (not expected on separate N+1-only deployment): isolate mutation.
    after_values: dict[str, object]
    try:
        after_values = dict(
            (await tenant_a_client.threads.get_state(thread_id)).get("values") or {}
        )
        after_status = await get_thread_status(tenant_a_client, thread_id)
    except APIStatusError:
        after_values = dict(
            (await tenant_a_client_n1.threads.get_state(thread_id)).get("values") or {}
        )
        after_status = await get_thread_status(tenant_a_client_n1, thread_id)
    assert "claim_tokens_v2" in after_values, (
        "unexpected fail-open without N+1 channel mutation; "
        f"keys={sorted(after_values)} status={after_status}"
    )
    assert not after_values.get("claim_tokens"), (
        "unexpected fail-open that retained N claim_tokens; "
        f"claim_tokens={after_values.get('claim_tokens')!r}"
    )


@pytest.mark.asyncio
@pytest.mark.block_c_restart
async def test_restart_phase_prepare_or_resume(
    tenant_a_client,
    qualification_assistant_id: str,
) -> None:
    phase = os.getenv("BLOCK_C_RUN_RESTART_PHASE", "").strip().lower()
    if phase not in {"prepare", "resume"}:
        pytest.skip(
            "Set BLOCK_C_RUN_RESTART_PHASE=prepare|resume and BLOCK_C_RESTART_STATE_PATH"
        )
    state_path = Path(
        os.getenv(
            "BLOCK_C_RESTART_STATE_PATH",
            str(Path.cwd() / ".tmp" / "block_c_restart_state.json"),
        )
    )
    if phase == "prepare":
        thread = await tenant_a_client.threads.create(
            metadata={
                "request_scope": "tenant-a",
                "belllabs_run_id": f"qual-restart-{uuid4().hex}",
            },
        )
        thread_id = str(thread["thread_id"])
        await tenant_a_client.runs.wait(
            thread_id,
            qualification_assistant_id,
            input=_run_input(scenario="single_interrupt"),
        )
        await wait_thread_status(tenant_a_client, thread_id, statuses={"interrupted"})
        state = await tenant_a_client.threads.get_state(thread_id)
        checkpoint = state.get("checkpoint") or {}
        runs = await tenant_a_client.runs.list(thread_id, limit=10)
        assert runs
        checkpoint_id = str(
            checkpoint.get("checkpoint_id") or state.get("checkpoint_id") or ""
        )
        assert checkpoint_id
        write_restart_state(
            state_path,
            {
                "thread_id": thread_id,
                "assistant_id": qualification_assistant_id,
                "run_id": str(runs[0]["run_id"]),
                "checkpoint_id": checkpoint_id,
                "claim_tokens": (state.get("values") or {}).get("claim_tokens"),
                "status": await get_thread_status(tenant_a_client, thread_id),
            },
        )
        saved = read_restart_state(state_path)
        assert saved["thread_id"] == thread_id
        return

    payload = read_restart_state(state_path)
    thread_id = str(payload["thread_id"])
    assistant_id = str(payload["assistant_id"])
    state = await tenant_a_client.threads.get_state(thread_id)
    assert await get_thread_status(tenant_a_client, thread_id) == "interrupted"
    assert "stable-claim:block-c-single" in (
        (state.get("values") or {}).get("claim_tokens") or []
    )
    await tenant_a_client.runs.wait(
        thread_id,
        assistant_id,
        command={"resume": "post-restart-approval"},
    )
    await wait_thread_status(tenant_a_client, thread_id, statuses={"idle"})
    final_values = (await tenant_a_client.threads.get_state(thread_id)).get("values") or {}
    assert final_values.get("decisions") == ["post-restart-approval"]
    assert (final_values.get("claim_tokens") or []).count("stable-claim:block-c-single") == 1
