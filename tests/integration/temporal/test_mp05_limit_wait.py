"""MP-05 capacity waits on the real local Temporal server (127.0.0.1:7233, `make temporal-up`).

A probe workflow plans a provider rate-limit response with `plan_limit_response` and waits it
out through `wait_for_limit_reset` with `workflow.wait_condition` as the wait primitive - the
same wiring proposed for the operation workflow. It proves on real Temporal timers that the
wait is bounded by the run deadline, that a cancel Signal wakes it immediately (so the cancel
path reaches pending work), and that a reset after the deadline never starts a wait. The
limit signal is a SYNTHETIC FIXTURE; no provider is contacted.
"""

from __future__ import annotations

import asyncio
import os
import time
from datetime import timedelta
from typing import Any
from uuid import uuid4

from temporalio import workflow
from temporalio.client import Client
from temporalio.worker import Worker

from mission_control.adapters.temporal.workflow_sandbox import coordinator_workflow_runner

with workflow.unsafe.imports_passed_through():
    from mission_control.application.execution.usage_admission import (
        LimitWaitPolicy,
        ProviderLimitSignal,
        plan_limit_response,
        wait_for_limit_reset,
    )

ADDRESS = os.environ.get("MC_TEMPORAL_TEST_ADDRESS", "127.0.0.1:7233")
NAMESPACE = os.environ.get("MC_TEMPORAL_TEST_NAMESPACE", "default")


@workflow.defn(name="mc.mp05.limit_wait_probe")
class LimitWaitProbe:
    def __init__(self) -> None:
        self._cancel = False
        self._pending_dispatched = False

    @workflow.signal
    def cancel(self) -> None:
        self._cancel = True

    @workflow.run
    async def run(self, args: dict[str, Any]) -> dict[str, Any]:
        start = workflow.now()
        deadline = start + timedelta(seconds=args["deadline_s"])
        signal = ProviderLimitSignal(
            lane_profile="codex",
            kind="rate_limited",
            resets_at=start + timedelta(seconds=args["reset_in_s"]),
            source="fixture",
            fixture=True,
        )
        decision = plan_limit_response(
            signal, now=start, deadline=deadline, policy=LimitWaitPolicy(reset_margin_s=0)
        )
        if decision.disposition != "wait":
            return {"decision": decision.code, "waited": False}

        async def wait(predicate: Any, timeout_s: float) -> bool:
            try:
                await workflow.wait_condition(predicate, timeout=timedelta(seconds=timeout_s))
            except TimeoutError:
                return False
            return True

        outcome = await wait_for_limit_reset(
            decision,
            now=workflow.now,
            deadline=deadline,
            wait=wait,
            cancelled=lambda: self._cancel,
        )
        if outcome.outcome == "reset_elapsed" and not self._cancel:
            self._pending_dispatched = True
        return {
            "decision": decision.code,
            "outcome": outcome.outcome,
            "waited_s": outcome.waited_s,
            "waits_used": outcome.ledger.waits_used,
            "pending_dispatched": self._pending_dispatched,
        }


async def _run(client: Client, args: dict[str, Any], *, cancel_after_s: float | None) -> Any:
    queue = f"mp05-limit-wait-{uuid4().hex[:8]}"
    async with Worker(
        client,
        task_queue=queue,
        workflows=[LimitWaitProbe],
        workflow_runner=coordinator_workflow_runner(),
    ):
        handle = await client.start_workflow(
            LimitWaitProbe.run,
            args,
            id=f"mp05-limit-wait-{uuid4().hex}",
            task_queue=queue,
            execution_timeout=timedelta(minutes=5),
        )
        if cancel_after_s is not None:
            await asyncio.sleep(cancel_after_s)
            await handle.signal(LimitWaitProbe.cancel)
        return await handle.result()


async def test_cancel_signal_wakes_a_long_reset_wait_on_real_temporal() -> None:
    client = await Client.connect(ADDRESS, namespace=NAMESPACE)
    began = time.monotonic()
    result = await _run(client, {"deadline_s": 3600, "reset_in_s": 600}, cancel_after_s=1.0)
    assert result["outcome"] == "cancelled"
    assert result["pending_dispatched"] is False
    assert result["waited_s"] < 60
    assert time.monotonic() - began < 60


async def test_short_reset_elapses_on_a_real_timer() -> None:
    client = await Client.connect(ADDRESS, namespace=NAMESPACE)
    result = await _run(client, {"deadline_s": 3600, "reset_in_s": 2}, cancel_after_s=None)
    assert result["outcome"] == "reset_elapsed"
    assert result["waited_s"] >= 1.5
    assert result["waits_used"] == 1
    assert result["pending_dispatched"] is True


async def test_reset_after_deadline_never_starts_a_wait() -> None:
    client = await Client.connect(ADDRESS, namespace=NAMESPACE)
    began = time.monotonic()
    result = await _run(client, {"deadline_s": 60, "reset_in_s": 7200}, cancel_after_s=None)
    assert result == {"decision": "CAPACITY_RESET_AFTER_DEADLINE", "waited": False}
    assert time.monotonic() - began < 30
