"""RRM-009: the production composition of RRM-008's running cancellation.

Hermetic proofs of the composition seams; the end-to-end proofs on the production stack are
`tests/acceptance/control_plane/test_rrm_009_production_cancellation.py`.

* heartbeat timeout per operation class and a worker drain shorter than every timeout;
* the cancel path's credential-resolving `AsyncChildCancellationPort`;
* `DeploymentOperationRuntime` never swallows `CancelledError` and forwards `observe_latest`
  inside the operation's granted egress;
* the generic artifact worker serves `operation.cancel` beside `operation.execute`;
* the `liability_reconciled` hint after operator decisions, only to a cancelling family;
* the API's privileged usage reconciliation of a cancelled child.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, cast

import pytest

from app.application.async_subagents.service import AsyncSubagentError
from app.application.async_subagents.usage_reconciliation import (
    AsyncChildUsageReconciliation,
    ProviderNotComposed,
)
from app.application.run_control.liability_hints import (
    LIABILITY_DECISION_KINDS,
    FamilyLiabilityHints,
)
from app.config import get_settings
from app.domain.control_plane.contracts import SecretRef
from app.domain.operation_execution.heartbeats import (
    DEFAULT_OPERATION_HEARTBEATS,
    OperationHeartbeatPolicy,
    cancel_latency_bound_seconds,
    operation_heartbeat_class,
)
from app.domain.run_control.contracts import CancelAction, RunPhase
from app.temporal.artifact_activities import generic_artifact_activities
from app.temporal.worker import operation_heartbeat_policy
from tests.unit.operations.test_operation_execution import operation_request
from tests.unit.run_control.test_boundary_commands import FAMILY_WORKFLOW_ID, TARGET, started
from tests.unit.run_control.test_run_control import command, service


def _deep(*contracts: str) -> Any:
    return SimpleNamespace(deep_agent_binding=SimpleNamespace(async_subagents=contracts))


# --- Heartbeat timeout per operation class ------------------------------------------------


def test_heartbeat_timeout_is_chosen_per_operation_class() -> None:
    policy = OperationHeartbeatPolicy(
        deep_agent_seconds=30, deep_agent_async_children_seconds=15, bound_seconds=45
    )
    bound = operation_request()
    assert bound.deep_agent_binding is None
    assert operation_heartbeat_class(bound) == "bound"
    assert operation_heartbeat_class(_deep()) == "deep_agent"
    assert operation_heartbeat_class(_deep("contract")) == "deep_agent_async_children"
    assert policy.timeout_for(bound) == 45
    assert policy.timeout_for(_deep()) == 30
    assert policy.timeout_for(_deep("contract")) == 15
    assert policy.shortest_seconds == 15
    # The default policy is the contract's default: a family composed without a policy
    # declares exactly what it declared before RRM-009.
    assert set(DEFAULT_OPERATION_HEARTBEATS.timeouts().values()) == {30}
    with pytest.raises(ValueError, match="between"):
        OperationHeartbeatPolicy(bound_seconds=0)


def test_cancel_latency_bound_is_the_sdk_heartbeat_throttle() -> None:
    # temporalio sends at most one heartbeat per 0.8 * timeout, capped at 60 s.
    assert cancel_latency_bound_seconds(30) == pytest.approx(24.0)
    assert cancel_latency_bound_seconds(15) == pytest.approx(12.0)
    assert cancel_latency_bound_seconds(600) == 60.0
    disclosure = OperationHeartbeatPolicy(deep_agent_async_children_seconds=15).disclosure()
    assert disclosure["deep_agent_async_children"] == {
        "heartbeat_timeout_seconds": 15,
        "cancel_latency_bound_seconds": pytest.approx(12.0),
    }


def test_worker_drain_must_be_shorter_than_every_heartbeat_timeout() -> None:
    policy = OperationHeartbeatPolicy(deep_agent_async_children_seconds=15)
    policy.verify_graceful_shutdown(10)
    policy.verify_graceful_shutdown(0)
    for drain in (15, 20):
        with pytest.raises(ValueError, match="shorter than the shortest"):
            policy.verify_graceful_shutdown(drain)
    with pytest.raises(ValueError, match="non-negative"):
        policy.verify_graceful_shutdown(-1)


def test_deployment_settings_compose_the_policy_and_its_drain(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    get_settings.cache_clear()
    try:
        defaults = get_settings()
        policy = operation_heartbeat_policy(defaults)
        assert policy.timeouts() == {
            "deep_agent": 30,
            "deep_agent_async_children": 15,
            "bound": 30,
        }
        assert defaults.worker_graceful_shutdown_seconds == 10
        policy.verify_graceful_shutdown(defaults.worker_graceful_shutdown_seconds)
        monkeypatch.setenv("OPERATION_ASYNC_CHILDREN_HEARTBEAT_TIMEOUT_SECONDS", "8")
        get_settings.cache_clear()
        tightened = get_settings()
        with pytest.raises(ValueError, match="shorter than the shortest"):
            operation_heartbeat_policy(tightened).verify_graceful_shutdown(
                tightened.worker_graceful_shutdown_seconds
            )
    finally:
        get_settings.cache_clear()


# --- Worker surfaces -----------------------------------------------------------------------


def test_generic_artifact_worker_serves_the_cancel_beside_the_execute() -> None:
    operations = SimpleNamespace(execute="operation.execute", cancel="operation.cancel")
    artifacts = SimpleNamespace(promote="artifact.promote")
    assert generic_artifact_activities(cast(Any, operations), cast(Any, artifacts)) == (
        "operation.execute",
        "operation.cancel",
        "artifact.promote",
    )


# --- The cancel path's async-child port ----------------------------------------------------


@pytest.mark.asyncio
async def test_child_cancellation_resolves_the_operation_credential_only_when_children_exist(
) -> None:
    from app.temporal.deployment_composition import ProductionAsyncChildCancellation

    calls: list[Any] = []
    children: dict[str, tuple[str, ...]] = {"binding-quiet": (), "binding-parent": ("child-1",)}

    class Authority:
        async def list_child_ids(self, request_scope: str, binding_id: str) -> tuple[str, ...]:
            calls.append(("list", request_scope, binding_id))
            return children[binding_id]

    class Secrets:
        async def resolve(self, refs: tuple[SecretRef, ...]) -> dict[str, str]:
            calls.append(("resolve", tuple(f"{ref.provider}:{ref.key}" for ref in refs)))
            return {f"{ref.provider}:{ref.key}": "resolved" for ref in refs}

    class Service:
        async def cancel_children(self, binding: Any, *, reason: str, requested_at: Any) -> Any:
            calls.append(("cancel_children", binding.binding_id, reason, requested_at))
            return ("record",)

    class Factory:
        def service(self, binding: Any, secrets: Any) -> tuple[Any, Any]:
            calls.append(("service", binding.binding_id, dict(secrets)))
            return Service(), object()

    port = ProductionAsyncChildCancellation(
        cast(Any, Factory()), cast(Any, Authority()), cast(Any, Secrets())
    )
    token = SecretRef(provider="environment", key="BELLABS_ASYNC_SUBAGENT_SERVER_TOKEN")
    at = datetime(2026, 10, 2, tzinfo=UTC)
    quiet = SimpleNamespace(binding_id="binding-quiet", request_scope="tenant-1", secret_refs=())
    assert await port.cancel_children(cast(Any, quiet), reason="r", requested_at=at) == ()
    # No child: no secret resolved, no provider service built.
    assert calls == [("list", "tenant-1", "binding-quiet")]
    calls.clear()
    parent = SimpleNamespace(
        binding_id="binding-parent", request_scope="tenant-1", secret_refs=(token,)
    )
    assert await port.cancel_children(cast(Any, parent), reason="r", requested_at=at) == (
        "record",
    )
    assert calls == [
        ("list", "tenant-1", "binding-parent"),
        ("resolve", ("environment:BELLABS_ASYNC_SUBAGENT_SERVER_TOKEN",)),
        (
            "service",
            "binding-parent",
            {"environment:BELLABS_ASYNC_SUBAGENT_SERVER_TOKEN": "resolved"},
        ),
        ("cancel_children", "binding-parent", "r", at),
    ]


# --- DeploymentOperationRuntime under cancellation ---------------------------------------


def _invocation(*contracts: str) -> Any:
    return SimpleNamespace(
        binding=SimpleNamespace(
            binding_id="binding-1",
            capability_grant=SimpleNamespace(network_hosts=frozenset({"example.com"})),
            deep_agent_binding=SimpleNamespace(async_subagents=contracts, execution_generation=1),
        )
    )


@pytest.mark.asyncio
async def test_deployment_runtime_never_swallows_a_cancel_during_cognition_or_the_child_wait(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import app.temporal.deployment_composition as composition
    from app.domain.operation_execution.contracts import RuntimeResult
    from app.integrations.agents.deep_agents.browser_tool import GRANTED_NETWORK_HOSTS

    stage = {"cognition_blocks": True}
    entered = asyncio.Event()

    class Inner:
        async def execute(self, invocation: Any, secrets: Any) -> RuntimeResult:
            del invocation, secrets
            if stage["cognition_blocks"]:
                entered.set()
                await asyncio.Event().wait()
            return RuntimeResult(output_text="done")

    class Children:
        def service(self, binding: Any, secrets: Any) -> tuple[Any, Any]:
            return object(), object()

    class Completion:
        def __init__(self, service: Any, lineage: Any, **kwargs: Any) -> None:
            pass

        async def complete(self, binding: Any, *, execution_generation: int) -> tuple[Any, ...]:
            entered.set()
            await asyncio.Event().wait()  # the child is still running
            raise AssertionError("unreachable")

    monkeypatch.setattr(composition, "AsyncChildCompletion", Completion)
    monkeypatch.setattr(composition, "PostgresAsyncSubagentAuthority", lambda pool: pool)
    runtime = composition.DeploymentOperationRuntime(
        cast(Any, Inner()), cast(Any, Children()), pool=cast(Any, "pool"), policies={},
        wait_seconds=5,
    )
    for cognition_blocks in (True, False):
        stage["cognition_blocks"] = cognition_blocks
        entered.clear()
        task = asyncio.create_task(runtime.execute(_invocation("contract"), {}))
        await asyncio.wait_for(entered.wait(), timeout=5)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert task.cancelled()
        # The egress grant does not leak out of the cancelled step.
        assert GRANTED_NETWORK_HOSTS.get() is None


@pytest.mark.asyncio
async def test_deployment_runtime_observes_the_latest_checkpoint_inside_the_granted_egress(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import app.temporal.deployment_composition as composition
    from app.domain.operation_execution.contracts import RuntimeResult
    from app.integrations.agents.deep_agents.browser_tool import GRANTED_NETWORK_HOSTS

    observed: list[Any] = []

    class Inner:
        async def observe_latest(self, invocation: Any, secrets: Any) -> RuntimeResult:
            granted = GRANTED_NETWORK_HOSTS.get()
            observed.append((sorted(granted) if granted is not None else None, dict(secrets)))
            return RuntimeResult(output_text="latest")

        def unrelated(self) -> str:
            return "delegated"

    runtime = composition.DeploymentOperationRuntime(
        cast(Any, Inner()), cast(Any, object()), pool=cast(Any, "pool"), policies={},
        wait_seconds=5,
    )
    result = await runtime.observe_latest(_invocation(), {"environment:KEY": "v"})
    assert result.output_text == "latest"
    assert observed == [(["example.com"], {"environment:KEY": "v"})]
    assert GRANTED_NETWORK_HOSTS.get() is None
    assert runtime.unrelated() == "delegated"  # every other capability is the adapter's


# --- liability_reconciled -----------------------------------------------------------------


class RecordingHints:
    def __init__(self, error: Exception | None = None) -> None:
        self.sent: list[tuple[str, str]] = []
        self.error = error

    async def liability_reconciled(self, family_workflow_id: str, reference: str) -> None:
        if self.error is not None:
            raise self.error
        self.sent.append((family_workflow_id, reference))


@pytest.mark.asyncio
async def test_liability_hint_reaches_only_a_cancelling_family() -> None:
    run_service, _ = service()
    run_id = await started(run_service, "hint-run", TARGET)
    transport = RecordingHints()
    hints = FamilyLiabilityHints(run_service, transport)
    # Active run: nothing waits on a liability, so nothing is sent.
    assert await hints.notify("tenant-1", run_id, "usage") is False
    assert transport.sent == []
    cancelled = await run_service.execute(command(run_id, 2, "cancel", CancelAction()))
    assert cancelled.phase == RunPhase.CANCELLING
    assert await hints.notify("tenant-1", run_id, "async-child-usage:c1") is True
    assert transport.sent == [(FAMILY_WORKFLOW_ID, "async-child-usage:c1")]
    # A hint the transport cannot deliver is not an error of the decision (timer fallback).
    failing = FamilyLiabilityHints(run_service, RecordingHints(RuntimeError("down")))
    assert await failing.notify("tenant-1", run_id, "usage") is False
    # Run without an execution target (no family): nothing to signal.
    bare = await started(run_service, "hint-bare", None)
    await run_service.execute(command(bare, 2, "cancel-bare", CancelAction()))
    assert await hints.notify("tenant-1", bare, "usage") is False
    assert {"settle_effect", "settle_pending_usage", "decide_async_child_fact"} <= (
        LIABILITY_DECISION_KINDS
    )


@pytest.mark.asyncio
async def test_usage_reconciliation_is_bound_to_the_run_and_hints_once_settled() -> None:
    reconciled: list[Any] = []

    class Service:
        def __init__(self, settled: bool) -> None:
            self.settled = settled

        async def execution(self, request_scope: str, child_id: str) -> Any:
            return SimpleNamespace(parent_run_id="run-1")

        async def reconcile_usage(self, request_scope: str, child_id: str, **kwargs: Any) -> Any:
            reconciled.append((child_id, kwargs["settlement_ref"]))
            return SimpleNamespace(settled=self.settled)

    class Hints:
        def __init__(self) -> None:
            self.calls: list[tuple[str, str, str]] = []

        async def notify(self, request_scope: str, run_id: str, reference: str) -> bool:
            self.calls.append((request_scope, run_id, reference))
            return True

    hints = Hints()
    reconciliation = AsyncChildUsageReconciliation(cast(Any, Service(True)), cast(Any, hints))
    arguments: dict[str, Any] = {
        "actor": object(),
        "run_usage": {},
        "settlement_ref": "settlement:c1:reconciled",
        "reconciled_at": datetime.now(UTC),
    }
    with pytest.raises(AsyncSubagentError, match="another run"):
        await reconciliation.reconcile_usage("tenant-1", "run-2", "c1", **arguments)
    assert reconciled == [] and hints.calls == []
    link, hinted = await reconciliation.reconcile_usage("tenant-1", "run-1", "c1", **arguments)
    assert link.settled and hinted
    assert hints.calls == [("tenant-1", "run-1", "async-child-usage:c1")]
    still_pending = AsyncChildUsageReconciliation(cast(Any, Service(False)), cast(Any, hints))
    _link, hinted = await still_pending.reconcile_usage("tenant-1", "run-1", "c1", **arguments)
    assert hinted is False and len(hints.calls) == 1  # nothing settled, nothing to wake


@pytest.mark.asyncio
async def test_the_api_reconciliation_never_reaches_the_provider() -> None:
    provider = ProviderNotComposed()
    with pytest.raises(AsyncSubagentError, match="not composed"):
        await provider.cancel(cast(Any, None), cast(Any, None))
    with pytest.raises(AsyncSubagentError, match="not composed"):
        await provider.check(cast(Any, None), cast(Any, None))
