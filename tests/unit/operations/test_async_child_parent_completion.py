"""RRM-009: the parent operation boundary completes the async children it spawned.

`AsyncChildCompletion` (REQ-CP-DA-011, REQ-CP-RUN-009) reconciles each child of the parent
binding, records the decision the contract's registered admission policy yields for a
blocking child, and settles the terminal child against the parent budget exactly once. A
child that is still active, or whose policy the deployment does not register, is left
undecided and unsettled.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import pytest

from mission_control.application.subordinates.parent_completion import (
    AsyncChildCompletion,
    admit_typed_manifest,
)
from mission_control.application.subordinates.parent_effects import (
    RunControlAsyncChildEffects,
    async_child_effect_id,
)
from mission_control.application.subordinates.service import (
    AsyncSubagentService,
    InMemoryAsyncSubagentAuthority,
    InMemoryAsyncSubagentDetailRepository,
)
from mission_control.domain.execution.contracts import OperationExecutionBinding
from mission_control.domain.policies.contracts import EffectDisposition
from tests.acceptance.control_plane.test_wp_cp_045 import DeterministicProvider
from tests.unit.operations.test_async_child_parent_budget import admitted_parent, spawn
from tests.unit.run_control.test_run_control import actor

POLICY = "policy:async-result:v1"


class Lineage:
    def __init__(self, *child_ids: str, parent_binding_id: str = "binding-1") -> None:
        self.child_ids = list(child_ids)
        self.parent_binding_id = parent_binding_id

    async def list_children(self, request_scope: str, parent_run_id: str) -> tuple[Any, ...]:
        del request_scope, parent_run_id
        return (
            SimpleNamespace(child_execution_id="foreign", parent_binding_id="another-binding"),
            *(
                SimpleNamespace(child_execution_id=item, parent_binding_id=self.parent_binding_id)
                for item in self.child_ids
            ),
        )


async def _spawned(run_id: str, run_control: Any) -> tuple[AsyncSubagentService, Any, str]:
    provider = DeterministicProvider([])
    service = AsyncSubagentService(
        InMemoryAsyncSubagentDetailRepository(),
        InMemoryAsyncSubagentAuthority(),
        provider,
        parent_effects=RunControlAsyncChildEffects(run_control, actor=actor()),
        allow_new_spawns=True,
    )
    child = await service.spawn(spawn(run_id))
    return service, provider, child.child_execution_id


def _binding(run_id: str) -> OperationExecutionBinding:
    return cast(
        OperationExecutionBinding,
        SimpleNamespace(request_scope="tenant-1", run_id=run_id, binding_id="binding-1"),
    )


@pytest.mark.asyncio
async def test_completed_blocking_child_is_admitted_and_settled_once_at_the_boundary() -> None:
    run_control, run_id = await admitted_parent()
    service, provider, child_id = await _spawned(run_id, run_control)
    provider.next_status = "success"
    completion = AsyncChildCompletion(
        service, Lineage(child_id), policies={POLICY: admit_typed_manifest}, poll_seconds=0.01
    )

    (record,) = await completion.complete(_binding(run_id), execution_generation=1)

    assert record.as_payload() == {
        "child_execution_id": child_id,
        "lifecycle": "completed",
        "dependency_class": "required_blocking",
        "result_decision": "admit",
        "usage_disposition": "settled",
        "reason": None,
    }
    effects = await run_control.get_effects("tenant-1", run_id)  # type: ignore[attr-defined]
    assert effects.claims[async_child_effect_id(child_id)].disposition == (
        EffectDisposition.SUCCEEDED
    )
    # A retried or reconstructed boundary completes the same child the same way.
    (again,) = await completion.complete(_binding(run_id), execution_generation=1)
    assert again == record
    link = await service.link("tenant-1", child_id)
    assert link.result_decision == "admit" and link.settlement_revision == 1


@pytest.mark.asyncio
async def test_active_child_and_unregistered_policy_are_left_undecided_and_unsettled() -> None:
    run_control, run_id = await admitted_parent()
    service, provider, child_id = await _spawned(run_id, run_control)

    running = AsyncChildCompletion(
        service, Lineage(child_id), policies={POLICY: admit_typed_manifest}, wait_seconds=0
    )
    (record,) = await running.complete(_binding(run_id), execution_generation=1)
    assert (record.lifecycle, record.result_decision, record.usage_disposition, record.reason) == (
        "running",
        None,
        "unsettled",
        "child_running",
    )

    provider.next_status = "success"
    unregistered = AsyncChildCompletion(service, Lineage(child_id), policies={}, poll_seconds=0.01)
    (record,) = await unregistered.complete(_binding(run_id), execution_generation=1)
    assert (record.lifecycle, record.result_decision, record.reason) == (
        "completed",
        None,
        "admission_policy_not_registered",
    )
    link = await service.link("tenant-1", child_id)
    assert link.result_decision is None and not link.settled
    effects = await run_control.get_effects("tenant-1", run_id)  # type: ignore[attr-defined]
    assert effects.claims[async_child_effect_id(child_id)].settlement is None


@pytest.mark.asyncio
async def test_failed_child_is_rejected_and_its_unattributed_usage_stays_pending() -> None:
    run_control, run_id = await admitted_parent()
    service, provider, child_id = await _spawned(run_id, run_control)
    provider.next_status = "error"
    completion = AsyncChildCompletion(
        service, Lineage(child_id), policies={POLICY: admit_typed_manifest}, poll_seconds=0.01
    )

    (record,) = await completion.complete(_binding(run_id), execution_generation=1)

    assert (record.lifecycle, record.result_decision, record.usage_disposition) == (
        "failed",
        "reject",
        "pending_usage",
    )
    effects = await run_control.get_effects("tenant-1", run_id)  # type: ignore[attr-defined]
    assert effects.claims[async_child_effect_id(child_id)].settlement is None


def test_completion_bounds_are_validated() -> None:
    with pytest.raises(ValueError):
        AsyncChildCompletion(
            cast(Any, None), Lineage(), policies={}, wait_seconds=-1, poll_seconds=1
        )
    with pytest.raises(ValueError):
        AsyncChildCompletion(cast(Any, None), Lineage(), policies={}, poll_seconds=0)


@pytest.mark.asyncio
async def test_deployment_runtime_completes_children_after_cognition_and_delegates_the_rest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import mission_control.adapters.temporal.deployment_composition as composition
    from mission_control.adapters.deep_agents.browser_tool import GRANTED_NETWORK_HOSTS
    from mission_control.domain.execution.contracts import RuntimeResult

    calls: list[str] = []

    class Inner:
        async def execute(self, invocation: Any, secrets: Any) -> RuntimeResult:
            del invocation, secrets
            granted = GRANTED_NETWORK_HOSTS.get()
            calls.append(f"cognition:{sorted(granted) if granted is not None else 'unbound'}")
            return RuntimeResult(output_text="done", event_payloads=({"kind": "inspection"},))

        async def observe_latest(self, invocation: Any, secrets: Any) -> RuntimeResult:
            del secrets
            granted = GRANTED_NETWORK_HOSTS.get()
            calls.append(f"observe:{sorted(granted) if granted is not None else 'unbound'}")
            return RuntimeResult(output_text="delegated")

    class Children:
        def service(self, binding: Any, secrets: Any) -> tuple[Any, Any]:
            calls.append(f"service:{binding.binding_id}:{sorted(secrets)}")
            return object(), object()

    class Completion:
        def __init__(self, service: Any, lineage: Any, **kwargs: Any) -> None:
            assert kwargs["wait_seconds"] == 5 and set(kwargs["policies"]) == {POLICY}

        async def complete(self, binding: Any, *, execution_generation: int) -> tuple[Any, ...]:
            calls.append(f"complete:{execution_generation}")
            return (SimpleNamespace(as_payload=lambda: {"child_execution_id": "child-1"}),)

    monkeypatch.setattr(composition, "AsyncChildCompletion", Completion)
    monkeypatch.setattr(composition, "PostgresAsyncSubagentAuthority", lambda pool: pool)
    runtime = composition.DeploymentOperationRuntime(
        cast(Any, Inner()),
        cast(Any, Children()),
        pool=cast(Any, "pool"),
        policies={POLICY: admit_typed_manifest},
        wait_seconds=5,
    )
    with_children = SimpleNamespace(
        binding=SimpleNamespace(
            binding_id="binding-1",
            capability_grant=SimpleNamespace(network_hosts=frozenset({"example.com"})),
            deep_agent_binding=SimpleNamespace(
                async_subagents=("contract",), execution_generation=2
            ),
        )
    )
    result = await runtime.execute(cast(Any, with_children), {"environment:TOKEN": "x"})
    # Cognition ran with the operation's granted hosts bound for its governed browser tool.
    assert calls == [
        "cognition:['example.com']",
        "service:binding-1:['environment:TOKEN']",
        "complete:2",
    ]
    assert GRANTED_NETWORK_HOSTS.get() is None
    assert result.event_payloads == (
        {"kind": "inspection"},
        {
            "kind": composition.ASYNC_CHILD_COMPLETION_KIND,
            "binding_id": "binding-1",
            "children": [{"child_execution_id": "child-1"}],
        },
    )
    calls.clear()
    without_children = SimpleNamespace(
        binding=SimpleNamespace(
            binding_id="binding-2",
            capability_grant=SimpleNamespace(network_hosts=frozenset()),
            deep_agent_binding=SimpleNamespace(async_subagents=(), execution_generation=1),
        )
    )
    plain = await runtime.execute(cast(Any, without_children), {})
    assert calls == ["cognition:[]"] and plain.event_payloads == ({"kind": "inspection"},)
    # RRM-008's `observe_latest` is the adapter's, inside the operation's granted egress.
    calls.clear()
    latest = await runtime.observe_latest(cast(Any, with_children), {})
    assert latest.output_text == "delegated" and calls == ["observe:['example.com']"]
