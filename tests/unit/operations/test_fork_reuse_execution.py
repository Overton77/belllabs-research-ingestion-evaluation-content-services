"""RRM-006: a fork-derived unit settles by the immutable source result (REQ-CP-EXEC-012).

The source unit runs through the real operation boundary (a real `create_deep_agent` graph
with the deterministic scripted model, the journaled coordinator, checkpoint lineage). The
fork is admitted through run control; the derived run's matching unit then settles by
reference: no model call, no checkpoint in its new namespace, zero usage of its own, and a
settlement manifest that names the reused source result. An invalidated unit re-executes.
Every inconsistency fails closed instead of falling back to re-execution.
"""

from __future__ import annotations

import json
from typing import Any

import pytest

from app.application.runtime.run_forks import (
    ForkOfRun,
    ForkPatchPolicyRegistry,
    ForkReuseResolver,
    InMemoryForkMaterializationStore,
    reuse_compatibility_digest,
)
from app.application.workspaces.artifact_promotion import ArtifactPayloadAddress
from app.domain.operation_execution.checkpoint_lineage import cognitive_session_namespace
from app.domain.operation_execution.contracts import OperationSettlement
from app.domain.run_control.contracts import CommandStatus, StartAction
from app.domain.run_control.forks import ForkRejected, derived_unit_identity
from tests.fixtures.checkpoint_recovery import recovery_harness, stage_recovery_unit
from tests.fixtures.run_forks import (
    FakeForkSourceReader,
    compose_in_memory_forks,
    fork_command,
    inspection_reads,
    review_objective_patch,
    stage_policy,
    stagegraph_head,
)
from tests.unit.run_control.test_run_control import WORKFLOW_DIGEST, command

SCOPE = "tenant-1"


class LateResolver:
    """Lets the harness compose its service before the fork store exists."""

    def __init__(self) -> None:
        self.inner: ForkReuseResolver | None = None

    async def reused_result(self, binding: Any) -> Any:
        return None if self.inner is None else await self.inner.reused_result(binding)


@pytest.mark.asyncio
async def test_derived_unit_reuses_the_source_result_and_invalidated_unit_reexecutes() -> None:
    resolver = LateResolver()
    harness = await recovery_harness(fork_reuse=resolver)
    source_draft = stage_recovery_unit(harness.run_id, "draft")
    source_result = await harness.run(await harness.request(source_draft))
    assert source_result.status == "completed"
    calls_after_source = len(harness.model.calls)

    sources = FakeForkSourceReader(
        harness.run_control,
        heads={
            harness.run_id: (stagegraph_head(stages={"draft": "completed", "review": "blocked"}),)
        },
    )
    policies = ForkPatchPolicyRegistry()
    policies.register(WORKFLOW_DIGEST, stage_policy())
    forks = compose_in_memory_forks(
        harness.run_control,
        harness.repository,
        inspection_reads(harness.repository, harness.lineage, harness.journal),
        sources,
        policies=policies,
    )
    assert harness.results is not None and harness.bindings is not None
    resolver.inner = ForkReuseResolver(
        forks.store, results=harness.results, bindings=harness.bindings
    )
    snapshot = await forks.snapshots.take(SCOPE, harness.run_id)
    receipt = await forks.forks.fork(
        fork_command(
            snapshot, changes=(review_objective_patch(),), invalidation_frontier=("review",)
        )
    )
    derived_run = receipt.target_run_id
    started = await harness.run_control.execute(
        command(derived_run, 1, "derived-start", StartAction())
    )
    assert started.status == CommandStatus.ACCEPTED

    source_namespace_checkpoints = len(
        [
            item
            async for item in harness.saver.alist(
                {"configurable": {"thread_id": cognitive_session_namespace(source_draft, 1)}}
            )
        ]
    )
    harness.run_id = derived_run
    derived_draft = derived_unit_identity(source_draft, derived_run)
    assert derived_draft == stage_recovery_unit(derived_run, "draft")
    reused = await harness.run(await harness.request(derived_draft))

    # Settled by reference: no cognition, no checkpoint, no usage of its own.
    assert reused.status == "completed"
    assert reused.output_text == source_result.output_text
    assert reused.output_refs == source_result.output_refs
    assert reused.usage.amounts == {}
    assert reused.checkpoint_transition_id is None and reused.result_checkpoint is None
    assert len(harness.model.calls) == calls_after_source
    derived_namespace = cognitive_session_namespace(derived_draft, 1)
    assert [
        item
        async for item in harness.saver.alist({"configurable": {"thread_id": derived_namespace}})
    ] == []
    observation = harness.lineage.results[(SCOPE, derived_draft.unit_key, 1)]
    assert observation.status == "completed"
    assert observation.checkpoint_transition_id is None
    candidate = snapshot.reuse_candidates[0]
    # A distinct result manifest that names the reused, immutable source result.
    assert observation.result_manifest_ref != candidate.result_manifest_ref
    manifest = json.loads(
        await harness.results.retrieve(
            ArtifactPayloadAddress(
                object_ref=observation.result_manifest_ref,
                content_digest=observation.result_manifest_digest,
                size_bytes=observation.result_manifest_size_bytes,
            )
        )
    )
    assert manifest["reused_result"] == {
        "fork_request_id": receipt.request_id,
        "source_run_id": snapshot.source_run_id,
        "source_unit_key": source_draft.unit_key,
        "source_settlement_id": candidate.settlement_id,
        "source_result_manifest_ref": candidate.result_manifest_ref,
        "source_result_manifest_digest": candidate.result_manifest_digest,
    }
    derived_budget = await harness.run_control.get_budget(SCOPE, derived_run)
    assert derived_budget.consumed.get("tokens.total", 0) == 0
    assert f"reservation:{derived_draft.unit_key}" not in derived_budget.reservations
    # The source run's unit and namespace are untouched.
    assert harness.lineage.results[(SCOPE, source_draft.unit_key, 1)].result_manifest_ref == (
        candidate.result_manifest_ref
    )
    assert (
        len(
            [
                item
                async for item in harness.saver.alist(
                    {"configurable": {"thread_id": cognitive_session_namespace(source_draft, 1)}}
                )
            ]
        )
        == source_namespace_checkpoints
    )

    # The invalidated `review` unit runs fresh cognition in the derived run's namespace.
    derived_review = stage_recovery_unit(derived_run, "review")
    fresh = await harness.run(await harness.request(derived_review))
    assert fresh.status == "completed"
    assert len(harness.model.calls) == calls_after_source + 2
    assert fresh.result_checkpoint is not None
    assert fresh.result_checkpoint.thread_id == cognitive_session_namespace(derived_review, 1)


class Store:
    def __init__(self, *, fork: ForkOfRun | None, decision: Any = None) -> None:
        self._fork = fork
        self._decision = decision

    async def fork_of_run(self, _scope: str, _run: str) -> ForkOfRun | None:
        return self._fork

    async def get_reuse_decision(self, _scope: str, _run: str, _unit: str) -> Any:
        return self._decision


@pytest.mark.asyncio
async def test_reuse_resolver_fails_closed_on_every_inconsistency() -> None:
    harness = await recovery_harness()
    source_draft = stage_recovery_unit(harness.run_id, "draft")
    await harness.run(await harness.request(source_draft))
    assert harness.results is not None and harness.bindings is not None
    sources = FakeForkSourceReader(
        harness.run_control,
        heads={harness.run_id: (stagegraph_head(stages={"draft": "completed"}),)},
    )
    forks = compose_in_memory_forks(
        harness.run_control,
        harness.repository,
        inspection_reads(harness.repository, harness.lineage, harness.journal),
        sources,
    )
    snapshot = await forks.snapshots.take(SCOPE, harness.run_id)
    receipt = await forks.forks.fork(fork_command(snapshot))
    decision = await forks.store.get_reuse_decision(
        SCOPE,
        receipt.target_run_id,
        derived_unit_identity(source_draft, receipt.target_run_id).unit_key,
    )
    assert decision is not None and decision.decision == "reuse"
    await harness.run_control.execute(
        command(receipt.target_run_id, 1, "derived-start", StartAction())
    )
    harness.run_id = receipt.target_run_id
    request = await harness.request(derived_unit_identity(source_draft, receipt.target_run_id))
    from app.application.operations.operation_execution import bind_operation_execution_request

    binding = bind_operation_execution_request(request)
    source_binding = await harness.bindings.get_binding_by_id(
        decision.candidate.binding_id, request_scope=SCOPE
    )
    assert source_binding is not None
    assert reuse_compatibility_digest(source_binding) == reuse_compatibility_digest(binding)

    def resolver(store: Any, *, results: Any = None, bindings: Any = None) -> ForkReuseResolver:
        return ForkReuseResolver(
            store, results=results or harness.results, bindings=bindings or harness.bindings
        )

    # An ordinary run (no fork) and a re-executed unit resolve to fresh execution.
    assert await resolver(Store(fork=None)).reused_result(binding) is None
    materialized = ForkOfRun(fork_request_id=receipt.request_id, materialized=True)
    invalidated = decision.model_copy(update={"decision": "invalidated", "candidate": None})
    assert (
        await resolver(Store(fork=materialized, decision=invalidated)).reused_result(binding)
        is None
    )

    # A fork-derived run never executes before its fork is materialized.
    unmaterialized = ForkOfRun(fork_request_id=receipt.request_id, materialized=False)
    with pytest.raises(ForkRejected) as caught:
        await resolver(Store(fork=unmaterialized)).reused_result(binding)
    assert caught.value.code == "fork_not_materialized"

    # Incompatible restore: a different state schema, or a drifted binding.
    other_schema = decision.model_copy(
        update={
            "candidate": decision.candidate.model_copy(
                update={"state_schema_digest": "sha256:" + "9" * 64}
            )
        }
    )
    with pytest.raises(ForkRejected) as caught:
        await resolver(Store(fork=materialized, decision=other_schema)).reused_result(binding)
    assert caught.value.code == "incompatible_restore"

    class DriftedBindings:
        async def get_binding_by_id(self, _binding_id: str, *, request_scope: str) -> Any:
            del request_scope
            return source_binding.model_copy(update={"budget_limits": {"tokens.total": 11}})

    with pytest.raises(ForkRejected, match="patch frontier drift"):
        await resolver(
            Store(fork=materialized, decision=decision), bindings=DriftedBindings()
        ).reused_result(binding)

    # A manifest that is not the recorded settlement is refused.
    wrong_settlement = decision.model_copy(
        update={"candidate": decision.candidate.model_copy(update={"settlement_id": "other"})}
    )
    with pytest.raises(ForkRejected, match="not the recorded settlement"):
        await resolver(Store(fork=materialized, decision=wrong_settlement)).reused_result(binding)
    reused = await resolver(Store(fork=materialized, decision=decision)).reused_result(binding)
    assert reused is not None
    assert isinstance(reused.source_settlement, OperationSettlement)
    assert reused.ref.source_unit_key == source_draft.unit_key
    assert isinstance(forks.store, InMemoryForkMaterializationStore)
