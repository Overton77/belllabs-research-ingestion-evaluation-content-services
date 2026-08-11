from __future__ import annotations

import asyncio
import random
from datetime import UTC, datetime, timedelta

import pytest
from pydantic import ValidationError

from app.agent_server.common_state import CommonStateMetadata, _reject_noncompact_payload
from app.agent_server.reducers import (
    ReducerMergeConflict,
    merge_keyed_canonical_digest,
    merge_monotonic_integer,
    merge_single_assignment,
    merge_unique_events,
)
from app.application.operations.operation_executor import (
    CancelledOperationOutcome,
    CompletedOperationOutcome,
    ExactStageExecutionBinding,
    OperationExecutorConformanceHarness,
    StageOperationRequest,
    operation_execution_outcome_adapter,
)
from app.domain.graph_runtime.contracts import (
    _reject_redacted_runtime_payload,
    _reject_sensitive_payload,
)
from app.domain.graph_runtime.kernel import (
    CancellationContext,
    DecisionRequest,
    LineageKind,
    LineageParentEdge,
    ProviderQualifiedLineageRecord,
    ResourceKind,
    ResourceLeaseRecord,
    ResourceLeaseRequest,
    ResourceLeaseStatus,
    WaitLeaseProjection,
)

DIGEST_A = "sha256:" + "a" * 64
DIGEST_B = "sha256:" + "b" * 64
NOW = datetime(2026, 8, 6, 20, 0, tzinfo=UTC)


def lease() -> ResourceLeaseRecord:
    request = ResourceLeaseRequest(
        lease_id="lease-1",
        request_scope="tenant-1",
        semantic_identity="operation:one",
        envelope_digest=DIGEST_A,
        resources=(ResourceKind.TENANT, ResourceKind.OPERATION_WORKER),
        requested_at=NOW,
        deadline=NOW + timedelta(minutes=10),
        ttl_seconds=60,
    )
    return ResourceLeaseRecord(
        request=request,
        status=ResourceLeaseStatus.ACQUIRED,
        acquired_at=NOW,
        expires_at=NOW + timedelta(minutes=1),
        canonical_digest=DIGEST_A,
    )


def test_reducers_are_deterministic_idempotent_and_fail_closed() -> None:
    entries = [
        {"key": key, "canonical_digest": digest}
        for key, digest in (("result:a", DIGEST_A), ("result:b", DIGEST_B), ("result:c", DIGEST_A))
    ]
    expected = merge_keyed_canonical_digest(entries, entries)
    for seed in range(20):
        shuffled = entries[:]
        random.Random(seed).shuffle(shuffled)
        assert merge_keyed_canonical_digest(shuffled, entries) == expected
    assert merge_single_assignment("bound", "bound") == "bound"
    assert merge_monotonic_integer(4, 2) == 4
    with pytest.raises(ValueError, match="conflicting assignments"):
        merge_single_assignment("first", "second")
    with pytest.raises(ReducerMergeConflict, match="canonical digest conflict"):
        merge_keyed_canonical_digest(entries, [{"key": "result:a", "canonical_digest": DIGEST_B}])


def test_existing_event_reducer_remains_compatible() -> None:
    assert merge_unique_events(("event:a",), ("event:a", "event:b")) == ("event:a", "event:b")


def test_common_state_is_frozen_compact_and_rejects_payload_shapes() -> None:
    state = CommonStateMetadata(
        runtime_binding_ref="binding:1",
        definition_digest=DIGEST_A,
        assembly_digest=DIGEST_A,
        state_schema_digest=DIGEST_A,
        lifecycle_projection_ref="lifecycle:1",
        lifecycle_projection_version=1,
        pending_decisions=(
            DecisionRequest(
                decision_id="decision-1",
                request_scope="tenant-1",
                binding_id="binding-1",
                decision_type="approval",
                schema_ref="schema:approval",
                expected_lifecycle_version=1,
                policy_ref="policy:approval",
                request_digest=DIGEST_A,
                requested_at=NOW,
            ),
        ),
    )
    with pytest.raises(ValidationError):
        state.runtime_binding_ref = "other"
    with pytest.raises(ValueError, match="sensitive"):
        _reject_noncompact_payload({"api_key": "not-allowed"})
    with pytest.raises(ValueError, match="compact"):
        _reject_noncompact_payload("x" * 5_000)
    with pytest.raises(ValueError, match="sensitive references only"):
        _reject_sensitive_payload({"secret_id": "raw-secret-identifier"})
    _reject_sensitive_payload({"secret_ref": "vault-reference-only"})
    with pytest.raises(ValueError, match="non-allowlisted"):
        _reject_redacted_runtime_payload({"patient_id": "synthetic-patient"})


def test_lineage_identities_and_parent_edges_remain_distinct() -> None:
    parent = ProviderQualifiedLineageRecord(
        kind=LineageKind.EXECUTION_EPOCH,
        provider="belllabs",
        provider_identity="run-1.epoch-1",
        request_scope="tenant-1",
        canonical_digest=DIGEST_A,
    )
    child = ProviderQualifiedLineageRecord(
        kind=LineageKind.AGENT_RUN,
        provider="langgraph",
        provider_identity="run-1",
        request_scope="tenant-1",
        canonical_digest=DIGEST_B,
    )
    assert parent.canonical_key != child.canonical_key
    assert LineageParentEdge(child=child, parent=parent, relationship="attempt_of").child == child
    with pytest.raises(ValidationError, match="distinct parent"):
        LineageParentEdge(child=parent, parent=parent, relationship="contains")
    delimiter_left = parent.model_copy(
        update={"provider": "a:b", "provider_identity": "c"}
    )
    delimiter_right = parent.model_copy(
        update={"provider": "a", "provider_identity": "b:c"}
    )
    assert delimiter_left.canonical_key != delimiter_right.canonical_key


def test_resource_order_wait_projection_and_cancellation_deadline() -> None:
    with pytest.raises(ValidationError, match="canonical acquisition"):
        ResourceLeaseRequest(
            lease_id="lease-2",
            request_scope="tenant-1",
            semantic_identity="operation:two",
            envelope_digest=DIGEST_A,
            resources=(ResourceKind.MODEL_CALL, ResourceKind.TENANT),
            requested_at=NOW,
            deadline=NOW + timedelta(minutes=1),
            ttl_seconds=1,
        )
    with pytest.raises(ValidationError, match="both retained and released"):
        WaitLeaseProjection(
            wait_binding_ref="wait:1",
            retained_reservations=("lease:1",),
            released_reservations=("lease:1",),
        )
    expired = CancellationContext(
        cancellation_id="cancel-1",
        deadline=NOW - timedelta(seconds=1),
        cascade_policy_ref="policy:cascade",
    )
    assert expired.is_cancelled_or_expired(NOW)


def test_operation_outcomes_validate_and_executor_harness_enforces_cancellation() -> None:
    completed = operation_execution_outcome_adapter.validate_python(
        {"kind": "completed", "result_manifest_ref": "result:1"}
    )
    assert isinstance(completed, CompletedOperationOutcome)
    with pytest.raises(ValidationError):
        operation_execution_outcome_adapter.validate_python({"kind": "failed"})

    class CancelledExecutor:
        async def execute(self, *_: object) -> CancelledOperationOutcome:
            return CancelledOperationOutcome(settlement_refs=("settlement:1",))

    request = StageOperationRequest(
        request_scope="tenant-1",
        operation_id="operation-1",
        semantic_attempt_id="attempt-1",
        input_manifest_ref="input:1",
        input_digest=DIGEST_A,
    )
    binding = ExactStageExecutionBinding(
        binding_ref="binding:1",
        operation_assembly_digest=DIGEST_A,
    )
    cancelled = CancellationContext(
        cancellation_id="cancel-2",
        requested=True,
        requested_at=NOW,
        cascade_policy_ref="policy:cascade",
    )
    result = asyncio.run(
        OperationExecutorConformanceHarness().assert_conforms(
            CancelledExecutor(), request, binding, lease(), cancelled
        )
    )
    assert isinstance(result, CancelledOperationOutcome)


def test_operation_outcome_union_is_complete_and_cannot_mutate_lifecycle() -> None:
    examples = (
        {"kind": "completed", "result_manifest_ref": "result:1"},
        {
            "kind": "waiting",
            "wait": {
                "wait_binding_ref": "wait:1",
                "retained_reservations": ["lease:budget"],
                "released_reservations": ["lease:worker"],
            },
        },
        {"kind": "paused", "decision_ref": "decision:1"},
        {
            "kind": "degraded",
            "reason_code": "authored_degradation",
            "result_manifest_ref": "result:degraded",
        },
        {
            "kind": "failed",
            "failure_class": "ambiguous_external_effect",
            "retryability": "reconcile",
        },
        {"kind": "cancelled", "settlement_refs": ["settlement:cancelled"]},
    )

    assert {
        operation_execution_outcome_adapter.validate_python(item).kind for item in examples
    } == {"completed", "waiting", "paused", "degraded", "failed", "cancelled"}
    with pytest.raises(ValidationError, match="lifecycle_status"):
        operation_execution_outcome_adapter.validate_python(
            {
                "kind": "completed",
                "result_manifest_ref": "result:1",
                "lifecycle_status": "completed",
            }
        )
