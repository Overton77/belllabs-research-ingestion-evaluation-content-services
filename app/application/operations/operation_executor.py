"""Runtime-neutral operation executor port and adapter conformance checks."""

from __future__ import annotations

from typing import Annotated, Literal, Protocol

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter

from app.domain.graph_runtime.identities import DIGEST_PATTERN
from app.domain.graph_runtime.kernel import (
    CancellationContext,
    OperationFailureClass,
    OperationFailureClassV2,
    ResourceLeaseRecord,
    WaitLeaseProjection,
)


class ExecutorContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class StageOperationRequest(ExecutorContract):
    request_scope: str = Field(min_length=1, max_length=256)
    operation_id: str = Field(min_length=1, max_length=512)
    semantic_attempt_id: str = Field(min_length=1, max_length=1_024)
    input_manifest_ref: str = Field(min_length=1, max_length=1_024)
    input_digest: str = Field(pattern=DIGEST_PATTERN)


class ExactStageExecutionBinding(ExecutorContract):
    binding_ref: str = Field(min_length=1, max_length=1_024)
    operation_assembly_digest: str = Field(pattern=DIGEST_PATTERN)


class CompletedOperationOutcome(ExecutorContract):
    kind: Literal["completed"] = "completed"
    result_manifest_ref: str = Field(min_length=1, max_length=1_024)
    evidence_refs: tuple[str, ...] = ()
    usage_refs: tuple[str, ...] = ()


class WaitingOperationOutcome(ExecutorContract):
    kind: Literal["waiting"] = "waiting"
    wait: WaitLeaseProjection


class PausedOperationOutcome(ExecutorContract):
    kind: Literal["paused"] = "paused"
    decision_ref: str = Field(min_length=1, max_length=1_024)


class DegradedOperationOutcome(ExecutorContract):
    kind: Literal["degraded"] = "degraded"
    reason_code: str = Field(min_length=1, max_length=256)
    result_manifest_ref: str = Field(min_length=1, max_length=1_024)


class FailedOperationOutcome(ExecutorContract):
    kind: Literal["failed"] = "failed"
    failure_class: OperationFailureClass
    retryability: Literal["never", "safe", "reconcile"]
    evidence_refs: tuple[str, ...] = ()


class CancelledOperationOutcome(ExecutorContract):
    kind: Literal["cancelled"] = "cancelled"
    settlement_refs: tuple[str, ...] = ()


OperationExecutionOutcome = Annotated[
    CompletedOperationOutcome
    | WaitingOperationOutcome
    | PausedOperationOutcome
    | DegradedOperationOutcome
    | FailedOperationOutcome
    | CancelledOperationOutcome,
    Field(discriminator="kind"),
]
operation_execution_outcome_adapter: TypeAdapter[OperationExecutionOutcome] = TypeAdapter(
    OperationExecutionOutcome
)


class FailedOperationOutcomeV2(ExecutorContract):
    kind: Literal["failed"] = "failed"
    failure_class: OperationFailureClassV2
    retryability: Literal["never", "safe", "reconcile"]
    evidence_refs: tuple[str, ...] = ()


OperationExecutionOutcomeV2 = Annotated[
    CompletedOperationOutcome
    | WaitingOperationOutcome
    | PausedOperationOutcome
    | DegradedOperationOutcome
    | FailedOperationOutcomeV2
    | CancelledOperationOutcome,
    Field(discriminator="kind"),
]
operation_execution_outcome_v2_adapter: TypeAdapter[OperationExecutionOutcomeV2] = TypeAdapter(
    OperationExecutionOutcomeV2
)


class OperationExecutor(Protocol):
    """Adapters receive frozen authority and return only a typed outcome."""

    async def execute(
        self,
        stage_request: StageOperationRequest,
        exact_stage_execution_binding: ExactStageExecutionBinding,
        execution_resource_lease: ResourceLeaseRecord,
        cancellation_context: CancellationContext,
    ) -> OperationExecutionOutcome: ...


class OperationExecutorConformanceHarness:
    """Small reusable contract harness for every later native/provider adapter."""

    async def assert_conforms(
        self,
        executor: OperationExecutor,
        stage_request: StageOperationRequest,
        exact_stage_execution_binding: ExactStageExecutionBinding,
        execution_resource_lease: ResourceLeaseRecord,
        cancellation_context: CancellationContext,
    ) -> OperationExecutionOutcome:
        outcome = await executor.execute(
            stage_request,
            exact_stage_execution_binding,
            execution_resource_lease,
            cancellation_context,
        )
        parsed = operation_execution_outcome_adapter.validate_python(outcome)
        if cancellation_context.requested and parsed.kind != "cancelled":
            raise AssertionError("cancelled operations must return the cancelled outcome")
        self._assert_refs_are_compact(parsed)
        return parsed

    @staticmethod
    def _assert_refs_are_compact(outcome: OperationExecutionOutcome) -> None:
        for field_name, value in outcome.model_dump(mode="python").items():
            if field_name.endswith(("_ref", "_refs")):
                _validate_refs(value)


def _validate_refs(value: object) -> None:
    if isinstance(value, str):
        if not value or len(value) > 1_024:
            raise AssertionError("operation outcomes must use compact references")
    elif isinstance(value, tuple | list):
        for item in value:
            _validate_refs(item)
