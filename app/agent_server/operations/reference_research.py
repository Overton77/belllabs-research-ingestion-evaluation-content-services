"""Bounded LangGraph Q/D classifier with a deterministic CI provider stub."""

from __future__ import annotations

from typing import Any, TypedDict

from langgraph.graph import END, START, StateGraph

from app.application.operation_executor import (
    CancellationContext,
    CancelledOperationOutcome,
    CompletedOperationOutcome,
    ExactStageExecutionBinding,
    OperationExecutionOutcome,
    StageOperationRequest,
)
from app.application.reference_research import (
    ImmutableManifestStore,
    ReferenceOperationManifest,
    classify_reference_fixture,
)
from app.domain.graph_runtime.kernel import ResourceLeaseRecord, ResourceLeaseStatus
from app.domain.reference_research.contracts import DaveFixtureInput, QualiaFixtureInput


class CanaryState(TypedDict):
    messages: list[str]
    result: dict[str, object] | None


def build_reference_canary_graph() -> Any:
    """Build one operation-local graph; callers retain all macro scheduling authority."""

    async def classify(state: CanaryState) -> dict[str, object]:
        return {"messages": ["deterministic-provider-stub:classified"], "result": state["result"]}

    builder = StateGraph(CanaryState)
    builder.add_node("bounded_classify", classify)
    builder.add_edge(START, "bounded_classify")
    builder.add_edge("bounded_classify", END)
    return builder.compile()


class ReferenceLangGraphCanaryExecutor:
    """OperationExecutor adapter with explicit zero-network/zero-model CI ceilings."""

    MAX_GRAPH_STEPS = 2
    MAX_MODEL_CALLS = 0
    MAX_TOOL_CALLS = 0
    MAX_REQUESTS = 0
    MAX_TOKENS = 0
    MAX_COST_MICRODOLLARS = 0
    MAX_PAGES = 0
    WALL_CLOCK_SECONDS = 5

    def __init__(
        self,
        fixture: QualiaFixtureInput | DaveFixtureInput,
        store: ImmutableManifestStore,
        expected_assembly_digest: str,
    ) -> None:
        self._fixture = fixture
        self._store = store
        self._expected_assembly_digest = expected_assembly_digest
        self._graph = build_reference_canary_graph()

    async def execute(
        self,
        stage_request: StageOperationRequest,
        exact_stage_execution_binding: ExactStageExecutionBinding,
        execution_resource_lease: ResourceLeaseRecord,
        cancellation_context: CancellationContext,
    ) -> OperationExecutionOutcome:
        if cancellation_context.requested:
            return CancelledOperationOutcome(
                settlement_refs=(f"settlement:{stage_request.semantic_attempt_id}:cancelled",)
            )
        if execution_resource_lease.status != ResourceLeaseStatus.ACQUIRED:
            raise ValueError("bounded reference canary requires an acquired lease")
        if stage_request.request_scope != "reference-fixtures":
            raise ValueError("bounded reference canary denies cross-scope execution")
        if exact_stage_execution_binding.operation_assembly_digest != (
            self._expected_assembly_digest
        ):
            raise ValueError("bounded reference canary rejects assembly digest drift")
        typed_result = classify_reference_fixture(self._fixture)
        invoked = await self._graph.ainvoke(
            {
                "messages": ["deterministic-provider-stub:input"],
                "result": typed_result.model_dump(mode="json"),
            },
            {"recursion_limit": self.MAX_GRAPH_STEPS + 1},
        )
        manifest = ReferenceOperationManifest(
            family_id=self._fixture.family_id,
            stage_id=stage_request.operation_id,
            as_of=self._fixture.as_of,
            result=invoked["result"],
            evidence_refs=tuple(f"evidence:{source.source_id}" for source in self._fixture.sources),
        )
        ref, _digest, _size = self._store.put(manifest)
        return CompletedOperationOutcome(
            result_manifest_ref=ref,
            evidence_refs=manifest.evidence_refs,
            usage_refs=(f"usage:{stage_request.semantic_attempt_id}:stub-zero",),
        )
