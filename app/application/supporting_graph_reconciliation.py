from __future__ import annotations

import inspect
from datetime import datetime
from typing import Any, Literal, Protocol
from uuid import NAMESPACE_URL, uuid5

from app.application.schema_grounding_repository import (
    SchemaGroundingRecordRepository,
    schema_grounding_record,
)
from app.application.schema_workspace_binding import SchemaGraphAdmissionService
from app.domain.schema_context.canonicalization import sha256_digest
from app.domain.schema_context.contracts import (
    GraphReconciliationEvidence,
    IntentResultReference,
    QueryExecutionIntent,
    QueryExecutionResult,
    SchemaOperationProjection,
)
from app.domain.schema_grounding.contracts import (
    GraphAdmissionDecision,
    GraphAdmissionRequest,
    SupportingGraphReconciliationRecord,
    SupportingGraphReconciliationRequest,
)
from app.domain.schema_grounding.errors import (
    CatalogPublicationConflict,
    SchemaGroundingRecordNotFound,
)


class BoundedReadExecutor(Protocol):
    async def execute(
        self,
        intent: QueryExecutionIntent,
        projection: SchemaOperationProjection,
    ) -> QueryExecutionResult: ...


class BoundedReadExecutorFactory(Protocol):
    async def create(
        self,
        admission: GraphAdmissionRequest,
        decision: GraphAdmissionDecision,
    ) -> BoundedReadExecutor: ...


class SupportingGraphReconciliationWorkflow:
    """Application-owned, observational execution of one bounded reconciliation question."""

    def __init__(
        self,
        *,
        admission: SchemaGraphAdmissionService,
        executor_factory: BoundedReadExecutorFactory,
        records: SchemaGroundingRecordRepository,
    ) -> None:
        self._admission = admission
        self._executor_factory = executor_factory
        self._records = records

    async def run(
        self,
        request: SupportingGraphReconciliationRequest,
        *,
        evidence: GraphReconciliationEvidence | None = None,
        completed_at: datetime | None = None,
    ) -> SupportingGraphReconciliationRecord:
        request_digest = _reconciliation_request_digest(request, evidence)
        try:
            prior_envelope = await self._records.get(
                request.request_scope,
                "reconciliation",
                request.reconciliation_id,
            )
        except SchemaGroundingRecordNotFound:
            prior_envelope = None
        if prior_envelope is not None:
            prior = SupportingGraphReconciliationRecord.model_validate(prior_envelope.payload)
            if prior.request_digest != request_digest:
                raise CatalogPublicationConflict(
                    "reconciliation identity was reused with different governed inputs"
                )
            return prior

        timestamp = completed_at or request.created_at
        decision = await self._admission.decide(
            request.admission,
        )
        if not decision.admitted:
            record = self._record(
                request,
                request_digest,
                decision,
                (),
                (),
                status="rejected",
                evidence=None,
                completed_at=timestamp,
            )
            await self._persist_reconciliation(record)
            return record
        if len(request.intents) > request.maximum_intents:
            raise ValueError("bounded reconciliation intent ceiling exceeded")

        persisted_results = await self._records.list_for_run(
            request.request_scope,
            request.run_id,
            record_type="query_result",
        )
        results_by_intent: dict[str, QueryExecutionResult] = {}
        for envelope in persisted_results:
            result = QueryExecutionResult.model_validate(envelope.payload)
            if result.intent_id in results_by_intent:
                raise CatalogPublicationConflict(
                    "multiple immutable query results exist for one intent"
                )
            results_by_intent[result.intent_id] = result

        executor: BoundedReadExecutor | None = None
        results: list[QueryExecutionResult] = []
        references: list[IntentResultReference] = []
        try:
            for expected_sequence, intent in enumerate(request.intents, start=1):
                await self._persist_intent(request, intent, timestamp)
                existing_result = results_by_intent.get(intent.intent_id)
                if existing_result is not None:
                    if (
                        existing_result.intent_digest
                        != sha256_digest(intent.model_dump(mode="json"))
                        or existing_result.query_kind != intent.query_kind
                    ):
                        raise CatalogPublicationConflict(
                            "persisted query result does not match its immutable intent"
                        )
                    result = existing_result
                else:
                    rejection = _validate_intent(
                        intent,
                        request=request,
                        expected_sequence=expected_sequence,
                    )
                    if rejection is not None:
                        result = _terminal_result(
                            intent,
                            status="rejected",
                            diagnostic=rejection,
                            timestamp=timestamp,
                        )
                    else:
                        if executor is None:
                            executor = await self._executor_factory.create(
                                request.admission,
                                decision,
                            )
                        try:
                            result = await executor.execute(
                                intent,
                                request.projection,
                            )
                        except Exception as error:
                            result = _terminal_result(
                                intent,
                                status="failed",
                                diagnostic=(
                                    f"{type(error).__name__} at governed Neo4j read boundary"
                                ),
                                timestamp=timestamp,
                            )
                results.append(result)
                references.append(
                    IntentResultReference(
                        intent_id=intent.intent_id,
                        result_id=result.result_id,
                    )
                )
                await self._persist_result(request, result, timestamp)
        finally:
            close = getattr(executor, "close", None) if executor is not None else None
            if close is not None:
                outcome = close()
                if inspect.isawaitable(outcome):
                    await outcome

        actual_references = tuple(references)
        if evidence is not None:
            if evidence.intent_result_references != actual_references:
                raise ValueError(
                    "query evidence references do not exactly match persisted intent/results"
                )
            admitted_evidence = evidence
        else:
            admitted_evidence = _default_evidence(request, actual_references, results)
        status: Literal["completed", "failed"] = (
            "completed" if all(result.status == "succeeded" for result in results) else "failed"
        )
        record = self._record(
            request,
            request_digest,
            decision,
            actual_references,
            tuple(results),
            status=status,
            evidence=admitted_evidence,
            completed_at=timestamp,
        )
        await self._persist_reconciliation(record)
        return record

    async def _persist_intent(
        self,
        request: SupportingGraphReconciliationRequest,
        intent: QueryExecutionIntent,
        timestamp: datetime,
    ) -> None:
        await self._records.append(
            schema_grounding_record(
                record_type="query_intent",
                record_id=intent.intent_id,
                request_scope=request.request_scope,
                run_id=request.run_id,
                payload=intent.model_dump(mode="json"),
                created_at=timestamp,
            )
        )

    async def _persist_result(
        self,
        request: SupportingGraphReconciliationRequest,
        result: QueryExecutionResult,
        timestamp: datetime,
    ) -> None:
        await self._records.append(
            schema_grounding_record(
                record_type="query_result",
                record_id=result.result_id,
                request_scope=request.request_scope,
                run_id=request.run_id,
                payload=result.model_dump(mode="json"),
                created_at=timestamp,
            )
        )

    async def _persist_reconciliation(self, record: SupportingGraphReconciliationRecord) -> None:
        await self._records.append(
            schema_grounding_record(
                record_type="reconciliation",
                record_id=record.reconciliation_id,
                request_scope=record.request_scope,
                run_id=record.run_id,
                payload=record.model_dump(mode="json"),
                created_at=record.completed_at,
            )
        )
        await self._records.append(
            schema_grounding_record(
                record_type="evaluation",
                record_id=f"{record.reconciliation_id}:evaluation",
                request_scope=record.request_scope,
                run_id=record.run_id,
                payload={
                    "evaluation_schema_version": "1",
                    "reconciliation_id": record.reconciliation_id,
                    "status": record.status,
                    "independent_graph_admission": record.admission_decision.admitted,
                    "intent_count": len(record.intent_result_references),
                    "successful_count": record.successful_count,
                    "zero_result_count": record.zero_result_count,
                    "rejected_count": record.rejected_count,
                    "failed_count": record.failed_count,
                    "evidence_reference_count": (
                        len(record.evidence.intent_result_references)
                        if record.evidence is not None
                        else 0
                    ),
                    "broad_knowledge_preflight_claimed": False,
                    "completed_at": record.completed_at.isoformat(),
                },
                created_at=record.completed_at,
            )
        )

    @staticmethod
    def _record(
        request: SupportingGraphReconciliationRequest,
        request_digest: str,
        decision: GraphAdmissionDecision,
        references: tuple[IntentResultReference, ...],
        results: tuple[QueryExecutionResult, ...],
        *,
        status: Literal["completed", "rejected", "failed"],
        evidence: GraphReconciliationEvidence | None,
        completed_at: datetime,
    ) -> SupportingGraphReconciliationRecord:
        successful = tuple(result for result in results if result.status == "succeeded")
        return SupportingGraphReconciliationRecord(
            reconciliation_id=request.reconciliation_id,
            request_digest=request_digest,
            request_scope=request.request_scope,
            run_id=request.run_id,
            status=status,
            question=request.question,
            admission_decision=decision,
            intent_result_references=references,
            evidence=evidence,
            successful_count=len(successful),
            zero_result_count=sum(result.record_count == 0 for result in successful),
            rejected_count=sum(result.status == "rejected" for result in results),
            failed_count=sum(result.status == "failed" for result in results),
            projection_id=request.projection.projection_id,
            projection_digest=request.projection.projection_digest,
            workspace_binding_id=decision.workspace_binding_id,
            deployment_manifest_id=decision.deployment_manifest_id,
            graph_capability_grant_id=decision.graph_capability_grant_id,
            completed_at=completed_at,
        )


def _validate_intent(
    intent: QueryExecutionIntent,
    *,
    request: SupportingGraphReconciliationRequest,
    expected_sequence: int,
) -> str | None:
    projection = request.projection
    grant = request.admission.graph_capability
    if intent.sequence != expected_sequence:
        return "intent sequence is not contiguous and execution ordered"
    if (
        intent.projection_id != projection.projection_id
        or intent.projection_digest != projection.projection_digest
        or intent.schema_definition_digest != projection.source_schema_digest
        or intent.selection_digest != projection.accepted_selection_digest
    ):
        return "intent lineage does not match the admitted purpose-bound projection"
    if intent.purpose != "pre_ingestion_graph_reconciliation":
        return "intent purpose is not the admitted bounded reconciliation purpose"
    if intent.proposed_cypher:
        return "agents cannot submit arbitrary Cypher"
    if grant is None:
        return "graph capability grant disappeared after admission"
    if intent.query_kind not in grant.query_kinds:
        return "intent query kind exceeds the graph capability grant"
    if not set(intent.labels).issubset(grant.allowed_node_labels):
        return "intent labels exceed the graph capability grant"
    if not set(intent.relationship_types).issubset(grant.allowed_relationship_types):
        return "intent relationship types exceed the graph capability grant"
    if intent.limit > min(projection.maximum_limit, grant.maximum_limit):
        return "intent limit exceeds the admitted projection or capability bound"
    if intent.max_depth > min(
        projection.maximum_traversal_depth,
        grant.maximum_traversal_depth,
    ):
        return "intent traversal depth exceeds the admitted projection or capability bound"
    return None


def _terminal_result(
    intent: QueryExecutionIntent,
    *,
    status: str,
    diagnostic: str,
    timestamp: datetime,
) -> QueryExecutionResult:
    intent_digest = sha256_digest(intent.model_dump(mode="json"))
    result_id = str(
        uuid5(
            NAMESPACE_URL,
            f"schema-read-result:{intent.intent_id}:{status}:{intent_digest}",
        )
    )
    logical: dict[str, Any] = {
        "result_id": result_id,
        "intent_id": intent.intent_id,
        "intent_digest": intent_digest,
        "query_kind": intent.query_kind,
        "status": status,
        "compiled_cypher": None,
        "redacted_parameters": {},
        "columns": (),
        "records": (),
        "record_count": 0,
        "truncated": False,
        "elapsed_ms": 0,
        "database": None,
        "server_info": {},
        "diagnostics": (diagnostic,),
        "error_type": (
            "query_intent_rejected" if status == "rejected" else "query_execution_failed"
        ),
        "started_at": timestamp,
        "finished_at": timestamp,
    }
    return QueryExecutionResult(
        **logical,
        result_digest=sha256_digest(
            {
                **logical,
                "started_at": timestamp.isoformat(),
                "finished_at": timestamp.isoformat(),
            }
        ),
    )


def _default_evidence(
    request: SupportingGraphReconciliationRequest,
    references: tuple[IntentResultReference, ...],
    results: list[QueryExecutionResult],
) -> GraphReconciliationEvidence:
    failures = tuple(
        diagnostic
        for result in results
        if result.status != "succeeded"
        for diagnostic in result.diagnostics
    )
    return GraphReconciliationEvidence(
        reconciliation_question=request.question,
        query_goals=tuple(intent.goal for intent in request.intents),
        intent_result_references=references,
        matched_existing_entities=(),
        existing_relationships=(),
        aliases_used=(),
        match_method="host_compiled_bounded_read_intents",
        confidence="observational_only",
        unresolved_candidates=(),
        schema_mismatches=(),
        legacy_name_mappings=(),
        query_failures=failures,
        stopping_rationale=(
            "All admitted bounded intents were attempted; this result is Supporting Graph "
            "Reconciliation evidence and does not claim broad Knowledge Preflight coverage."
        ),
    )


def _reconciliation_request_digest(
    request: SupportingGraphReconciliationRequest,
    evidence: GraphReconciliationEvidence | None,
) -> str:
    return sha256_digest(
        {
            "request": request.model_dump(mode="json"),
            "evidence": (evidence.model_dump(mode="json") if evidence is not None else None),
        }
    )
