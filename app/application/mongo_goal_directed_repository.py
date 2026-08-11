from __future__ import annotations

from datetime import datetime

from beanie import Document
from pymongo.errors import DuplicateKeyError

from app.application.goal_directed import document_payload
from app.domain.control_plane.canonical import sha256_digest
from app.domain.operation_execution.contracts import OperationExecutionRequest
from app.domain.orchestration.contracts import (
    GoalExecutionResult,
    GoalHandoff,
    GoalRevision,
    GoalVerificationResult,
)
from app.domain.run_control.errors import IdempotencyConflict
from app.models.goal_directed import (
    GoalHandoffDocument,
    GoalIterationDocument,
    GoalOperationTemplateDocument,
    GoalRevisionDocument,
    GoalVerificationDocument,
)


class MongoGoalDirectedDocumentRepository:
    """Immutable GoalDirected detail repository; PostgreSQL receipts own acceptance."""

    async def persist_revision(
        self,
        request_scope: str,
        run_id: str,
        revision: GoalRevision,
        recorded_at: datetime,
    ) -> str:
        payload = document_payload(revision)
        document = GoalRevisionDocument(
            request_scope=request_scope,
            run_id=run_id,
            goal_revision_id=revision.revision_id,
            revision=revision.revision,
            envelope_digest=revision.envelope_digest,
            document_digest=sha256_digest(payload),
            payload=payload,
            recorded_at=recorded_at,
        )
        await _insert_exact(
            document,
            GoalRevisionDocument,
            {
                "request_scope": request_scope,
                "run_id": run_id,
                "goal_revision_id": revision.revision_id,
            },
        )
        return _ref("revision", revision.revision_id, document.document_digest)

    async def persist_iteration(
        self,
        request_scope: str,
        result: GoalExecutionResult,
        goal_revision_id: str,
        recorded_at: datetime,
    ) -> str:
        payload = document_payload(result)
        iteration_key = result.identity.semantic_key
        document = GoalIterationDocument(
            request_scope=request_scope,
            run_id=result.identity.iteration.run_id,
            iteration_key=iteration_key,
            goal_revision_id=goal_revision_id,
            document_digest=sha256_digest(payload),
            payload=payload,
            recorded_at=recorded_at,
        )
        await _insert_exact(
            document,
            GoalIterationDocument,
            {
                "request_scope": request_scope,
                "run_id": result.identity.iteration.run_id,
                "iteration_key": iteration_key,
            },
        )
        return _ref("iteration", iteration_key, document.document_digest)

    async def persist_handoff(
        self,
        request_scope: str,
        handoff: GoalHandoff,
        recorded_at: datetime,
    ) -> str:
        payload = document_payload(handoff)
        document = GoalHandoffDocument(
            request_scope=request_scope,
            run_id=handoff.run_id,
            handoff_id=handoff.handoff_id,
            goal_revision_id=handoff.goal_revision_id,
            document_digest=sha256_digest(payload),
            payload=payload,
            recorded_at=recorded_at,
        )
        await _insert_exact(
            document,
            GoalHandoffDocument,
            {
                "request_scope": request_scope,
                "run_id": handoff.run_id,
                "handoff_id": handoff.handoff_id,
            },
        )
        return _ref("handoff", handoff.handoff_id, document.document_digest)

    async def persist_verification(
        self,
        request_scope: str,
        run_id: str,
        goal_revision_id: str,
        verification: GoalVerificationResult,
        recorded_at: datetime,
    ) -> str:
        payload = document_payload(verification)
        document = GoalVerificationDocument(
            request_scope=request_scope,
            run_id=run_id,
            verification_id=verification.verification_id,
            goal_revision_id=goal_revision_id,
            document_digest=sha256_digest(payload),
            payload=payload,
            recorded_at=recorded_at,
        )
        await _insert_exact(
            document,
            GoalVerificationDocument,
            {
                "request_scope": request_scope,
                "run_id": run_id,
                "verification_id": verification.verification_id,
            },
        )
        return _ref("verification", verification.verification_id, document.document_digest)

    async def persist_templates(
        self,
        *,
        request_scope: str,
        semantic_input_binding_ref: str,
        executor: OperationExecutionRequest,
        verifier: OperationExecutionRequest,
        recorded_at: datetime,
    ) -> None:
        for operation_role, template in (("executor", executor), ("verifier", verifier)):
            payload = template.model_dump(mode="json")
            document = GoalOperationTemplateDocument(
                request_scope=request_scope,
                semantic_input_binding_ref=semantic_input_binding_ref,
                operation_role=operation_role,
                document_digest=sha256_digest(payload),
                payload=payload,
                recorded_at=recorded_at,
            )
            await _insert_exact(
                document,
                GoalOperationTemplateDocument,
                {
                    "request_scope": request_scope,
                    "semantic_input_binding_ref": semantic_input_binding_ref,
                    "operation_role": operation_role,
                },
            )

    async def get_template(
        self,
        *,
        semantic_input_binding_ref: str,
        operation_role: str,
        request_scope: str,
        run_id: str,
    ) -> OperationExecutionRequest:
        document = await GoalOperationTemplateDocument.find_one(
            {
                "request_scope": request_scope,
                "semantic_input_binding_ref": semantic_input_binding_ref,
                "operation_role": operation_role,
            }
        )
        if document is None:
            raise ValueError("GoalDirected operation template is unavailable")
        if sha256_digest(document.payload) != document.document_digest:
            raise ValueError("GoalDirected operation template digest mismatch")
        template = OperationExecutionRequest.model_validate(document.payload)
        if template.request_scope != request_scope:
            raise ValueError("GoalDirected operation template belongs to another request scope")
        return template


async def _insert_exact(
    document: Document,
    model: type[Document],
    identity: dict[str, str],
) -> None:
    try:
        await document.insert()
        return
    except DuplicateKeyError:
        prior = await model.find_one(identity)
    if prior is None or prior.model_dump(mode="python", exclude={"id"}) != document.model_dump(
        mode="python", exclude={"id"}
    ):
        raise IdempotencyConflict("GoalDirected immutable document identity conflict")


def _ref(kind: str, identity: str, digest: str) -> str:
    return f"goal-{kind}:{identity}@{digest}"


__all__ = ["MongoGoalDirectedDocumentRepository"]
