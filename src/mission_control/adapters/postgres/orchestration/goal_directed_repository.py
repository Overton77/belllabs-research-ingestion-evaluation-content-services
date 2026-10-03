"""Immutable GoalDirected detail persistence; acceptance remains ledger-owned."""

from __future__ import annotations

import json
from datetime import datetime

import asyncpg

from mission_control.adapters.postgres.documents import PostgresDocumentStore
from mission_control.application.programs.goal_directed import document_payload
from mission_control.domain.authoring.canonical import stable_json_dump
from mission_control.domain.execution.contracts import OperationExecutionRequest
from mission_control.domain.programs.contracts import (
    GoalExecutionResult,
    GoalHandoff,
    GoalRevision,
    GoalVerificationResult,
)


def _key(*parts: str) -> str:
    return json.dumps(parts, separators=(",", ":"))


class PostgresGoalDirectedDocumentRepository:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._documents = PostgresDocumentStore(pool)

    async def persist_revision(
        self,
        request_scope: str,
        run_id: str,
        revision: GoalRevision,
        recorded_at: datetime,
    ) -> str:
        document = await self._documents.put(
            request_scope=request_scope,
            contract="goal.revision/1",
            identity=_key(run_id, revision.revision_id),
            payload=document_payload(revision),
            recorded_at=recorded_at,
        )
        return f"goal-revision:{revision.revision_id}@{document.digest}"

    async def persist_iteration(
        self,
        request_scope: str,
        result: GoalExecutionResult,
        goal_revision_id: str,
        recorded_at: datetime,
    ) -> str:
        # Include the revision binding in the immutable envelope, while preserving
        # the established externally visible digest of the result itself.
        from mission_control.domain.authoring.canonical import sha256_digest

        payload = document_payload(result)
        await self._documents.put(
            request_scope=request_scope,
            contract="goal.iteration/1",
            identity=_key(result.identity.iteration.run_id, result.identity.semantic_key),
            payload={"goal_revision_id": goal_revision_id, "result": payload},
            recorded_at=recorded_at,
        )
        return f"goal-iteration:{result.identity.semantic_key}@{sha256_digest(payload)}"

    async def persist_handoff(
        self,
        request_scope: str,
        handoff: GoalHandoff,
        recorded_at: datetime,
    ) -> str:
        document = await self._documents.put(
            request_scope=request_scope,
            contract="goal.handoff/1",
            identity=_key(handoff.run_id, handoff.handoff_id),
            payload=document_payload(handoff),
            recorded_at=recorded_at,
        )
        return f"goal-handoff:{handoff.handoff_id}@{document.digest}"

    async def persist_verification(
        self,
        request_scope: str,
        run_id: str,
        goal_revision_id: str,
        verification: GoalVerificationResult,
        recorded_at: datetime,
    ) -> str:
        from mission_control.domain.authoring.canonical import sha256_digest

        payload = document_payload(verification)
        await self._documents.put(
            request_scope=request_scope,
            contract="goal.verification/1",
            identity=_key(run_id, verification.verification_id),
            payload={"goal_revision_id": goal_revision_id, "verification": payload},
            recorded_at=recorded_at,
        )
        return f"goal-verification:{verification.verification_id}@{sha256_digest(payload)}"

    async def persist_templates(
        self,
        *,
        request_scope: str,
        semantic_input_binding_ref: str,
        executor: OperationExecutionRequest,
        verifier: OperationExecutionRequest,
        recorded_at: datetime,
    ) -> None:
        for role, template in (("executor", executor), ("verifier", verifier)):
            if template.request_scope != request_scope:
                raise ValueError("GoalDirected operation template belongs to another request scope")
            await self._documents.put(
                request_scope=request_scope,
                contract="goal.template/1",
                identity=_key(semantic_input_binding_ref, role),
                payload=stable_json_dump(template),
                recorded_at=recorded_at,
            )

    async def get_template(
        self,
        *,
        semantic_input_binding_ref: str,
        operation_role: str,
        request_scope: str,
        run_id: str,
    ) -> OperationExecutionRequest:
        document = await self._documents.get(
            request_scope=request_scope,
            contract="goal.template/1",
            identity=_key(semantic_input_binding_ref, operation_role),
        )
        if document is None:
            raise ValueError("GoalDirected operation template is unavailable")
        template = OperationExecutionRequest.model_validate(document.payload)
        if template.request_scope != request_scope:
            raise ValueError("GoalDirected operation template belongs to another request scope")
        return template
