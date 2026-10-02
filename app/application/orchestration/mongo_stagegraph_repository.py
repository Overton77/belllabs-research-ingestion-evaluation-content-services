"""Immutable StageGraph operation templates in MongoDB (RRM-009 production composition).

A StageGraph run's operation templates are frozen under its semantic input binding before
launch; `StageGraphOperationPreparationService` resolves them by the interpreter's
`operation_request_key`. Documents are insert-once: a re-persist of identical content is
idempotent, anything else conflicts. A fork-derived run gets its own binding reference, so a
patched template never overwrites the source's.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime

from pymongo.errors import DuplicateKeyError

from app.domain.control_plane.canonical import sha256_digest, stable_json_dump
from app.domain.operation_execution.contracts import OperationExecutionRequest
from app.domain.run_control.errors import IdempotencyConflict
from app.models.stagegraph import StageGraphOperationTemplateDocument


class MongoStageGraphOperationTemplateRepository:
    async def persist_templates(
        self,
        *,
        request_scope: str,
        semantic_input_binding_ref: str,
        templates: Mapping[str, OperationExecutionRequest],
        recorded_at: datetime,
    ) -> None:
        if not templates:
            raise ValueError("StageGraph operation templates cannot be empty")
        for key, template in sorted(templates.items()):
            if not key:
                raise ValueError("StageGraph operation template keys cannot be empty")
            if template.request_scope != request_scope:
                raise ValueError("StageGraph operation template belongs to another request scope")
            payload = stable_json_dump(template)
            document = StageGraphOperationTemplateDocument(
                request_scope=request_scope,
                semantic_input_binding_ref=semantic_input_binding_ref,
                operation_request_key=key,
                document_digest=sha256_digest(payload),
                payload=payload,
                recorded_at=recorded_at,
            )
            try:
                await document.insert()
            except DuplicateKeyError:
                prior = await StageGraphOperationTemplateDocument.find_one(
                    {
                        "request_scope": request_scope,
                        "semantic_input_binding_ref": semantic_input_binding_ref,
                        "operation_request_key": key,
                    }
                )
                if prior is None or prior.document_digest != document.document_digest:
                    raise IdempotencyConflict(
                        "StageGraph operation template identity conflict"
                    ) from None

    async def get_template(
        self,
        *,
        semantic_input_binding_ref: str,
        operation_request_key: str,
        request_scope: str,
        run_id: str,
    ) -> OperationExecutionRequest:
        del run_id
        document = await StageGraphOperationTemplateDocument.find_one(
            {
                "request_scope": request_scope,
                "semantic_input_binding_ref": semantic_input_binding_ref,
                "operation_request_key": operation_request_key,
            }
        )
        if document is None:
            raise ValueError("StageGraph operation template is unavailable")
        if sha256_digest(document.payload) != document.document_digest:
            raise ValueError("StageGraph operation template digest mismatch")
        template = OperationExecutionRequest.model_validate(document.payload)
        if template.request_scope != request_scope:
            raise ValueError("StageGraph operation template belongs to another request scope")
        return template

    async def list_templates(
        self, *, request_scope: str, semantic_input_binding_ref: str
    ) -> dict[str, OperationExecutionRequest]:
        documents = await StageGraphOperationTemplateDocument.find(
            {
                "request_scope": request_scope,
                "semantic_input_binding_ref": semantic_input_binding_ref,
            }
        ).to_list()
        templates: dict[str, OperationExecutionRequest] = {}
        for document in documents:
            if sha256_digest(document.payload) != document.document_digest:
                raise ValueError("StageGraph operation template digest mismatch")
            templates[document.operation_request_key] = OperationExecutionRequest.model_validate(
                document.payload
            )
        return templates


__all__ = ["MongoStageGraphOperationTemplateRepository"]
