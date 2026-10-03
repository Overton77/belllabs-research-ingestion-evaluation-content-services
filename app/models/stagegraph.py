from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from beanie import Document
from pymongo import ASCENDING, IndexModel


class StageGraphOperationTemplateDocument(Document):
    """One immutable StageGraph operation template per semantic input binding and key.

    The key is the interpreter's `operation_request_key`
    (`{stage_id}/{operation_slot_id}/{operation_variant_id}`); the family's preparation
    service resolves it before admitting the operation (RRM-009 production templates).
    """

    contract_id: Literal["CON-BP-STAGEGRAPH-V2"] = "CON-BP-STAGEGRAPH-V2"
    request_scope: str
    semantic_input_binding_ref: str
    operation_request_key: str
    document_digest: str
    payload: dict[str, Any]
    recorded_at: datetime

    class Settings:
        name = "stagegraph_operation_templates"
        indexes = [
            IndexModel(
                [
                    ("request_scope", ASCENDING),
                    ("semantic_input_binding_ref", ASCENDING),
                    ("operation_request_key", ASCENDING),
                ],
                unique=True,
            )
        ]


__all__ = ["StageGraphOperationTemplateDocument"]
