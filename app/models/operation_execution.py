from __future__ import annotations

from datetime import datetime
from typing import Any

from beanie import Document
from pymongo import ASCENDING, IndexModel


class OperationExecutionBindingDocument(Document):
    binding_id: str
    semantic_attempt_key: str
    request_fingerprint: str
    run_id: str
    operation_id: str
    operation_attempt: int
    payload: dict[str, Any]
    bound_at: datetime

    class Settings:
        name = "operation_execution_bindings"
        indexes = [
            IndexModel([("binding_id", ASCENDING)], unique=True),
            IndexModel([("semantic_attempt_key", ASCENDING)], unique=True),
            IndexModel([("run_id", ASCENDING), ("operation_id", ASCENDING)]),
        ]


class OperationSettlementDocument(Document):
    settlement_id: str
    binding_id: str
    payload: dict[str, Any]
    settled_at: datetime

    class Settings:
        name = "operation_execution_settlements"
        indexes = [
            IndexModel([("settlement_id", ASCENDING)], unique=True),
            IndexModel([("binding_id", ASCENDING)], unique=True),
        ]


class OperationExecutionClaimDocument(Document):
    side_effect_key: str
    binding_id: str
    claimed_at: datetime

    class Settings:
        name = "operation_execution_claims"
        indexes = [
            IndexModel([("side_effect_key", ASCENDING)], unique=True),
            IndexModel([("binding_id", ASCENDING)], unique=True),
        ]
