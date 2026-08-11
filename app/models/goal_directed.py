from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from beanie import Document
from pydantic import Field
from pymongo import ASCENDING, IndexModel


class GoalOperationTemplateDocument(Document):
    contract_id: Literal["CON-BP-GOAL-DIRECTED-V1"] = "CON-BP-GOAL-DIRECTED-V1"
    request_scope: str
    semantic_input_binding_ref: str
    operation_role: Literal["executor", "verifier"]
    document_digest: str
    payload: dict[str, Any]
    recorded_at: datetime

    class Settings:
        name = "goal_directed_operation_templates"
        indexes = [
            IndexModel(
                [
                    ("request_scope", ASCENDING),
                    ("semantic_input_binding_ref", ASCENDING),
                    ("operation_role", ASCENDING),
                ],
                unique=True,
            )
        ]


class GoalRevisionDocument(Document):
    contract_id: Literal["CON-BP-GOAL-DIRECTED-V1"] = "CON-BP-GOAL-DIRECTED-V1"
    request_scope: str
    run_id: str
    goal_revision_id: str
    revision: int = Field(ge=1)
    envelope_digest: str
    document_digest: str
    payload: dict[str, Any]
    recorded_at: datetime

    class Settings:
        name = "goal_directed_revisions"
        indexes = [
            IndexModel(
                [
                    ("request_scope", ASCENDING),
                    ("run_id", ASCENDING),
                    ("goal_revision_id", ASCENDING),
                ],
                unique=True,
            )
        ]


class GoalIterationDocument(Document):
    contract_id: Literal["CON-BP-GOAL-DIRECTED-V1"] = "CON-BP-GOAL-DIRECTED-V1"
    request_scope: str
    run_id: str
    iteration_key: str
    goal_revision_id: str
    document_digest: str
    payload: dict[str, Any]
    recorded_at: datetime

    class Settings:
        name = "goal_directed_iterations"
        indexes = [
            IndexModel(
                [("request_scope", ASCENDING), ("run_id", ASCENDING), ("iteration_key", ASCENDING)],
                unique=True,
            )
        ]


class GoalHandoffDocument(Document):
    contract_id: Literal["CON-BP-GOAL-HANDOFF-V1"] = "CON-BP-GOAL-HANDOFF-V1"
    request_scope: str
    run_id: str
    handoff_id: str
    goal_revision_id: str
    document_digest: str
    payload: dict[str, Any]
    recorded_at: datetime

    class Settings:
        name = "goal_directed_handoffs"
        indexes = [
            IndexModel(
                [("request_scope", ASCENDING), ("run_id", ASCENDING), ("handoff_id", ASCENDING)],
                unique=True,
            )
        ]


class GoalVerificationDocument(Document):
    contract_id: Literal["CON-BP-GOAL-VERIFICATION-V1"] = "CON-BP-GOAL-VERIFICATION-V1"
    request_scope: str
    run_id: str
    verification_id: str
    goal_revision_id: str
    document_digest: str
    payload: dict[str, Any]
    recorded_at: datetime

    class Settings:
        name = "goal_directed_verifications"
        indexes = [
            IndexModel(
                [
                    ("request_scope", ASCENDING),
                    ("run_id", ASCENDING),
                    ("verification_id", ASCENDING),
                ],
                unique=True,
            )
        ]


__all__ = [
    "GoalHandoffDocument",
    "GoalIterationDocument",
    "GoalOperationTemplateDocument",
    "GoalRevisionDocument",
    "GoalVerificationDocument",
]
