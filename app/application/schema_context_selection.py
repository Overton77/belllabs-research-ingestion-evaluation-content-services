from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Protocol

from app.application.schema_catalog import SchemaCatalog
from app.application.schema_grounding_repository import (
    SchemaGroundingRecordRepository,
    schema_grounding_record,
)
from app.domain.schema_context.canonicalization import write_json
from app.domain.schema_context.contracts import (
    AcceptedSchemaContextSelection,
    SchemaContextSelection,
    SchemaContextSelectionRequest,
    SchemaSelectionReview,
    SelectionValidationDiagnostic,
)
from app.domain.schema_context.validation import accept_selection, validate_selection
from app.domain.schema_grounding.contracts import SchemaGroundingRecordType


@dataclass(frozen=True)
class AgentRunOutput:
    output: Any
    usage: dict[str, int]


class SelectionAgentPort(Protocol):
    async def select(
        self, run_root: Path, *, revision_feedback: str | None = None
    ) -> AgentRunOutput: ...


class ReviewAgentPort(Protocol):
    async def review(
        self, run_root: Path, *, retry_reason: str | None = None
    ) -> AgentRunOutput: ...


@dataclass(frozen=True)
class SelectionWorkflowOutcome:
    draft: SchemaContextSelection
    validation: SelectionValidationDiagnostic
    review: SchemaSelectionReview
    accepted: AcceptedSchemaContextSelection | None
    usage: dict[str, int]
    revision_count: int


class SchemaContextSelectionWorkflow:
    """Application-owned bounded selector/validator/independent-review workflow."""

    def __init__(
        self,
        *,
        selector: SelectionAgentPort,
        reviewer: ReviewAgentPort,
        catalog: SchemaCatalog,
        records: SchemaGroundingRecordRepository | None = None,
        request_scope: str = "experiment",
        run_id: str | None = None,
    ) -> None:
        if selector is reviewer:
            # One runtime adapter may implement both ports, but selection and review still execute
            # as distinct semantic operations. Authority separation is enforced by their bindings.
            pass
        self.selector = selector
        self.reviewer = reviewer
        self.catalog = catalog
        self._records = records
        self._request_scope = request_scope
        self._run_id = run_id

    async def run(
        self,
        request: SchemaContextSelectionRequest,
        run_root: Path,
    ) -> SelectionWorkflowOutcome:
        usage = {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0, "requests": 0}
        feedback: str | None = None
        for revision in (1, 2):
            selected = await self.selector.select(run_root, revision_feedback=feedback)
            draft = selected.output
            if not isinstance(draft, SchemaContextSelection):
                draft = SchemaContextSelection.model_validate(draft)
            _add_usage(usage, selected.usage)
            write_json(run_root / "selection" / "draft.json", draft.model_dump(mode="json"))
            await self._persist("selection_draft", draft.selection_id, draft, draft.created_at)

            validation = validate_selection(request, draft, self.catalog)
            write_json(
                run_root / "selection" / "deterministic-validation.json",
                validation.model_dump(mode="json"),
            )
            await self._persist(
                "selection_validation",
                f"{draft.selection_id}:validation",
                validation,
                draft.created_at,
            )

            review: SchemaSelectionReview | None = None
            retry_reason: str | None = None
            for review_attempt in (1, 2):
                reviewed = await self.reviewer.review(run_root, retry_reason=retry_reason)
                review = reviewed.output
                if not isinstance(review, SchemaSelectionReview):
                    review = SchemaSelectionReview.model_validate(review)
                _add_usage(usage, reviewed.usage)
                if review.selection_id == draft.selection_id:
                    break
                write_json(
                    run_root / "selection" / f"review-binding-failure-{review_attempt}.json",
                    review.model_dump(mode="json"),
                )
                await self._persist(
                    "selection_review",
                    f"{review.review_id}:discarded-binding-attempt:{review_attempt}",
                    review,
                    review.created_at,
                )
                retry_reason = (
                    "selection_id binding mismatch; expected "
                    f"`{draft.selection_id}` but received `{review.selection_id}`. "
                    "Return a fresh independent review with the expected identifier."
                )
            assert review is not None
            write_json(run_root / "selection" / "review.json", review.model_dump(mode="json"))
            await self._persist("selection_review", review.review_id, review, review.created_at)

            review_is_bound = review.selection_id == draft.selection_id
            if validation.structurally_valid and review_is_bound and review.decision == "accepted":
                accepted = accept_selection(draft, validation, review)
                write_json(
                    run_root / "selection" / "accepted.json",
                    accepted.model_dump(mode="json"),
                )
                await self._persist(
                    "accepted_selection",
                    draft.selection_id,
                    accepted,
                    accepted.accepted_at,
                )
                return SelectionWorkflowOutcome(
                    draft, validation, review, accepted, usage, revision
                )
            if revision == 1 and review_is_bound and review.decision == "revision_required":
                feedback = "\n".join(review.required_revisions + validation.errors)
                continue
            return SelectionWorkflowOutcome(draft, validation, review, None, usage, revision)
        raise AssertionError("bounded semantic selection revision loop exhausted unexpectedly")

    async def _persist(
        self,
        record_type: SchemaGroundingRecordType,
        record_id: str,
        value: Any,
        created_at: datetime,
    ) -> None:
        if self._records is None:
            return
        payload = value.model_dump(mode="json")
        await self._records.append(
            schema_grounding_record(
                record_type=record_type,
                record_id=record_id,
                request_scope=self._request_scope,
                run_id=self._run_id,
                payload=payload,
                created_at=created_at,
            )
        )


def _add_usage(total: dict[str, int], addition: dict[str, int]) -> None:
    for key, value in addition.items():
        total[key] = total.get(key, 0) + value


__all__ = [
    "AgentRunOutput",
    "ReviewAgentPort",
    "SchemaContextSelectionWorkflow",
    "SelectionAgentPort",
    "SelectionWorkflowOutcome",
]
