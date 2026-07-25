from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from app.application.schema_catalog import SchemaCatalog
from app.domain.schema_context.contracts import (
    AcceptedSchemaContextSelection,
    SchemaContextSelection,
    SchemaContextSelectionRequest,
    SchemaSelectionReview,
    SelectionValidationDiagnostic,
)
from app.domain.schema_context.validation import accept_selection, validate_selection
from app.experiments.schema_context_selection.agents import (
    ReviewAgentPort,
    SelectionAgentPort,
)
from app.experiments.schema_context_selection.prompts import (
    REVIEWER_INSTRUCTIONS,
    SELECTOR_INSTRUCTIONS,
)
from app.experiments.schema_context_selection.workspace import write_json, write_text


@dataclass(frozen=True)
class SelectionWorkflowOutcome:
    draft: SchemaContextSelection
    validation: SelectionValidationDiagnostic
    review: SchemaSelectionReview
    accepted: AcceptedSchemaContextSelection | None
    usage: dict[str, int]
    revision_count: int


class SchemaContextSelectionWorkflow:
    def __init__(
        self,
        *,
        selector: SelectionAgentPort,
        reviewer: ReviewAgentPort,
        catalog: SchemaCatalog,
    ) -> None:
        self.selector = selector
        self.reviewer = reviewer
        self.catalog = catalog

    async def run(
        self,
        request: SchemaContextSelectionRequest,
        run_root: Path,
    ) -> SelectionWorkflowOutcome:
        write_text(run_root / "selection" / "selector-prompt.md", SELECTOR_INSTRUCTIONS)
        write_text(run_root / "selection" / "reviewer-prompt.md", REVIEWER_INSTRUCTIONS)
        usage = {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0, "requests": 0}
        feedback: str | None = None
        for revision in (1, 2):
            selected = await self.selector.select(run_root, revision_feedback=feedback)
            draft = selected.output
            if not isinstance(draft, SchemaContextSelection):
                draft = SchemaContextSelection.model_validate(draft)
            _add_usage(usage, selected.usage)
            write_json(run_root / "selection" / "draft.json", draft.model_dump(mode="json"))
            validation = validate_selection(request, draft, self.catalog)
            write_json(
                run_root / "selection" / "deterministic-validation.json",
                validation.model_dump(mode="json"),
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
                retry_reason = (
                    "selection_id binding mismatch; expected "
                    f"`{draft.selection_id}` but received `{review.selection_id}`. "
                    "Return a fresh independent review with the expected identifier."
                )
            assert review is not None
            write_json(run_root / "selection" / "review.json", review.model_dump(mode="json"))
            review_is_bound = review.selection_id == draft.selection_id
            if validation.structurally_valid and review_is_bound and review.decision == "accepted":
                accepted = accept_selection(draft, validation, review)
                write_json(
                    run_root / "selection" / "accepted.json",
                    accepted.model_dump(mode="json"),
                )
                return SelectionWorkflowOutcome(
                    draft, validation, review, accepted, usage, revision
                )
            if revision == 1 and review_is_bound and review.decision == "revision_required":
                feedback = "\n".join(review.required_revisions + validation.errors)
                continue
            return SelectionWorkflowOutcome(draft, validation, review, None, usage, revision)
        raise AssertionError("bounded revision loop exhausted unexpectedly")


def _add_usage(total: dict[str, int], addition: dict[str, int]) -> None:
    for key, value in addition.items():
        total[key] = total.get(key, 0) + value
