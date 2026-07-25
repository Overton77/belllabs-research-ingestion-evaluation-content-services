from __future__ import annotations

from pathlib import Path

import pytest

from app.domain.schema_context.contracts import SchemaSelectionReview
from app.domain.schema_context.validation import accept_selection, validate_selection
from app.experiments.schema_context_selection.agents import AgentRunOutput
from app.experiments.schema_context_selection.selection_workflow import (
    SchemaContextSelectionWorkflow,
)
from tests.schema_context_helpers import catalog, request, selection


def test_unknown_name_and_lineage_mismatch_are_rejected() -> None:
    value = catalog()
    draft = selection(value).model_copy(
        update={
            "selected_node_types": ("Missing", "Organization", "Product"),
            "schema_definition_digest": "sha256:" + "f" * 64,
        }
    )
    diagnostic = validate_selection(request(value), draft, value)
    assert not diagnostic.structurally_valid
    assert any("unknown node" in item for item in diagnostic.errors)
    assert any("schema_definition_digest" in item for item in diagnostic.errors)


def test_selector_cannot_self_approve() -> None:
    value = catalog()
    draft = selection(value)
    diagnostic = validate_selection(request(value), draft, value)
    review = _review(draft.selection_id).model_copy(update={"decision": "revision_required"})
    with pytest.raises(ValueError, match="did not accept"):
        accept_selection(draft, diagnostic, review)


@pytest.mark.asyncio
async def test_child_workflow_runs_selector_then_independent_reviewer(tmp_path: Path) -> None:
    value = catalog()
    draft = selection(value)
    (tmp_path / "selection").mkdir()

    class Selector:
        calls = 0

        async def select(self, _root: Path, *, revision_feedback=None):  # type: ignore[no-untyped-def]
            self.calls += 1
            return AgentRunOutput(draft, {"total_tokens": 1})

    class Reviewer:
        calls = 0

        async def review(  # type: ignore[no-untyped-def]
            self, _root: Path, *, retry_reason=None
        ):
            self.calls += 1
            return AgentRunOutput(_review(draft.selection_id), {"total_tokens": 1})

    selector = Selector()
    reviewer = Reviewer()
    outcome = await SchemaContextSelectionWorkflow(
        selector=selector, reviewer=reviewer, catalog=value
    ).run(request(value), tmp_path)
    assert selector.calls == 1
    assert reviewer.calls == 1
    assert outcome.accepted is not None


@pytest.mark.asyncio
async def test_reviewer_binding_mismatch_is_retried_without_rerunning_selector(
    tmp_path: Path,
) -> None:
    value = catalog()
    draft = selection(value)
    (tmp_path / "selection").mkdir()

    class Selector:
        calls = 0

        async def select(self, _root: Path, *, revision_feedback=None):  # type: ignore[no-untyped-def]
            self.calls += 1
            return AgentRunOutput(draft, {"total_tokens": 1})

    class Reviewer:
        calls = 0
        retry_reasons: list[str | None] = []

        async def review(  # type: ignore[no-untyped-def]
            self, _root: Path, *, retry_reason=None
        ):
            self.calls += 1
            self.retry_reasons.append(retry_reason)
            selection_id = "mistyped-selection-id" if self.calls == 1 else draft.selection_id
            return AgentRunOutput(_review(selection_id), {"total_tokens": 1})

    selector = Selector()
    reviewer = Reviewer()
    outcome = await SchemaContextSelectionWorkflow(
        selector=selector, reviewer=reviewer, catalog=value
    ).run(request(value), tmp_path)

    assert selector.calls == 1
    assert reviewer.calls == 2
    assert reviewer.retry_reasons[0] is None
    assert "binding mismatch" in reviewer.retry_reasons[1]
    assert (tmp_path / "selection" / "review-binding-failure-1.json").is_file()
    assert outcome.accepted is not None


def _review(selection_id: str) -> SchemaSelectionReview:
    from datetime import UTC, datetime

    return SchemaSelectionReview(
        review_id="review-1",
        selection_id=selection_id,
        reviewer_role="independent_schema_reviewer",
        decision="accepted",
        structural_valid=True,
        coverage_findings=("covered",),
        missing_concepts=(),
        overbroad_selections=(),
        unjustified_selections=(),
        temporal_coverage="covered",
        identity_coverage="covered",
        provenance_coverage="covered",
        near_miss_assessment="covered",
        required_revisions=(),
        rationale="independent acceptance",
        created_at=datetime.now(UTC),
    )
