from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from pydantic import SecretStr

from app.application.schema_catalog import parse_schema_catalog
from app.domain.schema_context.contracts import SchemaContextSelection, SchemaSelectionReview
from app.experiments.schema_context_selection.agents import AgentRunOutput
from app.experiments.schema_context_selection.reconciliation_workflow import (
    ReconciliationRunConfig,
    ReportGraphReconciliationWorkflow,
)
from tests.schema_context_helpers import SDL


@pytest.mark.asyncio
async def test_parent_invokes_child_and_offline_run_materializes_projection(tmp_path: Path) -> None:
    schema = tmp_path / "schema.graphql"
    report = tmp_path / "report.md"
    candidates = tmp_path / "candidates.json"
    schema.write_bytes(SDL)
    report.write_text("# TruDiagnostic\nProducts TruAge and TruHealth.", encoding="utf-8")
    candidates.write_text("{}", encoding="utf-8")

    class FakeHarness:
        selected = 0
        reviewed = 0

        def __init__(self, **_kwargs) -> None:  # type: ignore[no-untyped-def]
            pass

        async def select(self, run_root: Path, *, revision_feedback=None):  # type: ignore[no-untyped-def]
            type(self).selected += 1
            request = json.loads((run_root / "inputs/request.json").read_text())
            return AgentRunOutput(
                SchemaContextSelection(
                    selection_id="selection-parent-test",
                    revision=1,
                    purpose=request["purpose"],
                    schema_definition_ref=request["schema_definition_ref"],
                    schema_definition_digest=request["schema_definition_digest"],
                    catalog_digest=request["catalog_digest"],
                    report_ref=request["report_ref"],
                    report_digest=request["report_digest"],
                    selected_node_types=("Organization", "Product"),
                    selected_relationship_types=("OFFERS",),
                    property_intent_hints=(),
                    coverage_obligations=tuple(request["coverage_obligations"]),
                    rationale=(
                        "OrganizationState maps to OrganizationSnapshot; ProductState maps "
                        "to ProductSnapshot."
                    ),
                    evidence_locators=("report.md",),
                    explicit_exclusions=("Document",),
                    unresolved_mappings=(),
                    near_miss_candidates=(),
                    parent_selection_id=None,
                    created_at=datetime.now(UTC),
                ),
                {"total_tokens": 10},
            )

        async def review(  # type: ignore[no-untyped-def]
            self, run_root: Path, *, retry_reason=None
        ):
            type(self).reviewed += 1
            draft = json.loads((run_root / "selection/draft.json").read_text())
            return AgentRunOutput(
                SchemaSelectionReview(
                    review_id="review-parent-test",
                    selection_id=draft["selection_id"],
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
                ),
                {"total_tokens": 5},
            )

        def close(self) -> None:
            pass

    class MinimalSettings:
        openai_api_key = SecretStr("test-only-key")
        sandbox_image = "unused"

    output = tmp_path / "runs"
    result = await ReportGraphReconciliationWorkflow(
        settings=MinimalSettings(),  # type: ignore[arg-type]
        harness_factory=FakeHarness,  # type: ignore[arg-type]
    ).run(
        ReconciliationRunConfig(
            schema_path=schema,
            report_path=report,
            structured_candidates_path=candidates,
            output_root=output,
            run_id="parent-test",
            offline=True,
            semantic_overlay_path=None,
        )
    )

    assert FakeHarness.selected == 1
    assert FakeHarness.reviewed == 1
    assert result.status == "offline"
    assert result.compatibility_decision is not None
    assert result.compatibility_decision.compatible
    assert (output / "parent-test/selection/operation-projection.json").exists()
    assert result.query_result_references == ()


def test_real_fixture_catalog_parses_directly_without_typescript_runtime() -> None:
    catalog = parse_schema_catalog(SDL, "fixture.graphql")
    assert "Organization" in catalog.nodes
    assert "OFFERS" in catalog.relationships
