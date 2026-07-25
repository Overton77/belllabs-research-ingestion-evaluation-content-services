from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from agents.tool_context import ToolContext
from agents.usage import Usage
from pydantic import SecretStr

from app.domain.schema_context.canonicalization import sha256_digest
from app.domain.schema_context.contracts import (
    GraphReconciliationEvidence,
    IntentResultReference,
    QueryExecutionIntent,
    QueryExecutionResult,
    SchemaContextSelection,
    SchemaSelectionReview,
)
from app.domain.schema_context.validation import make_test_attestation
from app.experiments.schema_context_selection.agents import AgentRunOutput
from app.experiments.schema_context_selection.reconciliation_workflow import (
    ReconciliationRunConfig,
    ReportGraphReconciliationWorkflow,
)
from tests.schema_context_helpers import SDL


class _Settings:
    openai_api_key = SecretStr("test-only-key")
    sandbox_image = "unused"


class _Driver:
    closed = False

    async def close(self) -> None:
        self.closed = True


class _Harness:
    review_decision = "accepted"
    planner_calls = 0

    def __init__(self, **_kwargs: Any) -> None:
        pass

    async def select(
        self, run_root: Path, *, revision_feedback: str | None = None
    ) -> AgentRunOutput:
        request = json.loads((run_root / "inputs/request.json").read_text(encoding="utf-8"))
        selection = SchemaContextSelection(
            selection_id="selection-gate-test",
            revision=2 if revision_feedback else 1,
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
            rationale="Bounded test selection with explicit legacy snapshot mappings.",
            evidence_locators=("inputs/report.md",),
            explicit_exclusions=(),
            unresolved_mappings=(
                "OrganizationState -> OrganizationSnapshot",
                "ProductState -> ProductSnapshot",
            ),
            near_miss_candidates=(),
            parent_selection_id=None,
            created_at=datetime.now(UTC),
        )
        return AgentRunOutput(selection, {"total_tokens": 1})

    async def review(self, run_root: Path, *, retry_reason: str | None = None) -> AgentRunOutput:
        selection = json.loads((run_root / "selection/draft.json").read_text(encoding="utf-8"))
        accepted = self.review_decision == "accepted"
        review = SchemaSelectionReview(
            review_id="review-gate-test",
            selection_id=selection["selection_id"],
            reviewer_role="independent_schema_reviewer",
            decision="accepted" if accepted else "revision_required",
            structural_valid=True,
            coverage_findings=("covered",),
            missing_concepts=(),
            overbroad_selections=(),
            unjustified_selections=(),
            temporal_coverage="covered",
            identity_coverage="covered",
            provenance_coverage="covered",
            near_miss_assessment="covered",
            required_revisions=() if accepted else ("Add required semantic coverage.",),
            rationale="Independent fake review.",
            created_at=datetime.now(UTC),
        )
        return AgentRunOutput(review, {"total_tokens": 1})

    async def plan_queries(
        self,
        run_root: Path,
        *,
        execute_tool: Any,
        max_turns: int,
        retry_reason: str | None = None,
    ) -> AgentRunOutput:
        del max_turns, retry_reason
        type(self).planner_calls += 1
        brief = json.loads((run_root / "selection/query-brief.json").read_text(encoding="utf-8"))
        intents = [brief["required_first_intent"], *brief["required_seed_intents"]]
        references: list[IntentResultReference] = []
        for raw_intent in intents:
            arguments = json.dumps({"intent": raw_intent})
            context = ToolContext(
                context=None,
                usage=Usage(),
                tool_name="execute_read_intent",
                tool_call_id=f"call-{raw_intent['intent_id']}",
                tool_arguments=arguments,
            )
            output = await execute_tool.on_invoke_tool(context, arguments)
            result = QueryExecutionResult.model_validate_json(output)
            references.append(
                IntentResultReference(intent_id=result.intent_id, result_id=result.result_id)
            )
        evidence = GraphReconciliationEvidence(
            reconciliation_question="What already exists?",
            query_goals=tuple(item["goal"] for item in intents),
            intent_result_references=tuple(references),
            matched_existing_entities=(),
            existing_relationships=(),
            aliases_used=(),
            match_method="fake_bounded_reads",
            confidence="test",
            unresolved_candidates=(),
            schema_mismatches=(),
            legacy_name_mappings=(),
            query_failures=(),
            stopping_rationale="All required fake intents executed.",
        )
        return AgentRunOutput(evidence, {"total_tokens": 1})

    def close(self) -> None:
        pass


class _RejectingHarness(_Harness):
    review_decision = "revision_required"


class _Executor:
    instances: list[_Executor] = []

    def __init__(self, _driver: Any, *, database: str) -> None:
        self.database = database
        self.intents: list[QueryExecutionIntent] = []
        type(self).instances.append(self)

    async def capability_snapshot(self) -> tuple[tuple[dict, ...], dict[str, Any]]:
        return (({"name": "OrganizationName", "state": "ONLINE"},), {})

    async def execute(self, intent: QueryExecutionIntent, _projection: Any) -> QueryExecutionResult:
        self.intents.append(intent)
        now = datetime.now(UTC)
        logical = {
            "result_id": f"result-{intent.intent_id}",
            "intent_id": intent.intent_id,
            "intent_digest": sha256_digest(intent.model_dump(mode="json")),
            "query_kind": intent.query_kind,
            "status": "succeeded",
            "compiled_cypher": "MATCH (n) RETURN n LIMIT $limit",
            "redacted_parameters": {"limit": intent.limit},
            "columns": ("entity",),
            "records": ({"entity": {"id": "fake-id", "name": "TruDiagnostic"}},),
            "record_count": 1,
            "truncated": False,
            "elapsed_ms": 1,
            "database": self.database,
            "server_info": {"agent": "fake"},
            "diagnostics": (),
            "error_type": None,
            "started_at": now,
            "finished_at": now,
        }
        return QueryExecutionResult(
            **logical,
            result_digest=sha256_digest(
                {**logical, "started_at": now.isoformat(), "finished_at": now.isoformat()}
            ),
        )


def _config(
    tmp_path: Path,
    run_id: str,
    *,
    execution_mode: str = "stagegraph",
) -> ReconciliationRunConfig:
    schema = tmp_path / "schema.graphql"
    report = tmp_path / "report.md"
    schema.write_bytes(SDL)
    report.write_text("# TruDiagnostic\nProducts TruAge and TruHealth.", encoding="utf-8")
    return ReconciliationRunConfig(
        schema_path=schema,
        report_path=report,
        structured_candidates_path=None,
        output_root=tmp_path / "runs",
        run_id=run_id,
        semantic_overlay_path=None,
        execution_mode=execution_mode,  # type: ignore[arg-type]
    )


@pytest.mark.asyncio
async def test_rejected_selection_prevents_driver_and_neo4j_calls(tmp_path: Path) -> None:
    driver_calls = 0

    async def driver_factory(_settings: Any) -> _Driver:
        nonlocal driver_calls
        driver_calls += 1
        return _Driver()

    result = await ReportGraphReconciliationWorkflow(
        settings=_Settings(),  # type: ignore[arg-type]
        driver_factory=driver_factory,
        harness_factory=_RejectingHarness,  # type: ignore[arg-type]
    ).run(_config(tmp_path, "selection-rejected"))

    assert result.status == "selection_rejected"
    assert driver_calls == 0


@pytest.mark.asyncio
async def test_compatibility_failure_prevents_driver_and_neo4j_calls(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SCHEMA_EXPERIMENT_ALLOW_TEST_ATTESTATION", "1")
    driver_calls = 0

    async def driver_factory(_settings: Any) -> _Driver:
        nonlocal driver_calls
        driver_calls += 1
        return _Driver()

    def mismatched_attestation(**kwargs: Any):  # type: ignore[no-untyped-def]
        kwargs["schema_definition_digest"] = "sha256:" + "f" * 64
        return make_test_attestation(**kwargs)

    with pytest.raises(RuntimeError, match="compatibility gate"):
        await ReportGraphReconciliationWorkflow(
            settings=_Settings(),  # type: ignore[arg-type]
            driver_factory=driver_factory,
            attestation_factory=mismatched_attestation,
            harness_factory=_Harness,  # type: ignore[arg-type]
        ).run(_config(tmp_path, "compatibility-rejected"))

    assert driver_calls == 0


@pytest.mark.asyncio
async def test_accepted_selection_executes_and_persists_one_result_per_intent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SCHEMA_EXPERIMENT_ALLOW_TEST_ATTESTATION", "1")
    _Executor.instances.clear()
    _Harness.planner_calls = 0
    driver = _Driver()

    async def driver_factory(_settings: Any) -> _Driver:
        return driver

    config = _config(tmp_path, "accepted-query")
    result = await ReportGraphReconciliationWorkflow(
        settings=_Settings(),  # type: ignore[arg-type]
        driver_factory=driver_factory,
        executor_factory=_Executor,  # type: ignore[arg-type]
        harness_factory=_Harness,  # type: ignore[arg-type]
    ).run(config)

    executor = _Executor.instances[-1]
    query_root = config.output_root / config.run_id / "queries"
    intent_files = sorted(query_root.glob("[0-9][0-9][0-9]-intent.json"))
    result_files = sorted(query_root.glob("[0-9][0-9][0-9]-result.json"))
    assert result.status == "completed"
    assert len(executor.intents) == len(intent_files) == len(result_files) == 2
    assert len(result.query_result_references) == 2
    assert len(result.reconciliation_evidence.intent_result_references) == 2
    assert result.reconciliation_evidence.match_method == (
        "exact_identity + bounded_neighborhood"
    )
    assert _Harness.planner_calls == 0
    run = json.loads((config.output_root / config.run_id / "run.json").read_text())
    assert run["execution_mode"] == "stagegraph"
    assert "stagegraph-required-intents" in run["implementation_id"]
    assert driver.closed


@pytest.mark.asyncio
async def test_goal_directed_mode_preserves_bounded_agent_planner(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SCHEMA_EXPERIMENT_ALLOW_TEST_ATTESTATION", "1")
    _Executor.instances.clear()
    _Harness.planner_calls = 0
    driver = _Driver()

    async def driver_factory(_settings: Any) -> _Driver:
        return driver

    config = _config(tmp_path, "goal-directed-query", execution_mode="goal-directed")
    result = await ReportGraphReconciliationWorkflow(
        settings=_Settings(),  # type: ignore[arg-type]
        driver_factory=driver_factory,
        executor_factory=_Executor,  # type: ignore[arg-type]
        harness_factory=_Harness,  # type: ignore[arg-type]
    ).run(config)

    assert result.status == "completed"
    assert _Harness.planner_calls == 1
    assert result.reconciliation_evidence.match_method == "fake_bounded_reads"
    runtime = result.evaluation_metrics["runtime"]
    assert runtime["execution_mode"] == "goal-directed"
    assert "goal-directed-planner" in runtime["implementation_id"]
