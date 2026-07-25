from __future__ import annotations

import json
import os
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from time import perf_counter
from typing import Any
from uuid import NAMESPACE_URL, uuid5

from agents import function_tool, set_default_openai_key
from neo4j import AsyncDriver

from app.application.schema_catalog import DEFAULT_SEMANTIC_OVERLAY, parse_schema_catalog
from app.application.schema_workspace import materialize_schema_workspace
from app.config import Settings
from app.domain.schema_context.canonicalization import sha256_digest
from app.domain.schema_context.contracts import (
    GraphReconciliationEvidence,
    QueryExecutionIntent,
    QueryExecutionResult,
    ReportGraphReconciliationResult,
    SchemaContextSelectionRequest,
    SchemaDeploymentAttestation,
)
from app.domain.schema_context.expansion import expand_selection
from app.domain.schema_context.projection import build_operation_projection
from app.domain.schema_context.validation import (
    decide_compatibility,
    make_test_attestation,
)
from app.experiments.schema_context_selection.agents import SandboxAgentHarness
from app.experiments.schema_context_selection.evaluation import (
    evaluate_query_results,
    evaluate_selection,
)
from app.experiments.schema_context_selection.prompts import QUERY_PLANNER_INSTRUCTIONS
from app.experiments.schema_context_selection.selection_workflow import (
    SchemaContextSelectionWorkflow,
)
from app.experiments.schema_context_selection.workspace import (
    freeze_input,
    reset_run_directory,
    write_json,
    write_source_manifest,
    write_text,
)
from app.integrations.neo4j import create_neo4j
from app.integrations.neo4j_read_executor import (
    Neo4jReadExecutor,
    rejected_query_result,
)

DEFAULT_MODEL = "gpt-5-mini"


@dataclass(frozen=True)
class ReconciliationRunConfig:
    schema_path: Path
    report_path: Path
    structured_candidates_path: Path | None
    output_root: Path
    run_id: str
    model: str = DEFAULT_MODEL
    build_only: bool = False
    offline: bool = False
    skip_vector: bool = True
    max_query_intents: int = 12
    database: str = "neo4j"
    semantic_overlay_path: Path | None = DEFAULT_SEMANTIC_OVERLAY


class ReportGraphReconciliationWorkflow:
    def __init__(
        self,
        *,
        settings: Settings | None = None,
        driver_factory: Callable[[Settings], Any] = create_neo4j,
        executor_factory: Callable[..., Neo4jReadExecutor] = Neo4jReadExecutor,
        attestation_factory: Callable[..., SchemaDeploymentAttestation] = make_test_attestation,
        harness_factory: Callable[..., SandboxAgentHarness] = SandboxAgentHarness,
    ) -> None:
        self.settings = settings
        self.driver_factory = driver_factory
        self.executor_factory = executor_factory
        self.attestation_factory = attestation_factory
        self.harness_factory = harness_factory

    async def run(self, config: ReconciliationRunConfig) -> ReportGraphReconciliationResult:
        started = perf_counter()
        run_root = (config.output_root / config.run_id).resolve()
        reset_run_directory(run_root)
        for name in ("selection", "queries", "traces", "schema/runtime"):
            (run_root / name).mkdir(parents=True, exist_ok=True)
        timings: dict[str, int] = {}
        warnings = [
            "Test-only attestation proves experiment gate mechanics, not a production "
            "deployment attestation.",
            "Research reconciliation output is not medical advice.",
        ]
        self._event(run_root, "run.started", {"run_id": config.run_id})
        write_json(
            run_root / "run.json",
            {
                "run_id": config.run_id,
                "model": config.model,
                "build_only": config.build_only,
                "offline": config.offline,
                "skip_vector": config.skip_vector,
                "max_query_intents": config.max_query_intents,
                "started_at": datetime.now(UTC).isoformat(),
            },
        )

        stage = perf_counter()
        schema_source = config.schema_path.resolve().read_bytes()
        schema_digest = sha256_digest(schema_source)
        report_digest = freeze_input(config.report_path.resolve(), run_root / "inputs/report.md")
        candidate_digest: str | None = None
        if config.structured_candidates_path:
            candidate_digest = freeze_input(
                config.structured_candidates_path.resolve(),
                run_root / "inputs/structured-extract-products-biomarkers.json",
            )
        else:
            write_json(run_root / "inputs/structured-extract-products-biomarkers.json", {})
        catalog = parse_schema_catalog(
            schema_source,
            str(config.schema_path.resolve()),
            semantic_overlay=config.semantic_overlay_path,
        )
        repeat_catalog = parse_schema_catalog(
            schema_source,
            str(config.schema_path.resolve()),
            semantic_overlay=config.semantic_overlay_path,
        )
        if repeat_catalog.catalog_digest != catalog.catalog_digest:
            raise RuntimeError("deterministic catalog rebuild changed the logical digest")
        manifest = materialize_schema_workspace(
            catalog,
            schema_source,
            run_root / "schema",
            report=(run_root / "inputs/report.md").read_bytes(),
        )
        source_inputs = {
            "schema": {
                "ref": str(config.schema_path.resolve()),
                "digest": schema_digest,
            },
            "report": {
                "ref": str(config.report_path.resolve()),
                "digest": report_digest,
            },
        }
        if candidate_digest and config.structured_candidates_path:
            source_inputs["structured_candidates"] = {
                "ref": str(config.structured_candidates_path.resolve()),
                "digest": candidate_digest,
            }
        write_source_manifest(run_root, source_inputs)
        timings["catalog_ms"] = int((perf_counter() - stage) * 1000)
        workspace_metrics = {
            "catalog_resource_count": len(manifest["resources"]),
            "node_count": len(catalog.nodes),
            "relationship_count": len(catalog.relationships),
            "total_catalog_bytes": sum(item["size_bytes"] for item in manifest["resources"]),
            "deterministic_digest_stability": True,
        }
        self._event(run_root, "catalog.completed", workspace_metrics)
        base_inputs = {
            "schema": schema_digest,
            "report": report_digest,
            **({"structured_candidates": candidate_digest} if candidate_digest else {}),
        }
        if config.build_only:
            metrics = {"workspace": workspace_metrics, "runtime": timings}
            result = self._result(
                config=config,
                run_root=run_root,
                status="build_only",
                input_digests=base_inputs,
                schema_digest=schema_digest,
                catalog_digest=catalog.catalog_digest,
                usage={},
                timings=timings,
                metrics=metrics,
                warnings=warnings,
            )
            self._finish(run_root, result, metrics, started)
            return result

        request = SchemaContextSelectionRequest(
            request_id=str(uuid5(NAMESPACE_URL, f"selection-request:{config.run_id}")),
            purpose="pre_ingestion_graph_reconciliation",
            intended_operations=("read", "exact_lookup", "bounded_traversal"),
            schema_definition_ref=str(config.schema_path.resolve()),
            schema_definition_digest=schema_digest,
            catalog_digest=catalog.catalog_digest,
            report_ref=str(config.report_path.resolve()),
            report_digest=report_digest,
            coverage_obligations=(
                "organization_identity",
                "offered_products",
                "lab_tests_panels_platforms",
                "biomarker_sample",
                "legacy_state_snapshot_mapping",
                "provenance_boundary",
            ),
            workspace_ref=str(run_root),
            created_at=datetime.now(UTC),
        )
        write_json(run_root / "inputs/request.json", request.model_dump(mode="json"))

        settings = self.settings or Settings()
        set_default_openai_key(settings.openai_api_key.get_secret_value())
        harness = self.harness_factory(model=config.model, image=settings.sandbox_image)
        driver: AsyncDriver | None = None
        try:
            stage = perf_counter()
            child = SchemaContextSelectionWorkflow(
                selector=harness,
                reviewer=harness,
                catalog=catalog,
            )
            selection_outcome = await child.run(request, run_root)
            timings["selection_ms"] = int((perf_counter() - stage) * 1000)
            if selection_outcome.accepted is None:
                metrics = {
                    "workspace": workspace_metrics,
                    "selection": {
                        **evaluate_selection(selection_outcome.draft),
                        "review_decision": selection_outcome.review.decision,
                        "revision_count": selection_outcome.revision_count,
                    },
                }
                result = self._result(
                    config=config,
                    run_root=run_root,
                    status="selection_rejected",
                    input_digests=base_inputs,
                    schema_digest=schema_digest,
                    catalog_digest=catalog.catalog_digest,
                    usage=selection_outcome.usage,
                    timings=timings,
                    metrics=metrics,
                    warnings=warnings,
                    selection_ref="selection/draft.json",
                    review_ref="selection/review.json",
                )
                self._finish(run_root, result, metrics, started)
                return result

            accepted = selection_outcome.accepted
            expanded = expand_selection(accepted, catalog)
            write_json(run_root / "selection/expanded-slice.json", expanded.model_dump(mode="json"))
            expanded_bytes = (run_root / "selection/expanded-slice.json").stat().st_size
            selector_transcripts = tuple(
                (run_root / "agent-runs").glob(
                    "schemacontextselectionworkflow.selector-*-transcript.json"
                )
            )
            selector_text = "\n".join(
                path.read_text(encoding="utf-8") for path in selector_transcripts
            )
            observable_schema_files = tuple(
                sorted(
                    resource["logical_path"]
                    for resource in manifest["resources"]
                    if resource["logical_path"] in selector_text
                )
            )
            workspace_metrics.update(
                {
                    "selected_schema_slice_bytes": expanded_bytes,
                    "selected_to_catalog_byte_ratio": (
                        expanded_bytes / workspace_metrics["total_catalog_bytes"]
                    ),
                    "selector_observable_files": observable_schema_files,
                    "selector_observable_file_count": len(observable_schema_files),
                }
            )
            attestation = self.attestation_factory(
                environment="local-preproduction-experiment",
                database=config.database,
                schema_definition_ref=str(config.schema_path.resolve()),
                schema_definition_digest=schema_digest,
            )
            decision = decide_compatibility(schema_digest, attestation)
            write_json(
                run_root / "schema/runtime/deployment-attestation.json",
                attestation.model_dump(mode="json"),
            )
            write_json(
                run_root / "schema/runtime/compatibility-decision.json",
                decision.model_dump(mode="json"),
            )

            live_indexes: tuple[dict, ...] = ()
            executor: Neo4jReadExecutor | None = None
            if config.offline:
                warnings.append(
                    "Offline mode: no live Neo4j capability snapshot or query execution occurred."
                )
                write_json(run_root / "schema/runtime/live-indexes.json", [])
                write_json(run_root / "schema/runtime/live-schema.json", {"offline": True})
                write_json(run_root / "schema/runtime/index-options.json", {})
            else:
                if os.getenv("SCHEMA_EXPERIMENT_ALLOW_TEST_ATTESTATION") != "1":
                    raise RuntimeError(
                        "SCHEMA_EXPERIMENT_ALLOW_TEST_ATTESTATION=1 is required for live reads"
                    )
                if not decision.compatible:
                    raise RuntimeError("schema compatibility gate rejected live query execution")
                driver = await self.driver_factory(settings)
                executor = self.executor_factory(driver, database=config.database)
                stage = perf_counter()
                live_indexes, live_schema = await executor.capability_snapshot()
                timings["neo4j_snapshot_ms"] = int((perf_counter() - stage) * 1000)
                write_json(run_root / "schema/runtime/live-indexes.json", live_indexes)
                write_json(run_root / "schema/runtime/live-schema.json", live_schema)
                write_json(
                    run_root / "schema/runtime/index-options.json",
                    {str(item.get("name")): item.get("options", {}) for item in live_indexes},
                )
            projection = build_operation_projection(
                accepted,
                expanded,
                live_indexes=live_indexes,
                allow_vector=not config.skip_vector,
            )
            write_json(
                run_root / "selection/operation-projection.json",
                projection.model_dump(mode="json"),
            )
            required_first_intent = QueryExecutionIntent(
                intent_id=f"{config.run_id}-organization-exact-1",
                sequence=1,
                purpose="pre_ingestion_graph_reconciliation",
                query_kind="exact_identity",
                projection_id=projection.projection_id,
                projection_digest=projection.projection_digest,
                schema_definition_digest=projection.source_schema_digest,
                selection_digest=projection.accepted_selection_digest,
                goal="Locate the existing TruDiagnostic Organization by exact name.",
                coverage_obligation_ids=("organization_identity",),
                labels=("Organization",),
                relationship_types=(),
                parameters={"field": "name", "value": "TruDiagnostic"},
                requested_fields=("id", "name", "legalName", "displayName"),
                limit=5,
                max_depth=0,
                stopping_evidence="A matching Organization identity record or a proven zero match.",
                semantic_query_text=None,
                proposed_cypher=None,
                created_at=datetime.now(UTC),
            )
            required_seed_intents = [
                QueryExecutionIntent(
                    intent_id=f"{config.run_id}-organization-offers-2",
                    sequence=2,
                    purpose="pre_ingestion_graph_reconciliation",
                    query_kind="bounded_neighborhood",
                    projection_id=projection.projection_id,
                    projection_digest=projection.projection_digest,
                    schema_definition_digest=projection.source_schema_digest,
                    selection_digest=projection.accepted_selection_digest,
                    goal="Recover products connected to TruDiagnostic through OFFERS.",
                    coverage_obligation_ids=("offered_products",),
                    labels=("Organization",),
                    relationship_types=("OFFERS",),
                    parameters={"field": "name", "value": "TruDiagnostic"},
                    requested_fields=("id", "name", "description"),
                    limit=20,
                    max_depth=1,
                    stopping_evidence="The bounded OFFERS neighborhood has been observed.",
                    semantic_query_text=None,
                    proposed_cypher=None,
                    created_at=datetime.now(UTC),
                )
            ]
            product_relationships = tuple(
                relationship
                for relationship in (
                    "DELIVERS_LABTEST",
                    "IMPLEMENTS_PANEL",
                    "IMPLEMENTS",
                )
                if relationship in projection.allowed_relationship_types
            )
            for sequence, product_name in enumerate(
                ("TruAge", "TruHealth", "TruAge + TruHealth"), start=3
            ) if product_relationships else ():
                required_seed_intents.append(
                    QueryExecutionIntent(
                        intent_id=f"{config.run_id}-product-neighborhood-{sequence}",
                        sequence=sequence,
                        purpose="pre_ingestion_graph_reconciliation",
                        query_kind="bounded_neighborhood",
                        projection_id=projection.projection_id,
                        projection_digest=projection.projection_digest,
                        schema_definition_digest=projection.source_schema_digest,
                        selection_digest=projection.accepted_selection_digest,
                        goal=(
                            f"Recover admitted lab-test, panel, and platform neighbors for "
                            f"{product_name}."
                        ),
                        coverage_obligation_ids=("lab_tests_panels_platforms",),
                        labels=("Product",),
                        relationship_types=product_relationships,
                        parameters={"field": "name", "value": product_name},
                        requested_fields=("id", "name", "description"),
                        limit=100,
                        max_depth=1,
                        stopping_evidence="The bounded product neighborhood has been observed.",
                        semantic_query_text=None,
                        proposed_cypher=None,
                        created_at=datetime.now(UTC),
                    )
                )
            write_json(
                run_root / "selection/query-brief.json",
                {
                    "purpose": projection.purpose,
                    "projection_id": projection.projection_id,
                    "projection_digest": projection.projection_digest,
                    "schema_definition_digest": projection.source_schema_digest,
                    "selection_digest": projection.accepted_selection_digest,
                    "coverage_obligations": accepted.selection.coverage_obligations,
                    "allowed_node_labels": projection.allowed_node_labels,
                    "allowed_relationship_types": projection.allowed_relationship_types,
                    "allowed_traversals": projection.allowed_traversals,
                    "identity_fields_by_label": projection.identity_fields_by_label,
                    "online_fulltext_capabilities": tuple(
                        item
                        for item in projection.fulltext_capabilities
                        if item.get("live_online")
                    ),
                    "required_first_intent": required_first_intent.model_dump(mode="json"),
                    "required_seed_intents": tuple(
                        intent.model_dump(mode="json") for intent in required_seed_intents
                    ),
                },
            )
            write_text(run_root / "queries/planner-prompt.md", QUERY_PLANNER_INSTRUCTIONS)

            query_results: list[QueryExecutionResult] = []
            query_intents: list[QueryExecutionIntent] = []
            if executor is not None:

                @function_tool(name_override="execute_read_intent", strict_mode=False)
                async def execute_read_intent(intent: QueryExecutionIntent) -> str:
                    """Validate and execute one bounded read intent.

                    The host admits it against the purpose-bound projection.
                    """
                    query_intents.append(intent)
                    sequence = len(query_intents)
                    if sequence > config.max_query_intents:
                        result = rejected_query_result(intent, "query-intent ceiling exhausted")
                    else:
                        result = await executor.execute(intent, projection)
                    query_results.append(result)
                    write_json(
                        run_root / f"queries/{sequence:03d}-intent.json",
                        intent.model_dump(mode="json"),
                    )
                    write_json(
                        run_root / f"queries/{sequence:03d}-result.json",
                        result.model_dump(mode="json"),
                    )
                    return result.model_dump_json()

                stage = perf_counter()
                planned = await harness.plan_queries(
                    run_root,
                    execute_tool=execute_read_intent,
                    max_turns=config.max_query_intents * 2 + 4,
                )
                planned_usage = dict(planned.usage)
                required_intent_ids = {
                    required_first_intent.intent_id,
                    *(intent.intent_id for intent in required_seed_intents),
                }

                def missing_required_successes() -> set[str]:
                    successful = {
                        result.intent_id
                        for result in query_results
                        if result.status == "succeeded"
                    }
                    return required_intent_ids - successful

                missing_required = missing_required_successes()
                if missing_required:
                    retry = await harness.plan_queries(
                        run_root,
                        execute_tool=execute_read_intent,
                        max_turns=config.max_query_intents * 2 + 4,
                        retry_reason=(
                            "required host-compiled intents did not all produce successful "
                            f"Neo4j results: {sorted(missing_required)}"
                        ),
                    )
                    for key, value in retry.usage.items():
                        planned_usage[key] = planned_usage.get(key, 0) + value
                    planned = retry
                missing_required = missing_required_successes()
                if missing_required:
                    raise RuntimeError(
                        "query planner did not successfully execute all required bounded intents "
                        f"after retry: {sorted(missing_required)}"
                    )
                timings["query_planner_ms"] = int((perf_counter() - stage) * 1000)
                evidence = planned.output
                if not isinstance(evidence, GraphReconciliationEvidence):
                    evidence = GraphReconciliationEvidence.model_validate(evidence)
                actual_references = tuple(
                    (intent.intent_id, result.result_id)
                    for intent, result in zip(query_intents, query_results, strict=True)
                )
                evidence_references = tuple(
                    (reference.intent_id, reference.result_id)
                    for reference in evidence.intent_result_references
                )
                if evidence_references != actual_references:
                    raise RuntimeError(
                        "query evidence references do not exactly match persisted intent/results"
                    )
                usage = dict(selection_outcome.usage)
                for key, value in planned_usage.items():
                    usage[key] = usage.get(key, 0) + value
            else:
                evidence = GraphReconciliationEvidence(
                    reconciliation_question="What report entities and relationships already exist?",
                    query_goals=(),
                    intent_result_references=(),
                    matched_existing_entities=(),
                    existing_relationships=(),
                    aliases_used=(),
                    match_method="offline_no_execution",
                    confidence="not_assessed",
                    unresolved_candidates=("Live graph execution disabled",),
                    schema_mismatches=(),
                    legacy_name_mappings=(
                        "OrganizationState -> OrganizationSnapshot",
                        "ProductState -> ProductSnapshot",
                    ),
                    query_failures=(),
                    stopping_rationale="Offline mode prohibits Neo4j reads.",
                )
                usage = selection_outcome.usage
            write_json(run_root / "queries/final-evidence.json", evidence.model_dump(mode="json"))
            selection_metrics = {
                **evaluate_selection(selection_outcome.draft),
                "review_decision": selection_outcome.review.decision,
                "revision_count": selection_outcome.revision_count,
                "invalid_name_count": len(selection_outcome.validation.errors),
                "unjustified_addition_count": len(
                    selection_outcome.review.unjustified_selections
                ),
            }
            query_metrics = evaluate_query_results(query_results)
            query_metrics.update(
                {
                    "intent_count": len(query_intents),
                    "kinds": {
                        kind: sum(intent.query_kind == kind for intent in query_intents)
                        for kind in projection.permitted_query_kinds
                    },
                    "stopping_reason": evidence.stopping_rationale,
                    "tool_call_count": len(query_intents),
                    "query_elapsed_ms": sum(result.elapsed_ms for result in query_results),
                }
            )
            metrics = {
                "workspace": workspace_metrics,
                "selection": selection_metrics,
                "query": query_metrics,
                "runtime": {**timings, **usage},
            }
            status = "offline" if config.offline else "completed"
            result = self._result(
                config=config,
                run_root=run_root,
                status=status,
                input_digests=base_inputs,
                schema_digest=schema_digest,
                catalog_digest=catalog.catalog_digest,
                usage=usage,
                timings=timings,
                metrics=metrics,
                warnings=warnings,
                selection_ref="selection/accepted.json",
                review_ref="selection/review.json",
                expanded_ref="selection/expanded-slice.json",
                projection_ref="selection/operation-projection.json",
                compatibility=decision,
                query_refs=tuple(
                    f"queries/{index:03d}-result.json" for index in range(1, len(query_results) + 1)
                ),
                evidence=evidence,
            )
            self._finish(run_root, result, metrics, started)
            return result
        finally:
            if driver is not None:
                await driver.close()
            harness.close()

    @staticmethod
    def _event(run_root: Path, event_type: str, payload: dict[str, Any]) -> None:
        path = run_root / "traces/events.ndjson"
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as stream:
            stream.write(
                json.dumps(
                    {
                        "event_type": event_type,
                        "occurred_at": datetime.now(UTC).isoformat(),
                        "payload": payload,
                    },
                    sort_keys=True,
                )
                + "\n"
            )

    @staticmethod
    def _result(
        *,
        config: ReconciliationRunConfig,
        run_root: Path,
        status: str,
        input_digests: dict[str, str],
        schema_digest: str,
        catalog_digest: str,
        usage: dict[str, int],
        timings: dict[str, int],
        metrics: dict[str, Any],
        warnings: list[str],
        selection_ref: str | None = None,
        review_ref: str | None = None,
        expanded_ref: str | None = None,
        projection_ref: str | None = None,
        compatibility: Any = None,
        query_refs: tuple[str, ...] = (),
        evidence: GraphReconciliationEvidence | None = None,
    ) -> ReportGraphReconciliationResult:
        return ReportGraphReconciliationResult(
            run_id=config.run_id,
            status=status,
            input_digests=input_digests,
            schema_digest=schema_digest,
            catalog_digest=catalog_digest,
            model=config.model,
            selection_ref=selection_ref,
            review_ref=review_ref,
            expanded_slice_ref=expanded_ref,
            projection_ref=projection_ref,
            compatibility_decision=compatibility,
            query_result_references=query_refs,
            reconciliation_evidence=evidence,
            usage=usage,
            timings=timings,
            evaluation_metrics=metrics,
            artifact_root=str(run_root),
            warnings=tuple(warnings),
        )

    def _finish(
        self,
        run_root: Path,
        result: ReportGraphReconciliationResult,
        metrics: dict[str, Any],
        started: float,
    ) -> None:
        metrics.setdefault("runtime", {})["total_elapsed_ms"] = int(
            (perf_counter() - started) * 1000
        )
        write_json(run_root / "metrics.json", metrics)
        write_json(run_root / "traces/usage.json", result.usage)
        write_json(run_root / "result.json", result.model_dump(mode="json"))
        evidence = result.reconciliation_evidence
        summary = [
            f"# Schema context selection run {result.run_id}",
            "",
            f"Status: **{result.status}**",
            f"Model: `{result.model}`",
            f"Schema digest: `{result.schema_digest}`",
            f"Catalog digest: `{result.catalog_digest}`",
            "",
            "## Outcome",
            "",
            (
                evidence.stopping_rationale
                if evidence is not None
                else "No live reconciliation was performed in this run mode."
            ),
            "",
            "## Limitations and warnings",
            "",
            *(f"- {warning}" for warning in result.warnings),
            "",
            "## Next tuning steps",
            "",
            "- Compare repeated baseline runs for semantic selection stability.",
            "- Compare full-report navigation with a generated selection brief.",
            "- Evaluate vector fallback only after the exact/full-text/topology baseline.",
        ]
        write_text(run_root / "summary.md", "\n".join(summary))
        self._event(run_root, "run.completed", {"status": result.status})
