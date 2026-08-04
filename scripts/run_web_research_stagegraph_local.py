from __future__ import annotations

import argparse
import asyncio
import json
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any

from app.application.control_plane_repository import BeanieDefinitionRepository
from app.application.mongo_operation_execution_repository import (
    MongoOperationBindingRepository,
)
from app.application.orchestration_binding_repository import (
    InMemoryRunSemanticInputBindingRepository,
)
from app.application.web_research_semantic_binding import (
    verify_web_research_operation_bindings,
)
from app.application.web_research_semantic_handlers import (
    resolve_web_research_run_authority,
)
from app.config import PROJECT_ROOT, Settings
from app.domain.control_plane.canonical import sha256_digest
from app.domain.control_plane.contracts import DefinitionKind, ExactDefinitionRef
from app.domain.coordinator.contracts import (
    AuthorizationState,
    CapabilitySearchHit,
    CapabilitySearchRequest,
)
from app.domain.coordinator.web_research_runtime import (
    ExactOperationExecutionBinding,
    OperationExecutionBindingAuthority,
    WebResearchGoal,
)
from app.domain.orchestration.contracts import (
    LifecycleCommandOutcome,
    LifecycleCommandRequest,
    StageGraphRunInput,
)
from app.integrations.catalog_projection_admin import list_published_definition_refs
from app.integrations.mongodb import create_mongodb
from app.integrations.temporal import create_temporal_client
from app.integrations.web_research_runtime import (
    BrowserScreenshotArtifactPort,
    attest_reviewed_web_research_runtime,
    build_live_web_research_handler_dependencies,
)
from app.temporal.web_research_smoke import run_web_research_stagegraph_smoke


class LocalArtifactScreenshotStore(BrowserScreenshotArtifactPort):
    """Bounded local smoke artifact store; paths remain below the selected root."""

    def __init__(self, root: Path) -> None:
        self._root = root
        self.paths: dict[str, str] = {}

    async def store(
        self,
        *,
        request_scope: str,
        run_id: str,
        idempotency_key: str,
        source_url: str,
        content: bytes,
        media_type: str,
    ) -> str:
        if media_type != "image/png":
            raise ValueError("local browser evidence must be PNG")
        digest = sha256(content).hexdigest()
        run_root = self._root / _safe_segment(run_id)
        await asyncio.to_thread(run_root.mkdir, parents=True, exist_ok=True)
        path = run_root / f"{digest}.png"
        if not path.exists():
            await asyncio.to_thread(path.write_bytes, content)
        ref = f"belllabs://local-web-research-artifacts/{_safe_segment(run_id)}/{digest}.png"
        self.paths[ref] = str(path)
        return ref


class LocalSmokeLifecycle:
    """Explicit non-production lifecycle for a pre-admitted localhost smoke run."""

    async def execute(
        self,
        request: LifecycleCommandRequest,
    ) -> LifecycleCommandOutcome:
        terminal = request.action.get("kind") == "terminalize"
        evidence_digest = sha256_digest(
            {
                "command_id": request.command_id,
                "evidence_refs": request.evidence_refs,
            }
        )
        return LifecycleCommandOutcome(
            accepted=True,
            resulting_run_version=request.expected_run_version + 1,
            phase="terminal" if terminal else "active",
            reason_code="local_smoke_pre_admitted",
            evidence_frontier_digest=evidence_digest,
            obligation_revision="local-smoke:1",
            accepted_obligation_evidence_digest=evidence_digest,
            required_obligations_accepted=True,
        )


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run the exact Scenario D StageGraph against localhost Temporal using "
            "coordinator retrieval evidence and a pre-existing OperationExecutionBinding."
        )
    )
    parser.add_argument("--goal", required=True)
    parser.add_argument("--selection-evidence", required=True, type=Path)
    parser.add_argument("--search-firecrawl-binding-id", required=True)
    parser.add_argument("--search-tavily-binding-id", required=True)
    parser.add_argument("--browser-verify-binding-id", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--request-scope", required=True)
    parser.add_argument("--effective-configuration-digest", required=True)
    parser.add_argument("--node", required=True, type=Path)
    parser.add_argument("--artifact-dir", required=True, type=Path)
    parser.add_argument("--temporal-address", default="localhost:7233")
    parser.add_argument(
        "--task-queue",
        default="biotech-research-ingestion-web-research-smoke",
    )
    parser.add_argument("--workflow-id")
    parser.add_argument("--maximum-results", type=int, default=5)
    parser.add_argument("--browser-verification-limit", type=int, default=3)
    return parser.parse_args()


async def _run(args: argparse.Namespace) -> dict[str, Any]:
    artifact_root = _checked_artifact_root(args.artifact_dir)
    evidence = json.loads(args.selection_evidence.read_text(encoding="utf-8"))
    CapabilitySearchRequest.model_validate(evidence["retrieval_request"])
    retrieval_hits = tuple(
        CapabilitySearchHit.model_validate(item) for item in evidence["selected_hits"]
    )
    selected_refs = tuple(hit.exact_ref for hit in retrieval_hits if hit.exact_ref is not None)
    settings = Settings().model_copy(
        update={
            "temporal_address": args.temporal_address,
            "web_research_agent_browser_node": args.node.resolve(strict=True),
        }
    )
    mongo_client, _ = await create_mongodb(settings)
    try:
        definitions = BeanieDefinitionRepository()
        refs = await list_published_definition_refs()
        records = tuple([await definitions.get(ref) for ref in refs])
        runtime_ref = _current_ref(
            records,
            DefinitionKind.RUNTIME_PROFILE,
            "web-research-browser-verification-runtime-v1",
        )
        workspace_ref = _current_ref(
            records,
            DefinitionKind.WORKSPACE_TEMPLATE,
            "web-research-browser-verification-workspace-v1",
        )
        artifacts = attest_reviewed_web_research_runtime(settings)
        goal = WebResearchGoal(question=args.goal)
        if any(
            hit.exact_ref is None
            or hit.candidate_id is not None
            or hit.authorization_state != AuthorizationState.SELECTABLE
            for hit in retrieval_hits
        ):
            raise RuntimeError(
                "selection evidence must contain only selectable internal catalog hits"
            )
        operation_repository = MongoOperationBindingRepository()
        operation_binding_ids = {
            "search_firecrawl": args.search_firecrawl_binding_id,
            "search_tavily": args.search_tavily_binding_id,
            "browser_verify": args.browser_verify_binding_id,
        }
        operation_bindings = {
            stage_id: await operation_repository.get_binding_by_id(binding_id)
            for stage_id, binding_id in operation_binding_ids.items()
        }
        if any(binding is None for binding in operation_bindings.values()):
            raise RuntimeError("one or more pre-existing Scenario D OEBs were not found")
        exact_operation_bindings = {
            stage_id: binding
            for stage_id, binding in operation_bindings.items()
            if binding is not None
        }
        mcp_servers, skills, browser_grant = verify_web_research_operation_bindings(
            exact_operation_bindings,
            selected_refs=selected_refs,
            runtime_profile_ref=runtime_ref,
            workspace_template_ref=workspace_ref,
            effective_configuration_digest=args.effective_configuration_digest,
            request_scope=args.request_scope,
            run_id=args.run_id,
            catalog={(item.ref.kind, item.ref.logical_id): item for item in records},
        )
        authority = resolve_web_research_run_authority(
            catalog_records=records,
            request_scope=args.request_scope,
            run_id=args.run_id,
            goal=goal,
            effective_configuration_digest=args.effective_configuration_digest,
            created_at=datetime.now(UTC),
            firecrawl_runtime=artifacts.firecrawl,
            tavily_runtime=artifacts.tavily,
            browser_runtime=artifacts.browser,
            selected_capability_refs=selected_refs,
            mcp_servers=mcp_servers,
            skills=skills,
            browser_grant=browser_grant,
            operation_execution=OperationExecutionBindingAuthority(
                bindings={
                    stage_id: ExactOperationExecutionBinding(
                        binding_id=binding.binding_id,
                        binding_digest=sha256_digest(binding),
                    )
                    for stage_id, binding in exact_operation_bindings.items()
                },
                effective_configuration_digest=args.effective_configuration_digest,
            ),
            maximum_results=args.maximum_results,
            browser_verification_limit=args.browser_verification_limit,
        )
        screenshots = LocalArtifactScreenshotStore(artifact_root)
        dependencies = build_live_web_research_handler_dependencies(
            settings=settings,
            firecrawl_tool_ref=authority.firecrawl_tool_ref,
            tavily_tool_ref=authority.tavily_tool_ref,
            screenshot_artifacts=screenshots,
        )
        bindings = InMemoryRunSemanticInputBindingRepository()
        client = await create_temporal_client(settings)
        workflow_id = args.workflow_id or f"web-research-smoke-{args.run_id}"
        result = await run_web_research_stagegraph_smoke(
            client,
            task_queue=args.task_queue,
            workflow_id=workflow_id,
            run_input=StageGraphRunInput(
                run_id=args.run_id,
                request_scope=args.request_scope,
                effective_configuration_digest=args.effective_configuration_digest,
                blueprint_digest=authority.blueprint_ref.digest,
                blueprint=authority.blueprint.model_dump(mode="json"),
                max_concurrency=2,
                task_timeout_seconds=30,
                semantic_input_binding_ref=authority.semantic_binding.binding_id,
                correlation_id=f"local-smoke:{args.run_id}",
            ),
            semantic_binding=authority.semantic_binding,
            bindings=bindings,
            lifecycle=LocalSmokeLifecycle(),  # type: ignore[arg-type]
            dependencies=dependencies,
            operation_bindings=operation_repository,
        )
        return {
            "mode": "local-smoke-pre-admitted",
            "workflow_id": result.workflow_id,
            "temporal_run_id": result.temporal_run_id,
            "run_id": result.run_result.run_id,
            "operation_execution_bindings": {
                stage_id: {
                    "binding_id": binding.binding_id,
                    "binding_digest": sha256_digest(binding),
                }
                for stage_id, binding in exact_operation_bindings.items()
            },
            "semantic_binding_id": authority.semantic_binding.binding_id,
            "final_result_ref": result.final_result_ref,
            "exact_evidence_refs": result.exact_evidence_refs,
            "output_refs": result.run_result.output_refs,
            "local_artifacts": screenshots.paths,
        }
    finally:
        await mongo_client.close()


def _current_ref(
    records: tuple[Any, ...],
    kind: DefinitionKind,
    logical_id: str,
) -> ExactDefinitionRef:
    matches = [
        item
        for item in records
        if item.ref.kind == kind and item.ref.logical_id == logical_id and item.retired_at is None
    ]
    if not matches:
        raise RuntimeError(f"published definition is unavailable: {kind.value}:{logical_id}")
    return max(matches, key=lambda item: item.ref.revision).ref


def _checked_artifact_root(path: Path) -> Path:
    resolved = path.resolve()
    project = PROJECT_ROOT.resolve()
    if not resolved.is_relative_to(project):
        raise ValueError("--artifact-dir must stay inside the project directory")
    resolved.mkdir(parents=True, exist_ok=True)
    return resolved


def _safe_segment(value: str) -> str:
    normalized = "".join(
        character if character.isalnum() or character in "-_" else "-" for character in value
    ).strip("-")
    if not normalized:
        raise ValueError("artifact path identity is empty after normalization")
    return normalized[:120]


if __name__ == "__main__":
    print(json.dumps(asyncio.run(_run(_arguments())), indent=2, sort_keys=True))
