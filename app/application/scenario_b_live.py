from __future__ import annotations

import asyncio
import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from pydantic import ValidationError

from app.application.capability_search import CapabilitySearchService
from app.application.control_plane_repository import BeanieDefinitionRepository
from app.application.external_candidate_inspection import (
    BeanieExternalCandidateInspectionRepository,
    ExternalCandidateInspectionRequest,
    ExternalCandidateInspectionService,
    InspectionBounds,
    InspectionPrincipal,
)
from app.application.external_candidate_repository import (
    BeanieExternalCandidateRepository,
    PersistedExternalCandidate,
)
from app.application.external_capability_discovery import (
    ExternalCapabilityDiscoveryService,
    ExternalDiscoveryBatch,
)
from app.application.postgres_capability_search_repository import (
    PostgresCatalogSearchRepository,
)
from app.config import PROJECT_ROOT, Settings
from app.domain.control_plane.canonical import sha256_digest
from app.domain.control_plane.contracts import DefinitionKind, ExactDefinitionRef
from app.domain.coordinator.contracts import (
    AuthorizationState,
    CapabilitySearchRequest,
    SelectionFacts,
)
from app.domain.coordinator.errors import CoordinatorDomainError
from app.domain.coordinator.policy import evaluate_selection, require_selectable
from app.integrations.capability_embeddings import OpenAICapabilityEmbeddingAdapter
from app.integrations.mcp_registry import HttpxMCPRegistryRunner, MCPRegistryAdapter
from app.integrations.mongodb import create_mongodb
from app.integrations.npx_skills_discovery import (
    AsyncioSkillDiscoverySubprocessRunner,
    NpxSkillsDiscoveryAdapter,
)
from app.integrations.postgres import create_postgres_pool
from app.integrations.quarantine_inspection import (
    AsyncioQuarantineSubprocessRunner,
    SanitizedCandidateMetadataPayloadProvider,
    StaticQuarantineInspectionRunner,
)

MISSING_CAPABILITY = "external.experimental.workspace-file-analysis"
INTERNAL_GAP_QUERY = (
    "a governed workspace file analysis capability that is not yet reviewed "
    "or promoted into the internal catalog"
)
MCP_DISCOVERY_QUERY = "filesystem"
SKILL_DISCOVERY_QUERY = "workspace file analysis"


async def run_scenario_b_live(
    *,
    tenant_scope: str,
    artifact_path: Path,
    workspace_root: Path,
    external_limit: int,
) -> dict[str, Any]:
    settings, python_executable, npx_executable = _live_settings()
    mongo_client, _ = await create_mongodb(settings)
    postgres_pool = await create_postgres_pool(settings)
    try:
        candidates = BeanieExternalCandidateRepository()
        internal_gap = await _internal_gap(settings, postgres_pool, tenant_scope)
        discovery = ExternalCapabilityDiscoveryService(
            enabled=True,
            mcp_registry=MCPRegistryAdapter(
                HttpxMCPRegistryRunner(),
                base_url=settings.mcp_registry_base_url,
                api_version=settings.mcp_registry_api_version,
                timeout_seconds=settings.external_discovery_request_timeout_seconds,
                max_response_bytes=settings.external_discovery_max_output_bytes,
                max_pages=settings.external_discovery_max_pages,
                max_retries=settings.external_discovery_max_retries,
            ),
            skills=NpxSkillsDiscoveryAdapter(
                AsyncioSkillDiscoverySubprocessRunner(),
                executable=str(npx_executable),
                package_version=settings.npx_skills_package_version,
                timeout_seconds=settings.external_discovery_command_timeout_seconds,
                max_output_bytes=settings.external_discovery_max_output_bytes,
                working_directory_root=workspace_root / "npx",
            ),
            candidates=candidates,
            max_results=settings.external_discovery_max_results,
        )
        mcp_batch, skill_batch = await asyncio.gather(
            discovery.discover_mcp_servers(
                MCP_DISCOVERY_QUERY,
                limit=external_limit,
            ),
            discovery.discover_agent_skills(
                SKILL_DISCOVERY_QUERY,
                limit=external_limit,
            ),
        )
        discovered = (*skill_batch.candidates, *mcp_batch.candidates)
        if not discovered:
            raise RuntimeError("Scenario B external discovery returned no inspectable candidates")
        persisted = tuple(
            [await candidates.get_candidate(candidate.candidate_id) for candidate in discovered]
        )
        selected = persisted[0]
        refusal = _direct_attachment_refusal(selected)
        inspection_records = BeanieExternalCandidateInspectionRepository()
        runner = StaticQuarantineInspectionRunner(
            payloads=SanitizedCandidateMetadataPayloadProvider(),
            process_runner=AsyncioQuarantineSubprocessRunner(),
            python_executable=python_executable,
            scanner_script=PROJECT_ROOT / "scripts" / "quarantine_static_scan.py",
            workspace_root=workspace_root / "inspection",
        )
        inspection = ExternalCandidateInspectionService(
            candidates=candidates,
            runner=runner,
            records=inspection_records,
            bounds=InspectionBounds(
                timeout_seconds=min(
                    settings.external_discovery_command_timeout_seconds,
                    30,
                ),
                workspace_ttl_seconds=300,
                max_download_bytes=1_000_000,
                max_files=100,
                max_file_bytes=250_000,
                max_network_requests=0,
                max_network_destinations=1,
                max_findings=50,
                max_report_bytes=100_000,
            ),
            service_identity="scenario-b-quarantine-inspector",
            allow_mcp_tools_list_probe=False,
        )
        report = await inspection.inspect(
            InspectionPrincipal(
                actor_id="scenario-b-live-coordinator",
                tenant_scope=tenant_scope,
                roles=frozenset({"coordinator_planner"}),
            ),
            ExternalCandidateInspectionRequest(
                candidate_id=selected.candidate.candidate_id,
                correlation_id=f"scenario-b:{uuid4()}",
                requested_at=datetime.now(UTC),
                requested_capabilities=frozenset({MISSING_CAPABILITY}),
            ),
        )
        workspace = await inspection_records.get_workspace(report.workspace_id)
        result = {
            "scenario": "B",
            "completed_at": datetime.now(UTC).isoformat(),
            "internal_catalog_gap": internal_gap,
            "external_discovery": {
                "mcp_registry": await _batch_evidence(
                    mcp_batch,
                    candidates,
                ),
                "npx_skills": await _batch_evidence(
                    skill_batch,
                    candidates,
                ),
                "npx_package_version": settings.npx_skills_package_version,
            },
            "direct_attachment_refusal": refusal,
            "inspection": {
                "candidate_id": report.candidate_id,
                "candidate_record_id": report.candidate_record_id,
                "evidence_id": report.evidence_id,
                "workspace_id": report.workspace_id,
                "workspace_ref": workspace.root_ref,
                "workspace_digest": workspace.content_digest,
                "inspection_id": report.inspection_id,
                "inspection_report_ref": f"inspection-report:{report.inspection_id}",
                "inspection_report_digest": report.report_digest,
                "status": report.status.value,
                "finding_codes": [finding.code for finding in report.findings],
                "promotion_request": report.promotion_request.model_dump(mode="json"),
            },
            "safety_proof": {
                "installs_performed": 0,
                "candidate_executions": 0,
                "candidate_bundle_mounted": workspace.candidate_bundle_mounted,
                "agent_environment_mounted": workspace.agent_environment_mounted,
                "inputs_read_only": workspace.inputs_read_only,
                "network_requests": 0,
                "raw_evidence_inline": False,
                "temporary_workspaces_remaining": _temporary_entry_count(workspace_root),
            },
        }
        await asyncio.to_thread(
            artifact_path.parent.mkdir,
            parents=True,
            exist_ok=True,
        )
        await asyncio.to_thread(
            artifact_path.write_text,
            json.dumps(result, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        return {
            **result,
            "artifact_path": str(artifact_path),
        }
    finally:
        await postgres_pool.close()
        await mongo_client.close()


async def _internal_gap(
    settings: Settings,
    postgres_pool: Any,
    tenant_scope: str,
) -> dict[str, object]:
    request = CapabilitySearchRequest(
        query=INTERNAL_GAP_QUERY,
        kinds=frozenset(
            {
                DefinitionKind.MCP_SERVER,
                DefinitionKind.MCP_TOOL,
                DefinitionKind.SKILL,
                DefinitionKind.AGENT_PROFILE,
            }
        ),
        tenant_scope=tenant_scope,
        required_capabilities=frozenset({MISSING_CAPABILITY}),
        limit=20,
    )
    response = await CapabilitySearchService(
        search=PostgresCatalogSearchRepository(postgres_pool),
        definitions=BeanieDefinitionRepository(),
        embeddings=OpenAICapabilityEmbeddingAdapter(settings),
        embedding_model_id=settings.capability_embedding_model,
        embedding_dimensions=settings.capability_embedding_dimensions,
    ).search(request)
    selectable = tuple(
        hit
        for hit in response.hits
        if hit.authorization_state == AuthorizationState.SELECTABLE and hit.exact_ref is not None
    )
    if selectable:
        raise RuntimeError(
            "Scenario B fixture is not an internal gap; a selectable capability exists"
        )
    return {
        "query": request.query,
        "required_capability": MISSING_CAPABILITY,
        "selectable_hit_count": 0,
        "returned_hit_count": len(response.hits),
        "authorization_states": sorted({hit.authorization_state.value for hit in response.hits}),
        "token_use": [item.model_dump(mode="json") for item in response.token_use],
    }


def _direct_attachment_refusal(
    persisted: PersistedExternalCandidate,
) -> dict[str, object]:
    decision = evaluate_selection(SelectionFacts(candidate_id=persisted.candidate.candidate_id))
    try:
        require_selectable(decision)
    except CoordinatorDomainError as error:
        policy_error = error.envelope().model_dump(mode="json")
    else:
        raise AssertionError("external candidate unexpectedly became selectable")
    try:
        ExactDefinitionRef.model_validate(persisted.candidate.model_dump(mode="json"))
    except ValidationError as error:
        type_refusal = {
            "accepted_as_exact_definition_ref": False,
            "validation_error_types": sorted({str(item["type"]) for item in error.errors()}),
        }
    else:
        raise AssertionError(
            "external candidate unexpectedly entered the exact definition ref boundary"
        )
    return {
        "authorization_state": decision.authorization_state.value,
        "policy_error": policy_error,
        "erc_exact_ref_boundary": type_refusal,
        "promoted_ref": persisted.candidate.promoted_ref,
        "included_in_erc": False,
        "included_in_launch_ticket": False,
    }


async def _batch_evidence(
    batch: ExternalDiscoveryBatch,
    repository: BeanieExternalCandidateRepository,
) -> dict[str, object]:
    records = tuple(
        [await repository.get_candidate(candidate.candidate_id) for candidate in batch.candidates]
    )
    evidence_ids = sorted({f"discovery-evidence:{sha256_digest(item)}" for item in batch.evidence})
    evidence = tuple([await repository.get_evidence(identity) for identity in evidence_ids])
    return {
        "source": batch.source.value,
        "candidate_count": len(records),
        "candidates": [_candidate_ref(record) for record in records],
        "evidence": [
            {
                "evidence_id": item.evidence_id,
                "record_digest": item.record_digest,
                "source_version": item.evidence.source_version,
                "query": item.evidence.query,
                "retrieved_at": item.evidence.retrieved_at.isoformat(),
                "raw_response_digest": item.evidence.raw_response_digest,
                "raw_response_size_bytes": item.evidence.raw_response_size_bytes,
            }
            for item in evidence
        ],
    }


def _candidate_ref(record: PersistedExternalCandidate) -> dict[str, object]:
    candidate = record.candidate
    return {
        "candidate_id": candidate.candidate_id,
        "candidate_record_id": record.candidate_record_id,
        "content_digest": record.content_digest,
        "evidence_id": record.evidence_id,
        "raw_response_ref": candidate.raw_response_ref,
        "source": candidate.source.value,
        "upstream_identity": candidate.upstream_identity,
        "upstream_version": candidate.upstream_version,
        "trust_tier": candidate.trust_tier,
        "inspection_status": candidate.inspection_status,
        "promoted_ref": candidate.promoted_ref,
    }


def _live_settings() -> tuple[Settings, Path, Path]:
    python_executable = Path(sys.executable).resolve(strict=True)
    node = (Path(sys.base_prefix).resolve().parent / "node" / "bin" / "node.exe").resolve(
        strict=True
    )
    npx = (PROJECT_ROOT.parent / ".tools" / "node_modules" / ".bin" / "npx.CMD").resolve(
        strict=True
    )
    node_bin = str(node.parent)
    current_path = os.environ.get("PATH", "")
    if node_bin.casefold() not in {
        part.casefold() for part in current_path.split(os.pathsep) if part
    }:
        os.environ["PATH"] = node_bin + os.pathsep + current_path
    return (
        Settings().model_copy(
            update={
                "npx_skills_executable": str(npx),
                "external_discovery_request_timeout_seconds": 30.0,
                "external_discovery_command_timeout_seconds": 60.0,
                "external_discovery_max_retries": 4,
            }
        ),
        python_executable,
        npx,
    )


def _temporary_entry_count(workspace_root: Path) -> int:
    transient_prefixes = ("inspection-", "belllabs-skills-discovery-")
    return sum(
        1
        for item in workspace_root.rglob("*")
        if any(item.name.startswith(prefix) for prefix in transient_prefixes)
    )


__all__ = [
    "INTERNAL_GAP_QUERY",
    "MCP_DISCOVERY_QUERY",
    "MISSING_CAPABILITY",
    "SKILL_DISCOVERY_QUERY",
    "run_scenario_b_live",
]
