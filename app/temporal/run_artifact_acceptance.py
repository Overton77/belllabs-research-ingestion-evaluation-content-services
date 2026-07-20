from __future__ import annotations

import asyncio
import os
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4

import docker
import httpx
from temporalio.contrib.openai_agents import OpenAIAgentsPlugin

from app.api.control_plane import (
    ControlPlanePrincipal,
    get_control_plane_principal,
)
from app.api.run_control import (
    get_generic_artifact_submitter,
    get_run_control_service,
)
from app.application.artifact_promotion import (
    ArtifactPayloadAddress,
    ArtifactPromotionService,
    StaticArtifactValidationAuthority,
)
from app.application.mongo_artifact_repository import (
    MongoArtifactMetadataRepository,
)
from app.application.mongo_operation_execution_repository import (
    MongoOperationBindingRepository,
)
from app.application.mongo_workspace_repository import (
    MongoWorkspaceManifestRepository,
)
from app.application.operation_execution import (
    OperationExecutionService,
    RunControlOperationBudgetAuthority,
)
from app.application.postgres_artifact_repository import (
    PostgresArtifactDurableReferenceRepository,
)
from app.application.postgres_run_control_repository import (
    PostgresRunControlRepository,
)
from app.application.run_control import AdmissionPolicyRegistry, RunControlService
from app.application.workspace_candidates import (
    FilesystemWorkspaceCandidateContents,
    WorkspaceCandidateCaptureService,
)
from app.application.workspace_materialization import (
    BindingWorkspaceMaterializer,
    InMemoryDurableWorkspaceInputs,
    WorkspaceMaterializationService,
)
from app.config import get_settings
from app.domain.control_plane.canonical import sha256_digest
from app.domain.control_plane.contracts import (
    DefinitionKind,
    ExactDefinitionRef,
    RunInputManifestRef,
    SecretRef,
)
from app.domain.operation_execution.contracts import (
    ArtifactCheckEvidence,
    ArtifactPromotionPlan,
    CapabilityGrant,
    GenericArtifactWorkflowRequest,
    ImmutableAssetBinding,
    ModelPolicy,
    OperationAttemptIdentity,
    OperationExecutionRequest,
    PromptSegment,
    PromptTrustClass,
    ToolBinding,
    WorkspaceContract,
    WorkspaceOwner,
    WorkspaceOwnerKind,
    WorkspaceSlotBinding,
)
from app.domain.run_control.contracts import (
    ActorContext,
    BudgetApplicability,
    BudgetDimensionLimit,
    BudgetEnvelope,
    LifecycleCommand,
    ReserveBudgetAction,
    RunRequest,
    StartAction,
    VerifiedRunConfiguration,
)
from app.integrations.artifact_payloads import S3ArtifactPayloadStore
from app.integrations.conformance_operation_runtime import (
    ConformanceAssetVerifier,
    ConformanceAuthority,
    ConformanceEventSink,
    ConformanceSecretResolver,
)
from app.integrations.filesystem_workspace import FilesystemWorkspaceProvisioner
from app.integrations.mongodb import create_mongodb
from app.integrations.openai_agents_runtime import OpenAIAgentsSandboxRuntime
from app.integrations.postgres import (
    apply_application_migrations,
    create_application_migration_pool,
    create_application_postgres_pool,
)
from app.integrations.temporal import create_temporal_client
from app.integrations.temporal_operation_submission import (
    TemporalGenericArtifactSubmitter,
)
from app.server import api
from app.temporal.agentic_probe_assets import (
    TAVILY_BEST_PRACTICES_SKILL,
    VERCEL_AGENT_BROWSER_SKILL,
)
from app.temporal.artifact_activities import (
    ArtifactPromotionActivities,
    create_generic_artifact_worker,
)
from app.temporal.operation_activities import OperationExecutionActivities

EXPECTED = "BELL-LABS-ARTIFACT-PROMOTION-OK"
PROBE_IMAGE = "belllabs-agentic-probe:local"
REPORT_PATH = "/workspace/output/senescence-research.md"
CREATE_AND_PATCH_SKILL = b"""\
---
name: create-and-patch-report
description: Create and then revise the declared research report.
---
Research cellular senescence using `tvly search`. Use `agent-browser` to open one
public source returned by the search. Save brief raw evidence under
`/workspace/output/evidence.txt` with shell commands. Then use the sandbox-native
`apply_patch` tool to create `/workspace/output/senescence-research.md` containing
the marker DRAFT_CREATED. Use `apply_patch` a second time to replace that marker
with PATCH_CONFIRMED and add source URLs plus a concise synthesis. Never write
credentials. The final response must include BELL-LABS-ARTIFACT-PROMOTION-OK.
"""


class LiveConfigurationVerifier:
    async def verify(self, request: RunRequest) -> VerifiedRunConfiguration:
        return VerifiedRunConfiguration(
            effective_configuration_digest=request.effective_configuration_digest,
            workflow_type_ref=request.workflow_type_ref,
            input_manifest=request.input_manifest,
            effective_budget_ceilings={
                "tokens.input": 100_000,
                "tokens.output": 20_000,
                "tokens.total": 120_000,
                "model.turns": 8,
            },
            max_concurrency=1,
            input_admission_contract="contract:live-artifact-input@1",
            invariant_refs=frozenset({"contract:live-artifact-invariant@1"}),
            obligation_revision="live-artifact-obligations:1",
        )


def _digest_bytes(value: bytes) -> str:
    return f"sha256:{sha256(value).hexdigest()}"


def _ref(kind: DefinitionKind, logical_id: str, digest: str) -> ExactDefinitionRef:
    return ExactDefinitionRef(
        kind=kind,
        logical_id=logical_id,
        revision=1,
        digest=digest,
    )


def _actor(*permissions: str) -> ActorContext:
    return ActorContext(
        actor_id="artifact-acceptance-operator",
        authority_refs=frozenset({"authority:artifact-acceptance"}),
        permissions=frozenset(permissions),
    )


def _run_request(configuration_digest: str, request_id: str) -> RunRequest:
    workflow_digest = sha256_digest("generic-artifact-workflow@1")
    manifest_digest = sha256_digest("generic-artifact-input@1")
    dimensions = (
        BudgetDimensionLimit(
            dimension="tokens.input",
            applicability=BudgetApplicability.BOUNDED,
            hard_cap=100_000,
        ),
        BudgetDimensionLimit(
            dimension="tokens.output",
            applicability=BudgetApplicability.BOUNDED,
            hard_cap=20_000,
        ),
        BudgetDimensionLimit(
            dimension="tokens.total",
            applicability=BudgetApplicability.BOUNDED,
            hard_cap=120_000,
        ),
        BudgetDimensionLimit(
            dimension="model.turns",
            applicability=BudgetApplicability.BOUNDED,
            hard_cap=8,
        ),
    )
    return RunRequest(
        request_scope="local-artifact-acceptance",
        idempotency_issuer="artifact-acceptance-operator",
        request_id=request_id,
        actor=_actor(),
        effective_configuration_digest=configuration_digest,
        workflow_type_ref=_ref(
            DefinitionKind.WORKFLOW_TYPE,
            "generic.artifact-acceptance",
            workflow_digest,
        ),
        input_manifest=RunInputManifestRef(
            manifest_id="generic-artifact-acceptance-input",
            revision=1,
            digest=manifest_digest,
        ),
        budget_envelope=BudgetEnvelope(dimensions=dimensions),
        requested_at=datetime.now(UTC),
        correlation_id=f"artifact-acceptance:{request_id}",
        sponsorship_ref="sponsorship:artifact-acceptance",
        approval_refs=("approval:artifact-acceptance",),
        delegation_authority_refs=frozenset({"authority:artifact-acceptance"}),
    )


def _command(
    run_id: str,
    version: int,
    command_id: str,
    action: StartAction | ReserveBudgetAction,
    permission: str,
) -> LifecycleCommand:
    return LifecycleCommand(
        command_id=command_id,
        idempotency_issuer="artifact-acceptance-operator",
        request_scope="local-artifact-acceptance",
        run_id=run_id,
        expected_run_version=version,
        actor=_actor(permission),
        action=action,
        reason="live artifact acceptance",
        occurred_at=datetime.now(UTC),
        correlation_id=f"artifact-acceptance:{run_id}",
    )


def _operation(
    *,
    run_id: str,
    run_revision: int,
    configuration_digest: str,
    image_digest: str,
    workspace_contract_digest: str,
    owner: WorkspaceOwner,
    skill_digests: tuple[str, str, str],
) -> OperationExecutionRequest:
    create_digest, tavily_digest, browser_digest = skill_digests
    slot = WorkspaceSlotBinding(
        slot_name="report",
        logical_path="/workspace/output",
        access="exclusive_write",
        owner=owner,
    )
    instruction = (
        "Use every bound skill. Perform the declared web research in the Docker sandbox, "
        "create the report, and patch the same report afterward. The report is a local "
        "candidate until the workflow explicitly promotes it."
    )
    task = "Complete the create-and-patch report skill exactly."
    return OperationExecutionRequest(
        identity=OperationAttemptIdentity(
            run_id=run_id,
            operation_id="generic-senescence-research",
            operation_attempt=1,
        ),
        request_scope="local-artifact-acceptance",
        effective_configuration_digest=configuration_digest,
        run_control_revision=run_revision,
        operation_contract_ref="operation:generic-artifact-research@1",
        prompt_segments=(
            PromptSegment(
                source_ref="prompt:artifact-acceptance-system@1",
                source_revision=1,
                trust_class=PromptTrustClass.SYSTEM_AUTHORITY,
                content=instruction,
                rendered_digest=sha256_digest(instruction),
            ),
            PromptSegment(
                source_ref="input:artifact-acceptance-task@1",
                source_revision=1,
                trust_class=PromptTrustClass.ADMITTED_INPUT,
                content=task,
                rendered_digest=sha256_digest(task),
            ),
        ),
        model_policy=ModelPolicy(
            provider="openai",
            model="gpt-5-mini",
            reasoning_effort="minimal",
            verbosity="low",
            max_turns=8,
        ),
        tools=(
            ToolBinding(
                tool_id="sandbox.filesystem",
                revision=1,
                schema_digest=sha256_digest("agents-sandbox:filesystem@0.17.8"),
                approval_policy="never",
            ),
            ToolBinding(
                tool_id="sandbox.shell",
                revision=1,
                schema_digest=sha256_digest("agents-sandbox:shell@0.17.8"),
                approval_policy="never",
            ),
        ),
        skills=(
            ImmutableAssetBinding(
                ref=_ref(
                    DefinitionKind.SKILL,
                    "acceptance.create-and-patch",
                    create_digest,
                ),
                manifest_digest=create_digest,
                mount_path="/skills/create-and-patch/SKILL.md",
            ),
            ImmutableAssetBinding(
                ref=_ref(
                    DefinitionKind.SKILL,
                    "tavily.best-practices",
                    tavily_digest,
                ),
                manifest_digest=tavily_digest,
                mount_path="/skills/tavily-best-practices/SKILL.md",
            ),
            ImmutableAssetBinding(
                ref=_ref(
                    DefinitionKind.SKILL,
                    "vercel.agent-browser",
                    browser_digest,
                ),
                manifest_digest=browser_digest,
                mount_path="/skills/agent-browser/SKILL.md",
            ),
        ),
        agent_profile_ref=_ref(
            DefinitionKind.AGENT_PROFILE,
            "acceptance.sandbox-agent",
            sha256_digest("acceptance.sandbox-agent@1"),
        ),
        capability_grant=CapabilityGrant(
            capabilities=frozenset(
                {
                    "model.invoke",
                    "sandbox.filesystem",
                    "sandbox.shell",
                    "skill.read",
                    "artifact.promote",
                }
            ),
            tool_ids=frozenset({"sandbox.filesystem", "sandbox.shell"}),
            data_scope_refs=frozenset(
                {
                    "permission:public-web-research@1",
                    "check:sha256-after-capture",
                }
            ),
            network_hosts=frozenset(
                {
                    "api.tavily.com",
                    "www.nia.nih.gov",
                    "nia.nih.gov",
                    "www.nature.com",
                    "nature.com",
                }
            ),
        ),
        workspace=WorkspaceContract(
            namespace_id=f"workspace-namespace:{run_id}",
            workspace_id=f"workspace:{run_id}:research",
            provider="docker-sandbox",
            template_ref=_ref(
                DefinitionKind.WORKSPACE_TEMPLATE,
                "acceptance.generic-workspace",
                sha256_digest("acceptance.generic-workspace@1"),
            ),
            workflow_contract_digest=workspace_contract_digest,
            slot_bindings=(slot,),
            exclusive_write_paths=(slot.logical_path,),
            network_policy="allowlisted",
            runtime_digest=sha256_digest("agents-sandbox-runtime@0.17.8"),
            image_digest=image_digest,
            package_digest=sha256_digest("acceptance-probe-packages@1"),
            environment_digest=sha256_digest("acceptance-probe-environment@1"),
        ),
        secret_refs=(
            SecretRef(provider="environment", key="OPENAI_API_KEY"),
            SecretRef(provider="environment", key="TAVILY_API_KEY"),
        ),
        budget_reservation_id="artifact-acceptance-operation",
        budget_limits={
            "tokens.input": 100_000,
            "tokens.output": 20_000,
            "tokens.total": 120_000,
            "model.turns": 8,
        },
        tracing_policy_ref="tracing:acceptance-no-sensitive@1",
        sensitive_data_policy_ref="sensitive:acceptance-redact@1",
        snapshot_policy_ref="snapshot:acceptance-none@1",
        requested_at=datetime.now(UTC),
        idempotency_key=f"artifact-acceptance-operation:{run_id}",
    )


async def main() -> None:
    settings = get_settings()
    if not settings.s3_bucket:
        raise RuntimeError("S3_BUCKET is required for live artifact acceptance")
    tavily_key = os.environ.get("TAVILY_API_KEY")
    if not tavily_key:
        raise RuntimeError("TAVILY_API_KEY is required for live artifact acceptance")
    image = docker.from_env().images.get(PROBE_IMAGE)
    configuration_digest = sha256_digest("live-generic-artifact-configuration@1")
    workspace_contract_digest = sha256_digest("live-generic-workspace-contract@1")
    request_id = f"artifact-acceptance-{uuid4()}"
    migration_pool = await create_application_migration_pool(settings)
    try:
        await apply_application_migrations(migration_pool)
    finally:
        await migration_pool.close()
    postgres_pool = await create_application_postgres_pool(settings)
    mongo_client, _database = await create_mongodb(settings)
    policies = AdmissionPolicyRegistry()
    policies.register("contract:live-artifact-input@1", lambda _request, _config: None)
    policies.register("contract:live-artifact-invariant@1", lambda _request, _config: None)
    run_control = RunControlService(
        PostgresRunControlRepository(postgres_pool),
        LiveConfigurationVerifier(),
        policies,
    )
    owner = WorkspaceOwner(
        kind=WorkspaceOwnerKind.STAGE,
        owner_id="stage:generic-senescence-research",
    )
    skill_digests = (
        _digest_bytes(CREATE_AND_PATCH_SKILL),
        _digest_bytes(TAVILY_BEST_PRACTICES_SKILL),
        _digest_bytes(VERCEL_AGENT_BROWSER_SKILL),
    )
    asset_contents = dict(
        zip(
            skill_digests,
            (
                CREATE_AND_PATCH_SKILL,
                TAVILY_BEST_PRACTICES_SKILL,
                VERCEL_AGENT_BROWSER_SKILL,
            ),
            strict=True,
        )
    )
    asset_verifier = ConformanceAssetVerifier(
        asset_manifest_digests={
            "skill:acceptance.create-and-patch:1": skill_digests[0],
            "skill:tavily.best-practices:1": skill_digests[1],
            "skill:vercel.agent-browser:1": skill_digests[2],
        }
    )
    try:
        with TemporaryDirectory(prefix="belllabs-artifact-acceptance-") as root:
            workspace_service = WorkspaceMaterializationService(
                manifests=MongoWorkspaceManifestRepository(),
                provisioner=FilesystemWorkspaceProvisioner(Path(root)),
                durable_inputs=InMemoryDurableWorkspaceInputs(),
            )
            candidates = WorkspaceCandidateCaptureService(
                materializer=workspace_service,
                contents=FilesystemWorkspaceCandidateContents(Path(root) / "candidate-content"),
            )
            runtime = OpenAIAgentsSandboxRuntime(
                fixture_asset_contents=asset_contents,
                required_sandbox_tools=frozenset({"apply_patch", "exec_command"}),
                required_sandbox_tool_counts={"apply_patch": 2, "exec_command": 2},
                required_artifact_paths=(REPORT_PATH,),
                candidate_sink=candidates,
            )
            binding_repository = MongoOperationBindingRepository()
            operation_authority = ConformanceAuthority(
                accepted_run_id="pending",
                configuration_digest=configuration_digest,
                control_revision=3,
                reservation_id="artifact-acceptance-operation",
            )
            operation_service = OperationExecutionService(
                authority=operation_authority,
                bindings=binding_repository,
                runtime=runtime,
                sandbox=BindingWorkspaceMaterializer(workspace_service),
                assets=asset_verifier,
                mcp=asset_verifier,
                secrets=ConformanceSecretResolver(
                    {
                        "environment:OPENAI_API_KEY": settings.openai_api_key.get_secret_value(),
                        "environment:TAVILY_API_KEY": tavily_key,
                    }
                ),
                events=ConformanceEventSink(),
                budget=RunControlOperationBudgetAuthority(
                    run_control,
                    actor=_actor(
                        "workflow_run.report_usage",
                        "workflow_run.settle_usage",
                    ),
                ),
            )
            artifact_payloads = S3ArtifactPayloadStore(settings, settings.s3_bucket)
            durable_references = PostgresArtifactDurableReferenceRepository(postgres_pool)
            artifact_service = ArtifactPromotionService(
                bindings=binding_repository,
                metadata=MongoArtifactMetadataRepository(),
                payloads=artifact_payloads,
                workspaces=workspace_service,
                durable_references=durable_references,
                validation_authority=StaticArtifactValidationAuthority(
                    permission_outcomes={
                        (
                            "operation:generic-artifact-research@1",
                            "permission:public-web-research@1",
                        ): "allowed"
                    },
                    check_outcomes={
                        (
                            "operation:generic-artifact-research@1",
                            "content-integrity",
                            "check:sha256-after-capture",
                        ): "passed"
                    },
                    required_check_ids={
                        "operation:generic-artifact-research@1": frozenset({"content-integrity"})
                    },
                ),
            )
            plugin = OpenAIAgentsPlugin(register_activities=False)
            temporal_client = await create_temporal_client(settings, plugins=[plugin])
            task_queue = f"{settings.temporal_task_queue}-artifact-acceptance"
            operations = OperationExecutionActivities(operation_service)
            artifacts = ArtifactPromotionActivities(
                service=artifact_service,
                candidates=candidates,
            )
            submitter = TemporalGenericArtifactSubmitter(temporal_client, task_queue=task_queue)
            principal = ControlPlanePrincipal(
                actor_id="artifact-acceptance-operator",
                roles=frozenset({"operator"}),
                tenant_scopes=frozenset({"local-artifact-acceptance"}),
                authority_refs=frozenset({"authority:artifact-acceptance"}),
                sponsorship_refs=frozenset({"sponsorship:artifact-acceptance"}),
                approval_refs=frozenset({"approval:artifact-acceptance"}),
            )
            api.dependency_overrides[get_run_control_service] = lambda: run_control
            api.dependency_overrides[get_generic_artifact_submitter] = lambda: submitter
            api.dependency_overrides[get_control_plane_principal] = lambda: principal
            try:
                async with create_generic_artifact_worker(
                    temporal_client,
                    task_queue=task_queue,
                    operations=operations,
                    artifacts=artifacts,
                ):
                    async with httpx.AsyncClient(
                        transport=httpx.ASGITransport(app=api),
                        base_url="http://belllabs.local",
                    ) as client:
                        admitted_response = await client.post(
                            "/run-control/v1/run-requests",
                            json=_run_request(configuration_digest, request_id).model_dump(
                                mode="json"
                            ),
                        )
                        admitted_response.raise_for_status()
                        run_id = str(admitted_response.json()["run_id"])
                        start_response = await client.post(
                            f"/run-control/v1/runs/{run_id}/commands",
                            json=_command(
                                run_id,
                                1,
                                f"start:{request_id}",
                                StartAction(),
                                "workflow_run.start",
                            ).model_dump(mode="json"),
                        )
                        start_response.raise_for_status()
                        reserve_response = await client.post(
                            f"/run-control/v1/runs/{run_id}/commands",
                            json=_command(
                                run_id,
                                2,
                                f"reserve:{request_id}",
                                ReserveBudgetAction(
                                    reservation_id="artifact-acceptance-operation",
                                    amounts={
                                        "tokens.input": 100_000,
                                        "tokens.output": 20_000,
                                        "tokens.total": 120_000,
                                        "model.turns": 8,
                                    },
                                ),
                                "workflow_run.reserve_budget",
                            ).model_dump(mode="json"),
                        )
                        reserve_response.raise_for_status()
                        operation = _operation(
                            run_id=run_id,
                            run_revision=int(reserve_response.json()["resulting_run_version"]),
                            configuration_digest=configuration_digest,
                            image_digest=image.id,
                            workspace_contract_digest=workspace_contract_digest,
                            owner=owner,
                            skill_digests=skill_digests,
                        )
                        operation_authority.accepted_run_id = run_id
                        submission = GenericArtifactWorkflowRequest(
                            request_scope="local-artifact-acceptance",
                            run_id=run_id,
                            operation=operation,
                            promotion=ArtifactPromotionPlan(
                                namespace_id=operation.workspace.namespace_id,
                                workspace_id=operation.workspace.workspace_id,
                                output_slot="report",
                                logical_path=REPORT_PATH,
                                owner=owner,
                                permission_ref="permission:public-web-research@1",
                                permission_outcome="allowed",
                                output_contract_ref=operation.operation_contract_ref,
                                checks=(
                                    ArtifactCheckEvidence(
                                        check_id="content-integrity",
                                        outcome="passed",
                                        evidence_ref="check:sha256-after-capture",
                                    ),
                                ),
                            ),
                        )
                        response = await client.post(
                            f"/run-control/v1/runs/{run_id}/operations",
                            json=submission.model_dump(mode="json"),
                        )
                        response.raise_for_status()
                        result = response.json()
            finally:
                api.dependency_overrides.pop(get_run_control_service, None)
                api.dependency_overrides.pop(get_generic_artifact_submitter, None)
                api.dependency_overrides.pop(get_control_plane_principal, None)
            artifact_id = str(result["artifact"]["artifact_id"])
            visible = await artifact_service.get_visible(artifact_id)
            if visible is None:
                raise RuntimeError("live promoted artifact is not publicly visible")
            _candidate, captured_content = await candidates.get_for_path(
                operation.workspace.namespace_id,
                operation.workspace.workspace_id,
                REPORT_PATH,
            )
            address = ArtifactPayloadAddress(
                object_ref=visible.object_ref,
                content_digest=visible.content_digest,
                size_bytes=len(captured_content),
            )
            report = (await artifact_payloads.retrieve(address)).decode("utf-8")
            if (
                "PATCH_CONFIRMED" not in report
                or "DRAFT_CREATED" in report
                or EXPECTED not in result["operation"]["output_text"]
            ):
                raise RuntimeError("agent did not prove create-then-patch acceptance")
            events = await durable_references.pending_events("local-artifact-acceptance")
            if not any(event["payload"]["artifact_id"] == artifact_id for event in events):
                raise RuntimeError("artifact admission event was not committed")
            print(EXPECTED)
    finally:
        await mongo_client.close()
        await postgres_pool.close()


if __name__ == "__main__":
    asyncio.run(main())
