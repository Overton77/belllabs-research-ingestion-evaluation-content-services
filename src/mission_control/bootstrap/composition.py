"""Concrete per-installation PostgreSQL composition over the common component.

This factory never migrates, seeds identity rows, selects secrets, or connects to a
fallback database. The operator supplies app-bound restricted pools; readiness
requires the attested common mission_control release for this exact binding.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Literal

import asyncpg

from mission_control.adapters.postgres.async_subagents.async_subagents import (
    PostgresAsyncSubagentAuthority,
)
from mission_control.adapters.postgres.chains.store import install_chain_release_hook
from mission_control.adapters.postgres.context.continuation_repository import (
    PostgresContinuationRepository,
    PostgresRunIds,
)
from mission_control.adapters.postgres.control_plane.definition_repository import (
    PostgresDefinitionRepository,
)
from mission_control.adapters.postgres.frames.repository import PostgresFrameRepository
from mission_control.adapters.postgres.frames.transcript_projection import PostgresRunMissionIds
from mission_control.adapters.postgres.orchestration.stagegraph_repository import (
    PostgresStageGraphOperationTemplateRepository,
)
from mission_control.adapters.postgres.run_control.inspection_repository import (
    PostgresInspectionReadRepository,
)
from mission_control.adapters.postgres.run_control.run_control_repository import (
    PostgresRunControlRepository,
)
from mission_control.adapters.postgres.run_control.stop_fence import PostgresStopFenceRepository
from mission_control.adapters.postgres.runtime.run_forks import (
    PostgresForkMaterializationStore,
    PostgresForkSourceReader,
    PostgresRunSnapshotRepository,
)
from mission_control.adapters.postgres.runtime.stage3_kernel_repository import (
    RETENTION_DAYS,
    PostgresForkRepository,
)
from mission_control.application.authoring.service import ControlPlaneService
from mission_control.application.context.continuation import (
    ContinuationCommands,
    ContinuationTriggers,
    FrameSessionLocator,
)
from mission_control.application.execution.boundary_interventions import (
    BoundaryCommandDeliveryService,
    BoundaryCommandTransport,
    BoundaryInterventionService,
)
from mission_control.application.execution.operations.unit_reconciliation import (
    AcceptedCheckpointVerifier,
    UnitReconciliationNudge,
)
from mission_control.application.execution.run_launch import RunLaunchService, RunWorkflowSubmitter
from mission_control.application.execution.service import (
    AdmissionPolicyRegistry,
    F1RunConfigurationVerifier,
    FamilyAdmissionRegistry,
    RunControlService,
)
from mission_control.application.installations.registry import (
    ApplicationBinding,
    ApplicationRegistry,
    InstallationObservation,
    InstallationUnavailable,
    VerifiedApplicationIdentity,
    request_scope,
)
from mission_control.application.missions.admission import MissionAdmissionService
from mission_control.application.missions.runtime import MissionControlRuntimeService
from mission_control.application.missions.service import MissionControlService
from mission_control.application.ports.payloads import ContentAddressedPayloadStore
from mission_control.application.programs.fork_templates import StageGraphForkTemplateDerivation
from mission_control.application.recovery.run_forks import (
    ForkPatchPolicyRegistry,
    LedgerPendingCommands,
    LineageAsyncChildForkClassifier,
    RecordingForkMaterializer,
    RunControlForkAuthority,
    RunSnapshotService,
    SemanticForkService,
)
from mission_control.application.recovery.runtime_recovery import RuntimeForkService
from mission_control.bootstrap.common_installation import (
    CommonReadiness,
    inspect_common_installation,
    verify_pool_role,
)
from mission_control.bootstrap.operation_recovery_composition import (
    compose_postgres_operation_recovery,
)
from mission_control.domain.authoring.extensions import ExtensionRegistry


@dataclass(frozen=True)
class MissionApplicationServices:
    binding: ApplicationBinding
    readiness: CommonReadiness
    catalog_scope: str
    control_plane: ControlPlaneService
    run_control: RunControlService
    lifecycle: MissionControlService
    runtime: MissionControlRuntimeService
    admission: MissionAdmissionService
    launch: RunLaunchService | None
    delivery: BoundaryCommandDeliveryService | None
    snapshots: RunSnapshotService
    forks: SemanticForkService
    fork_receipts: PostgresForkRepository
    materializations: PostgresForkMaterializationStore


async def compose_application_services(
    binding: ApplicationBinding,
    identity: VerifiedApplicationIdentity,
    *,
    runtime_pool: asyncpg.Pool,
    admission_policies: AdmissionPolicyRegistry,
    extensions: ExtensionRegistry,
    payload_store: ContentAddressedPayloadStore,
    storage_mode: Literal["production_common"] = "production_common",
    registry: ApplicationRegistry | None = None,
    boundary_transport: BoundaryCommandTransport | None = None,
    submitter: RunWorkflowSubmitter | None = None,
    family_writer_pool: asyncpg.Pool | None = None,
    family_admissions: FamilyAdmissionRegistry | None = None,
    fork_policies: ForkPatchPolicyRegistry | None = None,
    nudge: UnitReconciliationNudge | None = None,
    checkpoint_verifier: AcceptedCheckpointVerifier | None = None,
    externalize_above_bytes: int = 256_000,
) -> MissionApplicationServices:
    if registry is not None:
        registry.disable(binding.application_id)
    try:
        binding = ApplicationBinding.model_validate(binding.model_dump(mode="python"))
    except ValueError as error:
        raise InstallationUnavailable("application binding digest or identity mismatch") from error
    if storage_mode != "production_common":
        raise InstallationUnavailable("only the common mission_control component is supported")
    if (
        identity.application_id != binding.application_id
        or identity.installation_id != binding.installation_id
        or identity.issuer not in binding.accepted_issuers
        or not identity.audiences & binding.accepted_audiences
    ):
        raise InstallationUnavailable("identity is not bound to this installation")
    readiness = await inspect_common_installation(runtime_pool, binding)
    if family_writer_pool is not None:
        await inspect_family_writer_installation(
            family_writer_pool, readiness.database_name, binding
        )
    scope = request_scope(identity)
    catalog_scope = f"mc/{binding.installation_id}/{binding.application_id}/catalog"
    catalog = ControlPlaneService(
        PostgresDefinitionRepository(runtime_pool, catalog_scope=catalog_scope),
        extensions,
        payload_store,
        externalize_above_bytes=externalize_above_bytes,
    )
    # FT-D2: chain links release inside the ledger commit that satisfies them.
    install_chain_release_hook()
    repository = PostgresRunControlRepository(runtime_pool, family_writer_pool=family_writer_pool)
    run_control = RunControlService(
        repository, F1RunConfigurationVerifier(catalog), admission_policies, family_admissions
    )
    delivery = (
        BoundaryCommandDeliveryService(run_control, boundary_transport)
        if boundary_transport is not None
        else None
    )
    interventions = BoundaryInterventionService(run_control, delivery)
    snapshot_repository = PostgresRunSnapshotRepository(runtime_pool)
    materializations = PostgresForkMaterializationStore(runtime_pool)
    fork_receipts = PostgresForkRepository(runtime_pool)
    saga = RuntimeForkService(
        repository=fork_receipts,
        authority=RunControlForkAuthority(run_control, repository),
        materializer=RecordingForkMaterializer(
            materializations, retention=lambda at: at + timedelta(days=RETENTION_DAYS)
        ),
    )
    snapshots = RunSnapshotService(
        reads=PostgresInspectionReadRepository(runtime_pool),
        sources=PostgresForkSourceReader(runtime_pool),
        snapshots=snapshot_repository,
        async_children=LineageAsyncChildForkClassifier(
            PostgresAsyncSubagentAuthority(runtime_pool)
        ),
        commands=LedgerPendingCommands(run_control),
    )
    forks = SemanticForkService(snapshots=snapshot_repository, saga=saga, policies=fork_policies)
    recovery = compose_postgres_operation_recovery(
        runtime_pool, run_control=run_control, nudge=nudge, verifier=checkpoint_verifier
    )
    launch = (
        RunLaunchService(
            run_control=run_control,
            submitter=submitter,
            forks=fork_receipts,
            materializations=materializations,
            fork_templates=StageGraphForkTemplateDerivation(
                PostgresStageGraphOperationTemplateRepository(runtime_pool)
            ),
            # FT-C4: the root starts with the ledger mission as `mc_mission_id`.
            mission_ids=PostgresRunMissionIds(runtime_pool),
        )
        if submitter is not None
        else None
    )
    result = MissionApplicationServices(
        binding=binding,
        readiness=readiness,
        catalog_scope=catalog_scope,
        control_plane=catalog,
        run_control=run_control,
        lifecycle=MissionControlService(
            run_control,
            interventions,
            request_scope=scope,
            stop_fences=PostgresStopFenceRepository(runtime_pool),
            # FT-B4: request_continuation records a trigger for the live session.
            continuations=ContinuationCommands(
                ContinuationTriggers(PostgresContinuationRepository(runtime_pool)),
                FrameSessionLocator(
                    PostgresFrameRepository(runtime_pool), PostgresRunIds(runtime_pool)
                ),
                request_scope=scope,
            ),
        ),
        runtime=MissionControlRuntimeService(
            snapshots, forks, request_scope=scope, reconciliation_service=recovery.reconciliation
        ),
        admission=MissionAdmissionService(run_control, request_scope=scope, launch_service=launch),
        launch=launch,
        delivery=delivery,
        snapshots=snapshots,
        forks=forks,
        fork_receipts=fork_receipts,
        materializations=materializations,
    )
    if registry is not None:
        if registry.bindings.get(binding.application_id) != binding:
            raise InstallationUnavailable("composition binding differs from deployment registry")
        registry.observe(
            binding.application_id,
            InstallationObservation(
                binding.installation_id,
                binding.application_id,
                binding.supabase_project_ref,
                frozenset({readiness.component_version}),
            ),
        )
    return result


async def inspect_family_writer_installation(
    pool: asyncpg.Pool, database_name: str, binding: ApplicationBinding
) -> None:
    """The family writer is a distinct restricted login on the same installation."""
    async with pool.acquire() as connection, connection.transaction(readonly=True):
        role = await verify_pool_role(connection, "mission_control_family_writer")
        actual = await connection.fetch(
            """
            SELECT installation_id, application_id, supabase_project_ref,
                   schema_component_version
            FROM mission_control.application_installation WHERE state = 'active'
            """
        )
    if (
        role["database_name"] != database_name
        or len(actual) != 1
        or actual[0]["installation_id"] != binding.installation_id
        or actual[0]["application_id"] != binding.application_id
        or actual[0]["supabase_project_ref"] != binding.supabase_project_ref
        or actual[0]["schema_component_version"] != binding.required_component_version
    ):
        raise InstallationUnavailable("family writer points to another installation")
