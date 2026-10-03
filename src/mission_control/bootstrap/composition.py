"""Concrete per-installation PostgreSQL composition, with honest readiness boundaries.

This factory never migrates, seeds identity rows, selects secrets, or connects to a
fallback database. The operator supplies app-bound restricted pools and qualified
runtime adapters. Current persistence adapters target the transitional schema, so
production common-schema readiness is deliberately unavailable.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Literal

import asyncpg

from mission_control.adapters.postgres.async_subagents.async_subagents import (
    PostgresAsyncSubagentAuthority,
)
from mission_control.adapters.postgres.control_plane.definition_repository import (
    PostgresDefinitionRepository,
)
from mission_control.adapters.postgres.orchestration.stagegraph_repository import (
    PostgresStageGraphOperationTemplateRepository,
)
from mission_control.adapters.postgres.run_control.inspection_repository import (
    PostgresInspectionReadRepository,
)
from mission_control.adapters.postgres.run_control.run_control_repository import (
    PostgresRunControlRepository,
)
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
from mission_control.bootstrap.operation_recovery_composition import (
    compose_postgres_operation_recovery,
)
from mission_control.domain.authoring.extensions import ExtensionRegistry

TRANSITIONAL_COMPONENT_VERSION = "transitional-local-v1"
REQUIRED_MIGRATIONS = frozenset(
    {
        "0024_run_snapshots_semantic_forks_v1.sql",
        "0027_immutable_runtime_documents.sql",
        "0030_definition_catalog.sql",
        "0034_mission_installation_identity.sql",
        "0036_mission_identity_family_read.sql",
    }
)


@dataclass(frozen=True)
class CompositionReadiness:
    storage_mode: Literal["transitional_local"]
    database_name: str
    applied_migrations: frozenset[str]
    runtime_role: str
    production_ready: Literal[False] = False


@dataclass(frozen=True)
class MissionApplicationServices:
    binding: ApplicationBinding
    readiness: CompositionReadiness
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
    storage_mode: Literal["production_common", "transitional_local"] = "production_common",
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
    if storage_mode != "transitional_local":
        raise InstallationUnavailable(
            "common mission_control schema adapters are not released; "
            "transitional local qualification cannot enable production"
        )
    if (
        identity.application_id != binding.application_id
        or identity.installation_id != binding.installation_id
        or identity.issuer not in binding.accepted_issuers
        or not identity.audiences & binding.accepted_audiences
        or binding.required_component_version != TRANSITIONAL_COMPONENT_VERSION
    ):
        raise InstallationUnavailable("identity is not bound to this transitional installation")
    readiness = await inspect_transitional_installation(runtime_pool, binding)
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
        lifecycle=MissionControlService(run_control, interventions, request_scope=scope),
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
                frozenset({TRANSITIONAL_COMPONENT_VERSION}),
            ),
        )
    return result


async def inspect_transitional_installation(
    pool: asyncpg.Pool, binding: ApplicationBinding
) -> CompositionReadiness:
    async with pool.acquire() as connection:
        role = await connection.fetchrow("""
            SELECT current_user AS name, current_database() AS database_name,
                   rolsuper, rolbypassrls,
                   pg_has_role(current_user,'belllabs_control_runtime','member') AS runtime_member,
                   pg_has_role(current_user,'belllabs_family_repository_writer','member')
                       AS family_member
            FROM pg_roles WHERE rolname=current_user
        """)
        if (
            role is None
            or role["rolsuper"]
            or role["rolbypassrls"]
            or not role["runtime_member"]
            or role["family_member"]
        ):
            raise InstallationUnavailable("runtime pool must use the restricted control role")
        try:
            rows = await connection.fetch("""
                SELECT installation_id, application_id, project_ref,
                       component_version, database_name
                FROM belllabs_control.mission_installation_identity
            """)
            migrations = frozenset(
                row["version"]
                for row in await connection.fetch(
                    "SELECT version FROM belllabs_control.schema_migrations"
                )
            )
        except (asyncpg.UndefinedTableError, asyncpg.InsufficientPrivilegeError) as error:
            raise InstallationUnavailable(
                "transitional installation evidence is unavailable"
            ) from error
        if len(rows) != 1:
            raise InstallationUnavailable("exactly one persisted installation identity is required")
        actual = rows[0]
        if (
            actual["installation_id"] != binding.installation_id
            or actual["application_id"] != binding.application_id
            or actual["project_ref"] != binding.supabase_project_ref
            or actual["component_version"] != TRANSITIONAL_COMPONENT_VERSION
            or actual["database_name"] != role["database_name"]
            or not REQUIRED_MIGRATIONS <= migrations
        ):
            raise InstallationUnavailable(
                "persisted installation identity or migration release mismatch"
            )
        return CompositionReadiness(
            storage_mode="transitional_local",
            database_name=role["database_name"],
            applied_migrations=migrations,
            runtime_role=role["name"],
        )


async def inspect_family_writer_installation(
    pool: asyncpg.Pool, database_name: str, binding: ApplicationBinding
) -> None:
    async with pool.acquire() as connection:
        row = await connection.fetchrow("""
            SELECT current_database() AS database_name, rolsuper, rolbypassrls,
                   pg_has_role(current_user,'belllabs_family_repository_writer','member')
                       AS family_member,
                   pg_has_role(current_user,'belllabs_control_runtime','member') AS runtime_member
            FROM pg_roles WHERE rolname=current_user
        """)
        if (
            row is None
            or row["database_name"] != database_name
            or row["rolsuper"]
            or row["rolbypassrls"]
            or not row["family_member"]
            or row["runtime_member"]
        ):
            raise InstallationUnavailable(
                "family pool must be a distinct restricted writer in this database"
            )
        actual = await connection.fetchrow("""
            SELECT installation_id,application_id,project_ref,component_version,database_name
            FROM belllabs_control.mission_installation_identity
        """)
        if (
            actual is None
            or actual["installation_id"] != binding.installation_id
            or actual["application_id"] != binding.application_id
            or actual["project_ref"] != binding.supabase_project_ref
            or actual["component_version"] != TRANSITIONAL_COMPONENT_VERSION
            or actual["database_name"] != database_name
        ):
            raise InstallationUnavailable("family writer points to another installation")
