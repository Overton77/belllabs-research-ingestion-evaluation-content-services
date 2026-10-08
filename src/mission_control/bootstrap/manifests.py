"""Composition of the Mission Manifest service (FT-E2 compile, FT-E3 submit and start)."""

from __future__ import annotations

import asyncpg

from mission_control.adapters.postgres.control_plane.definition_repository import (
    PostgresDefinitionRepository,
)
from mission_control.adapters.postgres.control_plane.manifest_submission import (
    PostgresManifestSubmissionRepository,
)
from mission_control.application.authoring.manifest_service import (
    ManifestCompileService,
    ManifestProgramCompiler,
    MissionManifestService,
)
from mission_control.application.authoring.manifest_submit import (
    LaunchInputPort,
    ManifestSubmitService,
    SubscriptionPort,
    register_manifest_admission_policies,
)
from mission_control.application.capabilities.catalog import CatalogService
from mission_control.application.execution.run_launch import RunLaunchService
from mission_control.application.execution.service import (
    AdmissionPolicyRegistry,
    RunControlService,
)
from mission_control.application.ports.payloads import ContentAddressedPayloadStore
from mission_control.domain.authoring.extensions import ExtensionRegistry


def compose_manifest_service(
    pool: asyncpg.Pool,
    *,
    request_scope: str,
    catalog: CatalogService,
    extensions: ExtensionRegistry,
    payload_store: ContentAddressedPayloadStore,
    run_control: RunControlService | None = None,
    admission_policies: AdmissionPolicyRegistry | None = None,
    launches: RunLaunchService | None = None,
    subscriptions: SubscriptionPort | None = None,
    launch_inputs: LaunchInputPort | None = None,
) -> MissionManifestService:
    """Compile resolves through the tenant's catalog search and lowers onto the compiler of the
    installation catalog (dry-run overlay: compile persists nothing). With run control composed,
    submit publishes the lowered definitions, commits the revision and admits the run; start
    launches through the governed launch service (no launcher: start is unavailable)."""

    definitions = PostgresDefinitionRepository(pool, catalog_scope=catalog.catalog_scope)
    programs = ManifestProgramCompiler(definitions, extensions, payload_store)
    compiler = ManifestCompileService(
        definitions=catalog.definitions,
        search=catalog.search,
        programs=programs,
        # The production projection is partitioned by the installation catalog scope.
        catalog_scope=catalog.catalog_scope,
    )
    lifecycle = None
    if run_control is not None:
        if admission_policies is not None:
            register_manifest_admission_policies(admission_policies)
        lifecycle = ManifestSubmitService(
            compiler=compiler,
            programs=programs,
            run_control=run_control,
            submissions=PostgresManifestSubmissionRepository(pool),
            request_scope=request_scope,
            launches=launches,
            launch_inputs=launch_inputs,
            subscriptions=subscriptions,
        )
    return MissionManifestService(
        compiler=compiler, request_scope=request_scope, lifecycle=lifecycle
    )
