"""Composition of the Mission Manifest service (FT-E2 compile, FT-E3 submit and start)."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import TYPE_CHECKING

import asyncpg

from mission_control.adapters.capabilities.capability_pins import CapabilityPins
from mission_control.adapters.postgres.chains.store import PostgresChainIntentStore
from mission_control.adapters.postgres.control_plane.definition_repository import (
    PostgresDefinitionRepository,
)
from mission_control.adapters.postgres.control_plane.manifest_submission import (
    PostgresManifestSubmissionRepository,
)
from mission_control.adapters.postgres.frames.transcript_projection import PostgresRunMissionIds
from mission_control.adapters.postgres.orchestration.goal_directed_repository import (
    PostgresGoalDirectedDocumentRepository,
)
from mission_control.adapters.postgres.orchestration.stagegraph_repository import (
    PostgresStageGraphOperationTemplateRepository,
)
from mission_control.adapters.postgres.run_control.cluster_bindings import (
    PostgresRunClusterBindings,
)
from mission_control.application.authoring.manifest_launch_inputs import (
    CapabilityComponentBinding,
    ManifestChainLaunchInputs,
    ManifestLaunchBindings,
    ManifestLaunchInputAuthor,
    ManifestLaunchResolver,
    ServedComponents,
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
from mission_control.application.authoring.service import ControlPlaneService
from mission_control.application.capabilities.catalog import CatalogService
from mission_control.application.chains.relay import (
    ChainIntentRelay,
    ChainRelayPump,
    LaunchServiceChainStarter,
)
from mission_control.application.execution.run_launch import (
    RunLaunchService,
    RunWorkflowSubmitter,
    TemporalClusterIdentity,
)
from mission_control.application.execution.service import (
    AdmissionPolicyRegistry,
    RunControlService,
)
from mission_control.application.ports.payloads import ContentAddressedPayloadStore
from mission_control.bootstrap.settings import Settings
from mission_control.domain.authoring.extensions import ExtensionRegistry

if TYPE_CHECKING:
    from mission_control.adapters.temporal.deployment_composition import (
        DeploymentCapabilityComponents,
    )


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


# --- MP-02: the production launch input author and the chain relay -----------------------


def served_components(
    pins: CapabilityPins,
    settings: Settings,
    additional: DeploymentCapabilityComponents | None = None,
) -> ServedComponents:
    """The digests the workers' exact component registry serves (pins plus registrations)."""

    return ServedComponents(
        models=frozenset(item.ref.digest for item in pins.models)
        | frozenset(additional.model_factories if additional is not None else ()),
        sandboxes=frozenset(item.ref.digest for item in pins.sandboxes)
        | frozenset(additional.sandbox_factories if additional is not None else ()),
        checkpointers=frozenset(item.ref.digest for item in pins.checkpointers)
        | frozenset(settings.deep_agent_checkpointer_digests),
        stores=frozenset(item.ref.digest for item in pins.stores)
        | frozenset(settings.deep_agent_store_digests),
        mcp_servers=frozenset(item.ref.digest for item in pins.mcp_servers)
        | frozenset(additional.mcp_servers if additional is not None else ()),
        skills=frozenset(item.bundle_digest for item in pins.skills)
        | frozenset(additional.skill_bundles if additional is not None else ()),
        tools=frozenset(item.ref.digest for item in pins.tools)
        | frozenset(additional.tools if additional is not None else ()),
    )


def resolve_pinned_capabilities(
    bindings: ManifestLaunchBindings, pins: CapabilityPins, *, node_executable: Path | None
) -> ManifestLaunchBindings:
    """Expand ``pinned`` capability bindings into the exact components the pin file states."""

    resolved: dict[str, CapabilityComponentBinding] = {}
    for capability_id, binding in bindings.capabilities.items():
        if binding.pinned is None or binding.resolved:
            resolved[capability_id] = binding
            continue
        update: dict[str, object]
        if binding.kind == "mcp_server":
            if node_executable is None:
                raise ValueError(
                    f"{capability_id}: pinned MCP servers need WEB_RESEARCH_AGENT_BROWSER_NODE"
                )
            update = {
                "mcp_server": pins.mcp_server(binding.pinned).component(
                    node_executable=node_executable
                )
            }
        elif binding.kind == "skill":
            update = {"skill": pins.skill(binding.pinned).component()}
        else:
            update = {"tool": pins.tool(binding.pinned).component()}
        resolved[capability_id] = CapabilityComponentBinding.model_validate(
            {**binding.model_dump(mode="python"), **update}
        )
    return bindings.model_copy(update={"capabilities": resolved})


def compose_manifest_launch_inputs(
    pool: asyncpg.Pool,
    *,
    settings: Settings,
    run_control: RunControlService,
    control_plane: ControlPlaneService,
    pins: CapabilityPins | None = None,
    additional: DeploymentCapabilityComponents | None = None,
) -> ManifestLaunchInputAuthor | None:
    """The production manifest launch author, or ``None`` when no bindings file is set."""

    path = settings.manifest_launch_bindings_path
    if path is None:
        return None
    pins = pins if pins is not None else CapabilityPins.from_settings(settings)
    bindings = resolve_pinned_capabilities(
        ManifestLaunchBindings.load(path),
        pins,
        node_executable=settings.web_research_agent_browser_node,
    )
    resolver = ManifestLaunchResolver(
        bindings,
        served_components(pins, settings, additional),
        provider_secret_env=settings.manifest_provider_secret_env,
    )
    return ManifestLaunchInputAuthor(
        resolver=resolver,
        run_control=run_control,
        control_plane=control_plane,
        definitions=PostgresManifestSubmissionRepository(pool),
        stage_templates=PostgresStageGraphOperationTemplateRepository(pool),
        goal_templates=PostgresGoalDirectedDocumentRepository(pool),
    )


def compose_chain_relay_pump(
    pool: asyncpg.Pool,
    *,
    settings: Settings,
    author: LaunchInputPort,
    run_control: RunControlService,
    submitter: RunWorkflowSubmitter,
    request_scopes: Sequence[str],
    lease_owner: str,
    cluster: TemporalClusterIdentity | None = None,
) -> ChainRelayPump:
    """The chain relay over the governed launch: duplicate deliveries start a run once."""

    launches = RunLaunchService(
        run_control=run_control,
        submitter=submitter,
        mission_ids=PostgresRunMissionIds(pool),
        # MP-22: a released consumer run is bound to the relaying worker's cluster.
        cluster_bindings=PostgresRunClusterBindings(pool) if cluster is not None else None,
        cluster=cluster,
    )
    relay = ChainIntentRelay(
        store=PostgresChainIntentStore(pool),
        starter=LaunchServiceChainStarter(
            inputs=ManifestChainLaunchInputs(author), launches=launches
        ),
        lease_owner=settings.chain_relay_lease_owner or lease_owner,
    )
    return ChainRelayPump(
        relay,
        request_scopes,
        interval_seconds=settings.chain_relay_interval_seconds,
        limit=settings.chain_relay_batch_limit,
    )
