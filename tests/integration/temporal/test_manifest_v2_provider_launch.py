"""MP-02 x MP-07/MP-08 on real local services: a mission/v2 Stage Graph with a
``claude_agent_sdk`` (or ``codex``) node is compiled, submitted and started through the
production path; its sealed ``mc.execution_binding.v2`` reaches ``lane.turn`` on the lane's own
task queue and the unit settles.

Real: a disposable common-component PostgreSQL 17 (``MISSION_CONTROL_TEST_ADMIN_DSN``), a real
Temporal dev server started by the production-stack fixture, the production API/worker
composition, the public ``missions:submit``/``missions:start`` router, the manifest compile and
submit services, the production launch author (``compose_manifest_launch_inputs`` over a
``mc.manifest_launch_bindings.v2`` file named by ``MANIFEST_LAUNCH_BINDINGS_PATH``), the
StageGraph family, ``OperationWorkflow``, the production operation boundary and
``LaneTurnService`` over PostgreSQL frames and lane state.

FIXTURE: the Claude Agent SDK client (``tests/unit/claude/fixtures.py``) and the Codex
app-server (``tests/unit/codex/fixture_app_server.py``): no Claude Code or Codex process is
launched and nothing is paid for; their tmp-dir workspace ports and static auth admissions; the
deployment bindings (Deep Agents scaffold of the test stack, the lane bound to a test queue);
the ``application: biotech`` transform of the checked examples.

Production now routes claude/codex units through `lane.turn` segments
(`OperationHeartbeatPolicy.segments_for`) and records a Session Lane's attempt at admission
(`OperationExecutionService.admit_lane_session(attempt=)`). The FIXTURE harness joins the
production boundary's own registry here because `compose_lane_registry` composes the real
harnesses only on an opted-in Linux/WSL worker.

The Temporal server is the production-stack fixture's dev server (``rrm009_production_harness.
TEMPORAL_PORT``), as in ``test_manifest_launch_production.py``, not a shared 127.0.0.1:7233.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any, cast
from uuid import uuid4

import pytest
from fastapi import FastAPI
from temporalio.worker import Worker

from mission_control.adapters.claude.harness import (
    ClaudeAgentSdkHarness,
    ClaudeLaneSettings,
    StaticAuthAdmitter,
)
from mission_control.adapters.codex.harness import CodexLocalHarness
from mission_control.adapters.cursor.projection import RenderedProjectionSource, static_rows
from mission_control.adapters.postgres.chains.store import HOOK_NAME, ChainReleaseHook
from mission_control.adapters.postgres.control_plane.definition_repository import (
    PostgresDefinitionRepository,
)
from mission_control.adapters.postgres.control_plane.manifest_submission import (
    PostgresManifestSubmissionRepository,
)
from mission_control.adapters.postgres.frames.repository import PostgresFrameRepository
from mission_control.adapters.postgres.lanes.execution_state import PostgresLaneExecutionStateStore
from mission_control.adapters.postgres.run_control.canonical import register_post_append_hook
from mission_control.adapters.postgres.run_control.stop_fence import PostgresStopFenceRepository
from mission_control.adapters.postgres.subscriptions.store import PostgresSubscriptionStore
from mission_control.adapters.storage.control_plane_payloads import InMemoryPayloadStore
from mission_control.adapters.temporal.operation_activities import OperationExecutionActivities
from mission_control.adapters.temporal.registration.activities import agent_cognitive_activities
from mission_control.application.authoring.manifest_launch_inputs import (
    ManifestLaunchBindings,
    ManifestLaunchInputAuthor,
)
from mission_control.application.authoring.manifest_service import (
    ManifestCompileService,
    ManifestProgramCompiler,
    MissionManifestService,
)
from mission_control.application.authoring.manifest_submit import (
    ManifestSubmitService,
    register_manifest_admission_policies,
)
from mission_control.application.authoring.provider_launch import (
    LAUNCH_BINDINGS_SCHEMA_V2,
    ProviderLaneBinding,
    ProviderLanes,
)
from mission_control.application.execution.harness.hook_callbacks import InMemoryHookIntentLedger
from mission_control.application.execution.harness.lane_turns import LaneTurnService
from mission_control.application.execution.harness.registry import LaneRegistry
from mission_control.application.execution.harness.sessions import WorkerSessionManager
from mission_control.application.execution.run_launch import RunLaunchService
from mission_control.application.execution.service import (
    AdmissionPolicyRegistry,
    RunControlService,
)
from mission_control.application.subscriptions.service import SubscriptionService
from mission_control.bootstrap.manifests import compose_manifest_launch_inputs
from mission_control.bootstrap.provider_auth import provider_child_environment
from mission_control.bootstrap.technical_api import api
from mission_control.domain.authoring.extensions import ExtensionRegistry
from mission_control.domain.authoring.manifest import canonical_manifest_bytes, load_manifest_yaml
from tests.fixtures.catalog.fast_track_catalog import fast_track_catalog
from tests.fixtures.manifest_runtime import ChainScript, chain_components
from tests.fixtures.mission_control_production_stack import open_postgres_production_stack
from tests.fixtures.rrm009_production_harness import ProductionStack, _diagnose, _run, _until
from tests.fixtures.rrm009_production_stack import (
    AGENT_COGNITIVE_QUEUE,
    SCOPE,
    technical_binding,
)
from tests.integration.temporal.test_manifest_launch_production import (
    host_pins,
    missions_app,
    post,
    submit,
)
from tests.unit.authoring.manifest_launch_fixture import fixture_bindings
from tests.unit.claude.fixtures import (
    FIXTURE_ENVIRON,
    FixtureClientFactory,
    FixtureScript,
    FixtureWorkspace,
    fixture_admission,
)
from tests.unit.codex.fixture_app_server import FixtureLauncher, load_script
from tests.unit.codex.support import (
    MemoryArtifacts,
    StaticAuth,
    TmpLeaser,
    fixture_settings,
)
from tests.unit.codex.support import admission as codex_admission

pytestmark = pytest.mark.common_db

ROOT = Path(__file__).resolve().parents[3]
EXAMPLES = ROOT / "docs/specs/multi-provider-2026-10/examples"
BINDINGS_EXAMPLE = (
    ROOT / "deployments/examples/manifest-launch-bindings.multi-provider.example.json"
)
AUTH_PROFILES = {
    "claude_agent_sdk": "claude-sdk-owner-subscription",
    "codex": "codex-owner-chatgpt",
}
RUNTIMES = {"claude_agent_sdk": "claude", "codex": "codex"}
OPERATION_WORKFLOW = "belllabs.operation.v2"


def stage_manifest(profile: str) -> str:
    """FIXTURE transform of the checked example: the stack's `biotech` application and its
    `patch` node alone (one provider stage; the Deep Agents and dependency nodes are dropped)."""

    name = f"{profile.replace('_', '-')}.stage-graph.mission.yml"
    document = load_manifest_yaml((EXAMPLES / name).read_text(encoding="utf-8"))
    mission = document["mission"]
    mission["application"] = "biotech"
    (patch,) = [node for node in mission["program"]["nodes"] if node["key"] == "patch"]
    patch.pop("depends_on")
    patch.pop("inputs")
    mission["program"]["nodes"] = [patch]
    return canonical_manifest_bytes(document).decode("utf-8")


def provider_lane(
    bindings: ManifestLaunchBindings, profile: str, task_queue: str
) -> ProviderLaneBinding:
    """The example's provider lane, on the test queue and the stack's own scaffold refs."""

    document = json.loads(BINDINGS_EXAMPLE.read_text(encoding="utf-8"))
    lane = document["providers"][profile]
    scaffold = bindings.deep_agents
    lane.update(
        task_queue=task_queue,
        agent_profile_ref=scaffold.agent_profile_ref.model_dump(mode="json"),
        tracing_policy_ref=scaffold.tracing_policy_ref,
        sensitive_data_policy_ref=scaffold.sensitive_data_policy_ref,
        snapshot_policy_ref=scaffold.snapshot_policy_ref,
        workspace=scaffold.workspace.model_dump(mode="json"),
    )
    return ProviderLaneBinding.model_validate(lane)


@dataclass
class ProviderLaunchStack:
    profile: str
    stack: ProductionStack
    author: ManifestLaunchInputAuthor
    app: FastAPI
    lane_queue: str
    sends: Callable[[], int]
    released: Callable[[], bool]


def fixture_lane(
    profile: str, tmp_path: Path, inputs: _PayloadInputs, fences: PostgresStopFenceRepository
) -> tuple[Any, Callable[[], int], Callable[[], bool]]:
    """The FIXTURE provider harness, how many turns it sent and whether its lease was released
    after custody."""

    projections = RenderedProjectionSource(static_rows(()))
    if profile == "claude_agent_sdk":
        clients = FixtureClientFactory(FixtureScript.load("full_run"))
        workspace = FixtureWorkspace(tmp_path / "leases")
        harness = ClaudeAgentSdkHarness(
            clients=clients,
            workspaces=workspace,
            projections=projections,
            auth=StaticAuthAdmitter(
                fixture_admission().model_copy(update={"profile_id": AUTH_PROFILES[profile]})
            ),
            child_environment=provider_child_environment,
            environ=FIXTURE_ENVIRON,
            fences=fences,
            intents=InMemoryHookIntentLedger(),
            inputs=inputs,
            settings=ClaudeLaneSettings(
                require_executables=False, drain_timeout_s=5.0, init_timeout_s=5.0
            ),
        )

        def claude_sends() -> int:
            assert len(clients.clients) == 1 and clients.last.disconnected
            return len(clients.last.sent)

        return harness, claude_sends, lambda: bool(workspace.released)
    launcher = FixtureLauncher(scripts=[load_script("turn_full")])
    leaser = TmpLeaser(tmp_path / "leases")
    harness = CodexLocalHarness(
        launcher=launcher,
        leaser=leaser,
        projections=projections,
        artifacts=MemoryArtifacts(),
        auth=StaticAuth(codex_admission(profile_id=AUTH_PROFILES[profile])),
        settings=fixture_settings(tmp_path),
        child_environment=provider_child_environment,
        inputs=inputs,
    )
    return harness, lambda: len(launcher.launches), lambda: bool(leaser.released)


@pytest.fixture(params=["claude_agent_sdk", "codex"])
async def provider_launch(
    request: pytest.FixtureRequest, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> AsyncIterator[ProviderLaunchStack]:
    profile = str(request.param)
    lane_queue = f"mp02-{RUNTIMES[profile]}-lane-{uuid4().hex[:8]}"
    technical = technical_binding()
    model_log: list[dict[str, Any]] = []
    components = chain_components(technical, ChainScript(request_scope=SCOPE), model_log)
    base = fixture_bindings(task_queue=AGENT_COGNITIVE_QUEUE)
    bindings = ManifestLaunchBindings.model_validate(
        {
            **base.model_dump(mode="python"),
            "schema_version": LAUNCH_BINDINGS_SCHEMA_V2,
            "model_profiles": {"frontier.default": technical.binding.model},
            "sandbox_profiles": {"research.standard": technical.binding.sandbox},
            "providers": ProviderLanes(**{profile: provider_lane(base, profile, lane_queue)}),
        }
    )
    bindings_path = tmp_path / "manifest-launch-bindings.json"
    bindings_path.write_text(bindings.model_dump_json(indent=2), encoding="utf-8")
    async with open_postgres_production_stack(
        root=tmp_path,
        monkeypatch=monkeypatch,
        technical_override=technical,
        components=components,
        model_log=model_log,
        extra_environment={
            "MANIFEST_LAUNCH_BINDINGS_PATH": str(bindings_path),
            "CAPABILITY_PINS_PATH": str(host_pins(tmp_path / "capability-pins.json")),
            # The provider profiles are implemented but unqualified (no live drill): a
            # local proof admits them only with the documented flag.
            "MISSION_CONTROL_ALLOW_UNQUALIFIED_LANES": "true",
        },
    ) as production:
        unregister = register_post_append_hook(HOOK_NAME, ChainReleaseHook())
        try:
            run_control = cast(RunControlService, api.state.run_control_service)
            register_manifest_admission_policies(
                cast(AdmissionPolicyRegistry, api.state.admission_policy_registry)
            )
            author = compose_manifest_launch_inputs(
                production.worker_pool,
                settings=production.settings,
                run_control=run_control,
                control_plane=production.control_plane,
                additional=components,
            )
            assert author is not None
            definitions, search = await fast_track_catalog()
            catalog = PostgresDefinitionRepository(
                production.worker_pool,
                catalog_scope=production.settings.mission_control_catalog_scope or "",
            )
            programs = ManifestProgramCompiler(catalog, ExtensionRegistry(), InMemoryPayloadStore())
            compiler = ManifestCompileService(
                definitions=definitions, search=search, programs=programs
            )
            lifecycle = MissionManifestService(
                compiler=compiler,
                request_scope=SCOPE,
                lifecycle=ManifestSubmitService(
                    compiler=compiler,
                    programs=programs,
                    run_control=run_control,
                    submissions=PostgresManifestSubmissionRepository(production.worker_pool),
                    request_scope=SCOPE,
                    launches=cast(RunLaunchService, api.state.run_launch_service),
                    launch_inputs=author,
                    subscriptions=SubscriptionService(
                        PostgresSubscriptionStore(production.worker_pool, SCOPE)
                    ),
                ),
            )
            # The lane worker: the FIXTURE provider behind the production `LaneTurnService`,
            # the production operation boundary and PostgreSQL frames and lane state.
            operation = production.factory.operation
            assert operation is not None
            fences = PostgresStopFenceRepository(production.worker_pool)
            harness, sends, released = fixture_lane(
                profile, tmp_path, _PayloadInputs(operation.payloads), fences
            )
            # TEST-LOCAL COMPOSITION: the production `compose_lane_registry` has no claude/codex
            # input yet (integrator-owned `deployment_composition.py`); the FIXTURE harness
            # joins the production boundary's own registry, as that input would register it.
            registry: LaneRegistry = operation.service._lanes
            registry.register(harness)
            frames = PostgresFrameRepository(production.worker_pool)
            lane_turns = LaneTurnService(
                lanes=registry,
                boundary=operation.service,
                frames=frames,
                states=PostgresLaneExecutionStateStore(production.worker_pool),
                frame_reader=frames,
                sessions=WorkerSessionManager(
                    owner_ref=f"mp02-{profile}-worker", min_lease=timedelta(seconds=5)
                ),
                fences=fences,
            )
            activities = OperationExecutionActivities(
                operation.service, worker_identity=f"mp02-{profile}-worker", lane_turns=lane_turns
            )
            async with Worker(
                production.client,
                task_queue=lane_queue,
                activities=agent_cognitive_activities(activities),
            ):
                yield ProviderLaunchStack(
                    profile=profile,
                    stack=production,
                    author=author,
                    app=missions_app(lifecycle),
                    lane_queue=lane_queue,
                    sends=sends,
                    released=released,
                )
        finally:
            unregister()


class _PayloadInputs:
    """Governed read-only workspace inputs from the content-addressed payload store (the
    production worker's reader, `deployment_composition._DurableInputsFromPayloads`)."""

    def __init__(self, payloads: Any) -> None:
        self._payloads = payloads

    async def retrieve(self, durable_ref: str) -> bytes:
        from mission_control.application.artifacts.artifact_promotion import ArtifactPayloadAddress

        object_ref, _, rest = durable_ref.partition("#")
        digest, _, size = rest.rpartition(":")
        data: bytes = await self._payloads.retrieve(
            ArtifactPayloadAddress(
                object_ref=object_ref, content_digest=digest, size_bytes=int(size)
            )
        )
        return data


async def _activity_failures(stack: ProductionStack, run_id: str) -> list[str]:
    """The failure messages of every failed activity of the run (diagnostics only)."""

    found: list[str] = []
    async for execution in stack.client.list_workflows(f"BellLabsRunId = '{run_id}'"):
        history = await stack.client.get_workflow_handle(execution.id).fetch_history()
        for event in history.events:
            if event.HasField("activity_task_failed_event_attributes"):
                failure = event.activity_task_failed_event_attributes.failure
                found.append(f"{execution.id}: {failure.message} {failure.stack_trace[-1500:]}")
    return found


def _lane_turn_inputs(history: Any) -> list[tuple[str, dict[str, Any]]]:
    scheduled: list[tuple[str, dict[str, Any]]] = []
    for event in history.events:
        if not event.HasField("activity_task_scheduled_event_attributes"):
            continue
        attributes = event.activity_task_scheduled_event_attributes
        if attributes.activity_type.name != "lane.turn":
            continue
        payload = json.loads(attributes.input.payloads[0].data)
        scheduled.append((attributes.task_queue.name, payload))
    return scheduled


@pytest.mark.asyncio
async def test_a_v2_provider_stage_reaches_lane_turn_with_its_sealed_binding_and_settles(
    provider_launch: ProviderLaunchStack,
) -> None:
    launch = provider_launch
    profile, stack, app = launch.profile, launch.stack, launch.app
    receipt = await submit(app, stage_manifest(profile))
    (mission,) = receipt["missions"]
    run_id = mission["run_id"]
    assert run_id is not None

    started = await post(app, "/missions:start", {"run_id": run_id})
    assert started.status_code == 202, started.text
    assert started.json()["family"] == "StageGraph"
    frozen = await launch.author.bind(SCOPE, run_id, family="StageGraph")
    template = frozen["patch/execute/default"]
    binding = template.provider_binding
    assert binding is not None and binding.lane_profile == profile
    assert binding.task_queue == launch.lane_queue
    assert template.execution_runtime == RUNTIMES[profile]
    assert template.lane_profile == profile
    assert binding.auth.profile == AUTH_PROFILES[profile]
    assert binding.binding_digest == binding.computed_digest()

    async def operation_closed() -> bool:
        query = f"BellLabsRunId = '{run_id}' AND WorkflowType = '{OPERATION_WORKFLOW}'"
        async for execution in stack.client.list_workflows(query):
            if execution.status is not None and execution.status.name == "COMPLETED":
                return True
        return False

    try:
        await _until(operation_closed, 240)
    except (TimeoutError, AssertionError) as error:
        failures = await _activity_failures(stack, run_id)
        raise AssertionError(f"{error}: {failures} | {await _diagnose(stack, run_id)}") from error

    query = f"BellLabsRunId = '{run_id}' AND WorkflowType = '{OPERATION_WORKFLOW}'"
    (execution,) = [item async for item in stack.client.list_workflows(query)]
    handle = stack.client.get_workflow_handle(execution.id)
    result = await handle.result()
    assert result["disposition"] == "completed", result
    turns = _lane_turn_inputs(await handle.fetch_history())
    assert turns, "the claude unit ran through lane.turn segments"
    for task_queue, payload in turns:
        assert task_queue == launch.lane_queue
        assert payload["lane_profile"] == profile
        sent = payload["operation"]["provider_binding"]
        assert sent["binding_digest"] == binding.binding_digest
        assert sent["materialization_digest"] == binding.materialization_digest
    assert launch.sends() == 1, "one native turn (session) for the unit"
    assert launch.released(), "the lease was released after custody"

    # The settled unit reached the family: its usage reservation is released by the
    # authoritative settlement and the StageGraph decided the stage's result.
    async def reservation_released() -> bool:
        budget = await stack.http.get(
            f"/run-control/v1/runs/{run_id}/budget", params={"request_scope": SCOPE}
        )
        return budget.status_code == 200 and not budget.json()["reservations"]

    await _until(reservation_released, 120)

    # The family decided the stage from the settled unit: the lane's custody output (the
    # FIXTURE workspace snapshot or patch artifact) is accepted output evidence. The required
    # obligation stays open (the FIXTURE turn writes no typed `patch_report`), so the run
    # remains active; acceptance is never inferred from execution completion.
    async def stage_decided() -> bool:
        evidence = (await _run(stack, run_id))["accepted_output_evidence"]
        return bool(evidence)

    await _until(stage_decided, 120)
    run = await _run(stack, run_id)
    assert run["phase"] == "active" and run["terminal_outcome"] is None
    assert run["accepted_obligation_evidence"] == []
