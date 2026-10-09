"""MP-02 acceptance: manifest runs start with the production launch input author.

Real local services: the production API/worker composition on a disposable common-component
PostgreSQL 17 (``MISSION_CONTROL_TEST_ADMIN_DSN``) with the local Temporal dev server and
deterministic local cognition (``ChainModel``; no provider, no paid effect).

What is production here: the launch input author (``compose_manifest_launch_inputs`` over the
settings-declared ``MANIFEST_LAUNCH_BINDINGS_PATH``), the public ``POST .../missions:submit``
and ``POST .../missions:start`` router (no ``family_input`` in the body), the manifest submit
service, the governed launch, the chain release hook and the chain relay pump
(``compose_chain_relay_pump``). What is FIXTURE: the seeded fast-track catalog the manifest
resolves against, the deployment bindings file (every model/sandbox profile mapped to the
WP-CP-040 qualification components the worker serves as deterministic local cognition, see
``tests.unit.authoring.manifest_launch_fixture``), the Mission 2 lane transform onto
``deep_agents``, and the test-stack root submitter (the technical stack's coordinator queues,
as its API launch composes them).

1. Mission 2 (``research`` supplies ``ingestion``) is submitted and its first member started
   through HTTP; the production author freezes its Goal Loop templates.
2. ``research`` accepts, the chain releases ``ingestion`` and the production relay pump starts
   it; the start intent is then delivered again (acknowledgement lost) and the consumer still
   has exactly one root workflow.
3. A manifest whose node selects an unbound model profile is refused before any dispatch with
   409 ``start_unavailable`` naming the manifest pointer; the run stays pending.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast
from uuid import uuid4

import httpx
import pytest
from fastapi import FastAPI
from temporalio.service import RPCError

from mission_control.adapters.capabilities.capability_pins import (
    CapabilityPinError,
    PinnedSkill,
    PinnedTool,
)
from mission_control.adapters.postgres.chains.store import HOOK_NAME, ChainReleaseHook
from mission_control.adapters.postgres.control_plane.definition_repository import (
    PostgresDefinitionRepository,
)
from mission_control.adapters.postgres.control_plane.manifest_submission import (
    PostgresManifestSubmissionRepository,
)
from mission_control.adapters.postgres.run_control.canonical import register_post_append_hook
from mission_control.adapters.postgres.subscriptions.store import PostgresSubscriptionStore
from mission_control.adapters.storage.control_plane_payloads import InMemoryPayloadStore
from mission_control.adapters.temporal.coordinator_runtime import coordinator_task_queues
from mission_control.adapters.temporal.submission import TemporalWorkflowSubmitter
from mission_control.application.authoring.manifest_launch_inputs import (
    ManifestLaunchInputAuthor,
    binding_ref,
)
from mission_control.application.authoring.manifest_service import (
    ManifestCompileService,
    ManifestProgramCompiler,
    ManifestScope,
    MissionManifestService,
)
from mission_control.application.authoring.manifest_submit import (
    ManifestSubmitService,
    register_manifest_admission_policies,
)
from mission_control.application.chains.relay import ChainRelayPump, ChainRelayReport
from mission_control.application.execution.run_launch import RunLaunchService
from mission_control.application.execution.service import (
    AdmissionPolicyRegistry,
    RunControlService,
)
from mission_control.application.installations.registry import (
    ApplicationBinding,
    ApplicationRegistry,
    InstallationObservation,
)
from mission_control.application.subscriptions.service import SubscriptionService
from mission_control.bootstrap.manifests import (
    compose_chain_relay_pump,
    compose_manifest_launch_inputs,
)
from mission_control.bootstrap.settings import get_settings
from mission_control.bootstrap.technical_api import api
from mission_control.contracts.identities import parse_request_scope
from mission_control.domain.authoring.extensions import ExtensionRegistry
from mission_control.interfaces.http.mission_control import MissionPrincipal, get_mission_principal
from mission_control.interfaces.http.missions import router as missions_router
from tests.fixtures.catalog.fast_track_catalog import fast_track_catalog
from tests.fixtures.manifest_runtime import (
    AUTHOR,
    ChainScript,
    GoalScript,
    chain_components,
)
from tests.fixtures.mission_control_common_db import INSTALLATIONS
from tests.fixtures.mission_control_production_stack import open_postgres_production_stack
from tests.fixtures.rrm009_production_harness import ProductionStack, _diagnose, _run, _until
from tests.fixtures.rrm009_production_stack import (
    AGENT_COGNITIVE_QUEUE,
    SCOPE,
    TechnicalBinding,
    technical_binding,
)
from tests.unit.authoring.manifest_launch_fixture import (
    deep_agents_chain,
    fixture_bindings,
    pubmed_bound,
)

pytestmark = pytest.mark.common_db

ROOT = Path(__file__).resolve().parents[3]
MISSION_2 = (
    ROOT / "docs/specs/fast-track-2026-10/missions/02-research-ingestion-cursor-cloud-chain.yml"
)
MINIMAL = ROOT / "tests/fixtures/manifests/minimal-stage-graph.yml"
KEY = parse_request_scope(SCOPE)
OWNER_KEY = (KEY.installation_id, KEY.application_id, KEY.tenant_id)
SCOPED = "installation_id = $1 AND application_id = $2 AND tenant_id = $3"
PREFIX = f"/v1/applications/{KEY.application_id}"


async def pubmed_digest() -> str:
    """The catalog revision ``pubmed literature retrieval`` resolves to (fast-track fixture)."""

    definitions, search = await fast_track_catalog()
    service = ManifestCompileService(
        definitions=definitions,
        search=search,
        programs=ManifestProgramCompiler(definitions, ExtensionRegistry(), InMemoryPayloadStore()),
    )
    compilation = await service.compile(
        deep_agents_chain(MISSION_2.read_text(encoding="utf-8")),
        ManifestScope(request_scope=SCOPE, actor_id="owner", at=datetime.now(UTC)),
    )
    assert compilation.ok, [item.message for item in compilation.report.blockers]
    (capability,) = [
        item for item in compilation.definitions[0].capabilities if item.alias == "pubmed"
    ]
    assert capability.resolved is not None and capability.resolved.digest is not None
    return capability.resolved.digest


async def write_bindings(path: Path, technical: TechnicalBinding) -> None:
    """FIXTURE deployment bindings: `frontier.default` only (`frontier.long_context` is
    deliberately unbound), both sandbox profiles, pubmed on the qualification MCP server."""

    (server,) = technical.binding.mcp_servers
    bindings = fixture_bindings(task_queue=AGENT_COGNITIVE_QUEUE).model_copy(
        update={
            "model_profiles": {"frontier.default": technical.binding.model},
            "sandbox_profiles": {
                "research.standard": technical.binding.sandbox,
                "ingestion.standard": technical.binding.sandbox,
            },
        }
    )
    bound = pubmed_bound(bindings, await pubmed_digest(), server)
    await asyncio.to_thread(path.write_text, bound.model_dump_json(indent=2), encoding="utf-8")


def host_pins(path: Path) -> Path:
    """FIXTURE pin file: the deployment pins minus the tool and Skill pins that do not resolve
    on this host (a git worktree's parent has no ``.tools``/``.agents``; a developer workspace
    may carry a re-installed Skill whose bytes drifted from the reviewed pin). No manifest here
    selects a tool or Skill; every other pin is unchanged, and pin drift itself stays covered by
    the capability-pin tests, which fail closed."""

    source = get_settings().capability_pins_path
    document = json.loads(source.read_text(encoding="utf-8"))

    def resolves(verify: Callable[[], object]) -> bool:
        try:
            verify()
        except CapabilityPinError:
            return False
        return True

    document["tools"] = [
        item
        for item in document.get("tools", ())
        if resolves(PinnedTool.model_validate(item).verify_entrypoint)
    ]
    document["skills"] = [
        item
        for item in document.get("skills", ())
        if resolves(PinnedSkill.model_validate(item).bundle)
    ]
    path.write_text(json.dumps(document, indent=2), encoding="utf-8")
    return path


class CountingAuthor:
    """Delegates to the production author; counts what the relay asked of it."""

    def __init__(self, author: ManifestLaunchInputAuthor) -> None:
        self.author = author
        self.calls: list[str] = []

    async def family_input(self, **values: Any) -> dict[str, Any]:
        self.calls.append(values["run_id"])
        return await self.author.family_input(**values)


@dataclass
class LaunchStack:
    stack: ProductionStack
    script: ChainScript
    author: ManifestLaunchInputAuthor
    app: FastAPI
    model_log: list[dict[str, Any]] = field(default_factory=list)


def missions_app(lifecycle: MissionManifestService) -> FastAPI:
    installation_id, project_ref = INSTALLATIONS[KEY.application_id]
    registry = ApplicationRegistry(
        (
            ApplicationBinding.seal(
                application_id=KEY.application_id,
                installation_id=installation_id,
                binding_version="1",
                supabase_project_ref=project_ref,
                database_secret_ref="TEST_DATABASE_URL",
                accepted_issuers={"https://issuer.invalid"},
                accepted_audiences={"authenticated"},
                required_component_version="1",
            ),
        )
    )
    registry.observe(
        KEY.application_id,
        InstallationObservation(installation_id, KEY.application_id, project_ref, frozenset({"1"})),
    )
    app = FastAPI()
    app.include_router(missions_router)
    app.state.mission_control_registry = registry
    app.state.mission_control_manifest_services = {OWNER_KEY: lifecycle}
    app.dependency_overrides[get_mission_principal] = lambda: MissionPrincipal(
        installation_id=installation_id,
        application_id=KEY.application_id,
        tenant_id=KEY.tenant_id,
        issuer="https://issuer.invalid",
        audiences=frozenset({"authenticated"}),
        actor=AUTHOR,
        sponsorship_refs=frozenset({"sponsorship:test"}),
    )
    return app


@pytest.fixture
async def launch_stack(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> AsyncIterator[LaunchStack]:
    script = ChainScript(request_scope=SCOPE)
    model_log: list[dict[str, Any]] = []
    technical = technical_binding()
    components = chain_components(technical, script, model_log)
    bindings_path = tmp_path / "manifest-launch-bindings.json"
    await write_bindings(bindings_path, technical)
    async with open_postgres_production_stack(
        root=tmp_path,
        monkeypatch=monkeypatch,
        technical_override=technical,
        components=components,
        model_log=model_log,
        extra_environment={
            "MANIFEST_LAUNCH_BINDINGS_PATH": str(bindings_path),
            "CAPABILITY_PINS_PATH": str(host_pins(tmp_path / "capability-pins.json")),
        },
    ) as production:
        unregister = register_post_append_hook(HOOK_NAME, ChainReleaseHook())
        try:
            run_control = cast(RunControlService, api.state.run_control_service)
            register_manifest_admission_policies(
                cast(AdmissionPolicyRegistry, api.state.admission_policy_registry)
            )
            # The production composition; the worker's components are served beside the pins.
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
            yield LaunchStack(production, script, author, missions_app(lifecycle), model_log)
        finally:
            unregister()


async def post(app: FastAPI, path: str, body: dict[str, Any]) -> httpx.Response:
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://mission-control"
    ) as client:
        return await client.post(f"{PREFIX}{path}", json=body)


async def submit(app: FastAPI, manifest_yaml: str) -> dict[str, Any]:
    response = await post(
        app, "/missions:submit", {"manifest_yaml": manifest_yaml, "request_id": str(uuid4())}
    )
    assert response.status_code == 201, response.text
    return cast(dict[str, Any], response.json())


async def fetch(stack: ProductionStack, query: str, *args: Any) -> list[Any]:
    async with stack.owner_pool.acquire() as connection:
        return list(await connection.fetch(query, *OWNER_KEY, *args))


async def root_executions(stack: ProductionStack, run_key: str) -> list[str]:
    workflow_id = f"belllabs-run/{run_key}"
    return [
        item.run_id async for item in stack.client.list_workflows(f"WorkflowId = '{workflow_id}'")
    ]


def relay_pump(stack: ProductionStack, author: CountingAuthor) -> ChainRelayPump:
    """The production relay composition over the test stack's root submitter."""

    queues = coordinator_task_queues(stack.settings.temporal_task_queue)
    return compose_chain_relay_pump(
        stack.worker_pool,
        settings=stack.settings,
        author=author,
        run_control=cast(RunControlService, api.state.run_control_service),
        submitter=TemporalWorkflowSubmitter.for_production(
            stack.client,
            stagegraph_task_queue=queues.stagegraph,
            goal_directed_task_queue=queues.goal_directed,
            search_attribute_policy="required",
        ),
        request_scopes=[SCOPE],
        lease_owner="mp02-relay",
    )


@contextlib.asynccontextmanager
async def pumping(pump: ChainRelayPump) -> AsyncIterator[None]:
    stop = asyncio.Event()
    task = asyncio.create_task(pump.run(stop))
    try:
        yield
    finally:
        stop.set()
        await asyncio.wait_for(task, timeout=30)


def delivered(reports: tuple[ChainRelayReport, ...]) -> list[str]:
    return [key for report in reports for key in report.delivered]


# --- Acceptance 1 and 2 -----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_chain_starts_over_http_and_its_consumer_starts_once_under_redelivery(
    launch_stack: LaunchStack,
) -> None:
    stack, script, app = launch_stack.stack, launch_stack.script, launch_stack.app
    receipt = await submit(app, deep_agents_chain(MISSION_2.read_text(encoding="utf-8")))
    members = {item["mission_key"]: item for item in receipt["missions"]}
    research, ingestion = members["research"], members["ingestion"]
    assert research["run_id"] is not None and ingestion["run_id"] is None
    script.by_configuration[research["effective_configuration_digest"]] = GoalScript(
        obligation="evidence_map",
        output_contract="output:evidence_map",
        output_name="evidence_map",
        accept_at=1,
    )
    script.by_configuration[ingestion["effective_configuration_digest"]] = GoalScript(
        obligation="ingested",
        output_contract="output:ingestion_receipt",
        output_name="ingestion_receipt",
        accept_at=1,
    )

    # Acceptance 1: the public start, no family_input; the production author binds the run.
    started = await post(app, "/missions:start", {"run_id": research["run_id"]})
    assert started.status_code == 202, started.text
    assert started.json()["family"] == "GoalDirected"
    frozen = await launch_stack.author.bind(SCOPE, research["run_id"], family="GoalDirected")
    executor = frozen["executor"]
    assert executor.deep_agent_binding is not None
    assert executor.deep_agent_binding.task_queue == AGENT_COGNITIVE_QUEUE
    assert [item.server_name for item in executor.deep_agent_binding.mcp_servers] == [
        stack.technical.binding.mcp_servers[0].server_name
    ]
    assert executor.secret_refs[0].key == "OPENAI_API_KEY"

    author = CountingAuthor(launch_stack.author)
    pump = relay_pump(stack, author)
    async with pumping(pump):

        async def consumer_started() -> bool:
            rows = await fetch(
                stack,
                f"SELECT run_key FROM mission_control.mission_run "
                f"WHERE {SCOPED} AND mission_id = $4::uuid",
                ingestion["mission_id"],
            )
            return bool(rows) and bool(await root_executions(stack, rows[0]["run_key"]))

        try:
            await _until(consumer_started, 420)
        except (TimeoutError, AssertionError) as error:
            raise AssertionError(
                f"{error}: {await _diagnose(stack, research['run_id'])}"
            ) from error

    (consumer,) = await fetch(
        stack,
        f"SELECT run_key, created_by_actor_ref FROM mission_control.mission_run "
        f"WHERE {SCOPED} AND mission_id = $4::uuid",
        ingestion["mission_id"],
    )
    consumer_key = consumer["run_key"]
    assert consumer["created_by_actor_ref"].startswith("chain:")
    assert author.calls == [consumer_key]
    (intent,) = await fetch(
        stack,
        f"SELECT delivery_key, delivery_state FROM mission_control.outbox "
        f"WHERE {SCOPED} AND destination_kind = $4",
        "mc.chain.start_run",
    )
    assert intent["delivery_state"] == "delivered"

    # Acceptance 2: the acknowledgement is lost; the relay delivers the same intent again.
    async with stack.owner_pool.acquire() as connection:
        await connection.execute(
            f"UPDATE mission_control.outbox SET delivery_state = 'pending', delivered_at = NULL "
            f"WHERE {SCOPED} AND delivery_key = $4",
            *OWNER_KEY,
            intent["delivery_key"],
        )
    reports = await pump.run_once()
    assert delivered(reports) == [intent["delivery_key"]], reports
    (redelivery,) = [report.receipts[intent["delivery_key"]] for report in reports]
    # Either the run already left `pending`, or the launch attached to the root the first
    # delivery started (USE_EXISTING): never a second execution.
    executions = await root_executions(stack, consumer_key)
    assert len(executions) == 1, executions
    if not redelivery.already_started:
        assert redelivery.workflow_id == f"belllabs-run/{consumer_key}"
        assert redelivery.temporal_run_id == executions[0]
    assert author.calls == [consumer_key, consumer_key]
    # The second delivery reused the frozen templates (the documents are immutable).
    again = await launch_stack.author.bind(SCOPE, consumer_key, family="GoalDirected")
    assert again["executor"].identity.run_id == consumer_key
    assert binding_ref(consumer_key) == f"semantic-input:manifest:{consumer_key}"
    consumer_run = await _run(stack, consumer_key)
    assert consumer_run["phase"] != "pending", consumer_run


# --- Acceptance 3 -----------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_an_unbound_model_is_refused_before_dispatch_with_its_manifest_pointer(
    launch_stack: LaunchStack,
) -> None:
    stack, app = launch_stack.stack, launch_stack.app
    receipt = await submit(app, MINIMAL.read_text(encoding="utf-8"))
    (mission,) = receipt["missions"]
    run_id = mission["run_id"]
    assert run_id is not None

    refused = await post(app, "/missions:start", {"run_id": run_id})
    assert refused.status_code == 409, refused.text
    detail = refused.json()["detail"]
    assert detail["code"] == "start_unavailable"
    assert detail["message"].startswith("/mission/program/nodes/1/environment/model/profile: ")
    assert "frontier.long_context" in detail["message"]

    # Nothing was dispatched: no root workflow, the run is still pending, no templates frozen.
    with pytest.raises(RPCError):
        await stack.client.get_workflow_handle(f"belllabs-run/{run_id}").describe()
    assert (await _run(stack, run_id))["phase"] == "pending"
    documents = await fetch(
        stack,
        f"SELECT count(*) AS n FROM mission_control.runtime_document "
        f"WHERE {SCOPED} AND identity LIKE $4",
        f'["{binding_ref(run_id)}"%',
    )
    assert documents[0]["n"] == 0
    assert not launch_stack.model_log
