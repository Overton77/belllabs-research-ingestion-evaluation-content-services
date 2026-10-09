"""MP-22 acceptance: public start and chain from the committed deployment bindings example.

Real local services, as MP-02's production test: the production API/worker composition on a
disposable common-component PostgreSQL 17 (``MISSION_CONTROL_TEST_ADMIN_DSN``), the local
Temporal dev server and deterministic local cognition (no provider, no paid effect).

The bindings file is ``deployments/examples/manifest-launch-bindings.deep-agents.example.json``
read from disk and completed with ``compose_launch_bindings`` (the ``preflight
compose-bindings`` path an owner uses). Every value the example leaves to the owner is a FIXTURE
selection here, taken from the test stack's WP-CP-040 technical binding: the model name, prompt,
context-assembly, backend and tracing refs, policy refs, workspace provision, the test stack's
agent-cognitive queue, and ``mcp.pubmed`` bound to the qualification MCP server (no pubmed
server is pinned). The model, sandbox, checkpointer and store stay the example's pinned
components. The readiness check of the composed file must report nothing unresolved before
anything starts.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any, cast

import pytest

from mission_control.adapters.capabilities.capability_pins import CapabilityPins
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
from mission_control.application.authoring.manifest_launch_inputs import CapabilityComponentBinding
from mission_control.application.authoring.manifest_service import (
    ManifestCompileService,
    ManifestProgramCompiler,
    MissionManifestService,
)
from mission_control.application.authoring.manifest_submit import (
    ManifestSubmitService,
    register_manifest_admission_policies,
)
from mission_control.application.execution.run_launch import RunLaunchService
from mission_control.application.execution.service import (
    AdmissionPolicyRegistry,
    RunControlService,
)
from mission_control.application.subscriptions.service import SubscriptionService
from mission_control.bootstrap.manifests import compose_manifest_launch_inputs, served_components
from mission_control.bootstrap.preflight import check_bindings, compose_launch_bindings
from mission_control.bootstrap.technical_api import api
from mission_control.domain.authoring.extensions import ExtensionRegistry
from tests.acceptance.control_plane.test_wp_cp_040 import exact_fixture
from tests.fixtures.catalog.fast_track_catalog import fast_track_catalog
from tests.fixtures.manifest_runtime import ChainScript, GoalScript, chain_components
from tests.fixtures.mission_control_production_stack import open_postgres_production_stack
from tests.fixtures.rrm009_production_harness import _diagnose, _run, _until
from tests.fixtures.rrm009_production_stack import (
    AGENT_COGNITIVE_QUEUE,
    SCOPE,
    TechnicalBinding,
    technical_binding,
)
from tests.integration.temporal.test_manifest_launch_production import (
    MISSION_2,
    SCOPED,
    CountingAuthor,
    LaunchStack,
    delivered,
    fetch,
    host_pins,
    missions_app,
    post,
    pubmed_digest,
    pumping,
    relay_pump,
    root_executions,
    submit,
)
from tests.unit.authoring.manifest_launch_fixture import deep_agents_chain, fixture_bindings

pytestmark = pytest.mark.common_db

ROOT = Path(__file__).resolve().parents[3]
EXAMPLE = ROOT / "deployments/examples/manifest-launch-bindings.deep-agents.example.json"


def _json(value: Any) -> Any:
    return json.loads(value.model_dump_json())


async def fixture_selections(technical: TechnicalBinding) -> dict[str, object]:
    """FIXTURE owner selections: the technical stack's values for every example placeholder."""

    _binding, qualification, _bundle = exact_fixture()
    scaffold = fixture_bindings().deep_agents
    (server,) = technical.binding.mcp_servers
    model_name = technical.binding.model.model_name
    return {
        "/model_profiles/frontier.default/model_name": model_name,
        "/model_profiles/frontier.long_context/model_name": model_name,
        "/deep_agents/profile/model/model_name": model_name,
        "/deep_agents/profile/prompt_refs": _json_list(qualification.prompt_refs),
        "/deep_agents/profile/context_assembly_ref": _json(qualification.context_assembly_ref),
        "/deep_agents/profile/backend_ref": _json(qualification.backend_ref),
        "/deep_agents/profile/tracing_policy_ref": _json(qualification.tracing_policy_ref),
        "/deep_agents/placement/task_queue": AGENT_COGNITIVE_QUEUE,
        "/deep_agents/authority_refs": list(scaffold.authority_refs),
        "/deep_agents/redaction_policy_ref": scaffold.redaction_policy_ref,
        "/deep_agents/tracing_policy_ref": scaffold.tracing_policy_ref,
        "/deep_agents/sensitive_data_policy_ref": scaffold.sensitive_data_policy_ref,
        "/deep_agents/snapshot_policy_ref": scaffold.snapshot_policy_ref,
        "/deep_agents/agent_profile_ref": _json(scaffold.agent_profile_ref),
        "/deep_agents/workspace": _json(scaffold.workspace),
        "/capabilities/mcp.pubmed": _json(
            CapabilityComponentBinding(
                catalog_digest=await pubmed_digest(), kind="mcp_server", mcp_server=server
            )
        ),
    }


def _json_list(values: Any) -> list[Any]:
    return [_json(item) for item in values]


@pytest.fixture
async def example_stack(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> AsyncIterator[LaunchStack]:
    script = ChainScript(request_scope=SCOPE)
    model_log: list[dict[str, Any]] = []
    technical = technical_binding()
    components = chain_components(technical, script, model_log)
    base = json.loads(EXAMPLE.read_text(encoding="utf-8"))
    composed = compose_launch_bindings(base, await fixture_selections(technical))
    bindings_path = tmp_path / "manifest-launch-bindings.json"
    await asyncio.to_thread(
        bindings_path.write_text, composed.model_dump_json(indent=2), encoding="utf-8"
    )
    pins_path = host_pins(tmp_path / "capability-pins.json")
    async with open_postgres_production_stack(
        root=tmp_path,
        monkeypatch=monkeypatch,
        technical_override=technical,
        components=components,
        model_log=model_log,
        extra_environment={
            "MANIFEST_LAUNCH_BINDINGS_PATH": str(bindings_path),
            "CAPABILITY_PINS_PATH": str(pins_path),
        },
    ) as production:
        # The readiness gate sees nothing unresolved in the composed file before any start.
        pins = CapabilityPins.load(pins_path)
        served = served_components(pins, production.settings, components)
        loaded, issues = check_bindings(bindings_path, served, pins, production.settings)
        assert loaded is not None and issues == [], issues
        assert loaded.model_profiles["frontier.default"].ref == technical.binding.model.ref
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
            yield LaunchStack(production, script, author, missions_app(lifecycle), model_log)
        finally:
            unregister()


@pytest.mark.asyncio
async def test_public_start_and_chain_run_from_the_example_bindings(
    example_stack: LaunchStack,
) -> None:
    stack, script, app = example_stack.stack, example_stack.script, example_stack.app
    receipt = await submit(app, deep_agents_chain(MISSION_2.read_text(encoding="utf-8")))
    members = {item["mission_key"]: item for item in receipt["missions"]}
    research, ingestion = members["research"], members["ingestion"]
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

    started = await post(app, "/missions:start", {"run_id": research["run_id"]})
    assert started.status_code == 202, started.text
    frozen = await example_stack.author.bind(SCOPE, research["run_id"], family="GoalDirected")
    executor = frozen["executor"]
    assert executor.deep_agent_binding is not None
    assert executor.deep_agent_binding.task_queue == AGENT_COGNITIVE_QUEUE
    assert executor.output_schema is not None
    assert executor.output_schema.schema_id == "belllabs.goal-executor-observation.v1"

    author = CountingAuthor(example_stack.author)
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
        f"SELECT run_key FROM mission_control.mission_run WHERE {SCOPED} AND mission_id = $4::uuid",
        ingestion["mission_id"],
    )
    consumer_key = consumer["run_key"]
    assert author.calls == [consumer_key]
    (intent,) = await fetch(
        stack,
        f"SELECT delivery_key, delivery_state FROM mission_control.outbox "
        f"WHERE {SCOPED} AND destination_kind = $4",
        "mc.chain.start_run",
    )
    assert intent["delivery_state"] == "delivered"
    assert len(await root_executions(stack, consumer_key)) == 1
    reports = await pump.run_once()
    assert delivered(reports) == []
    assert (await _run(stack, research["run_id"]))["phase"] != "pending"

    async def consumer_active() -> bool:
        return bool((await _run(stack, consumer_key))["phase"] != "pending")

    await _until(consumer_active, 120)
