"""FT-E2: Mission Manifest compile resolves searches to pins, validates lanes and lowers onto
the existing compiler, against the seeded catalog fixture (lexical search, no embeddings)."""

from __future__ import annotations

import copy
import json
import os
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from fastmcp import Client, FastMCP

from mission_control.adapters.storage.control_plane_payloads import InMemoryPayloadStore
from mission_control.application.authoring.control_plane_repository import (
    InMemoryDefinitionRepository,
)
from mission_control.application.authoring.manifest_service import (
    ManifestCompilation,
    ManifestCompileService,
    ManifestProgramCompiler,
    ManifestScope,
    MissionManifestService,
)
from mission_control.application.capabilities.capability_search import (
    CapabilitySearchResponse,
    CapabilitySearchService,
)
from mission_control.application.installations.registry import (
    ApplicationBinding,
    ApplicationRegistry,
    InstallationObservation,
)
from mission_control.domain.authoring.canonical import sha256_digest
from mission_control.domain.authoring.contracts import (
    DefinitionKind,
    GoalDirectedBlueprint,
    MCPServerDefinition,
)
from mission_control.domain.authoring.extensions import ExtensionRegistry
from mission_control.domain.authoring.manifest import (
    ManifestErrorCode,
    canonical_manifest_bytes,
    load_manifest_yaml,
)
from mission_control.domain.capabilities.catalog_entry import capability_pin
from mission_control.domain.coordinator.contracts import CapabilitySearchRequest
from mission_control.domain.policies.contracts import ActorContext
from mission_control.interfaces.http.mission_control import MissionPrincipal, get_mission_principal
from mission_control.interfaces.http.missions import router
from mission_control.interfaces.mcp.coordinator_server import CoordinatorPrincipal, _principal_call
from mission_control.interfaces.mcp.mission_tools import (
    COMPILE_TOOL,
    ScopedManifests,
    register_manifest_tools,
)
from tests.fixtures.catalog.fast_track_catalog import fast_track_catalog
from tests.fixtures.mission_control_common_db import canonical_scope
from tests.fixtures.provider_frames import INSTALLATION, SCOPE, TENANT

ROOT = Path(__file__).resolve().parents[3]
MISSIONS = ROOT / "docs/specs/fast-track-2026-10/missions"
GOLDEN = ROOT / "tests/golden/manifests"
MINIMAL = ROOT / "tests/fixtures/manifests/minimal-stage-graph.yml"
AT = datetime(2026, 10, 8, 12, tzinfo=UTC)
OWNER_MANIFESTS = (
    ("01-research-ingestion-deep-agents.yml", "biotech"),
    ("02-research-ingestion-cursor-cloud-chain.yml", "biotech"),
    ("03-codebase-feature-cursor-local.yml", "ai-engineer"),
)


class RecordingSearch:
    """The real search service, with every request recorded."""

    def __init__(self, inner: CapabilitySearchService) -> None:
        self.inner = inner
        self.requests: list[CapabilitySearchRequest] = []

    async def search(self, request: CapabilitySearchRequest) -> CapabilitySearchResponse:
        self.requests.append(request)
        return await self.inner.search(request)

    def max_fused_score(self, mode: Any) -> float:
        return self.inner.max_fused_score(mode)


async def service(
    mutate: Callable[[InMemoryDefinitionRepository], Any] | None = None,
) -> tuple[ManifestCompileService, InMemoryDefinitionRepository, RecordingSearch]:
    definitions, search = await fast_track_catalog()
    if mutate is not None:
        await mutate(definitions)
    recording = RecordingSearch(search)
    compile_service = ManifestCompileService(
        definitions=definitions,
        search=recording,  # type: ignore[arg-type]
        programs=ManifestProgramCompiler(definitions, ExtensionRegistry(), InMemoryPayloadStore()),
    )
    return compile_service, definitions, recording


def scope(application: str = "biotech") -> ManifestScope:
    return ManifestScope(
        request_scope=canonical_scope("tenant-1", application), actor_id="owner", at=AT
    )


def minimal() -> dict[str, Any]:
    return load_manifest_yaml(MINIMAL.read_text(encoding="utf-8"))


def text(document: dict[str, Any]) -> str:
    return canonical_manifest_bytes(document).decode("utf-8")


def reasons(compilation: ManifestCompilation, *, warnings: bool = False) -> set[tuple[str, str]]:
    issues = compilation.report.warnings if warnings else compilation.report.blockers
    return {(issue.pointer, issue.reason or "") for issue in issues}


def resolution_json(compilation: ManifestCompilation) -> str:
    assert compilation.resolution is not None
    return json.dumps(
        compilation.resolution.model_dump(mode="json", by_alias=True), indent=1, sort_keys=True
    )


# --- the three owner manifests ---------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(("name", "application"), OWNER_MANIFESTS)
async def test_owner_manifests_compile_with_zero_blockers_deterministically(
    name: str, application: str
) -> None:
    compile_service, definitions, _ = await service()
    before = len(await definitions.list_published_definitions())
    source = (MISSIONS / name).read_text(encoding="utf-8")
    first = await compile_service.compile(source, scope(application))
    second = await compile_service.compile(source, scope(application))
    assert first.report.blockers == (), first.report.blockers
    assert first.ok and first.resolution is not None
    assert first.resolution.catalog_resolution == "resolved"
    # Every request resolved to an exact pin; every mission lowered to a Compiled Program.
    assert all(item.result is not None for item in first.resolution.resolved)
    assert all(definition.is_resolved for definition in first.definitions)
    assert {program.mission_key for program in first.programs} == {
        definition.mission_key for definition in first.definitions
    }
    assert {item.mission_key for item in first.resolution.lowering} == {
        definition.mission_key for definition in first.definitions
    }
    # Determinism: manifest digest, definition digests and the resolution document.
    assert first.report.manifest_digest == second.report.manifest_digest
    assert [item.digest for item in first.definitions] == [
        item.digest for item in second.definitions
    ]
    assert resolution_json(first) == resolution_json(second)
    # Compile persists nothing in the catalog.
    assert len(await definitions.list_published_definitions()) == before
    golden = GOLDEN / f"{name.removesuffix('.yml')}.resolution.json"
    if os.environ.get("UPDATE_GOLDENS") == "1":
        golden.parent.mkdir(parents=True, exist_ok=True)
        golden.write_text(resolution_json(first) + "\n", encoding="utf-8", newline="\n")
    assert golden.read_text(encoding="utf-8") == resolution_json(first) + "\n"


@pytest.mark.asyncio
async def test_goal_loop_lowers_to_goal_directed_with_goal_obligations() -> None:
    compile_service, _, _ = await service()
    compilation = await compile_service.compile(
        (MISSIONS / "02-research-ingestion-cursor-cloud-chain.yml").read_text(encoding="utf-8"),
        scope(),
    )
    research = compilation.program("research")
    assert research.lowered.family == "GoalDirected"
    blueprint = research.configuration.selected_blueprint
    assert isinstance(blueprint, GoalDirectedBlueprint)
    assert blueprint.required_obligation_refs == frozenset({"evidence_map"})
    assert blueprint.max_iterations == 8
    assert research.configuration.workflow_type.obligations == frozenset({"evidence_map"})
    assert research.lowered.initial_goal is not None
    assert "skeletal muscle" in research.lowered.initial_goal
    budgets = research.configuration.effective_authority.budgets.dimensions
    assert budgets["tokens.total"] == 2_500_000
    assert budgets["currency.estimated_micros"] == 30_000_000
    # Every resolved capability is pinned by exact ref on the runtime profile.
    refs = research.lowered.runtime_profile.operation_binding_refs
    assert {ref.logical_id for ref in refs} >= {"mcp.pubmed-remote", "mcp.tavily"}
    lowering = next(
        item for item in compilation.resolution.lowering if item.mission_key == "research"
    )  # type: ignore[union-attr]
    assert lowering.blueprint_digest == sha256_digest(research.lowered.blueprint)


@pytest.mark.asyncio
async def test_stage_graph_lowering_records_the_nested_goal_loop_and_human_gate_wait() -> None:
    compile_service, _, _ = await service()
    compilation = await compile_service.compile(
        (MISSIONS / "01-research-ingestion-deep-agents.yml").read_text(encoding="utf-8"),
        scope(),
    )
    program = compilation.programs[0]
    assert program.lowered.family == "StageGraph"
    blueprint = program.lowered.blueprint
    assert {stage.stage_id for stage in blueprint.stages} == {  # type: ignore[union-attr]
        "collect",
        "synthesize",
        "review",
        "ingest",
    }
    assert [wait.scope_id for wait in blueprint.waits] == ["review"]  # type: ignore[union-attr]
    assert ("/mission/program/nodes/0", "nested_goal_loop_lowered_as_stage") in reasons(
        compilation, warnings=True
    )


# --- scope, searches and pins ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_application_must_equal_the_authenticated_scope() -> None:
    compile_service, _, recording = await service()
    compilation = await compile_service.compile(
        MINIMAL.read_text(encoding="utf-8"), scope("ai-engineer")
    )
    assert not compilation.ok
    (blocker,) = compilation.report.blockers
    assert blocker.code is ManifestErrorCode.APPLICATION_FORBIDDEN
    assert blocker.pointer == "/mission/application"
    assert recording.requests == []  # nothing is resolved for a foreign application


@pytest.mark.asyncio
async def test_searches_use_kind_host_support_of_the_lane_and_five_results() -> None:
    compile_service, _, recording = await service()
    await compile_service.compile(
        (MISSIONS / "02-research-ingestion-cursor-cloud-chain.yml").read_text(encoding="utf-8"),
        scope(),
    )
    pubmed = next(item for item in recording.requests if item.query.startswith("pubmed"))
    assert pubmed.kinds == frozenset({DefinitionKind.MCP_SERVER})
    assert {profile.value for profile in pubmed.host_profiles} == {"cursor_cloud"}
    assert pubmed.limit == 5
    tool = next(item for item in recording.requests if item.query.startswith("claim schema"))
    assert tool.kinds == frozenset({DefinitionKind.TOOL}) and not tool.host_profiles


@pytest.mark.asyncio
async def test_zero_admitted_hits_is_a_typed_blocker_and_near_ties_warn() -> None:
    compile_service, _, _ = await service()
    document = minimal()
    document["mission"]["environment"]["capabilities"].append(
        {"search": "quantum widget calibration", "kind": "mcp_server", "as": "widget"}
    )
    compilation = await compile_service.compile(text(document), scope())
    blocker = next(
        item for item in compilation.report.blockers if item.reason == "search_no_admitted_hits"
    )
    assert blocker.code is ManifestErrorCode.CAPABILITY_UNAVAILABLE
    assert blocker.pointer == "/mission/environment/capabilities/1"
    # The two subagent profiles are near ties: a warning naming both, the top hit pinned.
    document["mission"]["environment"]["agents"] = [
        {"search": "literature verifier subagent", "kind": "subagent_profile", "as": "lit"}
    ]
    tied = await compile_service.compile(text(document), scope())
    ambiguous = next(item for item in tied.report.warnings if item.reason == "ambiguous_search")
    assert ambiguous.pointer == "/mission/environment/agents/0"
    assert "subagent.literature-verifier" in ambiguous.message
    assert "subagent.code-verifier" in ambiguous.message


@pytest.mark.asyncio
async def test_lane_without_host_support_has_no_admitted_hit() -> None:
    compile_service, _, _ = await service()
    document = minimal()
    document["mission"]["environment"]["capabilities"][0]["require"] = ["cursor_cloud"]
    document["mission"]["environment"]["capabilities"][0]["search"] = "biomcp variant gene"
    compilation = await compile_service.compile(text(document), scope())
    assert ("/mission/environment/capabilities/0", "search_no_admitted_hits") in reasons(
        compilation
    )


async def _retire_and_add_revision(definitions: InMemoryDefinitionRepository) -> None:
    published = await definitions.list_published_definitions()
    browser = next(item for item in published if item.ref.logical_id == "mcp.agent-browser")
    revised = MCPServerDefinition.model_validate(
        {**browser.definition.model_dump(mode="python"), "title": "agent-browser MCP (revised)"}
    )
    await definitions.publish(revised, "fixture", AT, 1)
    biomcp = next(item for item in published if item.ref.logical_id == "mcp.biomcp")
    await definitions.retire(biomcp.ref, "fixture", AT)


@pytest.mark.asyncio
async def test_pins_missing_retired_drifted_and_latest() -> None:
    compile_service, definitions, _ = await service(_retire_and_add_revision)
    published = {
        item.ref.logical_id: item for item in await definitions.list_published_definitions()
    }
    biomcp = capability_pin(published["mcp.biomcp"]).render()
    document = minimal()
    document["mission"]["environment"]["capabilities"] = [
        {"pin": "mcp.nonexistent@1.0.0#sha256:" + "a" * 64, "as": "missing"},
        {"pin": biomcp, "as": "retired"},
        {"pin": "skill.mission-control-observe@0.2.0#sha256:" + "0" * 64, "as": "drifted"},
        {"pin": "skill.mission-control-observe@latest", "as": "latest"},
        {"search": "pubmed literature retrieval", "kind": "mcp_server", "as": "pubmed"},
    ]
    compilation = await compile_service.compile(text(document), scope())
    found = {(item.pointer, item.code.value, item.reason) for item in compilation.report.blockers}
    base = "/mission/environment/capabilities"
    assert (f"{base}/0", "CAPABILITY_UNAVAILABLE", "pin_not_found") in found
    assert (f"{base}/1", "CAPABILITY_UNAVAILABLE", "pin_revoked") in found
    assert (f"{base}/2", "CAPABILITY_DRIFT", "digest_mismatch") in found
    assert compilation.resolution is not None
    latest = next(item for item in compilation.resolution.resolved if item.alias == "latest")
    assert latest.request == {"kind": None, "pin": "skill.mission-control-observe@latest"}
    assert latest.result is not None and latest.result.version == "0.2.0"
    assert latest.result.pin.startswith("skill.mission-control-observe@0.2.0#sha256:")


@pytest.mark.asyncio
async def test_plugins_expand_to_members_with_plugin_provenance() -> None:
    compile_service, _, _ = await service()
    compilation = await compile_service.compile(
        (MISSIONS / "03-codebase-feature-cursor-local.yml").read_text(encoding="utf-8"),
        scope("ai-engineer"),
    )
    assert compilation.ok and compilation.resolution is not None
    (plugin,) = compilation.resolution.plugins
    assert plugin.alias == "agent_browser"
    assert {member.partition("@")[0] for member in plugin.members} == {
        "skill.agent-browser",
        "mcp.agent-browser",
    }
    members = [
        item
        for item in compilation.resolution.resolved
        if item.provenance == "plugin:agent_browser"
    ]
    assert {item.result.capability_id for item in members if item.result} == {
        "skill.agent-browser",
        "mcp.agent-browser",
    }
    root = next(
        node
        for node in compilation.resolution.nodes
        if node.node_key == "root" and node.role == "node"
    )
    assert "plugin:agent_browser" in root.field_provenance.values()


@pytest.mark.asyncio
async def test_two_versions_of_one_capability_are_invalid() -> None:
    compile_service, definitions, _ = await service(_retire_and_add_revision)
    published = await definitions.list_published_definitions()
    revised = max(
        (item for item in published if item.ref.logical_id == "mcp.agent-browser"),
        key=lambda item: item.ref.revision,
    )
    source = (MISSIONS / "03-codebase-feature-cursor-local.yml").read_text(encoding="utf-8")
    document = load_manifest_yaml(source)
    document["mission"]["environment"]["capabilities"].append(
        {"pin": capability_pin(revised).render(), "as": "browser_mcp"}
    )
    compilation = await compile_service.compile(text(document), scope("ai-engineer"))
    assert any(item.reason == "conflicting_versions" for item in compilation.report.blockers)


# --- lanes and hooks ---------------------------------------------------------------------------


def _goal_loop_on(lane: str, behavior: str = "goal_loop") -> dict[str, Any]:
    document = minimal()
    mission = document["mission"]
    mission["environment"]["lane"] = lane
    if lane != "deep_agents":
        mission["environment"].pop("sandbox")
        mission["environment"]["workspace"] = {"repo": {"url": "https://example.invalid/r.git"}}
        if lane == "cursor_local":
            mission["environment"]["workspace"]["repo"]["path"] = "C:/repo"
    mission["environment"]["capabilities"] = []
    node: dict[str, Any] = {
        "key": "root",
        "behavior": behavior,
        "objective": "evidence_map",
        "outputs": [{"name": "claims", "schema": "claim_table@1"}],
    }
    if behavior == "goal_loop":
        node["action_space"] = ["root"]
    mission["program"] = node
    return document


@pytest.mark.asyncio
async def test_lane_support_matrix() -> None:
    compile_service, _, _ = await service()
    swarm = await compile_service.compile(
        text(_goal_loop_on("cursor_local", "parallel_swarm")), scope()
    )
    assert ("/mission/program", "behavior_unsupported_on_lane") in reasons(swarm)
    assert swarm.report.blockers[0].code is ManifestErrorCode.UNSUPPORTED_BEHAVIOR
    on_deep_agents = await compile_service.compile(
        text(_goal_loop_on("deep_agents", "parallel_swarm")), scope()
    )
    assert not any(
        item.reason == "behavior_unsupported_on_lane" for item in on_deep_agents.report.blockers
    )
    goal_loop = await compile_service.compile(text(_goal_loop_on("cursor_cloud")), scope())
    assert goal_loop.resolution is not None
    support = {
        (item.node_key, item.lane): item.supported for item in goal_loop.resolution.lane_support
    }
    assert support[("root", "cursor_cloud")] is True
    reserved = _goal_loop_on("deep_agents")
    reserved["mission"]["environment"]["lane"] = "codex"
    codex = await compile_service.compile(text(reserved), scope())
    assert any(
        item.code is ManifestErrorCode.UNSUPPORTED_BEHAVIOR for item in codex.report.blockers
    )


@pytest.mark.asyncio
async def test_hook_events_absent_on_the_lane_warn_or_block_when_fail_closed() -> None:
    compile_service, _, _ = await service()
    document = _goal_loop_on("cursor_cloud")
    document["mission"]["environment"]["hooks"] = [
        {
            "search": "citation presence check",
            "kind": "hook_script",
            "as": "citation_check",
            "events": ["session_start", "stop"],
        }
    ]
    warned = await compile_service.compile(text(document), scope())
    pointer = "/mission/program/environment/hooks/0/events"
    assert (pointer, "unsupported_on_lane") in reasons(warned, warnings=True)
    assert warned.resolution is not None
    events = {(item.event, item.unsupported_on_lane) for item in warned.resolution.hook_events}
    assert ("session_start", True) in events and ("stop", False) in events
    blocked_document = copy.deepcopy(document)
    blocked_document["mission"]["environment"]["hooks"][0]["fail_closed"] = True
    blocked = await compile_service.compile(text(blocked_document), scope())
    assert (pointer, "unsupported_on_lane") in reasons(blocked)


# --- coverage and budgets ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_coverage_failures_carry_pointers() -> None:
    compile_service, _, _ = await service()
    document = minimal()
    mission = document["mission"]
    mission["goals"][0]["criteria"][0]["evidence"] = ["claims", "unknown_output"]
    nodes = mission["program"]["nodes"]
    nodes[1]["inputs"].append({"name": "extra", "from": "collect.missing"})
    nodes[0]["action_space"] = ["pubmed", "nothing_here"]
    compilation = await compile_service.compile(text(document), scope())
    found = reasons(compilation)
    assert ("/mission/goals/0/criteria/0/evidence/1", "unknown_evidence") in found
    assert ("/mission/program/nodes/1/inputs/1/from", "unresolved_from") in found
    assert ("/mission/program/nodes/0/action_space/1", "unresolved_action_space") in found


@pytest.mark.asyncio
async def test_sibling_budgets_may_oversubscribe_with_a_warning() -> None:
    compile_service, _, _ = await service()
    document = minimal()
    nodes = document["mission"]["program"]["nodes"]
    nodes[0]["environment"]["budget"] = {"usd": 20}
    nodes[1]["environment"]["budget"] = {"usd": 20}
    compilation = await compile_service.compile(text(document), scope())
    assert ("/mission/program/environment/budget/usd", "oversubscribed_budget") in reasons(
        compilation, warnings=True
    )
    assert compilation.ok


# --- HTTP and MCP ------------------------------------------------------------------------------


def http_app(manifests: MissionManifestService, permissions: frozenset[str]) -> FastAPI:
    registry = ApplicationRegistry(
        (
            ApplicationBinding.seal(
                application_id="biotech",
                installation_id=INSTALLATION,
                binding_version="1",
                supabase_project_ref="project",
                database_secret_ref="TEST_DATABASE_URL",
                accepted_issuers={"https://issuer.invalid"},
                accepted_audiences={"authenticated"},
                required_component_version="1",
            ),
        )
    )
    registry.observe(
        "biotech", InstallationObservation(INSTALLATION, "biotech", "project", frozenset({"1"}))
    )
    app = FastAPI()
    app.include_router(router)
    app.state.mission_control_registry = registry
    app.state.mission_control_manifest_services = {(INSTALLATION, "biotech", TENANT): manifests}
    app.dependency_overrides[get_mission_principal] = lambda: MissionPrincipal(
        installation_id=INSTALLATION,
        application_id="biotech",
        tenant_id=TENANT,
        issuer="https://issuer.invalid",
        audiences=frozenset({"authenticated"}),
        actor=ActorContext(actor_id="owner", permissions=permissions),
    )
    return app


READER = frozenset({"workflow_run.read", "catalog:read"})


@pytest.mark.asyncio
async def test_http_compile_returns_200_or_422_with_the_report() -> None:
    compile_service, _, _ = await service()
    manifests = MissionManifestService(compiler=compile_service, request_scope=SCOPE)
    client = TestClient(http_app(manifests, READER))
    path = "/v1/applications/biotech/missions:compile"
    accepted = client.post(path, json={"manifest_yaml": MINIMAL.read_text(encoding="utf-8")})
    assert accepted.status_code == 200, accepted.text
    report = accepted.json()["report"]
    assert report["ok"] is True
    assert report["resolution"]["schema_version"] == "mc.manifest_resolution.v1"
    assert report["resolution"]["catalog_resolution"] == "resolved"
    document = minimal()
    document["mission"]["application"] = "ai-engineer"
    rejected = client.post(path, json={"manifest_yaml": text(document)})
    assert rejected.status_code == 422
    assert rejected.json()["report"]["blockers"][0]["code"] == "APPLICATION_FORBIDDEN"
    denied = TestClient(http_app(manifests, frozenset({"workflow_run.read"})))
    assert denied.post(path, json={"manifest_yaml": "x"}).status_code == 403


@pytest.mark.asyncio
async def test_mcp_compile_tool_is_read_only_and_needs_catalog_read() -> None:
    compile_service, _, _ = await service()
    manifests = MissionManifestService(compiler=compile_service, request_scope=SCOPE)

    def principal(permissions: frozenset[str]) -> CoordinatorPrincipal:
        return CoordinatorPrincipal(
            actor_id="coordinator",
            tenant_scope=SCOPE,
            roles=frozenset(),
            permissions=permissions,
            request_scope=SCOPE,
        )

    class Principals:
        def __init__(self, permissions: frozenset[str]) -> None:
            self.permissions = permissions

        async def resolve(self, context: Any) -> CoordinatorPrincipal:
            del context
            return principal(self.permissions)

    for permissions, expect_ok in (
        (frozenset({"workflow_run.read", "catalog.read"}), True),
        (frozenset({"workflow_run.read"}), False),
    ):
        server = FastMCP("manifest-test")
        register_manifest_tools(
            server,
            ScopedManifests({SCOPE: manifests}),
            Principals(permissions),
            call=_principal_call,
        )
        async with Client(server) as client:
            tools = {tool.name: tool for tool in await client.list_tools()}
            assert tools[COMPILE_TOOL].annotations is not None
            assert tools[COMPILE_TOOL].annotations.readOnlyHint is True
            result = await client.call_tool(
                COMPILE_TOOL, {"manifest_yaml": MINIMAL.read_text(encoding="utf-8")}
            )
            envelope = result.structured_content
            assert envelope is not None
            assert envelope["ok"] is expect_ok
            if expect_ok:
                assert envelope["data"]["ok"] is True
            else:
                assert envelope["error"]["code"] == "FORBIDDEN"


@pytest.mark.asyncio
async def test_cli_compile_goes_through_the_service_and_offline_stays_structural(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    import httpx

    from mission_control.interfaces.cli import main as cli

    compile_service, _, _ = await service()
    manifests = MissionManifestService(compiler=compile_service, request_scope=SCOPE)
    app = http_app(manifests, READER)
    real_client = httpx.Client

    class Bridge(httpx.BaseTransport):
        def __init__(self) -> None:
            self._client = TestClient(app)

        def handle_request(self, request: httpx.Request) -> httpx.Response:
            response = self._client.request(
                request.method,
                str(request.url),
                headers=dict(request.headers),
                content=request.content,
            )
            return httpx.Response(
                response.status_code, headers=response.headers, content=response.content
            )

    def factory(*args: Any, **kwargs: Any) -> httpx.Client:
        kwargs["transport"] = Bridge()
        return real_client(*args, **kwargs)

    monkeypatch.setenv("MISSION_CONTROL_TOKEN", "token")
    monkeypatch.setenv("MISSION_CONTROL_APPLICATION_ID", "biotech")
    monkeypatch.setattr(cli.httpx, "Client", factory)
    assert await _run(cli.main, ["mission", "compile", str(MINIMAL), "--json"]) == 0
    printed = json.loads(capsys.readouterr().out)
    assert printed["report"]["resolution"]["catalog_resolution"] == "resolved"
    broken = minimal()
    broken["mission"]["application"] = "ai-engineer"
    path = tmp_path / "foreign.yml"
    path.write_text(text(broken), encoding="utf-8")
    assert await _run(cli.main, ["mission", "compile", str(path)]) == 2
    capsys.readouterr()
    assert await _run(cli.main, ["mission", "compile", str(MINIMAL), "--offline"]) == 0
    offline = json.loads(capsys.readouterr().out)
    assert offline["resolution"]["catalog_resolution"] == "deferred"


async def _run(function: Callable[[list[str]], int], argv: list[str]) -> int:
    """Run the synchronous CLI off the event loop (its HTTP bridge drives an ASGI app)."""

    import asyncio

    return await asyncio.to_thread(function, argv)
