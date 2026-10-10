"""The production manifest launch author seals ``mc.cursor_binding.v1`` for the Cursor lanes.

- The checked Cursor v2 examples (``docs/specs/multi-provider-2026-10/examples/cursor-*``)
  compile with the fast-track fixture catalog and author, per ``cursor_local`` /
  ``cursor_cloud`` stage or Goal Loop role, an exact sealed ``CursorExecutionBinding`` routed to
  the lane's task queue from ``providers.cursor_*`` of
  ``deployments/examples/manifest-launch-bindings.multi-provider.example.json``.
- The pinned projection digests are exactly those the Cursor harness re-renders at ``prepare``:
  the adapter's own ``RenderedProjectionSource`` over its own ``CatalogRows`` (both over the
  same fixture catalog) passes ``verify_projection`` for the sealed binding.
- mission/v1 Cursor nodes (Missions 2 and 3) launch through the same author; what the binding
  cannot carry is refused at the manifest pointer that selected it.

Offline: fixture catalog, in-memory compiler; no provider is contacted.
"""

from __future__ import annotations

import asyncio
import copy
import json
from pathlib import Path
from typing import Any

import pytest

from mission_control.adapters.cursor import bridge
from mission_control.adapters.cursor.projection import (
    MISSION_CONTEXT_POINTER,
    CatalogRows,
    RenderedProjectionSource,
    operating_contract,
    projection_digests,
    verify_projection,
)
from mission_control.application.agentic_components.projections import render_host_files
from mission_control.application.authoring.cursor_launch import (
    CURSOR_KERNEL_HOOKS,
    REQUIRED_CURSOR_PINS,
    cursor_operating_contract,
    cursor_projection_digests,
)
from mission_control.application.authoring.manifest_launch_inputs import (
    AdmittedLaunch,
    ManifestLaunchBindingError,
    ManifestLaunchBindings,
    provider_capability_pins,
)
from mission_control.application.authoring.provider_launch import (
    CatalogProjectionRows,
    ProviderLanes,
)
from mission_control.domain.agentic_components.projection import DEFAULT_KERNEL_HOOKS
from mission_control.domain.authoring.manifest import canonical_manifest_bytes, load_manifest_yaml
from mission_control.domain.authoring.mission_definition import MissionDefinition
from mission_control.domain.execution.contracts import (
    OperationExecutionRequest,
    OperationWorkflowRequest,
)
from mission_control.domain.execution.lanes import CursorExecutionBinding
from tests.fixtures.catalog.fast_track_catalog import fast_track_catalog
from tests.fixtures.mission_control_common_db import canonical_scope
from tests.fixtures.projections.rows import (
    hook_row,
    skill_row,
    tavily_row,
    verifier_row,
)
from tests.unit.authoring.test_manifest_v2_launch import (
    BINDINGS,
    EXAMPLES,
    SCOPE,
    V1_BINDINGS,
    bindings,
    compile_yaml,
    compiled_ok,
    edited,
    resolver,
    templates_of,
)

ROOT = Path(__file__).resolve().parents[3]
MISSIONS = ROOT / "docs/specs/fast-track-2026-10/missions"
LOCAL = EXAMPLES / "cursor-local.stage-graph.mission.yml"
CLOUD = EXAMPLES / "cursor-cloud.goal-loop.mission.yml"
PROFILE_OF = {LOCAL: "cursor_local", CLOUD: "cursor_cloud"}


def cursor_templates(path: Path, bound: ManifestLaunchBindings | None = None) -> dict[str, Any]:
    compilation = compiled_ok(path.read_text(encoding="utf-8"))
    templates: dict[str, Any] = {}
    for definition, program in zip(compilation.definitions, compilation.programs, strict=True):
        templates.update(templates_of(program, definition, bound))
    return templates


def _refused(text: str, bound: ManifestLaunchBindings | None = None, **rows: Any) -> Any:
    compilation = compiled_ok(text)
    (definition,), (program,) = compilation.definitions, compilation.programs
    admitted = AdmittedLaunch.from_configuration(SCOPE, "run-cursor", program.configuration)
    target = resolver(bound)
    with pytest.raises(ManifestLaunchBindingError) as raised:
        if program.lowered.family == "StageGraph":
            target.stage_templates(definition, admitted, rows=rows.get("rows"))
        else:
            target.goal_templates(definition, admitted, rows=rows.get("rows"))
    return raised.value


def _without_lane(profile: str, **changes: Any) -> ManifestLaunchBindings:
    bound = bindings()
    assert bound.providers is not None
    lane = getattr(bound.providers, profile)
    return bound.model_copy(
        update={
            "providers": bound.providers.model_copy(
                update={
                    profile: type(lane).model_validate(
                        {**lane.model_dump(mode="python"), **changes}
                    )
                }
            )
        }
    )


# --- The checked examples: sealed, routed, exact -------------------------------------------------


@pytest.mark.parametrize("path", [LOCAL, CLOUD], ids=lambda item: item.name)
def test_each_cursor_example_authors_sealed_cursor_bindings(path: Path) -> None:
    profile = PROFILE_OF[path]
    providers = bindings().providers
    assert providers is not None
    lane = providers.cursor(profile)
    assert lane is not None
    cursor_units = 0
    for key, template in cursor_templates(path).items():
        if template.execution_runtime == "deep_agent":
            assert template.cursor_binding is None and template.deep_agent_binding is not None
            continue
        cursor_units += 1
        binding = template.cursor_binding
        assert isinstance(binding, CursorExecutionBinding), key
        assert template.execution_runtime == "cursor" and template.lane_profile == profile
        assert template.provider_binding is None and template.deep_agent_binding is None
        assert template.native_placement is None and template.secret_refs == ()
        # Sealed: the digest is the content's, and a re-validation from the stored JSON holds.
        assert binding.binding_digest == binding.computed_digest()
        stored = OperationExecutionRequest.model_validate(template.model_dump(mode="json"))
        assert stored.cursor_binding == binding
        assert binding.lane_profile == profile and binding.pins == REQUIRED_CURSOR_PINS
        assert binding.model_id == lane.model_profiles["cursor.default"].model_id
        assert template.model_policy.provider == "cursor"
        assert template.model_policy.model == binding.model_id
        assert binding.task_queue == lane.task_queue
        workflow = OperationWorkflowRequest(
            semantic_attempt_id=template.identity.semantic_key,
            operation_kind="bound_operation",
            operation=template,
        )
        assert workflow.activity_task_queue == lane.task_queue, key
        assert binding.workspace.base_ref == "main" and binding.workspace.branch_prefix == "mc/"
        if profile == "cursor_local":
            assert binding.workspace.repo_url == "/srv/repos/mission-control-fixture"
            assert binding.hook_callback is not None and binding.cloud is None
            # Narrowed by the node: wall_clock 1h < 2h; continuation.max_session_turns 12 < 16.
            assert (binding.budgets.wall_clock_s, binding.budgets.max_turns) == (3600, 12)
        else:
            assert (
                binding.workspace.repo_url == "https://github.com/Overton77/mission-control-fixture"
            )
            assert binding.cloud == lane.environments["cursor-cloud.default"].options  # type: ignore[union-attr]
            assert binding.hook_callback is None
            assert binding.budgets == lane.budgets
        # No capability selected: the Kernel Hooks (local only) and the operating contract.
        rendered = render_host_files(
            (), profile, operating_contract(template), None, CURSOR_KERNEL_HOOKS[profile]
        )
        verify_projection(binding, projection_digests(rendered))
    assert cursor_units >= 2


def test_templates_are_deterministic() -> None:
    assert cursor_templates(LOCAL) == cursor_templates(LOCAL)
    assert cursor_templates(CLOUD) == cursor_templates(CLOUD)


# --- The digest the harness recomputes at `prepare` ----------------------------------------------


def _with_catalog_capabilities(document: dict[str, Any]) -> None:
    """Mission-wide Cursor capabilities (the Deep Agents node, which would inherit them
    without a Deep Agents component binding, is dropped)."""

    program = document["mission"]["program"]
    if program["behavior"] == "stage_graph":
        program["nodes"] = [node for node in program["nodes"] if node["key"] != "summarize"]
    environment = document["mission"]["environment"]
    environment["capabilities"] = [
        {"search": "tavily web search extract", "kind": "mcp_server", "as": "tavily"}
    ]
    environment["agents"] = [
        {"search": "literature verifier subagent", "kind": "subagent_profile", "as": "checker"}
    ]


@pytest.mark.parametrize("path", [LOCAL, CLOUD], ids=lambda item: item.name)
def test_the_sealed_projection_is_what_the_harness_renders_at_prepare(path: Path) -> None:
    """Author side: `CatalogProjectionRows` (the composed resolver); worker side: the adapter's
    `RenderedProjectionSource(CatalogRows(...))` with the composed Kernel Hooks per profile.
    Both read the same catalog; `verify_projection` accepts the sealed binding."""

    profile = PROFILE_OF[path]
    compilation = compiled_ok(edited(path, _with_catalog_capabilities))
    definitions, _search = asyncio.run(fast_track_catalog())
    for definition, program in zip(compilation.definitions, compilation.programs, strict=True):
        pins = provider_capability_pins(definition)
        assert len(pins) == 2, pins
        rows = asyncio.run(CatalogProjectionRows(definitions)(pins))
        admitted = AdmittedLaunch.from_configuration(SCOPE, "run-cursor", program.configuration)
        target = resolver()
        templates = (
            target.stage_templates(definition, admitted, rows=rows)
            if program.lowered.family == "StageGraph"
            else target.goal_templates(definition, admitted, rows=rows)
        )
        sealed = [item for item in templates.values() if item.cursor_binding is not None]
        assert sealed
        for template in sealed:
            binding = template.cursor_binding
            assert binding is not None
            (mcp,) = binding.projections.mcp_servers
            (agent,) = binding.inline_subagents
            assert mcp.startswith("mcp.tavily@") and agent.startswith("subagent.")
            assert binding.projections.skills == ()
            worker = RenderedProjectionSource(
                CatalogRows(definitions), kernel_hooks=CURSOR_KERNEL_HOOKS[profile]
            )
            projection = asyncio.run(worker.project(template, profile=profile, packet_index=None))
            verify_projection(binding, projection_digests(projection))
            assert any(item.path.startswith(".cursor/agents/") for item in projection.files)
            # A binding sealed without the capabilities differs: the digests pin the rows.
            empty = render_host_files(
                (), profile, operating_contract(template), None, CURSOR_KERNEL_HOOKS[profile]
            )
            assert projection_digests(empty)["agents_digest"] != binding.projections.agents_digest


def test_the_application_digests_are_the_adapters() -> None:
    rows = (skill_row(), tavily_row(), verifier_row())
    for profile in ("cursor_local", "cursor_cloud"):
        for kernel in (DEFAULT_KERNEL_HOOKS, ()):
            projection = render_host_files(rows, profile, "contract", None, kernel)
            assert cursor_projection_digests(projection) == projection_digests(projection)
    template = cursor_templates(LOCAL)["diagnose/execute/default"]
    assert cursor_operating_contract(template.prompt_segments) == operating_contract(template)
    assert MISSION_CONTEXT_POINTER in operating_contract(template)
    # The composed harnesses: Kernel Hooks on cursor_local, none on cursor_cloud.
    assert CURSOR_KERNEL_HOOKS == {"cursor_local": DEFAULT_KERNEL_HOOKS, "cursor_cloud": ()}
    # The pins are the composed adapter's versions.
    assert REQUIRED_CURSOR_PINS.cursor_sdk == bridge.CURSOR_SDK_PIN
    assert REQUIRED_CURSOR_PINS.bridge == bridge.CURSOR_SDK_PIN
    assert REQUIRED_CURSOR_PINS.protocol == bridge.BRIDGE_PROTOCOL
    assert REQUIRED_CURSOR_PINS.cloud_api == "v1"


# --- mission/v1: Missions 2 and 3 ---------------------------------------------------------------


def _mission(name: str, application: str, change: Any) -> str:
    document = load_manifest_yaml((MISSIONS / name).read_text(encoding="utf-8"))
    change(document)
    return canonical_manifest_bytes(document).decode("utf-8")


def _v1_cloud_bound() -> ManifestLaunchBindings:
    bound = bindings()
    assert bound.providers is not None and bound.providers.cursor_cloud is not None
    repositories = {
        **bound.providers.cursor_cloud.repositories,
        "https://github.com/Overton77/biotech-research-notes": {
            "repo_url": "https://github.com/Overton77/biotech-research-notes",
            "base_refs": ["main"],
        },
    }
    lane = type(bound.providers.cursor_cloud).model_validate(
        {**bound.providers.cursor_cloud.model_dump(mode="python"), "repositories": repositories}
    )
    return bound.model_copy(
        update={"providers": bound.providers.model_copy(update={"cursor_cloud": lane})}
    )


def _mission_two_research(document: dict[str, Any]) -> None:
    """FIXTURE transform: Mission 2's research member alone, in the test scope's application,
    without its two Skills (their bundle bytes need custody, which this offline test has not;
    MCP servers and the subagent profile resolve from the fixture catalog as composed). Its Deep
    Agents verifier would inherit the mission's MCP servers, for which the Deep Agents deployment
    binds no component; the independent verifier stays on the mission's Cursor lane here."""

    (research,) = [item for item in document["missions"] if item["key"] == "research"]
    research["application"] = "ai-engineer"
    research["environment"]["workspace"].pop("skills")
    research["program"]["verifier"].pop("environment")
    document.pop("missions")
    document.pop("links")
    document["mission"] = research


def test_mission_two_research_launches_on_cursor_cloud_from_a_v1_manifest() -> None:
    """mission/v1 has no selections: the deployment's reviewed `v1_environment` applies and
    the worker's Cursor credential is the binding's. Mission 2's hook script has no slot in
    `mc.cursor_binding.v1`: it is refused at its pointer (never silently dropped)."""

    text = _mission(
        "02-research-ingestion-cursor-cloud-chain.yml", "ai-engineer", _mission_two_research
    )
    compilation = compiled_ok(text)
    (definition,) = compilation.definitions
    assert definition.program.selections is None
    pins = provider_capability_pins(definition)
    assert not any(item.startswith("hook.") for item in pins)
    definitions, _search = asyncio.run(fast_track_catalog())
    rows = asyncio.run(CatalogProjectionRows(definitions)(pins))
    refused = _refused(text, _v1_cloud_bound(), rows=rows)
    assert refused.pointer == "/mission/environment/hooks/0"
    assert "hook_script has no cursor_cloud launch binding" in str(refused)

    def without_hooks(document: dict[str, Any]) -> None:
        _mission_two_research(document)
        document["mission"]["environment"].pop("hooks")

    clean = _mission("02-research-ingestion-cursor-cloud-chain.yml", "ai-engineer", without_hooks)
    compilation = compiled_ok(clean)
    (definition,), (program,) = compilation.definitions, compilation.programs
    admitted = AdmittedLaunch.from_configuration(SCOPE, "run-v1", program.configuration)
    templates = resolver(_v1_cloud_bound()).goal_templates(definition, admitted, rows=rows)
    executor = templates["executor"].cursor_binding
    assert executor is not None and executor.lane_profile == "cursor_cloud"
    assert executor.workspace.repo_url == "https://github.com/Overton77/biotech-research-notes"
    assert len(executor.projections.mcp_servers) == 2 and executor.projections.skills == ()
    assert len(executor.inline_subagents) == 1
    # The worker renders the same rows (`CatalogRows`, no Kernel Hooks on cursor_cloud).
    worker = RenderedProjectionSource(CatalogRows(definitions), kernel_hooks=())
    projection = asyncio.run(
        worker.project(templates["executor"], profile="cursor_cloud", packet_index=None)
    )
    verify_projection(executor, projection_digests(projection))
    # The independent verifier is its own sealed unit on the same lane and admission.
    verifier = templates["verifier"].cursor_binding
    assert verifier is not None and verifier.projections == executor.projections


def test_a_v1_cloud_node_without_a_reviewed_v1_environment_is_refused() -> None:
    bound = _v1_cloud_bound()
    assert bound.providers is not None and bound.providers.cursor_cloud is not None
    lane = type(bound.providers.cursor_cloud).model_validate(
        {**bound.providers.cursor_cloud.model_dump(mode="python"), "v1_environment": None}
    )
    unbound = bound.model_copy(
        update={"providers": bound.providers.model_copy(update={"cursor_cloud": lane})}
    )
    text = _mission(
        "02-research-ingestion-cursor-cloud-chain.yml", "ai-engineer", _mission_two_research
    )
    refused = _refused(text, unbound)
    assert refused.pointer == "/mission/environment/lane"
    assert "v1_environment" in str(refused)


def test_mission_three_is_refused_at_its_first_unbindable_selection() -> None:
    """Mission 3 (cursor_local, v1): with its repository bound, its deterministic executors are
    refused first (a Cursor binding projects skills, MCP servers and subagents only)."""

    path = MISSIONS / "03-codebase-feature-cursor-local.yml"
    text = path.read_text(encoding="utf-8")
    compilation = compile_yaml(text, canonical_scope("tenant-1", "ai-engineer"))
    assert compilation.ok
    refused = _refused_in(compilation, None)
    assert refused.pointer == "/mission/environment/workspace/repo"
    bound = _without_lane(
        "cursor_local",
        repositories={
            r"C:\Users\Pinda\Proyectos\Biotech\mission-control": {
                "repo_url": "/srv/repos/mission-control",
                "base_refs": ("main",),
            }
        },
    )
    refused = _refused_in(compilation, bound)
    assert refused.pointer == "/mission/environment/capabilities/0"
    assert "deterministic_executor has no cursor_local launch binding" in str(refused)


def _refused_in(compilation: Any, bound: ManifestLaunchBindings | None) -> Any:
    (definition,), (program,) = compilation.definitions, compilation.programs
    admitted = AdmittedLaunch.from_configuration(
        canonical_scope("tenant-1", "ai-engineer"), "run-v1", program.configuration
    )
    with pytest.raises(ManifestLaunchBindingError) as raised:
        resolver(bound).goal_templates(definition, admitted)
    return raised.value


# --- Refusals name the manifest pointer ----------------------------------------------------------


def test_a_bindings_file_without_cursor_entries_refuses_at_the_lane() -> None:
    for path in (LOCAL, CLOUD):
        for bound in (
            ManifestLaunchBindings.load(V1_BINDINGS),
            bindings().model_copy(update={"providers": ProviderLanes()}),
        ):
            refused = _refused(path.read_text(encoding="utf-8"), bound)
            assert refused.pointer == "/mission/environment/lane"
            assert f"providers.{PROFILE_OF[path]}" in str(refused)


@pytest.mark.parametrize(
    ("path", "section", "pointer"),
    [
        (LOCAL, "model_profiles", "/mission/environment/model/profile"),
        (LOCAL, "auth_profiles", "/mission/environment/auth/profile"),
        (LOCAL, "host_profiles", "/mission/environment/execution_environment/profile"),
        (LOCAL, "repositories", "/mission/environment/workspace/repo"),
        (CLOUD, "model_profiles", "/mission/environment/model/profile"),
        (CLOUD, "auth_profiles", "/mission/environment/auth/profile"),
        (CLOUD, "repositories", "/mission/environment/workspace/repo"),
    ],
)
def test_an_unbound_selection_fails_at_its_manifest_pointer(
    path: Path, section: str, pointer: str
) -> None:
    refused = _refused(path.read_text("utf-8"), _without_lane(PROFILE_OF[path], **{section: {}}))
    assert refused.pointer == pointer, refused


def test_an_unbound_cloud_environment_or_revision_fails_at_its_pointer() -> None:
    bound = bindings()
    assert bound.providers is not None and bound.providers.cursor_cloud is not None
    lane = bound.providers.cursor_cloud
    unbound = _without_lane("cursor_cloud", environments={}, v1_environment=None)
    refused = _refused(CLOUD.read_text("utf-8"), unbound)
    assert refused.pointer == "/mission/environment/execution_environment/environment_ref"

    def pinned(document: dict[str, Any]) -> None:
        execution = document["mission"]["environment"]["execution_environment"]
        execution["expected_revision"] = "sha256:" + "1" * 64

    refused = _refused(edited(CLOUD, pinned))
    assert refused.pointer == "/mission/environment/execution_environment/expected_revision"
    assert lane.environments["cursor-cloud.default"].environment.expected_revision in str(refused)


def test_a_disallowed_ref_and_an_unmapped_model_setting_fail_at_their_pointers() -> None:
    def feature_branch(document: dict[str, Any]) -> None:
        document["mission"]["environment"]["workspace"]["repo"]["ref"] = "feature/x"

    refused = _refused(edited(LOCAL, feature_branch))
    assert refused.pointer == "/mission/environment/workspace/repo/ref"

    def effort(document: dict[str, Any]) -> None:
        document["mission"]["environment"]["model"]["settings"] = {"reasoning": {"effort": "high"}}

    refused = _refused(edited(CLOUD, effort))
    assert refused.pointer == "/mission/environment/model/settings/reasoning/effort"


def test_a_selected_capability_needs_its_projection_row() -> None:
    refused = _refused(edited(LOCAL, _with_catalog_capabilities))
    assert refused.pointer == "/mission/environment/capabilities/0"
    assert "no projection row" in str(refused)


def test_a_hook_row_is_refused_even_when_a_resolver_returns_it() -> None:
    """A capability selected without a declared kind is classified by its resolved row."""

    compilation = compiled_ok(edited(LOCAL, _with_catalog_capabilities))
    (definition,) = compilation.definitions
    pins = provider_capability_pins(definition)
    rows = {pins[0]: hook_row(), pins[1]: verifier_row()}
    # Strip the declared kinds: only the row can classify the selection now.
    stripped = MissionDefinition.model_validate(
        {
            **definition.model_dump(mode="json"),
            "capabilities": [
                {**item, "kind": None}
                for item in definition.model_dump(mode="json")["capabilities"]
            ],
        }
    )
    (program,) = compilation.programs
    admitted = AdmittedLaunch.from_configuration(SCOPE, "run-cursor", program.configuration)
    with pytest.raises(ManifestLaunchBindingError) as raised:
        resolver().stage_templates(stripped, admitted, rows=rows)
    assert raised.value.pointer == "/mission/environment/capabilities/0"
    assert "hook_script has no cursor_local launch binding" in str(raised.value)


# --- The deployment file -------------------------------------------------------------------------


def test_the_bindings_file_pins_the_composed_cursor_adapter_additively() -> None:
    document = json.loads(BINDINGS.read_text(encoding="utf-8"))
    loaded = ManifestLaunchBindings.model_validate(document)
    assert loaded.providers is not None
    assert loaded.providers.cursor("cursor_local") is not None
    assert (
        loaded.providers.cursor("codex") is None and loaded.providers.lane("cursor_local") is None
    )

    wrong_pin = copy.deepcopy(document)
    wrong_pin["providers"]["cursor_local"]["pins"]["cursor_sdk"] = "1.0.36"
    with pytest.raises(ValueError, match="1.0.37"):
        ManifestLaunchBindings.model_validate(wrong_pin)

    no_callback = copy.deepcopy(document)
    no_callback["providers"]["cursor_local"].pop("hook_callback")
    with pytest.raises(ValueError):
        ManifestLaunchBindings.model_validate(no_callback)

    misnamed = copy.deepcopy(document)
    environments = misnamed["providers"]["cursor_cloud"]["environments"]
    environments["other"] = environments.pop("cursor-cloud.default")
    with pytest.raises(ValueError):
        ManifestLaunchBindings.model_validate(misnamed)

    swapped = copy.deepcopy(document)
    swapped["providers"]["cursor_local"] = document["providers"]["codex"]
    with pytest.raises(ValueError):
        ManifestLaunchBindings.model_validate(swapped)

    # Additive: a v2 file without Cursor entries dumps without them; a v1 file is unchanged.
    without = copy.deepcopy(document)
    for profile in ("cursor_local", "cursor_cloud"):
        without["providers"].pop(profile)
    dumped = ManifestLaunchBindings.model_validate(without).model_dump(mode="json")
    assert set(dumped["providers"]) == {"claude_agent_sdk", "codex"}
    v1 = ManifestLaunchBindings.load(V1_BINDINGS)
    assert v1.providers is None and "providers" not in v1.model_dump(mode="json")
    assert ManifestLaunchBindings.model_validate(loaded.model_dump(mode="json")) == loaded
