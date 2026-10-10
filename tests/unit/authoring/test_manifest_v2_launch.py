"""MP-02 for mission/v2: compile dispatch, v2 lowering, V01 admission and provider launch.

- ``mission/v1`` takes the exact v1 parser and lowering (no ``selections`` in any dump);
  ``mission/v2`` documents compile through the same definitions with additive selections.
- Every checked v2 example under ``docs/specs/multi-provider-2026-10/examples/`` compiles with the
  fast-track fixture catalog and authors its launch templates against
  ``deployments/examples/manifest-launch-bindings.multi-provider.example.json``: claude/codex
  stages and Goal Loop roles carry a sealed ``mc.execution_binding.v2`` routed to their lane
  queue, Deep Agents nodes keep their Deep Agent binding.
- V01: required features a lane's declared matrix cannot provide are pointed compile blockers;
  hosted profiles compile structurally and their launch is refused.
- Missing deployment bindings fail at the manifest pointer that selected them.

Everything here is offline (fixture catalog, in-memory compiler); no provider is contacted.
"""

from __future__ import annotations

import asyncio
import copy
import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from mission_control.adapters.cursor.projection import operating_contract
from mission_control.adapters.storage.control_plane_payloads import InMemoryPayloadStore
from mission_control.application.agentic_components.materialization import projection_digest
from mission_control.application.agentic_components.projections import render_host_files
from mission_control.application.authoring.manifest_launch_inputs import (
    AdmittedLaunch,
    ManifestLaunchBindingError,
    ManifestLaunchBindings,
    ManifestLaunchInputAuthor,
    ManifestLaunchResolver,
    ServedComponents,
    provider_capability_pins,
)
from mission_control.application.authoring.manifest_service import (
    CompiledMission,
    ManifestCompilation,
    ManifestCompileService,
    ManifestProgramCompiler,
    ManifestScope,
)
from mission_control.application.authoring.provider_launch import (
    ProviderBindingRows,
    ProviderLanes,
    binding_capability_pins,
    materialization_digest,
    operating_contract_text,
)
from mission_control.application.chains.service import ManifestStructureService
from mission_control.application.execution.harness.describe import declared_matrix
from mission_control.domain.authoring.extensions import ExtensionRegistry
from mission_control.domain.authoring.manifest import (
    MissionManifest,
    canonical_manifest_bytes,
    load_manifest_yaml,
    parse_manifest_yaml,
    resolve_environments,
)
from mission_control.domain.authoring.manifest_v2 import (
    MissionManifestV2,
    parse_manifest_any,
    parse_manifest_yaml_versioned,
)
from mission_control.domain.authoring.mission_definition import (
    MissionDefinition,
    manifest_to_definitions,
)
from mission_control.domain.execution.contracts import OperationWorkflowRequest
from mission_control.domain.execution.lane_requirements import RequirementSet, admit_requirements
from tests.fixtures.catalog.fast_track_catalog import fast_track_catalog
from tests.fixtures.mission_control_common_db import canonical_scope

ROOT = Path(__file__).resolve().parents[3]
EXAMPLES = ROOT / "docs/specs/multi-provider-2026-10/examples"
BINDINGS = ROOT / "deployments/examples/manifest-launch-bindings.multi-provider.example.json"
V1_BINDINGS = ROOT / "deployments/examples/manifest-launch-bindings.deep-agents.example.json"
MINIMAL = ROOT / "tests/fixtures/manifests/minimal-stage-graph.yml"
SCOPE = canonical_scope("tenant-1", "ai-engineer")
AT = datetime(2026, 10, 9, tzinfo=UTC)
PROFILES = ("claude_agent_sdk", "codex")
FORMS = ("stage-graph", "goal-loop", "chain")


def example(profile: str, form: str) -> Path:
    return EXAMPLES / f"{profile.replace('_', '-')}.{form}.mission.yml"


async def _compile(manifest_yaml: str, scope: str = SCOPE) -> ManifestCompilation:
    definitions, search = await fast_track_catalog()
    service = ManifestCompileService(
        definitions=definitions,
        search=search,
        programs=ManifestProgramCompiler(definitions, ExtensionRegistry(), InMemoryPayloadStore()),
    )
    return await service.compile(
        manifest_yaml, ManifestScope(request_scope=scope, actor_id="owner", at=AT)
    )


def compile_yaml(manifest_yaml: str, scope: str = SCOPE) -> ManifestCompilation:
    return asyncio.run(_compile(manifest_yaml, scope))


def compiled_ok(manifest_yaml: str) -> ManifestCompilation:
    compilation = compile_yaml(manifest_yaml)
    assert compilation.ok, [(item.pointer, item.message) for item in compilation.report.blockers]
    return compilation


def bindings() -> ManifestLaunchBindings:
    return ManifestLaunchBindings.load(BINDINGS)


def served(bound: ManifestLaunchBindings) -> ServedComponents:
    """FIXTURE: the example file's Deep Agent components as a test deployment serves them."""

    profile = bound.deep_agents.profile
    return ServedComponents(
        models=frozenset(item.ref.digest for item in bound.model_profiles.values()),
        sandboxes=frozenset(item.ref.digest for item in bound.sandbox_profiles.values()),
        checkpointers=frozenset({profile.checkpointer_ref.digest}),
        stores=frozenset({profile.store_ref.digest}),
    )


def resolver(bound: ManifestLaunchBindings | None = None) -> ManifestLaunchResolver:
    bound = bound or bindings()
    return ManifestLaunchResolver(
        bound, served(bound), provider_secret_env={"openai": "OPENAI_API_KEY"}
    )


def templates_of(
    program: CompiledMission, definition: MissionDefinition, bound: ManifestLaunchBindings | None
) -> dict[str, Any]:
    admitted = AdmittedLaunch.from_configuration(SCOPE, "run-v2", program.configuration)
    target = resolver(bound)
    if program.lowered.family == "StageGraph":
        return dict(target.stage_templates(definition, admitted))
    return dict(target.goal_templates(definition, admitted))


def edited(path: Path, change: Any) -> str:
    document = load_manifest_yaml(path.read_text(encoding="utf-8"))
    change(document)
    return canonical_manifest_bytes(document).decode("utf-8")


def blockers(compilation: ManifestCompilation) -> dict[str, str]:
    return {item.pointer: str(item.reason) for item in compilation.report.blockers}


# --- Compile dispatch: v1 exactly unchanged ----


def test_v1_documents_take_the_exact_v1_parser() -> None:
    text = MINIMAL.read_text(encoding="utf-8")
    manifest, document = parse_manifest_yaml_versioned(text)
    reference, reference_document = parse_manifest_yaml(text)
    assert type(manifest) is MissionManifest and manifest == reference
    assert document == reference_document
    definitions = manifest_to_definitions(manifest, document)
    assert all("selections" not in item.model_dump_json() for item in definitions)
    # An unknown version is rejected exactly as the v1-only parser rejects it.
    unknown = text.replace("manifest: mission/v1", "manifest: mission/v9")
    with pytest.raises(Exception) as versioned:
        parse_manifest_yaml_versioned(unknown)
    with pytest.raises(Exception) as original:
        parse_manifest_yaml(unknown)
    assert getattr(versioned.value, "issues", None) == getattr(original.value, "issues", None)


def test_v1_compile_reports_are_byte_identical_through_the_dispatching_compile() -> None:
    """The structure service gives v1 the v1 definition digest; no compiled v1 definition grows
    a selections block (the resolved digests themselves are pinned by the golden
    resolutions in ``test_manifest_compile.py``)."""

    text = MINIMAL.read_text(encoding="utf-8")
    report = ManifestStructureService().compile(text)
    manifest, document = parse_manifest_yaml(text)
    (expected,) = manifest_to_definitions(manifest, document)
    assert report.ok and report.definitions[0].definition_digest == expected.digest
    compilation = compiled_ok_scope(text)
    assert "selections" not in compilation.definitions[0].model_dump_json()
    assert compilation.definitions[0].program.selections is None


def compiled_ok_scope(text: str) -> ManifestCompilation:
    compilation = compile_yaml(text, canonical_scope("tenant-1"))
    assert compilation.ok, [(item.pointer, item.message) for item in compilation.report.blockers]
    return compilation


# --- The checked examples ----


@pytest.mark.parametrize("profile", PROFILES)
@pytest.mark.parametrize("form", FORMS)
def test_every_v2_example_compiles_and_lowers_with_selections(profile: str, form: str) -> None:
    text = example(profile, form).read_text(encoding="utf-8")
    manifest = parse_manifest_any(load_manifest_yaml(text))
    assert isinstance(manifest, MissionManifestV2)
    compilation = compiled_ok(text)
    families = [program.lowered.family for program in compilation.programs]
    expected = {
        "stage-graph": ["StageGraph"],
        "goal-loop": ["GoalDirected"],
        "chain": ["GoalDirected", "StageGraph"],
    }[form]
    assert families == expected
    for definition in compilation.definitions:
        stored = MissionDefinition.model_validate(definition.model_dump(mode="json"))
        # The committed form (JSON) keeps the digest and the v2 selections.
        assert stored.digest == definition.digest
        assert stored.program.selections == definition.program.selections
        selections = definition.program.selections
        assert selections is not None and selections.auth is not None
        assert selections.execution_environment is not None
        assert selections.execution_environment.kind == "local_workspace"
    if form == "goal-loop":
        (definition,) = compilation.definitions
        assert definition.program.environment.governors is not None
        assert (definition.program.environment.governors.iterations or 0) >= 2
        assert compilation.programs[0].lowered.blueprint.max_iterations >= 2  # type: ignore[union-attr]


@pytest.mark.parametrize("profile", PROFILES)
@pytest.mark.parametrize("form", FORMS)
def test_every_v2_example_authors_sealed_provider_bindings(profile: str, form: str) -> None:
    compilation = compiled_ok(example(profile, form).read_text(encoding="utf-8"))
    lane = bindings().providers.lane(profile)  # type: ignore[union-attr]
    assert lane is not None
    provider_templates = 0
    for definition, program in zip(compilation.definitions, compilation.programs, strict=True):
        templates = templates_of(program, definition, None)
        assert templates
        for key, template in templates.items():
            binding = template.provider_binding
            if template.execution_runtime == "deep_agent":
                assert binding is None and template.deep_agent_binding is not None
                continue
            provider_templates += 1
            assert binding is not None and binding.lane_profile == profile
            assert template.lane_profile == profile
            assert template.execution_runtime == ("claude" if profile != "codex" else "codex")
            assert template.native_placement is None and template.deep_agent_binding is None
            assert binding.binding_digest == binding.computed_digest()
            assert binding.task_queue == lane.task_queue
            assert binding.pins == lane.pins
            assert binding.requirements.describe_digest == declared_matrix(profile).digest
            assert binding.workspace_policy.mode == "managed_worktree"
            assert binding.repo_url == "https://github.com/Overton77/mission-control-fixture"
            assert binding.auth.profile in lane.auth_profiles
            assert binding.environment.host_profile in lane.host_profiles
            assert template.secret_refs == ()
            # The lane recomputes exactly this projection digest at `prepare` (no catalog
            # capability selected: the Kernel Hooks and the operating contract only).
            assert binding.materialization_digest == projection_digest(
                render_host_files((), profile, operating_contract(template), None)
            )
            workflow = OperationWorkflowRequest(
                semantic_attempt_id=template.identity.semantic_key,
                operation_kind="bound_operation",
                operation=template,
            )
            assert workflow.activity_task_queue == lane.task_queue, key
    assert provider_templates >= 1


def test_codex_reasoning_effort_maps_to_the_typed_provider_option() -> None:
    compilation = compiled_ok(example("codex", "stage-graph").read_text(encoding="utf-8"))
    templates = templates_of(compilation.programs[0], compilation.definitions[0], None)
    patch = templates["patch/execute/default"]
    assert patch.provider_binding is not None
    options = patch.provider_binding.provider_options
    assert getattr(options, "reasoning_effort", None) == "high"
    assert patch.model_policy.reasoning_effort == "high"
    assert patch.provider_binding.requirements.controls == (
        "cancel",
        "queue_instruction",
        "request_continuation",
    )
    # The Deep Agents node of the same mission keeps its own lane binding.
    summarize = templates["summarize/execute/default"]
    assert summarize.deep_agent_binding is not None and summarize.provider_binding is None


def test_session_budgets_are_narrowed_by_the_node_never_raised() -> None:
    compilation = compiled_ok(example("claude_agent_sdk", "stage-graph").read_text("utf-8"))
    templates = templates_of(compilation.programs[0], compilation.definitions[0], None)
    patch = templates["patch/execute/default"].provider_binding
    diagnose = templates["diagnose/execute/default"].provider_binding
    assert patch is not None and diagnose is not None
    assert patch.budgets.token_ceiling == 200_000  # node budget below the deployment ceiling
    assert diagnose.budgets.token_ceiling == 400_000
    assert patch.budgets.max_turns == 12  # continuation.max_session_turns < deployment 16
    assert patch.budgets.wall_clock_s == 7_200


def test_templates_are_deterministic_and_reused_by_the_author() -> None:
    compilation = compiled_ok(example("claude_agent_sdk", "goal-loop").read_text("utf-8"))
    (definition,), (program,) = compilation.definitions, compilation.programs
    assert templates_of(program, definition, None) == templates_of(program, definition, None)

    stored: dict[str, Any] = {}
    persisted: list[int] = []

    class Stores:
        async def persist_templates(self, **values: Any) -> None:
            persisted.append(1)
            stored.update({"executor": values["executor"], "verifier": values["verifier"]})

        async def get_template(self, *, operation_role: str, **_: Any) -> Any:
            if operation_role not in stored:
                raise ValueError("unavailable")
            return stored[operation_role]

        async def list_templates(self, **_: Any) -> dict[str, Any]:
            return {}

    class Runs:
        async def get_run(self, _scope: str, _run: str) -> Any:
            return SimpleNamespace(effective_configuration_digest=program.configuration.digest)

    class Plane:
        async def retrieve_for_admission(self, _digest: str) -> Any:
            return program.configuration

    class Definitions:
        async def run_subscriptions(self, _scope: str, _run: str) -> dict[str, Any]:
            return {"definition": definition.model_dump(mode="json")}

    def author(bound: ManifestLaunchBindings) -> ManifestLaunchInputAuthor:
        return ManifestLaunchInputAuthor(
            resolver=resolver(bound),
            run_control=Runs(),  # type: ignore[arg-type]
            control_plane=Plane(),  # type: ignore[arg-type]
            definitions=Definitions(),
            stage_templates=Stores(),  # type: ignore[arg-type]
            goal_templates=Stores(),  # type: ignore[arg-type]
        )

    first = asyncio.run(author(bindings()).bind(SCOPE, "run-v2", family="GoalDirected"))
    # The deployment later drops the claude binding: a frozen run is never re-authored.
    unbound = bindings().model_copy(update={"providers": ProviderLanes()})
    second = asyncio.run(author(unbound).bind(SCOPE, "run-v2", family="GoalDirected"))
    assert first == second and persisted == [1]
    assert first["executor"].provider_binding is not None
    assert first["verifier"].provider_binding is not None


# --- V01: pointed compile blockers ----


def test_an_unsupported_required_approval_is_a_pointed_blocker() -> None:
    def require_elicitation(document: dict[str, Any]) -> None:
        node = document["mission"]["program"]["nodes"][1]
        node["environment"]["requires"]["approvals"] = ["workflow_gate", "mcp_elicitation"]

    compilation = compile_yaml(
        edited(example("claude_agent_sdk", "stage-graph"), require_elicitation)
    )
    found = blockers(compilation)
    pointer = "/mission/program/nodes/1/environment/requires/approvals/1"
    assert found.get(pointer) == "LANE_REQUIREMENT_UNSUPPORTED", found
    assert any(item.reason == "ELICITATION_NOT_FORWARDED" for item in compilation.report.blockers)
    assert not compilation.programs  # nothing lowered, nothing compiled, no provider work


def test_an_inherited_refused_control_points_at_the_mission_environment() -> None:
    """The code comes from the declared matrix (never hard-coded here): `hard_pause` has no
    delivery on the claude lane, so it is refused at the mission entry that required it."""

    describe = declared_matrix("claude_agent_sdk")
    (expected,) = admit_requirements(describe, RequirementSet(controls=("hard_pause",)))

    def require_hard_pause(document: dict[str, Any]) -> None:
        document["mission"]["environment"]["requires"]["controls"] = ["cancel", "hard_pause"]

    compilation = compile_yaml(edited(example("claude_agent_sdk", "goal-loop"), require_hard_pause))
    assert blockers(compilation) == {"/mission/environment/requires/controls/1": expected.code}


def test_all_writes_gated_needs_enforcement_on_every_write_family() -> None:
    def gate_all_writes(document: dict[str, Any]) -> None:
        document["mission"]["environment"]["requires"]["all_writes_gated"] = True

    compilation = compile_yaml(edited(example("codex", "goal-loop"), gate_all_writes))
    reasons = [
        (item.pointer, item.reason)
        for item in compilation.report.blockers
        if item.reason == "GATE_COVERAGE_UNENFORCEABLE"
    ]
    # Codex enforces shell and file natively and MCP emulated; network has no evidence.
    assert reasons == [
        ("/mission/environment/requires/all_writes_gated", "GATE_COVERAGE_UNENFORCEABLE")
    ]
    assert "network" in compilation.report.blockers[0].message


def test_switching_lane_revalidates_the_inherited_requirements() -> None:
    """A Deep Agents node inheriting a provider-permission requirement is refused: a child
    cannot drop the approval and the v1 Deep Agents describe cannot prove it."""

    def require_permission_everywhere(document: dict[str, Any]) -> None:
        requires = document["mission"]["environment"]["requires"]
        requires["approvals"] = ["workflow_gate", "provider_permission"]

    compilation = compile_yaml(
        edited(example("claude_agent_sdk", "stage-graph"), require_permission_everywhere)
    )
    assert blockers(compilation) == {
        "/mission/environment/requires/approvals/1": "LANE_REQUIREMENT_UNQUALIFIED"
    }
    (issue,) = compilation.report.blockers
    assert "deep_agents" in issue.message


@pytest.mark.parametrize(
    ("lane", "provider"), [("claude_cloud", "anthropic"), ("codex_cloud", "openai")]
)
def test_hosted_profiles_compile_structurally_and_their_launch_is_refused(
    lane: str, provider: str
) -> None:
    def hosted(document: dict[str, Any]) -> None:
        environment = document["mission"]["environment"]
        environment["lane"] = lane
        environment["execution_environment"] = {
            "kind": "provider_hosted",
            "provider": provider,
            "environment_ref": "env-fixture",
            "expected_revision": "rev-1",
        }
        environment["workspace"]["policy"] = {"mode": "provider_workspace", "reuse": "none"}
        environment.pop("requires")
        environment.pop("continuation", None)

    compilation = compiled_ok(edited(example("claude_agent_sdk", "goal-loop"), hosted))
    warnings = {item.pointer: item.reason for item in compilation.report.warnings}
    assert warnings.get("/mission/environment/lane") == "hosted_launch_refused"
    (definition,), (program,) = compilation.definitions, compilation.programs
    with pytest.raises(ManifestLaunchBindingError) as raised:
        templates_of(program, definition, None)
    assert raised.value.pointer == "/mission/environment/lane"
    assert "evidence-blocked" in str(raised.value)


def test_a_hosted_profile_cannot_compile_a_required_lifecycle_feature() -> None:
    def hosted_with_cancel(document: dict[str, Any]) -> None:
        environment = document["mission"]["environment"]
        environment["lane"] = "codex_cloud"
        environment["execution_environment"] = {
            "kind": "provider_hosted",
            "provider": "openai",
            "environment_ref": "env-fixture",
            "expected_revision": "rev-1",
        }
        environment["workspace"]["policy"] = {"mode": "provider_workspace", "reuse": "none"}
        environment["requires"] = {"controls": ["cancel"]}

    compilation = compile_yaml(edited(example("codex", "goal-loop"), hosted_with_cancel))
    assert blockers(compilation) == {
        "/mission/environment/requires/controls/0": "LANE_REQUIREMENT_UNSUPPORTED"
    }


# --- SPEC-02 inheritance for the v2 fields ----


def structural_blockers(text: str) -> dict[str, str]:
    manifest, document = parse_manifest_yaml_versioned(text)
    structure = resolve_environments(manifest, document)
    return {item.pointer: str(item.reason) for item in structure.blockers}


def test_a_child_cannot_widen_egress_remove_mandatory_hooks_or_raise_session_turns() -> None:
    hook = {
        "pin": "hook.citation-check@1.0.0#sha256:" + "a" * 64,
        "events": ["before_tool"],
        "fail_closed": True,
    }

    def widen(document: dict[str, Any]) -> None:
        environment = document["mission"]["environment"]
        environment["sandbox"] = {"egress": ["pubmed"]}
        environment["hooks"] = [hook]
        node = document["mission"]["program"]["nodes"][0]
        node["environment"] = {
            "sandbox": {"egress": ["pubmed", "anywhere"]},
            "hooks": [],
            "continuation": {"max_session_turns": 40},
            "requires": {"all_writes_gated": False},
        }
        environment["requires"]["all_writes_gated"] = True

    found = structural_blockers(edited(example("claude_agent_sdk", "stage-graph"), widen))
    env = "/mission/program/nodes/0/environment"
    assert found[f"{env}/sandbox/egress/1"] == "widens_authority"
    assert found[f"{env}/hooks"] == "removes_mandatory_policy"
    assert found[f"{env}/continuation/max_session_turns"] == "widens_authority"
    assert found[f"{env}/requires/all_writes_gated"] == "widens_authority"


def test_an_explicit_empty_list_clears_optional_hooks_only() -> None:
    def optional(document: dict[str, Any]) -> None:
        environment = document["mission"]["environment"]
        environment["hooks"] = [
            {"pin": "hook.note@1.0.0#sha256:" + "b" * 64, "events": ["stop"], "fail_closed": False}
        ]
        document["mission"]["program"]["nodes"][0]["environment"] = {"hooks": []}

    assert structural_blockers(edited(example("claude_agent_sdk", "stage-graph"), optional)) == {}


def test_a_node_switching_to_a_provider_lane_declares_its_placement() -> None:
    def switch(document: dict[str, Any]) -> None:
        environment = document["mission"]["environment"]
        environment.update(lane="deep_agents", model={"profile": "frontier.default"})
        environment["sandbox"] = {"profile": "research.standard"}
        environment.pop("execution_environment")
        document["mission"]["program"]["nodes"][0]["environment"] = {
            "lane": "claude_agent_sdk",
            "model": {"profile": "claude.default"},
        }

    found = structural_blockers(edited(example("claude_agent_sdk", "stage-graph"), switch))
    assert found == {"/mission/program/nodes/0/environment/execution_environment": "missing_field"}


# --- Launch refusals name their manifest pointer ----


def _goal_loop(profile: str = "claude_agent_sdk") -> tuple[MissionDefinition, CompiledMission]:
    compilation = compiled_ok(example(profile, "goal-loop").read_text(encoding="utf-8"))
    return compilation.definitions[0], compilation.programs[0]


def test_a_v1_bindings_file_has_no_provider_binding() -> None:
    definition, program = _goal_loop()
    with pytest.raises(ManifestLaunchBindingError) as raised:
        templates_of(program, definition, ManifestLaunchBindings.load(V1_BINDINGS))
    assert raised.value.pointer == "/mission/environment/lane"
    assert "providers.claude_agent_sdk" in str(raised.value)


@pytest.mark.parametrize(
    ("section", "pointer"),
    [
        ("model_profiles", "/mission/environment/model/profile"),
        ("auth_profiles", "/mission/environment/auth/profile"),
        ("host_profiles", "/mission/environment/execution_environment/profile"),
    ],
)
def test_an_unbound_selection_fails_at_its_manifest_pointer(section: str, pointer: str) -> None:
    definition, program = _goal_loop()
    bound = bindings()
    assert bound.providers is not None and bound.providers.claude_agent_sdk is not None
    lane = bound.providers.claude_agent_sdk.model_copy(update={section: {}})
    unbound = bound.model_copy(
        update={"providers": bound.providers.model_copy(update={"claude_agent_sdk": lane})}
    )
    with pytest.raises(ManifestLaunchBindingError) as raised:
        templates_of(program, definition, unbound)
    assert raised.value.pointer == pointer


def test_an_unmapped_provider_model_setting_fails_at_its_pointer() -> None:
    def temperature(document: dict[str, Any]) -> None:
        document["mission"]["environment"]["model"]["settings"] = {"temperature": 0.2}

    compilation = compiled_ok(edited(example("claude_agent_sdk", "goal-loop"), temperature))
    with pytest.raises(ManifestLaunchBindingError) as raised:
        templates_of(compilation.programs[0], compilation.definitions[0], None)
    assert raised.value.pointer == "/mission/environment/model/settings/temperature"


def test_a_provider_node_selecting_a_capability_needs_projection_rows() -> None:
    """Capabilities project into the lane's files; without a rows resolver nothing is invented."""

    def with_skill(document: dict[str, Any]) -> None:
        document["mission"]["environment"]["workspace"]["skills"] = [
            {"search": "biotech literature review procedure", "kind": "skill_bundle", "as": "lit"}
        ]

    compilation = compile_yaml(edited(example("claude_agent_sdk", "goal-loop"), with_skill))
    if not compilation.ok:  # the fixture skill does not declare claude host support
        assert all(
            item.reason in {"search_no_admitted_hits", "pin_lane_unsupported"}
            for item in compilation.report.blockers
        )
        return
    definition, program = compilation.definitions[0], compilation.programs[0]
    assert provider_capability_pins(definition)
    with pytest.raises(ManifestLaunchBindingError) as raised:
        templates_of(program, definition, None)
    assert raised.value.pointer.startswith("/mission/environment/workspace/skills/0")


# --- The deployment file and the lanes' projection ----


def test_the_bindings_file_pins_exactly_the_composed_adapters() -> None:
    document = json.loads(BINDINGS.read_text(encoding="utf-8"))
    assert ManifestLaunchBindings.model_validate(document).providers is not None

    wrong_pin = copy.deepcopy(document)
    wrong_pin["providers"]["claude_agent_sdk"]["pins"]["sdk.claude_agent_sdk"] = "0.2.164"
    with pytest.raises(ValueError, match="0.2.165"):
        ManifestLaunchBindings.model_validate(wrong_pin)

    swapped = copy.deepcopy(document)
    swapped["providers"]["claude_agent_sdk"]["provider_options"] = document["providers"]["codex"][
        "provider_options"
    ]
    with pytest.raises(ValueError, match="codex_app_server options"):
        ManifestLaunchBindings.model_validate(swapped)

    free_form = copy.deepcopy(document)
    free_form["providers"]["codex"]["provider_options"]["model_reasoning_summary"] = "auto"
    with pytest.raises(ValueError):
        ManifestLaunchBindings.model_validate(free_form)

    hosted = copy.deepcopy(document)
    hosted["providers"]["claude_cloud"] = document["providers"]["claude_agent_sdk"]
    with pytest.raises(ValueError):
        ManifestLaunchBindings.model_validate(hosted)

    v1_with_providers = copy.deepcopy(document)
    v1_with_providers["schema_version"] = "mc.manifest_launch_bindings.v1"
    with pytest.raises(ValueError, match="needs schema_version"):
        ManifestLaunchBindings.model_validate(v1_with_providers)
    # A v1 file still reads, unchanged and without a providers key.
    v1 = ManifestLaunchBindings.load(V1_BINDINGS)
    assert v1.providers is None and "providers" not in v1.model_dump(mode="json")


def test_the_operating_contract_is_the_lanes_instruction_channel() -> None:
    compilation = compiled_ok(example("codex", "stage-graph").read_text(encoding="utf-8"))
    templates = templates_of(compilation.programs[0], compilation.definitions[0], None)
    for template in templates.values():
        assert operating_contract_text(template.prompt_segments) == operating_contract(template)
        if template.provider_binding is not None:
            assert template.provider_binding.materialization_digest == materialization_digest(
                (), template.provider_binding.lane_profile, template.prompt_segments
            )


def test_the_worker_rows_resolver_reads_the_binding_pins_in_render_order() -> None:
    compilation = compiled_ok(example("claude_agent_sdk", "goal-loop").read_text("utf-8"))
    template = templates_of(compilation.programs[0], compilation.definitions[0], None)["executor"]
    binding = template.provider_binding
    assert binding is not None and binding_capability_pins(binding.pins) == ()
    asked: list[tuple[str, ...]] = []

    async def rows(pins: Any) -> dict[str, Any]:
        asked.append(tuple(pins))
        return {}

    assert asyncio.run(ProviderBindingRows(rows)(template)) == ()
    assert asked == []
    pins = {"capability.skill.b": "b@1#x", "capability.hook.a": "a@1#y", "sdk.x": "1"}
    assert binding_capability_pins(pins) == ("a@1#y", "b@1#x")


def test_projected_capabilities_are_pinned_and_rendered_like_the_lane_renders_them() -> None:
    """A claude node's Skill: the author pins it in the binding and renders its rows; the
    worker's `ProviderBindingRows` re-resolves the same rows, so `prepare` recomputes the
    same projection digest (`RenderedProjectionSource`, as the claude/codex harnesses use)."""

    from mission_control.adapters.cursor.projection import RenderedProjectionSource
    from tests.fixtures.projections.rows import skill_row

    def with_skill(document: dict[str, Any]) -> None:
        document["mission"]["environment"]["workspace"]["skills"] = [
            {"search": "biotech literature review procedure", "kind": "skill_bundle", "as": "lit"}
        ]

    compilation = compiled_ok(edited(example("claude_agent_sdk", "goal-loop"), with_skill))
    definition, program = compilation.definitions[0], compilation.programs[0]
    (text,) = provider_capability_pins(definition)
    row = skill_row()
    admitted = AdmittedLaunch.from_configuration(SCOPE, "run-v2", program.configuration)
    executor = resolver().goal_templates(definition, admitted, rows={text: row})["executor"]
    binding = executor.provider_binding
    assert binding is not None
    (key,) = [name for name in binding.pins if name.startswith("capability.")]
    assert key.startswith("capability.skill.") and binding.pins[key] == text
    assert binding.materialization_digest == materialization_digest(
        (row,), "claude_agent_sdk", executor.prompt_segments
    )

    async def port(pins: Any) -> dict[str, Any]:
        assert tuple(pins) == (text,)
        return {text: row}

    worker = RenderedProjectionSource(ProviderBindingRows(port))
    projection = asyncio.run(
        worker.project(executor, profile="claude_agent_sdk", packet_index=None)
    )
    assert projection_digest(projection) == binding.materialization_digest


def test_an_execution_environment_of_the_other_kind_replaces_the_inherited_one() -> None:
    """Recovery 2026-10-09 (MP-20 Cursor finding): hosted over local is a replacement, not a
    deep merge (which produced an invalid selection and a 500 at submit)."""

    from mission_control.domain.authoring.manifest import merge_environment_documents

    local = {"execution_environment": {"kind": "local_workspace", "profile": "worker.linux"}}
    hosted = {
        "execution_environment": {
            "kind": "provider_hosted",
            "provider": "cursor",
            "environment_ref": "env-1",
        }
    }
    assert merge_environment_documents(local, hosted) == hosted
    assert merge_environment_documents(hosted, local) == local
    same = {"execution_environment": {"profile": "worker.linux.wsl"}}
    merged = merge_environment_documents(local, same)
    assert merged["execution_environment"] == {
        "kind": "local_workspace",
        "profile": "worker.linux.wsl",
    }
