"""FT-E1: Mission Manifest v1 schema, inheritance and JSON Schema export (SPEC-05)."""

from __future__ import annotations

import copy
import json
from decimal import Decimal
from pathlib import Path
from typing import Any

import jsonschema
import pytest

from mission_control.domain.authoring.manifest import (
    MANIFEST_SCHEMA_FILENAME,
    GoalLoopNode,
    Lane,
    ManifestErrorCode,
    ManifestRejected,
    StageGraphNode,
    load_manifest_yaml,
    manifest_digest,
    manifest_json_schema,
    manifest_json_schema_text,
    merge_environment_documents,
    parse_duration,
    parse_manifest,
    parse_manifest_yaml,
    resolve_environments,
)
from mission_control.domain.authoring.mission_definition import (
    MissionDefinition,
    manifest_to_definitions,
)
from mission_control.interfaces.cli.main import main as missionctl

ROOT = Path(__file__).resolve().parents[3]
OWNER_MISSIONS = sorted((ROOT / "docs/specs/fast-track-2026-10/missions").glob("*.yml"))
FIXTURES = ROOT / "tests/fixtures/manifests"
SCHEMA_PATH = ROOT / "src/mission_control/contracts/schemas" / MANIFEST_SCHEMA_FILENAME


def minimal() -> dict[str, Any]:
    return load_manifest_yaml((FIXTURES / "minimal-stage-graph.yml").read_text(encoding="utf-8"))


def chain() -> dict[str, Any]:
    return load_manifest_yaml((FIXTURES / "two-mission-chain.yml").read_text(encoding="utf-8"))


def rejection(document: dict[str, Any]) -> list[tuple[str, str]]:
    with pytest.raises(ManifestRejected) as caught:
        parse_manifest(document)
    assert all(issue.code is ManifestErrorCode.INVALID_DEFINITION for issue in caught.value.issues)
    return [(issue.pointer, issue.message) for issue in caught.value.issues]


def structure(document: dict[str, Any]):
    return resolve_environments(parse_manifest(document), document)


def json_plain(value: Any) -> Any:
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    if isinstance(value, dict):
        return {key: json_plain(item) for key, item in value.items()}
    if isinstance(value, list):
        return [json_plain(item) for item in value]
    return value


# --- owner manifests and the exported JSON Schema --------------------------------------


def test_three_owner_manifests_exist():
    assert [path.name for path in OWNER_MISSIONS] == [
        "01-research-ingestion-deep-agents.yml",
        "02-research-ingestion-cursor-cloud-chain.yml",
        "03-codebase-feature-cursor-local.yml",
    ]


@pytest.mark.parametrize("path", OWNER_MISSIONS, ids=lambda path: path.name)
def test_owner_manifests_validate_against_the_committed_json_schema(path: Path):
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    jsonschema.Draft202012Validator.check_schema(schema)
    document = load_manifest_yaml(path.read_text(encoding="utf-8"))
    errors = list(jsonschema.Draft202012Validator(schema).iter_errors(json_plain(document)))
    assert errors == []


@pytest.mark.parametrize("path", OWNER_MISSIONS, ids=lambda path: path.name)
def test_owner_manifests_parse_with_no_structural_blockers(path: Path):
    manifest, document = parse_manifest_yaml(path.read_text(encoding="utf-8"))
    result = resolve_environments(manifest, document)
    assert result.blockers == ()
    assert all(mission.nodes for mission in result.missions)


def test_committed_json_schema_is_the_generated_schema():
    committed = SCHEMA_PATH.read_text(encoding="utf-8").replace("\r\n", "\n")
    assert committed == manifest_json_schema_text()


def test_missionctl_mission_schema_writes_the_schema(tmp_path: Path):
    out = tmp_path / "schema.json"
    assert missionctl(["mission", "schema", "--out", str(out)]) == 0
    assert out.read_text(encoding="utf-8") == manifest_json_schema_text()


def test_json_schema_rejects_unknown_keys_and_both_entry_forms():
    validator = jsonschema.Draft202012Validator(manifest_json_schema())
    document = json_plain(minimal())
    assert list(validator.iter_errors(document)) == []
    unknown = copy.deepcopy(document)
    unknown["mission"]["environment"]["budget"]["euros"] = 3
    assert list(validator.iter_errors(unknown))
    both = copy.deepcopy(document)
    both["mission"]["environment"]["capabilities"][0]["pin"] = (
        "mcp.pubmed@2.10.20#sha256:" + "a" * 64
    )
    assert list(validator.iter_errors(both))
    chain_without_links = json_plain(chain())
    del chain_without_links["links"]
    assert list(validator.iter_errors(chain_without_links))


# --- YAML loading ----------------------------------------------------------------------


def test_yaml_12_core_scalars_exact_decimals_and_duplicate_keys():
    document = load_manifest_yaml(
        "on: execution_complete\nyes: no\nusd: 25.50\nwhen: 2026-10-07\nn: 012\nflag: true\n"
    )
    assert document == {
        "on": "execution_complete",
        "yes": "no",
        "usd": Decimal("25.50"),
        "when": "2026-10-07",
        "n": 12,
        "flag": True,
    }
    with pytest.raises(ManifestRejected) as caught:
        load_manifest_yaml("a: 1\na: 2\n")
    assert "duplicate key" in caught.value.issues[0].message
    with pytest.raises(ManifestRejected):
        load_manifest_yaml("usd: .nan\n")
    with pytest.raises(ManifestRejected):
        load_manifest_yaml("- not\n- a mapping\n")
    with pytest.raises(ManifestRejected):
        load_manifest_yaml("x: !!python/object:os.system {}\n")


def test_manifest_digest_ignores_comments_order_and_line_endings():
    text = (FIXTURES / "minimal-stage-graph.yml").read_text(encoding="utf-8")
    first = load_manifest_yaml(text)
    reordered = load_manifest_yaml(
        "# a comment\r\n"
        + text.replace("manifest: mission/v1\n", "").replace("\n", "\r\n")
        + "manifest: mission/v1\r\n"
    )
    assert manifest_digest(first) == manifest_digest(reordered)
    changed = copy.deepcopy(first)
    changed["mission"]["title"] = "Another title"
    assert manifest_digest(changed) != manifest_digest(first)


def test_duration_grammar():
    assert parse_duration("4h") == 14_400
    assert parse_duration("1h30m") == 5_400
    assert parse_duration("2d") == 172_800
    assert parse_duration(90) == 90
    for bad in ("", "4", "h", "4x", "1m2h", 0, -5, True, "0s"):
        with pytest.raises(ValueError):
            parse_duration(bad)


# --- shape errors carry pointers ---------------------------------------------------------


def test_malformed_nested_budget_is_reported_at_its_pointer():
    document = minimal()
    document["mission"]["program"]["nodes"][1]["environment"]["budget"] = {"usd": "lots"}
    assert [pointer for pointer, _ in rejection(document)] == [
        "/mission/program/nodes/1/environment/budget/usd"
    ]


@pytest.mark.parametrize(
    ("path", "pointer"),
    [
        ((), "/surprise"),
        (("mission",), "/mission/surprise"),
        (("mission", "environment"), "/mission/environment/surprise"),
        (("mission", "environment", "model"), "/mission/environment/model/surprise"),
        (("mission", "goals", 0, "criteria", 0), "/mission/goals/0/criteria/0/surprise"),
        (("mission", "program", "nodes", 0), "/mission/program/nodes/0/surprise"),
        (("mission", "controls"), "/mission/controls/surprise"),
    ],
)
def test_unknown_keys_are_rejected_everywhere(path: tuple[Any, ...], pointer: str):
    document = minimal()
    target: Any = document
    for part in path:
        target = target[part]
    target["surprise"] = 1
    assert (pointer, "Extra inputs are not permitted") in rejection(document)


def test_capability_entry_needs_exactly_one_form_and_exact_pins_need_digests():
    document = minimal()
    entry = document["mission"]["environment"]["capabilities"][0]
    entry["pin"] = "mcp.pubmed@2.10.20#sha256:" + "a" * 64
    assert any("exactly one of pin | search" in message for _, message in rejection(document))
    document = minimal()
    document["mission"]["environment"]["capabilities"] = [{"pin": "mcp.pubmed@2.10.20"}]
    assert any("#sha256" in message for _, message in rejection(document))
    document["mission"]["environment"]["capabilities"] = [{"pin": "mcp.pubmed@latest"}]
    parse_manifest(document)
    document["mission"]["environment"]["capabilities"] = [{"search": "pubmed"}]
    assert any("needs kind" in message for _, message in rejection(document))


def test_acceptance_rejects_free_text_and_multiple_operators():
    document = minimal()
    criterion = document["mission"]["goals"][0]["criteria"][0]
    criterion["acceptance"] = "coverage > 0.8 and reviewer is happy"
    messages = [message for _, message in rejection(document)]
    assert any("free-text" in message for message in messages)
    criterion["acceptance"] = "lambda run: run.ok"
    assert any("code predicates" in message for _, message in rejection(document))
    criterion["acceptance"] = {"schema": "claim_table@1", "human": "approved"}
    assert any("exactly one of" in message for _, message in rejection(document))
    criterion["acceptance"] = {"schema": "import os; os.system('x')"}
    assert rejection(document)


# --- behavior bodies ---------------------------------------------------------------------

EVERY_BEHAVIOR: list[dict[str, Any]] = [
    {"key": "loop", "behavior": "goal_loop", "objective": "evidence_map", "action_space": ["a"]},
    {
        "key": "swarm",
        "behavior": "parallel_swarm",
        "objectives": ["evidence_map"],
        "params": {"workers": 3},
    },
    {"key": "eo", "behavior": "evaluator_optimizer", "objectives": ["evidence_map"]},
    {
        "key": "agent",
        "behavior": "agent_executor",
        "objectives": ["evidence_map"],
        "instruction": "Do it",
        "missing_output_policy": {"follow_up_turn": {"max_turns": 2}},
    },
    {
        "key": "tests",
        "behavior": "deterministic_executor",
        "objectives": ["evidence_map"],
        "kind": "test_run",
        "params": {"command": "make test"},
    },
    {
        "key": "gate",
        "behavior": "human_gate",
        "task": {
            "kind": "APPROVAL",
            "prompt": "Ship?",
            "reviewers": ["owner"],
            "packet": ["agent.report"],
            "timeout": "2d",
            "on_timeout": "keep_waiting",
        },
    },
    {
        "key": "wait",
        "behavior": "event_wait",
        "event_type": "artifact.registered",
        "match": {"kind": "pdf"},
        "timeout": "1h",
        "mode": {"rearm": {"max": 3}},
    },
    {"key": "pause", "behavior": "timer", "duration": "15m"},
    {"key": "later", "behavior": "timer", "until": "2026-12-01T00:00:00Z"},
    {
        "key": "proof",
        "behavior": "proof_gate",
        "evidence": "agent.report",
        "required_disposition": "accepted",
        "on_reject": "stop",
    },
    {
        "key": "child",
        "behavior": "child_mission_invocation",
        "mode": "spawn",
        "child": {"template": "biotech.ingestion"},
        "await": {"until": "mission_accepted"},
        "portal": "peek_and_command",
    },
    {
        "key": "nested",
        "behavior": "stage_graph",
        "objectives": ["evidence_map"],
        "nodes": [{"key": "inner", "behavior": "timer", "duration": 60}],
        "fail_fast": True,
        "concurrency": 2,
    },
]


def test_every_v1_behavior_body_validates():
    document = minimal()
    document["mission"]["program"]["nodes"] = copy.deepcopy(EVERY_BEHAVIOR)
    result = structure(document)
    assert result.blockers == ()
    kinds = {node.behavior.value for node in result.missions[0].nodes}
    assert {"parallel_swarm", "evaluator_optimizer", "child_mission_invocation"} <= kinds


@pytest.mark.parametrize(
    ("node", "fragment"),
    [
        ({"key": "t", "behavior": "timer"}, "exactly one of duration | until"),
        (
            {"key": "t", "behavior": "timer", "duration": "1h", "until": "2026-12-01T00:00:00Z"},
            "exactly one",
        ),
        (
            {"key": "g", "behavior": "goal_loop", "objective": "evidence_map", "action_space": []},
            "at least 1",
        ),
        ({"key": "a", "behavior": "agent_executor", "instruction": "x"}, "needs objectives"),
        ({"key": "x", "behavior": "teleport"}, "does not match any of the expected tags"),
        (
            {
                "key": "d",
                "behavior": "deterministic_executor",
                "objectives": ["evidence_map"],
                "kind": "rm_rf",
            },
            "Input should be",
        ),
    ],
)
def test_behavior_body_errors(node: dict[str, Any], fragment: str):
    document = minimal()
    document["mission"]["program"]["nodes"] = [node]
    issues = rejection(document)
    assert any(fragment in message for _, message in issues), issues
    assert all(pointer.startswith("/mission/program/nodes/0") for pointer, _ in issues)


# --- single and chain forms ----------------------------------------------------------------


def test_chain_form_requires_links_and_single_form_rejects_them():
    document = chain()
    parse_manifest(document)
    del document["links"]
    assert any("requires links" in message for _, message in rejection(document))
    single = minimal()
    single["links"] = chain()["links"]
    assert any("chain) form only" in message for _, message in rejection(single))
    both = minimal()
    both["missions"] = chain()["missions"]
    assert any("exactly one of mission | missions" in message for _, message in rejection(both))


def test_link_shapes_validate():
    document = chain()
    document["links"][0]["outputs"] = []
    assert any("at least one supplier output" in message for _, message in rejection(document))
    document = chain()
    document["links"][1]["outputs"] = ["evidence_map"]
    assert any("no outputs" in message for _, message in rejection(document))
    document = chain()
    document["links"][1]["on"] = "whenever"
    issues = rejection(document)
    assert any(pointer.startswith("/links/1/on") for pointer, _ in issues)
    manifest = parse_manifest(chain())
    assert manifest.links is not None
    assert manifest.links[1].on == "execution_complete"


# --- inheritance -----------------------------------------------------------------------------


def node_env(result, key: str, role: str = "node"):
    return next(
        node
        for mission in result.missions
        for node in mission.nodes
        if node.node_key == key and node.role == role
    )


def test_inheritance_deep_merges_mappings_and_replaces_lists_and_scalars():
    document = minimal()
    synth = document["mission"]["program"]["nodes"][1]
    synth["environment"] = {
        "model": {"settings": {"reasoning": {"budget": 4}}},
        "capabilities": [],
        "sandbox": {"egress": []},
    }
    result = structure(document)
    assert result.blockers == ()
    env = node_env(result, "synthesize")
    effective = env.effective_environment
    assert effective.model is not None and effective.model.profile == "frontier.default"
    assert effective.model.settings == {
        "temperature": Decimal("0.2"),
        "reasoning": {"effort": "high", "budget": 4},
    }
    assert effective.capabilities == ()
    assert effective.sandbox is not None
    assert effective.sandbox.profile == "research.standard" and effective.sandbox.egress == ()
    assert env.field_provenance["capabilities"] == "overlay"
    assert env.field_provenance["model.settings.reasoning.budget"] == "overlay"
    assert env.field_provenance["model.settings.reasoning.effort"] == "inherited"
    assert env.field_provenance["model.profile"] == "inherited"
    collect = node_env(result, "collect")
    assert collect.effective_environment.budget is not None
    assert collect.effective_environment.budget.usd == Decimal(10)
    assert collect.effective_environment.budget.wall_clock == 14_400
    assert collect.field_provenance["budget.usd"] == "overlay"
    assert collect.field_provenance["budget.tokens"] == "inherited"


def test_capability_entries_replace_as_a_unit():
    merged = merge_environment_documents(
        {"model": {"profile": {"search": "x", "kind": "model_profile", "as": "m"}}},
        {"model": {"profile": {"pin": "model.a@1#sha256:" + "0" * 64}}},
    )
    assert merged == {"model": {"profile": {"pin": "model.a@1#sha256:" + "0" * 64}}}


@pytest.mark.parametrize(
    ("overlay", "pointer"),
    [
        ({"budget": {"usd": 30}}, "/mission/program/nodes/0/environment/budget/usd"),
        (
            {"budget": {"wall_clock": "5h"}},
            "/mission/program/nodes/0/environment/budget/wall_clock",
        ),
        (
            {"governors": {"iterations": 9}},
            "/mission/program/nodes/0/environment/governors/iterations",
        ),
        (
            {"side_effects": ["read_only", "spend"]},
            "/mission/program/nodes/0/environment/side_effects/1",
        ),
    ],
)
def test_widening_overlays_fail_with_pointer(overlay: dict[str, Any], pointer: str):
    document = minimal()
    document["mission"]["program"]["nodes"][0]["environment"] = overlay
    result = structure(document)
    assert [(issue.pointer, issue.reason) for issue in result.blockers] == [
        (pointer, "widens_authority")
    ]
    assert result.blockers[0].code is ManifestErrorCode.INVALID_DEFINITION


def test_nested_overlays_narrow_against_their_parent_not_the_mission():
    document = minimal()
    document["mission"]["program"]["nodes"] = [
        {
            "key": "phase",
            "behavior": "stage_graph",
            "objectives": ["evidence_map"],
            "environment": {"budget": {"usd": 5}},
            "nodes": [
                {
                    "key": "inner",
                    "behavior": "timer",
                    "duration": 5,
                    "environment": {"budget": {"usd": 6}},
                }
            ],
        }
    ]
    result = structure(document)
    assert [issue.pointer for issue in result.blockers] == [
        "/mission/program/nodes/0/nodes/0/environment/budget/usd"
    ]


def test_lane_change_is_recorded_and_cursor_lanes_need_a_repository():
    document = minimal()
    document["mission"]["program"]["nodes"][1]["environment"] = {"lane": "cursor_local"}
    result = structure(document)
    assert [(issue.pointer, issue.reason) for issue in result.blockers] == [
        ("/mission/program/nodes/1/environment/workspace/repo", "missing_field")
    ]
    document["mission"]["program"]["nodes"][1]["environment"] = {
        "lane": "cursor_local",
        "workspace": {"repo": {"path": "C:\\repo", "ref": "main"}},
    }
    result = structure(document)
    assert result.blockers == ()
    synth = node_env(result, "synthesize")
    assert synth.lane is Lane.CURSOR_LOCAL and synth.lane_changed_from is Lane.DEEP_AGENTS


def test_reserved_lanes_are_unsupported_in_v1():
    document = minimal()
    document["mission"]["environment"]["lane"] = "codex"
    result = structure(document)
    assert [(issue.code, issue.reason) for issue in result.blockers] == [
        (ManifestErrorCode.UNSUPPORTED_BEHAVIOR, "reserved_lane")
    ]


def test_mission_environment_must_be_complete_and_goal_loops_need_governors():
    document = minimal()
    environment = document["mission"]["environment"]
    del environment["budget"]
    del environment["sandbox"]
    environment["governors"] = {"depth": 1}
    reasons = {(issue.pointer, issue.reason) for issue in structure(document).blockers}
    assert reasons == {
        ("/mission/environment/budget", "missing_field"),
        ("/mission/environment/sandbox", "missing_field"),
        ("/mission/program/nodes/0/environment/governors/patience", "missing_governors"),
    }


def test_references_keys_and_dependency_cycles():
    document = minimal()
    nodes = document["mission"]["program"]["nodes"]
    nodes[0]["depends_on"] = ["synthesize"]
    nodes[1]["objectives"] = ["unknown_goal"]
    nodes.append({"key": "collect", "behavior": "timer", "duration": 5, "depends_on": ["ghost"]})
    reasons = {(issue.pointer, issue.reason) for issue in structure(document).blockers}
    assert reasons == {
        ("/mission/program/nodes/0/depends_on", "dependency_cycle"),
        ("/mission/program/nodes/1/objectives", "unknown_objective"),
        ("/mission/program/nodes/2/key", "duplicate_key"),
        ("/mission/program/nodes/2/depends_on/0", "unknown_dependency"),
    }


def test_owner_mission_2_verifier_overlay_changes_lane():
    path = next(path for path in OWNER_MISSIONS if path.name.startswith("02"))
    manifest, document = parse_manifest_yaml(path.read_text(encoding="utf-8"))
    result = resolve_environments(manifest, document)
    verifier = node_env(result, "root", "verifier")
    assert verifier.lane is Lane.DEEP_AGENTS
    assert verifier.lane_changed_from is Lane.CURSOR_CLOUD
    assert verifier.field_provenance["lane"] == "overlay"
    assert manifest.missions is not None
    root = manifest.missions[0].program
    assert isinstance(root, GoalLoopNode)


# --- MissionDefinition@1 -----------------------------------------------------------------------


def test_manifest_to_definition_is_pure_and_deterministic():
    document = minimal()
    first = manifest_to_definitions(parse_manifest(document), document)
    second = manifest_to_definitions(parse_manifest(copy.deepcopy(document)), document)
    assert [item.digest for item in first] == [item.digest for item in second]
    definition = first[0]
    assert isinstance(definition, MissionDefinition)
    assert definition.schema_version == "mc.mission_definition.v1"
    assert definition.budget.usd == Decimal("25.50")
    assert definition.budget.wall_clock_seconds == 14_400
    assert [goal.key for goal in definition.goals] == ["evidence_map"]
    assert [item.key for item in definition.objectives] == ["find_trials"]
    assert [item.name for item in definition.inputs] == ["sources"]
    assert definition.inputs[0].ref == "collect.sources"
    assert definition.criteria[0].acceptance.op == "all"
    assert [item.op for item in definition.criteria[0].acceptance.operands] == ["schema", "human"]
    assert definition.completion_contract.goals[0].required is True
    assert definition.program.nodes[0].environment.budget is not None
    assert not definition.is_resolved
    assert [(item.role, item.alias) for item in definition.capabilities] == [
        ("capability", "pubmed")
    ]
    round_trip = MissionDefinition.model_validate_json(definition.model_dump_json())
    assert round_trip.digest == definition.digest


def test_definition_digest_tracks_content():
    document = minimal()
    base = manifest_to_definitions(parse_manifest(document), document)[0].digest
    document["mission"]["program"]["nodes"][0]["environment"]["budget"]["usd"] = 9
    assert manifest_to_definitions(parse_manifest(document), document)[0].digest != base


@pytest.mark.parametrize("path", OWNER_MISSIONS, ids=lambda path: path.name)
def test_owner_manifests_lower_to_definitions(path: Path):
    manifest, document = parse_manifest_yaml(path.read_text(encoding="utf-8"))
    definitions = manifest_to_definitions(manifest, document)
    assert len(definitions) == (2 if manifest.is_chain else 1)
    for definition in definitions:
        assert definition.capabilities
        assert all(
            capability.pointer.startswith("/mission") for capability in definition.capabilities
        )
    if path.name.startswith("01"):
        root = definitions[0].program
        assert root.behavior.value == "stage_graph"
        assert isinstance(manifest.mission and manifest.mission.program, StageGraphNode)
        assert [node.key for node in root.nodes] == ["collect", "synthesize", "review", "ingest"]
        hooks = [item for item in definitions[0].capabilities if item.role == "hook"]
        assert hooks[0].attributes["events"] == ["after_tool", "stop"]
