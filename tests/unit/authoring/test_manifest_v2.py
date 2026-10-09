"""MP-01: `mission/v2` parses as a superset of `mission/v1`, the v2 lane/placement rules are
pointed errors, inheritance narrowing holds for the new fields, and v1 is untouched."""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from mission_control.domain.authoring.manifest import (
    MANIFEST_SCHEMA_FILENAME,
    ManifestRejected,
    MissionManifest,
    load_manifest_yaml,
    manifest_json_schema_text,
    resolve_environments,
)
from mission_control.domain.authoring.manifest_v2 import (
    MANIFEST_V2_SCHEMA_FILENAME,
    MANIFEST_VERSIONS,
    EnvironmentV2,
    MissionManifestV2,
    manifest_v2_json_schema_text,
    parse_manifest_any,
)

ROOT = Path(__file__).resolve().parents[3]
MINIMAL = ROOT / "tests/fixtures/manifests/minimal-stage-graph.yml"
SCHEMAS = ROOT / "src/mission_control/contracts/schemas"


def minimal_v1() -> dict[str, Any]:
    return load_manifest_yaml(MINIMAL.read_text(encoding="utf-8"))


def as_v2(document: dict[str, Any]) -> dict[str, Any]:
    document = copy.deepcopy(document)
    document["manifest"] = "mission/v2"
    return document


def hosted_claude(document: dict[str, Any]) -> dict[str, Any]:
    env = document["mission"]["environment"]
    env["lane"] = "claude_cloud"
    env["execution_environment"] = {
        "kind": "provider_hosted",
        "provider": "anthropic",
        "environment_ref": "env-research",
    }
    env["workspace"] = {
        "repo": {"url": "https://github.com/acme/repo", "ref": "main"},
        "policy": {"mode": "provider_workspace", "reuse": "none"},
    }
    return document


def blockers(document: dict[str, Any]) -> set[tuple[str, str]]:
    manifest = parse_manifest_any(document)
    structure = resolve_environments(manifest, document)
    return {(issue.pointer, issue.reason or "") for issue in structure.blockers}


# --- parsing --------------------------------------------------------------------------------


def test_v1_fixture_parses_identically_as_v1_and_v2() -> None:
    document = minimal_v1()
    v1 = parse_manifest_any(document)
    assert type(v1) is MissionManifest
    v2 = parse_manifest_any(as_v2(document))
    assert isinstance(v2, MissionManifestV2)
    assert v2.manifest == "mission/v2"
    # Every v1 field survives with the same value; v2 adds nothing by default.
    v1_dump = v1.model_dump(mode="json", by_alias=True, exclude_none=True)
    v2_dump = v2.model_dump(mode="json", by_alias=True, exclude_none=True)
    v1_dump.pop("manifest"), v2_dump.pop("manifest")
    assert v1_dump == v2_dump
    assert blockers(document) == blockers(as_v2(document))


def test_v1_parser_refuses_v2_fields_and_v2_literal() -> None:
    document = as_v2(minimal_v1())
    with pytest.raises(ValidationError):
        MissionManifest.model_validate(document)
    v1_with_v2_field = minimal_v1()
    v1_with_v2_field["mission"]["environment"]["continuation"] = {"native_compaction": "never"}
    with pytest.raises(ValidationError, match="continuation"):
        MissionManifest.model_validate(v1_with_v2_field)


def test_unknown_manifest_version_is_a_pointed_blocker() -> None:
    assert set(MANIFEST_VERSIONS) == {"mission/v1", "mission/v2"}
    with pytest.raises(ManifestRejected, match="unsupported manifest version") as rejected:
        parse_manifest_any({**minimal_v1(), "manifest": "mission/v3"})
    assert any(issue.pointer == "/manifest" for issue in rejected.value.issues)


def test_v2_accepts_the_provider_lanes_v1_reserves() -> None:
    document = as_v2(minimal_v1())
    env = document["mission"]["environment"]
    env["lane"] = "claude_agent_sdk"
    env["execution_environment"] = {"kind": "local_workspace", "profile": "worker.default"}
    env["workspace"] = {"repo": {"url": "https://github.com/acme/repo", "ref": "main"}}
    assert blockers(document) == set()
    # The same lane in mission/v1 stays reserved (unchanged v1 behavior).
    v1 = minimal_v1()
    v1["mission"]["environment"]["lane"] = "claude_agent_sdk"
    assert any(pointer == "/mission/environment/lane" for pointer, _ in blockers(v1))


# --- v2 lane and placement rules --------------------------------------------------------------


def test_hosted_lane_requires_provider_hosted_environment_and_repo() -> None:
    document = hosted_claude(as_v2(minimal_v1()))
    assert blockers(document) == set()

    wrong_kind = copy.deepcopy(document)
    wrong_kind["mission"]["environment"]["execution_environment"] = {
        "kind": "local_workspace",
    }
    assert ("/mission/environment/execution_environment/kind", "placement_mismatch") in (
        blockers(wrong_kind)
    )

    wrong_provider = copy.deepcopy(document)
    wrong_provider["mission"]["environment"]["execution_environment"]["provider"] = "openai"
    assert ("/mission/environment/execution_environment/provider", "provider_mismatch") in (
        blockers(wrong_provider)
    )

    local_path = copy.deepcopy(document)
    local_path["mission"]["environment"]["workspace"]["repo"]["path"] = "./checkout"
    assert ("/mission/environment/workspace/repo/path", "hosted_local_path") in blockers(local_path)

    worktree = copy.deepcopy(document)
    worktree["mission"]["environment"]["workspace"]["policy"]["mode"] = "managed_worktree"
    assert ("/mission/environment/workspace/policy/mode", "hosted_worktree") in blockers(worktree)

    no_repo = copy.deepcopy(document)
    no_repo["mission"]["environment"]["workspace"] = {
        "policy": {"mode": "provider_workspace"},
    }
    assert ("/mission/environment/workspace/repo", "missing_field") in blockers(no_repo)

    missing_execution = copy.deepcopy(document)
    del missing_execution["mission"]["environment"]["execution_environment"]
    assert ("/mission/environment/execution_environment", "missing_field") in blockers(
        missing_execution
    )


def test_local_lane_cannot_claim_a_provider_workspace() -> None:
    document = as_v2(minimal_v1())
    env = document["mission"]["environment"]
    env["lane"] = "codex"
    env["execution_environment"] = {"kind": "local_workspace"}
    env["workspace"] = {
        "repo": {"url": "https://github.com/acme/repo", "ref": "main"},
        "policy": {"mode": "provider_workspace"},
    }
    assert ("/mission/environment/workspace/policy/mode", "local_provider_workspace") in (
        blockers(document)
    )


def test_execution_environment_shape_is_validated_by_kind() -> None:
    with pytest.raises(ValidationError, match="does not take"):
        EnvironmentV2.model_validate(
            {"execution_environment": {"kind": "local_workspace", "provider": "anthropic"}}
        )
    with pytest.raises(ValidationError, match="provider and environment_ref"):
        EnvironmentV2.model_validate({"execution_environment": {"kind": "provider_hosted"}})
    with pytest.raises(ValidationError, match="soft_context_ratio"):
        EnvironmentV2.model_validate(
            {"continuation": {"soft_context_ratio": "0.9", "hard_context_ratio": "0.8"}}
        )
    with pytest.raises(ValidationError, match="repeat"):
        EnvironmentV2.model_validate({"requires": {"controls": ["pause", "pause"]}})


# --- inheritance narrowing --------------------------------------------------------------------


def test_child_cannot_drop_required_features_or_weaken_workspace_policy() -> None:
    document = as_v2(minimal_v1())
    env = document["mission"]["environment"]
    env["requires"] = {"controls": ["pause", "cancel"], "approvals": ["workflow_gate"]}
    env["workspace"] = {"policy": {"dirty_input": "reject"}}
    env["continuation"] = {"max_transfers": 2}
    assert blockers(document) == set()

    collect = document["mission"]["program"]["nodes"][0]
    collect["environment"]["requires"] = {"controls": ["cancel"]}
    collect["environment"]["workspace"] = {"policy": {"dirty_input": "snapshot"}}
    collect["environment"]["continuation"] = {"max_transfers": 5}
    found = blockers(document)
    base = "/mission/program/nodes/0/environment"
    assert (f"{base}/requires/controls", "widens_authority") in found
    # An overlay that is silent on `approvals` inherits them; it drops nothing there.
    assert (f"{base}/requires/approvals", "widens_authority") not in found
    assert (f"{base}/workspace/policy/dirty_input", "widens_authority") in found
    assert (f"{base}/continuation/max_transfers", "widens_authority") in found

    # Keeping or tightening is fine.
    collect["environment"]["requires"] = {
        "controls": ["pause", "cancel", "fork"],
        "approvals": ["workflow_gate"],
    }
    collect["environment"]["workspace"] = {"policy": {"dirty_input": "reject"}}
    collect["environment"]["continuation"] = {"max_transfers": 1}
    assert blockers(document) == set()


# --- committed schemas --------------------------------------------------------------------------


def test_committed_schemas_are_the_generated_schemas() -> None:
    v1 = (SCHEMAS / MANIFEST_SCHEMA_FILENAME).read_text(encoding="utf-8").replace("\r\n", "\n")
    v2 = (SCHEMAS / MANIFEST_V2_SCHEMA_FILENAME).read_text(encoding="utf-8").replace("\r\n", "\n")
    assert v1 == manifest_json_schema_text()
    assert v2 == manifest_v2_json_schema_text()
    schema = json.loads(v2)
    assert schema["x-mc-schema-id"] == "mc.mission_manifest.v2"
    assert schema["properties"]["manifest"]["const"] == "mission/v2"
    assert "EnvironmentV2" in schema["$defs"] and "RequiredFeatures" in schema["$defs"]
    # v1 keeps its identity and admits no v2 field.
    v1_schema = json.loads(v1)
    assert v1_schema["x-mc-schema-id"] == "mc.mission_manifest.v1"
    assert "continuation" not in v1_schema["$defs"]["Environment"]["properties"]
