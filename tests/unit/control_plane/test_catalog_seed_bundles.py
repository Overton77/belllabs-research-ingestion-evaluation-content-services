"""Offline structure, closure and provenance checks for the catalog seed bundles."""

from __future__ import annotations

import importlib.util
import json
import re
from pathlib import Path
from types import ModuleType

import pytest

from mission_control.domain.authoring.canonical import sha256_digest
from mission_control.domain.authoring.contracts import PublishedDefinition
from mission_control_db_contract.errors import ContractError
from mission_control_db_contract.seeds import load_bundles, order_bundles, validate_bundle

ROOT = Path(__file__).resolve().parents[3]
SEEDS = ROOT / "packages" / "mission-control-db-contract" / "seeds"
EXPECTED = {
    ("mc.catalog.workflow-parity", "1.0.0"),
    ("mc.catalog.runtime-profiles", "1.0.0"),
    ("mc.catalog.approved-assets", "1.0.0"),
    ("mc.catalog.approved-assets", "1.0.1"),
    ("mc.catalog.agent-capabilities", "1.0.0"),
    ("mc.storage.capability-bundles", "1.0.0"),
    ("mc.qualification.parity", "1.0.0"),
}
STORAGE_KINDS = {"storage_bucket", "storage_policy"}
APP_AGENT_KEYS = {
    "biotech": "mc.app.biotech.agent-capabilities",
    "ai-engineer": "mc.app.ai-engineer.agent-capabilities",
}
SECRET_LIKE = re.compile(
    r"(postgres(ql)?://|password|passwd|secret_value|api[_-]?key|bearer |-----BEGIN)", re.IGNORECASE
)


def _generator() -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        "generate_catalog_seeds", SEEDS / "generate_catalog_seeds.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _bundles(*directories: str) -> list[dict]:
    return load_bundles([SEEDS / directory for directory in directories])


def test_committed_bundles_match_their_repository_sources() -> None:
    generated = _generator().build_bundles()
    committed = {
        path.relative_to(SEEDS).as_posix(): json.loads(path.read_bytes())
        for path in sorted(SEEDS.rglob("*.json"))
    }
    assert committed == generated, "regenerate with generate_catalog_seeds.py --write"


@pytest.mark.parametrize("app", ["biotech", "ai-engineer"])
def test_each_app_installation_set_is_valid_and_dependency_closed(app: str) -> None:
    bundles = _bundles("common", app)
    ordered = order_bundles(bundles, already_applied=set())
    keys = [(item["seed_key"], item["seed_version"]) for item in ordered]
    base = [
        ("mc.catalog.workflow-parity", "1.0.0"),
        ("mc.catalog.runtime-profiles", "1.0.0"),
        ("mc.catalog.approved-assets", "1.0.0"),
        ("mc.app.bindings", "1.0.0"),
        ("mc.app.bindings", "1.0.1"),
    ]
    agent = [("mc.catalog.agent-capabilities", "1.0.0"), (APP_AGENT_KEYS[app], "1.0.0")]
    # FT-A7: skills, the policy hook and the plugin are per application.
    skills = [(f"mc.app.{app}.agent-skills", "1.0.0")]
    storage = [("mc.storage.capability-bundles", "1.0.0")]
    # Succession of the applied, frozen approved-assets@1.0.0 (new logical keys only).
    successors = [("mc.catalog.approved-assets", "1.0.1")]
    assert sorted(keys) == sorted(base + agent + skills + storage + successors)
    assert position_of(keys, successors[0]) > position_of(keys, base[2])
    assert [key for key in keys if key in base] == base
    position = {key: index for index, key in enumerate(keys)}
    assert (
        position[("mc.catalog.approved-assets", "1.0.0")]
        < position[agent[0]]
        < position[agent[1]]
        < position[skills[0]]
    )
    bindings = [item for item in ordered if item["seed_key"] == "mc.app.bindings"]
    frozen, current = (item["records"][0]["fields"] for item in bindings)
    assert (frozen["version"], current["version"]) == ("1", "2")
    binding = current["manifest"]
    assert binding["application_id"] == app
    assert binding["approved_tenant_actor_mappings"] == []
    # The approved target has no unresolved operator fields; 1.0.0 recorded the placeholders.
    assert binding["unresolved_operator_fields"] == []
    assert frozen["manifest"]["unresolved_operator_fields"]
    # Common catalog bytes are one shared copy; only the app binding differs.
    assert _bundles("common") == [
        item
        for item in bundles
        if item["seed_key"]
        not in {"mc.app.bindings", APP_AGENT_KEYS[app], f"mc.app.{app}.agent-skills"}
    ]


def position_of(keys: list[tuple[str, str]], key: tuple[str, str]) -> int:
    return keys.index(key)


def test_approved_assets_successor_never_rewrites_an_applied_logical_key() -> None:
    (frozen,) = [
        b
        for b in _bundles("common")
        if (b["seed_key"], b["seed_version"]) == ("mc.catalog.approved-assets", "1.0.0")
    ]
    (successor,) = [
        b
        for b in _bundles("common")
        if (b["seed_key"], b["seed_version"]) == ("mc.catalog.approved-assets", "1.0.1")
    ]
    # The live 2026-10-03 receipts record this digest for 1.0.0; its bytes are immutable.
    assert frozen["seed_digest"] == (
        "sha256:f45e04f9a68ad5569304691fab2de48befa6dc017076b9bc8c3c557d5a1d3dd6"
    )
    held = {record["logical_key"] for record in frozen["records"]}
    assert successor["records"]
    assert not held & {record["logical_key"] for record in successor["records"]}
    assert {"seed_key": "mc.catalog.approved-assets", "seed_version": "1.0.0"} in successor[
        "depends_on"
    ]


def test_qualification_bundle_is_opt_in_and_closes_over_common() -> None:
    with pytest.raises(ContractError, match="neither supplied nor applied"):
        order_bundles(_bundles("qualification"), already_applied=set())
    ordered = order_bundles(_bundles("common", "qualification"), already_applied=set())
    assert {(item["seed_key"], item["seed_version"]) for item in ordered} == EXPECTED
    qualification = next(item for item in ordered if item["seed_key"] == "mc.qualification.parity")
    (tenant,) = [record for record in qualification["records"] if record["kind"] == "tenant"]
    assert tenant["fields"]["qualification_fixture"] is True
    assert "SYNTHETIC QUALIFICATION FIXTURE" in qualification["description"]


def test_bundles_seed_no_fabricated_authority_usage_or_secrets() -> None:
    for bundle in _bundles("common", "biotech", "ai-engineer", "qualification"):
        kinds = {record["kind"] for record in bundle["records"]}
        if bundle["seed_key"] == "mc.storage.capability-bundles":
            assert kinds == STORAGE_KINDS
            policies = [r["fields"] for r in bundle["records"] if r["kind"] == "storage_policy"]
            assert len(policies) == 4
            assert {p["command"] for p in policies} == {"INSERT", "SELECT", "ALL"}
            continue
        # No actor bindings, actor grants or capability grants without owner approval
        # (FT-A7 plugin expansion rows are catalog structure, not authority).
        assert kinds <= {
            "tenant",
            "asset_version",
            "asset_decision",
            "capability_plugin_member",
        }, bundle["seed_key"]
        if bundle["seed_key"] != "mc.qualification.parity":
            assert "tenant" not in kinds
        assets = {r["logical_key"] for r in bundle["records"] if r["kind"] == "asset_version"}
        decisions = [r for r in bundle["records"] if r["kind"] == "asset_decision"]
        assert {d["fields"]["asset_version"] for d in decisions} == assets
        assert all(d["fields"]["decision"] == "admit" for d in decisions)
        text = json.dumps(bundle)
        # Secret reference NAMES (e.g. TAVILY_API_KEY) are allowed; values never are.
        for name in _secret_ref_names(bundle):
            text = text.replace(name, "SECRET_REF_NAME")
        assert not SECRET_LIKE.search(text), bundle["seed_key"]
        for forbidden in ("embedding", "mission_run", "usage", "receipt"):
            assert f'"{forbidden}"' not in text


def _secret_ref_names(bundle: dict) -> set[str]:
    names: set[str] = set()
    for record in bundle["records"]:
        fields = record["fields"]
        names.update(fields.get("secret_refs", ()))
        definition = fields.get("manifest", {}).get("definition", {})
        names.update(definition.get("secret_refs", ()))
        names.update(definition.get("env_refs", {}))
        names.update(definition.get("env_refs", {}).values())
    return {name for name in names if re.fullmatch(r"[A-Z][A-Z0-9_]*", name)}


def test_published_definition_assets_are_exact_and_repository_readable() -> None:
    from mission_control.adapters.postgres.control_plane.catalog_assets import (
        PUBLISHED_DEFINITION_CONTRACT,
        definition_asset_id,
    )

    seen = set()
    for bundle in _bundles("common", "qualification"):
        for record in bundle["records"]:
            fields = record["fields"]
            if record["kind"] != "asset_version":
                continue
            if fields["contract"] != PUBLISHED_DEFINITION_CONTRACT:
                continue
            published = PublishedDefinition.model_validate(fields["manifest"])
            assert fields["manifest_digest"] == sha256_digest(fields["manifest"])
            assert published.ref.digest == sha256_digest(published.definition)
            assert fields["asset_id"] == definition_asset_id(
                published.ref.kind, published.ref.logical_id
            )
            assert fields["version"] == str(published.ref.revision) == "1"
            seen.add(published.ref.logical_id)
    assert {
        "skill.mission-control-coordinator",
        "prompt.coordinator.propose-workflow",
        "fixture.generic-stage-graph",
        "fixture.generic-goal-directed",
        "mcp.tavily",
        "mcp.firecrawl",
        "mcp.agent-browser",
    } <= seen


def test_changed_bytes_change_the_seed_digest() -> None:
    (bundle,) = [b for b in _bundles("common") if b["seed_key"] == "mc.catalog.workflow-parity"]
    original = {key: value for key, value in bundle.items() if key != "seed_digest"}
    changed = json.loads(json.dumps(original))
    changed["records"][0]["fields"]["manifest"]["family"] = "Invented"
    assert validate_bundle(changed)["seed_digest"] != bundle["seed_digest"]
    assert validate_bundle(original)["seed_digest"] == bundle["seed_digest"]
