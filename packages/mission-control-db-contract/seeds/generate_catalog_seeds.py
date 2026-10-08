"""Deterministically (re)generate the minimal Mission Control catalog seed bundles.

Every record is derived from a definition or artifact that exists in this repository;
nothing is invented. Run from the Mission Control repository root (``SEEDS`` =
``packages/mission-control-db-contract/seeds``):

    uv run --no-sync python SEEDS/generate_catalog_seeds.py --check
    uv run --no-sync python SEEDS/generate_catalog_seeds.py --write

``--check`` fails when a committed bundle differs from what the sources produce (a source
changed without a new seed version). Bundles never contain secrets, tenants/actors/grants
for real users, missions, runs, receipts, usage or embeddings.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import tomllib
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[3]
SEEDS_ROOT = Path(__file__).resolve().parent
SEED_FORMAT = "mission-control-seed/v1"
COMPATIBILITY = ">=1.0.0 <2.0.0"
SEED_ACTOR = "seed:mission-control-catalog"
SEED_TIME = datetime(2026, 10, 3, tzinfo=UTC)
SEED_TIME_TEXT = "2026-10-03T00:00:00Z"
POLICY_REF = "mission-control.seed-admission/1"
APPS = ("biotech", "ai-engineer")


def _sha256_bytes(payload: bytes) -> str:
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def _canonical_digest(value: Any) -> str:
    """Digest of compact sorted JSON (mission_control_db_contract.canonical.digest_value)."""
    encoded = json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str
    ).encode("utf-8")
    return _sha256_bytes(encoded)


def _repo_file(relative: str) -> bytes:
    # Line-ending normalized so digests do not depend on the checkout platform.
    return (REPO_ROOT / relative).read_bytes().replace(b"\r\n", b"\n")


def _admit(logical_key: str, asset_key: str, evidence: list[str]) -> dict[str, Any]:
    return {
        "kind": "asset_decision",
        "logical_key": logical_key,
        "fields": {
            "asset_version": asset_key,
            "decision": "admit",
            "disposition": "seeded-from-reviewed-source",
            "actor_ref": SEED_ACTOR,
            "evidence_refs": evidence,
            "policy_ref": POLICY_REF,
            "decided_at": SEED_TIME_TEXT,
        },
    }


def _asset(
    logical_key: str,
    *,
    asset_id: str,
    version: str,
    kind: str,
    contract: str,
    manifest_ref: str,
    manifest: dict[str, Any],
    manifest_digest: str | None = None,
    host_support: dict[str, Any] | None = None,
    secret_refs: list[str] | None = None,
) -> dict[str, Any]:
    fields: dict[str, Any] = {
        "asset_id": asset_id,
        "version": version,
        "kind": kind,
        "contract": contract,
        "manifest_ref": manifest_ref,
        "manifest_digest": manifest_digest or _canonical_digest(manifest),
        "manifest": manifest,
        "required_compatibility": [COMPATIBILITY],
        "status": "admitted",
    }
    # Agent-composition rows (migration 0025) carry their host-support matrix and secret
    # reference names; older rows omit both so their bundles stay byte-identical.
    if host_support is not None:
        fields["host_support"] = host_support
    if secret_refs:
        fields["secret_refs"] = secret_refs
    return {"kind": "asset_version", "logical_key": logical_key, "fields": fields}


def _published_definition_records(
    definition: Any,
    key_prefix: str,
    evidence: list[str],
    *,
    published_at: datetime = SEED_TIME,
) -> list[dict[str, Any]]:
    """Asset rows readable by PostgresDefinitionRepository (revision 1, exact digest)."""
    from mission_control.adapters.postgres.control_plane.catalog_assets import (
        ASSET_KIND,
        PUBLISHED_DEFINITION_CONTRACT,
        definition_asset_id,
        definition_manifest_ref,
    )
    from mission_control.domain.authoring.canonical import (
        canonical_data,
        sha256_digest,
        stable_json_dump,
    )
    from mission_control.domain.authoring.contracts import ExactDefinitionRef, PublishedDefinition

    ref = ExactDefinitionRef(
        kind=definition.kind,
        logical_id=definition.logical_id,
        revision=1,
        digest=sha256_digest(definition),
    )
    published = PublishedDefinition(
        ref=ref, definition=definition, published_at=published_at, published_by=SEED_ACTOR
    )
    manifest = canonical_data(stable_json_dump(published))["payload"]
    # Seed logical keys admit no underscore, so mcp_server/mcp_tool are spelled with hyphens.
    asset_key = f"{key_prefix}:{definition.kind.value.replace('_', '-')}:{definition.logical_id}/1"
    host_support = getattr(definition, "host_support", None)
    support = (
        host_support.model_dump(mode="json")
        if host_support is not None and host_support.profiles
        else None
    )
    return [
        _asset(
            asset_key,
            asset_id=definition_asset_id(definition.kind, definition.logical_id),
            version="1",
            kind=ASSET_KIND[definition.kind],
            contract=PUBLISHED_DEFINITION_CONTRACT,
            manifest_ref=definition_manifest_ref(definition.kind, definition.logical_id, 1),
            manifest=manifest,
            manifest_digest=sha256_digest(manifest),
            host_support=support,
            secret_refs=list(getattr(definition, "secret_refs", ())),
        ),
        _admit(asset_key + ":admit", asset_key, evidence),
    ]


class _NameRecorder:
    def __getattr__(self, name: str) -> str:
        return name


def workflow_parity() -> dict[str, Any]:
    from mission_control.adapters.temporal.registration.activities import coordinator_activities
    from mission_control.adapters.temporal.registration.workflows import coordinator_workflows

    sources = [
        "src/mission_control/domain/authoring/contracts.py",
        "src/mission_control/adapters/temporal/registration/workflows.py",
        "src/mission_control/adapters/temporal/registration/activities.py",
    ]
    records: list[dict[str, Any]] = []
    for family, slug, blueprint in (
        ("StageGraph", "stagegraph", "StageGraphBlueprint"),
        ("GoalDirected", "goal-directed", "GoalDirectedBlueprint"),
    ):
        manifest = {
            "vocabulary": "mission-control.workflow-family/1",
            "family": family,
            "blueprint_contract": f"mission_control.domain.authoring.contracts.{blueprint}",
            "temporal_workflows": sorted(item.__name__ for item in coordinator_workflows(family)),
            "temporal_activities": sorted(coordinator_activities(family, _NameRecorder())),
            "source_refs": sources,
        }
        key = f"workflow-family:{slug}/1"
        records.append(
            _asset(
                key,
                asset_id=f"workflow-family:{slug}",
                version="1",
                kind="policy",
                contract="mission-control.workflow-family/1",
                manifest_ref=f"repo://src/mission_control/adapters/temporal/registration#{family}",
                manifest=manifest,
            )
        )
        records.append(_admit(key + ":admit", key, sources))
    return {
        "format": SEED_FORMAT,
        "seed_key": "mc.catalog.workflow-parity",
        "seed_version": "1.0.0",
        "component_compatibility": COMPATIBILITY,
        "depends_on": [],
        "actor_ref": SEED_ACTOR,
        "description": (
            "Workflow-family vocabulary actually registered by the validator/compiler and the "
            "Temporal worker: StageGraph and GoalDirected only. No product Workflow Types "
            "(those are application-owned), no missions or runs."
        ),
        "records": records,
    }


def runtime_profiles() -> dict[str, Any]:
    descriptor_path = "packages/mission-control-db-contract/runtime/descriptor.json"
    descriptor_bytes = _repo_file(descriptor_path)
    descriptor = json.loads(descriptor_bytes)
    graphs_path = "agent_server/langgraph.json"
    graphs_bytes = _repo_file(graphs_path)
    graphs = json.loads(graphs_bytes)
    persistence = {
        "profile": "mission-control.runtime-persistence/1",
        "descriptor_ref": descriptor_path,
        "descriptor_sha256": _sha256_bytes(descriptor_bytes),
        "descriptor_schema": descriptor["descriptor_schema"],
        "schema": descriptor["schema"],
        "role": descriptor["role"],
        "pins": descriptor["pins"],
    }
    server = {
        "profile": "mission-control.agent-server-graphs/1",
        "config_ref": graphs_path,
        "config_sha256": _sha256_bytes(graphs_bytes),
        "graphs": graphs["graphs"],
        "python_version": graphs["python_version"],
        "production_native_persistence": "unqualified",
    }
    records: list[dict[str, Any]] = []
    for key, asset_id, contract, ref, manifest in (
        (
            "runtime-persistence:langgraph-postgres/1",
            "runtime-persistence:langgraph-postgres",
            "mission-control.runtime-persistence/1",
            "repo://" + descriptor_path,
            persistence,
        ),
        (
            "agent-server:graph-pins/1",
            "agent-server:graph-pins",
            "mission-control.agent-server-graphs/1",
            "repo://" + graphs_path,
            server,
        ),
    ):
        records.append(
            _asset(
                key,
                asset_id=asset_id,
                version="1",
                kind="profile",
                contract=contract,
                manifest_ref=ref,
                manifest=manifest,
            )
        )
        records.append(_admit(key + ":admit", key, [ref.removeprefix("repo://")]))
    return {
        "format": SEED_FORMAT,
        "seed_key": "mc.catalog.runtime-profiles",
        "seed_version": "1.0.0",
        "component_compatibility": COMPATIBILITY,
        "depends_on": [{"seed_key": "mc.catalog.workflow-parity", "seed_version": "1.0.0"}],
        "actor_ref": SEED_ACTOR,
        "description": (
            "Pinned runtime persistence descriptor and Agent Server graph pins as present in this "
            "repository. Pins only; no provider, model route or credential is seeded."
        ),
        "records": records,
    }


def approved_assets() -> dict[str, Any]:
    from mission_control.application.coordinator.coordinator_surface_promotion import (
        build_coordinator_surface,
    )

    skill_root = ".agents/skills/mission-control-coordinator"
    skill, prompt = build_coordinator_surface(REPO_ROOT / skill_root)
    evidence = [
        "src/mission_control/application/coordinator/coordinator_surface_promotion.py",
        skill_root,
    ]
    records = _published_definition_records(skill, "definition", evidence)
    records += _published_definition_records(prompt, "definition", evidence)
    manifest_path = "skills/mission-control/manifest.json"
    bundle = json.loads(_repo_file(manifest_path))
    key = f"skill-bundle-manifest:{bundle['name']}/{bundle['version']}"
    records.append(
        _asset(
            key,
            asset_id=f"skill-bundle-manifest:{bundle['name']}",
            version=bundle["version"],
            kind="skill",
            contract=bundle["schema_version"],
            manifest_ref="repo://" + manifest_path,
            manifest=bundle,
        )
    )
    records.append(_admit(key + ":admit", key, [manifest_path]))
    return {
        "format": SEED_FORMAT,
        "seed_key": "mc.catalog.approved-assets",
        "seed_version": "1.0.0",
        "component_compatibility": COMPATIBILITY,
        "depends_on": [{"seed_key": "mc.catalog.runtime-profiles", "seed_version": "1.0.0"}],
        "actor_ref": SEED_ACTOR,
        "description": (
            "Reviewed assets that exist in this repository: skill.mission-control-coordinator and "
            "prompt.coordinator.propose-workflow (published-definition revision 1) and the "
            "canonical skills/mission-control bundle manifest. Skill bytes are not uploaded here; "
            "remote byte admission remains a separate capability-bundles registration."
        ),
        "records": records,
    }


# Applied seed bytes are immutable. `mc.app.bindings@1.0.0` was applied to both live
# projects on 2026-10-03 while targets still held operator placeholders; it is frozen here
# by file SHA-256 and never regenerated. Later target facts ship as a new seed version
# with a new asset version, so nothing is overwritten.
FROZEN_BUNDLES = {
    # Applied to both live projects on 2026-10-03 (seed digest sha256:f45e04f9...). Its
    # sources moved on (FT-A1 relabelled the coordinator skill kind, the router manifest is
    # newer), so the successor below carries only the logical keys 1.0.0 does not hold.
    "common/mc.catalog.approved-assets-1.0.0.json": (
        "7c0e1f497726f776aeafc943b3eda9207c6c932b1529e37b44dae2b6b464ec1b"
    ),
    "biotech/mc.app.bindings-1.0.0.json": (
        "4cc5c1203b0fd0b9cabc8e0ebcaba2662cda427a4e5f4b876eb2f9b7790802c4"
    ),
    "ai-engineer/mc.app.bindings-1.0.0.json": (
        "a1364312eac8ad4fd2512888d1312f5eb574f8e261476dc339923fc9d4f493cb"
    ),
}
CURRENT_BINDING = ("1.0.1", "2")  # (seed_version, asset version)


def frozen_bundle(relative: str) -> dict[str, Any]:
    payload = (SEEDS_ROOT / relative).read_bytes()
    if _sha256_bytes(payload) != "sha256:" + FROZEN_BUNDLES[relative]:
        raise SystemExit(f"frozen applied seed bundle changed: {relative}")
    return dict(json.loads(payload))


APPROVED_ASSETS_FROZEN = "common/mc.catalog.approved-assets-1.0.0.json"


def approved_assets_successor() -> dict[str, Any]:
    """`mc.catalog.approved-assets@1.0.1`: the approved-asset records whose logical keys the
    applied 1.0.0 bundle does not hold (today the newer `skills/mission-control` manifest).

    Records 1.0.0 already holds are never re-emitted, even when their sources changed: an
    applied logical key with different bytes is a seed conflict. A changed definition needs
    a new definition revision (an owner decision), not new bytes under revision 1.
    """

    frozen = frozen_bundle(APPROVED_ASSETS_FROZEN)
    held = {record["logical_key"] for record in frozen["records"]}
    current = approved_assets()
    return {
        **current,
        "seed_version": "1.0.1",
        "depends_on": [{"seed_key": "mc.catalog.approved-assets", "seed_version": "1.0.0"}],
        "description": (
            "Successor of mc.catalog.approved-assets@1.0.0 (applied, frozen): the reviewed "
            "approved assets whose logical keys 1.0.0 does not hold, currently the canonical "
            "skills/mission-control bundle manifest at its present version. The coordinator "
            "skill and prompt definitions stay at the applied revision 1."
        ),
        "records": [record for record in current["records"] if record["logical_key"] not in held],
    }


def app_bindings(app: str) -> dict[str, Any]:
    seed_version, asset_version = CURRENT_BINDING
    target_path = f"deployments/{app}/target.toml"
    target_bytes = _repo_file(target_path)
    target = tomllib.loads(target_bytes.decode("utf-8"))["target"]
    unresolved = sorted(
        name
        for name, value in target.items()
        if isinstance(value, str) and value.startswith("REPLACE")
    )
    manifest = {
        "binding": "mission-control.app-binding/1",
        "application_id": target["application_id"],
        "project_label": target["project_label"],
        "project_ref": target["project_ref"],
        "installation_id": target["installation_id"],
        "target_ref": target_path,
        "target_sha256": _sha256_bytes(target_bytes),
        "secret_reference_names": {"migration_database_url_env": target["database_url_env"]},
        "common_catalog_seeds": [
            "mc.catalog.workflow-parity@1.0.0",
            "mc.catalog.runtime-profiles@1.0.0",
            "mc.catalog.approved-assets@1.0.0",
        ],
        "unresolved_operator_fields": unresolved,
        "approved_tenant_actor_mappings": [],
        "supersedes": f"app-binding:{app}@1",
    }
    key = f"app-binding:{app}/{asset_version}"
    return {
        "format": SEED_FORMAT,
        "seed_key": "mc.app.bindings",
        "seed_version": seed_version,
        "component_compatibility": COMPATIBILITY,
        "depends_on": [
            {"seed_key": "mc.catalog.approved-assets", "seed_version": "1.0.0"},
            {"seed_key": "mc.app.bindings", "seed_version": "1.0.0"},
        ],
        "actor_ref": SEED_ACTOR,
        "description": (
            f"Installation binding for app '{app}': identity facts copied from {target_path} and "
            "secret reference NAMES only. No tenant/actor mapping or grant is seeded until "
            "owner-approved (approved_tenant_actor_mappings is a documented empty placeholder)."
        ),
        "records": [
            _asset(
                key,
                asset_id=f"app-binding:{app}",
                version=asset_version,
                kind="profile",
                contract="mission-control.app-binding/1",
                manifest_ref="repo://" + target_path,
                manifest=manifest,
            ),
            _admit(key + ":admit", key, [target_path]),
        ],
    }


def qualification_parity() -> dict[str, Any]:
    from mission_control.domain.authoring.fixtures import (
        GENERIC_GOAL_DIRECTED,
        GENERIC_STAGE_GRAPH,
    )

    evidence = ["src/mission_control/domain/authoring/fixtures.py"]
    records: list[dict[str, Any]] = [
        {
            "kind": "tenant",
            "logical_key": "qualification:mission-control-parity",
            "fields": {
                "external_tenant_ref": "qualification:mission-control-parity",
                "state": "active",
                "qualification_fixture": True,
            },
        }
    ]
    records += _published_definition_records(GENERIC_STAGE_GRAPH, "qualification", evidence)
    records += _published_definition_records(GENERIC_GOAL_DIRECTED, "qualification", evidence)
    return {
        "format": SEED_FORMAT,
        "seed_key": "mc.qualification.parity",
        "seed_version": "1.0.0",
        "component_compatibility": COMPATIBILITY,
        "depends_on": [{"seed_key": "mc.catalog.workflow-parity", "seed_version": "1.0.0"}],
        "actor_ref": SEED_ACTOR,
        "description": (
            "OPT-IN SYNTHETIC QUALIFICATION FIXTURE. A clearly labeled qualification tenant "
            "(qualification_fixture=true) and the deterministic generic StageGraph/GoalDirected "
            "contract-fixture blueprints. Not production provider support, not real usage; no "
            "actors, grants, missions, runs or receipts."
        ),
        "records": records,
    }


def agent_capabilities() -> dict[str, dict[str, Any]]:
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "mc_seed_agent_capabilities", SEEDS_ROOT / "agent_capabilities.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    def published(definition: Any, key_prefix: str, evidence: list[str]) -> list[dict[str, Any]]:
        return _published_definition_records(
            definition, key_prefix, evidence, published_at=module.AGENT_SEED_TIME
        )

    return dict(module.bundles(published, SEED_FORMAT, COMPATIBILITY, SEED_ACTOR))


def agent_skills() -> dict[str, dict[str, Any]]:
    """FT-A7: per-application skill bundles, the policy hook and the web-research plugin."""
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "mc_seed_agent_skills", SEEDS_ROOT / "agent_skills.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    def published(definition: Any, key_prefix: str, evidence: list[str]) -> list[dict[str, Any]]:
        return _published_definition_records(
            definition, key_prefix, evidence, published_at=module.AGENT_SKILL_SEED_TIME
        )

    return dict(module.bundles(published, SEED_FORMAT, COMPATIBILITY, SEED_ACTOR))


def storage_capability_bundles() -> dict[str, Any]:
    """FT-A2 / ADR-0024: the private capability-bundles bucket and its four policies.

    ``${application_id}`` resolves to the target application at apply time, so one common
    bundle scopes every policy to that application's top-level prefix.
    """

    def policy(key: str, command: str, role: str, *, restrictive: bool = False) -> dict:
        fields: dict[str, Any] = {
            "bucket_id": "capability-bundles",
            "policy": f"mc_capability_bundles_{key.replace('-', '_')}",
            "command": command,
            "capability_role": role,
        }
        if restrictive:
            fields["restrictive"] = True
        return {
            "kind": "storage_policy",
            "logical_key": f"storage:capability-bundles:{key}",
            "fields": fields,
        }

    return {
        "format": SEED_FORMAT,
        "seed_key": "mc.storage.capability-bundles",
        "seed_version": "1.0.0",
        "component_compatibility": COMPATIBILITY,
        "depends_on": [],
        "actor_ref": SEED_ACTOR,
        "description": (
            "Private capability-bundles bucket (50 MiB per object, any MIME type) and its "
            "storage.objects policies, each scoped to the application's top-level prefix: "
            "publisher INSERT, publisher SELECT (verify before treating an existing path as "
            "published), reader SELECT (signed download URLs), and a restrictive prefix guard. "
            "No UPDATE or DELETE policy exists, so a digest path is never overwritten. "
            "Targets without the Storage schema report this bundle blocked."
        ),
        "records": [
            {
                "kind": "storage_bucket",
                "logical_key": "storage:capability-bundles",
                "fields": {
                    "bucket_id": "capability-bundles",
                    "public": False,
                    "file_size_limit": 52_428_800,
                },
            },
            policy("publisher-insert", "INSERT", "publisher"),
            policy("publisher-select", "SELECT", "publisher"),
            policy("reader-select", "SELECT", "reader"),
            policy("application-prefix-guard", "ALL", "*", restrictive=True),
        ],
    }


def build_bundles() -> dict[str, dict[str, Any]]:
    bundles = {
        "common/mc.catalog.workflow-parity-1.0.0.json": workflow_parity(),
        "common/mc.catalog.runtime-profiles-1.0.0.json": runtime_profiles(),
        "common/mc.catalog.approved-assets-1.0.1.json": approved_assets_successor(),
        "qualification/mc.qualification.parity-1.0.0.json": qualification_parity(),
    }
    for relative in FROZEN_BUNDLES:
        bundles[relative] = frozen_bundle(relative)
    for app in APPS:
        bundles[f"{app}/mc.app.bindings-{CURRENT_BINDING[0]}.json"] = app_bindings(app)
    bundles.update(agent_capabilities())
    bundles.update(agent_skills())
    bundles["common/mc.storage.capability-bundles-1.0.0.json"] = storage_capability_bundles()
    return bundles


def encode(bundle: dict[str, Any]) -> bytes:
    return (json.dumps(bundle, sort_keys=True, indent=2, ensure_ascii=False) + "\n").encode("utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--check", action="store_true")
    mode.add_argument("--write", action="store_true")
    args = parser.parse_args()
    drift = []
    for relative, bundle in build_bundles().items():
        path = SEEDS_ROOT / relative
        payload = encode(bundle)
        if args.write:
            if relative in FROZEN_BUNDLES:
                continue  # applied bytes are never rewritten
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(payload)
        elif not path.exists() or json.loads(path.read_bytes()) != bundle:
            drift.append(relative)
    if drift:
        print("seed bundles drifted from their sources: " + ", ".join(drift), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
