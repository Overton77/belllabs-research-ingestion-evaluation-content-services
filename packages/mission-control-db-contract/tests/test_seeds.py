"""Seed engine proofs on real installs (B uses a non-superuser owner under FORCE RLS)."""

from __future__ import annotations

import asyncio
import copy
import json
from typing import Any
from uuid import UUID

import pytest
from conftest import VERSIONS, run, write_target
from test_install import install

from mission_control_db_contract.errors import ContractError
from mission_control_db_contract.seeds import (
    apply_bundle,
    order_bundles,
    seed_apply,
    seed_plan,
    validate_bundle,
)
from mission_control_db_contract.target import connect, load_target

DIGEST = "sha256:" + "a" * 64


def tenant_bundle(version: str = "1.0.0") -> dict[str, Any]:
    return {
        "format": "mission-control-seed/v1",
        "seed_key": "mc.qualification.parity",
        "seed_version": version,
        "component_compatibility": ">=1.0.0 <2.0.0",
        "depends_on": [],
        "actor_ref": "seed:mc.qualification.parity",
        "records": [
            {
                "kind": "tenant",
                "logical_key": "tenant.qualification",
                "fields": {
                    "external_tenant_ref": "qualification-tenant",
                    "state": "active",
                    "qualification_fixture": True,
                },
            },
            {
                "kind": "actor_binding",
                "logical_key": "actor.qualification-operator",
                "fields": {
                    "tenant": "tenant.qualification",
                    "issuer": "https://issuer.invalid",
                    "subject": "operator-1",
                    "actor_ref": "actor:operator-1",
                    "actor_kind": "human",
                    "state": "active",
                },
            },
            {
                "kind": "actor_grant",
                "logical_key": "grant.qualification-operator.missions",
                "fields": {
                    "tenant": "tenant.qualification",
                    "actor_binding": "actor.qualification-operator",
                    "scope": "missions:write",
                    "resource_selector": "*",
                    "policy_ref": "policy:qualification",
                    "valid_from": "2026-10-01T00:00:00+00:00",
                },
            },
        ],
    }


def catalog_bundle() -> dict[str, Any]:
    return {
        "format": "mission-control-seed/v1",
        "seed_key": "mc.catalog.approved-assets",
        "seed_version": "1.0.0",
        "component_compatibility": ">=1.0.0",
        "depends_on": [{"seed_key": "mc.qualification.parity", "seed_version": "1.0.0"}],
        "actor_ref": "seed:mc.catalog.approved-assets",
        "records": [
            {
                "kind": "asset_version",
                "logical_key": "asset.skill.mission-control-coordinator@1",
                "fields": {
                    "asset_id": "skill.mission-control-coordinator",
                    "version": "1",
                    "kind": "skill",
                    "contract": "skill/1",
                    "manifest_ref": "bundle://skills/mission-control",
                    "manifest_digest": DIGEST,
                    "manifest": {"name": "mission-control"},
                    "required_compatibility": ["mission-control-runtime/1"],
                    "status": "admitted",
                },
            },
            {
                "kind": "asset_decision",
                "logical_key": "decision.skill.mission-control-coordinator@1.admit",
                "fields": {
                    "asset_version": "asset.skill.mission-control-coordinator@1",
                    "decision": "admit",
                    "disposition": "approved",
                    "actor_ref": "actor:operator-1",
                    "evidence_refs": ["review:fixture"],
                    "policy_ref": "policy:catalog",
                    "decided_at": "2026-10-02T00:00:00+00:00",
                },
            },
            {
                "kind": "capability_grant",
                "logical_key": "capgrant.qualification.coordinator",
                "fields": {
                    "tenant": "tenant.qualification",
                    "asset_version": "asset.skill.mission-control-coordinator@1",
                    "actor_selector": "actor:operator-1",
                    "resource_selector": "*",
                    "allowed_invocation_classes": ["skill"],
                    "allowed_side_effect_classes": [],
                    "ceilings": {"calls": 10},
                    "valid_from": "2026-10-01T00:00:00+00:00",
                },
            },
        ],
    }


def setup_target(make_db, release, tmp_path, monkeypatch, which="B"):
    db = make_db(which)
    target = load_target(write_target(tmp_path, db, monkeypatch=monkeypatch))
    install(target, release)
    return db, target


def apply(target, release, bundles):
    return run(
        seed_apply(
            target,
            release,
            [validate_bundle(copy.deepcopy(b)) for b in bundles],
            confirmation=f"{target['project_ref']}:{target['installation_id']}",
            **VERSIONS,
        )
    )


def test_seed_apply_replay_conflict_and_identity_reuse(make_db, release, tmp_path, monkeypatch):
    db, target = setup_target(make_db, release, tmp_path, monkeypatch)
    bundles = [catalog_bundle(), tenant_bundle()]  # deliberately out of dependency order
    plan = run(seed_plan(target, release, [validate_bundle(b) for b in bundles], **VERSIONS))
    assert plan["dependency_order"] == [
        "mc.qualification.parity@1.0.0",
        "mc.catalog.approved-assets@1.0.0",
    ]
    assert [b["status"] for b in plan["bundles"]] == ["pending", "pending"]
    first = apply(target, release, bundles)
    assert [b["outcome"] for b in first["bundles"]] == ["applied", "applied"]
    receipt = first["bundles"][0]["receipt"]
    assert receipt["record_counts"]["created"] == 3
    ids = run(
        db.fetchval(
            "SELECT json_object_agg(logical_key, record_id)::text FROM "
            "mission_control.seed_identity"
        )
    )
    for value in json.loads(ids).values():
        assert UUID(value).version == 7
    second = apply(target, release, bundles)
    assert [b["outcome"] for b in second["bundles"]] == ["replay", "replay"]
    assert second["bundles"][0]["receipt"] == receipt
    assert (
        run(
            db.fetchval(
                "SELECT json_object_agg(logical_key, record_id)::text FROM "
                "mission_control.seed_identity"
            )
        )
        == ids
    )
    # Changed bytes under the same key/version conflict; nothing else changes.
    changed = tenant_bundle()
    changed["records"][0]["fields"]["external_tenant_ref"] = "other"
    with pytest.raises(ContractError, match="different digest") as error:
        apply(target, release, [changed])
    assert "SEED_CONFLICT" == error.value.code
    plan = run(seed_plan(target, release, [validate_bundle(changed)], **VERSIONS))
    assert plan["status"] == "hold" and plan["bundles"][0]["status"] == "conflict"
    # A new version that edits an admitted asset in place is refused (append-only).
    edit = catalog_bundle()
    edit["seed_version"] = "1.1.0"
    edit["records"][0]["fields"]["manifest"] = {"name": "edited"}
    with pytest.raises(ContractError, match="no last-write-wins"):
        apply(target, release, [edit])
    assert run(db.fetchval("SELECT count(*) FROM mission_control.installation_seed_receipt")) == 2
    assert run(db.fetchval("SELECT manifest->>'name' FROM mission_control.asset_version")) == (
        "mission-control"
    )


def test_revoked_grants_are_never_revived(make_db, release, tmp_path, monkeypatch):
    db, target = setup_target(make_db, release, tmp_path, monkeypatch, which="A")
    apply(target, release, [tenant_bundle(), catalog_bundle()])
    revoke = tenant_bundle("1.1.0")
    revoke["records"][2]["fields"]["revocation_reason"] = "access review"
    result = apply(target, release, [revoke])
    assert result["bundles"][0]["receipt"]["record_counts"]["revoked"] == 1
    reason = run(db.fetchval("SELECT revocation_reason FROM mission_control.actor_grant"))
    assert reason == "access review"
    revive = tenant_bundle("1.2.0")  # omits revocation: would re-activate the grant
    with pytest.raises(ContractError, match="cannot be revived"):
        apply(target, release, [revive])
    # Database trigger also forbids revival outside the seed engine.
    with pytest.raises(Exception, match="terminal revocation"):
        run(
            db.execute(
                "UPDATE mission_control.actor_grant SET revoked_at=NULL, revocation_reason=NULL"
            )
        )
    cap = catalog_bundle()
    cap["seed_version"] = "1.1.0"
    cap["records"][2]["fields"]["revoked"] = True
    apply(target, release, [cap])
    unrevoke = catalog_bundle()
    unrevoke["seed_version"] = "1.2.0"
    with pytest.raises(ContractError, match="cannot be revived"):
        apply(target, release, [unrevoke])
    assert run(db.fetchval("SELECT revoked_at IS NOT NULL FROM mission_control.capability_grant"))


def test_concurrent_equal_seeders_apply_once(make_db, release, tmp_path, monkeypatch):
    db, target = setup_target(make_db, release, tmp_path, monkeypatch)
    bundle = validate_bundle(tenant_bundle())

    async def one() -> dict[str, Any]:
        connection = await connect(target, readonly=False)
        try:
            return await apply_bundle(connection, target, release, bundle, **VERSIONS)
        finally:
            await connection.close()

    async def both() -> list[dict[str, Any]]:
        return await asyncio.gather(one(), one())

    outcomes = sorted(r["outcome"] for r in run(both()))
    assert outcomes == ["applied", "replay"]
    assert run(db.fetchval("SELECT count(*) FROM mission_control.tenant")) == 1  # superuser view
    assert run(db.fetchval("SELECT count(*) FROM mission_control.seed_identity")) == 3
    # Differing concurrent content: one winner, one conflict.
    a, b = tenant_bundle("2.0.0"), tenant_bundle("2.0.0")
    a["records"][0]["logical_key"] = "tenant.second"
    a["records"][1]["fields"]["tenant"] = "tenant.second"
    a["records"][2]["fields"]["tenant"] = "tenant.second"
    a["records"][0]["fields"]["external_tenant_ref"] = "second"
    a["records"][1]["logical_key"] = "actor.second"
    a["records"][2]["fields"]["actor_binding"] = "actor.second"
    a["records"][2]["logical_key"] = "grant.second"
    b = copy.deepcopy(a)
    b["records"][2]["fields"]["scope"] = "missions:read"
    va, vb = validate_bundle(a), validate_bundle(b)

    async def race() -> list[Any]:
        async def go(bundle: dict[str, Any]) -> Any:
            connection = await connect(target, readonly=False)
            try:
                return await apply_bundle(connection, target, release, bundle, **VERSIONS)
            except ContractError as exc:
                return exc
            finally:
                await connection.close()

        return await asyncio.gather(go(va), go(vb))

    results = run(race())
    assert sum(isinstance(r, dict) and r["outcome"] == "applied" for r in results) == 1
    assert sum(isinstance(r, ContractError) and r.code == "SEED_CONFLICT" for r in results) == 1


def test_failed_bundle_leaves_no_partial_admission(make_db, release, tmp_path, monkeypatch):
    db, target = setup_target(make_db, release, tmp_path, monkeypatch)
    bad = tenant_bundle()
    # Valid JSON shape, but the grant's valid_until precedes valid_from (CHECK violation
    # raised by the database after the tenant and binding were inserted).
    bad["records"][2]["fields"]["valid_until"] = "2020-01-01T00:00:00+00:00"
    with pytest.raises(ContractError, match="23514"):
        apply(target, release, [bad])
    for table in ("installation_seed_receipt", "seed_identity"):
        assert run(db.fetchval(f"SELECT count(*) FROM mission_control.{table}")) == 0
    assert run(db.fetchval("SELECT count(*) FROM mission_control.tenant")) == 0  # superuser view


def test_bundle_validation_and_dependency_closure():
    assert validate_bundle(tenant_bundle())["seed_digest"].startswith("sha256:")
    for mutate, message in (
        (lambda b: b.update(seed_key="other.key"), "seed_key"),
        (lambda b: b.update(seed_version="1.0"), "semantic"),
        (lambda b: b.update(extra=1), "unknown fields"),
        (lambda b: b["records"][0].update(kind="mission"), "Unsupported"),
        (lambda b: b["records"][0]["fields"].update(surprise=1), "Unknown fields"),
        (lambda b: b["records"][0]["fields"].update(state="deleted"), "Invalid state"),
        (lambda b: b["records"][2]["fields"].update(valid_from="2026-10-01"), "valid_from"),
        (lambda b: b["records"].append(copy.deepcopy(b["records"][0])), "Duplicate"),
    ):
        bundle = tenant_bundle()
        mutate(bundle)
        with pytest.raises(ContractError, match=message):
            validate_bundle(bundle)
    catalog = validate_bundle(catalog_bundle())
    with pytest.raises(ContractError, match="neither supplied nor applied"):
        order_bundles([catalog], set())
    assert order_bundles([catalog], {("mc.qualification.parity", "1.0.0")}) == [catalog]
    orphan = tenant_bundle()
    orphan["records"] = orphan["records"][1:]
    with pytest.raises(ContractError, match="outside the dependency closure"):
        order_bundles([validate_bundle(orphan)], set())
    cyc_a, cyc_b = tenant_bundle(), tenant_bundle("2.0.0")
    cyc_a["depends_on"] = [{"seed_key": "mc.qualification.parity", "seed_version": "2.0.0"}]
    cyc_b["depends_on"] = [{"seed_key": "mc.qualification.parity", "seed_version": "1.0.0"}]
    with pytest.raises(ContractError, match="cycle"):
        order_bundles([validate_bundle(cyc_a), validate_bundle(cyc_b)], set())
