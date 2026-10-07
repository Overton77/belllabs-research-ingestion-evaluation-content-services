"""Two disposable databases, two different protected domains, one identical release.

Independent of the implementation lanes' own tests: installation uses the real
``mission-db`` library calls, but every comparison below uses the reviewer's own
catalog normalization (``catalog_evidence``) in addition to the package fingerprint.
"""

from __future__ import annotations

from typing import Any

import pytest

from tests.qualification.two_project import catalog_evidence as ce
from tests.qualification.two_project.conftest import APPS, require

pytestmark = pytest.mark.common_db

CTX_FUNCTIONS = ("ctx_installation_id", "ctx_application_id", "ctx_tenant_id")
TENANT_COLUMNS = {"installation_id", "application_id", "tenant_id"}


def _app(evidence: dict[str, Any], app: str) -> dict[str, Any]:
    return evidence["apps"][app]


def test_server_is_qualified_major(two_project_evidence: dict[str, Any]) -> None:
    require(two_project_evidence, "server_major")
    assert two_project_evidence["server_major"] == 17


def test_both_targets_apply_every_migration_then_replay_as_noop(
    two_project_evidence: dict[str, Any],
) -> None:
    for app in APPS:
        require(
            two_project_evidence, f"apps/{app}/install", f"apps/{app}/replay", f"apps/{app}/verify"
        )
    keys = [m["key"] for m in two_project_evidence["release"]["ordered_migrations"]]
    assert keys, "release declares no migrations"
    for app in APPS:
        entry = _app(two_project_evidence, app)
        assert entry["install"]["plan"]["plan"]["status"] == "pending", app
        assert entry["install"]["apply"]["outcome"] == "applied", app
        assert entry["install"]["apply"]["applied_migrations"] == keys, app
        assert entry["replay"]["plan"]["plan"]["status"] == "noop", app
        assert entry["replay"]["apply"]["outcome"] in {"noop", "noop_replay"}, app
        assert entry["replay"]["apply"]["applied_migrations"] == [], app
        assert entry["verify"]["status"] == "verified", app


def test_receipts_are_identical_and_match_the_manifest(
    two_project_evidence: dict[str, Any],
) -> None:
    for app in APPS:
        require(two_project_evidence, f"apps/{app}/receipts")
    manifest = [
        (m["key"], "sha256:" + m["sha256"])
        for m in two_project_evidence["release"]["ordered_migrations"]
    ]
    observed = {
        app: [
            (r["migration_key"], r["migration_digest"])
            for r in _app(two_project_evidence, app)["receipts"]
        ]
        for app in APPS
    }
    assert observed["biotech"] == manifest
    assert observed["ai-engineer"] == manifest


def test_independent_owned_fingerprints_are_equal_across_targets(
    two_project_evidence: dict[str, Any],
) -> None:
    for app in APPS:
        require(two_project_evidence, f"apps/{app}/owned_catalog")
    biotech = _app(two_project_evidence, "biotech")
    ai = _app(two_project_evidence, "ai-engineer")
    for schema in ce.OWNED_SCHEMAS:
        assert biotech["owned_catalog"][schema]["relations"], f"{schema} has no relations"
    differences = ce.diff_paths(biotech["owned_catalog"], ai["owned_catalog"])
    assert biotech["owned_digest"] == ai["owned_digest"], differences[:20]


def test_package_fingerprint_equal_across_targets_and_to_manifest(
    two_project_evidence: dict[str, Any],
) -> None:
    for app in APPS:
        require(two_project_evidence, f"apps/{app}/package_fingerprint")
    expected = two_project_evidence["release"]["manifest_schema_fingerprint"]
    for app in APPS:
        document = _app(two_project_evidence, app)["package_fingerprint"]
        assert document["schemas"] == sorted(ce.OWNED_SCHEMAS)
        assert document["fingerprint"] == expected, app
        assert (
            document["per_schema"]
            == two_project_evidence["release"]["manifest_schema_fingerprints"]
        ), app


def test_domains_are_genuinely_different(two_project_evidence: dict[str, Any]) -> None:
    for app in APPS:
        require(two_project_evidence, f"apps/{app}/protected_before")
    biotech = _app(two_project_evidence, "biotech")["protected_before"]
    ai = _app(two_project_evidence, "ai-engineer")["protected_before"]
    assert {"capability_search", "storage", "public", "extensions"} <= set(
        biotech["inventory"]["schemas"]
    )
    assert {"corpus", "knowledge", "temporal", "util", "public", "extensions"} <= set(
        ai["inventory"]["schemas"]
    )
    assert (
        biotech["data"]["relations"]["capability_search.capability_documents"]["row_count"] == 542
    )
    assert biotech["data"]["relations"]["storage.buckets"]["row_count"] == 3
    assert ai["data"]["sequences"], "sequence state must be part of the protected evidence"
    assert ce.digest(biotech["inventory"]["per_schema"]) != ce.digest(ai["inventory"]["per_schema"])


@pytest.mark.parametrize("app", APPS)
@pytest.mark.parametrize("stage", ["protected_after", "protected_after_replay"])
def test_protected_catalog_is_byte_identical(
    two_project_evidence: dict[str, Any], app: str, stage: str
) -> None:
    require(two_project_evidence, f"apps/{app}/protected_before", f"apps/{app}/{stage}")
    entry = _app(two_project_evidence, app)
    before = entry["protected_before"]["inventory"]
    after = entry[stage]["inventory"]
    assert before["schemas"], "protected schema discovery returned nothing"
    assert ce.digest(before) == ce.digest(after), ce.diff_paths(before, after)[:20]


@pytest.mark.parametrize("app", APPS)
@pytest.mark.parametrize("stage", ["protected_after", "protected_after_replay"])
def test_protected_rows_and_sequences_are_byte_identical(
    two_project_evidence: dict[str, Any], app: str, stage: str
) -> None:
    require(two_project_evidence, f"apps/{app}/protected_before", f"apps/{app}/{stage}")
    entry = _app(two_project_evidence, app)
    before = entry["protected_before"]["data"]
    after = entry[stage]["data"]
    assert before["relations"], "no protected relations were digested"
    assert ce.digest(before) == ce.digest(after), ce.diff_paths(before, after)[:20]


@pytest.mark.parametrize("app", APPS)
def test_every_tenant_table_forces_rls_with_all_three_context_functions(
    two_project_evidence: dict[str, Any], app: str
) -> None:
    require(two_project_evidence, f"apps/{app}/rls")
    tables = [
        row
        for row in _app(two_project_evidence, app)["rls"]
        if set(row["columns"]) >= TENANT_COLUMNS
    ]
    assert tables, "no tenant-scoped tables found in the common schemas"
    failures = []
    for row in tables:
        if not (row["rls"] and row["force_rls"]):
            failures.append(f"{row['relname']}: RLS enabled={row['rls']} forced={row['force_rls']}")
            continue
        if not row["policies"]:
            failures.append(f"{row['relname']}: no policy (forced RLS denies everything)")
        # Permissive policies OR together, so EVERY policy must carry the full scope.
        for policy in row["policies"]:
            using = policy["using"]
            check = policy["check"] or using
            if policy["cmd"] != "a" and not all(name in using for name in CTX_FUNCTIONS):
                failures.append(f"{row['relname']}.{policy['name']}: USING lacks full ctx scope")
            if policy["cmd"] not in {"r", "d"} and not all(name in check for name in CTX_FUNCTIONS):
                failures.append(f"{row['relname']}.{policy['name']}: CHECK lacks full ctx scope")
    assert not failures, failures


@pytest.mark.parametrize("app", APPS)
def test_installation_catalog_tables_force_rls_by_installation_and_application(
    two_project_evidence: dict[str, Any], app: str
) -> None:
    require(two_project_evidence, f"apps/{app}/rls")
    catalog = [
        row
        for row in _app(two_project_evidence, app)["rls"]
        if {"installation_id", "application_id"} <= set(row["columns"])
        and "tenant_id" not in row["columns"]
        and row["relname"] != "application_installation"
    ]
    failures = [
        row["relname"]
        for row in catalog
        if not (row["rls"] and row["force_rls"])
        or not any(
            "ctx_installation_id" in p["using"] and "ctx_application_id" in p["using"]
            for p in row["policies"]
        )
    ]
    assert not failures, failures


@pytest.mark.parametrize("app", APPS)
def test_capability_roles_are_restricted_and_own_nothing(
    two_project_evidence: dict[str, Any], app: str
) -> None:
    require(two_project_evidence, f"apps/{app}/capability_roles")
    roles = {row["rolname"]: row for row in _app(two_project_evidence, app)["capability_roles"]}
    declared = set(two_project_evidence["release"]["runtime_roles"])
    assert declared, "release declares no runtime capability roles"
    assert declared <= set(roles), sorted(declared - set(roles))
    for name, row in roles.items():
        for attribute in (
            "rolsuper",
            "rolbypassrls",
            "rolcanlogin",
            "rolcreaterole",
            "rolcreatedb",
            "rolreplication",
        ):
            assert row[attribute] is False, f"{name}.{attribute}"
        assert row["owned_objects"] == 0, f"{name} owns {row['owned_objects']} objects"
        assert not set(row["member_of"]) & set(roles), f"{name} inherits {row['member_of']}"


@pytest.mark.parametrize("app", APPS)
def test_owned_functions_pin_search_path_and_definers_deny_public(
    two_project_evidence: dict[str, Any], app: str
) -> None:
    require(two_project_evidence, f"apps/{app}/functions")
    functions = [f for f in _app(two_project_evidence, app)["functions"] if f["kind"] in {"f", "p"}]
    assert functions, "common schemas define no functions (ctx_* expected)"
    unpinned = [
        f["signature"]
        for f in functions
        if not any(item.startswith("search_path=pg_catalog") for item in f["config"])
    ]
    public_definers = [
        f["signature"] for f in functions if f["security_definer"] and f["public_execute"]
    ]
    assert not unpinned, unpinned
    assert not public_definers, public_definers
