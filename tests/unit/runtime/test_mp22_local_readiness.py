"""MP-22: the local run readiness gate refuses before any composition or paid call.

Pure checks over temporary files; the committed deployment examples are read as-is. The
cross-cluster guard is exercised with an in-memory presence probe here and against two real
Temporal namespaces in ``tests/integration/temporal/test_mp22_outage_drill.py``.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Mapping
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any

import pytest

from mission_control.adapters.capabilities.capability_pins import (
    CapabilityPins,
    read_skill_bundle,
)
from mission_control.application.authoring.manifest_launch_inputs import ManifestLaunchBindings
from mission_control.bootstrap import preflight
from mission_control.bootstrap.manifests import served_components
from mission_control.bootstrap.preflight import (
    UNSET_DIGEST,
    ClusterBinding,
    ClusterBindingLedger,
    ExpectedRelease,
    LocalRunProfile,
    ObservedRelease,
    Presence,
    ReadinessInputs,
    check_auth,
    check_bindings,
    check_lane_hosts,
    check_pins,
    compare_release,
    compose_launch_bindings,
    expected_release,
    guard_cluster_launch,
    readiness,
)
from mission_control.bootstrap.settings import Settings, get_settings
from mission_control.domain.authoring.canonical import sha256_digest

ROOT = Path(__file__).resolve().parents[3]
EXAMPLES = ROOT / "deployments" / "examples"
BINDINGS_EXAMPLE = EXAMPLES / "manifest-launch-bindings.deep-agents.example.json"
AUTH_EXAMPLE = EXAMPLES / "provider-auth-profiles.example.json"
PROFILE_EXAMPLE = EXAMPLES / "local-run-profile.example.json"
PINS = ROOT / "infra" / "capability-pins" / "research-capabilities.json"

EXPECTED_OWNER_POINTERS = {
    "/deep_agents/agent_profile_ref/digest",
    "/deep_agents/authority_refs/0",
    "/deep_agents/profile/backend_ref/digest",
    "/deep_agents/profile/context_assembly_ref/digest",
    "/deep_agents/profile/model/model_name",
    "/deep_agents/profile/prompt_refs/0/digest",
    "/deep_agents/profile/tracing_policy_ref/digest",
    "/deep_agents/redaction_policy_ref",
    "/deep_agents/sensitive_data_policy_ref",
    "/deep_agents/snapshot_policy_ref",
    "/deep_agents/tracing_policy_ref",
    "/deep_agents/workspace/environment_digest",
    "/deep_agents/workspace/image_digest",
    "/deep_agents/workspace/package_digest",
    "/deep_agents/workspace/provider",
    "/deep_agents/workspace/runtime_digest",
    "/model_profiles/frontier.default/model_name",
    "/model_profiles/frontier.long_context/model_name",
}


def settings_with(pins: Path) -> Settings:
    return get_settings().model_copy(update={"capability_pins_path": pins})


def profile(**changes: Any) -> LocalRunProfile:
    document = json.loads(PROFILE_EXAMPLE.read_text(encoding="utf-8"))
    document["temporal_clusters"] = document["temporal_clusters"][:1]
    document.update(changes)
    return LocalRunProfile.model_validate(document)


def write(path: Path, document: object) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(document), encoding="utf-8")
    return path


def owner_selections() -> dict[str, object]:
    """Values standing for an owner's reviewed choices (FIXTURE: not real policy refs)."""

    digest = sha256_digest("fixture-owner-choice")
    return {
        pointer: digest
        if pointer.endswith("digest")
        else ["authority:fixture"]
        if pointer == "/deep_agents/authority_refs"
        else f"fixture:{pointer.rsplit('/', 1)[-1]}"
        for pointer in (
            *(p for p in EXPECTED_OWNER_POINTERS if p != "/deep_agents/authority_refs/0"),
            "/deep_agents/authority_refs",
        )
    }


# --- The committed bindings example --------------------------------------------------------


def test_the_example_bindings_validate_and_bind_only_pinned_components() -> None:
    bindings = ManifestLaunchBindings.load(BINDINGS_EXAMPLE)
    pins = CapabilityPins.load(PINS)
    served = served_components(pins, get_settings())
    profile_ = bindings.deep_agents.profile
    assert profile_.checkpointer_ref.digest in served.checkpointers
    assert profile_.store_ref.digest in served.stores
    assert {item.ref.digest for item in bindings.model_profiles.values()} <= served.models
    assert {item.ref.digest for item in bindings.sandbox_profiles.values()} <= served.sandboxes
    assert set(bindings.model_profiles) == {"frontier.default", "frontier.long_context"}
    assert set(bindings.sandbox_profiles) == {"research.standard", "ingestion.standard"}
    assert bindings.deep_agents.placement.task_queue == "biotech-research-ingestion-agent-cognitive"
    for role, binding in bindings.deep_agents.goal_output_schemas.items():
        assert binding.schema_id == f"belllabs.goal-{role}-observation.v1" and binding.strict


def test_readiness_lists_every_owner_pointer_of_the_example_bindings() -> None:
    pins = CapabilityPins.load(PINS)
    bindings, issues = check_bindings(
        BINDINGS_EXAMPLE, served_components(pins, get_settings()), pins, get_settings()
    )
    assert bindings is not None
    unresolved = {
        issue.pointer.split("#", 1)[1] for issue in issues if issue.code == "BINDING_UNRESOLVED"
    }
    assert unresolved == EXPECTED_OWNER_POINTERS
    assert {issue.code for issue in issues} == {"BINDING_UNRESOLVED"}


def test_composing_owner_selections_reseals_digests_and_resolves_every_pointer(
    tmp_path: Path,
) -> None:
    base = json.loads(BINDINGS_EXAMPLE.read_text(encoding="utf-8"))
    composed = compose_launch_bindings(base, owner_selections())
    target = tmp_path / "bindings.json"
    target.write_text(composed.model_dump_json(), encoding="utf-8")
    pins = CapabilityPins.load(PINS)
    reloaded, issues = check_bindings(
        target, served_components(pins, get_settings()), pins, get_settings()
    )
    assert reloaded == composed
    assert issues == []
    assert (
        composed.deep_agents.profile.profile_digest
        != base["deep_agents"]["profile"]["profile_digest"]
    )


def test_a_hand_edit_inside_the_sealed_scaffold_is_refused(tmp_path: Path) -> None:
    document = json.loads(BINDINGS_EXAMPLE.read_text(encoding="utf-8"))
    document["deep_agents"]["placement"]["task_queue"] = "edited-by-hand"
    pins = CapabilityPins.load(PINS)
    bindings, issues = check_bindings(
        write(tmp_path / "edited.json", document),
        served_components(pins, get_settings()),
        pins,
        get_settings(),
    )
    assert bindings is None
    assert any(issue.code == "BINDINGS_INVALID" for issue in issues)


def test_a_missing_bindings_file_and_an_unserved_model_are_refused(tmp_path: Path) -> None:
    pins = CapabilityPins.load(PINS)
    served = served_components(pins, get_settings())
    missing, issues = check_bindings(tmp_path / "absent.json", served, pins, get_settings())
    assert missing is None and [issue.code for issue in issues] == ["BINDINGS_MISSING"]

    document = json.loads(BINDINGS_EXAMPLE.read_text(encoding="utf-8"))
    document["model_profiles"]["frontier.default"]["ref"]["digest"] = sha256_digest("unserved")
    _bindings, issues = check_bindings(
        write(tmp_path / "unserved.json", document), served, pins, get_settings()
    )
    (unserved,) = [issue for issue in issues if issue.code == "COMPONENT_UNSERVED"]
    assert unserved.pointer.endswith("#/model_profiles/frontier.default/ref")
    assert unserved.observed == sha256_digest("unserved")


# --- Profile, OS, auth ---------------------------------------------------------------------


def test_the_example_profile_validates_and_names_its_files() -> None:
    document = json.loads(PROFILE_EXAMPLE.read_text(encoding="utf-8"))
    parsed = LocalRunProfile.model_validate(document)
    assert (ROOT / parsed.manifest_launch_bindings).is_file()
    assert (ROOT / parsed.auth_profiles).is_file()
    assert parsed.cluster(parsed.new_run_cluster).target == "local"


@pytest.mark.asyncio
async def test_a_missing_profile_is_refused_before_anything_is_composed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def forbidden(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("readiness must refuse before composing the application")

    monkeypatch.setattr(preflight, "create_app", forbidden)
    monkeypatch.setattr(preflight, "configured_preflight", forbidden)
    report = await readiness(
        ReadinessInputs(profile_path=tmp_path / "absent.json"), settings_with(PINS)
    )
    assert not report.ready
    assert report.checks["profile"] == "failed"
    assert {report.checks[name] for name in ("lane_hosts", "launch_bindings", "db_release")} == {
        "skipped"
    }
    assert [issue.code for issue in report.issues if issue.code.startswith("PROFILE")] == [
        "PROFILE_MISSING"
    ]
    # The CLI exits blocked without reaching the configured-application preflight.
    code = await preflight.main(["--profile", str(tmp_path / "absent.json")])
    assert code == preflight.EXIT_BLOCKED


def test_cursor_local_is_refused_on_a_windows_worker_and_admitted_on_linux() -> None:
    selected = profile(
        lanes=[
            {"lane_profile": "deep_agents", "auth_profile_id": "deep-agents-openai-api"},
            {"lane_profile": "cursor_local", "auth_profile_id": "cursor-local-key"},
            {"lane_profile": "codex", "auth_profile_id": "codex-owner-chatgpt"},
        ]
    )
    windows = check_lane_hosts(selected, "Windows")
    assert [(issue.code, issue.pointer) for issue in windows] == [
        ("LANE_UNSUPPORTED_OS", "profile#/lanes/1/lane_profile"),
        ("LANE_UNSUPPORTED_OS", "profile#/lanes/2/lane_profile"),
    ]
    assert windows[0].observed == "Windows" and "WSL" in windows[0].message
    assert check_lane_hosts(selected, "Linux") == []


def test_hosted_outcome_3_lanes_are_refused_on_every_host() -> None:
    selected = profile(lanes=[{"lane_profile": "claude_cloud"}])
    for system in ("Linux", "Windows", "Darwin"):
        (issue,) = check_lane_hosts(selected, system)
        assert issue.code == "LANE_UNQUALIFIED"


def test_auth_routes_are_checked_without_reading_a_credential_value() -> None:
    selected = profile(
        lanes=[
            {"lane_profile": "deep_agents", "auth_profile_id": "deep-agents-openai-api"},
            {"lane_profile": "cursor_local"},
            {"lane_profile": "codex", "auth_profile_id": "deep-agents-openai-api"},
            {"lane_profile": "codex", "auth_profile_id": "codex-owner-chatgpt"},
        ]
    )
    issues = check_auth(selected, AUTH_EXAMPLE, environ={})
    codes = [(issue.code, issue.severity) for issue in issues]
    assert codes == [
        ("AUTH_CREDENTIAL_ABSENT", "blocking"),
        ("AUTH_PROFILE_MISSING", "blocking"),
        ("AUTH_PROFILE_LANE_MISMATCH", "blocking"),
        ("AUTH_STATUS_UNPROBED", "advisory"),
    ]
    assert issues[0].pointer.endswith("#/profiles/0/credential_ref")
    present = check_auth(selected, AUTH_EXAMPLE, environ={"OPENAI_API_KEY": "x"})
    assert "AUTH_CREDENTIAL_ABSENT" not in {issue.code for issue in present}
    assert all(issue.observed != "x" for issue in present)


def test_an_unqualified_auth_route_and_a_missing_registry_are_refused(tmp_path: Path) -> None:
    registry = write(
        tmp_path / "auth.json",
        {
            "profiles": [
                {
                    "profile_id": "cursor-login",
                    "lane_profile": "cursor_local",
                    "route": "owner_cli_login",
                    "billing_mode": "unknown",
                    "allowed_billing_modes": ["unknown"],
                }
            ]
        },
    )
    selected = profile(lanes=[{"lane_profile": "cursor_local", "auth_profile_id": "cursor-login"}])
    codes = [issue.code for issue in check_auth(selected, registry, environ={})]
    assert codes == ["AUTH_ROUTE_UNQUALIFIED", "AUTH_STATUS_UNPROBED"]
    (missing,) = check_auth(selected, tmp_path / "absent.json", environ={})
    assert missing.code == "AUTH_PROFILES_MISSING"


# --- Pins against an explicit workspace root -----------------------------------------------


def skill_pins(root: Path) -> dict[str, Any]:
    skill = root / ".agents" / "skills" / "demo"
    skill.mkdir(parents=True)
    (skill / "SKILL.md").write_text("---\nname: demo\ndescription: d\n---\nbody\n")
    tool = root / ".tools" / "demo" / "bin.js"
    tool.parent.mkdir(parents=True)
    tool.write_bytes(b"console.log('demo')\n")
    bundle = read_skill_bundle(skill)
    ref = {"kind": "skill", "logical_id": "skill.demo", "revision": 1, "digest": UNSET_DIGEST}
    return {
        "skills": [
            {
                "ref": ref,
                "skill_name": "demo",
                "source_locator": "workspace://.agents/skills/demo",
                "bundle_digest": bundle.bundle_digest,
                "skill_md_digest": sha256_digest((skill / "SKILL.md").read_bytes().decode()),
                "mount_root": "/skills/demo",
            }
        ],
        "tools": [
            {
                "ref": {**ref, "kind": "tool", "logical_id": "tool.demo"},
                "tool_name": "demo",
                "kind": "agent_browser_page",
                "schema_digest": UNSET_DIGEST,
                "entrypoint_locator": "workspace://.tools/demo/bin.js",
                "entrypoint_digest": "sha256:" + sha256(tool.read_bytes()).hexdigest(),
                "package_version": "1.0.0",
            }
        ],
    }


def test_pins_resolve_against_the_explicit_workspace_root(tmp_path: Path) -> None:
    pins = CapabilityPins.model_validate(skill_pins(tmp_path))
    assert check_pins(pins, tmp_path, "pins.json") == []


def test_pin_drift_names_the_pin_locator_and_both_digests(tmp_path: Path) -> None:
    pins = CapabilityPins.model_validate(skill_pins(tmp_path))
    nested = tmp_path / ".agents" / "skills" / "demo" / "demo" / "SKILL.md"
    nested.parent.mkdir()
    nested.write_text("---\nname: demo\ndescription: a nested re-install\n---\n")
    (tmp_path / ".tools" / "demo" / "bin.js").unlink()
    issues = check_pins(pins, tmp_path, "pins.json")
    by_code = {issue.code: issue for issue in issues}
    drift = by_code["PIN_DRIFT"]
    assert drift.pointer == "pins.json#/skills/0/bundle_digest"
    assert "workspace://.agents/skills/demo" in drift.message
    assert drift.expected == pins.skills[0].bundle_digest
    assert drift.observed == read_skill_bundle(tmp_path / ".agents/skills/demo").bundle_digest
    assert by_code["PIN_UNAVAILABLE"].pointer == "pins.json#/tools/0/entrypoint_digest"
    # The top-level SKILL.md is unchanged, so only the bundle digest drifted.
    assert [issue.pointer for issue in issues if issue.code == "PIN_DRIFT"] == [drift.pointer]


def test_a_workspace_root_from_a_scratch_worktree_reports_the_pins_unavailable(
    tmp_path: Path,
) -> None:
    pins = CapabilityPins.load(PINS)
    issues = check_pins(pins, tmp_path, "pins.json")
    assert {issue.code for issue in issues} == {"PIN_UNAVAILABLE"}
    assert len(issues) == len(pins.mcp_servers) + len(pins.tools) + len(pins.skills)


# --- DB release ----------------------------------------------------------------------------


def release_tree(tmp_path: Path, *, lock_digest: str | None = None) -> Path:
    manifest = {
        "component_version": "1.1.0",
        "schema_fingerprint": sha256_digest("schema"),
        "schema_fingerprint_algorithm": "mc-pg-catalog-v2",
    }
    content = json.dumps(manifest).encode()
    component = tmp_path / "component"
    component.mkdir()
    (component / "manifest.json").write_bytes(content)
    return write(
        tmp_path / "deployments" / "biotech" / "release.lock.json",
        {
            "release_root": "../../component",
            "manifest_path": "manifest.json",
            "files": {"manifest.json": lock_digest or sha256(content).hexdigest()},
        },
    )


def test_the_release_lock_pins_the_expected_fingerprint(tmp_path: Path) -> None:
    expected, issues = expected_release(release_tree(tmp_path))
    assert issues == [] and expected is not None
    assert expected.schema_fingerprint == sha256_digest("schema")


def test_a_stale_release_lock_is_refused(tmp_path: Path) -> None:
    _expected, issues = expected_release(release_tree(tmp_path, lock_digest="0" * 64))
    (issue,) = issues
    assert issue.code == "RELEASE_LOCK_DRIFT" and issue.expected == "0" * 64


def test_a_wrong_or_unattested_db_release_is_refused() -> None:
    expected = ExpectedRelease("1.1.0", sha256_digest("schema"), "mc-pg-catalog-v2", Path("x"))
    same = ObservedRelease("1.1.0", sha256_digest("schema"), "mc-pg-catalog-v2")
    assert compare_release(expected, same, pointer="db") == []
    fixture = ObservedRelease("1.1.0", sha256_digest("schema"), "fixture-unqualified")
    other = ObservedRelease("1.1.0", sha256_digest("other"), "mc-pg-catalog-v2")
    for observed in (fixture, other):
        (issue,) = compare_release(expected, observed, pointer="db")
        assert issue.code == "DB_RELEASE_MISMATCH"
        assert issue.observed == f"{observed.fingerprint_algorithm}:{observed.schema_fingerprint}"
    (unattested,) = compare_release(expected, None, pointer="db")
    assert unattested.code == "DB_RELEASE_UNATTESTED"


@pytest.mark.asyncio
async def test_readiness_gates_the_whole_profile(tmp_path: Path) -> None:
    lock = release_tree(tmp_path)
    root = lock.parents[2]
    bindings = write(
        root / "bindings.json",
        compose_launch_bindings(
            json.loads(BINDINGS_EXAMPLE.read_text(encoding="utf-8")), owner_selections()
        ).model_dump(mode="json"),
    )
    auth = write(root / "auth.json", json.loads(AUTH_EXAMPLE.read_text(encoding="utf-8")))
    document = json.loads(PROFILE_EXAMPLE.read_text(encoding="utf-8"))
    document.update(
        lanes=[{"lane_profile": "deep_agents", "auth_profile_id": "deep-agents-openai-api"}],
        manifest_launch_bindings=bindings.name,
        auth_profiles=auth.name,
        temporal_clusters=document["temporal_clusters"][:1],
    )
    profile_path = write(root / "profile.json", document)
    pins = write(root / "pins.json", skill_pins(root))
    real_pins = json.loads(PINS.read_text(encoding="utf-8"))
    pinned = json.loads(pins.read_text(encoding="utf-8"))
    for key in ("models", "sandboxes", "checkpointers", "stores"):
        pinned[key] = real_pins[key]
    write(pins, pinned)
    inputs = ReadinessInputs(
        profile_path=profile_path,
        root=root,
        workspace_root=root,
        system="Linux",
        environ={"OPENAI_API_KEY": "present"},
    )
    report = await readiness(inputs, settings_with(pins))
    assert report.ready, report.as_json()
    assert report.checks == {
        "profile": "passed",
        "capability_pins": "passed",
        "lane_hosts": "passed",
        "launch_bindings": "passed",
        "auth_routes": "passed",
        "db_release": "passed",
        "temporal": "passed",
    }
    blocked = await readiness(
        ReadinessInputs(
            profile_path=profile_path, root=root, workspace_root=root, system="Linux", environ={}
        ),
        settings_with(pins),
    )
    assert not blocked.ready
    assert blocked.as_json()["unresolved_pointers"] == ["auth.json#/profiles/0/credential_ref"]


# --- Cross-cluster launch guard (in-memory probe) ------------------------------------------

CLOUD = ClusterBinding(
    cluster_id="cloud", target="cloud", address="cloud:7233", namespace="ns", task_queue="q"
)
LOCAL = ClusterBinding(
    cluster_id="local", target="local", address="localhost:7233", namespace="ns", task_queue="q"
)


def fixed_probe(states: Mapping[str, Presence]) -> preflight.PresenceProbe:
    async def probe(cluster: ClusterBinding, _workflow_id: str) -> Presence:
        return states[cluster.cluster_id]

    return probe


async def guard(
    ledger: ClusterBindingLedger,
    target: ClusterBinding,
    states: Mapping[str, Presence],
    *,
    run_id: str = "run-1",
    phase: str = "pending",
) -> preflight.ClusterLaunchDecision:
    return await guard_cluster_launch(
        run_id,
        target,
        clusters=(CLOUD, LOCAL),
        ledger=ledger,
        run_phase=phase,
        probe=fixed_probe(states),
        now=lambda: datetime(2026, 10, 8, tzinfo=UTC),
    )


@pytest.mark.asyncio
async def test_a_run_bound_to_one_cluster_is_never_launched_in_another(tmp_path: Path) -> None:
    ledger = ClusterBindingLedger(tmp_path)
    first = await guard(ledger, CLOUD, {"local": "absent"})
    assert first.admitted and first.code == "BOUND"
    for states in ({"cloud": "unknown"}, {"cloud": "absent"}, {"cloud": "present"}):
        refused = await guard(ledger, LOCAL, states)
        assert not refused.admitted
        assert refused.code == "RUN_BOUND_TO_OTHER_CLUSTER"
        assert refused.bound_cluster_id == "cloud"
    again = await guard(ledger, CLOUD, {"local": "absent"})
    assert again.admitted and again.code == "BOUND_HERE"


@pytest.mark.asyncio
async def test_an_unbound_run_found_elsewhere_or_already_started_is_refused(
    tmp_path: Path,
) -> None:
    ledger = ClusterBindingLedger(tmp_path)
    found = await guard(ledger, LOCAL, {"cloud": "present"})
    assert (found.admitted, found.code) == (False, "ACTIVE_IN_OTHER_CLUSTER")
    started = await guard(ledger, LOCAL, {"cloud": "absent"}, phase="active")
    assert (started.admitted, started.code) == (False, "RUN_NOT_PENDING")
    assert ledger.get("run-1") is None


@pytest.mark.asyncio
async def test_a_new_run_binds_locally_during_an_outage_and_reports_the_unverified_cluster(
    tmp_path: Path,
) -> None:
    ledger = ClusterBindingLedger(tmp_path)
    decision = await guard(ledger, LOCAL, {"cloud": "unknown"}, run_id="run-new")
    assert decision.admitted and decision.unverified_clusters == ("cloud",)
    record = ledger.get("run-new")
    assert record is not None and record.cluster == LOCAL


@pytest.mark.asyncio
async def test_concurrent_launchers_bind_a_run_once(tmp_path: Path) -> None:
    ledger = ClusterBindingLedger(tmp_path)
    decisions = await asyncio.gather(
        guard(ledger, CLOUD, {"local": "absent"}), guard(ledger, LOCAL, {"cloud": "absent"})
    )
    assert sorted(item.admitted for item in decisions) == [False, True]
    (refused,) = [item for item in decisions if not item.admitted]
    assert refused.code == "RUN_BOUND_TO_OTHER_CLUSTER"
