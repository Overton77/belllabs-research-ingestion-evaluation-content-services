"""MP-05 composition: settings -> auth profiles, preflight context, capacity bounds and the
explicit provider child environment. Everything here is offline; no vendor command runs."""

from __future__ import annotations

import json
from pathlib import Path, PurePosixPath

import pytest

from mission_control.application.execution.auth_admission import (
    AuthAdmission,
    AuthAdmissionRejected,
    AuthAdmissionService,
)
from mission_control.bootstrap.provider_auth import (
    AuthProfilesUnavailable,
    capacity_policy,
    compose_auth_admission,
    load_auth_profiles,
    provider_child_environment,
    status_probe_environment,
    worker_auth_context,
)
from mission_control.bootstrap.settings import Settings

EXAMPLE = Path(__file__).resolve().parents[3] / (
    "deployments/examples/provider-auth-profiles.example.json"
)


def settings(**overrides: object) -> Settings:
    return Settings(_env_file=None, **overrides)  # type: ignore[call-arg]


def test_example_profiles_document_loads_with_unique_ids() -> None:
    profiles = load_auth_profiles(EXAMPLE)
    assert "claude-sdk-owner-subscription" in profiles
    assert len(profiles) == len(json.loads(EXAMPLE.read_text(encoding="utf-8"))["profiles"])


@pytest.mark.parametrize(
    "document",
    [
        "not json",
        json.dumps([]),
        json.dumps({"profiles": {}}),
        json.dumps({"profiles": [{"schema_version": "mc.auth_profile.v1"}]}),
    ],
)
def test_malformed_profile_documents_are_refused(tmp_path: Path, document: str) -> None:
    path = tmp_path / "profiles.json"
    path.write_text(document, encoding="utf-8")
    with pytest.raises(AuthProfilesUnavailable):
        load_auth_profiles(path)


def test_duplicate_profile_ids_are_refused(tmp_path: Path) -> None:
    profile = json.loads(EXAMPLE.read_text(encoding="utf-8"))["profiles"][0]
    path = tmp_path / "profiles.json"
    path.write_text(json.dumps({"profiles": [profile, profile]}), encoding="utf-8")
    with pytest.raises(AuthProfilesUnavailable, match="duplicate"):
        load_auth_profiles(path)


def test_no_profiles_path_composes_nothing() -> None:
    assert compose_auth_admission(settings(), environ={}) is None


def test_missing_profiles_file_fails_closed(tmp_path: Path) -> None:
    with pytest.raises(AuthProfilesUnavailable):
        compose_auth_admission(
            settings(mission_control_auth_profiles_path=tmp_path / "absent.json"), environ={}
        )


def test_worker_context_sees_names_and_locations_never_values() -> None:
    configured = settings(anthropic_api_key="sk-ant-not-a-real-key-000000")
    environ = {"ANTHROPIC_API_KEY": "sk-ant-value", "CLAUDE_CONFIG_DIR": "/cfg", "PATH": "/bin"}
    context = worker_auth_context(
        configured,
        environ,
        home=PurePosixPath("/home/w"),
        platform="linux",
        project_root=PurePosixPath("/repo"),
    )
    assert "ANTHROPIC_API_KEY" in context.env_names
    assert context.bound_settings == frozenset({"anthropic_api_key"})
    assert context.path_env == {"CLAUDE_CONFIG_DIR": "/cfg"}
    assert PurePosixPath("/cfg/settings.json") in context.settings_files
    assert PurePosixPath("/repo/.claude/settings.json") in context.settings_files
    assert "sk-ant-value" not in repr(context)


@pytest.mark.asyncio
async def test_composed_admission_refuses_a_shadowed_subscription_route(tmp_path: Path) -> None:
    """The example's Claude subscription profile rejects shadowing; with ANTHROPIC_API_KEY in
    the worker environment the composed service refuses before any launch (no probe ran)."""

    service = compose_auth_admission(
        settings(mission_control_auth_profiles_path=EXAMPLE),
        environ={"ANTHROPIC_API_KEY": "sk-ant-value", "PATH": "/bin"},
        home=PurePosixPath("/home/w"),
        platform="linux",
        project_root=PurePosixPath("/repo"),
    )
    assert isinstance(service, AuthAdmissionService)
    with pytest.raises(AuthAdmissionRejected) as rejected:
        await service.admit("claude_agent_sdk", "claude-sdk-owner-subscription")
    assert "sk-ant-value" not in str(rejected.value)


def test_status_probe_environment_carries_no_credentials() -> None:
    environ = {
        "PATH": "/bin",
        "HOME": "/home/w",
        "CODEX_HOME": "/codex",
        "ANTHROPIC_API_KEY": "sk-ant-value",
        "OPENAI_API_KEY": "sk-value",
        "CURSOR_API_KEY": "key",
    }
    assert status_probe_environment(environ) == {
        "PATH": "/bin",
        "HOME": "/home/w",
        "CODEX_HOME": "/codex",
    }


def test_capacity_policy_maps_settings_one_to_one() -> None:
    policy = capacity_policy(
        settings(
            mission_control_capacity_max_waits=2,
            mission_control_capacity_max_total_wait_s=600,
            mission_control_capacity_reset_margin_s=9,
            mission_control_capacity_fallback_backoff_s=7,
            mission_control_capacity_max_backoff_s=70,
        )
    )
    assert (
        policy.max_waits,
        policy.max_total_wait_s,
        policy.reset_margin_s,
        policy.fallback_backoff_s,
        policy.max_backoff_s,
    ) == (2, 600, 9, 7, 70)


def test_provider_child_environment_drops_unset_names_and_refuses_reintroduction() -> None:
    admission = AuthAdmission.model_construct(env_unset=("ANTHROPIC_API_KEY",))
    environ = {"PATH": "/bin", "ANTHROPIC_API_KEY": "sk-ant-value", "anthropic_api_key": "x"}
    child = provider_child_environment(environ, admission, extra={"CLAUDE_CONFIG_DIR": "/cfg"})
    assert child == {"PATH": "/bin", "CLAUDE_CONFIG_DIR": "/cfg"}
    with pytest.raises(ValueError, match="re-introduce"):
        provider_child_environment(environ, admission, extra={"Anthropic_Api_Key": "again"})
