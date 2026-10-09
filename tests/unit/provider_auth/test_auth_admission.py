"""MP-05 auth route admission (VALIDATION V24): pointed rejection before launch, no
subscription-to-API fallback, secret-free route records.

Every observation here comes from a SYNTHETIC FIXTURE
(`tests/fixtures/provider_auth/route_preflight.fixtures.json`) or is built in the test. No
status command, login or provider call runs; nothing here qualifies a route or a lane.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path, PurePosixPath
from typing import Any

import pytest
from pydantic import ValidationError

from mission_control.adapters.provider_auth.detection import WorkerAuthContext, probe
from mission_control.adapters.provider_auth.preflight import LocalAuthPreflight
from mission_control.adapters.provider_auth.status import StatusOutput
from mission_control.application.execution.auth_admission import (
    AUTH_OBSERVATION_NOT_REDACTED,
    AUTH_OBSERVATION_STALE,
    AUTH_PREFLIGHT_MISSING,
    AUTH_PROFILE_LANE_MISMATCH,
    AUTH_PROFILE_MISSING,
    AUTH_PROFILE_UNKNOWN,
    AUTH_ROUTE_SHADOWED,
    ROUTE_TABLE,
    AuthAdmission,
    AuthAdmissionRejected,
    AuthAdmissionService,
    AuthObservation,
    AuthProfile,
    InMemoryAuthProfileRegistry,
    admit_auth,
    route_spec,
    secret_kinds,
)
from mission_control.domain.execution.lanes import LANE_PROFILES

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "provider_auth"
RECORDS = json.loads((FIXTURES / "route_preflight.fixtures.json").read_text(encoding="utf-8"))
NOW = datetime(2026, 10, 8, 21, 0, tzinfo=UTC)
HOME = PurePosixPath("/home/owner")


def _context(spec: dict[str, Any]) -> WorkerAuthContext:
    present = {
        str(PurePosixPath(item) if item.startswith("/") else HOME / item)
        for item in spec.get("files_present", [])
    }
    settings = {
        str(PurePosixPath("/work") / path): (frozenset(v["keys"]), frozenset(v["env_keys"]))
        for path, v in spec.get("settings", {}).items()
    }
    return WorkerAuthContext(
        env_names=frozenset(spec["env_names"]),
        home=HOME,
        platform=spec["platform"],
        path_env=spec.get("path_env", {}),
        bound_settings=frozenset(spec.get("bound_settings", [])),
        settings_files=tuple(PurePosixPath(path) for path in settings),
        exists=lambda path: str(path) in present,
        settings_keys=lambda path: settings.get(str(path)),
    )


def _status(spec: dict[str, Any] | None) -> StatusOutput | None:
    return None if spec is None else StatusOutput(**spec)


def test_fixture_file_is_labelled_as_a_fixture() -> None:
    assert RECORDS["fixture"] is True
    assert "SYNTHETIC FIXTURE" in RECORDS["label"]
    assert "not a live provider observation" in RECORDS["label"]


@pytest.mark.parametrize("scenario", RECORDS["scenarios"], ids=lambda s: s["id"])
def test_fixture_routes_are_identified_and_redacted(scenario: dict[str, Any]) -> None:
    profile = AuthProfile.model_validate(scenario["profile"])
    observation = probe(
        profile,
        _context(scenario["context"]),
        status=_status(scenario["status"]),
        fixture=True,
        now=NOW,
    )
    result = admit_auth(
        profile,
        observation,
        lane_profile=profile.lane_profile,
        pointer="/mission/environment",
        now=NOW,
    )
    expect = scenario["expect"]

    # The record is labelled, identifies the route, and carries no credential or planted marker.
    assert observation.fixture is True and observation.source == "fixture"
    dumps = [observation.model_dump_json()]
    if isinstance(result, AuthAdmission):
        dumps.append(result.model_dump_json())
    else:
        dumps.extend(issue.model_dump_json() for issue in result)
    for dump in dumps:
        assert secret_kinds(dump) == ()
        for marker in RECORDS["planted_markers"][:2]:
            assert marker not in dump

    if expect["admitted"]:
        assert isinstance(result, AuthAdmission), result
        assert result.route == expect["route"]
        assert result.billing_mode == expect["billing_mode"]
        assert result.fixture is True
        assert any(note.startswith("fixture:") for note in result.disclosures)
        if "route_support" in expect:
            assert result.route_support == expect["route_support"]
        assert list(result.env_unset) == expect.get("env_unset", [])
        if "disclosure_contains" in expect:
            assert any(expect["disclosure_contains"] in note for note in result.disclosures)
        pin = result.auth_pin()
        assert pin.profile == profile.profile_id and pin.billing_mode == result.billing_mode
    else:
        assert not isinstance(result, AuthAdmission), result
        assert [issue.code for issue in result] == expect["codes"]
        for issue in result:
            assert issue.pointer.startswith("/mission/environment/auth")
        if "message_contains" in expect:
            assert expect["message_contains"] in result[0].message


def test_a_subscription_profile_never_admits_the_api_route_it_would_fall_back_to() -> None:
    """V24: with ANTHROPIC_API_KEY in the child environment the SDK would bill the API
    (documented precedence 3 over /login at 7). Admission refuses with the pointer and the
    variable name; there is no code path that returns an api_key admission instead."""

    profile = AuthProfile(
        profile_id="claude-owner-local",
        lane_profile="claude_agent_sdk",
        route="owner_cli_login",
        billing_mode="subscription",
        owner_attestation_ref="decision:owner-local-personal-use",
    )
    context = WorkerAuthContext(
        env_names=frozenset({"ANTHROPIC_API_KEY"}),
        home=HOME,
        exists=lambda path: True,
    )
    status = StatusOutput(0, '{"loggedIn": true}', "")
    observation = probe(profile, context, status=status, fixture=True, now=NOW)
    assert observation.effective_route == "api_key"
    result = admit_auth(profile, observation, lane_profile="claude_agent_sdk", now=NOW)
    assert not isinstance(result, AuthAdmission)
    (issue,) = result
    assert issue.code == AUTH_ROUTE_SHADOWED
    assert issue.pointer == "/environment/auth"
    assert "ANTHROPIC_API_KEY" in issue.message and "bills api" in issue.message
    assert "never falls back" in issue.message


def test_api_consent_must_be_explicit_even_when_the_api_route_is_requested() -> None:
    with pytest.raises(ValidationError):
        AuthProfile(
            profile_id="p",
            lane_profile="codex",
            route="api_key",
            billing_mode="api",
            allowed_billing_modes=("unknown",),
            credential_ref="env:OPENAI_API_KEY",
        )
    profile = AuthProfile(
        profile_id="p",
        lane_profile="codex",
        route="owner_cli_login",
        billing_mode="subscription",
    )
    assert profile.consented_billing == frozenset({"subscription"})


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("credential_ref", "sk-ant-api03-AAAAAAAAAAAAAAAAAAAA"),
        ("credential_ref", "env:sk-proj-AAAAAAAAAAAA"),
        ("account_ref", "owner@example.invalid"),
        ("owner_attestation_ref", "Bearer abcdefghijklmnop"),
    ],
)
def test_profiles_refuse_credential_values(field: str, value: str) -> None:
    fields: dict[str, Any] = {
        "profile_id": "p",
        "lane_profile": "claude_agent_sdk",
        "route": "api_key",
        "billing_mode": "api",
        "credential_ref": "env:ANTHROPIC_API_KEY",
        field: value,
    }
    with pytest.raises(ValidationError):
        AuthProfile.model_validate(fields)


def test_login_routes_carry_no_credential_ref_and_credential_routes_need_one() -> None:
    with pytest.raises(ValidationError):
        AuthProfile(
            profile_id="p",
            lane_profile="codex",
            route="owner_cli_login",
            billing_mode="subscription",
            credential_ref="env:OPENAI_API_KEY",
        )
    with pytest.raises(ValidationError):
        AuthProfile(profile_id="p", lane_profile="codex", route="api_key", billing_mode="api")


def _observation(**fields: Any) -> AuthObservation:
    base: dict[str, Any] = {
        "lane_profile": "codex",
        "source": "fixture",
        "fixture": True,
        "effective_route": "owner_cli_login",
        "route_without_shadowing": "owner_cli_login",
        "present_routes": ("owner_cli_login",),
        "credential_present": True,
        "signed_in": True,
        "observed_at": NOW,
    }
    return AuthObservation.model_validate({**base, **fields})


_CODEX_LOGIN = AuthProfile(
    profile_id="codex-owner-chatgpt",
    lane_profile="codex",
    route="owner_cli_login",
    billing_mode="subscription",
)


def test_missing_preflight_observation_rejects() -> None:
    result = admit_auth(_CODEX_LOGIN, None, lane_profile="codex", now=NOW)
    assert not isinstance(result, AuthAdmission)
    assert [i.code for i in result] == [AUTH_PREFLIGHT_MISSING]
    not_probed = _observation(source="not_probed")
    result = admit_auth(_CODEX_LOGIN, not_probed, lane_profile="codex", now=NOW)
    assert not isinstance(result, AuthAdmission)
    assert [i.code for i in result] == [AUTH_PREFLIGHT_MISSING]


def test_stale_observation_rejects() -> None:
    stale = _observation(observed_at=NOW - timedelta(seconds=301))
    result = admit_auth(_CODEX_LOGIN, stale, lane_profile="codex", now=NOW)
    assert not isinstance(result, AuthAdmission)
    assert [i.code for i in result] == [AUTH_OBSERVATION_STALE]


def test_an_observation_carrying_a_credential_is_discarded() -> None:
    leaked = _observation(status_summary="Logged in using an API key - sk-proj-ABCDEFGH1234")
    result = admit_auth(_CODEX_LOGIN, leaked, lane_profile="codex", now=NOW)
    assert not isinstance(result, AuthAdmission)
    assert [i.code for i in result] == [AUTH_OBSERVATION_NOT_REDACTED]
    assert "sk-proj" not in result[0].message


def test_observation_lane_must_match() -> None:
    other = _observation(lane_profile="codex_cloud")
    result = admit_auth(_CODEX_LOGIN, other, lane_profile="codex", now=NOW)
    assert not isinstance(result, AuthAdmission)
    assert [i.code for i in result] == [AUTH_PROFILE_LANE_MISMATCH]


def test_account_fingerprint_mismatch_rejects() -> None:
    profile = _CODEX_LOGIN.model_copy(update={"account_fingerprint": "sha256:" + "a" * 64})
    observation = _observation(account_fingerprint="sha256:" + "b" * 64)
    result = admit_auth(profile, observation, lane_profile="codex", now=NOW)
    assert not isinstance(result, AuthAdmission)
    assert [i.code for i in result] == ["AUTH_ACCOUNT_MISMATCH"]


def test_every_lane_profile_has_a_route_table_entry() -> None:
    assert set(ROUTE_TABLE) == set(LANE_PROFILES)
    assert route_spec("codex_cloud", "api_key").support == "unsupported"
    assert route_spec("claude_cloud", "api_key").support == "unsupported"
    # Hosted routes never imply the hosted lane is qualified; only the route is documented.
    assert route_spec("claude_cloud", "hosted_product_signin").support == "documented"
    for routes in ROUTE_TABLE.values():
        for spec in routes.values():
            assert spec.sources, spec


def test_deployment_example_profiles_validate_and_name_supported_routes() -> None:
    example = Path(__file__).resolve().parents[3] / "deployments" / "examples"
    payload = json.loads(
        (example / "provider-auth-profiles.example.json").read_text(encoding="utf-8")
    )
    assert "EXAMPLE ONLY" in payload["$comment"]
    profiles = [AuthProfile.model_validate(item) for item in payload["profiles"]]
    assert {p.lane_profile for p in profiles} >= {"claude_agent_sdk", "codex", "codex_cloud"}
    for profile in profiles:
        spec = route_spec(profile.lane_profile, profile.route)
        assert spec.support in {"documented", "policy_restricted"}, profile.profile_id
        assert spec.billing_mode == profile.billing_mode, profile.profile_id
        assert secret_kinds(profile.model_dump_json()) == ()


class _RecordingPreflight:
    def __init__(self, observation: AuthObservation) -> None:
        self.observation = observation
        self.calls = 0

    async def preflight(self, profile: AuthProfile) -> AuthObservation:
        self.calls += 1
        return self.observation


async def test_service_rejects_missing_and_unknown_profiles_before_any_preflight() -> None:
    preflight = _RecordingPreflight(_observation())
    service = AuthAdmissionService(InMemoryAuthProfileRegistry(), preflight)
    with pytest.raises(AuthAdmissionRejected) as missing:
        await service.admit("codex", None, pointer="/mission/environment", now=NOW)
    assert missing.value.codes == (AUTH_PROFILE_MISSING,)
    assert missing.value.issues[0].pointer == "/mission/environment/auth/profile"
    with pytest.raises(AuthAdmissionRejected) as unknown:
        await service.admit("codex", "nope", now=NOW)
    assert unknown.value.codes == (AUTH_PROFILE_UNKNOWN,)
    assert preflight.calls == 0


async def test_service_admits_through_the_local_preflight_adapter() -> None:
    class FixtureStatus:
        async def run(self, argv: Any) -> StatusOutput:
            assert tuple(argv) == ("codex", "login", "status")
            return StatusOutput(0, "", "Logged in using ChatGPT\n")

    context = WorkerAuthContext(
        env_names=frozenset({"PATH"}),
        home=HOME,
        exists=lambda path: str(path) == str(HOME / ".codex" / "auth.json"),
    )
    preflight = LocalAuthPreflight(lambda profile: context, status_runner=FixtureStatus())
    service = AuthAdmissionService(
        InMemoryAuthProfileRegistry({_CODEX_LOGIN.profile_id: _CODEX_LOGIN}), preflight
    )
    admission = await service.admit("codex", _CODEX_LOGIN.profile_id)
    assert admission.route == "owner_cli_login"
    assert admission.billing_mode == "subscription"
    assert admission.observation_source == "status_command"
    assert admission.fixture is False  # the status output itself is a test double
    assert admission.auth_pin().billing_mode == "subscription"


async def test_service_raises_pointed_error_for_shadowed_route() -> None:
    preflight = _RecordingPreflight(
        _observation(
            effective_route="api_key",
            route_without_shadowing="owner_cli_login",
            shadowing_env=("OPENAI_API_KEY",),
        )
    )
    service = AuthAdmissionService(
        InMemoryAuthProfileRegistry({_CODEX_LOGIN.profile_id: _CODEX_LOGIN}), preflight
    )
    with pytest.raises(AuthAdmissionRejected) as rejected:
        await service.admit("codex", _CODEX_LOGIN.profile_id, now=NOW)
    assert rejected.value.codes == (AUTH_ROUTE_SHADOWED,)
    assert "OPENAI_API_KEY" in str(rejected.value)
