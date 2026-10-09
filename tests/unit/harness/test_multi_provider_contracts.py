"""MP-01 shared contracts: requirement admission (VALIDATION V01), `mc.execution_binding.v2`,
`mc.environment_binding.v1`, `mc.workspace_snapshot.v1`, `mc.approval_binding.v1`,
`mc.stream_subscription.v1`, and the one lane-profile vocabulary."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from pydantic import ValidationError

from mission_control.application.execution.harness.describe import (
    CLAUDE_AGENT_SDK_DESCRIBE,
    CODEX_CLOUD_DESCRIBE,
    CURSOR_LOCAL_DESCRIBE,
    DECLARED_LANE_MATRICES,
    DEEP_AGENTS_DESCRIBE,
)
from mission_control.application.execution.harness.registry import (
    LaneRegistry,
    describe_only_registry,
    lane_profile_for,
)
from mission_control.domain.authoring.manifest import Lane
from mission_control.domain.capabilities.hooks import (
    CLAUDE_SDK_CALLBACK_EVENTS,
    HOOK_EVENT_MAPPING,
    HookEvent,
    native_hook,
    unsupported_events,
)
from mission_control.domain.capabilities.host_support import LaneProfile
from mission_control.domain.execution.approvals import (
    ApprovalBinding,
    ApprovalResolutionIntent,
    approval_contract_schemas,
)
from mission_control.domain.execution.bindings import (
    EnvironmentBinding,
    ProviderExecutionBinding,
    WorkspaceSnapshot,
    binding_contract_schemas,
)
from mission_control.domain.execution.lane_requirements import (
    REQUIREMENT_UNKNOWN,
    REQUIREMENT_UNQUALIFIED,
    REQUIREMENT_UNSUPPORTED,
    RequirementSet,
    admit_requirements,
    admits,
    unsupported_optional,
)
from mission_control.domain.execution.lanes import (
    HOSTED_PROFILES,
    LANE_OF_PROFILE,
    LANE_PROFILES,
    PLACEMENT_OF_PROFILE,
    RUNTIME_OF_LANE,
    FeatureEvidence,
)
from mission_control.domain.subscriptions.streams import (
    StreamEnvelope,
    StreamSubscription,
    stream_contract_schemas,
)

DIGEST = "sha256:" + "b" * 64
NOW = datetime(2026, 10, 8, 12, 0, tzinfo=UTC)


# --- one vocabulary -------------------------------------------------------------------------


def test_the_lane_profile_vocabulary_is_shared_by_every_layer() -> None:
    values = {profile.value for profile in LaneProfile}
    assert values == set(LANE_PROFILES) == set(DECLARED_LANE_MATRICES)
    assert set(HOOK_EVENT_MAPPING) == set(LaneProfile)
    assert Lane is LaneProfile
    assert {LANE_OF_PROFILE[p] for p in LANE_PROFILES} == set(RUNTIME_OF_LANE)
    assert {p for p in LANE_PROFILES if PLACEMENT_OF_PROFILE[p] == "cloud"} == HOSTED_PROFILES
    # Cloud means provider-hosted products only; our own remote worker is `worker_hosted`.
    assert {"cursor_cloud", "claude_cloud", "codex_cloud"} == HOSTED_PROFILES


def test_hook_mappings_are_evidence_backed() -> None:
    # The Python Claude Agent SDK callback union is a strict subset of the settings-file hooks.
    assert set(HOOK_EVENT_MAPPING[LaneProfile.CLAUDE_AGENT_SDK]) > CLAUDE_SDK_CALLBACK_EVENTS
    for event in (HookEvent.SESSION_START, HookEvent.SESSION_END, HookEvent.AFTER_COMPACTION):
        assert event not in CLAUDE_SDK_CALLBACK_EVENTS
    # Codex has no PostToolUseFailure; file edits are apply_patch; shell is "Bash".
    assert native_hook("codex", "after_tool_failure") is None
    codex_edit = native_hook("codex", "after_file_edit")
    assert codex_edit is not None and codex_edit.matcher == "apply_patch"
    codex_shell = native_hook("codex", "before_shell")
    assert codex_shell is not None and codex_shell.matcher == "Bash"
    # Hosted Claude runs the repository settings-file hooks; Codex Cloud runs none.
    assert (
        HOOK_EVENT_MAPPING[LaneProfile.CLAUDE_CLOUD]
        == (HOOK_EVENT_MAPPING[LaneProfile.CLAUDE_AGENT_SDK])
    )
    assert HOOK_EVENT_MAPPING[LaneProfile.CODEX_CLOUD] == {}
    assert unsupported_events("codex_cloud", [HookEvent.BEFORE_TOOL, HookEvent.STOP]) == (
        HookEvent.BEFORE_TOOL,
        HookEvent.STOP,
    )
    # Every event a stub describe lists is actually mapped on that profile.
    for profile, describe in DECLARED_LANE_MATRICES.items():
        for event in describe.hooks.events_supported:
            assert native_hook(profile, event) is not None, (profile, event)


# --- registry and dispatch ----------------------------------------------------------------------


def test_registry_publishes_bound_lanes_as_unqualified_stubs() -> None:
    registry = describe_only_registry(
        cursor_bound=True, claude_bound=True, codex_bound=False, allow_unqualified=False
    )
    assert registry.profiles() == (
        "claude_agent_sdk",
        "claude_cloud",
        "cursor_cloud",
        "cursor_local",
        "deep_agents",
    )
    for profile in registry.profiles():
        describe = registry.describe(profile)
        assert describe.qualified is (profile == "deep_agents")
        if profile != "deep_agents":
            assert set(describe.controls.values()) == {"unqualified"}
    with pytest.raises(PermissionError, match="not qualified"):
        registry.admit("claude_agent_sdk")
    assert describe_only_registry(cursor_bound=False, allow_unqualified=True).profiles() == (
        "deep_agents",
    )
    assert LaneRegistry([]).profiles() == ()


class _Request:
    def __init__(self, runtime: str, profile: str | None) -> None:
        self.execution_runtime = runtime
        self.lane_profile = profile


def test_lane_profile_for_pairs_runtime_and_lane() -> None:
    assert lane_profile_for(_Request("claude", "claude_agent_sdk")) == "claude_agent_sdk"  # type: ignore[arg-type]
    assert lane_profile_for(_Request("codex", "codex_cloud")) == "codex_cloud"  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="claude lane profile"):
        lane_profile_for(_Request("claude", "codex"))  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="deep_agents lane profile"):
        lane_profile_for(_Request("native", "claude_agent_sdk"))  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="cursor lane profile"):
        lane_profile_for(_Request("cursor", "claude_cloud"))  # type: ignore[arg-type]


# --- requirement admission (V01) ----------------------------------------------------------------


def test_required_features_a_stub_cannot_provide_are_pointed_errors() -> None:
    requires = RequirementSet(
        controls=("cancel", "pause"),
        approvals=("workflow_gate", "provider_permission"),
        observation=("terminal_result", "subordinate_lifecycle"),
    )
    issues = admit_requirements(CLAUDE_AGENT_SDK_DESCRIBE, requires, pointer="/mission/requires")
    by_pointer = {issue.pointer: issue for issue in issues}
    assert set(by_pointer) == {
        "/mission/requires/controls/0",
        "/mission/requires/controls/1",
        "/mission/requires/approvals/1",
        "/mission/requires/observation/0",
        "/mission/requires/observation/1",
    }
    assert {issue.code for issue in issues} == {REQUIREMENT_UNQUALIFIED}
    assert all(issue.lane_profile == "claude_agent_sdk" for issue in issues)
    assert "workflow_gate" not in {issue.requirement for issue in issues}
    assert not admits(CLAUDE_AGENT_SDK_DESCRIBE, requires)
    assert admits(CLAUDE_AGENT_SDK_DESCRIBE, RequirementSet())


def test_unsupported_and_unknown_requirements_are_distinguished() -> None:
    issues = admit_requirements(
        CODEX_CLOUD_DESCRIBE,
        RequirementSet(
            controls=("queue_instruction", "teleport"),
            approvals=("provider_permission", "telepathy"),
            observation=("subordinate_lifecycle", "compaction", "vibes"),
        ),
    )
    codes = {(issue.group, issue.requirement): issue.code for issue in issues}
    assert codes[("controls", "queue_instruction")] == REQUIREMENT_UNSUPPORTED
    assert codes[("controls", "teleport")] == REQUIREMENT_UNKNOWN
    assert codes[("approvals", "provider_permission")] == REQUIREMENT_UNSUPPORTED
    assert codes[("approvals", "telepathy")] == REQUIREMENT_UNKNOWN
    assert codes[("observation", "subordinate_lifecycle")] == REQUIREMENT_UNSUPPORTED
    assert codes[("observation", "compaction")] == REQUIREMENT_UNSUPPORTED
    assert codes[("observation", "vibes")] == REQUIREMENT_UNKNOWN


def test_v1_describes_admit_what_their_qualified_controls_prove() -> None:
    basic = RequirementSet(
        controls=("cancel", "queue_instruction"),
        approvals=("workflow_gate",),
        observation=("terminal_result", "usage"),
    )
    assert admits(DEEP_AGENTS_DESCRIBE, basic)
    # Unqualified Cursor stubs prove nothing beyond delivery semantics.
    issues = admit_requirements(CURSOR_LOCAL_DESCRIBE, basic)
    assert {issue.requirement for issue in issues} == {"terminal_result", "usage"}
    assert {issue.code for issue in issues} == {REQUIREMENT_UNQUALIFIED}
    # Native provider approvals and subordinate visibility have no v1 evidence cell.
    richer = RequirementSet(approvals=("mcp_elicitation",), observation=("compaction",))
    assert {i.code for i in admit_requirements(DEEP_AGENTS_DESCRIBE, richer)} == {
        REQUIREMENT_UNQUALIFIED
    }
    assert unsupported_optional(DEEP_AGENTS_DESCRIBE, ["compaction", "usage"]) == (
        "compaction",
        "usage",
    )


def test_a_qualified_v2_feature_admits_its_requirement() -> None:
    qualified_cell = FeatureEvidence(
        status="native",
        implemented=True,
        account_enabled="enabled",
        qualified=True,
        evidence_ref="docs/qualification/lanes/claude_agent_sdk/2026-10-08.json",
        transport="python_sdk",
        sdk_language="python",
        sdk_version="0.1.0",
        qualified_at=NOW,
    )
    describe = CLAUDE_AGENT_SDK_DESCRIBE.model_copy(
        update={
            "features": {
                **CLAUDE_AGENT_SDK_DESCRIBE.features,
                "cancel": qualified_cell,
                "observe": qualified_cell,
            }
        }
    )
    requires = RequirementSet(controls=("cancel",), observation=("terminal_result",))
    assert admits(describe, requires)
    assert not admits(describe, RequirementSet(controls=("fork",)))


# --- bindings -----------------------------------------------------------------------------------


def _local_environment(**changes: Any) -> dict[str, Any]:
    return {"kind": "local_workspace", "host_profile": "worker.linux.default", **changes}


def _hosted_environment(**changes: Any) -> dict[str, Any]:
    return {
        "kind": "provider_hosted",
        "provider": "anthropic",
        "environment_ref": "env_research",
        "expected_revision": "rev-7",
        **changes,
    }


def _binding(**changes: Any) -> dict[str, Any]:
    return {
        "lane_profile": "claude_agent_sdk",
        "model": {"profile": "frontier.default", "model_id": "claude-opus-5-5"},
        "auth": {"profile": "anthropic.api", "billing_mode": "api"},
        "environment": _local_environment(),
        "workspace_policy": {
            "mode": "managed_worktree",
            "reuse": "within_run",
            "dirty_input": "reject",
            "cleanup": "retain_until_artifacts_registered",
        },
        "materialization_digest": DIGEST,
        "requirements": {"controls": ["cancel"], "describe_digest": DIGEST},
        "policy_digest": DIGEST,
        "pins": {"sdk.claude_agent_sdk": "0.1.0", "cli.claude_code": "2.1.0"},
        "provider_options": {"provider": "claude_agent_sdk"},
        "budgets": {"max_turns": 8, "max_segments": 16, "wall_clock_s": 3600},
        **changes,
    }


def test_execution_binding_v2_is_sealed_consistent_and_digest_neutral_on_absent_fields() -> None:
    binding = ProviderExecutionBinding.sealed(**_binding())
    assert binding.schema_version == "mc.execution_binding.v2"
    assert binding.lane == "claude" and binding.binding_digest == binding.computed_digest()
    dumped = binding.model_dump(mode="json")
    assert "task_queue" not in dumped
    assert ProviderExecutionBinding.model_validate(dumped) == binding
    with pytest.raises(ValidationError, match="binding_digest"):
        ProviderExecutionBinding.model_validate({**dumped, "policy_digest": "sha256:" + "c" * 64})
    with pytest.raises(ValidationError, match="options"):
        ProviderExecutionBinding.sealed(
            **_binding(
                provider_options={
                    "provider": "codex_app_server",
                    "app_server_schema_version": "v2",
                }
            )
        )
    with pytest.raises(ValidationError, match="does not match"):
        ProviderExecutionBinding.sealed(**_binding(environment=_hosted_environment()))
    with pytest.raises(ValidationError, match="pins at least one"):
        ProviderExecutionBinding.sealed(**_binding(pins={}))
    with pytest.raises(ValidationError, match="provider_workspace needs"):
        ProviderExecutionBinding.sealed(
            **_binding(
                workspace_policy={
                    "mode": "provider_workspace",
                    "reuse": "none",
                    "dirty_input": "reject",
                    "cleanup": "retain",
                }
            )
        )


def test_hosted_binding_pins_repository_and_hosted_environment() -> None:
    hosted = _binding(
        lane_profile="claude_cloud",
        environment=_hosted_environment(),
        workspace_policy={
            "mode": "provider_workspace",
            "reuse": "none",
            "dirty_input": "reject",
            "cleanup": "retain",
        },
        provider_options={"provider": "claude_cloud", "repository_ref": "acme/repo"},
        repo_url="https://github.com/acme/repo",
        repo_commit="0123456789abcdef",
    )
    binding = ProviderExecutionBinding.sealed(**hosted)
    assert binding.lane == "claude" and binding.environment.readiness == "unverified"
    with pytest.raises(ValidationError, match="repo_url and repo_commit"):
        ProviderExecutionBinding.sealed(**{**hosted, "repo_commit": None})
    with pytest.raises(ValidationError, match="managed_worktree"):
        ProviderExecutionBinding.sealed(
            **{**hosted, "workspace_policy": _binding()["workspace_policy"]}
        )


def test_environment_binding_shape_and_readiness() -> None:
    EnvironmentBinding.model_validate(_local_environment())
    with pytest.raises(ValidationError, match="host_profile"):
        EnvironmentBinding.model_validate({"kind": "local_workspace"})
    with pytest.raises(ValidationError, match="does not carry"):
        EnvironmentBinding.model_validate(_local_environment(provider="anthropic"))
    with pytest.raises(ValidationError, match="expected_revision or observed_config_digest"):
        EnvironmentBinding.model_validate(_hosted_environment(expected_revision=None))
    with pytest.raises(ValidationError, match="verified_by/verified_at"):
        EnvironmentBinding.model_validate(
            _hosted_environment(expected_revision=None, observed_config_digest=DIGEST)
        )
    with pytest.raises(ValidationError, match="readiness_evidence_ref"):
        EnvironmentBinding.model_validate(_hosted_environment(readiness="ready"))
    with pytest.raises(ValidationError, match="never secret values"):
        EnvironmentBinding.model_validate(_local_environment(secret_refs=("sk-live-123",)))


def test_workspace_snapshot_names_base_patch_and_untracked_artifacts() -> None:
    document = {
        "lane_profile": "codex",
        "base_commit": "0123456789abcdef",
        "branch": "main",
        "patch_artifact_ref": "artifact://patch",
        "untracked_artifact_ref": "artifact://untracked",
        "tracked_deletions": ("old/file.py",),
        "exclusions": (".venv/", "node_modules/"),
        "manifest_digest": DIGEST,
        "producer_lease_id": "lease-1",
        "producer_generation": 2,
        "captured_at": NOW,
    }
    snapshot = WorkspaceSnapshot.model_validate(document)
    assert snapshot.schema_version == "mc.workspace_snapshot.v1" and snapshot.emulated
    with pytest.raises(ValidationError, match="patch, an untracked artifact or a head commit"):
        WorkspaceSnapshot.model_validate(
            {**document, "patch_artifact_ref": None, "untracked_artifact_ref": None}
        )
    with pytest.raises(ValidationError, match="repository-relative"):
        WorkspaceSnapshot.model_validate({**document, "tracked_deletions": ("../escape",)})
    assert set(binding_contract_schemas()) >= {
        "execution_binding",
        "environment_binding",
        "workspace_snapshot",
    }


# --- approvals ----------------------------------------------------------------------------------


def _approval(**changes: Any) -> dict[str, Any]:
    return {
        "human_task_id": "ht-1",
        "origin": "provider_permission",
        "lane_profile": "claude_agent_sdk",
        "harness_execution_id": "hx-1",
        "generation": 3,
        "native": {"native_request_ref": "req-9", "tool_call_ref": "toolu_1"},
        "tool_name": "Bash",
        "input_digest": DIGEST,
        "policy_digest": DIGEST,
        "opened_at": NOW,
        "deadline": NOW + timedelta(hours=1),
        "replay_strategy": "reissue_native_request",
        **changes,
    }


def test_approval_binding_correlates_a_human_task_with_its_native_request() -> None:
    binding = ApprovalBinding.model_validate(_approval())
    assert binding.schema_version == "mc.approval_binding.v1"
    assert binding.admits("approve_edited") and not binding.admits("request_changes")
    with pytest.raises(ValidationError, match="names the native request"):
        ApprovalBinding.model_validate(_approval(native={}))
    with pytest.raises(ValidationError, match="names the tool"):
        ApprovalBinding.model_validate(_approval(tool_name=None))
    with pytest.raises(ValidationError, match="follows opened_at"):
        ApprovalBinding.model_validate(_approval(deadline=NOW))
    with pytest.raises(ValidationError, match="review packet"):
        ApprovalBinding.model_validate(_approval(origin="workflow_gate", native={}, tool_name=None))
    gate = ApprovalBinding.model_validate(
        _approval(
            origin="workflow_gate",
            native={},
            tool_name=None,
            review_packet_ref="artifact://review",
        )
    )
    assert gate.admits("request_changes") and not gate.admits("approve_edited")


def test_approval_resolution_is_idempotent_versioned_and_bound_to_the_input_digest() -> None:
    binding = ApprovalBinding.model_validate(_approval())
    base = {
        "human_task_id": "ht-1",
        "request_id": "rq-1",
        "expected_task_version": 1,
        "decision": "approve",
        "actor_ref": "user:owner",
        "decided_at": NOW,
    }
    ApprovalResolutionIntent.model_validate(base).validate_against(binding)
    with pytest.raises(ValueError, match="stale resolution"):
        ApprovalResolutionIntent.model_validate(
            {**base, "expected_task_version": 2}
        ).validate_against(binding)
    with pytest.raises(ValueError, match="carries the edited input digest"):
        ApprovalResolutionIntent.model_validate(
            {**base, "decision": "approve_edited"}
        ).validate_against(binding)
    with pytest.raises(ValueError, match="must change the input digest"):
        ApprovalResolutionIntent.model_validate(
            {**base, "decision": "approve_edited", "edited_input_digest": DIGEST}
        ).validate_against(binding)
    with pytest.raises(ValueError, match="not admitted"):
        ApprovalResolutionIntent.model_validate(
            {**base, "decision": "request_changes"}
        ).validate_against(binding)
    assert set(approval_contract_schemas()) == {
        "approval_binding",
        "approval_resolution_intent",
    }


# --- streams ------------------------------------------------------------------------------------


def _subscription(**changes: Any) -> dict[str, Any]:
    return {
        "subscription_id": "sub-1",
        "request_id": "rq-1",
        "scope": {
            "installation_id": "inst",
            "application_id": "biotech",
            "tenant_id": "tenant-1",
        },
        "target": {"kind": "mission", "id": "mission-1"},
        "streams": ["mission_events", "provider_frames"],
        "cursors": [{"stream": "mission_events", "position": "17"}],
        **changes,
    }


def test_stream_subscription_is_scoped_with_per_stream_cursors() -> None:
    subscription = StreamSubscription.model_validate(_subscription())
    assert subscription.schema_version == "mc.stream_subscription.v1"
    assert subscription.filters.exclude_deltas is True
    with pytest.raises(ValidationError, match="not subscribed"):
        StreamSubscription.model_validate(
            _subscription(cursors=[{"stream": "presence", "position": "1"}])
        )
    with pytest.raises(ValidationError, match="per mission, run or execution"):
        StreamSubscription.model_validate(_subscription(target={"kind": "chain", "id": "c"}))
    with pytest.raises(ValidationError, match="unique"):
        StreamSubscription.model_validate(_subscription(streams=["presence", "presence"]))


def test_stream_envelopes_carry_their_own_cursor_domain() -> None:
    scope = _subscription()["scope"]
    envelope = StreamEnvelope.model_validate(
        {
            "stream": "provider_frames",
            "event_id": "ev-1",
            "scope": scope,
            "execution_ref": "hx-1",
            "generation": 2,
            "cursor": {"stream": "provider_frames", "position": "41", "generation": 2},
            "occurred_at": "2026-10-08T12:00:00Z",
            "recorded_at": "2026-10-08T12:00:01Z",
            "kind": "tool.started",
            "payload_ref": "artifact://frame/41",
        }
    )
    assert envelope.schema_version == "mc.stream_envelope.v1"
    with pytest.raises(ValidationError, match="own stream"):
        StreamEnvelope.model_validate(
            {
                **envelope.model_dump(mode="json"),
                "cursor": {"stream": "mission_events", "position": "41"},
            }
        )
    with pytest.raises(ValidationError, match="names execution and generation"):
        StreamEnvelope.model_validate({**envelope.model_dump(mode="json"), "execution_ref": None})
    with pytest.raises(ValidationError, match="payload or payload_ref"):
        StreamEnvelope.model_validate({**envelope.model_dump(mode="json"), "payload": {"a": 1}})
    assert set(stream_contract_schemas()) == {
        "stream_subscription",
        "subscribe_ack",
        "stream_envelope",
        "stream_error",
    }
