"""FT-G1: `mc.lane_describe.v1`, harness contracts and `mc.cursor_binding.v1`; MP-01: the
`mc.lane_describe.v2` stubs of the hosted profiles; MP-07/08/09: the implemented v2 matrices."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import re
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from mission_control.application.execution.harness.describe import (
    CURSOR_CLOUD_DESCRIBE,
    CURSOR_CLOUD_DESCRIBE_V1,
    CURSOR_LOCAL_DESCRIBE,
    CURSOR_LOCAL_DESCRIBE_V1,
    DECLARED_LANE_MATRICES,
    DEEP_AGENTS_DESCRIBE,
    V1_LANE_PROFILES,
    V2_IMPLEMENTED_LANE_PROFILES,
    V2_STUB_LANE_PROFILES,
    declared_matrix,
)
from mission_control.domain.execution.lanes import (
    DELIVERY_COMMANDS,
    LANE_CONTRACTS,
    LANE_CONTROLS,
    LANE_FEATURES,
    LANE_PROFILES,
    CursorExecutionBinding,
    LaneDescribe,
    UsageReport,
    lane_contract_schemas,
)

DIGEST = "sha256:" + "a" * 64

# 00-ARCHITECTURE section 6 (lane matrix), row by row.
ARCHITECTURE_MATRIX: dict[str, dict[str, str]] = {
    "deep_agents": {
        "prepare": "native",
        "start": "native",
        "reattach": "native",
        "send_turn": "native",
        "cancel_turn": "native",
        "observe": "native",
        "fork": "emulated",
        "snapshot": "emulated",
    },
    "cursor_local": {
        "prepare": "native",
        "start": "native",
        "reattach": "emulated",
        "cancel_turn": "native",
        "observe": "native",
        "pause": "unsupported",
        "fork": "emulated",
        "snapshot": "emulated",
    },
    "cursor_cloud": {
        "prepare": "native",
        "start": "native",
        "reattach": "native",
        "cancel_turn": "native",
        "observe": "native",
        "pause": "unsupported",
        "fork": "emulated",
        "snapshot": "emulated",
    },
}
ARCHITECTURE_DELIVERY: dict[str, dict[str, str]] = {
    "deep_agents": {
        "queue_instruction": "turn_boundary_guaranteed",
        "interrupt_and_inject": "cancel_and_replace",
        "pause": "pause_at_tool_gate",
    },
    "cursor_local": {
        "queue_instruction": "wait_then_send",
        "interrupt_and_inject": "cancel_and_replace",
        "pause": "unsupported",
    },
    "cursor_cloud": {
        "queue_instruction": "wait_then_send",
        "interrupt_and_inject": "cancel_and_replace",
        "pause": "unsupported",
    },
}


@pytest.mark.parametrize("profile", sorted(ARCHITECTURE_MATRIX))
def test_declared_matrices_match_the_architecture_lane_matrix(profile: str) -> None:
    describe = declared_matrix(profile)
    assert LaneDescribe.model_validate_json(describe.model_dump_json()) == describe
    if profile in V1_LANE_PROFILES:
        # Round trip through the wire contract: the fixture validates as mc.lane_describe.v1.
        assert describe.schema_version == "mc.lane_describe.v1"
        assert not describe.is_v2 and "features" not in describe.model_dump(mode="json")
    else:
        assert describe.is_v2 and set(describe.features) == set(LANE_FEATURES)
    for control, support in ARCHITECTURE_MATRIX[profile].items():
        assert describe.controls[control] == support, control
    for command, semantics in ARCHITECTURE_DELIVERY[profile].items():
        assert describe.delivery_semantics[command] == semantics, command


def test_the_seven_profiles_are_declared_and_partitioned() -> None:
    assert set(DECLARED_LANE_MATRICES) == set(LANE_PROFILES)
    groups = (V1_LANE_PROFILES, V2_IMPLEMENTED_LANE_PROFILES, V2_STUB_LANE_PROFILES)
    assert set(LANE_PROFILES) == frozenset().union(*groups)
    assert sum(len(group) for group in groups) == len(LANE_PROFILES)


def test_the_cursor_v1_shapes_stay_readable_beside_their_v2_matrices() -> None:
    """MP-09: v2 adds per-feature evidence; controls, delivery and identity are unchanged."""

    for v1, v2 in (
        (CURSOR_LOCAL_DESCRIBE_V1, CURSOR_LOCAL_DESCRIBE),
        (CURSOR_CLOUD_DESCRIBE_V1, CURSOR_CLOUD_DESCRIBE),
    ):
        assert v1.schema_version == "mc.lane_describe.v1" and not v1.is_v2
        assert LaneDescribe.model_validate_json(v1.model_dump_json()) == v1
        assert v2.is_v2 and v2.qualified is False and v1.digest != v2.digest
        assert (v2.controls, v2.delivery_semantics, v2.identity) == (
            v1.controls,
            v1.delivery_semantics,
            v1.identity,
        )
        assert not any(evidence.qualified for evidence in v2.features.values())


@pytest.mark.parametrize("profile", sorted(V2_IMPLEMENTED_LANE_PROFILES))
def test_implemented_v2_matrices_carry_evidence_and_nothing_is_qualified(profile: str) -> None:
    describe = declared_matrix(profile)
    assert LaneDescribe.model_validate_json(describe.model_dump_json()) == describe
    assert describe.is_v2 and describe.qualified is False
    assert set(describe.features) == set(LANE_FEATURES)
    assert not any(evidence.qualified for evidence in describe.features.values())
    assert "workflow_gate" in describe.approval_modes
    assert describe.placement == ("cloud" if profile == "cursor_cloud" else "worker_hosted")


@pytest.mark.parametrize("profile", sorted(V2_STUB_LANE_PROFILES))
def test_mp01_profiles_are_v2_stubs_with_every_cell_unqualified(profile: str) -> None:
    """A stub states design intent, not proof: nothing native/emulated, nothing qualified."""

    describe = declared_matrix(profile)
    assert LaneDescribe.model_validate_json(describe.model_dump_json()) == describe
    assert describe.schema_version == "mc.lane_describe.v2" and describe.is_v2
    assert describe.qualified is False
    assert set(describe.controls.values()) == {"unqualified"}
    assert set(describe.features) == set(LANE_FEATURES)
    for name, evidence in describe.features.items():
        assert evidence.status in {"unqualified", "unsupported"}, name
        assert not evidence.implemented and not evidence.qualified, name
        assert not describe.feature_implemented(name), name
    assert describe.compaction_control in {"unqualified", "unsupported"}
    assert describe.subordinate_visibility in {"unqualified", "unavailable"}
    assert "workflow_gate" in describe.approval_modes
    # `unqualified()` is idempotent on a stub (the registry publishes stubs through it).
    assert describe.unqualified() == describe


def test_hosted_stubs_state_the_feasibility_findings() -> None:
    claude_cloud = declared_matrix("claude_cloud")
    codex_cloud = declared_matrix("codex_cloud")
    assert claude_cloud.placement == codex_cloud.placement == "cloud"
    assert not claude_cloud.hooks.fail_closed and not codex_cloud.hooks.fail_closed
    assert codex_cloud.hooks.mechanism == "none" and codex_cloud.hooks.events_supported == ()
    # Neither hosted product exposes a public cancel/interrupt: the Stop Fence cannot reach
    # them, which is why admission refuses any workflow that requires `cancel` there.
    for describe in (claude_cloud, codex_cloud):
        assert describe.feature("cancel").status == "unsupported"
        assert describe.delivery_semantics["cancel"] == "unsupported"
        assert describe.feature("approval_suspension").status == "unsupported"
    assert codex_cloud.feature("follow_up").status == "unsupported"
    assert codex_cloud.feature("observe").status == "unsupported"
    assert codex_cloud.feature("continuation").status == "unsupported"
    assert claude_cloud.feature("status").status == "unsupported"
    assert claude_cloud.feature("follow_up").status == "unqualified"
    assert claude_cloud.approval_modes == ("workflow_gate",)
    assert codex_cloud.approval_modes == ("workflow_gate",)


def test_v2_describe_rules() -> None:
    stub = declared_matrix("codex").model_dump(mode="json")
    # v1 cannot carry v2 fields.
    with pytest.raises(ValidationError, match="mc.lane_describe.v2"):
        LaneDescribe.model_validate({**stub, "schema_version": "mc.lane_describe.v1"})
    # v2 carries evidence for every feature.
    features = dict(stub["features"])
    features.pop("launch")
    with pytest.raises(ValidationError, match="exactly"):
        LaneDescribe.model_validate({**stub, "features": features})
    # A qualified v2 lane cannot carry unqualified features.
    native = {
        **stub,
        "controls": dict.fromkeys(LANE_CONTROLS, "native"),
        "qualified": True,
    }
    with pytest.raises(ValidationError, match="unqualified features"):
        LaneDescribe.model_validate(native)
    # A qualified feature cites its evidence.
    with pytest.raises(ValidationError, match="evidence_ref"):
        LaneDescribe.model_validate(
            {
                **stub,
                "features": {
                    **stub["features"],
                    "launch": {"status": "native", "implemented": True, "qualified": True},
                },
            }
        )
    with pytest.raises(ValidationError, match="cannot be implemented"):
        LaneDescribe.model_validate(
            {
                **stub,
                "features": {
                    **stub["features"],
                    "launch": {"status": "unsupported", "implemented": True},
                },
            }
        )


def test_cursor_profiles_are_unqualified_and_differ_where_the_spec_says() -> None:
    assert not CURSOR_LOCAL_DESCRIBE.qualified and not CURSOR_CLOUD_DESCRIBE.qualified
    assert DEEP_AGENTS_DESCRIBE.qualified
    assert CURSOR_LOCAL_DESCRIBE.identity.cursor == "bridge_offset"
    assert CURSOR_CLOUD_DESCRIBE.identity.cursor == "sse_event_id"
    assert CURSOR_CLOUD_DESCRIBE.placement == "cloud"
    assert CURSOR_LOCAL_DESCRIBE.placement == "worker_hosted"
    assert "session_start" in CURSOR_LOCAL_DESCRIBE.hooks.events_supported
    assert "session_start" not in CURSOR_CLOUD_DESCRIBE.hooks.events_supported
    assert "session_end" not in CURSOR_CLOUD_DESCRIBE.hooks.events_supported
    assert "prompt_prefix" not in CURSOR_CLOUD_DESCRIBE.instruction_channel
    # Kernel Hooks are fail-closed on the worker; the cloud VM runs catalog command hooks
    # only (it cannot reach the loopback callback), so cloud does not claim fail-closed
    # (FT-G6 describe honesty).
    assert CURSOR_LOCAL_DESCRIBE.hooks.fail_closed
    assert not CURSOR_CLOUD_DESCRIBE.hooks.fail_closed


def _payload(**changes: Any) -> dict[str, Any]:
    return {**CURSOR_LOCAL_DESCRIBE.model_dump(mode="json"), **changes}


def test_describe_rejects_incomplete_or_dishonest_matrices() -> None:
    controls = dict(CURSOR_LOCAL_DESCRIBE.controls)
    controls.pop("fork")
    with pytest.raises(ValidationError, match="controls"):
        LaneDescribe.model_validate(_payload(controls=controls))
    with pytest.raises(ValidationError, match="delivery_semantics"):
        LaneDescribe.model_validate(_payload(delivery_semantics={"cancel": "wait_then_send"}))
    with pytest.raises(ValidationError):
        LaneDescribe.model_validate(_payload(controls={**controls, "fork": "magic"}))
    with pytest.raises(ValidationError, match="belong"):
        LaneDescribe.model_validate(_payload(lane="deep_agents"))
    hooks = {**CURSOR_LOCAL_DESCRIBE.hooks.model_dump(), "events_supported": ["on_anything"]}
    with pytest.raises(ValidationError, match="hook events"):
        LaneDescribe.model_validate(_payload(hooks=hooks))
    stub = CURSOR_LOCAL_DESCRIBE.unqualified()
    assert set(stub.controls.values()) == {"unqualified"}
    with pytest.raises(ValidationError, match="qualified"):
        LaneDescribe.model_validate({**stub.model_dump(mode="json"), "qualified": True})
    assert set(LANE_CONTROLS) == set(stub.controls)
    assert set(DELIVERY_COMMANDS) == set(stub.delivery_semantics)


def test_describe_digest_is_stable_and_content_bound() -> None:
    assert DEEP_AGENTS_DESCRIBE.digest == declared_matrix("deep_agents").digest
    assert CURSOR_LOCAL_DESCRIBE.digest != CURSOR_LOCAL_DESCRIBE.unqualified().digest
    with pytest.raises(ValueError, match="undeclared"):
        declared_matrix("gemini_cli")


def test_every_lane_contract_exports_a_json_schema() -> None:
    schemas = lane_contract_schemas()
    assert set(schemas) == set(LANE_CONTRACTS)
    for name, schema in schemas.items():
        assert schema["type"] == "object", name
        assert schema.get("additionalProperties") is False, name
    describe = schemas["lane_describe"]
    assert describe["properties"]["schema_version"]["enum"] == [
        "mc.lane_describe.v1",
        "mc.lane_describe.v2",
    ]
    assert describe["properties"]["schema_version"]["default"] == "mc.lane_describe.v1"


def _local_binding(**changes: Any) -> dict[str, Any]:
    return {
        "lane_profile": "cursor_local",
        "pins": {
            "cursor_sdk": "1.0.37",
            "bridge": "1.0.37",
            "protocol": "sdk.v1",
            "cloud_api": "v1",
        },
        "model_id": "composer-2",
        "workspace": {"base_ref": "main"},
        "projections": {"rules_digest": DIGEST, "agents_digest": DIGEST, "hooks_digest": DIGEST},
        "hook_callback": {"listen": "127.0.0.1:47555", "token_ttl_s": 3600},
        "budgets": {"max_turns": 4, "max_segments": 8, "wall_clock_s": 3600},
        **changes,
    }


def test_cursor_binding_is_sealed_by_its_digest_and_shaped_by_profile() -> None:
    binding = CursorExecutionBinding.sealed(**_local_binding())
    assert binding.schema_version == "mc.cursor_binding.v1"
    assert binding.binding_digest == binding.computed_digest()
    tampered = {**binding.model_dump(mode="json"), "model_id": "other-model"}
    with pytest.raises(ValidationError, match="binding_digest"):
        CursorExecutionBinding.model_validate(tampered)
    with pytest.raises(ValidationError, match="hook callback"):
        CursorExecutionBinding.sealed(**_local_binding(hook_callback=None))
    with pytest.raises(ValidationError, match="listen"):
        CursorExecutionBinding.sealed(
            **_local_binding(hook_callback={"listen": "0.0.0.0:1", "token_ttl_s": 60})
        )
    with pytest.raises(ValidationError, match="repository"):
        CursorExecutionBinding.sealed(**_local_binding(lane_profile="cursor_cloud"))
    cloud = CursorExecutionBinding.sealed(
        **_local_binding(
            lane_profile="cursor_cloud",
            workspace={"repo_url": "https://github.com/acme/repo", "base_ref": "main"},
            cloud={"auto_create_pr": False},
        )
    )
    assert cloud.cloud is not None and cloud.cloud.environment == "cloud"


def test_usage_report_never_counts_unknown_as_tokens() -> None:
    with pytest.raises(ValidationError, match="unknown"):
        UsageReport(disposition="unknown", total_tokens=3)
    assert UsageReport(disposition="estimated", total_tokens=3).total_tokens == 3


_DB_CONTRACT = Path(__file__).resolve().parents[3] / "packages/mission-control-db-contract"
_MIGRATIONS = _DB_CONTRACT / "component/migrations"
_LANE_DESCRIBE_MIGRATION = "0033_approvals_coordinator_inbox_lane_describes.sql"
# Released (locked 1.1.0, 0030) and unreleased-but-built (0031) bytes: never edited.
_IMMUTABLE_LANE_MIGRATIONS = {
    "0030_lane_bindings.sql": "c057bc275ab7162f1109646323a0aba82fd8c8b89bc9240df48ebc44501027cf",
    "0031_multi_provider_lanes.sql": (
        "21ac0c9ae225d322f6fa44a9f49187d3f5d9018f663558a999ff42f5b80d2df1"
    ),
}


def _seeded_describes(migration: str) -> dict[str, Any]:
    """The describe documents a migration INSERTs into `lane_profile`, by profile."""

    sql = (_MIGRATIONS / migration).read_text("utf-8")
    return {
        match.group(1): json.loads(match.group(2))
        for match in re.finditer(r"\('([a-z_]+)', '[a-z_]+', '[a-z_]+',\s*'(\{.*?\})'::jsonb", sql)
    }


def _revised_describes(migration: str) -> dict[str, Any]:
    """The describe documents a migration UPDATEs on `lane_profile`, by profile."""

    sql = (_MIGRATIONS / migration).read_text("utf-8")
    pattern = (
        r"UPDATE mission_control\.lane_profile\s+SET describe = '((?:[^']|'')*)'::jsonb\s+"
        r"WHERE lane_profile = '([a-z_]+)' AND NOT qualified;"
    )
    revised: dict[str, Any] = {}
    for match in re.finditer(pattern, sql):
        assert match.group(2) not in revised, f"{match.group(2)} revised twice in {migration}"
        revised[match.group(2)] = json.loads(match.group(1).replace("''", "'"))
    return revised


def test_lane_profile_migrations_end_at_exactly_the_declared_describes() -> None:
    """0030 seeded the FT-G1 v1 matrices and 0031 the MP-01 v2 stubs (their bytes never change);
    0033 revises Cursor, Claude Agent SDK and Codex to their implemented v2 matrices. The rows
    an installation ends with are exactly the declared registry. A describe change in code
    without a regenerated (still unreleased) 0033 section fails here: run
    `packages/mission-control-db-contract/scripts/lane_describe_refresh.py --write`."""

    for name, digest in _IMMUTABLE_LANE_MIGRATIONS.items():
        assert hashlib.sha256((_MIGRATIONS / name).read_bytes()).hexdigest() == digest, name
    seeded_0030 = _seeded_describes("0030_lane_bindings.sql")
    seeded_0031 = _seeded_describes("0031_multi_provider_lanes.sql")
    revised_0033 = _revised_describes(_LANE_DESCRIBE_MIGRATION)
    assert set(seeded_0030) == {"deep_agents", "cursor_local", "cursor_cloud"}
    assert set(seeded_0031) == {"claude_agent_sdk", "codex", "claude_cloud", "codex_cloud"}
    assert set(revised_0033) == V2_IMPLEMENTED_LANE_PROFILES
    assert {**seeded_0030, **seeded_0031, **revised_0033} == {
        profile: describe.model_dump(mode="json")
        for profile, describe in DECLARED_LANE_MATRICES.items()
    }
    assert set(seeded_0030) - set(revised_0033) == V1_LANE_PROFILES
    assert set(seeded_0031) - set(revised_0033) == V2_STUB_LANE_PROFILES
    for document in (*seeded_0031.values(), *revised_0033.values()):
        assert document["qualified"] is False
        assert document["schema_version"] == "mc.lane_describe.v2"


def test_migration_0033_lane_describe_section_is_the_generated_text() -> None:
    """The 0033 section is byte-for-byte what the generator renders from the code (same
    serialization as 0030/0031: sorted keys, compact separators)."""

    spec = importlib.util.spec_from_file_location(
        "lane_describe_refresh", _DB_CONTRACT / "scripts/lane_describe_refresh.py"
    )
    assert spec is not None and spec.loader is not None
    generator = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(generator)
    sql = (_MIGRATIONS / _LANE_DESCRIBE_MIGRATION).read_bytes().decode("utf-8")
    assert generator.current_section(sql) == generator.render_section(
        generator.declared_documents()
    ), "0033 lane describe section drifted from DECLARED_LANE_MATRICES; regenerate it"
    # The same serialization reproduces an applied 0031 seed literal exactly.
    claude_cloud = generator.describe_literal(
        DECLARED_LANE_MATRICES["claude_cloud"].model_dump(mode="json")
    )
    assert f"'{claude_cloud}'::jsonb" in (_MIGRATIONS / "0031_multi_provider_lanes.sql").read_text(
        "utf-8"
    )
