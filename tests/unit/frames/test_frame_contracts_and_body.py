"""mc.provider_frame.v1 contract, closing rule, redaction, digest and excerpt cap (SPEC-03)."""

from __future__ import annotations

import hashlib
import json
from uuid import uuid4

import pytest
from pydantic import ValidationError

from mission_control.domain.frames.body import (
    REDACTED,
    canonical_bytes,
    frame_body,
    redact_text,
)
from mission_control.domain.frames.contracts import (
    CLOSING_KINDS,
    FrameKind,
    FrameScope,
    LaneProfile,
    ProviderFrame,
    frame_id_from_native_event_ref,
    frame_json_schemas,
    is_closing,
    native_event_ref,
)
from tests.fixtures.provider_frames import FRAME_NOW, SCOPE

EXPECTED_CLOSING = {
    "tool_call_completed",
    "tool_call_failed",
    "approval_resolved",
    "hook_result",
    "after_compaction",
    "usage",
    "turn_ended",
    "run_result",
    "session_state",
    "error",
}


def frame(**updates: object) -> ProviderFrame:
    values: dict[str, object] = {
        "frame_id": uuid4(),
        "scope": FrameScope.from_request_scope(SCOPE),
        "run_id": uuid4(),
        "activation_id": uuid4(),
        "attempt_no": 1,
        "harness_execution_id": uuid4(),
        "generation": 1,
        "lane_profile": LaneProfile.DEEP_AGENTS,
        "native_session_ref": "thread-1",
        "provider_key": "thread-1::::call-1:tool_call_completed",
        "arrival_ordinal": 1,
        "observed_at": FRAME_NOW,
        "kind": FrameKind.TOOL_CALL_COMPLETED,
        "closing": True,
        "body_digest": "sha256:" + "a" * 64,
        "body_bytes": 2,
        "body_media_type": "application/json",
        "body_excerpt": "{}",
        "raw_kind": "updates.tool_message",
    }
    values.update(updates)
    return ProviderFrame.model_validate(values)


def test_closing_is_set_exactly_for_the_closing_kinds() -> None:
    assert {kind.value for kind in CLOSING_KINDS} == EXPECTED_CLOSING
    for kind in FrameKind:
        assert is_closing(kind) == (kind.value in EXPECTED_CLOSING)
        built = frame(kind=kind, closing=kind.value in EXPECTED_CLOSING)
        assert built.closing == is_closing(kind)
        with pytest.raises(ValidationError, match="closing must be"):
            frame(kind=kind, closing=kind.value not in EXPECTED_CLOSING)


def test_frame_rejects_unknown_fields_bad_digest_and_naive_time() -> None:
    with pytest.raises(ValidationError):
        frame(unexpected="value")
    with pytest.raises(ValidationError):
        frame(body_digest="md5:abc")
    with pytest.raises(ValidationError):
        frame(observed_at=FRAME_NOW.replace(tzinfo=None))
    with pytest.raises(ValidationError):
        frame(lane_profile="langsmith")
    with pytest.raises(ValidationError):
        frame(arrival_ordinal=0)


def test_native_event_ref_round_trips() -> None:
    built = frame()
    assert built.native_event_ref == native_event_ref(built.frame_id)
    assert frame_id_from_native_event_ref(built.native_event_ref) == built.frame_id
    with pytest.raises(ValueError):
        frame_id_from_native_event_ref("event:123")


def test_json_schema_is_exported_for_the_frame_contracts() -> None:
    schemas = frame_json_schemas()
    provider = schemas["provider_frame"]
    assert provider["additionalProperties"] is False
    assert {"frame_id", "harness_execution_id", "provider_key", "arrival_ordinal", "kind"} <= set(
        provider["properties"]
    )
    assert "frame_observation" in schemas and "append_receipt" in schemas
    json.dumps(schemas)


SECRET_SAMPLES = {
    "bearer_token": "Authorization header was Bearer abcdefghijklmnop.qrs",
    "env_secret": "env CURSOR_API_KEY=crsr_live_123456 and TAVILY_API_KEY: tvly-secretvalue1",
    "url_credential": "GET https://api.example.com/v1?q=1&token=s3cr3t-token&key=abc123",
    "sk_key": "use sk-proj-ABCDEFGHIJKLMNOP for the call",
    "github_token": "ghp_abcdefghijklmnopqrstuvwxyz0123",
    "jwt": (
        "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0."
        "dozjgNryP4J3jVmNHl0w5N_XgL0n3I9PlFUP0THsR8U"
    ),
}


@pytest.mark.parametrize(("pattern", "text"), sorted(SECRET_SAMPLES.items()))
def test_redaction_removes_every_secret_pattern_and_records_paths(pattern: str, text: str) -> None:
    body = frame_body({"outer": {"note": text}})
    stored = body.canonical.decode("utf-8")
    assert REDACTED in stored
    for secret in (
        "abcdefghijklmnop.qrs",
        "crsr_live_123456",
        "tvly-secretvalue1",
        "s3cr3t-token",
        "abc123",
        "sk-proj-ABCDEFGHIJKLMNOP",
        "ghp_abcdefghijklmnopqrstuvwxyz0123",
        "dozjgNryP4J3jVmNHl0w5N_XgL0n3I9PlFUP0THsR8U",
    ):
        assert secret not in stored
    assert {item.pattern for item in body.redactions} >= {pattern}
    assert all(item.path == "$.outer.note" for item in body.redactions)


def test_secret_keys_and_configured_secret_values_are_redacted() -> None:
    body = frame_body(
        {
            "headers": {"Authorization": "Token zzz", "X-Api-Key": "k-123456"},
            "FIRECRAWL_API_KEY": "fc-whatever",
            "stdout": "connected with value hunter2-configured-secret",
            "nested": [{"password": "pw"}],
        },
        secret_values=("hunter2-configured-secret",),
    )
    stored = json.loads(body.canonical)
    assert stored["headers"]["Authorization"] == REDACTED
    assert stored["headers"]["X-Api-Key"] == REDACTED
    assert stored["FIRECRAWL_API_KEY"] == REDACTED
    assert stored["nested"][0]["password"] == REDACTED
    assert "hunter2-configured-secret" not in body.canonical.decode()
    paths = {(item.path, item.pattern) for item in body.redactions}
    assert ("$.stdout", "configured_secret") in paths
    assert ("$.headers.Authorization", "secret_key") in paths


def test_excerpt_cap_is_honoured_and_digest_covers_the_whole_redacted_body() -> None:
    payload = {"text": "é" * 10_000}
    body = frame_body(payload, excerpt_cap_bytes=1_001)
    assert len(body.excerpt.encode("utf-8")) <= 1_001
    assert body.truncated
    assert body.body_bytes == len(canonical_bytes(payload))
    assert body.digest == "sha256:" + hashlib.sha256(body.canonical).hexdigest()
    small = frame_body({"a": 1})
    assert small.excerpt == '{"a":1}' and not small.truncated


def test_render_time_text_redaction() -> None:
    text, found = redact_text("curl -H 'Authorization: Bearer abcdefghijklmnop' https://x?key=zz")
    assert "abcdefghijklmnop" not in text and "key=zz" not in text
    assert {item.pattern for item in found} == {"bearer_token", "url_credential"}
