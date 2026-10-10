"""MP-08: the committed app-server schema pin is what the typed protocol module speaks.

The committed copy (`tests/fixtures/provider_frames/codex/schema/`, `codex-cli 0.162.0`) is
digest-pinned by `PIN.json`; the method registries in `protocol.py` equal the committed
registry files; every outbound params model, every native approval answer and every event
the FIXTURE app-server emits for the committed scripts validates against the pinned JSON
Schema definitions (jsonschema, Draft 7 as the generator emits).
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import jsonschema
import pytest

from mission_control.adapters.codex import protocol
from mission_control.adapters.codex.approvals import native_response
from mission_control.application.execution.approvals import NativeReply
from mission_control.domain.execution.lanes import LaneSegmentBounds
from tests.fixtures.lane_turns import RecordingSignals
from tests.unit.codex.fixture_app_server import FIXTURE_ROOT, SCRIPTS
from tests.unit.codex.support import codex_stack

SCHEMA_DIR = FIXTURE_ROOT / "schema"
PIN = json.loads((SCHEMA_DIR / "PIN.json").read_text(encoding="utf-8"))
BUNDLE = json.loads(
    (SCHEMA_DIR / "codex_app_server_protocol.v2.schemas.json").read_text(encoding="utf-8")
)
DIGEST_X = "sha256:" + "e" * 64
SERVER_REQUESTS = json.loads((SCHEMA_DIR / "ServerRequest.json").read_text(encoding="utf-8"))


def _methods(name: str) -> frozenset[str]:
    registry = json.loads((SCHEMA_DIR / "methods.json").read_text(encoding="utf-8"))
    return frozenset(item["method"] for item in registry[name])


def test_the_committed_schema_matches_its_pin() -> None:
    assert PIN["codex_cli_version"] == protocol.PINNED_CODEX_CLI_VERSION == "0.162.0"
    assert PIN["app_server_protocol"] == protocol.PINNED_PROTOCOL_VERSION
    for name, entry in PIN["files"].items():
        data = (SCHEMA_DIR / name).read_bytes()
        assert hashlib.sha256(data).hexdigest() == entry["sha256"], name
        assert len(data) == entry["bytes"], name
    assert PIN["files"]["codex_app_server_protocol.v2.schemas.json"]["sha256"] == (
        protocol.PINNED_SCHEMA_SHA256
    )
    assert BUNDLE["title"] == "CodexAppServerProtocolV2"
    assert PIN["generated_file_count"] == len(PIN["generated_files_sha256"]) == 317


def test_method_registries_equal_the_committed_registry_files() -> None:
    assert _methods("client_requests") == protocol.CLIENT_REQUEST_METHODS
    assert _methods("server_requests") == protocol.SERVER_REQUEST_METHODS
    assert _methods("server_notifications") == protocol.SERVER_NOTIFICATION_METHODS
    assert _methods("client_notifications") == protocol.CLIENT_NOTIFICATION_METHODS
    # The bundle's own ClientRequest/ServerNotification unions agree with the registry files.
    bundle_client = {
        v["properties"]["method"]["enum"][0]
        for v in BUNDLE["definitions"]["ClientRequest"]["oneOf"]
    }
    bundle_notifications = {
        v["properties"]["method"]["enum"][0]
        for v in BUNDLE["definitions"]["ServerNotification"]["oneOf"]
    }
    assert bundle_client == protocol.CLIENT_REQUEST_METHODS
    assert bundle_notifications == protocol.SERVER_NOTIFICATION_METHODS
    assert protocol.APPROVAL_REQUEST_METHODS < protocol.SERVER_REQUEST_METHODS


def _validator(definition: str, document: dict[str, Any] = BUNDLE) -> jsonschema.Draft7Validator:
    schema = {"$ref": f"#/definitions/{definition}", "definitions": document["definitions"]}
    return jsonschema.Draft7Validator(schema)


def _server_request_params_validator(method: str) -> jsonschema.Draft7Validator:
    for variant in SERVER_REQUESTS["oneOf"]:
        if variant["properties"]["method"]["enum"][0] == method:
            ref = variant["properties"]["params"]["$ref"].split("/")[-1]
            return _validator(ref, SERVER_REQUESTS)
    raise AssertionError(method)


def _file_validator(name: str) -> jsonschema.Draft7Validator:
    """A standalone generated schema file (`<Name>.json`) as its own root."""

    document = json.loads((SCHEMA_DIR / f"{name}.json").read_text(encoding="utf-8"))
    return jsonschema.Draft7Validator(document)


@pytest.mark.parametrize(
    ("definition", "params"),
    [
        (
            "InitializeParams",
            protocol.InitializeParams(
                client_info=protocol.ClientInfo(name="mission-control", version="0.1.0"),
                capabilities=protocol.InitializeCapabilities(),
            ).params(),
        ),
        (
            "ThreadStartParams",
            protocol.ThreadStartParams(
                cwd="/work",
                model="gpt-5-codex",
                approval_policy="on-request",
                approvals_reviewer="user",
                sandbox="workspace-write",
                ephemeral=False,
            ).params(),
        ),
        (
            "ThreadResumeParams",
            protocol.ThreadResumeParams(
                thread_id="thr", cwd="/work", approval_policy="untrusted", sandbox="read-only"
            ).params(),
        ),
        (
            "ThreadReadParams",
            protocol.ThreadReadParams(thread_id="thr", include_turns=True).params(),
        ),
        ("ThreadCompactStartParams", protocol.ThreadCompactStartParams(thread_id="thr").params()),
        (
            "TurnStartParams",
            protocol.TurnStartParams(
                thread_id="thr",
                input=(protocol.TextUserInput(text="hello"),),
                client_user_message_id="heid:1:turn:1",
                effort="medium",
            ).params(),
        ),
        (
            "TurnSteerParams",
            protocol.TurnSteerParams(
                thread_id="thr",
                expected_turn_id="turn-1",
                input=(protocol.TextUserInput(text="steer"),),
            ).params(),
        ),
        (
            "TurnInterruptParams",
            protocol.TurnInterruptParams(thread_id="thr", turn_id="t").params(),
        ),
    ],
)
def test_outbound_params_validate_against_the_pinned_schema(
    definition: str, params: dict[str, Any]
) -> None:
    _validator(definition).validate(params)


def test_the_approval_policy_contract_equals_the_pinned_enum() -> None:
    """`CodexAppServerOptions.approval_policy` admits exactly the pinned `AskForApproval`
    string variants (`on-failure` was dropped from the contract: the pin lacks it)."""

    from typing import get_args

    from mission_control.domain.execution.bindings import CodexAppServerOptions

    assert "on-failure" not in json.dumps(BUNDLE["definitions"]["AskForApproval"])
    pinned = set(BUNDLE["definitions"]["AskForApproval"]["oneOf"][0]["enum"])
    assert pinned == {"untrusted", "on-request", "never"}
    declared = CodexAppServerOptions.model_fields["approval_policy"].annotation
    assert set(get_args(declared)) == pinned


def _reply(action: str, **changes: Any) -> NativeReply:
    values: dict[str, Any] = {"action": action, "reason": "human_decision", "human_task_id": "t"}
    values.update(changes)
    return NativeReply.model_validate(values)


_QUESTIONS: dict[str, Any] = {
    "questions": [
        {"id": "q1", "header": "Branch", "question": "Which branch?"},
        {"id": "q2", "header": "Tests", "question": "Run tests?", "options": []},
    ]
}
_PERMISSIONS: dict[str, Any] = {
    "permissions": {"network": {"enabled": True}},
    "cwd": "/work",
}


@pytest.mark.parametrize(
    ("method", "params", "replies", "definition", "expected"),
    [
        (
            "item/commandExecution/requestApproval",
            {},
            [_reply("allow")],
            "CommandExecutionRequestApprovalResponse",
            {"decision": "accept"},
        ),
        (
            "item/commandExecution/requestApproval",
            {},
            [_reply("deny")],
            "CommandExecutionRequestApprovalResponse",
            {"decision": "decline"},
        ),
        (
            "item/commandExecution/requestApproval",
            {},
            [_reply("deny", reason="wait_expired", interrupt=True)],
            "CommandExecutionRequestApprovalResponse",
            {"decision": "cancel"},
        ),
        (
            "item/fileChange/requestApproval",
            {},
            [_reply("cancel", interrupt=True)],
            "FileChangeRequestApprovalResponse",
            {"decision": "cancel"},
        ),
        (
            "item/permissions/requestApproval",
            _PERMISSIONS,
            [_reply("allow")],
            "PermissionsRequestApprovalResponse",
            {"permissions": {"network": {"enabled": True}}},
        ),
        (
            "mcpServer/elicitation/request",
            {},
            [_reply("deny", elicitation_action="decline")],
            "McpServerElicitationRequestResponse",
            {"action": "decline"},
        ),
        (
            "mcpServer/elicitation/request",
            {},
            [
                _reply(
                    "allow",
                    elicitation_action="accept",
                    elicitation_content={"environment": "staging"},
                )
            ],
            "McpServerElicitationRequestResponse",
            {"action": "accept", "content": {"environment": "staging"}},
        ),
        (
            "mcpServer/elicitation/request",
            {},
            [_reply("deny", reason="stop_fenced", interrupt=True, elicitation_action="cancel")],
            "McpServerElicitationRequestResponse",
            {"action": "cancel"},
        ),
        (
            "item/tool/requestUserInput",
            _QUESTIONS,
            [_reply("answer", answer="main"), _reply("answer", answer="yes", selected=("x",))],
            "ToolRequestUserInputResponse",
            {"answers": {"q1": {"answers": ["main"]}, "q2": {"answers": ["yes", "x"]}}},
        ),
    ],
)
def test_native_approval_responses_validate_against_the_pinned_schema(
    method: str,
    params: dict[str, Any],
    replies: list[NativeReply],
    definition: str,
    expected: dict[str, Any],
) -> None:
    answer = native_response(method, params, replies)
    assert not answer.is_error and answer.result == expected
    _file_validator(definition).validate(answer.result)


@pytest.mark.parametrize(
    ("method", "params", "replies", "code"),
    [
        # approve_edited: edits are not expressible natively (refused, never a silent accept).
        (
            "item/commandExecution/requestApproval",
            {},
            [_reply("allow", updated_arguments={"command": "ls"}, updated_input_digest=DIGEST_X)],
            -32001,
        ),
        ("item/permissions/requestApproval", _PERMISSIONS, [_reply("deny")], -32002),
        (
            "item/permissions/requestApproval",
            _PERMISSIONS,
            [_reply("deny", reason="task_cancelled", interrupt=True)],
            -32002,
        ),
        ("item/tool/requestUserInput", _QUESTIONS, [_reply("answer"), _reply("cancel")], -32002),
        (
            "item/tool/requestUserInput",
            _QUESTIONS,
            [_reply("deny", reason="wait_expired", interrupt=True)],
            -32002,
        ),
    ],
)
def test_refusals_are_typed_json_rpc_errors(
    method: str, params: dict[str, Any], replies: list[NativeReply], code: int
) -> None:
    answer = native_response(method, params, replies)
    assert answer.is_error and answer.error_code == code and answer.result is None


_NOTIFICATION_DEFINITIONS = {
    "turn/started": "TurnStartedNotification",
    "turn/completed": "TurnCompletedNotification",
    "item/started": "ItemStartedNotification",
    "item/completed": "ItemCompletedNotification",
    "thread/tokenUsage/updated": "ThreadTokenUsageUpdatedNotification",
    "item/agentMessage/delta": "AgentMessageDeltaNotification",
    "error": "ErrorNotification",
    "thread/compacted": "ContextCompactedNotification",
    "serverRequest/resolved": "ServerRequestResolvedNotification",
    "thread/status/changed": "ThreadStatusChangedNotification",
    "thread/started": "ThreadStartedNotification",
}
_SCRIPTS_WITHOUT_A_RELEASE = {"turn_hold", "turn_steer", "turn_disconnect"}


@pytest.mark.parametrize(
    "name",
    sorted(
        path.stem for path in SCRIPTS.glob("*.jsonl") if path.stem not in _SCRIPTS_WITHOUT_A_RELEASE
    ),
)
async def test_fixture_app_server_events_validate_against_the_pinned_schema(
    tmp_path: Path, name: str
) -> None:
    """Every notification and server request the FIXTURE server emitted for a committed
    script is a valid instance of the pinned schema, so the fixtures exercise the lane's
    reading of the real shapes rather than a dialect of their own."""

    stack = codex_stack(tmp_path, name)
    result = await stack.lanes.service.turn(
        stack.turn(segment=LaneSegmentBounds(max_duration_s=20, start_to_close_s=60)),
        RecordingSignals(stack.lanes.frames),
    )
    assert result.done
    validated = 0
    for launch in stack.launcher.launches:
        for event in launch.connection.stats.events:
            if event.kind == "notification" and event.method in _NOTIFICATION_DEFINITIONS:
                _validator(_NOTIFICATION_DEFINITIONS[event.method]).validate(event.params)
                validated += 1
            elif event.kind == "server_request":
                _server_request_params_validator(event.method).validate(event.params)
                validated += 1
    assert validated >= 5, name
