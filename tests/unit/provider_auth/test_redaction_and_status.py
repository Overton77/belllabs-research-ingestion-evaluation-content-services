"""MP-05 redaction and status parsing. Status outputs are hand-written test doubles shaped
after the documented messages; no status command runs."""

from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path, PurePosixPath

import pytest

from mission_control.adapters.provider_auth.detection import (
    WorkerAuthContext,
    env_names_of,
    settings_key_names,
)
from mission_control.adapters.provider_auth.preflight import SubprocessStatusRunner
from mission_control.adapters.provider_auth.redaction import redact_text, redact_value
from mission_control.adapters.provider_auth.status import (
    StatusOutput,
    parse_claude_auth_status,
    parse_codex_login_status,
    parse_cursor_agent_status,
)
from mission_control.application.execution.auth_admission import looks_secret, secret_kinds

FAKE_SECRETS = [
    "sk-ant-api03-FAKEFAKEFAKEFAKE",
    "sk-proj-***FAKE0",
    "sk-FAKEFAKEFAKEFAKEFAKE",
    "Bearer abcdefghijklmnopqrstu",
    "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.c2lnbmF0dXJl",
    "ghp_FAKEFAKEFAKEFAKEFAKE",
    "AKIAABCDEFGHIJKLMNOP",
    "owner@example.invalid",
    "key_0123456789abcdef0123",
    "Abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGH",
]


@pytest.mark.parametrize("secret", FAKE_SECRETS)
def test_credential_shapes_are_detected_and_redacted(secret: str) -> None:
    text = f"status: {secret} done"
    assert looks_secret(text)
    redacted = redact_text(text)
    assert secret not in redacted
    assert not looks_secret(redacted)
    assert "<redacted:" in redacted


@pytest.mark.parametrize(
    "benign",
    [
        "sha256:" + "a" * 64,
        "claude-agent-sdk-owner-local-subscription-profile-for-testing",
        "risk-assessment",
        "https://code.claude.com/docs/en/authentication",
        "env:ANTHROPIC_API_KEY",
        "decision:owner-local-personal-use",
    ],
)
def test_identifiers_digests_and_references_are_not_secrets(benign: str) -> None:
    assert secret_kinds(benign) == ()


def test_redact_value_redacts_secret_keys_whatever_the_value() -> None:
    payload = {
        "access_token": "short",
        "nested": [{"apiKey": "x"}, {"note": "Bearer abcdefghijklmnop"}],
        "ok": "fine",
        "ChatGPT-Account-Id": "acct-1",
    }
    redacted = redact_value(payload)
    assert redacted["access_token"] == "<redacted>"
    assert redacted["nested"][0]["apiKey"] == "<redacted>"
    assert "abcdefghijklmnop" not in redacted["nested"][1]["note"]
    assert redacted["ok"] == "fine"
    assert redacted["ChatGPT-Account-Id"] == "<redacted>"


def test_codex_status_messages_follow_the_documented_source() -> None:
    chatgpt = parse_codex_login_status(StatusOutput(0, "", "Logged in using ChatGPT\n"))
    assert (chatgpt.signed_in, chatgpt.stored_route) == (True, "login")
    api = parse_codex_login_status(
        StatusOutput(0, "", "Logged in using an API key - sk-proj-***FAKE0\n")
    )
    assert (api.signed_in, api.stored_route) == (True, "api_key")
    assert "sk-" not in api.summary
    out = parse_codex_login_status(StatusOutput(1, "", "Not logged in\n"))
    assert out.signed_in is False
    odd = parse_codex_login_status(StatusOutput(1, "", "Error checking login status: boom"))
    assert odd.signed_in is None


def test_claude_status_parser_is_flagged_undocumented_and_fingerprints_accounts() -> None:
    reading = parse_claude_auth_status(
        StatusOutput(0, '{"loggedIn": true, "authMethod": "claude.ai", "email": "a@b.example"}', "")
    )
    assert reading.signed_in is True and reading.stored_route == "login"
    assert reading.format_documented is False
    assert reading.account_fingerprint is not None
    assert "a@b.example" not in reading.summary and "a@b.example" not in reading.account_fingerprint
    assert parse_claude_auth_status(StatusOutput(0, "not json", "")).signed_in is None
    api = parse_claude_auth_status(
        StatusOutput(0, '{"loggedIn": true, "authMethod": "api_key"}', "")
    )
    assert api.stored_route == "api_key"


def test_cursor_status_parser_only_classifies_explicit_phrases() -> None:
    assert parse_cursor_agent_status(StatusOutput(1, "Not logged in", "")).signed_in is False
    assert parse_cursor_agent_status(StatusOutput(0, "Logged in as x", "")).signed_in is True
    assert parse_cursor_agent_status(StatusOutput(0, "???", "")).signed_in is None


def test_env_names_drop_values() -> None:
    names = env_names_of({"ANTHROPIC_API_KEY": "sk-ant-FAKEFAKEFAKE", "EMPTY": " ", "PATH": "/bin"})
    assert names == frozenset({"ANTHROPIC_API_KEY", "PATH"})


def test_context_refuses_non_path_values() -> None:
    with pytest.raises(ValueError):
        WorkerAuthContext(
            env_names=frozenset(),
            home=PurePosixPath("/h"),
            path_env={"ANTHROPIC_API_KEY": "sk-ant-FAKE"},
        )
    context = WorkerAuthContext.from_environment(
        {"CODEX_HOME": "/srv/codex", "OPENAI_API_KEY": "sk-FAKEFAKEFAKEFAKE"},
        home=PurePosixPath("/h"),
        platform="linux",
    )
    assert dict(context.path_env) == {"CODEX_HOME": "/srv/codex"}
    assert "sk-FAKEFAKEFAKEFAKE" not in repr(context)


def test_settings_key_names_reads_names_only(tmp_path: Path) -> None:
    path = tmp_path / "settings.json"
    path.write_text(
        '{"apiKeyHelper": "/bin/helper", "env": {"ANTHROPIC_API_KEY": "sk-ant-FAKEFAKEFAKE"}}',
        encoding="utf-8",
    )
    keys = settings_key_names(path)
    assert keys == (frozenset({"apiKeyHelper", "env"}), frozenset({"ANTHROPIC_API_KEY"}))
    assert "sk-ant" not in repr(keys)
    (tmp_path / "bad.json").write_text("[", encoding="utf-8")
    assert settings_key_names(tmp_path / "bad.json") is None


async def test_subprocess_status_runner_is_bounded_and_tolerates_missing_binaries() -> None:
    """Runs the current Python interpreter as a stand-in status command (no vendor CLI)."""

    runner = SubprocessStatusRunner(dict(os.environ), timeout_s=10)
    output = await runner.run(
        [sys.executable, "-c", "import sys; sys.stderr.write('Not logged in')"]
    )
    assert output is not None and output.stderr == "Not logged in"
    assert await runner.run(["mc-definitely-not-a-binary-xyz"]) is None
    slow = SubprocessStatusRunner(dict(os.environ), timeout_s=0.2)
    started = asyncio.get_running_loop().time()
    assert await slow.run([sys.executable, "-c", "import time; time.sleep(5)"]) is None
    assert asyncio.get_running_loop().time() - started < 4
