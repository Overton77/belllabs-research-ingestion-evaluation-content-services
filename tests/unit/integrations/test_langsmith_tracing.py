from __future__ import annotations

import os
from types import SimpleNamespace

import pytest
from pydantic import SecretStr

from mission_control.adapters.langsmith.tracing import (
    configure_langsmith_tracing,
    create_traced_async_openai,
    process_embedding_inputs,
    process_runtime_execute_inputs,
    process_runtime_execute_outputs,
    runtime_execute_metadata,
)
from mission_control.bootstrap.settings import Settings
from tests.fixtures.isolated_settings import isolated_settings

_LANGSMITH_ENV = (
    "LANGSMITH_TRACING",
    "LANGSMITH_API_KEY",
    "LANGSMITH_PROJECT",
    "LANGSMITH_ENDPOINT",
    "LANGSMITH_WORKSPACE_ID",
)


@pytest.fixture(autouse=True)
def _isolated_langsmith_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Start every test with no LANGSMITH_* variables and restore the originals afterwards.

    `configure_langsmith_tracing` exports into `os.environ`; recording each name with
    `setenv` first makes `monkeypatch` delete whatever a test exports even when the variable
    was originally absent, so no test leaks into a later one.
    """

    for name in _LANGSMITH_ENV:
        monkeypatch.setenv(name, "")
        monkeypatch.delenv(name)


def test_settings_expose_langsmith_contract() -> None:
    settings = isolated_settings()
    assert settings.langsmith_project == "BellLabsBiotech"
    assert settings.langsmith_endpoint.startswith("https://")
    assert settings.langsmith_tracing is False
    assert settings.langsmith_api_key is None


def test_configure_langsmith_tracing_exports_env() -> None:
    import mission_control.adapters.langsmith.tracing as tracing

    settings = isolated_settings(
        langsmith_api_key=SecretStr("lsv2_pt_test_key"),
        langsmith_tracing=True,
        langsmith_project="BellLabsBiotech-Test",
    )
    enabled = configure_langsmith_tracing(settings)
    assert enabled is True
    assert os.environ["LANGSMITH_TRACING"] == "true"
    assert os.environ["LANGSMITH_API_KEY"] == "lsv2_pt_test_key"
    assert os.environ["LANGSMITH_PROJECT"] == "BellLabsBiotech-Test"
    assert not hasattr(tracing, "_PROCESSOR_INSTALLED")


def test_create_traced_async_openai_returns_client() -> None:
    client = create_traced_async_openai("sk-test")
    assert client.api_key == "sk-test"
    assert hasattr(client, "responses")
    assert hasattr(client, "embeddings")


def test_runtime_execute_redaction_drops_secrets_and_prompt_bodies() -> None:
    invocation = SimpleNamespace(
        binding=SimpleNamespace(
            run_id="run-1",
            binding_id="bind-1",
            operation_id="op-1",
            request_scope="tenant-a",
            model_policy=SimpleNamespace(model="gpt-5.4-nano", provider="openai"),
            effective_configuration_digest="sha256:abc",
        ),
        resolved_secret_names=("environment:OPENAI_API_KEY",),
        prompt_segments=(SimpleNamespace(content="SECRET PROMPT SYNTHETIC-PHI-000-00-0000"),),
        workspace=SimpleNamespace(workspace_id="ws-1"),
    )
    redacted = process_runtime_execute_inputs(
        {
            "invocation": invocation,
            "resolved_secrets": {"environment:OPENAI_API_KEY": "sk-live-secret"},
        }
    )
    assert redacted["resolved_secrets"] == "[redacted]"
    assert "sk-live-secret" not in str(redacted)
    assert "SECRET PROMPT" not in str(redacted)
    assert "SYNTHETIC-PHI-000-00-0000" not in str(redacted)
    assert redacted["secret_names"] == ["environment:OPENAI_API_KEY"]
    assert redacted["prompt_segment_count"] == 1

    metadata = runtime_execute_metadata(invocation)
    assert metadata["run_id"] == "run-1"
    assert metadata["workspace_id"] == "ws-1"

    outputs = process_runtime_execute_outputs(
        SimpleNamespace(
            provider_run_id="resp_1",
            output_refs=("artifact:1",),
            structured_output={"ok": True},
            usage=SimpleNamespace(amounts={"model.input_tokens": 12}),
            output_text="full answer text",
        )
    )
    assert outputs["output_text_chars"] == len("full answer text")
    assert "full answer text" not in outputs.values()


def test_embedding_input_processor_summarizes_batch() -> None:
    summary = process_embedding_inputs({"texts": ("alpha", "beta")})
    assert summary == {"batch_size": 2, "input_chars": 9}


def test_langsmith_settings_optional_when_unset() -> None:
    settings = isolated_settings(langsmith_api_key=None, langsmith_tracing=False)
    assert isinstance(settings, Settings)
    assert configure_langsmith_tracing(settings) is False
