from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True)
class TemporalStageInput:
    run_id: str
    thread_id: str
    attempt_id: str
    stage_id: str
    prompt: str
    delay_seconds: float
    model: str
    trace_headers: dict[str, str]


@dataclass(frozen=True)
class TemporalStageResult:
    attempt_id: str
    stage_id: str
    output_text: str
    output_digest: str


@dataclass(frozen=True)
class CompletionRecord:
    run_id: str
    thread_id: str
    attempt_id: str
    stage_id: str
    temporal_workflow_id: str
    temporal_run_id: str
    disposition: Literal["succeeded", "failed", "cancelled"]
    output_text: str | None
    output_digest: str | None
    error_type: str | None


def attempt_identity(run_id: str, stage_id: str, attempt_number: int = 1) -> str:
    return f"attempt:{run_id}:{stage_id}:{attempt_number}"


def workflow_identity(attempt_id: str) -> str:
    return f"stagegraph-experiment:{attempt_id}"


def completion_identity(attempt_id: str, output_digest: str) -> str:
    return f"completion:{attempt_id}:{output_digest}"


def digest_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()
