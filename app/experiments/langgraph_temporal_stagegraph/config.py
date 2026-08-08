from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


@dataclass(frozen=True)
class ExperimentSettings:
    application_database_dsn: str
    application_migration_database_dsn: str
    temporal_address: str
    temporal_namespace: str
    temporal_task_queue: str
    openai_model: str
    langsmith_tracing: bool
    overall_timeout_seconds: float


def load_settings(*, require_openai: bool = True) -> ExperimentSettings:
    project_root = Path(__file__).resolve().parents[3]
    load_dotenv(project_root / ".env", override=False)
    if require_openai and not os.getenv("OPENAI_API_KEY"):
        raise RuntimeError("OPENAI_API_KEY is required; add it to .env or the worker environment")
    application_dsn = os.getenv("APPLICATION_DATABASE_DIRECT") or os.getenv(
        "APPLICATION_DATABASE_URL"
    )
    if not application_dsn:
        raise RuntimeError("APPLICATION_DATABASE_DIRECT or APPLICATION_DATABASE_URL is required")
    return ExperimentSettings(
        application_database_dsn=application_dsn,
        application_migration_database_dsn=(
            os.getenv("APPLICATION_MIGRATION_DATABASE_DIRECT") or application_dsn
        ),
        temporal_address=os.getenv("TEMPORAL_ADDRESS", "localhost:7233"),
        temporal_namespace=os.getenv("TEMPORAL_NAMESPACE", "default"),
        temporal_task_queue=os.getenv(
            "STAGEGRAPH_EXPERIMENT_TASK_QUEUE",
            "stagegraph-temporal-deepagents-experiment",
        ),
        openai_model=os.getenv("OPENAI_MODEL", "gpt-5.4-nano"),
        langsmith_tracing=os.getenv("LANGSMITH_TRACING", "false").lower() in {"1", "true", "yes"},
        overall_timeout_seconds=float(os.getenv("STAGEGRAPH_EXPERIMENT_TIMEOUT", "120")),
    )
