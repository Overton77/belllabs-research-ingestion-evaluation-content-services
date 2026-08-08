from __future__ import annotations

import os
from dataclasses import dataclass

from app.experiments.langgraph_temporal_stagegraph.config import load_settings


@dataclass(frozen=True)
class SwarmSettings:
    application_database_dsn: str
    application_migration_database_dsn: str
    temporal_address: str
    temporal_namespace: str
    temporal_task_queue: str
    openai_model: str
    overall_timeout_seconds: float
    max_units: int
    max_sources_per_unit: int
    max_depth: int


def load_swarm_settings() -> SwarmSettings:
    base = load_settings()
    if not os.getenv("TAVILY_API_KEY"):
        raise RuntimeError("TAVILY_API_KEY is required for the live swarm experiment")
    return SwarmSettings(
        application_database_dsn=base.application_database_dsn,
        application_migration_database_dsn=base.application_migration_database_dsn,
        temporal_address=base.temporal_address,
        temporal_namespace=base.temporal_namespace,
        temporal_task_queue="dynamic-research-swarm-experiment",
        openai_model=base.openai_model,
        overall_timeout_seconds=float(os.getenv("SWARM_EXPERIMENT_TIMEOUT", "240")),
        max_units=int(os.getenv("SWARM_MAX_UNITS", "3")),
        max_sources_per_unit=int(os.getenv("SWARM_MAX_SOURCES", "3")),
        max_depth=int(os.getenv("SWARM_MAX_DEPTH", "1")),
    )
