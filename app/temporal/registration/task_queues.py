from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class BellLabsTaskQueues:
    """Five stable logical isolation classes; process topology remains deployment-owned."""

    coordinator_family: str
    agent_cognitive: str
    ingestion_io: str
    sandbox_external_job: str
    verification_reconciliation: str

    def __post_init__(self) -> None:
        values = tuple(self.__dict__.values())
        if any(not value for value in values) or len(set(values)) != len(values):
            raise ValueError("BellLabs logical task queues must be non-empty and unique")

    @classmethod
    def from_base(cls, base: str) -> BellLabsTaskQueues:
        if not base:
            raise ValueError("base Temporal task queue must be non-empty")
        return cls(
            coordinator_family=f"{base}-coordinator-family",
            agent_cognitive=f"{base}-agent-cognitive",
            ingestion_io=f"{base}-ingestion-io",
            sandbox_external_job=f"{base}-sandbox-external-job",
            verification_reconciliation=f"{base}-verification-reconciliation",
        )
