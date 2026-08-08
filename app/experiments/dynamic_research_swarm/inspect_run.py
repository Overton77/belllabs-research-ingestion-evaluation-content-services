from __future__ import annotations

import argparse
import asyncio

from app.experiments.langgraph_temporal_stagegraph.repository import ExperimentRepository

from .config import load_swarm_settings
from .repository import SwarmEvidenceRepository


async def inspect(run_id: str) -> None:
    settings = load_swarm_settings()
    repository = await ExperimentRepository.connect(settings.application_database_dsn)
    evidence = SwarmEvidenceRepository(repository.pool)
    try:
        timeline = await repository.timeline(run_id)
        records = await evidence.evidence_timeline(run_id)
        print(f"run={run_id} status={timeline['run']['status']}")
        for stage in timeline["attempts"]:
            print(
                f"stage={stage['stage_id']} status={stage['status']} "
                f"workflow={stage['temporal_workflow_id']}"
            )
        print(
            f"plans={len(records['plans'])} sources={len(records['sources'])} "
            f"claims={len(records['claims'])}"
        )
        for claim in records["claims"]:
            print(f"claim={claim['claim_id']} disposition={claim['disposition']}")
    finally:
        await repository.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("run_id")
    asyncio.run(inspect(parser.parse_args().run_id))


if __name__ == "__main__":
    main()
