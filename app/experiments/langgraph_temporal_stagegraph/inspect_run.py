from __future__ import annotations

import argparse
import asyncio

from .config import load_settings
from .repository import ExperimentRepository


async def inspect(run_id: str) -> None:
    settings = load_settings(require_openai=False)
    repository = await ExperimentRepository.connect(settings.application_database_dsn)
    try:
        timeline = await repository.timeline(run_id)
        if timeline["run"] is None:
            raise SystemExit(f"unknown run {run_id}")
        print(
            f"run={run_id} status={timeline['run']['status']} thread={timeline['run']['thread_id']}"
        )
        for item in timeline["attempts"]:
            print(
                f"{item['stage_id']}: {item['status']} workflow={item['temporal_workflow_id']} "
                f"launched={item['launched_at']} completed={item['completed_at']} "
                f"admitted={item['admitted_at']}"
            )
        for event in timeline["outbox"]:
            print(
                f"wake={event['event_id']} attempts={event['delivery_attempts']} "
                f"delivered={event['delivered_at']}"
            )
    finally:
        await repository.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("run_id")
    args = parser.parse_args()
    asyncio.run(inspect(args.run_id))


if __name__ == "__main__":
    main()
