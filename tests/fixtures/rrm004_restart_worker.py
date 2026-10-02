"""The first RRM-004 worker process: it serves the operation and is killed mid-operation.

Run as `python -m tests.fixtures.rrm004_restart_worker` by
`tests/integration/temporal/test_rrm_004_worker_restart_recovery.py`. It connects to the
test's Temporal dev server, composes the persistent stack, and stalls forever right after
the configured checkpoint is durable; the test then kills this process.
"""

from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

from temporalio.client import Client
from temporalio.worker import Worker

from app.temporal.operation_activities import OperationExecutionActivities
from app.temporal.workflow_sandbox import coordinator_workflow_runner
from app.temporal.workflows.operation import OperationWorkflow
from tests.fixtures.rrm004_persistent_stack import (
    WORKFLOW_TASK_QUEUE,
    StackPaths,
    open_persistent_stack,
)


async def main() -> None:
    client = await Client.connect(os.environ["RRM004_TEMPORAL_TARGET"])
    paths = StackPaths(Path(os.environ["RRM004_ROOT"]))
    async with open_persistent_stack(
        os.environ["RRM004_DSN"],
        paths,
        mongo_uri=os.environ["RRM004_MONGO_URI"],
        mongo_database=os.environ["RRM004_MONGO_DATABASE"],
        hang_after_put=int(os.environ["RRM004_HANG_AFTER_CHECKPOINT"]),
    ) as stack:
        activities = OperationExecutionActivities(
            stack.service, worker_identity=f"rrm004-worker-1:{os.getpid()}"
        )
        async with (
            Worker(
                client,
                task_queue=WORKFLOW_TASK_QUEUE,
                workflows=[OperationWorkflow],
                workflow_runner=coordinator_workflow_runner(),
            ),
            Worker(
                client,
                task_queue=stack.binding.task_queue,
                activities=[activities.execute, activities.cancel],
            ),
        ):
            (paths.root / "worker-1-ready").write_text(str(os.getpid()), encoding="utf-8")
            await asyncio.Event().wait()


if __name__ == "__main__":
    if sys.platform == "win32":
        # Psycopg's async driver requires a selector loop on Windows.
        asyncio.run(main(), loop_factory=asyncio.SelectorEventLoop)
    else:
        asyncio.run(main())
