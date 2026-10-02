"""The RRM-013 crash-window worker: runs the parent operation and stalls in a chosen window.

Run as `python -m tests.fixtures.rrm013_child_worker` by
`tests/integration/agent_server/test_rrm_013_async_subagent_live.py`. It composes the live
stack, executes the bound parent request as Activity attempt 1 with a short lease, and stalls
forever once the configured window is reached (after BellLabs reservation, before provider
submission; or after provider submission, before observation). The test kills this process.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from datetime import timedelta
from pathlib import Path

from app.domain.operation_execution.contracts import OperationExecutionRequest
from tests.fixtures.rrm013_live_stack import activity_attempt, open_live_stack


async def main() -> None:
    root = Path(os.environ["RRM013_ROOT"])
    request = OperationExecutionRequest.model_validate(
        json.loads((root / "request.json").read_text(encoding="utf-8"))
    )
    async with open_live_stack(
        os.environ["TEST_APPLICATION_POSTGRES_DSN"],
        mongo_uri=os.environ["TEST_MONGODB_URI"],
        mongo_database=os.environ["RRM013_MONGO_DATABASE"],
        endpoint=os.environ["AGENT_SERVER_ENDPOINT"],
        objective=os.environ["RRM013_OBJECTIVE"],
        crash_window=os.environ["RRM013_CRASH_WINDOW"],
        crash_marker=root / "crash-marker",
        model_log=root / "model-calls.jsonl",
        submitter_identity=f"rrm013-worker-1:{os.getpid()}",
    ) as stack:
        (root / "worker-1-ready").write_text(str(os.getpid()), encoding="utf-8")
        await stack.service.execute(
            request,
            activity_attempt(
                request, 1, lease=timedelta(seconds=int(os.environ["RRM013_LEASE_SECONDS"]))
            ),
        )


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.run(main(), loop_factory=asyncio.SelectorEventLoop)
    else:
        asyncio.run(main())
