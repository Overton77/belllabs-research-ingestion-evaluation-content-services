from __future__ import annotations

from temporalio.worker.workflow_sandbox import (
    SandboxedWorkflowRunner,
    SandboxRestrictions,
)


def coordinator_workflow_runner() -> SandboxedWorkflowRunner:
    """Keep process-wide type-check import hooks outside workflow sandboxes.

    FastMCP imports beartype's ``claw`` hook in the worker process. Re-importing
    that hook while Temporal validates a workflow can observe its process-global
    state half initialized. Beartype is runtime instrumentation rather than
    workflow authority, so pass the already imported package through unchanged.
    """

    return SandboxedWorkflowRunner(
        restrictions=SandboxRestrictions.default.with_passthrough_modules(
            "beartype",
        )
    )


__all__ = ["coordinator_workflow_runner"]
