"""MP-20: a Session Lane's declared outputs become workspace candidates of its operation.

A Deep Agents attempt registers every file it wrote into a writable slot as a captured
workspace candidate (`workspace-candidate://<id>`, a `workspace_candidate_descriptor` row and
content-addressed bytes); the Context Packer, the Stage Graph handoff and the chain release
resolve only such registered refs. A Session Lane (Claude Agent SDK, Codex, Cursor) leases its
own worktree, and its agent writes declared outputs under `outputs/` (the operating contract).
Before this module the lanes staged those files as opaque payload locators that nothing
downstream could resolve, so a provider stage could never hand an accepted output to the next
node or to a chain consumer.

`WorkspaceCandidateLaneOutputs` is the custody port the lanes' output collection calls at
`end_session` (after the session stopped, before the lease is released): the file becomes a
candidate of the exact operation binding, at the writable slot path the binding declares, with
its digest checked by `WorkspaceCandidateCaptureService`. The binding's workspace manifest is
materialized first (idempotent; the lane never mounts it, it is the custody record the
candidate hangs off). A path outside `outputs/`, an operation without a compiled writable slot
or an uncompiled workspace returns `None`, and the lane keeps staging the file as before.
"""

from __future__ import annotations

from pathlib import PurePosixPath
from typing import Protocol

from mission_control.application.artifacts.workspace_candidates import (
    WorkspaceCandidateCaptureService,
)
from mission_control.application.execution.operations.operation_execution import (
    SandboxPort,
    bind_operation_execution_request,
)
from mission_control.domain.context.refs import workspace_candidate_ref
from mission_control.domain.execution.contracts import (
    OperationExecutionRequest,
    WorkspaceContract,
)

OUTPUTS_DIR = "outputs"


class LaneOutputCustody(Protocol):
    """What a Session Lane's output collection calls for each declared output file."""

    async def register(
        self,
        operation: OperationExecutionRequest,
        path: str,
        content: bytes,
        *,
        mount_root: str = "",
    ) -> str | None:
        """The registered ref of `path` (relative to the lease, under `outputs/`), or `None`
        when this operation declares no slot for it (the lane then stages it as before)."""
        ...


def declared_output_path(
    workspace: WorkspaceContract, path: str, *, mount_root: str = ""
) -> str | None:
    """The writable slot path a lane file `outputs/<rel>` is captured at, or `None`.

    A writable slot that is itself under `outputs/` (the FT-G lane operations) keeps the
    file's own path; otherwise the first writable slot of the binding receives `<rel>` (the
    manifest Stage Graph `output` and Goal Loop `work` slots).
    """

    relative = PurePosixPath(path)
    parts = relative.parts
    if (
        relative.is_absolute()
        or len(parts) < 2
        or parts[0] != OUTPUTS_DIR
        or any(part in {"", ".", ".."} for part in parts)
    ):
        return None
    prefix = mount_root.rstrip("/")
    writable = [slot for slot in workspace.slot_bindings if slot.access == "exclusive_write"]
    for slot in writable:
        logical = slot.logical_path
        mounted = bool(prefix) and logical.startswith(prefix + "/")
        slot_relative = (logical[len(prefix) :] if mounted else logical).strip("/")
        if relative.as_posix() == slot_relative:
            return logical
        if relative.as_posix().startswith(slot_relative + "/"):
            return f"{logical.rstrip('/')}/{relative.as_posix()[len(slot_relative) + 1 :]}"
    if not writable:
        return None
    return f"{writable[0].logical_path.rstrip('/')}/{PurePosixPath(*parts[1:]).as_posix()}"


class WorkspaceCandidateLaneOutputs:
    """`LaneOutputCustody` over the deployment's workspace candidate capture service."""

    def __init__(
        self, *, workspaces: SandboxPort, candidates: WorkspaceCandidateCaptureService
    ) -> None:
        self._workspaces = workspaces
        self._candidates = candidates
        # Materialization is idempotent; remembering the last binding skips re-provisioning
        # for the other files of the same `end_session` without growing per worker.
        self._materialized: tuple[str, str] | None = None

    async def register(
        self,
        operation: OperationExecutionRequest,
        path: str,
        content: bytes,
        *,
        mount_root: str = "",
    ) -> str | None:
        workspace = operation.workspace
        if not workspace.slot_bindings or workspace.workflow_contract_digest is None:
            return None
        logical = declared_output_path(workspace, path, mount_root=mount_root)
        if logical is None:
            return None
        binding = bind_operation_execution_request(operation)
        key = (binding.request_scope, binding.binding_id)
        if self._materialized != key:
            await self._workspaces.materialize(binding)
            self._materialized = key
        candidate = await self._candidates.capture(binding, logical, content)
        return workspace_candidate_ref(candidate.candidate_id)


__all__ = [
    "OUTPUTS_DIR",
    "LaneOutputCustody",
    "WorkspaceCandidateLaneOutputs",
    "declared_output_path",
]
