"""A `shared` GoalDirected workspace joins the next iteration whatever its Context Packet holds.

Found by the FT-D3 acceptance: every iteration's unit carries the read-only `ctx-` slots of its
own Context Packet (`.mission/context.md`, `.mission/inputs.json`, materialized inputs) under
its role root. Their digests and their number change from one iteration to the next, so the
RRM-020 rule (same compiled slot set at every iteration) refused iteration 2 of every packed
shared Goal Loop as `IdempotencyConflict` before its runtime was invoked. The packet slots no
longer decide the compiled shape; every other slot still does.
"""

from __future__ import annotations

from hashlib import sha256
from pathlib import Path

import pytest

from mission_control.adapters.storage.filesystem_workspace import (
    FilesystemWorkspaceProvisioner,
    is_read_only,
)
from mission_control.application.artifacts.workspace_materialization import (
    InMemoryDurableWorkspaceInputs,
    InMemoryWorkspaceManifestRepository,
    WorkspaceMaterializationService,
)
from mission_control.domain.execution.contracts import (
    WorkspaceMaterializationRequest,
    WorkspaceSlotBinding,
)
from mission_control.domain.policies.errors import IdempotencyConflict
from tests.unit.workspaces.test_goal_role_slot_ownership import _goal_request
from tests.unit.workspaces.test_workspace_materialization import RecordingProvisioner

WORKSPACE = "run/run-1/execution-epoch/1/goal/workspace/1"


def _digest(content: bytes) -> str:
    return f"sha256:{sha256(content).hexdigest()}"


def _packed(iteration: int, files: dict[str, bytes]) -> WorkspaceMaterializationRequest:
    """The executor's compiled `/work` slot plus its packet's read-only `ctx-` slots."""

    request = _goal_request(WORKSPACE, iteration, "executor")
    owner = request.slots[0].owner
    packet = tuple(
        WorkspaceSlotBinding(
            slot_name=f"ctx-{index}",
            logical_path=f"/goal/{iteration}/executor/{path}",
            access="read_only",
            owner=owner,
            durable_ref=f"context:{iteration}:{path}",
            content_digest=_digest(content),
        )
        for index, (path, content) in enumerate(sorted(files.items()))
    )
    return request.model_copy(update={"slots": (*request.slots, *packet)})


def _service(files: dict[str, bytes]) -> WorkspaceMaterializationService:
    return WorkspaceMaterializationService(
        manifests=InMemoryWorkspaceManifestRepository(),
        provisioner=RecordingProvisioner(),
        durable_inputs=InMemoryDurableWorkspaceInputs(files),
    )


@pytest.mark.asyncio
async def test_the_next_iteration_joins_with_a_different_packet() -> None:
    first = {".mission/context.md": b"# iteration 1", ".mission/inputs.json": b"[]"}
    second = {
        ".mission/context.md": b"# iteration 2 with the verifier findings",
        ".mission/inputs.json": b'[{"path": "inputs/prior.json"}]',
        "inputs/prior.json": b"{}",
    }
    durable = {
        f"context:{iteration}:{path}": content
        for iteration, files in ((1, first), (2, second))
        for path, content in files.items()
    }
    service = _service(durable)
    await service.materialize(_packed(1, first))
    joined = await service.materialize(_packed(2, second))
    manifest = joined.materialization_manifest
    assert manifest is not None and manifest.revision == 2
    assert [slot.logical_path for slot in manifest.slots if slot.slot_name == "work"] == [
        "/goal/1/executor/work",
        "/goal/2/executor/work",
    ]
    assert {slot.logical_path for slot in manifest.slots if slot.slot_name.startswith("ctx-")} == {
        f"/goal/1/executor/{path}" for path in first
    } | {f"/goal/2/executor/{path}" for path in second}
    # A retry of the joined iteration appends nothing.
    again = await service.materialize(_packed(2, second))
    assert again.materialization_manifest == manifest


@pytest.mark.asyncio
async def test_compiled_slots_still_decide_the_shape() -> None:
    files = {".mission/context.md": b"# packet"}
    durable = {f"context:{iteration}:{path}": b"# packet" for iteration in (1, 2) for path in files}
    service = _service(durable)
    await service.materialize(_packed(1, files))
    request = _packed(2, files)
    # A declared (non-packet) read-only input the first iteration did not have is refused.
    extra = WorkspaceSlotBinding(
        slot_name="input",
        logical_path="/goal/2/executor/input",
        access="read_only",
        owner=request.slots[0].owner,
        durable_ref="context:2:.mission/context.md",
        content_digest=_digest(b"# packet"),
    )
    with pytest.raises(IdempotencyConflict, match="different materialization"):
        await service.materialize(request.model_copy(update={"slots": (*request.slots, extra)}))


@pytest.mark.asyncio
async def test_the_filesystem_provisioner_remounts_earlier_read_only_inputs(
    tmp_path: Path,
) -> None:
    """The next iteration re-provisions the whole shared workspace: the earlier iteration's
    read-only packet files are already mounted (0444) and are verified, not rewritten."""

    first = {".mission/context.md": b"# iteration 1"}
    second = {".mission/context.md": b"# iteration 2"}
    durable = {
        f"context:{iteration}:{path}": content
        for iteration, files in ((1, first), (2, second))
        for path, content in files.items()
    }
    service = WorkspaceMaterializationService(
        manifests=InMemoryWorkspaceManifestRepository(),
        provisioner=FilesystemWorkspaceProvisioner(tmp_path),
        durable_inputs=InMemoryDurableWorkspaceInputs(durable),
    )
    await service.materialize(_packed(1, first))
    joined = await service.materialize(_packed(2, second))
    assert joined.manifest_revision == 2
    retried = await service.materialize(_packed(2, second))
    assert retried.manifest_revision == 2
    assert _mounted(tmp_path) == [(b"# iteration 1", True), (b"# iteration 2", True)]


def _mounted(root: Path) -> list[tuple[bytes, bool]]:
    return [(path.read_bytes(), is_read_only(path)) for path in sorted(root.rglob("context.md"))]
