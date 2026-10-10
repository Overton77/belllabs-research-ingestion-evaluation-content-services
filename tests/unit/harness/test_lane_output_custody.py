"""MP-20: the Session Lanes' output collection hands declared outputs to the custody port.

Offline (G1). With a custody port composed, a file the agent wrote under the lease's
`outputs/` is registered through it (the port's ref replaces the opaque staged locator);
without one, the lane stages it exactly as before. The production port
(`WorkspaceCandidateLaneOutputs`) is covered in `tests/unit/operations/test_lane_outputs.py`
and end to end in `tests/integration/temporal/test_mp20_workflow_parity.py`.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from mission_control.adapters.claude.harness import _output_files
from mission_control.domain.execution.contracts import OperationExecutionRequest
from tests.fixtures.cursor_local import local_stack
from tests.fixtures.lane_turns import RecordingSignals
from tests.unit.harness.test_cursor_local import _identity, _service, _turn


class RecordingCustody:
    """FIXTURE `LaneOutputCustody`: registers every `outputs/` file under a fake ref."""

    def __init__(self) -> None:
        self.registered: list[tuple[str, str, bytes, str]] = []

    async def register(
        self,
        operation: OperationExecutionRequest,
        path: str,
        content: bytes,
        *,
        mount_root: str = "",
    ) -> str | None:
        self.registered.append((operation.identity.semantic_key, path, content, mount_root))
        return f"workspace-candidate://fixture-{len(self.registered)}"


@pytest.mark.asyncio
async def test_cursor_local_registers_declared_outputs_through_the_custody_port(
    tmp_path: Path,
) -> None:
    stack = local_stack(tmp_path)
    custody = RecordingCustody()
    stack.harness._outputs = custody  # the composed port (production: the deployment's)
    lanes = _service(stack)
    signals = RecordingSignals(stack.frames, _identity(stack).harness_execution_id)
    result = await lanes.service.turn(_turn(stack), signals)

    assert result.done and result.closing_facts is not None
    (registered,) = custody.registered
    assert registered[0] == stack.operation.identity.semantic_key
    assert registered[1] == "outputs/report.md"
    assert "workspace-candidate://fixture-1" in result.closing_facts.output_refs
    # The registered output is not also staged as an opaque output locator (the session's
    # snapshot still freezes `outputs/` with the rest of the lease, as before).
    heid = _identity(stack).harness_execution_id
    assert f"cursor-local/{heid}/outputs/report.md" not in {
        staged[0] for staged in stack.artifacts.staged.values()
    }


def test_claude_collects_only_regular_files_under_outputs(tmp_path: Path) -> None:
    (tmp_path / "outputs" / "nested").mkdir(parents=True)
    (tmp_path / "outputs" / "report.md").write_bytes(b"report")
    (tmp_path / "outputs" / "nested" / "data.json").write_bytes(b"{}")
    (tmp_path / "src.py").write_bytes(b"not an output")
    assert _output_files(tmp_path) == [
        ("outputs/nested/data.json", b"{}"),
        ("outputs/report.md", b"report"),
    ]
    assert _output_files(tmp_path / "missing") == []
