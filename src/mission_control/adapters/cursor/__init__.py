"""Cursor lane family (ADR-0030; SPEC-07 sections 5 and 6).

`cursor_local` (FT-G3) is `local.CursorLocalHarness`: the pinned `cursor-sdk` bridge on the
worker behind the `bridge.CursorBridgeLauncher` port, git worktree leases, Host Projections
with fail-closed Kernel Hooks calling the worker back, frames keyed by bridge offset.
`cursor_cloud` (FT-G5) is `cloud.CursorCloudHarness`: Cloud Agents API v1 over `httpx`, branch
`mc/<run>` published through the SCM, SSE resume with `Last-Event-ID`, artifacts and usage.
The stubs below stand in for a profile a process does not compose. Both profiles stay
`qualified=False` until FT-G6 records a qualification. Importing this package imports no SDK;
the SDK loads lazily inside the bridge adapter.
"""

from __future__ import annotations

from mission_control.application.execution.harness.describe import declared_matrix
from mission_control.application.execution.harness.protocol import UnsupportedHarnessOperations
from mission_control.domain.execution.lanes import LaneDescribe


class CursorLaneStub(UnsupportedHarnessOperations):
    """A registered, unqualified Cursor profile whose operations are not implemented yet."""

    def __init__(self, lane_profile: str) -> None:
        declared = declared_matrix(lane_profile)
        if declared.lane != "cursor":
            raise ValueError(f"{lane_profile} is not a Cursor lane profile")
        self._describe = declared.unqualified()

    def describe(self) -> LaneDescribe:
        return self._describe


def cursor_lane_stubs() -> tuple[CursorLaneStub, CursorLaneStub]:
    return CursorLaneStub("cursor_local"), CursorLaneStub("cursor_cloud")


__all__ = ["CursorLaneStub", "cursor_lane_stubs"]
