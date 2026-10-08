"""Cursor lane family (ADR-0030; SPEC-07 sections 5 and 6).

FT-G1 registers the two Cursor Lane Profiles as stubs: their declared matrices are published
through `describe` (every control `unqualified`, `qualified=False`) so the registry, `lane list`
and admission can name them, and every operation raises `HarnessUnsupported`. The bridge
(`cursor_local`, FT-G3) and Cloud Agents API (`cursor_cloud`, FT-G5) harnesses replace these
stubs; no Cursor SDK is imported or called here.
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
