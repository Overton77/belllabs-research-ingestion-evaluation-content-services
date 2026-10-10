"""Incremental subordinate lineage for live provider-frame streams (SPEC-04, ADR-0040).

`lineage.provider_subordinates` reads a whole list of persisted frames. A live stream sees
frames page by page, so `LineageTracker` folds the same rules over frames in arrival order
and answers, at any point, exactly what `provider_subordinates` would answer for the frames
observed so far (proven against it in `tests/unit/frames/test_lineage_stream.py`):

- identity and resolution come only from stable native refs (Claude `tool_use_id` /
  `task_id`, Codex thread ids and the parent's `collabAgentToolCall` spawn items); nothing is
  inferred from message text and an unmatched child stays `resolved=False`;
- visibility is the coverage actually persisted so far (`full` once a message or tool frame
  of the child exists, `lifecycle_only` while only lifecycle/status frames do, `unavailable`
  for spawn evidence alone); it only ever widens as frames arrive;
- the tracker never creates frames or child detail: it reports transitions (`started`,
  `ended`, visibility widened) of children it has observed.

The tracker holds bounded per-child state (kinds, counts, the last Claude `task_id`), never
frame bodies, so memory grows with the number of children and not with transcript length.
A tracker that did not observe an execution from its first frame is `complete=False`; it
then reports no `started` transition (the child may have started earlier) and its
visibility is a lower bound.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Literal

from mission_control.application.frames.lineage import (
    _CONTENT_KINDS,
    _ENDED_TASK_STATUSES,
    SubordinateNode,
    harness_execution_ref,
)
from mission_control.application.frames.provider_mapping import (
    CLAUDE_TASK_ID_PREFIX,
    CLAUDE_TASK_PREFIX,
    CODEX_THREAD_PREFIX,
)
from mission_control.domain.frames.body import frame_body_object
from mission_control.domain.frames.contracts import FrameKind, LaneProfile, ProviderFrame
from mission_control.domain.subscriptions.streams import SubordinateRef, Visibility

ChildKey = tuple[str, int, str]
Transition = Literal["started", "observed", "updated", "ended"]
"""`started`: first frame of a child in a complete tracker; `observed`: first frame seen by an
incomplete tracker; `updated`: visibility widened or the spawn resolved; `ended`: the child's
own end evidence (Claude task notification / terminal status, Codex child `turn_ended`)."""


@dataclass
class _Child:
    kinds: set[FrameKind] = field(default_factory=set)
    count: int = 0
    task_id: str | None = None
    ended: bool = False


@dataclass
class _Execution:
    lane: LaneProfile
    fallback_session: str | None
    parent_session: str | None = None
    has_parent_frame: bool = False
    spawns: dict[str, str] = field(default_factory=dict)

    @property
    def session(self) -> str | None:
        return self.parent_session if self.has_parent_frame else self.fallback_session


@dataclass(frozen=True)
class LineageChange:
    """What one observed frame changed in the lineage graph."""

    transitions: tuple[Transition, ...] = ()
    nodes: tuple[SubordinateNode, ...] = ()


def _visibility(kinds: set[FrameKind]) -> Visibility:
    if kinds & _CONTENT_KINDS:
        return "full"
    return "lifecycle_only" if kinds else "unavailable"


class LineageTracker:
    """Provider-subagent lineage of frames observed in arrival order."""

    def __init__(self, *, complete: bool = True) -> None:
        self.complete = complete
        self._executions: dict[tuple[str, int], _Execution] = {}
        self._children: dict[ChildKey, _Child] = {}

    def __len__(self) -> int:
        return len(self._children)

    def observe(self, frame: ProviderFrame) -> LineageChange:
        """Fold one frame; returns the transitions it caused and the nodes they touched."""

        execution_key = (str(frame.harness_execution_id), frame.generation)
        execution = self._executions.get(execution_key)
        if execution is None:
            execution = _Execution(frame.lane_profile, frame.native_session_ref)
            self._executions[execution_key] = execution
        if frame.subordinate_ref is None:
            if not execution.has_parent_frame:
                execution.has_parent_frame = True
                execution.parent_session = frame.native_session_ref
            return self._observe_spawn(execution_key, execution, frame)
        key = (*execution_key, frame.subordinate_ref)
        child = self._children.get(key)
        before = None if child is None else self._node(key, child)
        if child is None:
            child = _Child()
            self._children[key] = child
        child.kinds.add(frame.kind)
        child.count += 1
        if execution.lane == LaneProfile.CLAUDE_AGENT_SDK:
            body = frame_body_object(frame.body_excerpt, frame.body_bytes)
            if isinstance(body.get("task_id"), str):
                child.task_id = str(body["task_id"])
            status = body.get("status")
            if body.get("subtype") == "task_notification" or (
                isinstance(status, str) and status in _ENDED_TASK_STATUSES
            ):
                child.ended = True
        elif (
            execution.lane == LaneProfile.CODEX
            and frame.subordinate_ref.startswith(CODEX_THREAD_PREFIX)
            and frame.kind == FrameKind.TURN_ENDED
        ):
            child.ended = True
        after = self._node(key, child)
        transitions: list[Transition] = []
        if before is None:
            # An incomplete tracker cannot know whether the child started before it looked.
            transitions.append("started" if self.complete else "observed")
        elif before.ref != after.ref:
            transitions.append("updated")
        if after.lifecycle == "ended" and (before is None or before.lifecycle != "ended"):
            transitions.append("ended")
        return LineageChange(tuple(transitions), (after,) if transitions else ())

    def _observe_spawn(
        self, execution_key: tuple[str, int], execution: _Execution, frame: ProviderFrame
    ) -> LineageChange:
        if execution.lane != LaneProfile.CODEX:
            return LineageChange()
        item = frame_body_object(frame.body_excerpt, frame.body_bytes).get("item")
        if not isinstance(item, Mapping) or item.get("type") != "collabAgentToolCall":
            return LineageChange()
        receivers = item.get("receiverThreadIds")
        item_id = item.get("id")
        if not isinstance(receivers, list) or not isinstance(item_id, str):
            return LineageChange()
        touched: list[SubordinateNode] = []
        for receiver in receivers:
            if not isinstance(receiver, str) or receiver in execution.spawns:
                continue
            execution.spawns[receiver] = item_id
            key = (*execution_key, CODEX_THREAD_PREFIX + receiver)
            child = self._children.get(key)
            # A spawn item resolves a child already seen, or announces a spawn-only child.
            touched.append(
                self._node(key, child) if child is not None else self._spawn_only(key, receiver)
            )
        return LineageChange(("updated",) if touched else (), tuple(touched))

    def ref_for(self, frame: ProviderFrame) -> SubordinateRef | None:
        if frame.subordinate_ref is None:
            return None
        key = (str(frame.harness_execution_id), frame.generation, frame.subordinate_ref)
        child = self._children.get(key)
        return None if child is None else self._node(key, child).ref

    def nodes(self) -> tuple[SubordinateNode, ...]:
        """Exactly `provider_subordinates(observed frames)`, in its order."""

        nodes: list[SubordinateNode] = []
        for execution_key in sorted(self._executions):
            execution = self._executions[execution_key]
            children = sorted(key for key in self._children if key[:2] == execution_key)
            for key in children:
                nodes.append(self._node(key, self._children[key]))
            for child_id in sorted(execution.spawns):
                if (*execution_key, CODEX_THREAD_PREFIX + child_id) in self._children:
                    continue
                nodes.append(self._spawn_only((*execution_key, ""), child_id))
        return tuple(nodes)

    def _spawn_only(self, key: ChildKey, child_id: str) -> SubordinateNode:
        execution = self._executions[key[:2]]
        return SubordinateNode(
            ref=SubordinateRef(
                kind="provider_subagent",
                parent_execution_ref=harness_execution_ref(key[0]),
                native_parent_ref=execution.session,
                native_child_ref=child_id,
                spawn_correlation=execution.spawns[child_id],
                generation=key[1],
                visibility="unavailable",
            ),
            lane=execution.lane,
            resolved=True,
            lifecycle="unknown",
        )

    def _node(self, key: ChildKey, child: _Child) -> SubordinateNode:
        execution = self._executions[key[:2]]
        subordinate = key[2]
        native_child: str | None = subordinate
        spawn: str | None = None
        resolved = True
        if execution.lane == LaneProfile.CLAUDE_AGENT_SDK:
            if subordinate.startswith(CLAUDE_TASK_PREFIX):
                native_child = child.task_id
                spawn = subordinate.removeprefix(CLAUDE_TASK_PREFIX)
            else:
                native_child = subordinate.removeprefix(CLAUDE_TASK_ID_PREFIX)
                resolved = False
        elif execution.lane == LaneProfile.CODEX and subordinate.startswith(CODEX_THREAD_PREFIX):
            native_child = subordinate.removeprefix(CODEX_THREAD_PREFIX)
            spawn = execution.spawns.get(native_child)
            resolved = spawn is not None
        return SubordinateNode(
            ref=SubordinateRef(
                kind="provider_subagent",
                parent_execution_ref=harness_execution_ref(key[0]),
                native_parent_ref=execution.session,
                native_child_ref=native_child,
                spawn_correlation=spawn,
                generation=key[1],
                visibility=_visibility(child.kinds),
            ),
            lane=execution.lane,
            resolved=resolved,
            lifecycle="ended" if child.ended else "started",
            frame_count=child.count,
        )

    def index(self) -> dict[tuple[str, int, str], SubordinateRef]:
        """(execution ref, generation, frame `subordinate_ref`) -> the current lineage ref."""

        return {
            (harness_execution_ref(key[0]), key[1], key[2]): self._node(key, child).ref
            for key, child in self._children.items()
        }


__all__ = ["LineageChange", "LineageTracker", "Transition"]
