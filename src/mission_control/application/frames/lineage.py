"""Subordinate lineage, visibility coverage and usage aggregation (multi-provider SPEC-04).

Three subordinate kinds stay distinct (`domain/subscriptions/streams.SubordinateRef`):

- `provider_subagent`: a child the provider runs inside one harness execution (Claude
  Agent SDK Task subagents, Codex child threads, Deep Agents `task` subgraphs). Its frames
  live in the parent's Native Event Store rows with `subordinate_ref` set.
- `agent_server_child`: an Agent Server async child with its own execution, governed by the
  run's async-child authority (`AsyncChildAuthorityState`).
- `linked_mission`: a mission released by a chain link; its own run, budget and journal.

Lineage comes only from stable native refs (`parent_tool_use_id` / `tool_use_id`, Codex
thread ids, Agent Server child execution ids, chain link ids). A child whose spawn cannot
be matched stays visible and `resolved=False`; nothing is inferred from message text.

Visibility coverage is what was observed, never what a provider advertises: `full` when
the child's messages or tool frames were persisted, `lifecycle_only` when only lifecycle or
status frames were, `unavailable` when only spawn evidence exists.

Usage aggregation never double counts. Per lane and subordinate kind, each usage group is:

- `folded`: the lane's writer persists the child's own usage frames and fact derivation
  already sums them into the parent's turns (Deep Agents subgraph model calls; Codex child
  thread `thread/tokenUsage/updated`, one notification per model call);
- `provider_inclusive`: the parent's provider total already includes the child (Claude
  `ResultMessage.total_cost_usd`);
- `unattributable`: the parent total excludes the child and the child's split is not
  reported (Claude `ResultMessage.usage` covers the top-level loop only; `TaskUsage` has
  only `total_tokens`), so the aggregate is marked as excluding subordinates;
- `separate_execution`: an Agent Server child's own execution totals, added exactly once;
- `separate_mission`: a linked mission's usage, reported beside the parent and never added.

Claude facts: code.claude.com agent-sdk cost-tracking ("usage: excluded; total_cost_usd and
model_usage: included") via ctx7 /websites/code_claude_en_agent-sdk. Codex facts:
`ThreadTokenUsage.total` is cumulative and `last` is per response (codex-rs v2 thread.rs).
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Literal

from mission_control.application.frames.provider_mapping import (
    CLAUDE_TASK_ID_PREFIX,
    CLAUDE_TASK_PREFIX,
    CODEX_THREAD_PREFIX,
)
from mission_control.domain.frames.body import frame_body_object
from mission_control.domain.frames.contracts import FrameKind, LaneProfile, ProviderFrame
from mission_control.domain.frames.usage import (
    TOKEN_DIMENSIONS,
    UsageDimension,
    UsageDisposition,
    UsageReport,
)
from mission_control.domain.policies.contracts import AsyncChildAuthorityState
from mission_control.domain.subscriptions.streams import SubordinateKind, SubordinateRef, Visibility

UsageInclusion = Literal[
    "folded", "provider_inclusive", "unattributable", "separate_execution", "separate_mission"
]
Coverage = Literal["complete", "excludes_subordinates"]

_CONTENT_KINDS = frozenset(
    {
        FrameKind.MESSAGE,
        FrameKind.MESSAGE_DELTA,
        FrameKind.THINKING_DELTA,
        FrameKind.TOOL_CALL_STARTED,
        FrameKind.TOOL_CALL_DELTA,
        FrameKind.TOOL_CALL_COMPLETED,
        FrameKind.TOOL_CALL_FAILED,
    }
)


@dataclass(frozen=True)
class UsageRule:
    tokens: UsageInclusion
    cost: UsageInclusion


_PROVIDER_SUBAGENT_RULES: dict[LaneProfile, UsageRule] = {
    LaneProfile.DEEP_AGENTS: UsageRule(tokens="folded", cost="folded"),
    LaneProfile.CODEX: UsageRule(tokens="folded", cost="folded"),
    LaneProfile.CLAUDE_AGENT_SDK: UsageRule(tokens="unattributable", cost="provider_inclusive"),
}
_UNQUALIFIED_RULE = UsageRule(tokens="unattributable", cost="unattributable")


def usage_rule(lane: LaneProfile, kind: SubordinateKind) -> UsageRule:
    if kind == "agent_server_child":
        return UsageRule(tokens="separate_execution", cost="separate_execution")
    if kind == "linked_mission":
        return UsageRule(tokens="separate_mission", cost="separate_mission")
    return _PROVIDER_SUBAGENT_RULES.get(lane, _UNQUALIFIED_RULE)


def folds_subordinate_usage(lane: LaneProfile) -> bool:
    """Whether fact derivation sums a provider subagent's usage frames into parent turns."""

    return usage_rule(lane, "provider_subagent").tokens == "folded"


def counts_usage_frame(lane: LaneProfile, subordinate_ref: str | None) -> bool:
    """Whether a `usage` frame belongs in its parent turn's totals."""

    return subordinate_ref is None or folds_subordinate_usage(lane)


_CODEX_USAGE_FIELDS = {
    "inputTokens": "input_tokens",
    "cachedInputTokens": "cached_input_tokens",
    "outputTokens": "output_tokens",
    "reasoningOutputTokens": "reasoning_tokens",
    "input_tokens": "input_tokens",
    "cached_input_tokens": "cached_input_tokens",
    "output_tokens": "output_tokens",
    "reasoning_output_tokens": "reasoning_tokens",
}


def usage_body(lane: LaneProfile, body: Mapping[str, Any]) -> Mapping[str, Any]:
    """The usage a closing body reports, in `usage_report` field names.

    Codex `thread/tokenUsage/updated` carries a cumulative `total` and the per-response
    `last`; one notification is one model call, so a turn sums `last` and never `total`.
    `codex exec --json` reports `turn.completed{usage}` once per turn.
    """

    if lane != LaneProfile.CODEX:
        return body
    token_usage = body.get("tokenUsage")
    source = token_usage.get("last") if isinstance(token_usage, Mapping) else body.get("usage")
    if not isinstance(source, Mapping):
        return {}
    return {
        target: source[name]
        for name, target in _CODEX_USAGE_FIELDS.items()
        if isinstance(source.get(name), int) and not isinstance(source.get(name), bool)
    }


# --- Lineage --------------------------------------------------------------------------------


@dataclass(frozen=True)
class SubordinateNode:
    """One child as inspection shows it: the frozen ref plus what was observed."""

    ref: SubordinateRef
    lane: LaneProfile | None = None
    resolved: bool = True
    lifecycle: Literal["started", "ended", "unknown"] = "unknown"
    frame_count: int = 0

    @property
    def identity(self) -> tuple[str, str, str]:
        child = self.ref.native_child_ref or self.ref.spawn_correlation or ""
        return (self.ref.kind, self.ref.parent_execution_ref, child)


def harness_execution_ref(harness_execution_id: object) -> str:
    return f"harness_execution:{harness_execution_id}"


def _visibility(kinds: set[FrameKind]) -> Visibility:
    if kinds & _CONTENT_KINDS:
        return "full"
    return "lifecycle_only" if kinds else "unavailable"


_ENDED_TASK_STATUSES = frozenset({"completed", "failed", "stopped", "killed"})


def _claude_child(
    subordinate_ref: str, bodies: Iterable[Mapping[str, object]]
) -> tuple[str | None, str | None, bool, bool]:
    """(native child ref, spawn correlation, resolved, ended) for a Claude Task subagent."""

    task_id: str | None = None
    ended = False
    for body in bodies:
        if isinstance(body.get("task_id"), str):
            task_id = str(body["task_id"])
        status = body.get("status")
        if body.get("subtype") == "task_notification" or (
            isinstance(status, str) and status in _ENDED_TASK_STATUSES
        ):
            ended = True
    if subordinate_ref.startswith(CLAUDE_TASK_PREFIX):
        return task_id, subordinate_ref.removeprefix(CLAUDE_TASK_PREFIX), True, ended
    return subordinate_ref.removeprefix(CLAUDE_TASK_ID_PREFIX), None, False, ended


def _codex_spawns(frames: Iterable[ProviderFrame]) -> dict[str, str]:
    """Child thread id -> spawning `collabAgentToolCall` item id, from parent-thread items.

    The `collabAgentToolCall` field names (`receiverThreadIds`) follow codex-rs v2 item.rs as
    recalled for this fixture; they were not confirmed through ctx7 in this change.
    """

    spawns: dict[str, str] = {}
    for frame in frames:
        if frame.subordinate_ref is not None:
            continue
        item = frame_body_object(frame.body_excerpt, frame.body_bytes).get("item")
        if not isinstance(item, Mapping) or item.get("type") != "collabAgentToolCall":
            continue
        receivers = item.get("receiverThreadIds")
        item_id = item.get("id")
        if isinstance(receivers, list) and isinstance(item_id, str):
            for receiver in receivers:
                if isinstance(receiver, str):
                    spawns.setdefault(receiver, item_id)
    return spawns


def provider_subordinates(frames: Sequence[ProviderFrame]) -> tuple[SubordinateNode, ...]:
    """Provider subagents of the given executions, from persisted frames only."""

    grouped: dict[tuple[str, int, str], list[ProviderFrame]] = {}
    by_execution: dict[tuple[str, int], list[ProviderFrame]] = {}
    for frame in frames:
        by_execution.setdefault((str(frame.harness_execution_id), frame.generation), []).append(
            frame
        )
        if frame.subordinate_ref is not None:
            key = (str(frame.harness_execution_id), frame.generation, frame.subordinate_ref)
            grouped.setdefault(key, []).append(frame)
    nodes: list[SubordinateNode] = []
    seen: set[tuple[str, int, str]] = set()
    for (execution, generation), items in sorted(by_execution.items()):
        lane = items[0].lane_profile
        parent_session = next(
            (frame.native_session_ref for frame in items if frame.subordinate_ref is None),
            items[0].native_session_ref,
        )
        spawns = _codex_spawns(items) if lane == LaneProfile.CODEX else {}
        children = sorted(key for key in grouped if key[:2] == (execution, generation))
        for key in children:
            seen.add(key)
            child_frames = sorted(grouped[key], key=lambda frame: frame.arrival_ordinal)
            subordinate = key[2]
            bodies = [frame_body_object(f.body_excerpt, f.body_bytes) for f in child_frames]
            native_child: str | None = subordinate
            spawn: str | None = None
            resolved = True
            ended = False
            if lane == LaneProfile.CLAUDE_AGENT_SDK:
                native_child, spawn, resolved, ended = _claude_child(subordinate, bodies)
            elif lane == LaneProfile.CODEX and subordinate.startswith(CODEX_THREAD_PREFIX):
                native_child = subordinate.removeprefix(CODEX_THREAD_PREFIX)
                spawn = spawns.get(native_child)
                resolved = spawn is not None
                ended = any(f.kind == FrameKind.TURN_ENDED for f in child_frames)
            kinds = {frame.kind for frame in child_frames}
            nodes.append(
                SubordinateNode(
                    ref=SubordinateRef(
                        kind="provider_subagent",
                        parent_execution_ref=harness_execution_ref(execution),
                        native_parent_ref=parent_session,
                        native_child_ref=native_child,
                        spawn_correlation=spawn,
                        generation=generation,
                        visibility=_visibility(kinds),
                    ),
                    lane=lane,
                    resolved=resolved,
                    lifecycle="ended" if ended else "started",
                    frame_count=len(child_frames),
                )
            )
        for child, spawn in sorted(spawns.items()):
            if (execution, generation, CODEX_THREAD_PREFIX + child) in seen:
                continue
            nodes.append(
                SubordinateNode(
                    ref=SubordinateRef(
                        kind="provider_subagent",
                        parent_execution_ref=harness_execution_ref(execution),
                        native_parent_ref=parent_session,
                        native_child_ref=child,
                        spawn_correlation=spawn,
                        generation=generation,
                        visibility="unavailable",
                    ),
                    lane=lane,
                    resolved=True,
                    lifecycle="unknown",
                )
            )
    return tuple(nodes)


def agent_server_child(
    state: AsyncChildAuthorityState,
    *,
    generation: int | None = None,
    visibility: Visibility = "lifecycle_only",
) -> SubordinateNode:
    """An Agent Server async child: its own execution, spawned under a reservation."""

    return SubordinateNode(
        ref=SubordinateRef(
            kind="agent_server_child",
            parent_execution_ref=state.parent_operation_ref,
            native_child_ref=state.child_execution_id,
            spawn_correlation=state.reservation_id,
            generation=generation,
            visibility=visibility,
        ),
        lifecycle="started" if state.facts else "unknown",
    )


def linked_mission(
    *,
    link_id: str,
    from_mission_id: str,
    to_mission_id: str | None,
    released_run_id: str | None = None,
    visibility: Visibility = "lifecycle_only",
) -> SubordinateNode:
    """A mission released by a chain link: a separate run under its own journal and grants."""

    child = (
        f"run:{released_run_id}"
        if released_run_id
        else (f"mission:{to_mission_id}" if to_mission_id else None)
    )
    return SubordinateNode(
        ref=SubordinateRef(
            kind="linked_mission",
            parent_execution_ref=f"mission:{from_mission_id}",
            native_child_ref=child,
            spawn_correlation=f"chain_link:{link_id}",
            visibility=visibility if released_run_id else "unavailable",
        ),
        resolved=released_run_id is not None,
        lifecycle="started" if released_run_id else "unknown",
    )


# --- Usage aggregation ----------------------------------------------------------------------


@dataclass(frozen=True)
class ChildUsage:
    node: SubordinateNode
    usage: UsageReport | None


@dataclass(frozen=True)
class ChildAttribution:
    ref: SubordinateRef
    tokens: UsageInclusion
    cost: UsageInclusion
    usage: UsageReport | None


@dataclass(frozen=True)
class UsageAggregate:
    """Parent-inclusive totals, per-child attribution and coverage per dimension."""

    total: UsageReport | None
    coverage: dict[str, Coverage]
    children: tuple[ChildAttribution, ...] = ()
    separate: tuple[ChildAttribution, ...] = ()


def _add(left: UsageDimension, right: UsageDimension) -> UsageDimension:
    if left.value is None or right.value is None:
        return UsageDimension(value=None, disposition=UsageDisposition.UNKNOWN)
    settled = (
        left.disposition == UsageDisposition.SETTLED
        and right.disposition == UsageDisposition.SETTLED
    )
    return UsageDimension(
        value=left.value + right.value,
        disposition=UsageDisposition.SETTLED if settled else UsageDisposition.ESTIMATED,
    )


def _unknown() -> UsageDimension:
    return UsageDimension(value=None, disposition=UsageDisposition.UNKNOWN)


def aggregate_usage(
    lane: LaneProfile,
    parent: UsageReport | None,
    children: Sequence[ChildUsage],
) -> UsageAggregate:
    """Recursive-inspection totals that never count a child twice.

    A child is counted at most once by identity; only `separate_execution` children add to
    the parent total; `separate_mission` children are listed separately and never added.
    """

    dimensions = (*TOKEN_DIMENSIONS, "cost_micros")
    totals: dict[str, UsageDimension] = (
        dict(parent.dimensions) if parent is not None else {name: _unknown() for name in dimensions}
    )
    coverage: dict[str, Coverage] = dict.fromkeys(dimensions, "complete")
    attributed: list[ChildAttribution] = []
    separate: list[ChildAttribution] = []
    seen: set[tuple[str, str, str]] = set()
    for child in children:
        if child.node.identity in seen:
            continue
        seen.add(child.node.identity)
        rule = usage_rule(child.node.lane or lane, child.node.ref.kind)
        attribution = ChildAttribution(
            ref=child.node.ref, tokens=rule.tokens, cost=rule.cost, usage=child.usage
        )
        if rule.tokens == "separate_mission":
            separate.append(attribution)
            continue
        attributed.append(attribution)
        for name in dimensions:
            inclusion = rule.cost if name == "cost_micros" else rule.tokens
            if inclusion == "separate_execution":
                child_dimension = (
                    child.usage.dimensions.get(name) if child.usage is not None else None
                )
                totals[name] = _add(totals.get(name, _unknown()), child_dimension or _unknown())
            elif inclusion == "unattributable":
                coverage[name] = "excludes_subordinates"
    total = UsageReport(dimensions=totals) if parent is not None or attributed else None
    return UsageAggregate(
        total=total, coverage=coverage, children=tuple(attributed), separate=tuple(separate)
    )


__all__ = [
    "ChildAttribution",
    "ChildUsage",
    "SubordinateNode",
    "UsageAggregate",
    "UsageRule",
    "agent_server_child",
    "aggregate_usage",
    "folds_subordinate_usage",
    "harness_execution_ref",
    "linked_mission",
    "provider_subordinates",
    "usage_rule",
]
