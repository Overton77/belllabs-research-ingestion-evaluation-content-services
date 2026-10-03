"""Historical checkpoint reads for inspection (REQ-CP-RUN-011, `CON-CP-CHECKPOINT-LINEAGE-V1`).

The registered saver is read by qualified key only (`alist` over a namespace's root
checkpoints, `aget_tuple` for one checkpoint, and the saver's own delta-channel history
walk). It never builds or invokes an agent and never writes. Channel values are reduced to
the allowlisted redacted facts inside this adapter, so no checkpoint body, message, tool
argument, or transcript leaves it.

LangGraph 1.2 stores `DeltaChannel` state (for example `messages`) as ancestor writes
rather than as a checkpoint value. For those channels the adapter asks the saver for the
channel's delta history (`aget_delta_channel_history`) and folds it into counts only:
messages by identity (an `add_messages`-equivalent fold: same ID replaces, removal
deletes), mappings by key. No reducer, graph, or agent is constructed.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.base import BaseCheckpointSaver, CheckpointTuple

from mission_control.adapters.deep_agents.checkpoint_reads import (
    checkpoint_parent_id,
    root_checkpoint_config,
)
from mission_control.application.execution.inspection import RuntimeSourceUnavailable
from mission_control.domain.execution.checkpoint_lineage import ROOT_CHECKPOINT_NS
from mission_control.domain.graph_runtime.identities import QualifiedCheckpointKey
from mission_control.domain.policies.inspection import (
    CheckpointObservation,
    RedactedStateFacts,
    summarize_channel_values,
)

STAMP_PREFIX = "belllabs_"
_EXPOSED_METADATA = frozenset({"step", "source"})
# One namespace's root checkpoints are loaded per history read. A unit generation writes
# a handful of root checkpoints per model/tool step, and a GoalDirected session namespace
# accumulates its ordered units until a rollover; this bound is far above both. A
# namespace above it is reported unavailable (`namespace_history_exceeds_bound`) instead of
# being truncated, because a truncated list would silently break the recorded lineage.
MAX_NAMESPACE_CHECKPOINTS = 2_000
_TRIGGER_PREFIX = "branch:to:"
_TASKS_CHANNEL = "__pregel_tasks"
_REMOVE_ALL_MESSAGES = "__remove_all__"


class LangGraphCheckpointHistoryReader:
    def __init__(
        self,
        checkpointers: Mapping[str, BaseCheckpointSaver[Any]],
        *,
        max_checkpoints: int = MAX_NAMESPACE_CHECKPOINTS,
    ) -> None:
        if max_checkpoints < 1:
            raise ValueError("the namespace checkpoint bound must be positive")
        self._checkpointers = checkpointers
        self._max_checkpoints = max_checkpoints

    def supports(self, checkpointer_ref_digest: str) -> bool:
        return checkpointer_ref_digest in self._checkpointers

    async def list_root_checkpoints(
        self, checkpointer_ref_digest: str, namespace: str
    ) -> tuple[CheckpointObservation, ...]:
        saver = self._saver(checkpointer_ref_digest)
        config: RunnableConfig = {
            "configurable": {"thread_id": namespace, "checkpoint_ns": ROOT_CHECKPOINT_NS}
        }
        observations: list[CheckpointObservation] = []
        try:
            async for item in saver.alist(config, limit=self._max_checkpoints + 1):
                if item.config["configurable"].get("checkpoint_ns", "") != ROOT_CHECKPOINT_NS:
                    continue  # nested namespaces are evidence only, never root lineage
                observations.append(_observation(checkpointer_ref_digest, namespace, item))
        except Exception as error:  # noqa: BLE001 - a saver outage degrades the section
            raise RuntimeSourceUnavailable("the registered checkpointer is unavailable") from error
        if len(observations) > self._max_checkpoints:
            raise RuntimeSourceUnavailable(
                "the namespace holds more root checkpoints than the inspection bound",
                reason="namespace_history_exceeds_bound",
            )
        return tuple(observations)

    async def read_redacted_state(
        self, key: QualifiedCheckpointKey
    ) -> tuple[CheckpointObservation, RedactedStateFacts] | None:
        if not key.is_root:
            return None
        saver = self._saver(key.checkpointer_ref_digest)
        try:
            item = await saver.aget_tuple(root_checkpoint_config(key.thread_id, key.checkpoint_id))
        except Exception as error:  # noqa: BLE001
            raise RuntimeSourceUnavailable("the registered checkpointer is unavailable") from error
        if item is None:
            return None
        observation = _observation(key.checkpointer_ref_digest, key.thread_id, item)
        if observation.key.parent_checkpoint_id != key.parent_checkpoint_id:
            return None
        stored = item.checkpoint.get("channel_values") or {}
        values: dict[str, Any] = {
            name: value for name, value in stored.items() if not _internal(name)
        }
        delta_channels = [
            name
            for name in item.checkpoint.get("channel_versions") or {}
            if not _internal(name) and name not in values
        ]
        if delta_channels:
            try:
                histories = await saver.aget_delta_channel_history(
                    config=root_checkpoint_config(key.thread_id, key.checkpoint_id),
                    channels=delta_channels,
                )
            except Exception as error:  # noqa: BLE001
                raise RuntimeSourceUnavailable(
                    "the delta-channel history is unavailable"
                ) from error
            for name, history in histories.items():
                folded = _fold_delta(history)
                if folded is not None:
                    values[name] = folded
        facts = summarize_channel_values(values)
        withheld = facts.withheld_value_count + sum(1 for name in stored if _internal(name))
        return observation, facts.model_copy(update={"withheld_value_count": withheld})

    def _saver(self, digest: str) -> BaseCheckpointSaver[Any]:
        saver = self._checkpointers.get(digest)
        if saver is None:
            raise RuntimeSourceUnavailable("the checkpointer is not registered")
        return saver


def _observation(digest: str, namespace: str, item: CheckpointTuple) -> CheckpointObservation:
    metadata = dict(item.metadata or {})
    stamps = {
        name: value
        for name, value in metadata.items()
        if name.startswith(STAMP_PREFIX) and isinstance(value, str | int)
    }
    step = metadata.get("step")
    source = metadata.get("source")
    checkpoint = item.checkpoint
    created_at = checkpoint.get("ts")
    return CheckpointObservation(
        key=QualifiedCheckpointKey(
            checkpointer_ref_digest=digest,
            thread_id=namespace,
            checkpoint_ns=ROOT_CHECKPOINT_NS,
            checkpoint_id=str(item.config["configurable"]["checkpoint_id"]),
            parent_checkpoint_id=checkpoint_parent_id(item),
        ),
        step=step if isinstance(step, int) else None,
        source=source if isinstance(source, str) else None,
        created_at=str(created_at)[:64] if created_at is not None else None,
        stamps=stamps,
        pending_task_names=_pending_task_names(checkpoint),
        withheld_metadata_fields=sum(
            1 for name in metadata if name not in _EXPOSED_METADATA and name not in stamps
        ),
    )


def _internal(channel: object) -> bool:
    """LangGraph control channels (triggers, tasks, input); never state channels."""

    return not isinstance(channel, str) or channel.startswith((_TRIGGER_PREFIX, "__"))


def _pending_task_names(checkpoint: Mapping[str, Any]) -> tuple[str, ...]:
    """Next tasks without the graph: available triggers not yet seen, plus pending sends.

    A node is pending when its `branch:to:<node>` trigger holds a value (consumed triggers
    are empty) at a version the node has not seen. `Send` packets in the task channel name
    their target node; their arguments (tool calls) are never read.
    """

    versions = checkpoint.get("channel_versions") or {}
    values = checkpoint.get("channel_values") or {}
    seen = checkpoint.get("versions_seen") or {}
    names: set[str] = set()
    for channel, version in versions.items():
        if not isinstance(channel, str) or not channel.startswith(_TRIGGER_PREFIX):
            continue
        if channel not in values:
            continue
        node = channel.removeprefix(_TRIGGER_PREFIX)
        observed = (seen.get(node) or {}).get(channel)
        if observed is None or _newer(version, observed):
            names.add(node)
    tasks = values.get(_TASKS_CHANNEL)
    if isinstance(tasks, list | tuple):
        for packet in tasks:
            target = getattr(packet, "node", None)
            if isinstance(target, str):
                names.add(target)
    return tuple(sorted(names))


def _fold_delta(history: Mapping[str, Any]) -> list[str] | dict[str, None] | None:
    """Fold a delta channel's seed and writes into identity placeholders (counts only)."""

    seed = history.get("seed")
    seed = getattr(seed, "value", seed)  # `_DeltaSnapshot(value)` or a plain value
    writes = [write[2] for write in history.get("writes", ())]
    if isinstance(seed, Mapping) or (seed is None and writes and isinstance(writes[0], Mapping)):
        keys: dict[str, None] = {str(name): None for name in (seed or {})}
        for update in writes:
            if isinstance(update, Mapping):
                for name, value in update.items():
                    if value is None:
                        keys.pop(str(name), None)
                    else:
                        keys[str(name)] = None
        return keys
    if seed is None and not writes:
        return None
    identities: dict[str, None] = {}
    anonymous = 0

    def apply(message: Any) -> None:
        nonlocal anonymous
        identity = getattr(message, "id", None)
        if identity is None and isinstance(message, Mapping):
            identity = message.get("id")
        if getattr(message, "type", None) == "remove":
            if identity == _REMOVE_ALL_MESSAGES:
                identities.clear()
            else:
                identities.pop(str(identity), None)
            return
        if identity is None:
            anonymous += 1
            identity = f"anonymous:{anonymous}"
        identities[str(identity)] = None

    for message in seed if isinstance(seed, list | tuple) else ():
        apply(message)
    for update in writes:
        for message in update if isinstance(update, list | tuple) else (update,):
            apply(message)
    return list(identities)


def _newer(version: Any, observed: Any) -> bool:
    try:
        return bool(version > observed)
    except TypeError:
        return str(version) > str(observed)
