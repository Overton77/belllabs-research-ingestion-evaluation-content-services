"""Shared read-side helpers for the registered LangGraph checkpointer (RRM-003/004).

Both the invocation classifier in `adapter.py` and the operator-accepted descendant
verifier address root-namespace checkpoints by thread and checkpoint id, and walk parents
with the same bound.
"""

from __future__ import annotations

from typing import cast

from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.base import CheckpointTuple

from mission_control.domain.execution.checkpoint_lineage import ROOT_CHECKPOINT_NS

MAX_LINEAGE_WALK = 100_000


def root_checkpoint_config(thread_id: str, checkpoint_id: str) -> RunnableConfig:
    return {
        "configurable": {
            "thread_id": thread_id,
            "checkpoint_ns": ROOT_CHECKPOINT_NS,
            "checkpoint_id": checkpoint_id,
        }
    }


def checkpoint_parent_id(item: CheckpointTuple) -> str | None:
    if item.parent_config is None:
        return None
    return cast(str | None, item.parent_config["configurable"].get("checkpoint_id"))
