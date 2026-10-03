"""Verify an operator-accepted checkpoint against `CON-CP-CHECKPOINT-LINEAGE-V1`.

`accept_descendant` may only name a stamped root-namespace descendant of the unit
generation's expected source. This reads the registered checkpointer by qualified key
(`aget_tuple`, REQ-CP-RUN-011 historical-read rule): it never builds or invokes an agent.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from langgraph.checkpoint.base import BaseCheckpointSaver

from mission_control.adapters.deep_agents.checkpoint_reads import (
    MAX_LINEAGE_WALK,
    checkpoint_parent_id,
    root_checkpoint_config,
)
from mission_control.domain.execution.checkpoint_lineage import (
    STAMP_EXECUTION_GENERATION,
    STAMP_INVOCATION_ID,
    STAMP_UNIT_KEY,
    UnitReconciliationIncident,
    submission_invocation_id,
)
from mission_control.domain.graph_runtime.identities import QualifiedCheckpointKey


class LangGraphCheckpointDescendantVerifier:
    def __init__(self, checkpointers: Mapping[str, BaseCheckpointSaver[Any]]) -> None:
        self._checkpointers = checkpointers

    async def is_stamped_descendant(
        self, incident: UnitReconciliationIncident, key: QualifiedCheckpointKey
    ) -> bool:
        checkpointer = self._checkpointers.get(key.checkpointer_ref_digest)
        if (
            checkpointer is None
            or not key.is_root
            or incident.namespace is None
            or key.thread_id != incident.namespace
        ):
            return False
        stamps: dict[str, object] = {
            STAMP_UNIT_KEY: incident.unit_key,
            STAMP_EXECUTION_GENERATION: incident.execution_generation,
            STAMP_INVOCATION_ID: submission_invocation_id(
                incident.unit_key, incident.execution_generation
            ),
        }
        source = incident.expected_source
        stop_at = source.checkpoint_id if source is not None else None
        cursor: str | None = key.checkpoint_id
        first = True
        for _ in range(MAX_LINEAGE_WALK):
            if cursor == stop_at:
                return not first  # the source itself is not a descendant
            if cursor is None:
                return False
            item = await checkpointer.aget_tuple(root_checkpoint_config(key.thread_id, cursor))
            if item is None or any(
                item.metadata.get(name) != value for name, value in stamps.items()
            ):
                return False
            parent = checkpoint_parent_id(item)
            if first and parent != key.parent_checkpoint_id:
                return False
            first = False
            cursor = parent
        return False
