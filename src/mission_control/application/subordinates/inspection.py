"""Read model of an async child's lineage for inspection (REQ-CP-RUN-011; RRM-005 consumer).

Served from PostgreSQL authority rows only (the 0016 authority row with its 0021 lifecycle
mirror, the provider-run records and the incident), never from the provider. Reading never
mutates lifecycle, settles, reconciles or writes observations.
"""

from __future__ import annotations

from typing import Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

from mission_control.domain.execution.async_subagent_reconciliation import AsyncProviderRunRecord
from mission_control.domain.execution.contracts import (
    AsyncSubagentInDoubtReason,
    AsyncSubagentLifecycle,
)


class AsyncChildLineageView(BaseModel):
    """One child as inspection shows it: BellLabs identity, provider binding, graph identity."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    request_scope: str
    child_execution_id: str
    parent_run_id: str
    parent_operation_id: str
    parent_binding_id: str | None = None
    contract_id: str
    contract_digest: str
    graph_id: str | None = None
    graph_revision: str | None = None
    graph_binding_digest: str | None = None
    lifecycle: AsyncSubagentLifecycle
    provider_thread_id: str | None = None
    provider_run_id: str | None = None
    submission_fence: int = Field(ge=0)
    submission_holder: str | None = None
    in_doubt_reason: AsyncSubagentInDoubtReason | None = None
    incident_id: str | None = None
    reconciliation_decision: Literal["adopt_provider_run", "orphan_child"] | None = None
    result_decision: Literal["admit", "conditionally_admit", "reject", "defer"] | None = None
    settlement_ref: str | None = None
    provider_runs: tuple[AsyncProviderRunRecord, ...] = ()
    updated_at: AwareDatetime
