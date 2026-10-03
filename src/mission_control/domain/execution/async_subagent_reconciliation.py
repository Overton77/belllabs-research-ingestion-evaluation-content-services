"""Pure reconciliation rules for async subagents (REQ-CP-DA-008, REQ-CP-DA-019, REQ-CP-EXEC-016).

AMD-RRM-001 made the `in_doubt` lifecycle value, its observation exit and the two operator
decisions (`adopt_provider_run`, `orphan_child`) normative. This module holds the decisions as
data and the classification rules as functions; it knows nothing about Deep Agents, the Agent
Protocol SDK, PostgreSQL or any company fixture.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Final, Literal, Protocol
from uuid import NAMESPACE_URL, uuid5

from pydantic import AwareDatetime, Field, model_validator

from mission_control.domain.authoring.canonical import sha256_digest
from mission_control.domain.execution.contracts import (
    ACTIVE_ASYNC_SUBAGENT_LIFECYCLES,
    DIGEST_PATTERN,
    AsyncSubagentContract,
    AsyncSubagentInDoubtReason,
    AsyncSubagentLifecycle,
    AsyncSubagentUsage,
    Contract,
)

ASYNC_SUBAGENT_INCIDENT_TYPE: Final = "async_submission_in_doubt"
ASYNC_SUBAGENT_INCIDENT_SCHEMA_VERSION: Final = "belllabs.async-subagent-incident.v1"
ASYNC_PROVIDER_RUN_SCHEMA_VERSION: Final = "belllabs.async-provider-run.v1"

AsyncSubagentReconciliationDecision = Literal["adopt_provider_run", "orphan_child"]
# The privileged permission of the typed operator decisions (sibling of
# `workflow_run.reconcile_unit`, CON-CP-LIFECYCLE-V1); RRM-007 owns the governed route.
ASYNC_CHILD_RECONCILE_PERMISSION: Final = "workflow_run.reconcile_async_child"
TERMINAL_PROVIDER_RUN_STATUSES: Final = frozenset(
    {"success", "error", "timeout", "interrupted", "cancelled"}
)
CANCELLED_RUN_DISPOSITIONS: Final = frozenset({"duplicate_cancelled", "orphaned_cancelled"})


class AsyncServedGraphIdentity(Contract):
    """The graph identity an Agent Server reports for a hosted async subagent graph."""

    graph_id: str = Field(min_length=1)
    graph_revision: str = Field(min_length=1, max_length=256)
    graph_binding_digest: str = Field(pattern=DIGEST_PATTERN)
    deepagents_version: str = Field(min_length=1, max_length=64)

    def matches(self, contract: AsyncSubagentContract) -> bool:
        return (
            self.graph_id == contract.graph_id
            and self.graph_revision == contract.graph_revision
            and self.graph_binding_digest == contract.graph_binding_digest
        )


class AsyncProviderRunObservation(Contract):
    """One provider run that carries a child's spawn key, as observed (never authority)."""

    run_id: str = Field(min_length=1)
    status: str = Field(min_length=1)
    served_graph: AsyncServedGraphIdentity | None = None
    usage: AsyncSubagentUsage | None = None


class AsyncSpawnKeyObservation(Contract):
    """Every provider run carrying the BellLabs spawn key of one child, at one instant."""

    thread_id: str = Field(min_length=1)
    runs: tuple[AsyncProviderRunObservation, ...] = ()
    observed_at: AwareDatetime


class AsyncProviderRunRecord(Contract):
    """Durable record of a provider run observed for a child, with its usage disposition.

    Duplicate and orphaned runs are kept with their usage pending so nothing is dropped
    (REQ-CP-DA-011, REQ-CP-RUN-009).
    """

    schema_version: Literal["belllabs.async-provider-run.v1"] = ASYNC_PROVIDER_RUN_SCHEMA_VERSION
    child_execution_id: str = Field(min_length=1)
    provider_thread_id: str = Field(min_length=1)
    provider_run_id: str = Field(min_length=1)
    # `cancel_ambiguous`: BellLabs asked the provider to cancel the run but observed no
    # terminal status; the run stays a candidate until one is observed (RRM-013 review N2).
    disposition: Literal["bound", "duplicate_cancelled", "orphaned_cancelled", "cancel_ambiguous"]
    provider_status: str = Field(min_length=1)
    usage: AsyncSubagentUsage
    observed_at: AwareDatetime


class AsyncSubagentIncident(Contract):
    """Typed `in_doubt` incident of one async child (REQ-CP-DA-008).

    It records why the child's provider binding is ambiguous and the provider runs an operator
    may adopt. It holds identities only, never thread content or secrets. It is stored beside
    the runtime-unit incidents of RRM-004 (`runtime_reconciliation_incidents`) under its own
    incident type, and is resolved by observation or by exactly one typed decision.
    """

    schema_version: Literal["belllabs.async-subagent-incident.v1"] = (
        ASYNC_SUBAGENT_INCIDENT_SCHEMA_VERSION
    )
    incident_id: str = Field(min_length=1, max_length=256)
    request_scope: str = Field(min_length=1)
    parent_run_id: str = Field(min_length=1)
    parent_binding_id: str = Field(min_length=1)
    child_execution_id: str = Field(min_length=1)
    revision: int = Field(default=1, ge=1)
    reason: AsyncSubagentInDoubtReason
    provider_thread_id: str = Field(min_length=1)
    candidate_run_ids: tuple[str, ...] = Field(default=(), max_length=64)
    status: Literal["operator_required", "resolved"] = "operator_required"
    resolution: Literal["observation", "adopt_provider_run", "orphan_child"] | None = None
    decision_id: str | None = Field(default=None, min_length=1, max_length=256)
    adopted_run_id: str | None = Field(default=None, min_length=1)
    recorded_at: AwareDatetime

    @model_validator(mode="after")
    def incident_shape(self) -> AsyncSubagentIncident:
        if self.incident_id != async_subagent_incident_id(
            self.request_scope, self.child_execution_id, self.revision
        ):
            raise ValueError("incident id is not derived from its child revision")
        if (self.status == "resolved") != (self.resolution is not None):
            raise ValueError("exactly a resolved incident carries its resolution")
        if (self.resolution == "adopt_provider_run") != (self.adopted_run_id is not None):
            raise ValueError("only adopt_provider_run names an adopted run")
        if (self.resolution in {"adopt_provider_run", "orphan_child"}) != (
            self.decision_id is not None
        ):
            raise ValueError("exactly an operator decision carries its command identity")
        return self

    @property
    def identity_digest(self) -> str:
        return sha256_digest(
            {
                "request_scope": self.request_scope,
                "child_execution_id": self.child_execution_id,
                "revision": self.revision,
            }
        )


def async_subagent_incident_id(request_scope: str, child_execution_id: str, revision: int) -> str:
    return str(
        uuid5(NAMESPACE_URL, f"async-incident:{request_scope}:{child_execution_id}:{revision}")
    )


class SpawnKeyClassification(Contract):
    """The REQ-CP-DA-008 observation rule applied to one spawn-key observation."""

    outcome: Literal["bound", "no_provider_run", "in_doubt"]
    run: AsyncProviderRunObservation | None = None
    reason: AsyncSubagentInDoubtReason | None = None

    @model_validator(mode="after")
    def outcome_shape(self) -> SpawnKeyClassification:
        if (self.outcome == "bound") != (self.run is not None):
            raise ValueError("exactly a bound classification names the provider run")
        if (self.outcome == "in_doubt") != (self.reason is not None):
            raise ValueError("exactly an in_doubt classification carries its reason")
        return self


def classify_spawn_key_observation(
    contract: AsyncSubagentContract,
    observation: AsyncSpawnKeyObservation,
    *,
    resolved_run_ids: frozenset[str] = frozenset(),
) -> SpawnKeyClassification:
    """Exactly one run whose served graph identity verifies binds; none orphans; else in_doubt.

    A run without a reported served identity (for example one that has not started) is treated
    as verifiable only when it is the single candidate; its identity is re-verified when its
    checkpoint is observed (REQ-CP-DA-011). A run whose reported identity differs from the
    contract is a `graph_identity_mismatch`. `resolved_run_ids` are runs BellLabs already
    cancelled through `adopt_provider_run` or `orphan_child`; they keep the spawn key on the
    provider but are no longer candidates.
    """

    candidates = tuple(run for run in observation.runs if run.run_id not in resolved_run_ids)
    if not candidates:
        return SpawnKeyClassification(outcome="no_provider_run")
    if len(candidates) > 1:
        return SpawnKeyClassification(outcome="in_doubt", reason="multiple_provider_runs")
    run = candidates[0]
    if run.served_graph is not None and not run.served_graph.matches(contract):
        return SpawnKeyClassification(outcome="in_doubt", reason="graph_identity_mismatch")
    return SpawnKeyClassification(outcome="bound", run=run)


def lifecycle_for_provider_status(status: str) -> AsyncSubagentLifecycle:
    """Map an Agent Protocol run status onto the child lifecycle (observation only)."""

    return {
        "pending": AsyncSubagentLifecycle.SUBMITTED,
        "running": AsyncSubagentLifecycle.RUNNING,
        "interrupted": AsyncSubagentLifecycle.WAITING,
        "success": AsyncSubagentLifecycle.COMPLETED,
        "error": AsyncSubagentLifecycle.FAILED,
        "timeout": AsyncSubagentLifecycle.FAILED,
    }.get(status, AsyncSubagentLifecycle.RUNNING)


class AsyncChildForkClassification(Contract):
    """REQ-CP-EXEC-016 / RRM-001 section 8 #6: active async children make a snapshot unsafe."""

    admission: Literal["quiescent", "prohibited"]
    reason: Literal["snapshot_not_quiescent"] | None = None
    active_child_execution_ids: tuple[str, ...] = ()

    @model_validator(mode="after")
    def prohibited_names_children(self) -> AsyncChildForkClassification:
        if (self.admission == "prohibited") != bool(self.active_child_execution_ids):
            raise ValueError("a prohibited fork names the active children that block it")
        if (self.admission == "prohibited") != (self.reason is not None):
            raise ValueError("exactly a prohibited fork carries its reason")
        return self


class AsyncChildLifecycleSubject(Protocol):
    """Any record of one child's identity and lifecycle (detail document or authority view)."""

    @property
    def child_execution_id(self) -> str: ...

    @property
    def lifecycle(self) -> AsyncSubagentLifecycle: ...


def classify_async_children_for_fork(
    children: Sequence[AsyncChildLifecycleSubject],
) -> AsyncChildForkClassification:
    """A fork is admissible only when no child is admitted, submitted, running, waiting or in doubt.

    Children are never copied implicitly: a quiescent classification says only that the
    snapshot is safe, and a prohibited one names the children the operator must settle first.
    """

    active = tuple(
        sorted(
            child.child_execution_id
            for child in children
            if child.lifecycle in ACTIVE_ASYNC_SUBAGENT_LIFECYCLES
        )
    )
    if active:
        return AsyncChildForkClassification(
            admission="prohibited",
            reason="snapshot_not_quiescent",
            active_child_execution_ids=active,
        )
    return AsyncChildForkClassification(admission="quiescent")


def aggregate_child_usage(
    records: tuple[AsyncProviderRunRecord, ...],
    budget_limits: Mapping[str, int],
    *,
    manifest_usage: AsyncSubagentUsage | None = None,
) -> tuple[dict[str, int], dict[str, int]]:
    """Attributed and pending amounts of one child across every provider run it had.

    REQ-CP-RUN-009 / REQ-CP-DA-008 (RRM-013 review B1): a run whose usage the provider did
    not attribute (a cancelled duplicate, an orphaned or failed run, a cancel whose outcome is
    unknown) makes the pending amounts non-empty. Every budget dimension such a run does not
    report (neither attributed nor pending) is unknown, and the child's budget limit is its
    pending ceiling (one ceiling per dimension shared by every such run), so the parent's
    effect stays unsettled until the usage is reconciled (re-review G1: a partly reported
    run, e.g. `model.turns` counted but `tokens.total` unstamped, never drops the unreported
    dimension). Provider-attributed amounts are facts and are never capped, even above the
    child's reservation. The completed run's manifest usage replaces its record's usage.
    """

    attributed: dict[str, int] = {}
    pending: dict[str, int] = {}
    unknown_dimensions: set[str] = set()
    for record in records:
        usage = record.usage
        if manifest_usage is not None and manifest_usage.provider_run_id == record.provider_run_id:
            usage = manifest_usage
        if usage.attribution == "provider_attributed":
            # Provider-attributed facts are never capped, even beyond the reservation.
            for dimension, amount in usage.attributed_amounts.items():
                if dimension in budget_limits:
                    attributed[dimension] = attributed.get(dimension, 0) + amount
            continue
        reported = set(usage.attributed_amounts) | set(usage.pending_amounts)
        for dimension, amount in usage.pending_amounts.items():
            if dimension in budget_limits:
                pending[dimension] = pending.get(dimension, 0) + amount
        unknown_dimensions |= set(budget_limits) - reported
    for dimension in unknown_dimensions:
        # Runs of unknown usage share one ceiling: the child's budget limit per dimension.
        pending[dimension] = max(pending.get(dimension, 0), budget_limits[dimension])
    return attributed, {dimension: amount for dimension, amount in pending.items() if amount > 0}
