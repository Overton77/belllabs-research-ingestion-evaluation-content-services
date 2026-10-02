"""Scoped, non-mutating runtime inspection (REQ-CP-RUN-011/012, REQ-CP-EXEC-015).

`RuntimeInspectionService` joins four qualified sources behind narrow read ports:

* `InspectionReadRepository`: PostgreSQL authority (run projection, budget, effects,
  runtime units, attempts, transitions, results, incidents, journal, async-child
  authority), read as one consistent, read-only snapshot. It is the primary source;
  every response is served from it even when every other source is down.
* `TemporalVisibilityReader`: Temporal Visibility through Search Attributes only.
* `CheckpointHistoryReader`: the registered checkpointer through `aget_tuple`/`alist`
  by qualified key; it never builds or invokes an agent.
* `AsyncChildDetailReader`: the immutable async-child detail document.

No method writes, settles, reconciles, claims, opens an incident, or records an
observation. Privileged reconciliation stays the separate `reconcile_unit` command.
Freshness, source observation times, and the redaction applied are reported per section
(`CON-CP-INSPECTION-READ-V1`). The service is provider- and company-neutral.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Final, Protocol

from app.application.operations.checkpoint_lineage import (
    InMemoryCheckpointLineageRepository,
    NamespaceRecord,
    UnitGenerationRecord,
)
from app.application.run_control.inspection_cursor import (
    Clock,
    InspectionCursorCodec,
)
from app.application.run_control.run_control_repository import InMemoryRunControlRepository
from app.domain.control_plane.canonical import sha256_digest
from app.domain.graph_runtime.identities import QualifiedCheckpointKey, RuntimeUnitIdentity
from app.domain.operation_execution.checkpoint_lineage import (
    STAMP_BINDING_DIGEST,
    STAMP_EXECUTION_GENERATION,
    STAMP_INVOCATION_ID,
    STAMP_STATE_SCHEMA_DIGEST,
    STAMP_UNIT_KEY,
    ActivityAttemptObservation,
    CheckpointTransitionObservation,
    LineageWriteRejection,
    UnitReconciliationIncident,
    UnitResultObservation,
    submission_invocation_id,
)
from app.domain.run_control.contracts import (
    BoundaryCommandStatus,
    BudgetState,
    EffectDisposition,
    EffectLedgerState,
    RunPhase,
    RunProjection,
    UnitReconciliationDecision,
    WaitCondition,
)
from app.domain.run_control.errors import RunControlNotFound
from app.domain.run_control.inspection import (
    DEFAULT_PAGE_SIZE,
    MAX_PAGE_SIZE,
    AsyncChildInspection,
    BudgetSummary,
    CheckpointHistoryEntry,
    CheckpointHistoryPage,
    CheckpointNotInUnitLineage,
    CheckpointObservation,
    CheckpointRole,
    EffectStatus,
    Freshness,
    GenerationInspection,
    IncompatibleCheckpoint,
    InspectionNotFound,
    InspectionRead,
    InspectionSection,
    InspectionSourceUnavailable,
    InvalidInspectionCursor,
    JournalClaimInspection,
    ReconciliationState,
    RedactedCheckpointStateSummary,
    RedactedStateFacts,
    RedactionMarker,
    RunInspection,
    RunListItem,
    RunListPage,
    SectionSource,
    TemporalExecution,
    UnitInspection,
    UnitStatus,
    UnitSummary,
    checkpoint_summary_digest,
    lease_state,
    worst_reconciliation_state,
)

OPERATOR_RECONCILIATION_PREFIX: Final = "operator-reconciliation:"


# --- Persisted authority records ---------------------------------------------------------


@dataclass(frozen=True)
class RunRecord:
    projection: RunProjection
    updated_at: datetime


@dataclass(frozen=True)
class UnitRecord:
    identity: RuntimeUnitIdentity
    generations: tuple[UnitGenerationRecord, ...] = ()
    attempts: tuple[ActivityAttemptObservation, ...] = ()
    transitions: tuple[CheckpointTransitionObservation, ...] = ()
    results: tuple[UnitResultObservation, ...] = ()
    incidents: tuple[UnitReconciliationIncident, ...] = ()
    rejections: tuple[LineageWriteRejection, ...] = ()
    namespaces: tuple[NamespaceRecord, ...] = ()
    journal: tuple[JournalClaimInspection, ...] = ()


@dataclass(frozen=True)
class RunSnapshot:
    """One consistent, read-only snapshot of a run's persisted authority."""

    run: RunRecord
    observed_at: datetime
    budget: BudgetState | None = None
    effects: EffectLedgerState | None = None
    units: tuple[UnitRecord, ...] = ()
    async_children: tuple[AsyncChildInspection, ...] = ()
    # RRM-007: the run's boundary commands and receipts, read from the same authority.
    boundary_commands: tuple[BoundaryCommandStatus, ...] = ()


class InspectionReadRepository(Protocol):
    async def list_runs(
        self,
        request_scope: str,
        *,
        phases: frozenset[RunPhase],
        after_run_id: str | None,
        limit: int,
    ) -> tuple[tuple[RunRecord, ...], datetime]: ...

    async def read_run(
        self, request_scope: str, run_id: str, *, unit_key: str | None = None
    ) -> RunSnapshot | None: ...


# --- Qualified runtime sources -----------------------------------------------------------


class RuntimeSourceUnavailable(RuntimeError):
    """A qualified runtime source could not be read; reads degrade, they never fail.

    `reason` is the typed section reason reported to the caller (never a provider message).
    """

    def __init__(self, message: str, *, reason: str = "source_unavailable") -> None:
        super().__init__(message)
        self.reason = reason


class CheckpointLineageCycle(RuntimeSourceUnavailable):
    """The checkpointer's parent links do not form a finite chain; never walked forever."""

    def __init__(self, message: str) -> None:
        super().__init__(message, reason="checkpoint_lineage_cycle")


class TemporalVisibilityReader(Protocol):
    async def list_run_executions(
        self, request_scope: str, run_id: str
    ) -> tuple[TemporalExecution, ...]: ...


class CheckpointHistoryReader(Protocol):
    def supports(self, checkpointer_ref_digest: str) -> bool: ...

    async def list_root_checkpoints(
        self, checkpointer_ref_digest: str, namespace: str
    ) -> tuple[CheckpointObservation, ...]: ...

    async def read_redacted_state(
        self, key: QualifiedCheckpointKey
    ) -> tuple[CheckpointObservation, RedactedStateFacts] | None: ...


class AsyncChildDetailReader(Protocol):
    async def get_execution(self, request_scope: str, child_execution_id: str) -> Any: ...


def _page_size(limit: int | None) -> int:
    if limit is None:
        return DEFAULT_PAGE_SIZE
    return min(max(limit, 1), MAX_PAGE_SIZE)


# --- Service -----------------------------------------------------------------------------


@dataclass
class _Sections:
    observed_at: datetime
    projection_version: int | None = None
    items: dict[str, InspectionSection] = field(default_factory=dict)

    def authority(
        self,
        name: str,
        *,
        reconciliation_state: ReconciliationState = "none",
        withheld: int = 0,
    ) -> None:
        self.items[name] = InspectionSection(
            source="postgres_authority",
            observed_at=self.observed_at,
            projection_version=self.projection_version,
            freshness="current",
            reconciliation_state=reconciliation_state,
            redaction=RedactionMarker(withheld_field_count=withheld),
        )

    def runtime(
        self,
        name: str,
        source: SectionSource,
        freshness: Freshness,
        *,
        observed_at: datetime | None = None,
        reason: str | None = None,
        withheld: int = 0,
    ) -> None:
        self.items[name] = InspectionSection(
            source=source,
            observed_at=observed_at,
            freshness=freshness,
            reason=reason,
            redaction=RedactionMarker(withheld_field_count=withheld),
        )


class RuntimeInspectionService:
    def __init__(
        self,
        repository: InspectionReadRepository,
        *,
        cursors: InspectionCursorCodec | None = None,
        visibility: TemporalVisibilityReader | None = None,
        checkpoints: CheckpointHistoryReader | None = None,
        async_details: AsyncChildDetailReader | None = None,
        clock: Clock = lambda: datetime.now(UTC),
    ) -> None:
        self._repository = repository
        self._cursors = cursors or InspectionCursorCodec(clock=clock)
        self._visibility = visibility
        self._checkpoints = checkpoints
        self._async_details = async_details
        self._clock = clock

    # -- run list --

    async def list_runs(
        self,
        request_scope: str,
        *,
        phases: frozenset[RunPhase] = frozenset(),
        limit: int | None = None,
        cursor: str | None = None,
    ) -> InspectionRead[RunListPage]:
        size = _page_size(limit)
        filter_digest = sha256_digest({"phases": sorted(phase.value for phase in phases)})
        after: str | None = None
        if cursor is not None:
            position = self._cursors.decode(
                cursor, kind="runs", request_scope=request_scope, filter_digest=filter_digest
            )
            if len(position) != 1 or not isinstance(position[0], str):
                raise InvalidInspectionCursor("inspection cursor position is malformed")
            after = position[0]
        records, observed_at = await self._repository.list_runs(
            request_scope, phases=phases, after_run_id=after, limit=size + 1
        )
        page = records[:size]
        next_cursor = (
            self._cursors.encode(
                kind="runs",
                request_scope=request_scope,
                filter_digest=filter_digest,
                position=[page[-1].projection.run_id],
            )
            if len(records) > size
            else None
        )
        items = tuple(
            RunListItem(
                run_id=record.projection.run_id,
                request_scope=record.projection.request_scope,
                version=record.projection.version,
                phase=record.projection.phase,
                terminal_outcome=record.projection.terminal_outcome,
                workflow_type_ref=record.projection.workflow_type_ref,
                effective_configuration_digest=record.projection.effective_configuration_digest,
                reconciliation_state=_projection_reconciliation(record.projection),
                updated_at=record.updated_at,
            )
            for record in page
        )
        sections = _Sections(observed_at=observed_at)
        sections.authority(
            "runs",
            reconciliation_state=worst_reconciliation_state(
                [item.reconciliation_state for item in items]
            ),
        )
        return InspectionRead(
            data=RunListPage(items=items, next_cursor=next_cursor), sections=sections.items
        )

    # -- run detail --

    async def get_run(self, request_scope: str, run_id: str) -> InspectionRead[RunInspection]:
        snapshot = await self._snapshot(request_scope, run_id)
        projection = snapshot.run.projection
        sections = _Sections(
            observed_at=snapshot.observed_at, projection_version=projection.version
        )
        units = tuple(self._unit_summary(unit, snapshot) for unit in snapshot.units)
        operator_waits = _operator_waits(projection)
        run_state = worst_reconciliation_state(
            [_projection_reconciliation(projection), *(unit.reconciliation_state for unit in units)]
        )
        effects = _effect_statuses(snapshot.effects)
        sections.authority("run", reconciliation_state=run_state)
        if snapshot.budget is not None:
            sections.authority(
                "budget",
                withheld=len(snapshot.budget.usage_records)
                + len(snapshot.budget.usage_settlements),
            )
        else:
            sections.runtime("budget", "postgres_authority", "unavailable", reason="not_recorded")
        sections.authority(
            "effects",
            reconciliation_state="in_doubt" if any(item.ambiguous for item in effects) else "none",
        )
        sections.authority(
            "units",
            reconciliation_state=worst_reconciliation_state(
                [unit.reconciliation_state for unit in units]
            ),
        )
        children = await self._async_children(snapshot.async_children, request_scope, sections)
        executions = await self._temporal(request_scope, snapshot, sections)
        data = RunInspection(
            projection=projection,
            reconciliation_state=run_state,
            operator_reconciliation_waits=operator_waits,
            boundary_commands=snapshot.boundary_commands,
            output_refs=_output_refs(projection),
            budget=_budget_summary(snapshot.budget),
            effects=effects,
            units=units,
            async_children=children,
            temporal_executions=executions,
        )
        return InspectionRead(data=data, sections=sections.items)

    # -- unit detail --

    async def get_unit(
        self, request_scope: str, run_id: str, unit_key: str
    ) -> InspectionRead[UnitInspection]:
        snapshot, unit = await self._unit_snapshot(request_scope, run_id, unit_key)
        projection = snapshot.run.projection
        sections = _Sections(
            observed_at=snapshot.observed_at, projection_version=projection.version
        )
        generations = tuple(self._generation(unit, record, snapshot) for record in unit.generations)
        binding_ids = {record.binding_id for record in unit.generations}
        effects = tuple(
            item for item in _effect_statuses(snapshot.effects) if item.operation_ref in binding_ids
        )
        status, state = _unit_status(generations)
        decisions = tuple(
            item for item in projection.unit_reconciliations if item.unit_key == unit_key
        )
        waits = tuple(
            wait
            for wait in _operator_waits(projection)
            if wait.condition_id.startswith(f"{OPERATOR_RECONCILIATION_PREFIX}{unit_key}:")
        )
        if waits:
            state = worst_reconciliation_state([state, "operator_required"])
        sections.authority("unit", reconciliation_state=state)
        sections.authority("lineage", reconciliation_state=state)
        # The journal summary omits the settlement detail payload and claim fingerprints.
        sections.authority(
            "journal", withheld=sum(len(claim.settlements) for claim in unit.journal)
        )
        sections.authority(
            "effects",
            reconciliation_state="in_doubt" if any(item.ambiguous for item in effects) else "none",
        )
        sections.authority("reconciliation", reconciliation_state=state)
        # Attribute children by the unit's own exact bindings, never by the semantic
        # operation ID that other units of the run may share (see `_owned_by`).
        run_children = await self._async_children(snapshot.async_children, request_scope, sections)
        children = tuple(child for child in run_children if _owned_by(child, unit.generations))
        executions = tuple(
            execution
            for execution in await self._temporal(request_scope, snapshot, sections)
            if execution.unit_key == unit_key
        )
        data = UnitInspection(
            unit=unit.identity,
            unit_key=unit_key,
            run_phase=projection.phase,
            status=status,
            reconciliation_state=state,
            generations=generations,
            journal=unit.journal,
            effects=effects,
            reconciliation_decisions=decisions,
            operator_reconciliation_waits=waits,
            boundary_commands=tuple(
                item
                for item in snapshot.boundary_commands
                if item.command.kind == "reconcile_unit"
                and item.command.target.target_ref.startswith(f"{unit_key}:gen:")
            ),
            async_children=children,
            temporal_executions=executions,
        )
        return InspectionRead(data=data, sections=sections.items)

    # -- checkpoint history --

    async def get_checkpoint_history(
        self,
        request_scope: str,
        run_id: str,
        unit_key: str,
        *,
        execution_generation: int | None = None,
        limit: int | None = None,
        cursor: str | None = None,
    ) -> InspectionRead[CheckpointHistoryPage]:
        snapshot, unit = await self._unit_snapshot(request_scope, run_id, unit_key)
        generation = _select_generation(unit, execution_generation)
        sections = _Sections(
            observed_at=snapshot.observed_at,
            projection_version=snapshot.run.projection.version,
        )
        sections.authority("lineage")
        recorded = _recorded_keys(unit, generation)
        digest = _checkpointer_digest(recorded)
        namespace = generation.namespace
        page = CheckpointHistoryPage(
            unit_key=unit_key,
            execution_generation=generation.execution_generation,
            namespace=namespace or "",
            checkpointer_ref_digest=digest,
            recorded_keys=tuple(recorded.values()),
        )
        if namespace is None:
            sections.runtime(
                "checkpoints", "checkpointer", "unavailable", reason="unit_has_no_namespace"
            )
            return InspectionRead(data=page, sections=sections.items)
        lineage = await self._lineage_entries(unit, generation, recorded, digest, sections)
        if lineage is None:
            return InspectionRead(data=page, sections=sections.items)
        size = _page_size(limit)
        filter_digest = sha256_digest(
            {"unit_key": unit_key, "generation": generation.execution_generation}
        )
        start = 0
        if cursor is not None:
            position = self._cursors.decode(
                cursor,
                kind="checkpoints",
                request_scope=request_scope,
                filter_digest=filter_digest,
            )
            ids = [entry.key.checkpoint_id for entry in lineage]
            if len(position) != 1 or position[0] not in ids:
                raise InvalidInspectionCursor("inspection cursor position is not on this lineage")
            start = ids.index(str(position[0])) + 1
        entries = tuple(lineage[start : start + size])
        next_cursor = (
            self._cursors.encode(
                kind="checkpoints",
                request_scope=request_scope,
                filter_digest=filter_digest,
                position=[entries[-1].key.checkpoint_id],
            )
            if entries and start + size < len(lineage)
            else None
        )
        return InspectionRead(
            data=page.model_copy(update={"entries": entries, "next_cursor": next_cursor}),
            sections=sections.items,
        )

    async def get_checkpoint_summary(
        self,
        request_scope: str,
        run_id: str,
        unit_key: str,
        checkpoint_id: str,
        *,
        execution_generation: int | None = None,
    ) -> InspectionRead[RedactedCheckpointStateSummary]:
        """Historical read in the REQ-CP-RUN-011 order: ownership, lineage, compatibility."""

        # 1. scope, run, unit, and namespace ownership.
        snapshot, unit = await self._unit_snapshot(request_scope, run_id, unit_key)
        generation = _select_generation(unit, execution_generation)
        if generation.namespace is None:
            raise CheckpointNotInUnitLineage("the unit generation has no cognitive namespace")
        sections = _Sections(
            observed_at=snapshot.observed_at,
            projection_version=snapshot.run.projection.version,
        )
        sections.authority("lineage")
        recorded = _recorded_keys(unit, generation)
        digest = _checkpointer_digest(recorded)
        # 2. the checkpoint lies on the unit generation's recorded lineage.
        lineage = await self._lineage_entries(unit, generation, recorded, digest, sections)
        if lineage is None:
            raise InspectionSourceUnavailable("the unit's checkpointer is unavailable")
        entry = next((item for item in lineage if item.key.checkpoint_id == checkpoint_id), None)
        if entry is None:
            raise CheckpointNotInUnitLineage(
                "the checkpoint is not on the unit generation's recorded lineage"
            )
        # 3. stamped binding and state-schema digests match the unit's binding; never coerced.
        if not (entry.binding_compatible and entry.state_schema_compatible):
            raise IncompatibleCheckpoint(
                "the checkpoint's stamped binding or state-schema digest differs from the unit"
            )
        assert self._checkpoints is not None
        try:
            read = await self._checkpoints.read_redacted_state(entry.key)
        except RuntimeSourceUnavailable as error:
            raise InspectionSourceUnavailable("the unit's checkpointer is unavailable") from error
        if read is None:
            raise CheckpointNotInUnitLineage("the checkpoint is no longer readable")
        observation, facts = read
        stamped = {
            name: value
            for name, value in observation.stamps.items()
            if name
            in {
                STAMP_UNIT_KEY,
                STAMP_EXECUTION_GENERATION,
                STAMP_INVOCATION_ID,
                STAMP_BINDING_DIGEST,
                STAMP_STATE_SCHEMA_DIGEST,
            }
        }
        observed_at = self._clock()
        summary = RedactedCheckpointStateSummary(
            key=entry.key,
            unit_key=unit_key,
            execution_generation=generation.execution_generation,
            step=observation.step,
            facts=facts,
            pending_task_names=observation.pending_task_names,
            stamped_digests=stamped,
            summary_digest=checkpoint_summary_digest(
                entry.key, facts, observation.pending_task_names, stamped
            ),
            observed_at=observed_at,
        )
        sections.runtime(
            "checkpoint",
            "checkpointer",
            "current",
            observed_at=observed_at,
            withheld=facts.withheld_value_count + observation.withheld_metadata_fields,
        )
        return InspectionRead(data=summary, sections=sections.items)

    # -- internals --

    async def _snapshot(self, request_scope: str, run_id: str) -> RunSnapshot:
        snapshot = await self._repository.read_run(request_scope, run_id)
        if snapshot is None:
            raise InspectionNotFound("workflow run not found")
        return snapshot

    async def _unit_snapshot(
        self, request_scope: str, run_id: str, unit_key: str
    ) -> tuple[RunSnapshot, UnitRecord]:
        snapshot = await self._repository.read_run(request_scope, run_id, unit_key=unit_key)
        if snapshot is None:
            raise InspectionNotFound("workflow run not found")
        unit = next((item for item in snapshot.units if item.identity.unit_key == unit_key), None)
        if (
            unit is None
            or unit.identity.belllabs_run_id != run_id
            or unit.identity.request_scope != request_scope
        ):
            raise InspectionNotFound("runtime unit not found")
        return snapshot, unit

    def _unit_summary(self, unit: UnitRecord, snapshot: RunSnapshot) -> UnitSummary:
        generations = tuple(self._generation(unit, record, snapshot) for record in unit.generations)
        status, state = _unit_status(generations)
        identity = unit.identity
        return UnitSummary(
            unit_key=identity.unit_key,
            unit_kind=identity.unit_kind,
            family=identity.family,
            semantic_operation_id=identity.semantic_operation_id,
            semantic_attempt=identity.semantic_attempt,
            latest_generation=generations[-1].execution_generation if generations else None,
            status=status,
            reconciliation_state=state,
        )

    def _generation(
        self, unit: UnitRecord, record: UnitGenerationRecord, snapshot: RunSnapshot
    ) -> GenerationInspection:
        generation = record.execution_generation
        attempts = tuple(item for item in unit.attempts if item.execution_generation == generation)
        transition = next(
            (item for item in unit.transitions if item.execution_generation == generation), None
        )
        result = next(
            (item for item in unit.results if item.execution_generation == generation), None
        )
        incidents = tuple(
            sorted(
                (item for item in unit.incidents if item.execution_generation == generation),
                key=lambda item: item.revision,
            )
        )
        namespace = next(
            (item for item in unit.namespaces if item.namespace == record.namespace), None
        )
        settled = any(
            settlement.status != "reconciliation_required"
            for claim in unit.journal
            if claim.semantic_binding_id == record.binding_id
            for settlement in claim.settlements
        )
        state = _generation_reconciliation(
            unit.identity.unit_key,
            generation,
            incidents,
            snapshot.run.projection.unit_reconciliations,
            ambiguous=any(
                item.ambiguous
                for item in _effect_statuses(snapshot.effects)
                if item.operation_ref == record.binding_id
            ),
        )
        status: UnitStatus
        if record.superseded:
            status = "superseded"
        elif incidents and incidents[-1].status == "operator_required":
            status = "in_doubt"
        elif settled:
            status = "settled"
        elif attempts or result is not None:
            status = "active"
        else:
            status = "pending"
        return GenerationInspection(
            execution_generation=generation,
            claim_fence=record.claim_fence,
            binding_id=record.binding_id,
            binding_digest=record.binding_digest,
            cognitive_namespace=record.namespace,
            state_schema_digest=record.state_schema_digest,
            lease_holder=record.lease_holder,
            lease_expires_at=record.lease_expires_at,
            lease_state=lease_state(
                record.lease_holder, record.lease_expires_at, snapshot.observed_at
            ),
            superseded=record.superseded,
            attempts=attempts,
            transition=transition,
            result=result,
            incidents=incidents,
            rejections=tuple(
                item for item in unit.rejections if item.execution_generation == generation
            ),
            namespace_head=namespace.head if namespace is not None else None,
            namespace_in_flight=(
                namespace is not None
                and namespace.in_flight_unit_key == unit.identity.unit_key
                and namespace.in_flight_generation == generation
            ),
            status=status,
            reconciliation_state=state,
        )

    async def _async_children(
        self,
        children: tuple[AsyncChildInspection, ...],
        request_scope: str,
        sections: _Sections,
    ) -> tuple[AsyncChildInspection, ...]:
        sections.authority("async_children")
        if self._async_details is None:
            sections.runtime(
                "async_children_detail",
                "mongo_detail",
                "unavailable",
                reason="detail_reader_not_configured",
            )
            return children
        enriched: list[AsyncChildInspection] = []
        try:
            for child in children:
                detail = await self._async_details.get_execution(
                    request_scope, child.child_execution_id
                )
                enriched.append(_with_detail(child, detail))
        except Exception:  # noqa: BLE001 - a detail outage degrades the section only
            sections.runtime(
                "async_children_detail",
                "mongo_detail",
                "unavailable",
                reason="detail_read_failed",
            )
            return children
        sections.runtime(
            "async_children_detail", "mongo_detail", "current", observed_at=self._clock()
        )
        return tuple(enriched)

    async def _temporal(
        self, request_scope: str, snapshot: RunSnapshot, sections: _Sections
    ) -> tuple[TemporalExecution, ...]:
        if self._visibility is None:
            sections.runtime(
                "temporal",
                "temporal_visibility",
                "unavailable",
                reason="visibility_reader_not_configured",
            )
            return ()
        try:
            executions = await self._visibility.list_run_executions(
                request_scope, snapshot.run.projection.run_id
            )
        except Exception:  # noqa: BLE001 - Temporal unavailability degrades freshness only
            sections.runtime(
                "temporal",
                "temporal_visibility",
                "unavailable",
                reason="temporal_visibility_unavailable",
            )
            return ()
        observed_at = self._clock()
        terminal = snapshot.run.projection.phase == RunPhase.TERMINAL
        lagging = terminal and any(item.status == "RUNNING" for item in executions)
        sections.runtime(
            "temporal",
            "temporal_visibility",
            "stale" if lagging else "current",
            observed_at=observed_at,
            reason="visibility_lags_authority" if lagging else None,
        )
        return executions

    async def _lineage_entries(
        self,
        unit: UnitRecord,
        generation: UnitGenerationRecord,
        recorded: dict[str, QualifiedCheckpointKey],
        digest: str | None,
        sections: _Sections,
    ) -> list[CheckpointHistoryEntry] | None:
        namespace = generation.namespace
        assert namespace is not None
        if digest is None:
            sections.runtime(
                "checkpoints", "checkpointer", "unavailable", reason="no_recorded_checkpoint_key"
            )
            return None
        if self._checkpoints is None or not self._checkpoints.supports(digest):
            sections.runtime(
                "checkpoints",
                "checkpointer",
                "unavailable",
                reason="checkpointer_not_registered",
            )
            return None
        try:
            observations = await self._checkpoints.list_root_checkpoints(digest, namespace)
        except RuntimeSourceUnavailable as error:
            reason = (
                error.reason if error.reason != "source_unavailable" else "checkpointer_unavailable"
            )
            sections.runtime("checkpoints", "checkpointer", "unavailable", reason=reason)
            return None
        try:
            entries = _unit_lineage(unit, generation, recorded, observations)
        except CheckpointLineageCycle as error:
            sections.runtime("checkpoints", "checkpointer", "unavailable", reason=error.reason)
            return None
        observed_at = self._clock()
        sections.runtime(
            "checkpoints",
            "checkpointer",
            "current",
            observed_at=observed_at,
            withheld=sum(item.withheld_metadata_fields for item in observations),
        )
        return entries


# --- Pure helpers ------------------------------------------------------------------------


def _operator_waits(projection: RunProjection) -> tuple[WaitCondition, ...]:
    return tuple(wait for wait in projection.active_waits if wait.kind == "operator_reconciliation")


def _projection_reconciliation(projection: RunProjection) -> ReconciliationState:
    return "operator_required" if _operator_waits(projection) else "none"


def _output_refs(projection: RunProjection) -> tuple[str, ...]:
    refs = [item.output_ref for item in projection.accepted_output_evidence]
    refs.extend(projection.finalization_output_refs)
    for decision in projection.readiness:
        refs.extend(decision.output_refs)
    return tuple(dict.fromkeys(refs))


def _budget_summary(budget: BudgetState | None) -> BudgetSummary | None:
    if budget is None:
        return None
    return BudgetSummary(
        account_id=budget.account_id,
        limits=budget.limits,
        reserved=dict(budget.reserved),
        consumed=dict(budget.consumed),
        pending_settlement=dict(budget.pending_settlement),
        reservation_ids=tuple(sorted(budget.reservations)),
        outstanding_usage_count=len(budget.outstanding_usage_ids),
    )


def _effect_statuses(effects: EffectLedgerState | None) -> tuple[EffectStatus, ...]:
    if effects is None:
        return ()
    statuses: list[EffectStatus] = []
    for effect_id in sorted(effects.claims):
        claim = effects.claims[effect_id]
        observed = tuple(item.disposition for item in claim.observations)
        statuses.append(
            EffectStatus(
                effect_id=claim.effect_id,
                effect_kind=claim.effect_kind,
                operation_ref=claim.operation_ref,
                disposition=claim.disposition,
                observed_dispositions=observed,
                settlement_outcome=(
                    claim.settlement.outcome if claim.settlement is not None else None
                ),
                ambiguous=claim.settlement is None and EffectDisposition.AMBIGUOUS in observed,
            )
        )
    return tuple(statuses)


def _generation_reconciliation(
    unit_key: str,
    generation: int,
    incidents: tuple[UnitReconciliationIncident, ...],
    decisions: tuple[UnitReconciliationDecision, ...],
    *,
    ambiguous: bool,
) -> ReconciliationState:
    if incidents:
        latest = incidents[-1]
        if latest.status == "operator_required":
            decided = any(
                item.unit_key == unit_key
                and item.execution_generation == generation
                and item.incident_id == latest.incident_id
                for item in decisions
            )
            # An accepted decision not yet applied to lineage is pending, not unattended.
            return "pending" if decided else "operator_required"
    return "in_doubt" if ambiguous else "none"


def _unit_status(
    generations: tuple[GenerationInspection, ...],
) -> tuple[UnitStatus, ReconciliationState]:
    if not generations:
        return "pending", "none"
    latest = generations[-1]
    return latest.status, worst_reconciliation_state(
        [item.reconciliation_state for item in generations]
    )


def _select_generation(unit: UnitRecord, execution_generation: int | None) -> UnitGenerationRecord:
    if not unit.generations:
        raise InspectionNotFound("the runtime unit has no recorded generation")
    if execution_generation is None:
        return max(unit.generations, key=lambda item: item.execution_generation)
    for record in unit.generations:
        if record.execution_generation == execution_generation:
            return record
    raise InspectionNotFound("the runtime unit generation is not recorded")


def _recorded_keys(
    unit: UnitRecord, generation: UnitGenerationRecord
) -> dict[str, QualifiedCheckpointKey]:
    """Every qualified key BellLabs recorded for the generation, by role (keys only)."""

    number = generation.execution_generation
    keys: dict[str, QualifiedCheckpointKey] = {}
    sources = [
        item.expected_source
        for item in unit.attempts
        if item.execution_generation == number and item.expected_source is not None
    ]
    if sources:
        keys["expected_source"] = sources[0]
    transition = next(
        (item for item in unit.transitions if item.execution_generation == number), None
    )
    if transition is not None:
        if transition.source_key is not None:
            keys["expected_source"] = transition.source_key
        keys["result"] = transition.result_key
    for incident in unit.incidents:
        if incident.execution_generation != number:
            continue
        if incident.expected_source is not None:
            keys.setdefault("expected_source", incident.expected_source)
        for index, candidate in enumerate(incident.candidates):
            keys[f"incident_candidate:{incident.revision}:{index}"] = candidate
        if incident.accepted_checkpoint is not None:
            keys[f"accepted_descendant:{incident.revision}"] = incident.accepted_checkpoint
    namespace = next(
        (item for item in unit.namespaces if item.namespace == generation.namespace), None
    )
    if namespace is not None and namespace.head is not None:
        keys["namespace_head"] = namespace.head
    return keys


def _checkpointer_digest(recorded: Mapping[str, QualifiedCheckpointKey]) -> str | None:
    digests = {key.checkpointer_ref_digest for key in recorded.values()}
    return digests.pop() if len(digests) == 1 else None


def _unit_lineage(
    unit: UnitRecord,
    generation: UnitGenerationRecord,
    recorded: Mapping[str, QualifiedCheckpointKey],
    observations: tuple[CheckpointObservation, ...],
) -> list[CheckpointHistoryEntry]:
    """The generation's stamped root checkpoints on its recorded lineage, oldest first.

    With a transition, the lineage is the parent chain from the result key back to (but
    excluding) the expected source. Otherwise it is every root checkpoint carrying this
    generation's invocation stamp whose parent chain reaches the expected source (or the
    namespace root when there is none). Checkpoints of other units sharing the namespace
    are never part of it.
    """

    number = generation.execution_generation
    unit_key = unit.identity.unit_key
    invocation = submission_invocation_id(unit_key, number)
    by_id = {item.key.checkpoint_id: item for item in observations}
    source = recorded.get("expected_source")
    stop = source.checkpoint_id if source is not None else None

    def stamped(item: CheckpointObservation) -> bool:
        return (
            item.stamps.get(STAMP_INVOCATION_ID) == invocation
            and item.stamps.get(STAMP_UNIT_KEY) == unit_key
        )

    def chain(leaf: str) -> list[CheckpointObservation] | None:
        # Each listed checkpoint can be visited at most once, so the walk is bounded by the
        # (already bounded) observation count; a revisit means the parent links are cyclic.
        found: list[CheckpointObservation] = []
        visited: set[str] = set()
        cursor: str | None = leaf
        while cursor != stop:
            if cursor is None:
                return found if stop is None else None
            if cursor in visited or len(visited) > len(by_id):
                raise CheckpointLineageCycle(
                    "the checkpointer's parent links form a cycle in this namespace"
                )
            visited.add(cursor)
            item = by_id.get(cursor)
            if item is None:
                return None
            found.append(item)
            cursor = item.key.parent_checkpoint_id
        return found

    members: dict[str, CheckpointObservation] = {}
    result = recorded.get("result")
    if result is not None:
        for item in chain(result.checkpoint_id) or ():
            if stamped(item):
                members[item.key.checkpoint_id] = item
    else:
        for item in observations:
            if stamped(item) and chain(item.key.checkpoint_id) is not None:
                members[item.key.checkpoint_id] = item
    roles: dict[str, list[CheckpointRole]] = {}
    for name, key in recorded.items():
        role = name.split(":", 1)[0]
        if role in {"result", "namespace_head", "incident_candidate", "accepted_descendant"}:
            roles.setdefault(key.checkpoint_id, []).append(role)  # type: ignore[arg-type]
    ordered = sorted(
        members.values(),
        key=lambda item: (item.step if item.step is not None else -1, item.key.checkpoint_id),
    )
    return [
        CheckpointHistoryEntry(
            key=item.key,
            step=item.step,
            source=item.source,
            created_at=item.created_at,
            stamped=True,
            binding_compatible=item.stamps.get(STAMP_BINDING_DIGEST) == generation.binding_digest,
            state_schema_compatible=(
                generation.state_schema_digest is not None
                and item.stamps.get(STAMP_STATE_SCHEMA_DIGEST) == generation.state_schema_digest
            ),
            roles=tuple(dict.fromkeys(roles.get(item.key.checkpoint_id, ()))),
            pending_task_names=item.pending_task_names,
        )
        for item in ordered
    ]


def _owned_by(child: AsyncChildInspection, generations: Sequence[UnitGenerationRecord]) -> bool:
    """A child belongs to a unit generation only through that generation's exact binding.

    The 0016 parent authority row records `parent_operation_id` but no parent binding
    (`app/migrations/0016_async_subagent_parent_child_v1.sql`, `async_subagent_authority`);
    the binding is `AsyncSubagentExecution.parent_binding_id` in the immutable detail
    document (`app/domain/operation_execution/contracts.py`, `AsyncSubagentExecution`).
    With the detail, the child must name a binding of this unit at the same generation.
    Without it, the authority's `parent_operation_id` is attributed only when it is itself
    one of the unit's binding IDs. A semantic operation ID, which units of one run may
    share, never attributes a child; such a child stays visible on the run read.
    """

    for record in generations:
        if child.parent_binding_id is not None:
            if (
                child.parent_binding_id == record.binding_id
                and child.execution_generation == record.execution_generation
            ):
                return True
        elif child.parent_operation_id == record.binding_id:
            return True
    return False


def _with_detail(child: AsyncChildInspection, detail: Any) -> AsyncChildInspection:
    """Thin, tolerant mapping of the immutable detail document (unknown fields ignored)."""

    if detail is None:
        return child
    lifecycle = getattr(detail, "lifecycle", None)
    return child.model_copy(
        update={
            "parent_binding_id": getattr(detail, "parent_binding_id", None),
            "provider_thread_id": getattr(detail, "provider_thread_id", None),
            "provider_run_id": getattr(detail, "provider_run_id", None),
            "detail_lifecycle": (
                str(getattr(lifecycle, "value", lifecycle)) if lifecycle is not None else None
            ),
        }
    )


# --- In-memory adapter (behavioral tests) ------------------------------------------------


class InMemoryInspectionReadRepository:
    """Reads the in-memory run-control and lineage adapters; it never mutates them."""

    def __init__(
        self,
        run_control: InMemoryRunControlRepository,
        lineage: InMemoryCheckpointLineageRepository,
        *,
        async_children: Mapping[str, tuple[AsyncChildInspection, ...]] | None = None,
        journal: Mapping[str, tuple[JournalClaimInspection, ...]] | None = None,
        clock: Clock = lambda: datetime.now(UTC),
    ) -> None:
        self._run_control = run_control
        self._lineage = lineage
        self._async_children = dict(async_children or {})
        self._journal = dict(journal or {})
        self._clock = clock

    async def list_runs(
        self,
        request_scope: str,
        *,
        phases: frozenset[RunPhase],
        after_run_id: str | None,
        limit: int,
    ) -> tuple[tuple[RunRecord, ...], datetime]:
        records: list[RunRecord] = []
        for run_id in sorted(self._run_control.scoped_run_ids(request_scope)):
            if after_run_id is not None and run_id <= after_run_id:
                continue
            projection = await self._run_control.get_run(request_scope, run_id)
            if phases and projection.phase not in phases:
                continue
            records.append(RunRecord(projection=projection, updated_at=projection.updated_at))
            if len(records) == limit:
                break
        return tuple(records), self._clock()

    async def read_run(
        self, request_scope: str, run_id: str, *, unit_key: str | None = None
    ) -> RunSnapshot | None:
        if run_id not in self._run_control.scoped_run_ids(request_scope):
            return None
        try:
            projection = await self._run_control.get_run(request_scope, run_id)
        except RunControlNotFound:
            return None
        try:
            budget: BudgetState | None = await self._run_control.get_budget(request_scope, run_id)
        except RunControlNotFound:
            budget = None
        try:
            effects: EffectLedgerState | None = await self._run_control.get_effects(
                request_scope, run_id
            )
        except RunControlNotFound:
            effects = None
        lineage = self._lineage
        units: list[UnitRecord] = []
        for (scope, key), identity in sorted(lineage.units.items()):
            if scope != request_scope or identity.belllabs_run_id != run_id:
                continue
            if unit_key is not None and key != unit_key:
                continue
            generations = tuple(
                sorted(
                    (
                        record
                        for (g_scope, g_key, _), record in lineage.generations.items()
                        if g_scope == scope and g_key == key
                    ),
                    key=lambda record: record.execution_generation,
                )
            )
            namespaces = {record.namespace for record in generations if record.namespace}
            units.append(
                UnitRecord(
                    identity=identity,
                    generations=generations,
                    attempts=await lineage.list_attempts(scope, key),
                    transitions=tuple(
                        item
                        for (t_scope, t_key, _), item in sorted(lineage.transitions.items())
                        if t_scope == scope and t_key == key
                    ),
                    results=tuple(
                        item
                        for (r_scope, r_key, _), item in sorted(lineage.results.items())
                        if r_scope == scope and r_key == key
                    ),
                    incidents=_incident_revisions(lineage, scope, key),
                    rejections=await lineage.list_rejections(scope, key),
                    namespaces=tuple(
                        record
                        for (n_scope, name), record in sorted(lineage.namespaces.items())
                        if n_scope == scope and name in namespaces
                    ),
                    journal=self._journal.get(key, ()),
                )
            )
        return RunSnapshot(
            run=RunRecord(projection=projection, updated_at=projection.updated_at),
            observed_at=self._clock(),
            budget=budget,
            effects=effects,
            units=tuple(units),
            async_children=self._async_children.get(run_id, ()),
            boundary_commands=await self._run_control.list_boundary_commands(
                request_scope, run_id
            ),
        )


def _incident_revisions(
    lineage: InMemoryCheckpointLineageRepository, request_scope: str, unit_key: str
) -> tuple[UnitReconciliationIncident, ...]:
    """Every incident revision, each in its latest recorded state."""

    revisions: dict[tuple[int, int], UnitReconciliationIncident] = {}
    current = [
        item
        for (scope, key, _), item in lineage.incidents.items()
        if scope == request_scope and key == unit_key
    ]
    for item in (*lineage.incident_history, *current):
        if item.request_scope == request_scope and item.unit_key == unit_key:
            revisions[(item.execution_generation, item.revision)] = item
    return tuple(revisions[key] for key in sorted(revisions))
