"""Continuation fixtures (FT-B4): ledger facts, in-memory ports and a wired service."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any

from mission_control.application.context.continuation import (
    ContinuationService,
    InMemoryCheckpoints,
    InMemoryContinuationStore,
    InMemoryMailbox,
    RecordingContinuationEvents,
    SealTarget,
    WorkspaceSnapshot,
)
from mission_control.application.context.pack_service import (
    ArtifactContent,
    ContextPackService,
)
from mission_control.contracts.canonical import canonical_digest
from mission_control.domain.context.checkpoint import (
    ArtifactRefs,
    BudgetsRemaining,
    CheckpointIdentities,
    CompactionFailurePolicy,
    ContinuationFacts,
    ContinuationGovernorPolicy,
    ContinuationTrigger,
    ContinuationTriggerKind,
    GoalsAndCriteria,
    GovernorsRemaining,
    TypedStateRef,
    WorkState,
)
from mission_control.domain.context.packet import (
    ContextPacket,
    ModelBudgetProfile,
    PacketScope,
)
from mission_control.domain.context.refs import durable_input_locator
from mission_control.domain.context.render import ContextSelectionRecord

CONT_SCOPE = "mc/0192a4f0-0000-7000-8000-00000000b10e/biotech/1b0e1c4e-6d0f-5c6b-9a4e-0d6d6f1c2a01"
PACKET_SCOPE = PacketScope(
    installation_id="0192a4f0-0000-7000-8000-00000000b10e",
    application_id="biotech",
    tenant_id="1b0e1c4e-6d0f-5c6b-9a4e-0d6d6f1c2a01",
)
RUN_KEY = "run-continuation-1"
NOW = datetime(2026, 10, 8, 9, 0, tzinfo=UTC)


def digest(text: str | bytes) -> str:
    data = text.encode("utf-8") if isinstance(text, str) else text
    return "sha256:" + hashlib.sha256(data).hexdigest()


class StepClock:
    def __init__(self, start: datetime = NOW) -> None:
        self._now = start

    def __call__(self) -> datetime:
        value = self._now
        self._now = value + timedelta(seconds=1)
        return value


class NoArtifacts:
    async def capture(
        self, source_ref: str, *, request_scope: str, max_text_bytes: int
    ) -> ArtifactContent | None:
        return None


class FakeSelections:
    def __init__(self) -> None:
        self.packets: dict[str, ContextPacket] = {}
        self.selections: dict[str, ContextSelectionRecord] = {}

    async def record(
        self, packet: ContextPacket, selection: ContextSelectionRecord, *, request_scope: str
    ) -> ContextPacket:
        stored = self.packets.setdefault(packet.packet_id, packet)
        if stored.packet_digest != packet.packet_digest:
            raise AssertionError("conflicting packet")
        self.selections[packet.packet_id] = selection
        return stored

    async def get(self, packet_id: str, *, request_scope: str) -> ContextPacket | None:
        return self.packets.get(packet_id)


class FakeStaging:
    """Content-addressed staging plus retrieval (the durable input path)."""

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

    async def stage(self, *, request_scope: str, name: str, content: bytes, media_type: str) -> str:
        value = digest(content)
        object_ref = f"file-artifact://{value.removeprefix('sha256:')}"
        self.objects[object_ref] = content
        return durable_input_locator(object_ref, value, len(content))

    async def retrieve(self, durable_ref: str) -> bytes:
        try:
            return self.objects[durable_ref.partition("#")[0]]
        except KeyError as error:
            raise LookupError(durable_ref) from error


class MemorySnapshots:
    """A WorkspaceSnapshotPort over in-memory session files (path -> text)."""

    def __init__(self, staging: FakeStaging, files: dict[str, dict[str, str]]) -> None:
        self.staging = staging
        self.files = files
        self.snapshots: dict[str, WorkspaceSnapshot] = {}

    async def snapshot(
        self, *, request_scope: str, run_key: str, session_ref: str, roots: Sequence[str]
    ) -> WorkspaceSnapshot:
        manifest: dict[str, str] = {}
        durable: dict[str, str] = {}
        total = 0
        for path, text in sorted(self.files.get(session_ref, {}).items()):
            content = text.encode("utf-8")
            manifest[path] = digest(content)
            durable[path] = await self.staging.stage(
                request_scope=request_scope, name=path, content=content, media_type="text/plain"
            )
            total += len(content)
        ref = "workspace-snapshot:" + canonical_digest(manifest).removeprefix("sha256:")
        snapshot = WorkspaceSnapshot(
            snapshot_ref=ref, manifest=manifest, durable_refs=durable, total_bytes=total
        )
        self.snapshots[ref] = snapshot
        return snapshot

    async def load(self, *, request_scope: str, snapshot_ref: str) -> WorkspaceSnapshot | None:
        return self.snapshots.get(snapshot_ref)


PROFILE = ModelBudgetProfile(
    model_profile_ref="frontier.long_context",
    tokenizer_ref="unknown",
    context_window=200_000,
    reserved_output=16_000,
    system_prompt_tokens=3_000,
    tool_schema_allowance=6_000,
    skills_metadata_tokens=1_000,
    control_reserve=8_000,
    safety_margin=16_000,
)


def seal_target(generation: int = 1) -> SealTarget:
    return SealTarget(
        revision_id="rev-1",
        node_key="collect",
        activation_id="unit-collect-1",
        attempt_no=1,
        generation=generation,
        profile=PROFILE,
    )


def facts(**overrides: Any) -> ContinuationFacts:
    values: dict[str, Any] = {
        "scope": PACKET_SCOPE,
        "identities": CheckpointIdentities(
            mission_id="mission-1",
            run_id=RUN_KEY,
            revision_id="rev-1",
            node_key="collect",
            activation_id="unit-collect-1",
            logical_execution_id="logical-collect-1",
            source_agent_session_ref="thread-collect-1",
        ),
        "goals_and_criteria": GoalsAndCriteria(
            goal_refs=("goal://evidence_map",),
            objective_refs=("objective://pubmed_sweep",),
            criterion_refs=("criterion://180_records",),
            acceptance_state="in_progress",
        ),
        "decision_refs": ("decision://scope-muscle-aging",),
        "artifact_refs": ArtifactRefs(
            inputs=("artifact://inputs/sources",), outputs=("artifact://outputs/report",)
        ),
        "workspace_snapshot_ref": "workspace-snapshot:pending",
        "work": WorkState(
            completed=("search pubmed",), active=("summarize",), pending=("draft evidence map",)
        ),
        "open_questions": ("include preprints?",),
        "open_human_task_refs": ("human_task://review-scope",),
        "open_gate_refs": ("human_task://review-gate",),
        "queued_command_ids": ("cmd-queued-1",),
        "event_cursor": 42,
        "budgets_remaining": BudgetsRemaining(tokens=900_000, cost_micros=14_200_000),
        "governors_remaining": GovernorsRemaining(
            transfers=8, failed_compactions=3, no_progress_transfers=2
        ),
        "capability_pins": ("mcp.pubmed@1.0.0#sha256:" + "a" * 64,),
        "model_profile_ref": "frontier.long_context",
        "lane_profile": "deep_agents",
        "invariants": ("never write outside /outputs",),
        "typed_state": TypedStateRef(
            schema_ref="mc.loop_state.v1",
            state_version=3,
            state_digest=digest("loop-state-3"),
            state_ref=f"state://{RUN_KEY}/goal/3/loop_state",
        ),
        "pending_actions": ("draft evidence map",),
    }
    values.update(overrides)
    return ContinuationFacts.model_validate(values)


def trigger(
    kind: ContinuationTriggerKind = ContinuationTriggerKind.REQUEST_CONTINUATION,
    ref: str = "command://cmd-continue-1",
) -> ContinuationTrigger:
    return ContinuationTrigger(kind=kind, ref=ref, observed_at=NOW)


def build_service(
    *,
    session_files: Mapping[str, Mapping[str, str]] | None = None,
    pending_commands: Sequence[str] = (),
    compactor: Any = None,
    failure_policy: CompactionFailurePolicy | None = None,
    governors: ContinuationGovernorPolicy | None = None,
    confirmation: Any = None,
    staging: FakeStaging | None = None,
    snapshots: Any = None,
) -> dict[str, Any]:
    store = InMemoryContinuationStore()
    selections = FakeSelections()
    staging = staging or FakeStaging()
    snapshot_port = snapshots or MemorySnapshots(
        staging,
        {key: dict(value) for key, value in (session_files or {}).items()},
    )
    mailbox = InMemoryMailbox(pending={RUN_KEY: list(pending_commands)})
    events = RecordingContinuationEvents()
    packets = ContextPackService(artifacts=NoArtifacts(), selections=selections, staging=staging)
    service = ContinuationService(
        transfers=store,
        checkpoints=InMemoryCheckpoints(store),
        packets=packets,
        packet_reader=selections,
        snapshots=snapshot_port,
        events=events,
        mailbox=mailbox,
        compactor=compactor,
        confirmation=confirmation,
        failure_policy=failure_policy,
        governors=governors,
        clock=StepClock(),
    )
    return {
        "service": service,
        "store": store,
        "selections": selections,
        "staging": staging,
        "snapshots": snapshot_port,
        "mailbox": mailbox,
        "events": events,
    }
