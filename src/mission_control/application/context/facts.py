"""Ledger facts for a Session Lane continuation (MP-12).

``ContinuationService.seal`` needs :class:`ContinuationFacts` captured from authoritative
state after new agent actions were frozen. For a Session Lane unit driven by
``lane.turn`` segments, :class:`OperationFactsCapture` derives them from the admitted
operation request (identity, grants, budgets, model binding), the lane's persisted
execution state and the persisted frames (usage of the frozen turn, its reported outputs).
Nothing is taken from the agent's own account of its work.

What the worker cannot read is reported as what it is: ``event_cursor`` is ``0`` unless a
:class:`MissionEventCursor` is composed (the mission event sequence lives in run control),
and the model budget profile is the deployment's conservative default unless a
:class:`BudgetProfileResolver` names the exact profile. Both are composition inputs for the
integrator, not values this module guesses at.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from mission_control.application.context.continuation import ContinuationTransfer, SealTarget
from mission_control.application.execution.harness.controls import usage_from_frames
from mission_control.application.execution.harness.state import LaneExecutionState
from mission_control.contracts.canonical import canonical_digest
from mission_control.contracts.identities import parse_request_scope
from mission_control.domain.context.checkpoint import (
    ArtifactRefs,
    BudgetsRemaining,
    CheckpointIdentities,
    ContinuationFacts,
    GoalsAndCriteria,
    GovernorsRemaining,
    TypedStateRef,
    WorkState,
)
from mission_control.domain.context.packet import LaneFileSupport, ModelBudgetProfile, PacketScope
from mission_control.domain.execution.contracts import OperationExecutionRequest
from mission_control.domain.frames.contracts import FrameKind, ProviderFrame

LANE_STATE_SCHEMA = "mc.lane_execution_state.v1"
# Conservative terms for a lane whose exact model profile is not composed: a 128k window
# with generous reserves, so the packer's budget rule under-fills rather than over-fills.
CONSERVATIVE_CONTEXT_WINDOW = 128_000


@dataclass(frozen=True)
class CapturedFacts:
    facts: ContinuationFacts
    target: SealTarget


@dataclass(frozen=True)
class FactsContext:
    """What the activity knows when it captures the facts of one transfer."""

    operation: OperationExecutionRequest
    harness_execution_id: str
    generation: int
    turn_no: int
    lane_state: LaneExecutionState | None = None
    frames: Sequence[ProviderFrame] = ()


LaneFactsContext = FactsContext


class ContinuationFactsCapture(Protocol):
    async def capture(
        self, transfer: ContinuationTransfer, context: FactsContext, *, request_scope: str
    ) -> CapturedFacts: ...


class MissionEventCursor(Protocol):
    async def event_cursor(self, request_scope: str, run_key: str) -> int: ...


class BudgetProfileResolver(Protocol):
    def profile_for(self, operation: OperationExecutionRequest) -> ModelBudgetProfile: ...


class ConservativeBudgetProfiles:
    """The default resolver: the model reference from the binding, conservative terms."""

    def __init__(self, context_window: int = CONSERVATIVE_CONTEXT_WINDOW) -> None:
        self._window = context_window

    def profile_for(self, operation: OperationExecutionRequest) -> ModelBudgetProfile:
        return ModelBudgetProfile(
            model_profile_ref=model_profile_ref(operation),
            tokenizer_ref="unknown",
            context_window=self._window,
            reserved_output=self._window // 8,
            system_prompt_tokens=4_000,
            tool_schema_allowance=8_000,
            skills_metadata_tokens=2_000,
            control_reserve=8_000,
            safety_margin=self._window // 8,
        )


def model_profile_ref(operation: OperationExecutionRequest) -> str:
    binding = operation.cursor_binding
    if binding is not None:
        return f"{binding.lane_profile}:{binding.model_id}"
    policy = operation.model_policy
    return f"{policy.provider}:{policy.model}"


def packet_scope(request_scope: str) -> PacketScope:
    try:
        parsed = parse_request_scope(request_scope)
    except ValueError:
        # Non-canonical scopes (local proof) name all three parts by the same value, as
        # `lane_turns.harness_scope` does.
        return PacketScope(
            installation_id=request_scope, application_id=request_scope, tenant_id=request_scope
        )
    return PacketScope(
        installation_id=str(parsed.installation_id),
        application_id=parsed.application_id,
        tenant_id=str(parsed.tenant_id),
    )


def _budget(limits: Mapping[str, int], used_tokens: int | None) -> BudgetsRemaining:
    tokens = limits.get("tokens.total")
    if tokens is not None and used_tokens is not None:
        tokens = max(tokens - used_tokens, 0)
    cost = limits.get("cost.micros_usd", limits.get("cost.micros"))
    wall = limits.get("wall_clock_s", limits.get("wall_clock_seconds"))
    return BudgetsRemaining(tokens=tokens, cost_micros=cost, wall_clock_seconds=wall)


def _output_refs(frames: Sequence[ProviderFrame]) -> tuple[str, ...]:
    """Output references the provider's own closing frames named (never free text)."""

    from mission_control.domain.frames.body import frame_body_object

    refs: dict[str, None] = {}
    for frame in frames:
        if frame.kind != FrameKind.RUN_RESULT:
            continue
        body = frame_body_object(frame.body_excerpt, frame.body_bytes)
        for item in body.get("output_refs", ()) if isinstance(body, dict) else ():
            if isinstance(item, str) and item:
                refs.setdefault(item, None)
    return tuple(refs)


class OperationFactsCapture:
    """Facts for a Session Lane unit from its admitted request, lane state and frames."""

    def __init__(
        self,
        *,
        profiles: BudgetProfileResolver | None = None,
        event_cursor: MissionEventCursor | None = None,
        lane: LaneFileSupport | None = None,
        mission_id: str | None = None,
    ) -> None:
        self._profiles = profiles or ConservativeBudgetProfiles()
        self._event_cursor = event_cursor
        self._lane = lane or LaneFileSupport()
        self._mission_id = mission_id

    async def capture(
        self, transfer: ContinuationTransfer, context: FactsContext, *, request_scope: str
    ) -> CapturedFacts:
        operation = context.operation
        unit = operation.runtime_unit
        run_key = transfer.run_key
        activation = transfer.activation_key
        node_key = unit.unit_key if unit is not None else operation.identity.operation_id
        state = context.lane_state
        usage = usage_from_frames(context.frames, state.native_turn_ref if state else None)
        used_tokens = usage.total_tokens if usage.disposition != "unknown" else None
        cursor = 0
        if self._event_cursor is not None:
            cursor = await self._event_cursor.event_cursor(request_scope, run_key)
        state_fields: dict[str, Any] = {
            "harness_execution_id": transfer.harness_execution_id,
            "native_session_ref": transfer.source_session_ref,
            "native_turn_ref": state.native_turn_ref if state else None,
            "provider_cursor": state.provider_cursor if state else None,
            "turn_no": context.turn_no,
            "generation": context.generation,
            "source_generation": transfer.source_generation,
        }
        heid = transfer.harness_execution_id or operation.identity.operation_id
        outputs = _output_refs(context.frames)
        facts = ContinuationFacts(
            scope=packet_scope(request_scope),
            identities=CheckpointIdentities(
                mission_id=self._mission_id or f"run:{run_key}",
                run_id=run_key,
                revision_id=operation.effective_configuration_digest,
                node_key=node_key,
                activation_id=activation,
                logical_execution_id=transfer.logical_execution_id,
                source_agent_session_ref=transfer.source_session_ref,
            ),
            goals_and_criteria=GoalsAndCriteria(
                objective_refs=(operation.operation_contract_ref,),
                acceptance_state="in_progress",
            ),
            artifact_refs=ArtifactRefs(outputs=outputs),
            workspace_snapshot_ref=transfer.workspace_snapshot_ref or "workspace-snapshot:pending",
            workspace_manifest={},
            work=WorkState(active=(f"operation:{operation.identity.semantic_key}",)),
            queued_command_ids=transfer.held_command_ids,
            event_cursor=cursor,
            budgets_remaining=_budget(operation.budget_limits, used_tokens),
            governors_remaining=GovernorsRemaining(
                transfers=0, failed_compactions=0, no_progress_transfers=0
            ),
            capability_pins=tuple(sorted(operation.capability_grant.capabilities)),
            model_profile_ref=model_profile_ref(operation),
            lane_profile=transfer.lane_profile,
            invariants=(
                f"effective_configuration_digest:{operation.effective_configuration_digest}",
                f"binding:{_binding_digest(operation)}",
            ),
            typed_state=TypedStateRef(
                schema_ref=LANE_STATE_SCHEMA,
                state_version=context.turn_no,
                state_digest=canonical_digest(state_fields),
                state_ref=f"harness_execution://{heid}",
            ),
            pending_actions=(
                f"continue operation {operation.identity.semantic_key} in a fresh session",
            ),
        )
        target = SealTarget(
            revision_id=operation.effective_configuration_digest,
            node_key=node_key,
            activation_id=activation,
            attempt_no=max(context.generation, 1),
            generation=max(context.generation, 1),
            profile=self._profiles.profile_for(operation),
            lane=self._lane,
            mission_id=self._mission_id,
        )
        return CapturedFacts(facts=facts, target=target)


def _binding_digest(operation: OperationExecutionRequest) -> str:
    if operation.cursor_binding is not None:
        return operation.cursor_binding.binding_digest
    return operation.effective_configuration_digest


__all__ = [
    "BudgetProfileResolver",
    "CapturedFacts",
    "ConservativeBudgetProfiles",
    "ContinuationFactsCapture",
    "FactsContext",
    "LaneFactsContext",
    "MissionEventCursor",
    "OperationFactsCapture",
    "model_profile_ref",
    "packet_scope",
]
