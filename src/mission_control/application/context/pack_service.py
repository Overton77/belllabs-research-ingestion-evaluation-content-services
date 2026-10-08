"""Context Pack Service: capture candidates through ports, pack, render and persist (SPEC-02).

The packer in :mod:`mission_control.domain.context.packet` is pure. Everything that touches
storage or a tokenizer happens here, through these ports, before packing:

- :class:`ArtifactBytesPort` reads artifact metadata and (bounded) text for a durable ref.
- :class:`TokenCounterPort` resolves a deterministic :class:`TokenCounter` for a
  ``tokenizer_ref``; an unknown tokenizer yields the conservative bound.
- :class:`ContextSelectionRepository` persists a sealed packet with its
  ``mc.context_selection.v1`` record (idempotent on the packet digest per target).
- :class:`MissionFileStagingPort` stages the rendered ``.mission/`` files as content-addressed
  durable inputs, so they reach the workspace through the same verified path as artifacts.
- :class:`ModelBudgetProfilePort` resolves the budget terms of the operation's model.

Entry points: :meth:`ContextPackService.pack_for_stage` (FT-B2). ``pack_for_iteration`` (FT-B3),
``pack_for_continuation`` (FT-B4) and ``pack_for_chain_link`` (FT-D2) build on
:meth:`ContextPackService.seal`.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Protocol

from mission_control.domain.authoring.contracts import StageGraphBlueprint, StageNode
from mission_control.domain.context.checkpoint import (
    CONTINUATION_RESTORE_ROOTS,
    CheckpointBody,
    checkpoint_sections,
)
from mission_control.domain.context.packet import (
    RUN_PLACEHOLDER,
    ConservativeTokenCounter,
    ContextBinding,
    ContextPacket,
    ContextPackPolicy,
    ContextPurpose,
    ContextSourceKind,
    ContextTrust,
    ExpandMode,
    ItemProvenance,
    LaneFileSupport,
    ModelBudgetProfile,
    PackCandidate,
    PacketScope,
    PacketTarget,
    PackFailure,
    PackRequest,
    TokenCounter,
    WorkspaceRestore,
    pack,
)
from mission_control.domain.context.render import (
    ContextSelectionRecord,
    bytes_digest,
    context_selection_record,
    mission_file_slots,
    render_mission_files,
    render_prompt_segment,
    render_workspace_entries,
)
from mission_control.domain.execution.contracts import (
    OperationExecutionRequest,
    PromptSegment,
    WorkspaceOwner,
    WorkspaceOwnerKind,
    WorkspaceSlotBinding,
)
from mission_control.domain.graph_runtime.contracts import GoalHandoffReference
from mission_control.domain.graph_runtime.identities import GoalHandoffCheckpointKey
from mission_control.domain.policies.mailbox import MailboxEntry
from mission_control.domain.programs.contracts import (
    GoalHandoff,
    StageGraphAdmissionActivityRequest,
    StageInputBinding,
)
from mission_control.domain.programs.goal_directed_runtime import GoalOperationPreparationRequest
from mission_control.domain.programs.runtime_units import goal_operation_id

_PACKET_NAMESPACE = uuid.UUID("5b0f8a6e-2f7d-4c1a-9a3e-27c0c0de7a01")
DEFAULT_MAX_TEXT_BYTES = 65_536
DEFAULT_HANDOFF_PATH = "/goal/HANDOFF.md"
DEFAULT_CHECKPOINT_PATH = "/goal/checkpoint.json"
CONTEXT_PACKET_SEGMENT_PREFIX = "context-packet:"


# --------------------------------------------------------------------------------------
# Ports
# --------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ArtifactContent:
    """Captured view of one artifact: identity, digest, size and optionally its text."""

    source_ref: str
    durable_ref: str
    """Locator the workspace materializer fetches (``<object_ref>#<sha256>:<size>``)."""
    content_digest: str
    size_bytes: int
    media_type: str
    file_name: str | None = None
    text: str | None = None
    """Decoded text when the artifact is textual and within the capture limit."""
    summary: str | None = None


class ArtifactBytesPort(Protocol):
    async def capture(
        self, source_ref: str, *, request_scope: str, max_text_bytes: int
    ) -> ArtifactContent | None:
        """Metadata for ``source_ref``; ``None`` when it does not resolve in this scope."""
        ...


class TokenCounterPort(Protocol):
    def counter_for(self, tokenizer_ref: str) -> TokenCounter: ...


class ContextSelectionRepository(Protocol):
    async def record(
        self,
        packet: ContextPacket,
        selection: ContextSelectionRecord,
        *,
        request_scope: str,
    ) -> ContextPacket:
        """Persist the packet and its selection record; return the stored packet.

        Idempotent: a replay with the same packet digest for the same target returns the
        stored packet; a different digest for the same target is a conflict.
        """
        ...

    async def get(self, packet_id: str, *, request_scope: str) -> ContextPacket | None: ...


class MissionFileStagingPort(Protocol):
    async def stage(self, *, request_scope: str, name: str, content: bytes, media_type: str) -> str:
        """Stage content-addressed bytes; return ``<object_ref>#<sha256>:<size>``."""
        ...


class ModelBudgetProfilePort(Protocol):
    def profile_for(self, operation: OperationExecutionRequest) -> ModelBudgetProfile: ...


@dataclass(frozen=True, slots=True)
class ChainSupply:
    """The released chain packet of a consumer run, re-expressed as packer inputs (FT-D2)."""

    bindings: tuple[ContextBinding, ...]
    candidates: tuple[PackCandidate, ...]


class ChainSupplyPort(Protocol):
    async def supply_for(self, run_id: str, *, request_scope: str) -> ChainSupply | None:
        """The sealed ``chain_link`` packet of ``run_id`` (a released chain consumer), if any."""
        ...


class StaticTokenCounters:
    """A :class:`TokenCounterPort` over a fixed mapping; unknown tokenizers get the bound."""

    def __init__(self, counters: Mapping[str, TokenCounter] | None = None) -> None:
        self._counters = dict(counters or {})
        self._fallback = ConservativeTokenCounter()

    def counter_for(self, tokenizer_ref: str) -> TokenCounter:
        return self._counters.get(tokenizer_ref, self._fallback)


# Conservative default until model profiles carry budget terms (SPEC-05 manifests).
DEFAULT_MODEL_BUDGET_PROFILE = ModelBudgetProfile(
    model_profile_ref="mc.default_context_budget@1",
    tokenizer_ref="unknown",
    context_window=128_000,
    reserved_output=16_000,
    system_prompt_tokens=4_000,
    tool_schema_allowance=8_000,
    skills_metadata_tokens=2_000,
    control_reserve=8_000,
    safety_margin=16_000,
)


class StaticModelBudgetProfiles:
    """Profiles keyed by ``provider:model``; anything else gets the conservative default."""

    def __init__(
        self,
        profiles: Mapping[str, ModelBudgetProfile] | None = None,
        *,
        default: ModelBudgetProfile = DEFAULT_MODEL_BUDGET_PROFILE,
    ) -> None:
        self._profiles = dict(profiles or {})
        self._default = default

    def profile_for(self, operation: OperationExecutionRequest) -> ModelBudgetProfile:
        policy = operation.model_policy
        return self._profiles.get(f"{policy.provider}:{policy.model}", self._default)


# --------------------------------------------------------------------------------------
# Results and errors
# --------------------------------------------------------------------------------------


class ContextPackRejected(ValueError):
    """Packing failed (for example ``CONTEXT_BUDGET_EXCEEDED``); admission must not proceed."""

    def __init__(self, failure: PackFailure) -> None:
        super().__init__(f"{failure.code.value}: {failure.message}")
        self.failure = failure

    @property
    def code(self) -> str:
        return self.failure.code.value


@dataclass(frozen=True, slots=True)
class SealedPacket:
    """A persisted packet plus everything a lane needs to deliver it."""

    packet: ContextPacket
    selection: ContextSelectionRecord
    prompt_segment: PromptSegment
    slot_bindings: tuple[WorkspaceSlotBinding, ...]
    """Read-only slots: every ``materialize`` item plus the two ``.mission/`` files."""


@dataclass(frozen=True, slots=True)
class ContinuationPackTarget:
    """Who a continuation packet is for: the fresh session of one logical execution.

    Lane-neutral (SPEC-02 seal step 4): Deep Agents hydrates a new thread from it (B4),
    Cursor a new agent in a fresh workspace (G4). ``lane`` carries the only lane facts the
    packer knows; the workspace tier always restores ``/inputs/**``, ``/outputs/**`` and
    ``/.mission/**`` from ``workspace_snapshot_ref``.
    """

    request_scope: str
    run_id: str
    revision_id: str
    node_key: str
    activation_id: str
    attempt_no: int
    generation: int
    sealed_at: datetime
    profile: ModelBudgetProfile
    workspace_manifest_digest: str
    """Digest of the snapshot's restorable file manifest (path -> content digest)."""
    workspace_bytes: int = 0
    lane: LaneFileSupport = field(default_factory=LaneFileSupport)
    mission_id: str | None = None


# --------------------------------------------------------------------------------------
# Service
# --------------------------------------------------------------------------------------


class ContextPackService:
    def __init__(
        self,
        *,
        artifacts: ArtifactBytesPort,
        selections: ContextSelectionRepository,
        staging: MissionFileStagingPort,
        token_counters: TokenCounterPort | None = None,
        profiles: ModelBudgetProfilePort | None = None,
        policy: ContextPackPolicy | None = None,
        max_text_bytes: int = DEFAULT_MAX_TEXT_BYTES,
        chain_supplies: ChainSupplyPort | None = None,
    ) -> None:
        self._artifacts = artifacts
        # FT-D2: a chain consumer's first packet carries what its released links supplied.
        self._chain_supplies = chain_supplies
        self._selections = selections
        self._staging = staging
        self._counters = token_counters or StaticTokenCounters()
        self._profiles = profiles or StaticModelBudgetProfiles()
        self._policy = policy or ContextPackPolicy()
        self._max_text_bytes = max_text_bytes

    # -- stage handoff (FT-B2) ---------------------------------------------------------

    async def pack_for_stage(
        self,
        request: StageGraphAdmissionActivityRequest,
        template: OperationExecutionRequest,
        *,
        mount_root: str = "",
        extra_candidates: Sequence[PackCandidate] = (),
    ) -> SealedPacket:
        """Build the ``stage_start`` packet of an admitted StageGraph operation.

        Candidates: the producers' accepted outputs from ``frozen_input_bindings`` (one
        binding per consumer input slot, ``expand: auto``), the cycle objective as
        ``goals_and_criteria``, the stage's operating contract and workspace map, and any
        ``extra_candidates`` (selected catalog context, queued instructions).
        """

        proposal = request.proposal
        identity = proposal.identity
        compiled = bool(template.workspace.slot_bindings) and (
            template.workspace.workflow_contract_digest is not None
        )
        sandbox = template.deep_agent_binding.sandbox if template.deep_agent_binding else None
        lane = LaneFileSupport(
            writable_workspace=compiled,
            text_only_files=sandbox is not None and sandbox.backend == "state",
            mount_root=mount_root,
        )
        scope = _packet_scope(request.request_scope)
        target = PacketTarget(
            mission_id=request.run_id,
            run_id=request.run_id,
            revision_id=request.effective_configuration_digest or "unversioned",
            node_key=identity.stage_id,
            activation_id=identity.operation_id,
            attempt_no=identity.semantic_attempt,
            generation=identity.execution_generation,
            # FT-F4: the derived Run's first packet carries the fork's workspace restore.
            purpose=_purpose(extra_candidates, ContextPurpose.STAGE_START),
        )
        candidates: list[PackCandidate] = [
            *self._stage_contract_candidates(request, template, mount_root=mount_root),
        ]
        bindings: dict[str, ContextBinding] = {}
        if not proposal.frozen_input_bindings:
            supply = await self._chain_supply(request.run_id, request.request_scope)
            if supply is not None:
                for chain_binding in supply.bindings:
                    bindings.setdefault(chain_binding.binding_name, chain_binding)
                candidates.extend(supply.candidates)
        for binding in proposal.frozen_input_bindings:
            bindings.setdefault(
                binding.consumer_input_slot_id,
                ContextBinding(binding_name=binding.consumer_input_slot_id, expand=ExpandMode.AUTO),
            )
            candidates.append(
                await self._accepted_output(binding, request_scope=request.request_scope)
            )
        candidates.extend(extra_candidates)
        owner = WorkspaceOwner(kind=WorkspaceOwnerKind.STAGE, owner_id=identity.operation_id)
        return await self.seal(
            PackRequest(
                packet_id=_stable_uuid("packet", request.request_scope, identity.semantic_key),
                sealed_at=request.occurred_at,
                context_selection_ref=(
                    "context_selection:"
                    + _stable_uuid("selection", request.request_scope, identity.semantic_key)
                ),
                scope=scope,
                target=target,
                producer_refs=tuple(
                    sorted({item.producer_stage_key for item in proposal.frozen_input_bindings})
                ),
                profile=self._profiles.profile_for(template),
                bindings=tuple(bindings.values()),
                candidates=tuple(candidates),
                policy=self._policy,
                lane=lane,
            ),
            request_scope=request.request_scope,
            owner=owner,
            materialize_files=compiled,
        )

    # -- Goal Loop iteration handoff (FT-B3) ----------------------------------------------

    async def pack_for_iteration(
        self,
        request: GoalOperationPreparationRequest,
        template: OperationExecutionRequest,
        *,
        role_root: str,
        handoff_path: str = DEFAULT_HANDOFF_PATH,
        checkpoint_path: str = DEFAULT_CHECKPOINT_PATH,
        extra_candidates: Sequence[PackCandidate] = (),
    ) -> SealedPacket:
        """Build the ``iteration_start`` packet of one GoalDirected executor or verifier.

        Executor: goals and criteria, the bounded Loop State (inline, mandatory), the prior
        iteration's sealed handoff as the journal head (reference, mandatory), its Progress
        Review (model-authored, untrusted, inline), unresolved blockers (inline, mandatory),
        the handoff's artifacts (``auto``) and the handoff snapshot files at the policy paths
        under the role root (``materialize``). Verifier: an independent packet with only the
        executor's registered outputs (``verifier_input_refs``), never its handoff or prompt.
        ``extra_candidates`` is the hook for queued ``add_context`` items (FT-F1) and human
        answers.
        """

        role = request.operation_role
        operation_id = goal_operation_id(request.goal_iteration, role)
        scope_key = request.request_scope
        semantic = (
            f"{request.run_id}:goal:{request.goal_iteration}:{role}:"
            f"{request.operation_attempt}:{request.execution_generation}"
        )
        candidates: list[PackCandidate] = list(
            _goal_contract_candidates(request, template, role_root=role_root)
        )
        bindings: list[ContextBinding] = []
        producer_refs: list[str] = []
        if role == "verifier":
            # Independence (REQ-BP-GD-004): only registered executor outputs cross; they are
            # under verification, so they enter as provisional and never mandatory.
            if request.verifier_input_refs:
                bindings.append(ContextBinding(binding_name="executor_outputs"))
            for ref in request.verifier_input_refs:
                candidates.append(
                    await self._captured_output(
                        ref,
                        binding_name="executor_outputs",
                        request_scope=scope_key,
                        provenance=ItemProvenance(
                            producer_activation_id=goal_operation_id(
                                request.goal_iteration, "executor"
                            ),
                            iteration_id=str(request.goal_iteration),
                            provisional=True,
                        ),
                        label=f"executor output of iteration {request.goal_iteration}",
                    )
                )
        elif request.handoff is None:
            supply = await self._chain_supply(request.run_id, scope_key)
            if supply is not None:
                bindings.extend(supply.bindings)
                candidates.extend(supply.candidates)
        elif request.handoff is not None:
            handoff = request.handoff
            producer_refs.append(handoff.handoff_id)
            candidates.extend(
                await self._handoff_candidates(
                    request,
                    handoff,
                    role_root=role_root,
                    handoff_path=handoff_path,
                    checkpoint_path=checkpoint_path,
                )
            )
            if handoff.artifact_refs:
                bindings.append(ContextBinding(binding_name="prior_artifacts"))
            for ref in handoff.artifact_refs:
                candidates.append(
                    await self._captured_output(
                        ref,
                        binding_name="prior_artifacts",
                        request_scope=scope_key,
                        provenance=ItemProvenance(
                            producer_activation_id=goal_operation_id(
                                handoff.source_iteration.goal_iteration, "executor"
                            ),
                            accepted_decision_ref=handoff.handoff_id,
                            iteration_id=str(handoff.source_iteration.goal_iteration),
                        ),
                        label=f"artifact of iteration {handoff.source_iteration.goal_iteration}",
                    )
                )
        candidates.extend(extra_candidates)
        sandbox = template.deep_agent_binding.sandbox if template.deep_agent_binding else None
        owner = WorkspaceOwner(
            kind=(
                WorkspaceOwnerKind.ITERATION if role == "executor" else WorkspaceOwnerKind.EVALUATOR
            ),
            owner_id=operation_id,
        )
        return await self.seal(
            PackRequest(
                packet_id=_stable_uuid("packet", scope_key, semantic),
                sealed_at=request.decided_at,
                context_selection_ref=(
                    "context_selection:" + _stable_uuid("selection", scope_key, semantic)
                ),
                scope=_packet_scope(scope_key),
                target=PacketTarget(
                    mission_id=request.run_id,
                    run_id=request.run_id,
                    revision_id=request.goal_revision_id,
                    node_key=f"goal/{role}",
                    activation_id=operation_id,
                    attempt_no=request.operation_attempt,
                    generation=request.execution_generation,
                    purpose=_purpose(extra_candidates, ContextPurpose.ITERATION_START),
                ),
                producer_refs=tuple(producer_refs),
                profile=self._profiles.profile_for(template),
                bindings=tuple(bindings),
                candidates=tuple(candidates),
                policy=self._policy,
                lane=LaneFileSupport(
                    writable_workspace=True,
                    text_only_files=sandbox is not None and sandbox.backend == "state",
                    mount_root=role_root,
                ),
            ),
            request_scope=scope_key,
            owner=owner,
        )

    # -- continuation (FT-B4) --------------------------------------------------------------

    async def pack_for_continuation(
        self,
        body: CheckpointBody,
        target: ContinuationPackTarget,
        *,
        owner: WorkspaceOwner | None = None,
        extra_candidates: Sequence[PackCandidate] = (),
    ) -> SealedPacket:
        """Build the ``continuation`` packet a fresh session hydrates from (SPEC-02 seal).

        Exactly one ``workspace`` item restores ``/inputs/**``, ``/outputs/**`` candidates and
        ``/.mission/**`` from the checkpoint's workspace snapshot; the checkpoint's bounded
        fields (goals and criteria, pending commitments and held commands, budgets and
        governors, typed state, decisions and next actions) are inline and mandatory, and the
        typed state is also a reference the agent can fetch. ``body.context_packet_ref`` is
        ignored: the checkpoint names this packet after it is sealed.
        """

        scope_key = target.request_scope
        run_id = target.run_id
        semantic = (
            f"{run_id}:continuation:{body.identities.logical_execution_id}:{body.checkpoint_id}"
        )
        base = f"checkpoint://{run_id}/{body.checkpoint_id}"
        kinds = {
            "goals_and_criteria": ContextSourceKind.GOALS_AND_CRITERIA,
            "pending_commitments": ContextSourceKind.PENDING_COMMITMENTS,
            "budget_remaining": ContextSourceKind.BUDGET_REMAINING,
            "loop_state": ContextSourceKind.CONTINUATION_CHECKPOINT,
        }
        candidates: list[PackCandidate] = [
            _text_candidate(kinds[name], f"{base}/{name}", text)
            for name, text in checkpoint_sections(body).items()
        ]
        candidates.append(
            PackCandidate(
                source_kind=ContextSourceKind.LOOP_STATE,
                source_ref=body.typed_state.state_ref,
                content_digest=body.typed_state.state_digest,
                bytes=0,
                media_type="application/json",
                schema_ref=body.typed_state.schema_ref,
                trust=ContextTrust.AUTHORITATIVE,
                mandatory=True,
                expand=ExpandMode.REFERENCE,
                summary=(
                    f"typed state {body.typed_state.schema_ref} v{body.typed_state.state_version}"
                    f" at the seal of checkpoint {body.checkpoint_id}"
                ),
            )
        )
        candidates.append(
            PackCandidate(
                source_kind=ContextSourceKind.WORKSPACE_MAP,
                source_ref=f"snapshot://{body.workspace_snapshot_ref}",
                content_digest=target.workspace_manifest_digest,
                bytes=target.workspace_bytes,
                media_type="application/x-mission-workspace-snapshot",
                trust=ContextTrust.AUTHORITATIVE,
                mandatory=True,
                summary=(
                    "workspace restored from the checkpoint snapshot: /inputs read-only, "
                    "/outputs candidates read-write, .mission read-only"
                ),
                workspace=WorkspaceRestore(
                    snapshot_ref=body.workspace_snapshot_ref,
                    restore_paths=CONTINUATION_RESTORE_ROOTS,
                ),
            )
        )
        candidates.extend(extra_candidates)
        return await self.seal(
            PackRequest(
                packet_id=_stable_uuid("packet", scope_key, semantic),
                sealed_at=target.sealed_at,
                context_selection_ref=(
                    "context_selection:" + _stable_uuid("selection", scope_key, semantic)
                ),
                scope=_packet_scope(scope_key),
                target=PacketTarget(
                    mission_id=target.mission_id or run_id,
                    run_id=run_id,
                    revision_id=target.revision_id,
                    node_key=target.node_key,
                    activation_id=target.activation_id,
                    attempt_no=target.attempt_no,
                    generation=target.generation,
                    purpose=ContextPurpose.CONTINUATION,
                ),
                producer_refs=(f"checkpoint:{body.checkpoint_id}",),
                profile=target.profile,
                candidates=tuple(candidates),
                policy=self._policy,
                lane=target.lane,
            ),
            request_scope=scope_key,
            owner=owner
            or WorkspaceOwner(kind=WorkspaceOwnerKind.AGENT, owner_id=target.activation_id),
            materialize_files=target.lane.writable_workspace,
        )

    async def _handoff_candidates(
        self,
        request: GoalOperationPreparationRequest,
        handoff: GoalHandoff,
        *,
        role_root: str,
        handoff_path: str,
        checkpoint_path: str,
    ) -> list[PackCandidate]:
        run_id = request.run_id
        source = handoff.source_iteration.goal_iteration
        loop_state = _canonical_text(
            {
                "goal_revision_id": request.goal_revision_id,
                "goal_iteration": request.goal_iteration,
                "prior_iteration": source,
                "remaining_iterations": handoff.remaining_iterations,
                "remaining_budget": handoff.remaining_budget,
                "consumed_budget": handoff.consumed_budget,
                "accepted_fact_refs": list(handoff.accepted_fact_refs),
                "evidence_refs": list(handoff.evidence_refs),
                "artifact_refs": list(handoff.artifact_refs),
                "effect_frontier_refs": list(handoff.effect_frontier_refs),
                "pending_liability_refs": list(handoff.pending_liability_refs),
                "workspace_refs": list(handoff.workspace_refs),
                "compaction_status": handoff.compaction_status,
            }
        )
        candidates = [
            _text_candidate(
                ContextSourceKind.LOOP_STATE,
                f"state://{run_id}/goal/{request.goal_iteration}/loop_state",
                loop_state,
                trust=ContextTrust.ADMITTED_INPUT,
                media_type="application/json",
            ),
            PackCandidate(
                source_kind=ContextSourceKind.JOURNAL_DIGEST,
                source_ref=f"journal://{run_id}/goal/{source}/handoff/{handoff.handoff_digest}",
                content_digest=handoff.handoff_digest,
                bytes=0,
                media_type="application/json",
                trust=ContextTrust.AUTHORITATIVE,
                mandatory=True,
                expand=ExpandMode.REFERENCE,
                summary=(
                    f"sealed handoff of iteration {source} ({handoff.handoff_id}); "
                    "read it for the full journal head"
                ),
            ),
            _text_candidate(
                ContextSourceKind.PROGRESS_REVIEW,
                f"state://{run_id}/goal/{source}/progress_review",
                _progress_review(handoff),
                trust=ContextTrust.UNTRUSTED_CONTENT,
                mandatory=False,
            ),
        ]
        blockers = [f"blocker: {item}" for item in handoff.blockers] + [
            f"unresolved obligation: {item}" for item in handoff.unresolved_obligations
        ]
        if blockers:
            candidates.append(
                _text_candidate(
                    ContextSourceKind.BLOCKER,
                    f"state://{run_id}/goal/{source}/blockers",
                    "\n".join(blockers),
                    trust=ContextTrust.UNTRUSTED_CONTENT,
                )
            )
        snapshots = (
            (
                ContextSourceKind.PROGRESS_REVIEW,
                handoff_path,
                render_goal_handoff_markdown(handoff),
                "text/markdown",
            ),
            (
                ContextSourceKind.CONTINUATION_CHECKPOINT,
                checkpoint_path,
                render_goal_checkpoint_json(handoff),
                "application/json",
            ),
        )
        for kind, path, text, media_type in snapshots:
            content = text.encode("utf-8")
            durable_ref = await self._staging.stage(
                request_scope=request.request_scope,
                name=f"{handoff.handoff_digest}{path}",
                content=content,
                media_type=media_type,
            )
            candidates.append(
                PackCandidate(
                    source_kind=kind,
                    source_ref=f"journal://{run_id}/goal/{source}/handoff{path}",
                    content_digest=bytes_digest(content),
                    bytes=len(content),
                    media_type=media_type,
                    trust=ContextTrust.UNTRUSTED_CONTENT,
                    expand=ExpandMode.MATERIALIZE,
                    path=f"{role_root}{path}",
                    durable_ref=durable_ref,
                    summary=f"iteration {source} handoff snapshot ({path})",
                )
            )
        return candidates

    async def _captured_output(
        self,
        ref: str,
        *,
        binding_name: str,
        request_scope: str,
        provenance: ItemProvenance,
        label: str,
    ) -> PackCandidate:
        captured = await self._artifacts.capture(
            ref, request_scope=request_scope, max_text_bytes=self._max_text_bytes
        )
        if captured is None:
            # Unresolvable refs (for example model-emitted strings) stay visible as references
            # with no bytes; they can never be materialized or trusted.
            return PackCandidate(
                source_kind=ContextSourceKind.ACCEPTED_OUTPUT,
                source_ref=ref,
                binding_name=binding_name,
                content_digest=bytes_digest(ref.encode("utf-8")),
                bytes=0,
                media_type="application/x-mission-control-ref",
                trust=ContextTrust.UNTRUSTED_CONTENT,
                expand=ExpandMode.REFERENCE,
                summary=f"unresolved {label}",
                provenance=provenance,
            )
        return PackCandidate(
            source_kind=ContextSourceKind.ACCEPTED_OUTPUT,
            source_ref=ref,
            binding_name=binding_name,
            content_digest=captured.content_digest,
            bytes=captured.size_bytes,
            media_type=captured.media_type,
            # Model-produced artifacts are data, never instructions.
            trust=ContextTrust.UNTRUSTED_CONTENT,
            text=captured.text,
            summary=captured.summary
            or f"{label}: {captured.media_type}, {captured.size_bytes} bytes",
            file_name=captured.file_name,
            durable_ref=captured.durable_ref,
            provenance=provenance,
        )

    async def _chain_supply(self, run_id: str, request_scope: str) -> ChainSupply | None:
        if self._chain_supplies is None:
            return None
        return await self._chain_supplies.supply_for(run_id, request_scope=request_scope)

    # -- queued instructions and context (FT-F1) -------------------------------------------

    async def queued_candidates(
        self, entries: Sequence[MailboxEntry], *, request_scope: str
    ) -> tuple[PackCandidate, ...]:
        """Mailbox entries a boundary delivers, as mandatory ``queued_instruction`` items.

        Operator content is admitted input: data with provenance, never authority
        (ADR-0027). A ``queue_instruction`` is inline; an ``add_context`` follows its
        ``expand`` hint (``materialize`` degrades to a reference where the lane cannot write
        files, so it is not mandatory). An artifact reference that cannot be captured, or
        whose bytes differ from the commanded digest, stays a non-mandatory reference.
        """

        candidates: list[PackCandidate] = []
        for entry in entries:
            if entry.expand == "workspace":
                # FT-F4: a fork's Snapshot, restored by the lane before the first turn.
                candidates.append(
                    PackCandidate(
                        source_kind=ContextSourceKind.CONTINUATION_CHECKPOINT,
                        source_ref=entry.content_ref,
                        content_digest=entry.content_digest,
                        bytes=entry.content_bytes,
                        media_type=entry.media_type,
                        trust=ContextTrust.AUTHORITATIVE,
                        mandatory=True,
                        summary=f"fork workspace restored from {entry.content_ref}",
                        workspace=WorkspaceRestore(
                            snapshot_ref=entry.content_ref, restore_paths=FORK_RESTORE_PATHS
                        ),
                        provenance=ItemProvenance(
                            producer_generation=entry.generation,
                            accepted_decision_ref=entry.command_id,
                        ),
                    )
                )
                continue
            label = (
                f"{'instruction' if entry.kind == 'queue_instruction' else 'added context'} "
                f"queued by command {entry.command_id} ({entry.boundary}, "
                f"#{entry.admission_sequence})"
            )
            hint = ExpandMode(entry.expand) if entry.expand is not None else ExpandMode.INLINE
            provenance = ItemProvenance(
                producer_generation=entry.generation, accepted_decision_ref=entry.command_id
            )
            source_ref = f"mailbox://{entry.command_id}"
            if entry.content_inline is not None:
                durable_ref = await self._staging.stage(
                    request_scope=request_scope,
                    name=f"mailbox/{entry.content_digest}",
                    content=entry.content_inline.encode("utf-8"),
                    media_type=entry.media_type,
                )
                candidates.append(
                    PackCandidate(
                        source_kind=ContextSourceKind.QUEUED_INSTRUCTION,
                        source_ref=source_ref,
                        content_digest=entry.content_digest,
                        bytes=entry.content_bytes,
                        media_type=entry.media_type,
                        trust=ContextTrust.ADMITTED_INPUT,
                        mandatory=hint != ExpandMode.MATERIALIZE,
                        expand=hint,
                        text=entry.content_inline,
                        summary=label,
                        file_name=f"queued-{entry.admission_sequence}.md",
                        durable_ref=durable_ref,
                        provenance=provenance,
                    )
                )
                continue
            captured = await self._artifacts.capture(
                entry.content_ref, request_scope=request_scope, max_text_bytes=self._max_text_bytes
            )
            if captured is None or captured.content_digest != entry.content_digest:
                candidates.append(
                    PackCandidate(
                        source_kind=ContextSourceKind.QUEUED_INSTRUCTION,
                        source_ref=entry.content_ref,
                        content_digest=entry.content_digest,
                        bytes=0,
                        media_type="application/x-mission-control-ref",
                        trust=ContextTrust.ADMITTED_INPUT,
                        expand=ExpandMode.REFERENCE,
                        summary=f"unresolved {label}",
                        provenance=provenance,
                    )
                )
                continue
            candidates.append(
                PackCandidate(
                    source_kind=ContextSourceKind.QUEUED_INSTRUCTION,
                    source_ref=entry.content_ref,
                    content_digest=captured.content_digest,
                    bytes=captured.size_bytes,
                    media_type=captured.media_type,
                    trust=ContextTrust.ADMITTED_INPUT,
                    mandatory=hint != ExpandMode.MATERIALIZE,
                    expand=hint if captured.text is not None else ExpandMode.AUTO,
                    text=captured.text,
                    summary=captured.summary or label,
                    file_name=captured.file_name,
                    durable_ref=captured.durable_ref,
                    provenance=provenance,
                )
            )
        return tuple(candidates)

    async def pack_follow_up(
        self,
        request: OperationExecutionRequest,
        candidates: Sequence[PackCandidate],
        *,
        delivery_key: str,
        sealed_at: datetime,
    ) -> SealedPacket:
        """FT-F2: the `follow_up_turn` packet a replacement turn carries: the injected items
        (no files; the replacement continues the same session and workspace)."""

        unit = request.runtime_unit
        node_key = (
            str(getattr(unit.location, "stage_id", None) or unit.semantic_operation_id)
            if unit is not None and unit.family == "stage_graph"
            else f"goal/{getattr(unit.location, 'operation_role', 'executor')}"
            if unit is not None
            else request.identity.operation_id
        )
        deep = request.deep_agent_binding
        semantic = f"{request.identity.semantic_key}:follow-up:{delivery_key}"
        return await self.seal(
            PackRequest(
                packet_id=_stable_uuid("packet", request.request_scope, semantic),
                sealed_at=sealed_at,
                context_selection_ref=(
                    "context_selection:"
                    + _stable_uuid("selection", request.request_scope, semantic)
                ),
                scope=_packet_scope(request.request_scope),
                target=PacketTarget(
                    mission_id=request.identity.run_id,
                    run_id=request.identity.run_id,
                    revision_id=request.effective_configuration_digest,
                    node_key=node_key,
                    activation_id=request.identity.operation_id,
                    attempt_no=request.identity.operation_attempt,
                    generation=deep.execution_generation if deep is not None else 1,
                    purpose=ContextPurpose.FOLLOW_UP_TURN,
                ),
                profile=self._profiles.profile_for(request),
                candidates=tuple(candidates),
                policy=self._policy,
                lane=LaneFileSupport(writable_workspace=False),
            ),
            request_scope=request.request_scope,
            owner=WorkspaceOwner(
                kind=WorkspaceOwnerKind.STAGE, owner_id=request.identity.operation_id
            ),
            materialize_files=False,
        )

    # -- shared sealing -------------------------------------------------------------------

    async def seal(
        self,
        pack_request: PackRequest,
        *,
        request_scope: str,
        owner: WorkspaceOwner,
        materialize_files: bool = True,
    ) -> SealedPacket:
        """Pack, persist and render one packet; raise :class:`ContextPackRejected` on failure."""

        counter = self._counters.counter_for(pack_request.profile.tokenizer_ref)
        result = pack(pack_request, counter)
        if isinstance(result, PackFailure):
            raise ContextPackRejected(result)
        selection = context_selection_record(result)
        packet = await self._selections.record(result, selection, request_scope=request_scope)
        if packet.packet_digest != result.packet_digest:
            raise ValueError("persisted context packet differs from the sealed packet")
        slots: tuple[WorkspaceSlotBinding, ...] = ()
        if materialize_files:
            slots = render_workspace_entries(packet, owner)
            staged: dict[str, tuple[str, str]] = {}
            for name, text in render_mission_files(packet).items():
                media_type = "application/json" if name.endswith(".json") else "text/markdown"
                staged[name] = (
                    await self._staging.stage(
                        request_scope=request_scope,
                        name=f"{packet.packet_digest}/{name}",
                        content=text.encode("utf-8"),
                        media_type=media_type,
                    ),
                    text,
                )
            slots += mission_file_slots(staged, owner, mount_root=pack_request.lane.mount_root)
        return SealedPacket(
            packet=packet,
            selection=selection,
            prompt_segment=render_prompt_segment(packet),
            slot_bindings=slots,
        )

    # -- candidate capture ----------------------------------------------------------------

    async def _accepted_output(
        self, binding: StageInputBinding, *, request_scope: str
    ) -> PackCandidate:
        return await self._captured_output(
            binding.artifact_ref,
            binding_name=binding.consumer_input_slot_id,
            request_scope=request_scope,
            provenance=ItemProvenance(
                producer_activation_id=binding.producer_stage_key,
                accepted_decision_ref=binding.accepted_decision_ref,
                provisional=binding.provisional,
            ),
            label=f"{binding.producer_stage_key}.{binding.producer_output_slot_id}",
        )

    @staticmethod
    def _stage_contract_candidates(
        request: StageGraphAdmissionActivityRequest,
        template: OperationExecutionRequest,
        *,
        mount_root: str,
    ) -> list[PackCandidate]:
        identity = request.proposal.identity
        stage = _blueprint_stage(request.blueprint, identity.stage_id)
        outputs = ", ".join(
            slot.output_slot_id for slot in (stage.output_slots if stage is not None else ())
        )
        contract = (
            f"Stage `{identity.stage_id}` (slot `{identity.candidate.operation_slot_id}`, "
            f"workflow cycle {identity.workflow_cycle}, stage cycle {identity.stage_cycle}, "
            f"attempt {identity.semantic_attempt}). Declared outputs: {outputs or 'none'}. "
            "Write outputs only under your writable workspace paths; Mission Control decides "
            "acceptance."
        )
        writable = ", ".join(template.workspace.exclusive_write_paths)
        workspace_map = (
            f"Writable: {writable}. Read-only inputs: {mount_root}/inputs/<binding>/ "
            f"(see {mount_root}/.mission/inputs.json). "
            f"This index: {mount_root}/.mission/context.md."
        )
        candidates = [
            _text_candidate(
                ContextSourceKind.OPERATING_CONTRACT,
                f"state://{request.run_id}/{identity.stage_id}/operating_contract",
                contract,
            ),
            _text_candidate(
                ContextSourceKind.WORKSPACE_MAP,
                f"state://{request.run_id}/{identity.stage_id}/workspace_map",
                workspace_map,
            ),
        ]
        if request.proposal.objective_override is not None:
            candidates.append(
                _text_candidate(
                    ContextSourceKind.GOALS_AND_CRITERIA,
                    f"state://{request.run_id}/{identity.stage_id}/objective/"
                    f"{identity.workflow_cycle}.{identity.stage_cycle}",
                    request.proposal.objective_override,
                    trust=ContextTrust.ADMITTED_INPUT,
                )
            )
        return candidates


# The whole lane workspace is restored from the Snapshot (lanes map `/` to their root).
FORK_RESTORE_PATHS: tuple[str, ...] = ("/",)


def _purpose(candidates: Sequence[PackCandidate], default: ContextPurpose) -> ContextPurpose:
    """`fork` when a workspace restore rides the packet (exactly one is allowed)."""

    return (
        ContextPurpose.FORK
        if any(candidate.workspace is not None for candidate in candidates)
        else default
    )


def _text_candidate(
    kind: ContextSourceKind,
    ref: str,
    text: str,
    *,
    trust: ContextTrust = ContextTrust.AUTHORITATIVE,
    mandatory: bool = True,
    media_type: str = "text/markdown",
) -> PackCandidate:
    content = text.encode("utf-8")
    # Kernel-authored text is identified run-relatively (its `state://<run>/...` ref names the
    # run), so a fork over the same content seals the same packet digest (REQ-CP-EXEC-012).
    run_id = ref.removeprefix("state://").split("/", 1)[0] if ref.startswith("state://") else ""
    identity = text.replace(run_id, RUN_PLACEHOLDER) if run_id else text
    return PackCandidate(
        source_kind=kind,
        source_ref=ref,
        content_digest=bytes_digest(identity.encode("utf-8")),
        bytes=len(content),
        media_type=media_type,
        trust=trust,
        mandatory=mandatory,
        expand=ExpandMode.INLINE,
        text=text,
    )


def _blueprint_stage(blueprint: Mapping[str, object], stage_id: str) -> StageNode | None:
    stages = StageGraphBlueprint.model_validate(blueprint).stages
    return next((stage for stage in stages if stage.stage_id == stage_id), None)


def _packet_scope(request_scope: str) -> PacketScope:
    parts = request_scope.split("/")
    if len(parts) == 4 and parts[0] == "mc":
        return PacketScope(installation_id=parts[1], application_id=parts[2], tenant_id=parts[3])
    return PacketScope(installation_id=request_scope, application_id="-", tenant_id="-")


def _stable_uuid(kind: str, *parts: str) -> str:
    return str(uuid.uuid5(_PACKET_NAMESPACE, "\x1f".join((kind, *parts))))


def _canonical_text(value: object) -> str:
    return json.dumps(value, sort_keys=True, indent=2, ensure_ascii=False)


def _goal_contract_candidates(
    request: GoalOperationPreparationRequest,
    template: OperationExecutionRequest,
    *,
    role_root: str,
) -> list[PackCandidate]:
    role = request.operation_role
    revision = request.goal_revision
    identity = {
        "goal_revision_id": request.goal_revision_id,
        "goal_iteration": request.goal_iteration,
        "operation_role": role,
        "operation_attempt": request.operation_attempt,
    }
    duty = (
        "Advance the goal and finish with your typed executor observation and handoff."
        if role == "executor"
        else "Independently verify the executor's registered outputs against the goal; you "
        "see its outputs, never its reasoning."
    )
    contract = f"GoalDirected {role} operation. Identity: {identity!r}. {duty}"
    goals_lines = [f"Objective: {revision.objective}"]
    for title, values in (
        ("Tactics", revision.tactics),
        ("Subgoals", revision.subgoals),
        ("Coverage emphasis", revision.coverage_emphasis),
        ("Tactical changes", revision.tactical_changes),
        ("Unmet obligations", revision.unmet_obligations),
    ):
        if values:
            goals_lines.append(f"{title}: " + "; ".join(values))
    writable = ", ".join(template.workspace.exclusive_write_paths)
    workspace_map = (
        f"Writable (under {role_root}): {writable}. Read-only inputs: {role_root}/inputs/ "
        f"(see {role_root}/.mission/inputs.json). This index: {role_root}/.mission/context.md."
    )
    base = f"state://{request.run_id}/goal/{request.goal_iteration}/{role}"
    return [
        _text_candidate(
            ContextSourceKind.OPERATING_CONTRACT, f"{base}/operating_contract", contract
        ),
        _text_candidate(
            ContextSourceKind.GOALS_AND_CRITERIA,
            f"state://{request.run_id}/goal/revision/{request.goal_revision_id}",
            "\n".join(goals_lines),
            trust=ContextTrust.ADMITTED_INPUT,
        ),
        _text_candidate(ContextSourceKind.WORKSPACE_MAP, f"{base}/workspace_map", workspace_map),
    ]


def _progress_review(handoff: GoalHandoff) -> str:
    lines = [f"Continuation instructions: {handoff.continuation_instructions}"]
    if handoff.attempted_tactics:
        lines.append("Attempted tactics: " + "; ".join(handoff.attempted_tactics))
    if handoff.rejected_tactics:
        lines.append(
            "Rejected tactics: "
            + "; ".join(f"{tactic} ({reason})" for tactic, reason in handoff.rejected_tactics)
        )
    return "\n".join(lines)


def render_goal_handoff_markdown(handoff: GoalHandoff) -> str:
    """``HANDOFF.md``: the bound handoff as a readable snapshot (model content is data)."""

    def bullets(values: Sequence[str]) -> str:
        return "".join(f"- {value}\n" for value in values) or "- none\n"

    return (
        f"# Handoff from iteration {handoff.source_iteration.goal_iteration}\n\n"
        f"handoff: `{handoff.handoff_id}` (`{handoff.handoff_digest}`)\n\n"
        f"## Continuation instructions\n\n{handoff.continuation_instructions}\n\n"
        f"## Accepted facts\n\n{bullets(handoff.accepted_fact_refs)}\n"
        f"## Evidence\n\n{bullets(handoff.evidence_refs)}\n"
        f"## Artifacts\n\n{bullets(handoff.artifact_refs)}\n"
        f"## Attempted tactics\n\n{bullets(handoff.attempted_tactics)}\n"
        f"## Blockers\n\n{bullets(handoff.blockers)}\n"
        f"## Unresolved obligations\n\n{bullets(handoff.unresolved_obligations)}"
    )


def render_goal_checkpoint_json(handoff: GoalHandoff) -> str:
    """``checkpoint.json``: the bound handoff document, canonical JSON."""

    return json.dumps(asdict(handoff), sort_keys=True, indent=2, ensure_ascii=False) + "\n"


def context_packet_ref(segments: Sequence[PromptSegment]) -> str | None:
    """The packet a bound operation consumed, from its ``admitted_input`` packet segment."""

    return next(
        (
            segment.source_ref
            for segment in segments
            if segment.source_ref.startswith(CONTEXT_PACKET_SEGMENT_PREFIX)
        ),
        None,
    )


def goal_handoff_reference(
    handoff: GoalHandoff, *, request_scope: str
) -> GoalHandoffReference | None:
    """The runtime writer of ``GoalHandoffReference``: the packet the handoff's iteration read.

    ``_bind_handoff`` records the packet ref first in ``context_selection_refs``; the
    reference names that packet by digest under the iteration's handoff checkpoint key.
    """

    ref = next(
        (
            item
            for item in handoff.context_selection_refs
            if item.startswith(CONTEXT_PACKET_SEGMENT_PREFIX)
        ),
        None,
    )
    if ref is None:
        return None
    digest = ref.removeprefix(CONTEXT_PACKET_SEGMENT_PREFIX)
    return GoalHandoffReference(
        checkpoint=GoalHandoffCheckpointKey(
            request_scope=request_scope,
            belllabs_run_id=handoff.run_id,
            goal_handoff_checkpoint_id=f"context-packet:{digest.removeprefix('sha256:')}",
            goal_iteration=handoff.source_iteration.goal_iteration,
        ),
        artifact_ref=f"context-packet://{digest}",
        content_digest=digest,
    )


def chain_supply_from_packet(packet: ContextPacket) -> ChainSupply:
    """Re-express a sealed ``chain_link`` packet as packer inputs for the consumer's packet.

    Materialized items keep their bytes locator and file name but not their path, so the
    consumer's packer places them under its own mount root (``<root>/inputs/<binding>/``);
    inline and reference items keep their text, summary and retrieval instruction.
    """

    bindings: dict[str, ContextBinding] = {}
    candidates: list[PackCandidate] = []
    for item in packet.items:
        expand = {
            "inline": ExpandMode.INLINE,
            "reference": ExpandMode.REFERENCE,
            "materialize": ExpandMode.MATERIALIZE,
        }.get(item.tier.value, ExpandMode.REFERENCE)
        if item.binding_name is not None:
            bindings.setdefault(
                item.binding_name,
                ContextBinding(
                    binding_name=item.binding_name, expand=expand, mandatory=item.mandatory
                ),
            )
        text = item.inline.text if item.inline is not None else None
        summary = (
            item.reference.summary
            if item.reference is not None
            else item.materialize.summary
            if item.materialize is not None
            else None
        )
        candidates.append(
            PackCandidate(
                source_kind=item.source_kind,
                source_ref=item.source_ref,
                binding_name=item.binding_name,
                content_digest=item.content_digest,
                bytes=item.bytes,
                media_type=item.media_type,
                schema_ref=item.schema_ref,
                trust=item.trust,
                mandatory=item.mandatory,
                expand=expand,
                text=text,
                summary=summary or None,
                file_name=(
                    item.materialize.path.rsplit("/", 1)[-1]
                    if item.materialize is not None
                    else None
                ),
                durable_ref=item.materialize.durable_ref if item.materialize is not None else None,
                retrieval=item.reference.retrieval if item.reference is not None else None,
                provenance=item.provenance,
            )
        )
    return ChainSupply(bindings=tuple(bindings.values()), candidates=tuple(candidates))
