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

Entry points: :meth:`ContextPackService.pack_for_stage` (FT-B2). ``pack_for_iteration`` (FT-B3)
and ``pack_for_chain_link`` (FT-D2) build on :meth:`ContextPackService.seal`.
"""

from __future__ import annotations

import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Protocol

from mission_control.domain.authoring.contracts import StageGraphBlueprint, StageNode
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
from mission_control.domain.programs.contracts import (
    StageGraphAdmissionActivityRequest,
    StageInputBinding,
)

_PACKET_NAMESPACE = uuid.UUID("5b0f8a6e-2f7d-4c1a-9a3e-27c0c0de7a01")
DEFAULT_MAX_TEXT_BYTES = 65_536


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
    ) -> None:
        self._artifacts = artifacts
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
            purpose=ContextPurpose.STAGE_START,
        )
        candidates: list[PackCandidate] = [
            *self._stage_contract_candidates(request, template, mount_root=mount_root),
        ]
        bindings: dict[str, ContextBinding] = {}
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
