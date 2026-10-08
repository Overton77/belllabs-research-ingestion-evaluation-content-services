"""Context Packet (``mc.context_packet.v1``) and the deterministic Context Packer.

ADR-0027 and SPEC-02. Every handoff (stage to stage, iteration to iteration, session to
session, mission to mission) produces one sealed Context Packet and every attempt consumes
one. The packer here is a pure function: it performs no I/O, reads no clock and draws no
randomness. Capture (fetching bytes, computing summaries, resolving durable references,
choosing a token counter for the model profile's tokenizer) happens beforehand in the
application layer, which hands the packer already-captured candidates.

Public API (consumed by stage handoff, Goal Loop iterations, chains, the mailbox and forks):

- :func:`pack` builds a :class:`ContextPacket` or returns a :class:`PackFailure`.
- :func:`compute_budget` implements the budget arithmetic and raises
  :class:`ContextProfileError` for an invalid model profile.
- :func:`context_item_id` derives the stable item identity.
- :func:`packet_digest` recomputes the seal of a packet.
- :func:`index_row` renders the one index line the packer charges for every item; the
  renderers in :mod:`mission_control.domain.context.render` reuse it so the charged bytes and
  the rendered bytes are the same bytes.
"""

from __future__ import annotations

import json
import re
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any, Literal, Protocol, runtime_checkable

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

from mission_control.contracts.canonical import canonical_bytes, canonical_digest

PACKET_SCHEMA_VERSION: Literal["mc.context_packet.v1"] = "mc.context_packet.v1"
PACKER_VERSION = "mc.context_packer/1"
DIGEST_PATTERN = r"^sha256:[0-9a-f]{64}$"
RUN_PLACEHOLDER = "{run_id}"

DEFAULT_AUTO_INLINE_CAP = 4_000
DEFAULT_AUTO_MATERIALIZE_FLOOR = 65_536
DEFAULT_REFERENCE_SUMMARY_MAX_CHARS = 600
DEFAULT_MAX_PROMPT_CHARS = 90_000

_TEXT_MEDIA_TYPES = frozenset(
    {
        "application/json",
        "application/x-ndjson",
        "application/ld+json",
        "application/yaml",
        "application/x-yaml",
        "application/xml",
        "application/toml",
        "application/javascript",
        "application/sql",
    }
)
_FILE_NAME_SAFE = re.compile(r"[^A-Za-z0-9._-]+")
_SLUG_SAFE = re.compile(r"[^a-z0-9_-]+")


class _Contract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ContextPurpose(StrEnum):
    STAGE_START = "stage_start"
    ITERATION_START = "iteration_start"
    CONTINUATION = "continuation"
    FORK = "fork"
    CHAIN_LINK = "chain_link"
    FOLLOW_UP_TURN = "follow_up_turn"


class ContextSourceKind(StrEnum):
    ACCEPTED_OUTPUT = "accepted_output"
    LOOP_STATE = "loop_state"
    JOURNAL_DIGEST = "journal_digest"
    PROGRESS_REVIEW = "progress_review"
    HUMAN_ANSWER = "human_answer"
    BLOCKER = "blocker"
    CONTINUATION_CHECKPOINT = "continuation_checkpoint"
    CHAIN_SUPPLY = "chain_supply"
    CATALOG_CONTEXT = "catalog_context"
    OPERATING_CONTRACT = "operating_contract"
    GOALS_AND_CRITERIA = "goals_and_criteria"
    PENDING_COMMITMENTS = "pending_commitments"
    BUDGET_REMAINING = "budget_remaining"
    WORKSPACE_MAP = "workspace_map"
    QUEUED_INSTRUCTION = "queued_instruction"


class ContextTrust(StrEnum):
    AUTHORITATIVE = "authoritative"
    ADMITTED_INPUT = "admitted_input"
    UNTRUSTED_CONTENT = "untrusted_content"


class ExpansionTier(StrEnum):
    INLINE = "inline"
    REFERENCE = "reference"
    MATERIALIZE = "materialize"
    WORKSPACE = "workspace"


class ExpandMode(StrEnum):
    INLINE = "inline"
    REFERENCE = "reference"
    MATERIALIZE = "materialize"
    AUTO = "auto"


class RetrievalKind(StrEnum):
    READ_FILE = "read_file"
    TOOL_CALL = "tool_call"
    MISSIONCTL = "missionctl"
    MCP_RESOURCE = "mcp_resource"


class OmissionReason(StrEnum):
    BUDGET_EXHAUSTED = "budget_exhausted"
    TRUST_FILTERED = "trust_filtered"
    EXPIRED = "expired"
    DUPLICATE = "duplicate"
    NOT_ACCEPTED = "not_accepted"
    UNSUPPORTED_MEDIA = "unsupported_media"
    POLICY_DENIED = "policy_denied"


class TokenCounting(StrEnum):
    EXACT = "exact"
    CONSERVATIVE_BOUND = "conservative_bound"


class PackFailureCode(StrEnum):
    CONTEXT_BUDGET_EXCEEDED = "CONTEXT_BUDGET_EXCEEDED"
    """A mandatory item does not fit ``available_input``; nothing is truncated."""
    CONTEXT_MANDATORY_ITEM_UNAVAILABLE = "CONTEXT_MANDATORY_ITEM_UNAVAILABLE"
    """A mandatory item was filtered (not accepted, denied, expired, untrusted, media)."""
    CONTEXT_WORKSPACE_ITEM_INVALID = "CONTEXT_WORKSPACE_ITEM_INVALID"
    """Continuation and fork need exactly one workspace item; other purposes need none."""


# Deterministic ranking (SPEC-02 Packer algorithm step 5). Lower sorts first.
SOURCE_KIND_PRIORITY: dict[ContextSourceKind, int] = {
    kind: index
    for index, kind in enumerate(
        (
            ContextSourceKind.OPERATING_CONTRACT,
            ContextSourceKind.GOALS_AND_CRITERIA,
            ContextSourceKind.PENDING_COMMITMENTS,
            ContextSourceKind.BUDGET_REMAINING,
            ContextSourceKind.QUEUED_INSTRUCTION,
            ContextSourceKind.HUMAN_ANSWER,
            ContextSourceKind.BLOCKER,
            ContextSourceKind.PROGRESS_REVIEW,
            ContextSourceKind.LOOP_STATE,
            ContextSourceKind.ACCEPTED_OUTPUT,
            ContextSourceKind.CHAIN_SUPPLY,
            ContextSourceKind.CONTINUATION_CHECKPOINT,
            ContextSourceKind.JOURNAL_DIGEST,
            ContextSourceKind.CATALOG_CONTEXT,
            ContextSourceKind.WORKSPACE_MAP,
        )
    )
}
# accepted_output and chain_supply share one rank; binding declaration order separates them.
SOURCE_KIND_PRIORITY[ContextSourceKind.CHAIN_SUPPLY] = SOURCE_KIND_PRIORITY[
    ContextSourceKind.ACCEPTED_OUTPUT
]
_ACCEPTANCE_GATED = frozenset({ContextSourceKind.ACCEPTED_OUTPUT, ContextSourceKind.CHAIN_SUPPLY})
_WORKSPACE_PURPOSES = frozenset({ContextPurpose.CONTINUATION, ContextPurpose.FORK})


# --------------------------------------------------------------------------------------
# Contract: mc.context_packet.v1
# --------------------------------------------------------------------------------------


class PacketScope(_Contract):
    installation_id: str = Field(min_length=1)
    application_id: str = Field(min_length=1)
    tenant_id: str = Field(min_length=1)


class PacketTarget(_Contract):
    mission_id: str = Field(min_length=1)
    run_id: str = Field(min_length=1)
    revision_id: str = Field(min_length=1)
    node_key: str = Field(min_length=1)
    activation_id: str = Field(min_length=1)
    attempt_no: int = Field(ge=1)
    generation: int = Field(ge=0)
    purpose: ContextPurpose


class PacketBudget(_Contract):
    model_profile_ref: str = Field(min_length=1)
    tokenizer_ref: str = Field(min_length=1)
    context_window: int = Field(ge=0)
    reserved_output: int = Field(ge=0)
    fixed_overhead: int = Field(ge=0)
    control_reserve: int = Field(ge=0)
    safety_margin: int = Field(ge=0)
    available_input: int = Field(ge=0)
    inline_allocated: int = Field(ge=0)
    inline_remaining: int = Field(ge=0)
    counting: TokenCounting

    @model_validator(mode="after")
    def arithmetic_holds(self) -> PacketBudget:
        expected = (
            self.context_window
            - self.reserved_output
            - self.fixed_overhead
            - self.control_reserve
            - self.safety_margin
        )
        if self.available_input != expected:
            raise ValueError("available_input does not match the budget arithmetic")
        if self.inline_allocated + self.inline_remaining != self.available_input:
            raise ValueError("inline allocation does not add up to available_input")
        return self


class ToolRetrieval(_Contract):
    name: str = Field(min_length=1)
    args_digest: str = Field(pattern=DIGEST_PATTERN)
    args_excerpt: str = Field(default="", max_length=500)


class RetrievalInstruction(_Contract):
    """How an agent fetches a referenced item (RetrievalInstruction@1)."""

    kind: RetrievalKind
    path: str | None = None
    tool: ToolRetrieval | None = None
    command: str | None = None
    resource_uri: str | None = None
    range_hint: str | None = Field(default=None, max_length=200)

    @model_validator(mode="after")
    def exactly_the_kind_field(self) -> RetrievalInstruction:
        present = {
            RetrievalKind.READ_FILE: self.path,
            RetrievalKind.TOOL_CALL: self.tool,
            RetrievalKind.MISSIONCTL: self.command,
            RetrievalKind.MCP_RESOURCE: self.resource_uri,
        }
        if present[self.kind] is None:
            raise ValueError(f"{self.kind.value} retrieval requires its locator")
        if any(value is not None for kind, value in present.items() if kind != self.kind):
            raise ValueError("retrieval declares a locator of another kind")
        return self


class InlineBody(_Contract):
    text: str
    tokens: int = Field(ge=0)
    """Tokens charged: the item's index row plus its fenced inline block."""


class ReferenceBody(_Contract):
    summary: str = Field(max_length=2_000)
    tokens: int = Field(ge=0)
    """Tokens charged: the item's index row (summary and retrieval included)."""
    retrieval: RetrievalInstruction


class MaterializeBody(_Contract):
    path: str = Field(pattern=r"^/[A-Za-z0-9._/-]+$")
    mode: Literal["read_only"] = "read_only"
    manifest_entry_ref: str = Field(min_length=1)
    durable_ref: str = Field(min_length=1)
    """Byte locator the workspace materializer fetches and digest-verifies."""
    summary: str = Field(default="", max_length=2_000)
    tokens: int = Field(ge=0)
    """Tokens charged: the item's index row (path, digest, size, summary)."""


class WorkspaceBody(_Contract):
    snapshot_ref: str = Field(min_length=1)
    restore_paths: tuple[str, ...] = Field(min_length=1)
    tokens: int = Field(ge=0)
    """Tokens charged: the item's index row."""


class ItemProvenance(_Contract):
    producer_activation_id: str | None = None
    producer_generation: int | None = Field(default=None, ge=0)
    accepted_decision_ref: str | None = None
    iteration_id: str | None = None
    chain_link_id: str | None = None
    provisional: bool = False


class ContextItem(_Contract):
    """One item of a packet (ContextItem@1)."""

    item_id: str = Field(pattern=DIGEST_PATTERN)
    binding_name: str | None = None
    source_kind: ContextSourceKind
    source_ref: str = Field(min_length=1)
    content_digest: str = Field(pattern=DIGEST_PATTERN)
    bytes: int = Field(ge=0)
    media_type: str = Field(min_length=1)
    schema_ref: str | None = None
    trust: ContextTrust
    tier: ExpansionTier
    mandatory: bool
    inline: InlineBody | None = None
    reference: ReferenceBody | None = None
    materialize: MaterializeBody | None = None
    workspace: WorkspaceBody | None = None
    provenance: ItemProvenance = ItemProvenance()

    @model_validator(mode="after")
    def tier_body_matches(self) -> ContextItem:
        bodies = {
            ExpansionTier.INLINE: self.inline,
            ExpansionTier.REFERENCE: self.reference,
            ExpansionTier.MATERIALIZE: self.materialize,
            ExpansionTier.WORKSPACE: self.workspace,
        }
        if bodies[self.tier] is None:
            raise ValueError(f"{self.tier.value} item requires its {self.tier.value} body")
        if any(body is not None for tier, body in bodies.items() if tier != self.tier):
            raise ValueError("item carries a body of another tier")
        if self.mandatory and self.provenance.provisional:
            raise ValueError("a provisional item can never be mandatory")
        return self

    @property
    def charged_tokens(self) -> int:
        body = self.inline or self.reference or self.materialize or self.workspace
        assert body is not None  # guaranteed by tier_body_matches
        return body.tokens


class OmittedItem(_Contract):
    """A candidate that did not enter the packet at its requested tier (OmittedItem@1)."""

    item_id: str = Field(pattern=DIGEST_PATTERN)
    source_kind: ContextSourceKind
    source_ref: str = Field(min_length=1)
    content_digest: str = Field(pattern=DIGEST_PATTERN)
    bytes: int = Field(ge=0)
    reason: OmissionReason
    downgraded_to: Literal["reference", "none"] = "none"


_DIGEST_EXCLUDED_FIELDS = frozenset({"packet_id", "sealed_at", "context_selection_ref"})


class ContextPacket(_Contract):
    """The sealed handoff bundle (ContextPacket@1). Immutable once sealed."""

    schema_version: Literal["mc.context_packet.v1"] = PACKET_SCHEMA_VERSION
    packet_id: str = Field(min_length=1)
    packer_version: str = Field(min_length=1)
    scope: PacketScope
    target: PacketTarget
    producer_refs: tuple[str, ...] = ()
    budget: PacketBudget
    items: tuple[ContextItem, ...]
    omitted: tuple[OmittedItem, ...] = ()
    mandatory_item_ids: tuple[str, ...] = ()
    workspace_snapshot_ref: str | None = None
    packet_digest: str = Field(pattern=DIGEST_PATTERN)
    sealed_at: AwareDatetime
    context_selection_ref: str = Field(min_length=1)

    @model_validator(mode="after")
    def sealed_and_consistent(self) -> ContextPacket:
        item_ids = [item.item_id for item in self.items]
        if len(item_ids) != len(set(item_ids)):
            raise ValueError("packet items require unique item ids")
        run_id = self.target.run_id
        if any(
            item.item_id
            != context_item_id(item.source_kind, item.source_ref, item.binding_name, run_id=run_id)
            for item in self.items
        ):
            raise ValueError("item_id does not match its source identity")
        mandatory = tuple(item.item_id for item in self.items if item.mandatory)
        if self.mandatory_item_ids != mandatory:
            raise ValueError("mandatory_item_ids must list the mandatory items in packet order")
        if sum(item.charged_tokens for item in self.items) != self.budget.inline_allocated:
            raise ValueError("budget.inline_allocated does not match the items' charged tokens")
        snapshots = [item.workspace.snapshot_ref for item in self.items if item.workspace]
        if len(snapshots) > 1:
            raise ValueError("a packet holds at most one workspace item")
        if self.workspace_snapshot_ref != (snapshots[0] if snapshots else None):
            raise ValueError("workspace_snapshot_ref must name the workspace item's snapshot")
        if packet_digest(self) != self.packet_digest:
            raise ValueError("packet_digest does not match the packet content")
        return self


def packet_digest(packet: ContextPacket | dict[str, object]) -> str:
    """Seal digest over canonical JSON of every field but id, seal time and selection ref.

    Run-relative: every occurrence of the target's ``run_id`` is normalized to ``{run_id}``
    before hashing, so a fork's packet over the same content has the same digest as its
    source's (REQ-CP-EXEC-012 reuse compatibility compares bindings modulo the run id).
    """

    if isinstance(packet, ContextPacket):
        fields: dict[str, object] = {
            name: getattr(packet, name)
            for name in type(packet).model_fields
            if name not in _DIGEST_EXCLUDED_FIELDS and name != "packet_digest"
        }
    else:
        fields = {
            name: value
            for name, value in packet.items()
            if name not in _DIGEST_EXCLUDED_FIELDS and name != "packet_digest"
        }
    target = fields.get("target")
    run_id = (
        target.run_id
        if isinstance(target, PacketTarget)
        else str(target.get("run_id", ""))
        if isinstance(target, dict)
        else ""
    )
    return canonical_digest(run_relative(json.loads(canonical_bytes(fields)), run_id))


def run_relative(value: Any, run_id: str) -> Any:
    """Replace ``run_id`` inside every string of a JSON value by ``{run_id}``."""

    if not run_id:
        return value
    if isinstance(value, str):
        return value.replace(run_id, RUN_PLACEHOLDER)
    if isinstance(value, dict):
        return {key: run_relative(item, run_id) for key, item in value.items()}
    if isinstance(value, list):
        return [run_relative(item, run_id) for item in value]
    return value


def context_item_id(
    source_kind: ContextSourceKind,
    source_ref: str,
    binding_name: str | None,
    *,
    run_id: str = "",
) -> str:
    """Stable item identity: sha256 over (source_kind, run-relative source_ref, binding_name)."""

    return canonical_digest(
        {
            "source_kind": source_kind.value,
            "source_ref": run_relative(source_ref, run_id),
            "binding_name": binding_name,
        }
    )


# --------------------------------------------------------------------------------------
# Packer inputs
# --------------------------------------------------------------------------------------


class ContextProfileError(ValueError):
    """A model profile whose budget terms are negative or leave no input budget."""


class ModelBudgetProfile(_Contract):
    """The budget-relevant terms of a model profile (CONTEXT-STATE-AND-CONTROL budgets).

    Terms are plain integers on purpose: :func:`compute_budget` rejects a negative term with
    the typed :class:`ContextProfileError` instead of a generic validation error.
    """

    model_profile_ref: str = Field(min_length=1)
    tokenizer_ref: str = Field(min_length=1)
    context_window: int
    reserved_output: int
    system_prompt_tokens: int = 0
    tool_schema_allowance: int = 0
    skills_metadata_tokens: int = 0
    control_reserve: int = 0
    safety_margin: int = 0


@dataclass(frozen=True, slots=True)
class BudgetArithmetic:
    context_window: int
    reserved_output: int
    fixed_overhead: int
    control_reserve: int
    safety_margin: int
    available_input: int


def compute_budget(profile: ModelBudgetProfile) -> BudgetArithmetic:
    """``available_input = window - reserved_output - fixed_overhead - control - margin``."""

    terms = {
        "context_window": profile.context_window,
        "reserved_output": profile.reserved_output,
        "system_prompt_tokens": profile.system_prompt_tokens,
        "tool_schema_allowance": profile.tool_schema_allowance,
        "skills_metadata_tokens": profile.skills_metadata_tokens,
        "control_reserve": profile.control_reserve,
        "safety_margin": profile.safety_margin,
    }
    negative = sorted(name for name, value in terms.items() if value < 0)
    if negative:
        raise ContextProfileError(
            f"model profile {profile.model_profile_ref} has negative budget terms: "
            + ", ".join(negative)
        )
    fixed_overhead = (
        profile.system_prompt_tokens
        + profile.tool_schema_allowance
        + profile.skills_metadata_tokens
    )
    available = (
        profile.context_window
        - profile.reserved_output
        - fixed_overhead
        - profile.control_reserve
        - profile.safety_margin
    )
    if available < 0:
        raise ContextProfileError(
            f"model profile {profile.model_profile_ref} leaves negative available_input "
            f"({available})"
        )
    return BudgetArithmetic(
        context_window=profile.context_window,
        reserved_output=profile.reserved_output,
        fixed_overhead=fixed_overhead,
        control_reserve=profile.control_reserve,
        safety_margin=profile.safety_margin,
        available_input=available,
    )


@runtime_checkable
class TokenCounter(Protocol):
    """Deterministic token counter for one tokenizer. ``exact`` is false for a bound."""

    @property
    def exact(self) -> bool: ...

    def count(self, text: str) -> int: ...


class ConservativeTokenCounter:
    """Upper bound used when the tokenizer is unknown: ``ceil(utf8_bytes / 2.5)``."""

    exact = False

    def count(self, text: str) -> int:
        size = len(text.encode("utf-8"))
        return (2 * size + 4) // 5  # integer ceil(size / 2.5); never zero for non-empty text


class ContextPackPolicy(_Contract):
    """Packer policy defaults; SPEC-05 overrides them per node."""

    auto_inline_cap: int = Field(default=DEFAULT_AUTO_INLINE_CAP, ge=0)
    auto_materialize_floor: int = Field(default=DEFAULT_AUTO_MATERIALIZE_FLOOR, ge=0)
    reference_summary_max_chars: int = Field(
        default=DEFAULT_REFERENCE_SUMMARY_MAX_CHARS, ge=16, le=2_000
    )
    max_prompt_chars: int = Field(default=DEFAULT_MAX_PROMPT_CHARS, ge=1_000, le=95_000)
    """Character ceiling on the rendered index rows and inline blocks (the prompt segment
    contract caps content at 100,000 characters)."""
    allowed_trust: tuple[ContextTrust, ...] = (
        ContextTrust.AUTHORITATIVE,
        ContextTrust.ADMITTED_INPUT,
        ContextTrust.UNTRUSTED_CONTENT,
    )


class LaneFileSupport(_Contract):
    """The only lane facts the packer knows (SPEC-02 Further Notes)."""

    writable_workspace: bool = True
    text_only_files: bool = False
    """True for a text-only backend such as Deep Agents ``StateBackend``."""
    mount_root: str = Field(default="", pattern=r"^(/[A-Za-z0-9._-]+)*$")
    """Prefix of default materialization paths (a GoalDirected role root, for example)."""


class ContextBinding(_Contract):
    """A consumer input binding in declaration order."""

    binding_name: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")
    expand: ExpandMode = ExpandMode.AUTO
    mandatory: bool = False


class WorkspaceRestore(_Contract):
    snapshot_ref: str = Field(min_length=1)
    restore_paths: tuple[str, ...] = Field(min_length=1)


class PackCandidate(_Contract):
    """One already-captured candidate. Text and summaries are captured, never generated."""

    source_kind: ContextSourceKind
    source_ref: str = Field(min_length=1)
    binding_name: str | None = None
    content_digest: str = Field(pattern=DIGEST_PATTERN)
    bytes: int = Field(ge=0)
    media_type: str = Field(min_length=1)
    schema_ref: str | None = None
    trust: ContextTrust
    mandatory: bool = False
    expand: ExpandMode | None = None
    """Used when no binding of ``binding_name`` exists; bindings take precedence, except
    that ``reference`` here always wins (capture found nothing to inline or materialize)."""
    text: str | None = None
    """Pre-fetched text; required for an inline tier."""
    summary: str | None = None
    file_name: str | None = None
    path: str | None = Field(default=None, pattern=r"^/[A-Za-z0-9._/-]+$")
    """Explicit materialization path (for example ``/goal/HANDOFF.md``)."""
    durable_ref: str | None = None
    """Byte locator for materialization; defaults to ``source_ref``."""
    retrieval: RetrievalInstruction | None = None
    workspace: WorkspaceRestore | None = None
    provenance: ItemProvenance = ItemProvenance()
    grant_scope_refs: tuple[str, ...] | None = None
    """None means kernel-authored with no grant restriction."""
    expires_at: AwareDatetime | None = None


class PackRequest(_Contract):
    packet_id: str = Field(min_length=1)
    sealed_at: AwareDatetime
    """Seal instant, supplied by the caller; also the instant expiry is evaluated at."""
    context_selection_ref: str = Field(min_length=1)
    scope: PacketScope
    target: PacketTarget
    producer_refs: tuple[str, ...] = ()
    profile: ModelBudgetProfile
    bindings: tuple[ContextBinding, ...] = ()
    candidates: tuple[PackCandidate, ...] = ()
    policy: ContextPackPolicy = ContextPackPolicy()
    lane: LaneFileSupport = LaneFileSupport()
    consumer_scope_ref: str | None = None
    packer_version: str = PACKER_VERSION

    @model_validator(mode="after")
    def unique_bindings(self) -> PackRequest:
        names = [binding.binding_name for binding in self.bindings]
        if len(names) != len(set(names)):
            raise ValueError("consumer bindings must have unique names")
        return self


class PackFailure(_Contract):
    code: PackFailureCode
    message: str = Field(min_length=1)
    item_id: str | None = None
    source_ref: str | None = None
    required_tokens: int | None = None
    available_tokens: int | None = None


# --------------------------------------------------------------------------------------
# Packer
# --------------------------------------------------------------------------------------


def is_text_media(media_type: str) -> bool:
    base = media_type.split(";", 1)[0].strip().lower()
    return base.startswith("text/") or base in _TEXT_MEDIA_TYPES or base.endswith(("+json", "+xml"))


@dataclass(frozen=True, slots=True)
class _Prepared:
    candidate: PackCandidate
    item_id: str
    expand: ExpandMode
    mandatory: bool
    binding_index: int
    candidate_digest: str

    @property
    def sort_key(self) -> tuple[int, int, int, str, str, str, str]:
        # No timestamps and no random tie-breaks: the full candidate digest closes the order.
        return (
            0 if self.mandatory else 1,
            SOURCE_KIND_PRIORITY[self.candidate.source_kind],
            self.binding_index,
            self.candidate.content_digest,
            self.candidate.source_ref,
            self.item_id,
            self.candidate_digest,
        )


class _Fail(Exception):
    def __init__(self, failure: PackFailure) -> None:
        super().__init__(failure.message)
        self.failure = failure


@dataclass(slots=True)
class _Allocator:
    available: int
    max_chars: int
    allocated: int = 0
    chars: int = 0

    @property
    def remaining(self) -> int:
        return self.available - self.allocated

    def fits(self, tokens: int, chars: int) -> bool:
        return tokens <= self.remaining and self.chars + chars <= self.max_chars

    def charge(self, tokens: int, chars: int) -> None:
        self.allocated += tokens
        self.chars += chars


def pack(request: PackRequest, counter: TokenCounter | None = None) -> ContextPacket | PackFailure:
    """Build and seal a Context Packet; pure and deterministic over ``request`` and ``counter``.

    Raises :class:`ContextProfileError` for an invalid model profile and ``ValueError`` for a
    malformed request (for example an inline candidate without captured text).
    """

    budget = compute_budget(request.profile)
    token_counter: TokenCounter = counter or ConservativeTokenCounter()
    try:
        return _Packer(request, budget, token_counter).run()
    except _Fail as failure:
        return failure.failure


class _Packer:
    def __init__(
        self, request: PackRequest, budget: BudgetArithmetic, counter: TokenCounter
    ) -> None:
        self.request = request
        self.policy = request.policy
        self.lane = request.lane
        self.budget = budget
        self.counter = counter
        self.allocator = _Allocator(budget.available_input, request.policy.max_prompt_chars)
        self.items: list[ContextItem] = []
        self.omitted: list[OmittedItem] = []
        self.paths: set[str] = set()

    # -- steps ----------------------------------------------------------------------

    def run(self) -> ContextPacket:
        prepared = sorted(self._prepare(), key=lambda entry: entry.sort_key)
        admitted = self._filter(prepared)
        self._check_workspace(admitted)
        for entry in admitted:
            self._allocate(entry)
        return self._seal()

    def _prepare(self) -> Iterable[_Prepared]:
        bindings = {binding.binding_name: binding for binding in self.request.bindings}
        order = {binding.binding_name: index for index, binding in enumerate(self.request.bindings)}
        for candidate in self.request.candidates:
            binding = bindings.get(candidate.binding_name) if candidate.binding_name else None
            # Bindings choose the tier, except that capture may pin a candidate to reference
            # (it has no bytes the lane could materialize or inline).
            expand = (
                ExpandMode.REFERENCE
                if candidate.expand == ExpandMode.REFERENCE
                else binding.expand
                if binding is not None
                else candidate.expand
                if candidate.expand is not None
                else ExpandMode.AUTO
            )
            mandatory = (candidate.mandatory or (binding is not None and binding.mandatory)) and (
                not candidate.provenance.provisional
            )
            yield _Prepared(
                candidate=candidate,
                item_id=context_item_id(
                    candidate.source_kind,
                    candidate.source_ref,
                    candidate.binding_name,
                    run_id=self.request.target.run_id,
                ),
                expand=expand,
                mandatory=mandatory,
                binding_index=order.get(candidate.binding_name or "", len(order)),
                candidate_digest=canonical_digest(candidate),
            )

    def _filter(self, prepared: Sequence[_Prepared]) -> list[_Prepared]:
        admitted: list[_Prepared] = []
        seen: set[str] = set()
        for entry in prepared:
            candidate = entry.candidate
            reason: OmissionReason | None = None
            # 1. authorize
            if (
                candidate.grant_scope_refs is not None
                and self.request.consumer_scope_ref not in candidate.grant_scope_refs
            ):
                reason = OmissionReason.POLICY_DENIED
            # 2. accept-filter
            elif (
                candidate.source_kind in _ACCEPTANCE_GATED
                and candidate.provenance.accepted_decision_ref is None
                and not candidate.provenance.provisional
            ):
                reason = OmissionReason.NOT_ACCEPTED
            # 3. dedupe (sorted order makes the surviving duplicate deterministic)
            elif entry.item_id in seen:
                reason = OmissionReason.DUPLICATE
            # 4. trust and expiry
            elif candidate.trust not in self.policy.allowed_trust:
                reason = OmissionReason.TRUST_FILTERED
            elif (
                candidate.expires_at is not None and candidate.expires_at <= self.request.sealed_at
            ):
                reason = OmissionReason.EXPIRED
            if reason is None:
                seen.add(entry.item_id)
                admitted.append(entry)
                continue
            if entry.mandatory and reason != OmissionReason.DUPLICATE:
                raise _Fail(
                    PackFailure(
                        code=PackFailureCode.CONTEXT_MANDATORY_ITEM_UNAVAILABLE,
                        message=(
                            f"mandatory context item {candidate.source_ref} is unavailable: "
                            f"{reason.value}"
                        ),
                        item_id=entry.item_id,
                        source_ref=candidate.source_ref,
                    )
                )
            self._omit(entry, reason, "none")
        return admitted

    def _check_workspace(self, admitted: Sequence[_Prepared]) -> None:
        workspace = [entry for entry in admitted if entry.candidate.workspace is not None]
        purpose = self.request.target.purpose
        expected = 1 if purpose in _WORKSPACE_PURPOSES else 0
        if len(workspace) != expected:
            raise _Fail(
                PackFailure(
                    code=PackFailureCode.CONTEXT_WORKSPACE_ITEM_INVALID,
                    message=(
                        f"purpose {purpose.value} requires exactly {expected} workspace item(s), "
                        f"got {len(workspace)}"
                    ),
                )
            )

    def _allocate(self, entry: _Prepared) -> None:
        candidate = entry.candidate
        if candidate.workspace is not None:
            item = self._workspace_item(entry, candidate.workspace)
            self._admit_or_fail(entry, item)
            return
        tier, early_reason = self._desired_tier(entry)
        if tier == ExpansionTier.INLINE:
            item = self._inline_item(entry)
            if self._try_admit(item):
                return
            if entry.mandatory:
                self._fail_budget(entry, item)
            self._fallback_reference(entry, early_reason or OmissionReason.BUDGET_EXHAUSTED)
            return
        if tier == ExpansionTier.MATERIALIZE:
            item = self._materialize_item(entry)
            if self._try_admit(item):
                return
            if entry.mandatory:
                self._fail_budget(entry, item)
            self._fallback_reference(entry, early_reason or OmissionReason.BUDGET_EXHAUSTED)
            return
        # reference tier, possibly downgraded already for media or budget reasons
        if early_reason is not None:
            self._fallback_reference(entry, early_reason)
            return
        item = self._reference_item(entry)
        if self._try_admit(item):
            return
        if entry.mandatory:
            self._fail_budget(entry, item)
        self._omit(entry, OmissionReason.BUDGET_EXHAUSTED, "none")

    def _desired_tier(self, entry: _Prepared) -> tuple[ExpansionTier, OmissionReason | None]:
        """Return the target tier and, for a downgrade decided up front, its reason."""

        candidate = entry.candidate
        textual = is_text_media(candidate.media_type)
        can_materialize = self.lane.writable_workspace and (
            textual or not self.lane.text_only_files
        )
        expand = entry.expand
        if entry.mandatory and expand == ExpandMode.AUTO:
            # Mandatory items are never references for budget reasons: inline when textual,
            # else a file; a mandatory item fitting neither cannot be delivered.
            if candidate.text is not None and textual:
                return ExpansionTier.INLINE, None
            if can_materialize:
                return ExpansionTier.MATERIALIZE, None
            self._fail_unavailable(entry, OmissionReason.UNSUPPORTED_MEDIA)
        if expand == ExpandMode.INLINE:
            if not textual:
                if entry.mandatory:
                    self._fail_unavailable(entry, OmissionReason.UNSUPPORTED_MEDIA)
                return ExpansionTier.REFERENCE, OmissionReason.UNSUPPORTED_MEDIA
            if candidate.text is None:
                raise ValueError(
                    f"inline context candidate {candidate.source_ref} has no captured text"
                )
            return ExpansionTier.INLINE, None
        if expand == ExpandMode.MATERIALIZE:
            if not can_materialize:
                if entry.mandatory:
                    self._fail_unavailable(entry, OmissionReason.UNSUPPORTED_MEDIA)
                return ExpansionTier.REFERENCE, OmissionReason.UNSUPPORTED_MEDIA
            return ExpansionTier.MATERIALIZE, None
        if expand == ExpandMode.REFERENCE:
            return ExpansionTier.REFERENCE, None
        # auto
        budget_reason: OmissionReason | None = None
        if candidate.text is not None and textual:
            tokens = self.counter.count(candidate.text)
            if tokens <= self.policy.auto_inline_cap:
                inline = self._inline_item(entry)
                if self.allocator.fits(inline.charged_tokens, _item_chars(inline)):
                    return ExpansionTier.INLINE, None
                budget_reason = OmissionReason.BUDGET_EXHAUSTED
        wants_file = (not textual) or candidate.bytes > self.policy.auto_materialize_floor
        if wants_file and can_materialize:
            return ExpansionTier.MATERIALIZE, None
        if wants_file and self.lane.writable_workspace and not textual:
            # a binary file on a text-only backend
            return ExpansionTier.REFERENCE, OmissionReason.UNSUPPORTED_MEDIA
        return ExpansionTier.REFERENCE, budget_reason

    # -- item construction ----------------------------------------------------------

    def _base(self, entry: _Prepared, tier: ExpansionTier) -> dict[str, object]:
        candidate = entry.candidate
        return {
            "item_id": entry.item_id,
            "binding_name": candidate.binding_name,
            "source_kind": candidate.source_kind,
            "source_ref": candidate.source_ref,
            "content_digest": candidate.content_digest,
            "bytes": candidate.bytes,
            "media_type": candidate.media_type,
            "schema_ref": candidate.schema_ref,
            "trust": candidate.trust,
            "tier": tier,
            "mandatory": entry.mandatory,
            "provenance": candidate.provenance,
        }

    def _priced(self, build: Callable[[int], ContextItem]) -> ContextItem:
        """Build an item, render its row and inline block, then charge exactly those bytes."""

        draft = build(0)
        rendered = index_row(draft) + (inline_block(draft) if draft.inline else "")
        return build(self.counter.count(rendered))

    def _inline_item(self, entry: _Prepared) -> ContextItem:
        text = entry.candidate.text
        assert text is not None
        base = self._base(entry, ExpansionTier.INLINE)
        return self._priced(
            lambda tokens: ContextItem.model_validate(
                {**base, "inline": InlineBody(text=text, tokens=tokens)}
            )
        )

    def _reference_item(self, entry: _Prepared) -> ContextItem:
        candidate = entry.candidate
        base = self._base(entry, ExpansionTier.REFERENCE)
        summary = _bounded_summary(candidate, self.policy.reference_summary_max_chars)
        retrieval = candidate.retrieval or default_retrieval(
            candidate.source_kind, candidate.source_ref
        )
        return self._priced(
            lambda tokens: ContextItem.model_validate(
                {
                    **base,
                    "reference": ReferenceBody(summary=summary, tokens=tokens, retrieval=retrieval),
                }
            )
        )

    def _materialize_item(self, entry: _Prepared) -> ContextItem:
        candidate = entry.candidate
        base = self._base(entry, ExpansionTier.MATERIALIZE)
        path = self._materialize_path(entry)
        summary = _bounded_summary(candidate, self.policy.reference_summary_max_chars)
        durable_ref = candidate.durable_ref or candidate.source_ref
        return self._priced(
            lambda tokens: ContextItem.model_validate(
                {
                    **base,
                    "materialize": MaterializeBody(
                        path=path,
                        manifest_entry_ref=f".mission/inputs.json#{entry.item_id}",
                        durable_ref=durable_ref,
                        summary=summary,
                        tokens=tokens,
                    ),
                }
            )
        )

    def _workspace_item(self, entry: _Prepared, restore: WorkspaceRestore) -> ContextItem:
        base = {**self._base(entry, ExpansionTier.WORKSPACE), "mandatory": True}
        return self._priced(
            lambda tokens: ContextItem.model_validate(
                {
                    **base,
                    "workspace": WorkspaceBody(
                        snapshot_ref=restore.snapshot_ref,
                        restore_paths=restore.restore_paths,
                        tokens=tokens,
                    ),
                }
            )
        )

    def _materialize_path(self, entry: _Prepared) -> str:
        candidate = entry.candidate
        if candidate.path is not None:
            path = candidate.path
        else:
            folder = _sanitize_segment(candidate.binding_name or candidate.source_kind.value)
            name = _sanitize_segment(
                candidate.file_name or candidate.source_ref.rstrip("/").rsplit("/", 1)[-1]
            )
            path = f"{self.lane.mount_root}/inputs/{folder}/{name}"
        if path in self.paths:
            stem, dot, extension = path.rpartition(".")
            suffix = entry.item_id.removeprefix("sha256:")[:12]
            path = (
                f"{stem}-{suffix}.{extension}"
                if dot and "/" not in extension
                else f"{path}-{suffix}"
            )
        return path

    # -- admission ------------------------------------------------------------------

    def _try_admit(self, item: ContextItem) -> bool:
        chars = _item_chars(item)
        if not self.allocator.fits(item.charged_tokens, chars):
            return False
        self.allocator.charge(item.charged_tokens, chars)
        self.items.append(item)
        if item.materialize is not None:
            self.paths.add(item.materialize.path)
        return True

    def _admit_or_fail(self, entry: _Prepared, item: ContextItem) -> None:
        if not self._try_admit(item):
            self._fail_budget(entry, item)

    def _fallback_reference(self, entry: _Prepared, reason: OmissionReason) -> None:
        item = self._reference_item(entry)
        if self._try_admit(item):
            self._omit(entry, reason, "reference")
        else:
            self._omit(entry, reason, "none")

    def _omit(
        self, entry: _Prepared, reason: OmissionReason, downgraded_to: Literal["reference", "none"]
    ) -> None:
        candidate = entry.candidate
        self.omitted.append(
            OmittedItem(
                item_id=entry.item_id,
                source_kind=candidate.source_kind,
                source_ref=candidate.source_ref,
                content_digest=candidate.content_digest,
                bytes=candidate.bytes,
                reason=reason,
                downgraded_to=downgraded_to,
            )
        )

    def _fail_budget(self, entry: _Prepared, item: ContextItem) -> None:
        raise _Fail(
            PackFailure(
                code=PackFailureCode.CONTEXT_BUDGET_EXCEEDED,
                message=(
                    f"mandatory context item {entry.candidate.source_ref} needs "
                    f"{item.charged_tokens} tokens but {self.allocator.remaining} remain "
                    f"of {self.allocator.available}"
                ),
                item_id=entry.item_id,
                source_ref=entry.candidate.source_ref,
                required_tokens=item.charged_tokens,
                available_tokens=self.allocator.remaining,
            )
        )

    def _fail_unavailable(self, entry: _Prepared, reason: OmissionReason) -> None:
        raise _Fail(
            PackFailure(
                code=PackFailureCode.CONTEXT_MANDATORY_ITEM_UNAVAILABLE,
                message=(
                    f"mandatory context item {entry.candidate.source_ref} cannot be delivered "
                    f"on this lane: {reason.value}"
                ),
                item_id=entry.item_id,
                source_ref=entry.candidate.source_ref,
            )
        )

    # -- seal -----------------------------------------------------------------------

    def _seal(self) -> ContextPacket:
        request = self.request
        items = tuple(self.items)
        workspace = next((item for item in items if item.workspace is not None), None)
        counting = TokenCounting.EXACT if self.counter.exact else TokenCounting.CONSERVATIVE_BOUND
        budget = PacketBudget(
            model_profile_ref=request.profile.model_profile_ref,
            tokenizer_ref=request.profile.tokenizer_ref,
            context_window=self.budget.context_window,
            reserved_output=self.budget.reserved_output,
            fixed_overhead=self.budget.fixed_overhead,
            control_reserve=self.budget.control_reserve,
            safety_margin=self.budget.safety_margin,
            available_input=self.budget.available_input,
            inline_allocated=self.allocator.allocated,
            inline_remaining=self.allocator.remaining,
            counting=counting,
        )
        body: dict[str, object] = {
            "schema_version": PACKET_SCHEMA_VERSION,
            "packer_version": request.packer_version,
            "scope": request.scope,
            "target": request.target,
            "producer_refs": request.producer_refs,
            "budget": budget,
            "items": items,
            "omitted": tuple(self.omitted),
            "mandatory_item_ids": tuple(item.item_id for item in items if item.mandatory),
            "workspace_snapshot_ref": workspace.workspace.snapshot_ref
            if workspace is not None and workspace.workspace is not None
            else None,
        }
        return ContextPacket.model_validate(
            {
                **body,
                "packet_id": request.packet_id,
                "sealed_at": request.sealed_at,
                "context_selection_ref": request.context_selection_ref,
                "packet_digest": packet_digest(body),
            }
        )


# --------------------------------------------------------------------------------------
# Shared rendering primitives (charged by the packer, reused by the renderers)
# --------------------------------------------------------------------------------------


def default_retrieval(source_kind: ContextSourceKind, source_ref: str) -> RetrievalInstruction:
    """Retrieval chosen by the reference's scheme when capture supplied none."""

    scheme = source_ref.split("://", 1)[0] if "://" in source_ref else ""
    command = {
        "artifact": f"missionctl artifact get {source_ref}",
        "workspace-candidate": f"missionctl artifact get {source_ref}",
        "journal": f"missionctl journal read {source_ref}",
        "checkpoint": f"missionctl run checkpoint --get {source_ref}",
        "catalog": f"missionctl catalog get {source_ref}",
    }.get(scheme)
    if command is None:
        if source_ref.startswith("/"):
            return RetrievalInstruction(kind=RetrievalKind.READ_FILE, path=source_ref)
        if scheme == "mc":
            return RetrievalInstruction(kind=RetrievalKind.MCP_RESOURCE, resource_uri=source_ref)
        command = f"missionctl run context-item {source_kind.value} {source_ref}"
    return RetrievalInstruction(kind=RetrievalKind.MISSIONCTL, command=command)


def render_retrieval(retrieval: RetrievalInstruction) -> str:
    if retrieval.kind == RetrievalKind.READ_FILE:
        text = f"read_file {retrieval.path}"
    elif retrieval.kind == RetrievalKind.TOOL_CALL:
        assert retrieval.tool is not None
        excerpt = f" {retrieval.tool.args_excerpt}" if retrieval.tool.args_excerpt else ""
        text = f"tool {retrieval.tool.name}{excerpt} (args {retrieval.tool.args_digest})"
    elif retrieval.kind == RetrievalKind.MISSIONCTL:
        text = f"`{retrieval.command}`"
    else:
        text = f"mcp resource {retrieval.resource_uri}"
    if retrieval.range_hint:
        text += f" [{retrieval.range_hint}]"
    return text


def item_location(item: ContextItem) -> str:
    if item.inline is not None:
        return "inline below"
    if item.reference is not None:
        return render_retrieval(item.reference.retrieval)
    if item.materialize is not None:
        return f"{item.materialize.path} (read-only)"
    assert item.workspace is not None
    return f"restore {item.workspace.snapshot_ref}: " + ", ".join(item.workspace.restore_paths)


def item_summary(item: ContextItem) -> str:
    if item.reference is not None:
        return item.reference.summary
    if item.materialize is not None:
        return item.materialize.summary
    return ""


def _cell(text: str) -> str:
    return " ".join(text.split()).replace("|", "\\|")


def index_row(item: ContextItem) -> str:
    """One Markdown table row for the packet index; identical in prompt and context.md."""

    cells = (
        item.binding_name or "-",
        item.source_kind.value,
        item.tier.value + (" (mandatory)" if item.mandatory else ""),
        item_location(item),
        item.content_digest,
        str(item.bytes),
        item.trust.value,
        item_summary(item),
    )
    return "| " + " | ".join(_cell(cell) or "-" for cell in cells) + " |\n"


def _fence(text: str) -> str:
    longest = max((len(run) for run in re.findall(r"`+", text)), default=0)
    return "`" * max(3, longest + 1)


def inline_block(item: ContextItem) -> str:
    """Fenced data block for an inline item; content is always data, never instructions."""

    assert item.inline is not None
    text = item.inline.text
    fence = _fence(text)
    label = item.binding_name or item.source_kind.value
    info = f"data source={item.source_ref} trust={item.trust.value} kind={item.source_kind.value}"
    body = text if text.endswith("\n") else text + "\n"
    return f"### {label}\n\n{fence}{info}\n{body}{fence}\n\n"


def _expired(candidate: PackCandidate, instant: datetime) -> bool:
    return candidate.expires_at is not None and candidate.expires_at <= instant


def _item_chars(item: ContextItem) -> int:
    return len(index_row(item)) + (len(inline_block(item)) if item.inline is not None else 0)


def _bounded_summary(candidate: PackCandidate, limit: int) -> str:
    raw = candidate.summary or f"{candidate.media_type}, {candidate.bytes} bytes"
    text = " ".join(raw.split())
    if len(text) <= limit:
        return text
    return text[: limit - 3].rstrip() + "..."


def _sanitize_segment(value: str) -> str:
    cleaned = _FILE_NAME_SAFE.sub("_", value).strip("._") or "item"
    return cleaned[:128]


def slot_name_for(item: ContextItem) -> str:
    """Workspace slot name for a materialized item (``^[a-z][a-z0-9_-]*$``)."""

    label = _SLUG_SAFE.sub("-", (item.binding_name or item.source_kind.value).lower()).strip("-")
    return f"ctx-{label[:40] or 'item'}-{item.item_id.removeprefix('sha256:')[:12]}"
