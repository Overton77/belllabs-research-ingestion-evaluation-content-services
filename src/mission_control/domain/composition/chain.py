"""Mission Chains (``mc.chain.v1``, ``mc.chain_link.v1``): contracts and compile-time rules.

A manifest with ``missions:`` and ``links:`` compiles into independent missions plus typed
links (ADR-0029, SPEC-04). This module is pure: :func:`compile_chain` validates the link
graph and bindings against the parsed manifest and returns the topological order, the
compiled links and blockers with JSON pointers; :func:`build_mission_chain` turns a clean
compilation plus the ids assigned at submit into the persisted ``MissionChain`` contract.
Release, blocking and cancellation are decided by the chain reducer (FT-D2) and expressed as
a :class:`ChainTransition`.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from enum import StrEnum
from typing import Literal
from uuid import UUID, uuid5

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

from mission_control.domain.authoring.canonical import stable_json_digest
from mission_control.domain.authoring.manifest import (
    ChainLinkDeclaration,
    ExpansionTier,
    GoalAcceptedCondition,
    ManifestErrorCode,
    ManifestIssue,
    MissionBlock,
    MissionManifest,
    find_cycle,
    walk_program,
)
from mission_control.domain.policies.contracts import RunRequest, VerifiedRunConfiguration

CHAIN_SCHEMA_VERSION = "mc.chain.v1"
CHAIN_LINK_SCHEMA_VERSION = "mc.chain_link.v1"
_CHAIN_NAMESPACE = UUID("6f0a3c52-2f1e-5b8e-9d1c-c4a1f0e8d7b3")


class ChainContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ChainLinkKind(StrEnum):
    SUPPLIES = "supplies"
    DEPENDS_ON = "depends_on"


class ChainLinkState(StrEnum):
    ARMED = "armed"
    RELEASED = "released"
    BLOCKED = "blocked"
    DETACHED = "detached"
    CANCELLED = "cancelled"


# Forward-only link states (mirrored by the 0028 trigger).
LINK_TRANSITIONS: Mapping[ChainLinkState, frozenset[ChainLinkState]] = {
    ChainLinkState.ARMED: frozenset(
        {
            ChainLinkState.RELEASED,
            ChainLinkState.BLOCKED,
            ChainLinkState.DETACHED,
            ChainLinkState.CANCELLED,
        }
    ),
    ChainLinkState.RELEASED: frozenset({ChainLinkState.DETACHED}),
    ChainLinkState.BLOCKED: frozenset(),
    ChainLinkState.DETACHED: frozenset(),
    ChainLinkState.CANCELLED: frozenset(),
}


class ChainLifecycle(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"


class ChainPhase(StrEnum):
    RELEASING = "releasing"
    DRAINING = "draining"
    BLOCKED = "blocked"


class ChainTerminalOutcome(StrEnum):
    ACCEPTED = "accepted"
    NOT_ACCEPTED = "not_accepted"
    CANCELLED = "cancelled"
    EXECUTION_FAILED = "execution_failed"


class OnUpstreamCancel(StrEnum):
    CANCEL_DOWNSTREAM = "cancel_downstream"
    DETACH = "detach"


class ReleaseCondition(ChainContract):
    """``goal_accepted{goal_key}`` | ``mission_accepted`` | ``execution_complete``."""

    kind: Literal["goal_accepted", "mission_accepted", "execution_complete"]
    goal_key: str | None = None

    @model_validator(mode="after")
    def _goal_only_for_goal_accepted(self) -> ReleaseCondition:
        if (self.kind == "goal_accepted") != (self.goal_key is not None):
            raise ValueError("goal_accepted carries a goal_key; other conditions carry none")
        return self

    @property
    def satisfying_event(self) -> str:
        """The mission event type that satisfies the condition (SPEC-04 table)."""

        return {
            "goal_accepted": "disposition.recorded",
            "mission_accepted": "mission.closed",
            "execution_complete": "run.completed",
        }[self.kind]


class ChainBinding(ChainContract):
    output_name: str = Field(min_length=1)
    input_name: str = Field(min_length=1)
    schema_ref: str = Field(min_length=1)
    expand: ExpansionTier = ExpansionTier.AUTO


class ChainLink(ChainContract):
    """``mc.chain_link.v1``."""

    schema_version: Literal["mc.chain_link.v1"] = "mc.chain_link.v1"
    link_id: UUID
    chain_id: UUID
    link_key: str = Field(min_length=1)
    from_mission_key: str = Field(min_length=1)
    to_mission_key: str = Field(min_length=1)
    from_mission_id: UUID | None = None
    to_mission_id: UUID | None = None
    kind: ChainLinkKind
    bindings: tuple[ChainBinding, ...] = ()
    on: ReleaseCondition
    on_upstream_cancel: OnUpstreamCancel = OnUpstreamCancel.CANCEL_DOWNSTREAM
    on_upstream_not_accepted: Literal["stop"] = "stop"
    state: ChainLinkState = ChainLinkState.ARMED
    released_run_id: UUID | None = None
    released_at: AwareDatetime | None = None
    blocked_reason: str | None = None
    packet_ref: str | None = None
    packet_digest: str | None = Field(default=None, pattern=r"^sha256:[0-9a-f]{64}$")

    @model_validator(mode="after")
    def _shape(self) -> ChainLink:
        if self.from_mission_key == self.to_mission_key:
            raise ValueError("a chain link joins two distinct missions")
        if (self.kind is ChainLinkKind.SUPPLIES) != bool(self.bindings):
            raise ValueError("supplies links carry bindings; depends_on links carry none")
        if (self.state is ChainLinkState.RELEASED) and self.released_at is None:
            raise ValueError("a released link records released_at")
        if (self.state is ChainLinkState.BLOCKED) != (self.blocked_reason is not None):
            raise ValueError("blocked_reason is recorded exactly when the link is blocked")
        return self


class ChainMember(ChainContract):
    mission_key: str = Field(min_length=1)
    mission_id: UUID
    revision_id: UUID
    order: int = Field(ge=0)


class ChainScope(ChainContract):
    installation_id: UUID
    application_id: str = Field(min_length=1)
    tenant_id: UUID


class MissionChain(ChainContract):
    """``mc.chain.v1``."""

    schema_version: Literal["mc.chain.v1"] = "mc.chain.v1"
    chain_id: UUID
    scope: ChainScope
    chain_key: str = Field(pattern=r"^[a-z0-9][a-z0-9_.-]{0,127}$")
    title: str = Field(min_length=1)
    manifest_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    members: tuple[ChainMember, ...] = Field(min_length=2)
    links: tuple[ChainLink, ...] = Field(min_length=1)
    lifecycle: ChainLifecycle = ChainLifecycle.PENDING
    phase: ChainPhase = ChainPhase.RELEASING
    terminal_outcome: ChainTerminalOutcome | None = None
    created_at: AwareDatetime
    created_by_actor_ref: str = Field(min_length=1)
    version: int = Field(default=1, ge=1)

    @model_validator(mode="after")
    def _consistent(self) -> MissionChain:
        keys = [member.mission_key for member in self.members]
        if len(set(keys)) != len(keys):
            raise ValueError("chain members have unique mission keys")
        if sorted(member.order for member in self.members) != list(range(len(self.members))):
            raise ValueError("member order is a permutation of 0..n-1")
        for link in self.links:
            if link.chain_id != self.chain_id:
                raise ValueError("every link belongs to this chain")
            if link.from_mission_key not in keys or link.to_mission_key not in keys:
                raise ValueError("links join chain members")
        if (self.lifecycle is ChainLifecycle.COMPLETED) != (self.terminal_outcome is not None):
            raise ValueError("terminal_outcome is recorded exactly when the chain completed")
        return self


class ChainEvent(ChainContract):
    """A chain event to append to every member mission's stream (same ``event_id``)."""

    event_type: Literal[
        "chain.created",
        "chain_link.released",
        "chain_link.blocked",
        "chain_link.detached",
        "chain_link.cancelled",
        "chain.completed",
    ]
    payload: dict[str, object]
    link_id: UUID | None = None
    """The link the event is about (absent for chain-level events)."""


class LinkUpdate(ChainContract):
    link_id: UUID
    expected_version: int = Field(ge=1)
    state: ChainLinkState
    released_run_id: UUID | None = None
    blocked_reason: str | None = None
    packet_ref: str | None = None
    packet_digest: str | None = None


class ChainUpdate(ChainContract):
    chain_id: UUID
    expected_version: int = Field(ge=1)
    lifecycle: ChainLifecycle
    phase: ChainPhase
    terminal_outcome: ChainTerminalOutcome | None = None


class ConsumerAdmission(ChainContract):
    """The consumer run the reducer admits in the same transaction (D2 fills the packet)."""

    chain_id: UUID
    to_mission_key: str
    to_mission_id: UUID
    link_ids: tuple[UUID, ...] = Field(min_length=1)


class ConsumerCancellation(ChainContract):
    chain_id: UUID
    run_id: UUID
    run_key: str = Field(min_length=1)
    """The consumer run's run-control identity (the key commands address)."""
    link_id: UUID
    reason: Literal["upstream_cancelled"] = "upstream_cancelled"


class ChainTransition(ChainContract):
    """What the chain reducer decided for one committed mission event; applied by the
    canonical ledger writer in the same transaction (SPEC-04 "Chain reducer")."""

    cause_event_id: UUID
    link_updates: tuple[LinkUpdate, ...] = ()
    chain_updates: tuple[ChainUpdate, ...] = ()
    admissions: tuple[ConsumerAdmission, ...] = ()
    cancellations: tuple[ConsumerCancellation, ...] = ()
    events: tuple[ChainEvent, ...] = ()

    @property
    def is_noop(self) -> bool:
        return not (
            self.link_updates
            or self.chain_updates
            or self.admissions
            or self.cancellations
            or self.events
        )


def link_transition_allowed(current: ChainLinkState, target: ChainLinkState) -> bool:
    return target in LINK_TRANSITIONS[current]


ChainFamily = Literal["StageGraph", "GoalDirected"]


class ChainMemberAdmission(ChainContract):
    """A member's Run Request, verified at submit and admitted later by the chain reducer.

    ``verified_configuration`` is what ``RunControlService.verify_admission`` returned when the
    chain was submitted; the reducer replays ``accepted_admission_mutation`` from these two
    values inside the ledger transaction that releases the member's last incoming link.
    """

    chain_id: UUID
    mission_id: UUID
    mission_key: str = Field(min_length=1)
    revision_id: UUID
    family: ChainFamily
    initial_goal: str | None = Field(default=None, min_length=1)
    autostart: bool = True
    run_request: RunRequest
    verified_configuration: VerifiedRunConfiguration

    @model_validator(mode="after")
    def _goal_for_goal_directed(self) -> ChainMemberAdmission:
        if (self.family == "GoalDirected") != (self.initial_goal is not None):
            raise ValueError("a GoalDirected member carries its initial goal; StageGraph none")
        return self


# ---------------------------------------------------------------------------
# Compile
# ---------------------------------------------------------------------------


class CompiledChainLink(ChainContract):
    link_key: str
    pointer: str
    from_mission_key: str
    to_mission_key: str
    kind: ChainLinkKind
    bindings: tuple[ChainBinding, ...] = ()
    on: ReleaseCondition
    on_upstream_cancel: OnUpstreamCancel
    on_upstream_not_accepted: Literal["stop"] = "stop"


class ChainResolution(ChainContract):
    """The ``chain`` section of ``mc.manifest_resolution.v1``."""

    order: tuple[str, ...]
    links: tuple[CompiledChainLink, ...]


class ChainCompilation(ChainContract):
    resolution: ChainResolution | None
    blockers: tuple[ManifestIssue, ...] = ()
    warnings: tuple[ManifestIssue, ...] = ()

    @property
    def ok(self) -> bool:
        return not self.blockers and self.resolution is not None


def _blocker(pointer: str, message: str, reason: str) -> ManifestIssue:
    return ManifestIssue(
        code=ManifestErrorCode.INVALID_DEFINITION, pointer=pointer, message=message, reason=reason
    )


def _root_outputs(block: MissionBlock) -> dict[str, str]:
    return {output.name: output.schema_ for output in block.program.outputs}


def _chain_inputs(
    block: MissionBlock, pointer: str, mission_keys: set[str]
) -> list[tuple[str, str, str, str | None, ExpansionTier, str]]:
    """Inputs bound to another mission: (from_mission, output, input_name, schema, expand, ptr)."""

    found: list[tuple[str, str, str, str | None, ExpansionTier, str]] = []
    for visit in walk_program(block.program, f"{pointer}/program"):
        for index, binding in enumerate(visit.node.inputs):
            if binding.from_ is None:
                continue
            source, _, output = binding.from_.partition(".")
            if source in mission_keys and source != block.key:
                found.append(
                    (
                        source,
                        output,
                        binding.name,
                        binding.schema_,
                        binding.expand,
                        f"{visit.pointer}/inputs/{index}/from",
                    )
                )
    return found


def _goal_owning(block: MissionBlock, output: str | None) -> str | None:
    if output is not None:
        for goal in block.goals:
            if any(output in criterion.evidence for criterion in goal.criteria):
                return goal.key
    if len(block.goals) == 1:
        return block.goals[0].key
    return None


def _condition(
    declaration: ChainLinkDeclaration, supplier: MissionBlock, pointer: str
) -> tuple[ReleaseCondition | None, list[ManifestIssue]]:
    on = declaration.on
    if isinstance(on, GoalAcceptedCondition) or on == "goal_accepted":
        goal_key = on.goal_accepted.goal_key if isinstance(on, GoalAcceptedCondition) else None
        if goal_key is None:
            goal_key = _goal_owning(
                supplier, declaration.outputs[0] if declaration.outputs else None
            )
            if goal_key is None:
                return None, [
                    _blocker(
                        f"{pointer}/on",
                        f"cannot infer the goal of {supplier.key} that releases this link; "
                        "name it with on: {goal_accepted: {goal_key: ...}}",
                        "unresolved_release_goal",
                    )
                ]
        if goal_key not in {goal.key for goal in supplier.goals}:
            return None, [
                _blocker(
                    f"{pointer}/on/goal_accepted/goal_key",
                    f"{goal_key} is not a goal of mission {supplier.key}",
                    "unknown_goal",
                )
            ]
        return ReleaseCondition(kind="goal_accepted", goal_key=goal_key), []
    return ReleaseCondition(kind=on), []


def topological_order(keys: Sequence[str], edges: Iterable[tuple[str, str]]) -> tuple[str, ...]:
    """Kahn's algorithm; ties keep file order. Assumes an acyclic graph."""

    incoming: dict[str, set[str]] = {key: set() for key in keys}
    for source, target in edges:
        if source in incoming and target in incoming:
            incoming[target].add(source)
    order: list[str] = []
    remaining = list(keys)
    while remaining:
        ready = next((key for key in remaining if not (incoming[key] - set(order))), remaining[0])
        order.append(ready)
        remaining.remove(ready)
    return tuple(order)


def compile_chain(manifest: MissionManifest) -> ChainCompilation:
    """Validate a chain manifest's links (SPEC-04 "Compile"); single missions have none."""

    if manifest.missions is None or manifest.links is None:
        return ChainCompilation(resolution=None)
    blocks = {
        block.key: (f"/missions/{index}", block) for index, block in enumerate(manifest.missions)
    }
    keys = [block.key for block in manifest.missions]
    blockers: list[ManifestIssue] = []
    compiled: list[CompiledChainLink] = []
    supplied: set[tuple[str, str, str]] = set()
    seen: set[tuple[str, str, str]] = set()
    edges: list[tuple[str, str]] = []
    for index, declaration in enumerate(manifest.links):
        pointer = f"/links/{index}"
        source, target = declaration.from_, declaration.to
        unknown = False
        for name, key in (("from", source), ("to", target)):
            if key not in blocks:
                unknown = True
                blockers.append(
                    _blocker(
                        f"{pointer}/{name}",
                        f"{key} is not a mission in this manifest",
                        "unknown_mission_key",
                    )
                )
        if unknown:
            continue
        if source == target:
            blockers.append(
                _blocker(f"{pointer}/to", "a link cannot join a mission to itself", "chain_cycle")
            )
            continue
        identity = (source, target, declaration.kind)
        if identity in seen:
            blockers.append(
                _blocker(
                    pointer,
                    f"duplicate {declaration.kind} link {source} -> {target}",
                    "duplicate_link",
                )
            )
            continue
        seen.add(identity)
        edges.append((source, target))
        supplier_pointer, supplier = blocks[source]
        consumer_pointer, consumer = blocks[target]
        condition, condition_issues = _condition(declaration, supplier, pointer)
        blockers.extend(condition_issues)
        bindings: list[ChainBinding] = []
        if declaration.kind == "supplies":
            outputs = _root_outputs(supplier)
            consumer_inputs = {
                (item[0], item[1]): item
                for item in _chain_inputs(consumer, consumer_pointer, set(keys))
            }
            for output_index, output in enumerate(declaration.outputs):
                output_pointer = f"{pointer}/outputs/{output_index}"
                if output not in outputs:
                    blockers.append(
                        _blocker(
                            output_pointer,
                            f"{output} is not an output of {source}'s program root "
                            f"({supplier_pointer}/program/outputs)",
                            "unbound_chain_output",
                        )
                    )
                    continue
                consumer_input = consumer_inputs.get((source, output))
                if consumer_input is None:
                    blockers.append(
                        _blocker(
                            output_pointer,
                            f"{target} declares no input from: {source}.{output}",
                            "unbound_chain_output",
                        )
                    )
                    continue
                _, _, input_name, input_schema, expand, input_pointer = consumer_input
                if input_schema is not None and input_schema != outputs[output]:
                    blockers.append(
                        _blocker(
                            input_pointer.removesuffix("/from") + "/schema",
                            f"input {input_name} expects {input_schema} but {source}.{output} "
                            f"is {outputs[output]}",
                            "schema_mismatch",
                        )
                    )
                    continue
                supplied.add((source, target, output))
                bindings.append(
                    ChainBinding(
                        output_name=output,
                        input_name=input_name,
                        schema_ref=outputs[output],
                        expand=expand,
                    )
                )
        if condition is None or (declaration.kind == "supplies" and not bindings):
            continue
        compiled.append(
            CompiledChainLink(
                link_key=f"{source}->{target}:{declaration.kind}",
                pointer=pointer,
                from_mission_key=source,
                to_mission_key=target,
                kind=ChainLinkKind(declaration.kind),
                bindings=tuple(bindings),
                on=condition,
                on_upstream_cancel=OnUpstreamCancel(declaration.on_upstream_cancel),
            )
        )
    # Consumer inputs bound to another mission need a supplies link that binds the output.
    for key in keys:
        pointer, block = blocks[key]
        for source, output, _name, _schema, _expand, input_pointer in _chain_inputs(
            block, pointer, set(keys)
        ):
            if (source, key, output) not in supplied and not any(
                issue.reason in {"schema_mismatch", "unbound_chain_output"}
                and issue.message.startswith(f"input {_name} ")
                for issue in blockers
            ):
                blockers.append(
                    _blocker(
                        input_pointer,
                        f"input from {source}.{output} is not bound by a supplies link "
                        f"{source} -> {key}",
                        "unbound_chain_output",
                    )
                )
    adjacency: dict[str, list[str]] = {key: [] for key in keys}
    for source, target in edges:
        adjacency[source].append(target)
    cycle = find_cycle(adjacency)
    if cycle:
        first = next(
            index
            for index, link in enumerate(manifest.links)
            if (link.from_, link.to) == (cycle[0], cycle[1])
        )
        blockers.append(
            _blocker(
                f"/links/{first}",
                "chain links form a cycle: " + " -> ".join(cycle),
                "chain_cycle",
            )
        )
        return ChainCompilation(resolution=None, blockers=_unique(blockers))
    order = topological_order(keys, edges)
    has_incoming = {target for _, target in edges}
    for position, key in enumerate(order):
        if position > 0 and key not in has_incoming:
            blockers.append(
                _blocker(
                    f"{blocks[key][0]}/key",
                    f"mission {key} has no incoming link; only the first mission ({order[0]}) "
                    "is admitted at submit",
                    "missing_incoming_link",
                )
            )
    return ChainCompilation(
        resolution=ChainResolution(
            order=order,
            links=tuple(
                link
                for _, _, link in sorted(
                    (order.index(link.from_mission_key), index, link)
                    for index, link in enumerate(compiled)
                )
            ),
        ),
        blockers=_unique(blockers),
    )


def _unique(issues: Iterable[ManifestIssue]) -> tuple[ManifestIssue, ...]:
    seen: dict[tuple[str, str], ManifestIssue] = {}
    for issue in issues:
        seen.setdefault((issue.pointer, issue.message), issue)
    return tuple(seen.values())


def chain_link_id(chain_id: UUID, link_key: str) -> UUID:
    """Deterministic link id within a chain (retries of a submit produce the same rows)."""

    return uuid5(_CHAIN_NAMESPACE, f"{chain_id}:{link_key}")


def build_mission_chain(
    *,
    resolution: ChainResolution,
    chain_id: UUID,
    scope: ChainScope,
    chain_key: str,
    title: str,
    manifest_digest: str,
    members: Mapping[str, tuple[UUID, UUID]],
    created_at: AwareDatetime,
    created_by_actor_ref: str,
) -> MissionChain:
    """The persisted chain for a clean compilation; ``members`` maps mission key to
    ``(mission_id, revision_id)`` assigned at submit."""

    missing = [key for key in resolution.order if key not in members]
    if missing:
        raise ValueError(f"members missing for {missing}")
    return MissionChain(
        chain_id=chain_id,
        scope=scope,
        chain_key=chain_key,
        title=title,
        manifest_digest=manifest_digest,
        members=tuple(
            ChainMember(
                mission_key=key,
                mission_id=members[key][0],
                revision_id=members[key][1],
                order=position,
            )
            for position, key in enumerate(resolution.order)
        ),
        links=tuple(
            ChainLink(
                link_id=chain_link_id(chain_id, link.link_key),
                chain_id=chain_id,
                link_key=link.link_key,
                from_mission_key=link.from_mission_key,
                to_mission_key=link.to_mission_key,
                from_mission_id=members[link.from_mission_key][0],
                to_mission_id=members[link.to_mission_key][0],
                kind=link.kind,
                bindings=link.bindings,
                on=link.on,
                on_upstream_cancel=link.on_upstream_cancel,
            )
            for link in resolution.links
        ),
        created_at=created_at,
        created_by_actor_ref=created_by_actor_ref,
    )


def chain_digest(value: BaseModel) -> str:
    return stable_json_digest(value)


def chain_contract_schemas() -> dict[str, dict[str, object]]:
    """JSON Schemas exported to ``src/mission_control/contracts/schemas``."""

    return {
        CHAIN_SCHEMA_VERSION: MissionChain.model_json_schema(mode="validation"),
        CHAIN_LINK_SCHEMA_VERSION: ChainLink.model_json_schema(mode="validation"),
    }
