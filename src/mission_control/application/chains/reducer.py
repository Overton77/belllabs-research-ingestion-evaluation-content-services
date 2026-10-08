"""The chain reducer (SPEC-04 "Chain reducer", ADR-0029): pure over chain rows and run facts.

``ChainReducer.on_events`` is called by the canonical ledger writer after the mission events of
one commit are written and before the transaction commits. It reads nothing and writes nothing:
it receives the chain (with every link and its version) and the current run facts of each member
mission, and returns one :class:`ChainTransition` the caller applies in the same transaction.

Release conditions are evaluated on the supplier run's *state* after the commit (its accepted
obligation and output evidence, phase and terminal outcome), so a replayed or re-ordered commit
reaches the same decision and an already-decided link is never decided twice:

- ``goal_accepted{goal_key}``: the run accepted obligation evidence for the goal (obligation
  ref ``<goal_key>`` or ``goal:<goal_key>``); a ``supplies`` link additionally waits until the
  run has accepted output evidence, so a consumer never starts from provisional outputs.
- ``mission_accepted``: the run is terminal ``completed`` with every required obligation
  accepted (the kernel closes a mission with its run).
- ``execution_complete``: the run is terminal with any outcome but ``cancelled`` (outputs that
  were not accepted travel labelled provisional).

A terminal supplier that does not satisfy a link blocks it (``upstream_not_accepted`` or
``upstream_execution_failed``); a cancelled supplier applies the link's ``on_upstream_cancel``.
A consumer is admitted when every incoming link is released; a consumer that already has a run
the chain did not admit blocks the releasing link (``consumer_already_started``).
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Literal
from uuid import UUID, uuid5

from pydantic import AwareDatetime, Field

from mission_control.domain.composition.chain import (
    ChainContract,
    ChainEvent,
    ChainLifecycle,
    ChainLink,
    ChainLinkKind,
    ChainLinkState,
    ChainPhase,
    ChainTerminalOutcome,
    ChainTransition,
    ChainUpdate,
    ConsumerAdmission,
    ConsumerCancellation,
    LinkUpdate,
    MissionChain,
    OnUpstreamCancel,
    link_transition_allowed,
)
from mission_control.domain.policies.contracts import RunOutcome, RunPhase

_EVENT_NAMESPACE = UUID("0b7a1c55-3f0e-5a6d-9c2b-8e4f1d2a6c90")

# Mission events after which a chain may have something to decide. Anything else (budget
# reservations, frame facts, boundary deliveries) cannot change a release condition.
CHAIN_RELEVANT_EVENT_TYPES = frozenset(
    {
        "workflow_run.start",
        "workflow_run.record_obligation_evidence",
        "workflow_run.record_output_evidence",
        "workflow_run.apply_authority_batch",
        "workflow_run.terminalize",
        "workflow_run.cancel",
    }
)


class MemberRun(ChainContract):
    """The current run of one member mission, as the reducer needs it."""

    mission_id: UUID
    run_id: UUID
    """``mission_run.run_id`` (the row identity links record as ``released_run_id``)."""
    run_key: str = Field(min_length=1)
    """The run-control identity (``RunProjection.run_id``) commands address."""
    phase: RunPhase
    terminal_outcome: RunOutcome | None = None
    required_obligations: frozenset[str] = frozenset()
    accepted_obligations: frozenset[str] = frozenset()
    accepted_outputs: tuple[str, ...] = ()
    evidence_frontier_digest: str | None = None

    @property
    def terminal(self) -> bool:
        return self.phase is RunPhase.TERMINAL

    def goal_accepted(self, goal_key: str) -> bool:
        return bool({goal_key, f"goal:{goal_key}"} & self.accepted_obligations)

    @property
    def mission_accepted(self) -> bool:
        return (
            self.terminal
            and self.terminal_outcome is RunOutcome.COMPLETED
            and self.required_obligations <= self.accepted_obligations
        )


class ChainSnapshot(ChainContract):
    """The chain with every link's persisted version and the member runs (latest per mission)."""

    chain: MissionChain
    link_versions: dict[UUID, int]
    runs: dict[UUID, MemberRun] = Field(default_factory=dict)


Decision = Literal["release", "block", "cancel", "detach"]


def _event_id(chain_id: UUID, event_type: str, subject: str) -> UUID:
    return uuid5(_EVENT_NAMESPACE, f"{chain_id}:{event_type}:{subject}")


def _blocked_reason(run: MemberRun) -> str:
    if run.terminal_outcome is RunOutcome.FAILED:
        return "upstream_execution_failed"
    return "upstream_not_accepted"


def link_satisfied(link: ChainLink, run: MemberRun) -> bool:
    """Whether the supplier run's state satisfies the link's release condition."""

    condition = link.on
    if condition.kind == "goal_accepted":
        assert condition.goal_key is not None  # guaranteed by ReleaseCondition
        if not run.goal_accepted(condition.goal_key):
            return False
        return link.kind is not ChainLinkKind.SUPPLIES or bool(run.accepted_outputs)
    if condition.kind == "mission_accepted":
        return run.mission_accepted
    return run.terminal and run.terminal_outcome is not RunOutcome.CANCELLED


def decide_link(link: ChainLink, run: MemberRun) -> tuple[Decision, str | None] | None:
    """The decision for one armed outgoing link of ``run``'s mission, or None to wait."""

    if run.terminal and run.terminal_outcome is RunOutcome.CANCELLED:
        if link.on_upstream_cancel is OnUpstreamCancel.DETACH:
            return "detach", None
        return "cancel", "upstream_cancelled"
    if link_satisfied(link, run):
        return "release", None
    if run.terminal:
        return "block", _blocked_reason(run)
    return None


class ChainReducer:
    """Pure chain reducer; see the module docstring for the rules."""

    def on_events(
        self,
        snapshot: ChainSnapshot,
        *,
        cause_event_id: UUID,
        trigger_mission_id: UUID,
        occurred_at: AwareDatetime | datetime,
    ) -> ChainTransition:
        chain = snapshot.chain
        if chain.lifecycle is ChainLifecycle.COMPLETED:
            return ChainTransition(cause_event_id=cause_event_id)
        runs = snapshot.runs
        links = {link.link_id: link for link in chain.links}
        state: dict[UUID, ChainLinkState] = {link.link_id: link.state for link in chain.links}
        released_now: list[UUID] = []
        link_updates: list[LinkUpdate] = []
        events: list[ChainEvent] = []
        cancellations: list[ConsumerCancellation] = []

        def move(link: ChainLink, target: ChainLinkState, reason: str | None = None) -> None:
            if not link_transition_allowed(state[link.link_id], target):
                return
            state[link.link_id] = target
            link_updates.append(
                LinkUpdate(
                    link_id=link.link_id,
                    expected_version=snapshot.link_versions[link.link_id],
                    state=target,
                    blocked_reason=reason if target is ChainLinkState.BLOCKED else None,
                )
            )

        supplier_run = runs.get(trigger_mission_id)
        outgoing = sorted(
            (link for link in chain.links if link.from_mission_id == trigger_mission_id),
            key=lambda item: item.link_key,
        )
        if supplier_run is not None:
            for link in outgoing:
                if state[link.link_id] is ChainLinkState.ARMED:
                    decision = decide_link(link, supplier_run)
                    if decision is None:
                        continue
                    kind, reason = decision
                    if kind == "release":
                        consumer = _require(link.to_mission_id)
                        consumer_run = runs.get(consumer)
                        if consumer_run is not None and not _admitted_by_chain(
                            chain, consumer, consumer_run
                        ):
                            move(link, ChainLinkState.BLOCKED, "consumer_already_started")
                            events.append(_blocked_event(chain, link, "consumer_already_started"))
                        else:
                            move(link, ChainLinkState.RELEASED)
                            released_now.append(link.link_id)
                    elif kind == "block":
                        assert reason is not None
                        move(link, ChainLinkState.BLOCKED, reason)
                        events.append(_blocked_event(chain, link, reason))
                    elif kind == "cancel":
                        move(link, ChainLinkState.CANCELLED, reason)
                        events.append(_link_event(chain, link, "chain_link.cancelled", reason))
                    else:
                        move(link, ChainLinkState.DETACHED)
                        events.append(_link_event(chain, link, "chain_link.detached", None))
                elif (
                    state[link.link_id] is ChainLinkState.RELEASED
                    and supplier_run.terminal
                    and supplier_run.terminal_outcome is RunOutcome.CANCELLED
                ):
                    if link.on_upstream_cancel is OnUpstreamCancel.DETACH:
                        move(link, ChainLinkState.DETACHED)
                        events.append(_link_event(chain, link, "chain_link.detached", None))
                        continue
                    consumer_run = runs.get(_require(link.to_mission_id))
                    if consumer_run is not None and not consumer_run.terminal:
                        cancellations.append(
                            ConsumerCancellation(
                                chain_id=chain.chain_id,
                                run_id=consumer_run.run_id,
                                run_key=consumer_run.run_key,
                                link_id=link.link_id,
                            )
                        )

        # Releases: one event per released link (the caller adds run and packet facts).
        for link_id in released_now:
            events.append(_link_event(chain, links[link_id], "chain_link.released", None))

        admissions: list[ConsumerAdmission] = []
        consumers = sorted(
            {_require(links[link_id].to_mission_id) for link_id in released_now},
            key=str,
        )
        for consumer in consumers:
            incoming = sorted(
                (link for link in chain.links if link.to_mission_id == consumer),
                key=lambda item: item.link_key,
            )
            if runs.get(consumer) is not None:
                continue
            if any(link.released_run_id is not None for link in incoming):
                continue
            if all(state[link.link_id] is ChainLinkState.RELEASED for link in incoming):
                member = next(item for item in chain.members if item.mission_id == consumer)
                admissions.append(
                    ConsumerAdmission(
                        chain_id=chain.chain_id,
                        to_mission_key=member.mission_key,
                        to_mission_id=consumer,
                        link_ids=tuple(link.link_id for link in incoming),
                    )
                )

        chain_updates, chain_events = self._chain_state(
            chain, state, runs, admitted={item.to_mission_id for item in admissions}
        )
        events.extend(chain_events)
        del occurred_at  # decisions are state-based; the caller stamps the rows
        return ChainTransition(
            cause_event_id=cause_event_id,
            link_updates=tuple(link_updates),
            chain_updates=chain_updates,
            admissions=tuple(admissions),
            cancellations=tuple(cancellations),
            events=tuple(events),
        )

    @staticmethod
    def _chain_state(
        chain: MissionChain,
        state: Mapping[UUID, ChainLinkState],
        runs: Mapping[UUID, MemberRun],
        *,
        admitted: set[UUID],
    ) -> tuple[tuple[ChainUpdate, ...], tuple[ChainEvent, ...]]:
        statuses = member_statuses(chain, state, runs, admitted=admitted)
        started = any(run.phase is not RunPhase.PENDING for run in runs.values()) or any(
            item is ChainLinkState.RELEASED for item in state.values()
        )
        lifecycle = (
            ChainLifecycle.RUNNING
            if started or chain.lifecycle is ChainLifecycle.RUNNING
            else ChainLifecycle.PENDING
        )
        outcome: ChainTerminalOutcome | None = None
        if all(status in {"terminal", "unreachable"} for status in statuses.values()):
            lifecycle = ChainLifecycle.COMPLETED
            outcome = _terminal_outcome(chain, state, runs)
        if any(
            item in {ChainLinkState.BLOCKED, ChainLinkState.CANCELLED} for item in state.values()
        ):
            phase = ChainPhase.BLOCKED
        elif not any(item is ChainLinkState.ARMED for item in state.values()):
            phase = ChainPhase.DRAINING
        else:
            phase = ChainPhase.RELEASING
        if (lifecycle, phase, outcome) == (chain.lifecycle, chain.phase, chain.terminal_outcome):
            return (), ()
        update = ChainUpdate(
            chain_id=chain.chain_id,
            expected_version=chain.version,
            lifecycle=lifecycle,
            phase=phase,
            terminal_outcome=outcome,
        )
        if outcome is None:
            return (update,), ()
        members = [
            {
                "mission_key": member.mission_key,
                "mission_id": str(member.mission_id),
                "run_id": str(runs[member.mission_id].run_id)
                if member.mission_id in runs
                else None,
                "outcome": _member_outcome(runs.get(member.mission_id)),
            }
            for member in sorted(chain.members, key=lambda item: item.order)
        ]
        completed = ChainEvent(
            event_type="chain.completed",
            payload={
                "chain_id": str(chain.chain_id),
                "chain_key": chain.chain_key,
                "terminal_outcome": outcome.value,
                "members": members,
            },
        )
        return (update,), (completed,)


MemberStatus = Literal["terminal", "active", "waiting", "unreachable"]


def member_statuses(
    chain: MissionChain,
    state: Mapping[UUID, ChainLinkState],
    runs: Mapping[UUID, MemberRun],
    *,
    admitted: set[UUID] | frozenset[UUID] = frozenset(),
) -> dict[UUID, MemberStatus]:
    """Each member's status in topological order: a member without a run is unreachable once
    any incoming link is blocked, cancelled or detached, or its armed supplier can no longer
    release (unreachable, or terminal without releasing)."""

    statuses: dict[UUID, MemberStatus] = {}
    for member in sorted(chain.members, key=lambda item: item.order):
        mission_id = member.mission_id
        run = runs.get(mission_id)
        if run is not None:
            statuses[mission_id] = "terminal" if run.terminal else "active"
            continue
        if mission_id in admitted:
            statuses[mission_id] = "active"
            continue
        incoming = [link for link in chain.links if link.to_mission_id == mission_id]
        status: MemberStatus = "waiting"
        for link in incoming:
            link_state = state[link.link_id]
            if link_state in {
                ChainLinkState.BLOCKED,
                ChainLinkState.CANCELLED,
                ChainLinkState.DETACHED,
            }:
                status = "unreachable"
                break
            supplier = statuses.get(_require(link.from_mission_id))
            if link_state is ChainLinkState.ARMED and supplier in {"unreachable", "terminal"}:
                status = "unreachable"
                break
        statuses[mission_id] = status
    return statuses


def _terminal_outcome(
    chain: MissionChain,
    state: Mapping[UUID, ChainLinkState],
    runs: Mapping[UUID, MemberRun],
) -> ChainTerminalOutcome:
    member_runs = [runs.get(member.mission_id) for member in chain.members]
    if all(run is not None and run.mission_accepted for run in member_runs):
        return ChainTerminalOutcome.ACCEPTED
    if any(item is ChainLinkState.CANCELLED for item in state.values()) or any(
        run is not None and run.terminal_outcome is RunOutcome.CANCELLED for run in member_runs
    ):
        return ChainTerminalOutcome.CANCELLED
    if any(run is not None and run.terminal_outcome is RunOutcome.FAILED for run in member_runs):
        return ChainTerminalOutcome.EXECUTION_FAILED
    return ChainTerminalOutcome.NOT_ACCEPTED


def _member_outcome(run: MemberRun | None) -> str:
    if run is None:
        return "not_started"
    if not run.terminal:
        return run.phase.value
    if run.mission_accepted:
        return "mission_accepted"
    assert run.terminal_outcome is not None
    return {
        RunOutcome.COMPLETED: "not_accepted",
        RunOutcome.PARTIALLY_COMPLETED: "not_accepted",
        RunOutcome.FAILED: "execution_failed",
        RunOutcome.CANCELLED: "cancelled",
    }[run.terminal_outcome]


def _admitted_by_chain(chain: MissionChain, consumer: UUID, run: MemberRun) -> bool:
    return any(
        link.to_mission_id == consumer and link.released_run_id == run.run_id
        for link in chain.links
    )


def _require(value: UUID | None) -> UUID:
    if value is None:
        raise ValueError("a persisted chain link names both member missions")
    return value


def _link_payload(chain: MissionChain, link: ChainLink) -> dict[str, object]:
    return {
        "chain_id": str(chain.chain_id),
        "link_id": str(link.link_id),
        "link_key": link.link_key,
        "kind": link.kind.value,
        "from_mission_key": link.from_mission_key,
        "to_mission_key": link.to_mission_key,
        "from_mission_id": str(link.from_mission_id),
        "to_mission_id": str(link.to_mission_id),
    }


def _link_event(
    chain: MissionChain,
    link: ChainLink,
    event_type: Literal["chain_link.released", "chain_link.detached", "chain_link.cancelled"],
    reason: str | None,
) -> ChainEvent:
    payload = _link_payload(chain, link)
    if reason is not None:
        payload["reason"] = reason
    return ChainEvent(event_type=event_type, payload=payload, link_id=link.link_id)


def _blocked_event(chain: MissionChain, link: ChainLink, reason: str) -> ChainEvent:
    return ChainEvent(
        event_type="chain_link.blocked",
        payload={**_link_payload(chain, link), "blocked_reason": reason},
        link_id=link.link_id,
    )


def chain_event_id(chain_id: UUID, event: ChainEvent) -> UUID:
    """One envelope identity per chain event, shared by every member stream."""

    subject = str(event.link_id) if event.link_id is not None else "chain"
    return _event_id(chain_id, event.event_type, subject)


def outgoing_links(chain: MissionChain, mission_id: UUID) -> Sequence[ChainLink]:
    return tuple(link for link in chain.links if link.from_mission_id == mission_id)
