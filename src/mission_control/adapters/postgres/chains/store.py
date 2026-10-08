"""PostgreSQL Mission Chains (FT-D2, SPEC-04): rows, release hook, intents, projection.

- :func:`insert_chain` writes a chain, its links and the frozen admissions of its later members
  inside the caller's submit transaction (FT-E3).
- :class:`ChainReleaseHook` is the post-append hook of ``canonical.append_events``: on the
  same connection, before the ledger commit commits, it loads every chain the committing
  mission belongs to, runs the pure :class:`ChainReducer`, and applies the transition: link and
  chain state (compare-and-swap on version), the consumer's admission from its frozen request
  (``accepted_admission_mutation``: receipt, run, budget, effect ledger, events, transition),
  its sealed chain-link Context Packet (``context_selection``), ``mission_relationship`` rows,
  the ``mc.chain.start_run`` / ``mc.chain.cancel_run`` outbox intents, and the chain events in
  every member mission's stream with one envelope ``event_id``. It never calls Temporal, a
  provider or object storage: supplier outputs are read as custody metadata only.
- :class:`PostgresChainIntentStore` leases and acknowledges those intents for the relay.
- :class:`PostgresChainReader` serves the ``mc.chain.v1`` inspection projection.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime
from typing import Any
from uuid import UUID, uuid5

import asyncpg

from mission_control.adapters.postgres.control_plane.mission_revisions import (
    admit_run_for_revision,
)
from mission_control.adapters.postgres.run_control import canonical as mc
from mission_control.application.chains.packet import SuppliedArtifact, chain_pack_request
from mission_control.application.chains.reducer import (
    CHAIN_RELEVANT_EVENT_TYPES,
    ChainReducer,
    ChainSnapshot,
    MemberRun,
    chain_event_id,
    member_statuses,
)
from mission_control.application.chains.relay import (
    CANCEL_RUN_DESTINATION,
    START_RUN_DESTINATION,
    ChainIntent,
)
from mission_control.application.chains.service import ChainInspection, ChainMemberRun
from mission_control.application.context.pack_service import (
    ChainSupply,
    chain_supply_from_packet,
)
from mission_control.application.execution.service import accepted_admission_mutation
from mission_control.contracts.identities import parse_request_scope, uuid7
from mission_control.domain.composition.chain import (
    ChainBinding,
    ChainEvent,
    ChainLifecycle,
    ChainLink,
    ChainLinkKind,
    ChainLinkState,
    ChainMember,
    ChainMemberAdmission,
    ChainPhase,
    ChainScope,
    ChainTerminalOutcome,
    ChainTransition,
    ConsumerAdmission,
    LinkUpdate,
    MissionChain,
    OnUpstreamCancel,
    ReleaseCondition,
)
from mission_control.domain.context.packet import ContextPacket, PackFailure, pack
from mission_control.domain.context.refs import (
    durable_input_locator,
    parse_artifact_ref,
    parse_workspace_candidate_ref,
)
from mission_control.domain.context.render import context_selection_record
from mission_control.domain.policies.contracts import (
    ActorContext,
    DomainEventEnvelope,
    RunProjection,
)
from mission_control.domain.policies.errors import IdempotencyConflict, RunVersionConflict

HOOK_NAME = "mission-control.chain-release"
CHAIN_ACTOR_PREFIX = "chain:"
_SCOPE = mc.SCOPE


class ChainReleaseError(RuntimeError):
    """The release could not be applied; the whole ledger commit rolls back."""


def chain_actor_ref(chain_id: UUID) -> str:
    return f"{CHAIN_ACTOR_PREFIX}{chain_id}"


def _load(value: Any) -> Any:
    return json.loads(value) if isinstance(value, str) else value


def _dump(value: Any) -> str:
    return mc.dump(value)


# ------------------------------------------------------------------------------------------
# Writes at submit (FT-E3)
# ------------------------------------------------------------------------------------------


async def insert_chain(
    connection: asyncpg.Connection,
    args: tuple[Any, ...],
    chain: MissionChain,
    admissions: Sequence[ChainMemberAdmission],
) -> None:
    """Insert a chain, its armed links and the frozen admissions of its later members."""

    await connection.execute(
        """
        INSERT INTO mission_control.mission_chain (installation_id, application_id, tenant_id,
            chain_id, chain_key, title, manifest_digest, lifecycle, phase, terminal_outcome,
            members, version, created_at, updated_at, created_by_actor_ref)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb, $12, $13, $13, $14)
        """,
        *args,
        chain.chain_id,
        chain.chain_key,
        chain.title,
        chain.manifest_digest,
        chain.lifecycle.value,
        chain.phase.value,
        chain.terminal_outcome.value if chain.terminal_outcome else None,
        json.dumps([member.model_dump(mode="json") for member in chain.members]),
        chain.version,
        chain.created_at,
        chain.created_by_actor_ref,
    )
    for link in chain.links:
        await connection.execute(
            """
            INSERT INTO mission_control.chain_link (installation_id, application_id, tenant_id,
                link_id, chain_id, link_key, from_mission_key, to_mission_key, from_mission_id,
                to_mission_id, kind, bindings, release_condition, on_upstream_cancel,
                on_upstream_not_accepted, state, version, created_at, updated_at,
                created_by_actor_ref)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12::jsonb, $13::jsonb, $14,
                $15, $16, 1, $17, $17, $18)
            """,
            *args,
            link.link_id,
            chain.chain_id,
            link.link_key,
            link.from_mission_key,
            link.to_mission_key,
            link.from_mission_id,
            link.to_mission_id,
            link.kind.value,
            json.dumps([binding.model_dump(mode="json") for binding in link.bindings]),
            json.dumps(link.on.model_dump(mode="json", exclude_none=True)),
            link.on_upstream_cancel.value,
            link.on_upstream_not_accepted,
            link.state.value,
            chain.created_at,
            chain.created_by_actor_ref,
        )
    for admission in admissions:
        if admission.chain_id != chain.chain_id:
            raise ValueError("a member admission belongs to its chain")
        await connection.execute(
            """
            INSERT INTO mission_control.chain_member_admission (installation_id,
                application_id, tenant_id, chain_member_admission_id, chain_id, mission_id,
                mission_key, revision_id, family, initial_goal, autostart, run_request,
                verified_configuration, request_fingerprint, created_at, created_by_actor_ref)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12::jsonb, $13::jsonb, $14,
                $15, $16)
            """,
            *args,
            uuid7(),
            chain.chain_id,
            admission.mission_id,
            admission.mission_key,
            admission.revision_id,
            admission.family,
            admission.initial_goal,
            admission.autostart,
            admission.run_request.model_dump_json(),
            admission.verified_configuration.model_dump_json(),
            accepted_admission_mutation(
                admission.run_request, admission.verified_configuration
            ).decision.request_fingerprint,
            chain.created_at,
            chain.created_by_actor_ref,
        )


# ------------------------------------------------------------------------------------------
# Reads
# ------------------------------------------------------------------------------------------


def _chain_from_rows(
    chain_row: asyncpg.Record, link_rows: Sequence[asyncpg.Record]
) -> tuple[MissionChain, dict[UUID, int]]:
    links = []
    versions: dict[UUID, int] = {}
    for row in sorted(link_rows, key=lambda item: item["link_key"]):
        versions[row["link_id"]] = int(row["version"])
        links.append(
            ChainLink(
                link_id=row["link_id"],
                chain_id=row["chain_id"],
                link_key=row["link_key"],
                from_mission_key=row["from_mission_key"],
                to_mission_key=row["to_mission_key"],
                from_mission_id=row["from_mission_id"],
                to_mission_id=row["to_mission_id"],
                kind=ChainLinkKind(row["kind"]),
                bindings=tuple(
                    ChainBinding.model_validate(item) for item in _load(row["bindings"])
                ),
                on=ReleaseCondition.model_validate(_load(row["release_condition"])),
                on_upstream_cancel=OnUpstreamCancel(row["on_upstream_cancel"]),
                state=ChainLinkState(row["state"]),
                released_run_id=row["released_run_id"],
                released_at=row["released_at"],
                blocked_reason=row["blocked_reason"],
                packet_ref=row["packet_ref"],
                packet_digest=row["packet_digest"],
            )
        )
    chain = MissionChain(
        chain_id=chain_row["chain_id"],
        scope=ChainScope(
            installation_id=chain_row["installation_id"],
            application_id=chain_row["application_id"],
            tenant_id=chain_row["tenant_id"],
        ),
        chain_key=chain_row["chain_key"],
        title=chain_row["title"],
        manifest_digest=chain_row["manifest_digest"],
        members=tuple(ChainMember.model_validate(item) for item in _load(chain_row["members"])),
        links=tuple(links),
        lifecycle=ChainLifecycle(chain_row["lifecycle"]),
        phase=ChainPhase(chain_row["phase"]),
        terminal_outcome=(
            ChainTerminalOutcome(chain_row["terminal_outcome"])
            if chain_row["terminal_outcome"] is not None
            else None
        ),
        created_at=chain_row["created_at"],
        created_by_actor_ref=chain_row["created_by_actor_ref"],
        version=int(chain_row["version"]),
    )
    return chain, versions


async def _load_chain(
    connection: asyncpg.Connection, args: tuple[Any, ...], chain_id: UUID, *, lock: bool
) -> tuple[MissionChain, dict[UUID, int]] | None:
    chain_row = await connection.fetchrow(
        f"SELECT * FROM mission_control.mission_chain WHERE {_SCOPE} AND chain_id = $4"
        + (" FOR UPDATE" if lock else ""),
        *args,
        chain_id,
    )
    if chain_row is None:
        return None
    link_rows = await connection.fetch(
        f"SELECT * FROM mission_control.chain_link WHERE {_SCOPE} AND chain_id = $4"
        + (" FOR UPDATE" if lock else ""),
        *args,
        chain_id,
    )
    return _chain_from_rows(chain_row, link_rows)


def member_run(mission_id: UUID, run_id: UUID, projection: RunProjection) -> MemberRun:
    return MemberRun(
        mission_id=mission_id,
        run_id=run_id,
        run_key=projection.run_id,
        phase=projection.phase,
        terminal_outcome=projection.terminal_outcome,
        required_obligations=projection.required_obligation_refs,
        accepted_obligations=frozenset(
            item.obligation_ref for item in projection.accepted_obligation_evidence
        ),
        accepted_outputs=tuple(item.output_ref for item in projection.accepted_output_evidence),
        evidence_frontier_digest=projection.evidence_frontier_digest,
    )


async def _member_runs(
    connection: asyncpg.Connection, args: tuple[Any, ...], mission_ids: Iterable[UUID]
) -> dict[UUID, MemberRun]:
    rows = await connection.fetch(
        f"""
        SELECT DISTINCT ON (mission_id) mission_id, run_id, projection
        FROM mission_control.mission_run
        WHERE {_SCOPE} AND mission_id = ANY($4::uuid[])
        ORDER BY mission_id, created_at DESC, run_id DESC
        """,
        *args,
        list(mission_ids),
    )
    return {
        row["mission_id"]: member_run(
            row["mission_id"],
            row["run_id"],
            RunProjection.model_validate(_load(row["projection"])),
        )
        for row in rows
    }


async def _supplied_artifacts(
    connection: asyncpg.Connection, args: tuple[Any, ...], request_scope: str, refs: Sequence[str]
) -> tuple[SuppliedArtifact, ...]:
    """Custody metadata for the supplier's accepted output refs (no payload is read)."""

    found: list[SuppliedArtifact] = []
    for ref in refs:
        candidate_id = parse_workspace_candidate_ref(ref)
        if candidate_id is not None:
            row = await connection.fetchrow(
                f"""
                SELECT descriptor, object_ref, content_digest, size_bytes, logical_path
                FROM mission_control.workspace_candidate_descriptor
                WHERE {_SCOPE} AND candidate_key = $4
                """,
                *args,
                candidate_id,
            )
            if row is None:
                continue
            descriptor = _load(row["descriptor"]) or {}
            found.append(
                SuppliedArtifact(
                    source_ref=ref,
                    content_digest=row["content_digest"],
                    size_bytes=int(row["size_bytes"]),
                    media_type=str(descriptor.get("media_type") or "application/octet-stream"),
                    durable_ref=durable_input_locator(
                        row["object_ref"], row["content_digest"], int(row["size_bytes"])
                    ),
                    logical_path=row["logical_path"],
                )
            )
            continue
        parsed = parse_artifact_ref(ref)
        if parsed is None or parsed[0] != request_scope:
            continue
        row = await connection.fetchrow(
            f"""
            SELECT detail, object_key, content_digest, byte_size, media_type
            FROM mission_control.artifact
            WHERE {_SCOPE} AND artifact_key = $4
            """,
            *args,
            parsed[2],
        )
        if row is None or row["object_key"] is None:
            continue
        detail = _load(row["detail"]) or {}
        if detail.get("durable_reference") != ref:
            continue
        found.append(
            SuppliedArtifact(
                source_ref=ref,
                content_digest=row["content_digest"],
                size_bytes=int(row["byte_size"]),
                media_type=str(row["media_type"] or "application/octet-stream"),
                durable_ref=durable_input_locator(
                    row["object_key"], row["content_digest"], int(row["byte_size"])
                ),
                logical_path=str(detail.get("logical_path") or parsed[2]),
            )
        )
    return tuple(found)


# ------------------------------------------------------------------------------------------
# The release hook
# ------------------------------------------------------------------------------------------


class ChainReleaseHook:
    """``canonical.append_events`` post-append hook applying the chain reducer in-transaction."""

    def __init__(self, reducer: ChainReducer | None = None) -> None:
        self._reducer = reducer or ChainReducer()

    async def __call__(
        self,
        connection: asyncpg.Connection,
        args: tuple[Any, ...],
        appended: mc.AppendedEvents,
    ) -> None:
        if not any(event.event_type in CHAIN_RELEVANT_EVENT_TYPES for event in appended.events):
            return
        chain_ids = await connection.fetch(
            f"""
            SELECT DISTINCT chain_id FROM mission_control.chain_link
            WHERE {_SCOPE} AND (from_mission_id = $4 OR to_mission_id = $4)
            ORDER BY chain_id
            """,
            *args,
            appended.mission_id,
        )
        for row in chain_ids:
            await self._reduce_chain(connection, args, appended, row["chain_id"])

    async def _reduce_chain(
        self,
        connection: asyncpg.Connection,
        args: tuple[Any, ...],
        appended: mc.AppendedEvents,
        chain_id: UUID,
    ) -> None:
        loaded = await _load_chain(connection, args, chain_id, lock=True)
        if loaded is None:
            return
        chain, versions = loaded
        if chain.lifecycle is ChainLifecycle.COMPLETED:
            return
        runs = await _member_runs(connection, args, (member.mission_id for member in chain.members))
        cause = appended.events[-1]
        at = cause.recorded_at
        transition = self._reducer.on_events(
            ChainSnapshot(chain=chain, link_versions=versions, runs=runs),
            cause_event_id=_uuid(cause.event_id),
            trigger_mission_id=appended.mission_id,
            occurred_at=at,
        )
        if transition.is_noop:
            return
        await _apply(connection, args, chain, versions, runs, transition, appended, at)


_CAUSE_NAMESPACE = UUID("4a1f7c3e-9b2d-5f60-8e1a-2c3d4b5a6f70")


def _uuid(value: str) -> UUID:
    """The cause event's identity as a UUID (envelope ids are not always UUIDs)."""

    try:
        return UUID(value)
    except ValueError:
        return uuid5(_CAUSE_NAMESPACE, value)


async def _apply(
    connection: asyncpg.Connection,
    args: tuple[Any, ...],
    chain: MissionChain,
    versions: dict[UUID, int],
    runs: dict[UUID, MemberRun],
    transition: ChainTransition,
    appended: mc.AppendedEvents,
    at: datetime,
) -> None:
    actor_ref = chain_actor_ref(chain.chain_id)
    request_scope = _request_scope(args)
    links = {link.link_id: link for link in chain.links}
    updates: dict[UUID, LinkUpdate] = {item.link_id: item for item in transition.link_updates}
    # Admissions first: the run and packet facts go onto the released links.
    released_facts: dict[UUID, tuple[UUID, str, str]] = {}
    admitted_runs: dict[UUID, MemberRun] = {}
    for admission in transition.admissions:
        run, packet = await _admit_consumer(
            connection, args, chain, admission, runs, at, request_scope, actor_ref
        )
        admitted_runs[admission.to_mission_id] = run
        packet_ref = f"context-packet:{packet.packet_digest}"
        for link_id in admission.link_ids:
            released_facts[link_id] = (run.run_id, packet_ref, packet.packet_digest)
        await _record_relationships(
            connection, args, chain, [links[item] for item in admission.link_ids], runs, run, at
        )
    for link_id in sorted(set(updates) | set(released_facts), key=str):
        update = updates.get(link_id)
        link = links[link_id]
        facts = released_facts.get(link_id)
        state = update.state if update is not None else link.state
        released_at = (
            at
            if state is ChainLinkState.RELEASED and link.released_at is None
            else (link.released_at)
        )
        result = await connection.execute(
            f"""
            UPDATE mission_control.chain_link
            SET state = $5, blocked_reason = $6, released_at = $7, released_run_id = $8,
                packet_ref = $9, packet_digest = $10, version = version + 1, updated_at = $11
            WHERE {_SCOPE} AND link_id = $4 AND version = $12
            """,
            *args,
            link_id,
            state.value,
            update.blocked_reason if update is not None else link.blocked_reason,
            released_at,
            facts[0] if facts else link.released_run_id,
            facts[1] if facts else link.packet_ref,
            facts[2] if facts else link.packet_digest,
            at,
            versions[link_id],
        )
        if result != "UPDATE 1":
            raise RunVersionConflict(f"chain link {link_id} moved concurrently")
    chain_version = chain.version
    for chain_update in transition.chain_updates:
        result = await connection.execute(
            f"""
            UPDATE mission_control.mission_chain
            SET lifecycle = $5, phase = $6, terminal_outcome = $7, version = version + 1,
                updated_at = $8
            WHERE {_SCOPE} AND chain_id = $4 AND version = $9
            """,
            *args,
            chain.chain_id,
            chain_update.lifecycle.value,
            chain_update.phase.value,
            chain_update.terminal_outcome.value if chain_update.terminal_outcome else None,
            at,
            chain_update.expected_version,
        )
        if result != "UPDATE 1":
            raise RunVersionConflict(f"chain {chain.chain_id} moved concurrently")
        chain_version += 1
    for cancellation in transition.cancellations:
        consumer = next(
            (mid for mid, run in runs.items() if run.run_id == cancellation.run_id), None
        )
        if consumer is None:
            continue
        await _insert_intent(
            connection,
            args,
            ChainIntent(
                intent_kind="cancel_run",
                delivery_key=f"chain-cancel:{cancellation.run_key}",
                request_scope=request_scope,
                chain_id=chain.chain_id,
                mission_id=consumer,
                run_id=cancellation.run_id,
                run_key=cancellation.run_key,
                link_ids=(cancellation.link_id,),
                actor_ref=actor_ref,
                reason=cancellation.reason,
            ),
            appended.ledger_commit_id,
            at,
        )
    if transition.events:
        all_runs = {**runs, **admitted_runs}
        await _write_chain_events(
            connection,
            args,
            chain,
            transition.events,
            released_facts,
            all_runs,
            cause=appended.events[-1],
            version=chain_version,
            at=at,
            actor_ref=actor_ref,
        )


def _request_scope(args: tuple[Any, ...]) -> str:
    installation_id, application_id, tenant_id = args[:3]
    return f"mc/{installation_id}/{application_id}/{tenant_id}"


async def _admit_consumer(
    connection: asyncpg.Connection,
    args: tuple[Any, ...],
    chain: MissionChain,
    admission: ConsumerAdmission,
    runs: Mapping[UUID, MemberRun],
    at: datetime,
    request_scope: str,
    actor_ref: str,
) -> tuple[MemberRun, ContextPacket]:
    row = await connection.fetchrow(
        f"""
        SELECT mission_key, revision_id, family, initial_goal, autostart, run_request,
               verified_configuration, request_fingerprint
        FROM mission_control.chain_member_admission
        WHERE {_SCOPE} AND chain_id = $4 AND mission_id = $5
        """,
        *args,
        chain.chain_id,
        admission.to_mission_id,
    )
    if row is None:
        raise ChainReleaseError(
            f"chain {chain.chain_id} has no frozen admission for mission {admission.to_mission_key}"
        )
    frozen = ChainMemberAdmission(
        chain_id=chain.chain_id,
        mission_id=admission.to_mission_id,
        mission_key=row["mission_key"],
        revision_id=row["revision_id"],
        family=row["family"],
        initial_goal=row["initial_goal"],
        autostart=row["autostart"],
        run_request=_load(row["run_request"]),
        verified_configuration=_load(row["verified_configuration"]),
    )
    request = frozen.run_request.model_copy(update={"requested_at": at})
    mutation = accepted_admission_mutation(request, frozen.verified_configuration)
    decision = mutation.decision
    if decision.request_fingerprint != row["request_fingerprint"]:
        raise IdempotencyConflict("frozen chain member admission changed after submit")
    assert (
        mutation.projection is not None
        and mutation.budget is not None
        and mutation.effects is not None
        and mutation.transition is not None
    )
    if mutation.budget.parent_account_id is not None:
        raise ChainReleaseError("chain members carry their own budget; no parent account")
    run_uuid = await admit_run_for_revision(
        connection,
        args,
        mutation,
        mission_id=admission.to_mission_id,
        revision_id=frozen.revision_id,
        created_by=actor_ref,
    )
    consumer = member_run(admission.to_mission_id, run_uuid, mutation.projection)
    links = [link for link in chain.links if link.link_id in set(admission.link_ids)]
    suppliers = {link.from_mission_id for link in links if link.from_mission_id is not None}
    supplied = {
        supplier: await _supplied_artifacts(
            connection, args, request_scope, runs[supplier].accepted_outputs
        )
        for supplier in sorted(suppliers, key=str)
        if supplier in runs
    }
    packed = pack(
        chain_pack_request(
            chain=chain,
            consumer_mission_id=admission.to_mission_id,
            consumer_revision_id=frozen.revision_id,
            consumer_run_key=consumer.run_key,
            links=links,
            supplier_runs=runs,
            supplied=supplied,
            sealed_at=at,
            request_scope=request_scope,
        )
    )
    if isinstance(packed, PackFailure):
        raise ChainReleaseError(f"chain packet rejected: {packed.code.value}: {packed.message}")
    await _insert_context_selection(connection, args, packed, actor_ref)
    if frozen.autostart:
        await _insert_intent(
            connection,
            args,
            ChainIntent(
                intent_kind="start_run",
                delivery_key=f"chain-start:{consumer.run_key}",
                request_scope=request_scope,
                chain_id=chain.chain_id,
                mission_id=admission.to_mission_id,
                run_id=run_uuid,
                run_key=consumer.run_key,
                family=frozen.family,
                initial_goal=frozen.initial_goal,
                link_ids=admission.link_ids,
                actor_ref=actor_ref,
            ),
            None,
            at,
        )
    return consumer, packed


async def _insert_context_selection(
    connection: asyncpg.Connection, args: tuple[Any, ...], packet: ContextPacket, actor_ref: str
) -> None:
    selection = context_selection_record(packet)
    target = packet.target
    await connection.execute(
        """INSERT INTO mission_control.context_selection
           (installation_id, application_id, tenant_id, context_selection_id,
            selection_key, packet_key, run_key, node_key, activation_key, attempt_no,
            generation, purpose, packer_version, packet_digest, prompt_plan_digest,
            file_plan_digest, packet, selection, sealed_at, created_at,
            created_by_actor_ref)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,
                   $17::jsonb,$18::jsonb,$19,$19,$20)""",
        *args,
        uuid7(),
        selection.selection_id,
        packet.packet_id,
        target.run_id,
        target.node_key,
        target.activation_id,
        target.attempt_no,
        target.generation,
        target.purpose.value,
        packet.packer_version,
        packet.packet_digest,
        selection.prompt_plan_digest,
        selection.file_plan_digest,
        packet.model_dump_json(),
        selection.model_dump_json(),
        packet.sealed_at,
        actor_ref,
    )


async def _record_relationships(
    connection: asyncpg.Connection,
    args: tuple[Any, ...],
    chain: MissionChain,
    links: Sequence[ChainLink],
    runs: Mapping[UUID, MemberRun],
    consumer: MemberRun,
    at: datetime,
) -> None:
    """One Mission Graph row per released link (workflow-types/06 ``supplies``/``depends_on``)."""

    for link in links:
        supplier = runs.get(link.from_mission_id) if link.from_mission_id else None
        if supplier is None:
            continue
        await connection.execute(
            """
            INSERT INTO mission_control.mission_relationship (installation_id, application_id,
                tenant_id, relationship_id, relationship_key, source_run_id, target_run_id,
                kind, invocation_ref, grant_ref, projected_output_policy, detail, version,
                updated_at, created_at, created_by_actor_ref)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NULL, $10::jsonb, $11::jsonb, 1, $12,
                $12, $13)
            ON CONFLICT DO NOTHING
            """,
            *args,
            uuid7(),
            f"chain:{chain.chain_id}:{link.link_key}",
            supplier.run_id,
            consumer.run_id,
            link.kind.value,
            f"chain_link:{link.link_id}",
            json.dumps(
                {"outputs": [binding.output_name for binding in link.bindings]},
                sort_keys=True,
            ),
            json.dumps(
                {
                    "chain_id": str(chain.chain_id),
                    "link_id": str(link.link_id),
                    "link_key": link.link_key,
                    "release_condition": link.on.model_dump(mode="json", exclude_none=True),
                    "bindings": [binding.model_dump(mode="json") for binding in link.bindings],
                },
                sort_keys=True,
            ),
            at,
            chain_actor_ref(chain.chain_id),
        )


async def _insert_intent(
    connection: asyncpg.Connection,
    args: tuple[Any, ...],
    intent: ChainIntent,
    ledger_commit_id: UUID | None,
    at: datetime,
) -> None:
    destination = (
        START_RUN_DESTINATION if intent.intent_kind == "start_run" else CANCEL_RUN_DESTINATION
    )
    await connection.execute(
        """
        INSERT INTO mission_control.outbox (
            installation_id, application_id, tenant_id, outbox_id, ledger_commit_id,
            event_id, delivery_key, destination_kind, event_type, aggregate_key,
            aggregate_version, aggregate_sequence, payload, payload_ref, delivery_state,
            attempts, next_attempt_at, version, created_at, created_by_actor_ref
        )
        VALUES ($1, $2, $3, $4, $5, NULL, $6, $7, $8, NULL, NULL, NULL, $9::jsonb, NULL,
                'pending', 0, $10, 1, $10, $11)
        ON CONFLICT DO NOTHING
        """,
        *args,
        uuid7(),
        ledger_commit_id,
        intent.delivery_key,
        destination,
        f"chain.{intent.intent_kind}",
        intent.model_dump_json(),
        at,
        intent.actor_ref,
    )


async def _write_chain_events(
    connection: asyncpg.Connection,
    args: tuple[Any, ...],
    chain: MissionChain,
    events: Sequence[ChainEvent],
    released_facts: Mapping[UUID, tuple[UUID, str, str]],
    runs: Mapping[UUID, MemberRun],
    *,
    cause: DomainEventEnvelope,
    version: int,
    at: datetime,
    actor_ref: str,
) -> None:
    """Every chain event into every member stream: one envelope, one ``seq`` per mission."""

    actor = ActorContext(actor_id=actor_ref)
    envelopes: list[DomainEventEnvelope] = []
    for index, event in enumerate(events, start=1):
        payload = dict(event.payload)
        if (
            event.event_type == "chain_link.released"
            and event.link_id is not None
            and event.link_id in released_facts
        ):
            run_id, packet_ref, packet_digest = released_facts[event.link_id]
            payload.update(
                released_run_id=str(run_id), packet_ref=packet_ref, packet_digest=packet_digest
            )
        envelopes.append(
            DomainEventEnvelope(
                event_id=str(chain_event_id(chain.chain_id, event)),
                event_type=event.event_type,
                aggregate_id=f"chain:{chain.chain_id}",
                aggregate_version=version,
                sequence=index,
                is_version_final=index == len(events),
                occurred_at=at,
                recorded_at=at,
                actor=actor,
                correlation_id=f"chain:{chain.chain_id}",
                causation_id=cause.event_id,
                payload=payload,
            )
        )
    for member in sorted(chain.members, key=lambda item: item.order):
        run = runs.get(member.mission_id)
        await mc.append_mission_events(
            connection,
            args,
            mission_id=member.mission_id,
            run_id=run.run_id if run is not None else None,
            commit_key=f"chain:{chain.chain_id}:v{version}:{cause.event_id}:{member.mission_id}",
            events=envelopes,
            actor_ref=actor_ref,
        )


def install_chain_release_hook() -> None:
    """Register the chain reducer on the canonical ledger writer (idempotent)."""

    mc.register_post_append_hook(HOOK_NAME, ChainReleaseHook())


# ------------------------------------------------------------------------------------------
# Intents for the relay
# ------------------------------------------------------------------------------------------


class PostgresChainIntentStore:
    """Lease and acknowledge ``mc.chain.*`` outbox intents (``FOR UPDATE SKIP LOCKED``)."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def lease(
        self,
        request_scope: str,
        *,
        lease_owner: str,
        lease_until: datetime,
        now: datetime,
        limit: int,
    ) -> tuple[ChainIntent, ...]:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            rows = await connection.fetch(
                f"""
                WITH due AS (
                    SELECT outbox_id FROM mission_control.outbox
                    WHERE {_SCOPE} AND destination_kind = ANY($8::text[])
                      AND (delivery_state = 'pending'
                           OR (delivery_state = 'leased' AND lease_expires_at <= $6))
                      AND next_attempt_at <= $6
                    ORDER BY global_position
                    LIMIT $7
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE mission_control.outbox AS box
                SET delivery_state = 'leased', lease_owner = $4, lease_expires_at = $5,
                    version = box.version + 1
                FROM due
                WHERE {mc.scoped("box")} AND box.outbox_id = due.outbox_id
                RETURNING box.global_position, box.payload, box.attempts
                """,
                *args,
                lease_owner,
                lease_until,
                now,
                limit,
                [START_RUN_DESTINATION, CANCEL_RUN_DESTINATION],
            )
        return tuple(
            ChainIntent.model_validate({**_load(row["payload"]), "attempts": int(row["attempts"])})
            for row in sorted(rows, key=lambda item: item["global_position"])
        )

    async def mark_delivered(
        self, request_scope: str, delivery_key: str, *, lease_owner: str, delivered_at: datetime
    ) -> None:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            result = await connection.execute(
                f"""
                UPDATE mission_control.outbox
                SET attempts = attempts + 1, delivered_at = $6, delivery_state = 'delivered',
                    lease_owner = NULL, lease_expires_at = NULL, version = version + 1
                WHERE {_SCOPE} AND delivery_key = $4 AND lease_owner = $5
                  AND delivery_state = 'leased'
                """,
                *args,
                delivery_key,
                lease_owner,
                delivered_at,
            )
        if result != "UPDATE 1":
            raise RunVersionConflict(f"chain intent lease lost: {delivery_key}")

    async def mark_failed(
        self,
        request_scope: str,
        delivery_key: str,
        *,
        lease_owner: str,
        retry_at: datetime,
        dead: bool,
    ) -> None:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            await connection.execute(
                f"""
                UPDATE mission_control.outbox
                SET attempts = attempts + 1, next_attempt_at = $6,
                    delivery_state = CASE WHEN $7 THEN 'dead' ELSE 'pending' END,
                    lease_owner = NULL, lease_expires_at = NULL, version = version + 1
                WHERE {_SCOPE} AND delivery_key = $4 AND lease_owner = $5
                """,
                *args,
                delivery_key,
                lease_owner,
                retry_at,
                dead,
            )


# ------------------------------------------------------------------------------------------
# Inspection projection
# ------------------------------------------------------------------------------------------


class PostgresChainReader:
    """``ChainReadPort``: the ``mc.chain.v1`` projection with each member's current run."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def inspect(self, request_scope: str, chain_id: UUID) -> ChainInspection | None:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            return await _inspection(connection, args, chain_id)

    async def chains_for_mission(
        self, request_scope: str, mission_id: UUID
    ) -> tuple[ChainInspection, ...]:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            rows = await connection.fetch(
                f"""
                SELECT DISTINCT chain_id FROM mission_control.chain_link
                WHERE {_SCOPE} AND (from_mission_id = $4 OR to_mission_id = $4)
                ORDER BY chain_id
                """,
                *args,
                mission_id,
            )
            found = [await _inspection(connection, args, row["chain_id"]) for row in rows]
        return tuple(item for item in found if item is not None)


async def _inspection(
    connection: asyncpg.Connection, args: tuple[Any, ...], chain_id: UUID
) -> ChainInspection | None:
    loaded = await _load_chain(connection, args, chain_id, lock=False)
    if loaded is None:
        return None
    chain, _ = loaded
    runs = await _member_runs(connection, args, (member.mission_id for member in chain.members))
    statuses = member_statuses(chain, {link.link_id: link.state for link in chain.links}, runs)
    return ChainInspection(
        chain=chain,
        members=tuple(
            ChainMemberRun(
                mission_key=member.mission_key,
                mission_id=member.mission_id,
                order=member.order,
                status=statuses[member.mission_id],
                run_id=runs[member.mission_id].run_id if member.mission_id in runs else None,
                run_key=runs[member.mission_id].run_key if member.mission_id in runs else None,
                phase=runs[member.mission_id].phase.value if member.mission_id in runs else None,
                terminal_outcome=(
                    outcome.value
                    if member.mission_id in runs
                    and (outcome := runs[member.mission_id].terminal_outcome) is not None
                    else None
                ),
            )
            for member in sorted(chain.members, key=lambda item: item.order)
        ),
    )


class PostgresChainSupplies:
    """``ChainSupplyPort``: the sealed ``chain_link`` packet of a released consumer run."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def supply_for(self, run_id: str, *, request_scope: str) -> ChainSupply | None:
        async with self._pool.acquire() as connection, connection.transaction():
            await mc.begin(connection, request_scope)
            value = await connection.fetchval(
                f"""
                SELECT packet FROM mission_control.context_selection
                WHERE {_SCOPE} AND run_key = $4 AND purpose = 'chain_link'
                ORDER BY sealed_at LIMIT 1
                """,
                *mc.scope_args(parse_request_scope(request_scope)),
                run_id,
            )
        if value is None:
            return None
        return chain_supply_from_packet(ContextPacket.model_validate(_load(value)))
