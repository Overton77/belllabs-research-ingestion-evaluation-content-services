"""MP-13 on PostgreSQL 17: provider-subagent frames never open, close or end the parent's
turn, session or harness execution; a resent closing frame writes one `session_turn`; the
turn's usage follows the lane rule (folded for Codex child threads, excluded for Claude
subagents); and the public aliases `activation.completed` and `run.completed` are delivered
once each over a signed webhook whose receipts reference the canonical mission events.

The provider inputs are the hand-written FIXTURES under `tests/unit/frames/fixtures/`
(labelled there as not live recordings); they prove this code on real storage, not live
provider behaviour.
"""

from __future__ import annotations

import json
from datetime import timedelta
from typing import Any
from uuid import UUID, uuid4

import pytest

from mission_control.adapters.operations.runtime_ports import EnvironmentSecretResolver
from mission_control.adapters.postgres.frames.repository import PostgresFrameRepository
from mission_control.adapters.postgres.subscriptions.store import PostgresSubscriptionStore
from mission_control.adapters.subscriptions.webhook import HttpxWebhookTransport
from mission_control.application.frames.kinds import UnknownKindCounter
from mission_control.application.frames.reducer import (
    CompletionEvidence,
    FrameFactProjector,
    FrameFactTarget,
)
from mission_control.application.frames.writer import FrameWriter
from mission_control.application.subscriptions.aliases import derived_event_id
from mission_control.application.subscriptions.relay import SubscriptionRelay
from mission_control.application.subscriptions.service import SubscriptionService
from mission_control.domain.frames.contracts import FrameKind, FrameObservation, LaneProfile
from mission_control.domain.policies.contracts import (
    CancelAction,
    CommandStatus,
    RecordUsageAction,
    RunOutcome,
    TerminalizationProposal,
    TerminalizeAction,
)
from mission_control.domain.subscriptions.contracts import SubscriptionRequest, verify_signature
from tests.fixtures.mission_control_common_db import CommonDatabase
from tests.fixtures.provider_frames import StepClock
from tests.integration.postgres.frames_common import AdmittedUnit, admit_unit_attempt
from tests.integration.postgres.runtime_common import common_db as common_db
from tests.integration.postgres.runtime_common import owner_rows, scoped_command
from tests.integration.postgres.test_frame_facts_postgres import projector_actor
from tests.integration.postgres.test_subscriptions import (
    READER,
    SECRET_ENV,
    SECRET_VALUE,
    Clock,
    Receiver,
)
from tests.unit.frames.test_provider_lanes import claude_observations, codex_app_observations
from tests.unit.run_control.test_run_control import (
    EMPTY_EVIDENCE_DIGEST,
    NOW,
    WORKFLOW_DIGEST,
)

pytestmark = pytest.mark.common_db

COVERED = CompletionEvidence(
    declared_outputs=frozenset({"report"}), registered_outputs=frozenset({"report"})
)


async def identity_rows(db: CommonDatabase, harness_execution_id: UUID) -> dict[str, Any]:
    execution = await owner_rows(
        db,
        "SELECT lifecycle FROM mission_control.harness_execution WHERE harness_execution_id = $1",
        harness_execution_id,
    )
    sessions = await owner_rows(
        db,
        "SELECT session_id, state FROM mission_control.agent_session "
        "WHERE harness_execution_id = $1",
        harness_execution_id,
    )
    turns = await owner_rows(
        db,
        "SELECT t.turn_no, t.native_turn_ref, t.usage::text AS usage "
        "FROM mission_control.session_turn t JOIN mission_control.agent_session s "
        "ON s.session_id = t.session_id WHERE s.harness_execution_id = $1 ORDER BY t.turn_no",
        harness_execution_id,
    )
    return {
        "lifecycle": execution[0]["lifecycle"],
        "sessions": [row["state"] for row in sessions],
        "turns": [
            (row["turn_no"], row["native_turn_ref"], json.loads(row["usage"])) for row in turns
        ],
    }


def tokens(usage: dict[str, Any], dimension: str) -> Any:
    return usage["dimensions"][dimension]["value"]


async def run_events(db: CommonDatabase, run_key: str) -> list[Any]:
    return await owner_rows(
        db,
        """
        SELECT e.seq, e.event_id, e.event_type, e.mission_id, e.payload
        FROM mission_control.mission_event e
        JOIN mission_control.mission_run r
          ON r.installation_id = e.installation_id AND r.application_id = e.application_id
         AND r.tenant_id = e.tenant_id AND r.run_id = e.run_id
        WHERE r.run_key = $1
        ORDER BY e.seq
        """,
        run_key,
    )


async def write(frames: PostgresFrameRepository, handle: Any, items: list[FrameObservation]):
    await FrameWriter(frames, handle, clock=StepClock()).write(items)


@pytest.mark.asyncio
async def test_codex_child_thread_never_closes_the_parent_on_postgres(
    common_db: CommonDatabase,
) -> None:
    pool = await common_db.pool(max_size=4)
    try:
        admitted = await admit_unit_attempt(pool, common_db)
        frames = PostgresFrameRepository(pool)
        handle = await frames.open_execution(
            admitted.start(lane=LaneProfile.CODEX, native_session_ref="thr_root")
        )
        observations = codex_app_observations(UnknownKindCounter())
        root_end = next(
            index
            for index, item in enumerate(observations)
            if item.kind == FrameKind.TURN_ENDED and item.subordinate_ref is None
        )
        before = observations[:root_end]
        # The child thread's turn/completed and its result frame are already stored here.
        assert {FrameKind.TURN_ENDED, FrameKind.RUN_RESULT} <= {
            item.kind for item in before if item.subordinate_ref == "codex:thread:thr_child"
        }
        writer = FrameWriter(frames, handle, clock=StepClock())
        await writer.write(before)
        midway = await identity_rows(common_db, handle.harness_execution_id)
        assert midway == {"lifecycle": "live", "sessions": ["open"], "turns": []}

        await writer.write(observations[root_end:])
        settled = await identity_rows(common_db, handle.harness_execution_id)
        assert settled["lifecycle"] == "ended" and settled["sessions"] == ["closed"]
        # The resent root turn/completed wrote no second row.
        assert [(no, ref) for no, ref, _ in settled["turns"]] == [(1, "turn_r1")]
        usage = settled["turns"][0][2]
        # Root `last` (800 + 500) plus the child thread's own call (400): folded once.
        assert tokens(usage, "input_tokens") == 1700
        assert tokens(usage, "output_tokens") == 400
        assert len(usage["dimensions"]) >= 4

        projector = FrameFactProjector(frames, admitted.run_control, actor=projector_actor())
        target = FrameFactTarget.from_handle(handle, run_key=admitted.run_key)
        first = await projector.project(target, completion=COVERED)
        assert first.rejected_reason is None and first.applied == first.derived
        assert (await projector.project(target, completion=COVERED)).applied == 0
        events = await run_events(common_db, admitted.run_key)
        types = [row["event_type"] for row in events]
        assert types.count("session.turn_completed") == 1
        assert types.count("attempt.completed") == 1
        assert types.count("session.started") == 1
        turn = json.loads(events[types.index("session.turn_completed")]["payload"])["payload"]
        assert turn["usage"]["dimensions"]["input_tokens"]["value"] == 1700
        tool_events = [
            json.loads(row["payload"])["payload"]
            for row in events
            if row["event_type"] == "tool_call.completed"
        ]
        assert [item["tool_call_ref"] for item in tool_events] == ["item_cmd_1"]
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_claude_subagent_usage_is_excluded_and_its_frames_never_close_the_parent(
    common_db: CommonDatabase,
) -> None:
    pool = await common_db.pool(max_size=4)
    try:
        admitted = await admit_unit_attempt(pool, common_db)
        frames = PostgresFrameRepository(pool)
        handle = await frames.open_execution(
            admitted.start(lane=LaneProfile.CLAUDE_AGENT_SDK, native_session_ref="sess-fixture-1")
        )
        observations = claude_observations(UnknownKindCounter())
        result_at = next(
            index for index, item in enumerate(observations) if item.kind == FrameKind.TURN_ENDED
        )
        # Synthetic, test-only: a subagent usage frame and a subagent result inside the
        # parent's turn. Claude's ResultMessage.usage excludes subagents and the SDK gives
        # no per-subagent token split, so neither may change the parent's turn.
        child = "claude:task:toolu_task_01"
        synthetic = [
            observations[0].model_copy(
                update={
                    "kind": FrameKind.USAGE,
                    "raw_kind": "test.subagent_usage",
                    "provider_key": "test:subagent-usage",
                    "subordinate_ref": child,
                    "body": {"input_tokens": 9999, "output_tokens": 9999},
                    "native_turn_ref": None,
                    "tool_call_ref": None,
                }
            ),
            observations[0].model_copy(
                update={
                    "kind": FrameKind.RUN_RESULT,
                    "raw_kind": "test.subagent_result",
                    "provider_key": "test:subagent-result",
                    "subordinate_ref": child,
                    "body": {"subtype": "success", "is_error": False},
                    "native_turn_ref": None,
                    "tool_call_ref": None,
                }
            ),
        ]
        writer = FrameWriter(frames, handle, clock=StepClock())
        await writer.write([*observations[:result_at], *synthetic])
        midway = await identity_rows(common_db, handle.harness_execution_id)
        assert midway["lifecycle"] != "ended" and midway["sessions"] == ["open"]
        assert midway["turns"] == []

        await writer.write(observations[result_at:])
        settled = await identity_rows(common_db, handle.harness_execution_id)
        assert settled["lifecycle"] == "ended" and settled["sessions"] == ["closed"]
        assert len(settled["turns"]) == 1  # the duplicated ResultMessage settled once
        usage = settled["turns"][0][2]
        assert tokens(usage, "input_tokens") == 2000
        assert tokens(usage, "output_tokens") == 640
        assert tokens(usage, "cost_micros") == 42_100

        projector = FrameFactProjector(frames, admitted.run_control, actor=projector_actor())
        target = FrameFactTarget.from_handle(handle, run_key=admitted.run_key)
        first = await projector.project(target, completion=COVERED)
        assert first.rejected_reason is None and first.applied == first.derived
        events = await run_events(common_db, admitted.run_key)
        types = [row["event_type"] for row in events]
        assert types.count("attempt.completed") == 1
        assert types.count("session.turn_completed") == 1
        outcome = json.loads(events[types.index("attempt.completed")]["payload"])["payload"]
        assert outcome["outcome"] == "succeeded"
        tools = [
            json.loads(row["payload"])["payload"]
            for row in events
            if row["event_type"] == "tool_call.completed"
        ]
        assert sorted(item["tool_call_ref"] for item in tools) == [
            "toolu_sub_bash_01",
            "toolu_task_01",
        ]
    finally:
        await pool.close()


async def terminalize_cancelled(db: CommonDatabase, admitted: AdmittedUnit) -> None:
    authority = admitted.run_control
    run_key = admitted.run_key

    async def run(command_id: str, action: object) -> Any:
        current = await authority.get_run(db.scope(), run_key)
        decision = await authority.execute(
            scoped_command(db, run_key, current.version, command_id, action)
        )
        assert decision.status == CommandStatus.ACCEPTED, decision
        return decision

    await run(f"cancel-{uuid4()}", CancelAction())
    await run(
        f"release-{uuid4()}",
        RecordUsageAction(
            usage_id="usage:release",
            reservation_id="baseline",
            actual_amounts={},
            release_amounts={"tokens.total": 20},
        ),
    )
    projection = await authority.get_run(db.scope(), run_key)
    terminal = await run(
        f"terminalize-{uuid4()}",
        TerminalizeAction(
            proposal=TerminalizationProposal(
                proposal_id="terminal",
                expected_run_version=projection.version,
                workflow_type_digest=WORKFLOW_DIGEST,
                obligation_revision="obligations:1",
                evidence_frontier_digest=projection.evidence_frontier_digest,
                accepted_obligation_evidence_digest=EMPTY_EVIDENCE_DIGEST,
                proposing_execution_binding_ref="execution:test",
                required_obligations_accepted=True,
                cancellation_settled=True,
                budget_settled=True,
                effects_settled=True,
                proposed_at=NOW + timedelta(minutes=30),
            )
        ),
    )
    assert terminal.terminal_outcome == RunOutcome.CANCELLED


@pytest.mark.asyncio
async def test_public_aliases_are_delivered_once_with_canonical_receipts(
    common_db: CommonDatabase,
) -> None:
    pool = await common_db.pool(max_size=4)
    receiver = Receiver()
    transport = HttpxWebhookTransport()
    try:
        admitted = await admit_unit_attempt(pool, common_db)
        frames = PostgresFrameRepository(pool)
        handle = await frames.open_execution(
            admitted.start(lane=LaneProfile.CODEX, native_session_ref="thr_root_alias")
        )
        observations = [
            item.model_copy(update={"native_session_ref": "thr_root_alias"})
            for item in codex_app_observations(UnknownKindCounter())
        ]
        await write(frames, handle, observations)
        projector = FrameFactProjector(frames, admitted.run_control, actor=projector_actor())
        await projector.project(
            FrameFactTarget.from_handle(handle, run_key=admitted.run_key), completion=COVERED
        )
        await terminalize_cancelled(common_db, admitted)
        events = await run_events(common_db, admitted.run_key)
        mission_id = events[0]["mission_id"]
        canonical = {
            row["event_type"]: row
            for row in events
            if row["event_type"] in {"attempt.completed", "workflow_run.terminalize"}
        }
        assert [row["event_type"] for row in events].count("attempt.completed") == 1
        assert [row["event_type"] for row in events].count("workflow_run.terminalize") == 1

        url = await receiver.start()
        clock = Clock()
        store = PostgresSubscriptionStore(pool, common_db.scope())
        subscription = await SubscriptionService(store, clock=clock).subscribe(
            SubscriptionRequest.model_validate(
                {
                    "target": "mission",
                    "target_id": str(mission_id),
                    "events": ["run.completed", "activation.completed", "human_task.opened"],
                    "channel": {
                        "kind": "webhook",
                        "url": url,
                        "secret_ref": f"environment:{SECRET_ENV}",
                    },
                    "after_seq": 0,
                }
            ),
            READER,
        )
        relay = SubscriptionRelay(
            store,
            transport=transport,
            secrets=EnvironmentSecretResolver({SECRET_ENV: SECRET_VALUE}),
            owner="relay-mp13",
            clock=clock,
            jitter=lambda: 0.5,
        )
        report = await relay.run_once()
        assert report.failed == [] and report.dead_lettered == []
        again = await relay.run_once()
        assert again.failed == []

        bodies = [json.loads(body) for _, body in receiver.received]
        for headers, body in receiver.received:
            assert verify_signature(SECRET_VALUE.encode(), body, headers["x-mc-signature"])
        attempt = canonical["attempt.completed"]
        terminal = canonical["workflow_run.terminalize"]
        assert [(item["event_type"], item["seq"]) for item in bodies] == [
            ("activation.completed", attempt["seq"]),
            ("run.completed", terminal["seq"]),
        ]
        assert bodies[0]["event_id"] == str(
            derived_event_id("activation.completed", str(attempt["event_id"]))
        )
        assert bodies[1]["event_id"] == str(
            derived_event_id("run.completed", str(terminal["event_id"]))
        )
        assert bodies[1]["causation_ref"] == f"mission_event:{terminal['event_id']}"
        # Receipts satisfy the mission_event foreign key with the canonical ids.
        receipts = await owner_rows(
            common_db,
            "SELECT event_id, seq, status FROM mission_control.subscription_delivery "
            "WHERE subscription_id = $1 ORDER BY seq",
            subscription.subscription_id,
        )
        assert [(row["event_id"], row["seq"]) for row in receipts] == [
            (attempt["event_id"], attempt["seq"]),
            (terminal["event_id"], terminal["seq"]),
        ]
        current = await store.get(subscription.subscription_id)
        assert current.cursor_seq == events[-1]["seq"]
        # Canonical event types are unchanged in the store: aliases created no state.
        stored_types = {row["event_type"] for row in await run_events(common_db, admitted.run_key)}
        assert not stored_types & {"run.completed", "activation.completed", "human_task.opened"}
    finally:
        await transport.aclose()
        await receiver.stop()
        await pool.close()
