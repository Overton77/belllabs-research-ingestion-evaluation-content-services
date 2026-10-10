"""SPEC-04 realtime acceptance on PostgreSQL 17, uvicorn, python-socketio and Redis (V16-V18, G6).

Real: `create_application` with its lifespan and restricted pools, the `MissionTokenVerifier`
(RS256 tokens from a test key), the registry check, forced-RLS reads, the reducer journal,
the frame writer and repository, uvicorn servers, python-socketio clients and, for G6, a
second OS process built by `bootstrap.realtime.create_asgi_app` with Redis fanout and the
0032 `LISTEN mc_stream_hint` source. Provider frames are the hand-written FIXTURES under
`tests/unit/frames/fixtures/` (not live recordings): they prove this code on real storage and
transport, not live provider behaviour.

- V16: a provider subagent starts and finishes while only its lifecycle is persisted (partial
  transcript access): lineage and visibility are exact, no child detail is invented, and
  child usage is never added to the parent's turn totals.
- V17: clients disconnect across commits while the hint fanout is dead; replay from
  PostgreSQL loses nothing, duplicates keep their event id and scope never changes.
- V18: expired idle credentials, forged generation cursors and foreign descendants are
  denied; a slow frame consumer is resynced within its bound.
- G6: two OS processes share Redis; presence, durable events and a command cross processes;
  a killed process's client resumes on the other from its acknowledged cursor.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import random
import socket
import subprocess
import sys
import time
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest
from joserfc.jwk import RSAKey

from mission_control.adapters.postgres.frames.repository import PostgresFrameRepository
from mission_control.application.frames.kinds import UnknownKindCounter
from mission_control.application.frames.lineage import provider_subordinates
from mission_control.application.frames.writer import FrameWriter
from mission_control.contracts.realtime import CommandReceiptProgress, LineageNotice
from mission_control.domain.frames.contracts import FrameKind, FrameObservation, LaneProfile
from mission_control.domain.policies.contracts import (
    CommandStatus,
    ReceiptState,
    StartAction,
)
from mission_control.interfaces.socketio.server import MissionSocketLimits
from tests.fixtures.mission_control_common_db import CommonDatabase
from tests.fixtures.provider_frames import StepClock
from tests.integration.postgres.frames_common import admit_unit_attempt
from tests.integration.postgres.realtime_process_app import SPEC_ENV, build
from tests.integration.postgres.runtime_common import common_db as common_db
from tests.integration.postgres.runtime_common import owner_rows, scoped_command, scoped_request
from tests.integration.postgres.test_mission_socket_postgres import (
    DSN_ENV,
    FAST,
    ORIGIN,
    REDIS_ENV,
    Stack,
    auth,
    mission,
    stack,
    subscribe,
)
from tests.integration.postgres.test_provider_lineage_postgres import identity_rows, tokens
from tests.unit.frames.test_provider_lanes import claude_observations, codex_app_observations
from tests.unit.run_control.test_boundary_commands import TARGET, delivered
from tests.unit.socketio.harness import MissionClient, serve

pytestmark = pytest.mark.common_db

REPO_ROOT = Path(__file__).resolve().parents[3]


class RealtimeClient(MissionClient):
    """Also records the `lineage` and `human_task_receipt` events."""

    def __init__(self) -> None:
        super().__init__()
        for name in ("lineage", "human_task_receipt"):
            self.sio.on(name, self._recorder(name), namespace="/missions")


def frames_subscription(execution: str, cursor: int | None, **extra: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "request_id": f"req-{uuid4()}",
        "application_id": "biotech",
        "target": {"kind": "execution", "id": execution},
        "streams": ["provider_frames"],
    }
    if cursor is not None:
        body["cursors"] = [
            {"stream": "provider_frames", "position": f"{execution}:{cursor}", "generation": 1}
        ]
    body.update(extra)
    return body


def lifecycle_only(observations: list[FrameObservation]) -> list[FrameObservation]:
    """Partial transcript access: the provider persisted only the child's task lifecycle."""

    return [
        item
        for item in observations
        if item.subordinate_ref is None or item.kind == FrameKind.STATUS
    ]


def child_usage(template: FrameObservation, child: str) -> FrameObservation:
    """Synthetic, test-only: a subagent usage frame inside the parent's turn (Claude reports
    no per-subagent token split; it must never reach the parent's totals)."""

    return template.model_copy(
        update={
            "kind": FrameKind.USAGE,
            "raw_kind": "test.subagent_usage",
            "provider_key": "test:subagent-usage",
            "subordinate_ref": child,
            "body": {"input_tokens": 9999, "output_tokens": 9999},
            "native_turn_ref": None,
            "tool_call_ref": None,
        }
    )


def nodes_seen(client: MissionClient) -> list[dict[str, Any]]:
    return [node for notice in client.events["lineage"] for node in notice["subordinates"]]


async def test_v16_subordinate_lineage_visibility_and_usage_over_the_socket(
    common_db: CommonDatabase, tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with stack(common_db, tmp_path, monkeypatch) as s:
        repository = PostgresFrameRepository(s.pool)
        # Claude: the subagent's lifecycle only, plus a synthetic child usage frame.
        claude = await repository.open_execution(
            s.admitted.start(lane=LaneProfile.CLAUDE_AGENT_SDK, native_session_ref="sess-fixture-1")
        )
        observations = lifecycle_only(claude_observations(UnknownKindCounter()))
        result_at = next(
            index for index, item in enumerate(observations) if item.kind == FrameKind.TURN_ENDED
        )
        observations.insert(result_at, child_usage(observations[0], "claude:task:toolu_task_01"))
        await FrameWriter(repository, claude, clock=StepClock()).write(observations)
        # Codex: a child thread with its own messages and one model call (full coverage).
        codex = await repository.open_execution(
            s.admitted.start(lane=LaneProfile.CODEX, native_session_ref="thr_root")
        )
        await FrameWriter(repository, codex, clock=StepClock()).write(
            codex_app_observations(UnknownKindCounter())
        )

        viewer = RealtimeClient()
        await viewer.connect(s.url, auth(s.token("viewer")))
        claude_id = str(claude.harness_execution_id)
        reply = await viewer.call(
            "subscribe", frames_subscription(claude_id, 0, include_descendants=True)
        )
        assert reply["ok"] is True, reply
        assert reply["snapshot_versions"]["provider_frames.lineage"] == "complete"
        await viewer.wait_for(
            lambda: any(
                e["kind"] == "run_result" and e["subordinate_ref"] is None
                for e in viewer.events["provider_frame"]
            ),
            within=20,
        )
        envelopes = viewer.events["provider_frame"]
        children = [e for e in envelopes if e["subordinate_ref"] is not None]
        # Only lifecycle (and the usage) frames exist for the child: nothing else is shown.
        assert {e["kind"] for e in children} == {"status", "usage"}
        stored = await repository.frames_for_execution(
            s.db.scope(), claude.harness_execution_id, 1, after_ordinal=0, limit=500
        )
        (expected,) = provider_subordinates(stored)
        assert expected.ref.visibility == "lifecycle_only" and expected.lifecycle == "ended"
        assert children[-1]["subordinate_ref"] == expected.ref.model_dump(mode="json")
        assert (expected.ref.native_child_ref, expected.ref.spawn_correlation) == (
            "task-a1",
            "toolu_task_01",
        )
        facts = [fact for e in children for fact in e["payload"]["normalized"]]
        assert facts.count("subordinate.started") == 1
        assert facts.count("subordinate.ended") == 1
        listing = [LineageNotice.model_validate(n) for n in viewer.events["lineage"]]
        assert listing[0].full is True and listing[0].subordinates == ()
        final = listing[-1].subordinates[-1]
        assert (final.ref.visibility, final.lifecycle, final.resolved) == (
            "lifecycle_only",
            "ended",
            True,
        )
        assert final.usage is not None and final.usage.tokens == "unattributable"
        (usage_frame,) = [e for e in children if e["kind"] == "usage"]
        assert usage_frame["payload"]["usage_attribution"] == {
            "scope": "subordinate",
            "tokens": "unattributable",
            "cost": "provider_inclusive",
            "counted_in_parent_turn": False,
            "add_to_parent": False,
        }
        # The parent's turn keeps the provider totals: 9999 child tokens are not added.
        claude_rows = await identity_rows(s.db, claude.harness_execution_id)
        assert len(claude_rows["turns"]) == 1
        assert tokens(claude_rows["turns"][0][2], "input_tokens") == 2000
        # Tool frames of the viewer (summary detail) carry no tool arguments or results.
        for envelope in envelopes:
            if envelope["kind"].startswith("tool_call"):
                assert "body_excerpt" not in envelope["payload"]

        codex_id = str(codex.harness_execution_id)
        assert (
            await viewer.call(
                "subscribe", frames_subscription(codex_id, 0, include_descendants=True)
            )
        )["ok"]
        await viewer.wait_for(
            lambda: any(
                e["execution_ref"].endswith(codex_id) and e["kind"] == "run_result"
                for e in viewer.events["provider_frame"]
                if e["subordinate_ref"] is None
            ),
            within=20,
        )
        codex_children = [
            e
            for e in viewer.events["provider_frame"]
            if e["execution_ref"].endswith(codex_id) and e["subordinate_ref"] is not None
        ]
        assert {e["subordinate_ref"]["visibility"] for e in codex_children[-1:]} == {"full"}
        codex_usage = [e for e in codex_children if e["kind"] == "usage"]
        assert codex_usage and all(
            e["payload"]["usage_attribution"]["tokens"] == "folded"
            and e["payload"]["usage_attribution"]["counted_in_parent_turn"] is True
            for e in codex_usage
        )
        codex_rows = await identity_rows(s.db, codex.harness_execution_id)
        assert tokens(codex_rows["turns"][0][2], "input_tokens") == 1700  # folded once
        await viewer.close()

        # A tenant-2 actor cannot see either execution or its descendants.
        intruder = RealtimeClient()
        await intruder.connect(s.url, auth(s.token("intruder", tenant="tenant-2")))
        denied = await intruder.call(
            "subscribe", frames_subscription(claude_id, 0, include_descendants=True)
        )
        absent = await intruder.call(
            "subscribe", frames_subscription(str(uuid4()), 0, include_descendants=True)
        )
        assert denied["error"]["code"] == absent["error"]["code"] == "TARGET_NOT_FOUND"
        assert denied["error"]["detail"] == absent["error"]["detail"]
        await asyncio.sleep(0.5)
        assert intruder.events["lineage"] == [] == intruder.events["provider_frame"]
        await intruder.close()


async def mission_of(db: CommonDatabase, run_key: str) -> UUID:
    rows = await owner_rows(
        db, "SELECT mission_id FROM mission_control.mission_run WHERE run_key = $1", run_key
    )
    return UUID(str(rows[0]["mission_id"]))


async def link_missions(db: CommonDatabase, upstream: UUID, downstream: UUID) -> UUID:
    tenant = db.tenants["tenant-1"]
    chain_id, link_id = uuid4(), uuid4()
    connection = await __import__("asyncpg").connect(db.owner_dsn)
    try:
        await connection.execute(
            """
            INSERT INTO mission_control.mission_chain (installation_id, application_id,
              tenant_id, chain_id, chain_key, title, manifest_digest, lifecycle, phase, members,
              version, created_at, updated_at, created_by_actor_ref)
            VALUES ($1, $2, $3, $4, $5, 'realtime proof', $6, 'running', 'releasing',
              $7::jsonb, 1, now(), now(), 'operator')
            """,
            db.installation_id,
            db.application_id,
            tenant,
            chain_id,
            f"chain-{chain_id.hex[:8]}",
            "sha256:" + "c" * 64,
            json.dumps(
                [
                    {
                        "mission_key": key,
                        "mission_id": str(mission),
                        "revision_id": str(uuid4()),
                        "order": order,
                    }
                    for order, (key, mission) in enumerate(
                        (("upstream", upstream), ("downstream", downstream))
                    )
                ]
            ),
        )
        await connection.execute(
            """
            INSERT INTO mission_control.chain_link (installation_id, application_id, tenant_id,
              link_id, chain_id, link_key, from_mission_key, to_mission_key, from_mission_id,
              to_mission_id, kind, bindings, release_condition, on_upstream_cancel,
              on_upstream_not_accepted, state, version, created_at, updated_at,
              created_by_actor_ref)
            VALUES ($1, $2, $3, $4, $5, 'downstream', 'upstream', 'downstream', $6, $7,
              'depends_on', '[]'::jsonb, '{"kind": "execution_complete"}'::jsonb, 'detach',
              'stop', 'armed', 1, now(), now(), 'operator')
            """,
            db.installation_id,
            db.application_id,
            tenant,
            link_id,
            chain_id,
            upstream,
            downstream,
        )
    finally:
        await connection.close()
    return link_id


async def release_link(db: CommonDatabase, link_id: UUID, run_key: str) -> None:
    connection = await __import__("asyncpg").connect(db.owner_dsn)
    try:
        await connection.execute(
            """
            UPDATE mission_control.chain_link link SET state = 'released', released_at = now(),
              released_run_id = run.run_id, version = link.version + 1, updated_at = now()
            FROM mission_control.mission_run run
            WHERE link.link_id = $1 AND run.run_key = $2
            """,
            link_id,
            run_key,
        )
    finally:
        await connection.close()


async def test_mission_descendants_are_scoped_linked_missions_with_lifecycle_visibility(
    common_db: CommonDatabase, tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with stack(common_db, tmp_path, monkeypatch) as s:
        upstream = await mission_of(s.db, s.admitted.run_key)
        downstream_run = await s.admitted.run_control.admit(
            scoped_request(s.db, request_id=f"downstream-{uuid4()}")
        )
        assert downstream_run.run_id is not None
        downstream = await mission_of(s.db, downstream_run.run_id)
        link_id = await link_missions(s.db, upstream, downstream)
        start = await s.last_seq()

        viewer = RealtimeClient()
        await viewer.connect(s.url, auth(s.token("viewer")))
        body = subscribe(s.admitted.run_key, start, include_descendants=True)
        body["target"] = {"kind": "mission", "id": str(upstream)}
        reply = await viewer.call("subscribe", body)
        assert reply["ok"] is True, reply
        assert reply["snapshot_versions"]["mission_events.descendants"] == "lifecycle_only"
        await viewer.wait_for(lambda: bool(viewer.events["lineage"]))
        (armed,) = LineageNotice.model_validate(viewer.events["lineage"][0]).subordinates
        assert armed.ref.kind == "linked_mission"
        assert armed.ref.spawn_correlation == f"chain_link:{link_id}"
        assert (armed.ref.visibility, armed.resolved) == ("unavailable", False)

        await release_link(s.db, link_id, downstream_run.run_id)
        await s.record_usage(1)  # the next committed page refreshes the listing
        await viewer.wait_for(
            lambda: len(viewer.events["lineage"]) >= 2 and bool(viewer.events["mission_event"]),
            within=10,
        )
        (released,) = LineageNotice.model_validate(viewer.events["lineage"][-1]).subordinates
        assert released.ref.native_child_ref == f"run:{downstream_run.run_id}"
        assert (released.ref.visibility, released.lifecycle) == ("lifecycle_only", "started")
        assert released.usage is not None and released.usage.tokens == "separate_mission"
        # The descendant's own journal is a separate cursor domain: never merged here.
        assert {e["mission_ref"] for e in viewer.events["mission_event"]} == {f"mission:{upstream}"}
        await viewer.close()

        intruder = RealtimeClient()
        await intruder.connect(s.url, auth(s.token("intruder", tenant="tenant-2")))
        body["request_id"] = f"req-{uuid4()}"
        denied = await intruder.call("subscribe", body)
        assert denied["error"]["code"] == "TARGET_NOT_FOUND"
        assert intruder.events["lineage"] == []
        await intruder.close()


class DeadHints:
    """A hint fanout that is down for the whole test (every attempt fails)."""

    def __init__(self) -> None:
        self.attempts = 0

    async def run(self, deliver: Callable[[Any], None], stop: asyncio.Event) -> None:
        while not stop.is_set():
            self.attempts += 1
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=0.1)


async def test_v17_clients_hop_servers_across_commits_while_fanout_is_down(
    common_db: CommonDatabase, tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    dead = DeadHints()
    key = RSAKey.generate_key(2048, parameters={"kid": "socket"})
    async with (
        stack(common_db, tmp_path, monkeypatch, hints=dead, key=key) as first,
        stack(
            common_db, tmp_path, monkeypatch, hints=dead, key=key, admitted=first.admitted
        ) as second,
    ):
        servers = (first, second)
        start = await first.last_seq()
        rng = random.Random(1709)
        received: dict[str, dict[int, set[str]]] = {"a": {}, "b": {}}
        hops = {"a": 0, "b": 0}
        writer_done = asyncio.Event()

        async def writer() -> None:
            await first.record_usage(30, pause=0.05)
            writer_done.set()

        async def reader(name: str) -> None:
            acked = start
            final: int | None = None
            while final is None or acked < final:
                server = servers[hops[name] % 2]  # every reconnect lands on the other server
                client = MissionClient()
                await client.connect(server.url, auth(server.token("viewer")))
                ack = await client.call("subscribe", subscribe(first.admitted.run_key, acked))
                assert ack["ok"] is True, ack
                await asyncio.sleep(rng.uniform(0.1, 0.5))
                for envelope in client.events["mission_event"]:
                    assert envelope["scope"]["tenant_id"] == str(first.db.tenants["tenant-1"])
                    seq = int(envelope["cursor"]["position"])
                    received[name].setdefault(seq, set()).add(envelope["event_id"])
                contiguous = acked
                while contiguous + 1 in received[name]:
                    contiguous += 1
                if contiguous > acked and rng.random() < 0.6:
                    reply = await client.call(
                        "ack",
                        {
                            "subscription_id": ack["subscription_id"],
                            "cursors": [mission(contiguous)],
                        },
                    )
                    assert reply["ok"] is True, reply
                    acked = contiguous
                await client.sio.eio.disconnect(abort=True)
                await client.close()
                hops[name] += 1
                if writer_done.is_set():
                    final = await first.last_seq()

        await asyncio.wait_for(asyncio.gather(writer(), reader("a"), reader("b")), timeout=180)
        final = await first.last_seq()
        expected = set(range(start + 1, final + 1))
        assert len(expected) >= 30
        for name in ("a", "b"):
            assert set(received[name]) == expected, name
            assert all(len(ids) == 1 for ids in received[name].values()), name
            assert hops[name] >= 2
        assert dead.attempts >= 2  # the fanout really was consulted and never delivered


async def test_v18_idle_expiry_forged_generation_and_bounded_frame_consumer(
    common_db: CommonDatabase, tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    limits = MissionSocketLimits(
        max_connection_queue_bytes=65_536, pump=FAST, reauthorize_seconds=0.5
    )
    async with stack(common_db, tmp_path, monkeypatch, limits=limits) as s:
        # An idle connection (no subscription, no client event) ends at its credential's exp.
        idle = MissionClient()
        await idle.connect(s.url, auth(s.token("viewer", ttl=2)))
        await idle.wait_for(lambda: not idle.sio.connected, within=10)
        assert [e["code"] for e in idle.events["stream_error"]] == ["UNAUTHORIZED"]

        repository = PostgresFrameRepository(s.pool)
        handle = await repository.open_execution(s.admitted.start())
        noisy = [
            FrameObservation(
                provider_key=f"noise-{index}",
                raw_kind="fixture.message",
                kind=FrameKind.MESSAGE,
                body={"text": "x" * 2_000, "index": index},
                native_turn_ref="turn-1",
                native_session_ref="thread-1",
            )
            for index in range(120)
        ]
        await FrameWriter(repository, handle, clock=StepClock()).write(noisy)
        execution = str(handle.harness_execution_id)
        viewer = RealtimeClient()
        await viewer.connect(s.url, auth(s.token("viewer")))
        forged = frames_subscription(execution, 0)
        forged["cursors"][0]["generation"] = 7
        assert (await viewer.call("subscribe", forged))["error"]["code"] == "CURSOR_AHEAD"
        # Never acknowledges: bounded by the 64 KiB connection budget, then resynced.
        assert (await viewer.call("subscribe", frames_subscription(execution, 0)))["ok"]
        await viewer.wait_for(lambda: bool(viewer.events["resync_required"]), within=20)
        notice = viewer.events["resync_required"][0]
        assert notice["code"] == "SLOW_CONSUMER" and notice["detached"] is True
        assert notice["replay_from"][0]["position"] == f"{execution}:0"
        delivered_bytes = sum(
            len(json.dumps(e, separators=(",", ":"))) for e in viewer.events["provider_frame"]
        )
        assert 0 < len(viewer.events["provider_frame"]) < 120
        assert delivered_bytes <= 65_536 + 8_192
        await viewer.close()


async def test_command_receipts_follow_the_durable_ledger_beyond_acceptance(
    common_db: CommonDatabase, tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with stack(common_db, tmp_path, monkeypatch) as s:
        run_control = s.admitted.run_control
        admitted = await run_control.admit(scoped_request(s.db, request_id=f"boundary-{uuid4()}"))
        assert admitted.run_id is not None
        run_key = admitted.run_id
        # A family execution target: acceptance is not delivery, delivery is not application.
        started = await run_control.execute(
            scoped_command(
                s.db, run_key, 1, f"start-{uuid4()}", StartAction(execution_target=TARGET)
            )
        )
        assert started.status == CommandStatus.ACCEPTED
        projection = await run_control.get_run(s.db.scope(), run_key)
        operator = MissionClient()
        await operator.connect(s.url, auth(s.token("operator")))
        command = {
            "request_id": f"req-{uuid4()}",
            "application_id": "biotech",
            "run_id": run_key,
            "command": {
                "request_id": str(uuid4()),
                "expected_version": projection.version,
                "expected_generation": 1,
                "target": {"id": run_key},
                "kind": "pause",
                "payload": {
                    "decision": {
                        "decision_id": "pause-socket",
                        "scope": ["run"],
                        "reason": "operator hold",
                        "authority_ref": "authority:lifecycle",
                    },
                    "runnable_work_remains": False,
                },
                "reason": "socket receipts",
            },
        }
        receipt = await operator.call("command", command)
        assert receipt["ok"] is True, receipt
        assert (receipt["stage"], receipt["final"], receipt["following"]) == (
            "accepted",
            False,
            True,
        )
        status = await run_control.get_boundary_command(
            s.db.scope(),
            run_key,
            receipt["receipt"]["delivery"]["command"]["idempotency_issuer"],
            receipt["receipt"]["delivery"]["command"]["command_id"],
        )
        assert status is not None
        # A worker delivers it, then the boundary rejects it (superseded).
        await run_control.record_boundary_receipt(s.db.scope(), delivered(status))
        await operator.wait_for(
            lambda: any(e.get("stage") == "delivered" for e in operator.events["command_receipt"])
        )
        await run_control.record_boundary_receipt(
            s.db.scope(),
            delivered(status).model_copy(
                update={
                    "ordinal": 2,
                    "state": ReceiptState.REJECTED,
                    "rejection_reason": "superseded",
                    "recorded_by": "family-boundary",
                }
            ),
        )
        await operator.wait_for(
            lambda: any(e.get("final") for e in operator.events["command_receipt"][1:])
        )
        progress = [
            CommandReceiptProgress.model_validate(body)
            for body in operator.events["command_receipt"][1:]
        ]
        assert [(item.stage, item.final) for item in progress] == [
            ("delivered", False),
            ("rejected", True),
        ]
        # The same ledger over HTTP: the socket reported what HTTP reports, nothing more.
        async with httpx.AsyncClient(base_url=s.url) as http:
            listed = await http.get(
                f"/v1/applications/biotech/runs/{run_key}/commands",
                headers={"Authorization": f"Bearer {s.token('operator')}"},
            )
        assert listed.status_code == 200, listed.text
        states = [
            [r["state"] for r in item["receipts"]]
            for item in listed.json()["commands"]
            if item["command"]["command_id"] == status.command.command_id
        ]
        assert states == [["accepted", "delivered", "rejected"]]
        await operator.close()


def free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


def spawn_api(spec: dict[str, Any], port: int, output: Any) -> subprocess.Popen[bytes]:
    env = {**os.environ, SPEC_ENV: json.dumps(spec), "PYTHONPATH": str(REPO_ROOT)}
    return subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "tests.integration.postgres.realtime_process_app:create",
            "--factory",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--lifespan",
            "on",
            "--log-level",
            "warning",
            "--ws",
            "websockets-sansio",
        ],
        cwd=REPO_ROOT,
        env=env,
        stdout=output,
        stderr=subprocess.STDOUT,
    )


def log_tail(log: Path) -> str:
    return log.read_text(errors="replace")[-2000:]


@asynccontextmanager
async def api_process(spec: dict[str, Any], log: Path) -> AsyncIterator[str]:
    port = free_port()
    with log.open("wb") as output:
        process = spawn_api(spec, port, output)
        url = f"http://127.0.0.1:{port}"
        try:
            async with httpx.AsyncClient(base_url=url) as http:
                deadline = time.monotonic() + 90
                while True:
                    if process.poll() is not None:
                        raise AssertionError(f"API process exited: {log_tail(log)}")
                    with contextlib.suppress(httpx.HTTPError):
                        if (await http.get("/health/ready")).status_code == 200:
                            break
                    if time.monotonic() > deadline:
                        raise AssertionError("API process did not become ready")
                    await asyncio.sleep(0.3)
            yield url
        finally:
            if process.poll() is None:
                process.kill()
            process.wait(timeout=30)


@pytest.mark.skipif(not os.environ.get(REDIS_ENV), reason=f"blocked: {REDIS_ENV} unset")
async def test_g6_two_os_processes_share_presence_events_and_commands_over_redis(
    common_db: CommonDatabase, tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    redis_url = os.environ[REDIS_ENV]
    key = RSAKey.generate_key(2048, parameters={"kid": "socket"})
    db = common_db
    monkeypatch.setenv(DSN_ENV, db.dsn("mission_control_runtime"))
    jwks = tmp_path / "process-keys.json"
    jwks.write_text(json.dumps({"keys": [key.as_dict(private=False)]}))
    spec = {
        "application_id": db.application_id,
        "installation_id": str(db.installation_id),
        "project_ref": db.project_ref,
        "tenants": {name: str(value) for name, value in db.tenants.items()},
        "jwks": str(jwks),
        "redis_url": redis_url,
        "origin": ORIGIN,
    }
    # Process A: this test process, composed by the same bootstrap as process B.
    asgi = build(spec)
    pool = await db.pool(max_size=4)
    admitted = await admit_unit_attempt(pool, db)
    tokens_from = Stack(db, "", key, admitted, None, pool, [])
    try:
        async with serve(asgi) as (url_a, _server):
            watcher, joiner = RealtimeClient(), RealtimeClient()
            async with api_process(spec, tmp_path / "process-b.log") as url_b:
                start = await tokens_from.last_seq()
                await watcher.connect(url_a, auth(tokens_from.token("operator")))
                await joiner.connect(url_b, auth(tokens_from.token("operator")))
                body = subscribe(admitted.run_key, start, streams=["mission_events", "presence"])

                def joined(client: MissionClient) -> int:
                    return sum(e["kind"] == "presence.joined" for e in client.events["presence"])

                assert (await watcher.call("subscribe", body))["ok"]
                await watcher.wait_for(lambda: joined(watcher) == 1, within=10)
                joined_reply = await joiner.call("subscribe", {**body, "request_id": "req-b"})
                assert joined_reply["ok"] is True, joined_reply
                # Presence emitted in process B reaches process A's client through Redis.
                await watcher.wait_for(lambda: joined(watcher) == 2, within=15)

                # Durable events: both processes deliver every commit from PostgreSQL.
                await tokens_from.record_usage(10)
                after = await tokens_from.last_seq()
                expected = list(range(start + 1, after + 1))
                await watcher.wait_for(lambda: watcher.seqs() == expected, within=15)
                await joiner.wait_for(lambda: joiner.seqs() == expected, within=15)

                # A command sent to process B is admitted once; process A streams its event.
                projection = await admitted.run_control.get_run(db.scope(), admitted.run_key)
                cancel = {
                    "request_id": f"req-{uuid4()}",
                    "application_id": "biotech",
                    "run_id": admitted.run_key,
                    "command": {
                        "request_id": str(uuid4()),
                        "expected_version": projection.version,
                        "expected_generation": 1,
                        "target": {"id": admitted.run_key},
                        "kind": "cancel",
                        "payload": {},
                        "reason": "cross-process parity",
                    },
                }
                receipt = await joiner.call("command", cancel)
                assert receipt["ok"] is True and receipt["outcome"] == "accepted", receipt
                await watcher.wait_for(
                    lambda: any("cancel" in e["kind"] for e in watcher.events["mission_event"]),
                    within=15,
                )
                await joiner.wait_for(
                    lambda: any("cancel" in e["kind"] for e in joiner.events["mission_event"]),
                    within=15,
                )
                acked = start + len(expected)  # acknowledge the usage events only
                ack = await joiner.call(
                    "ack",
                    {
                        "subscription_id": joined_reply["subscription_id"],
                        "cursors": [mission(acked)],
                    },
                )
                assert ack["ok"] is True, ack
            # Process B was killed; its client resumes on process A from its acked cursor.
            await joiner.wait_for(lambda: not joiner.sio.connected, within=30)
            await joiner.close()
            # The process-B leave never arrives (killed); A's own presence is unaffected.
            await tokens_from.record_usage(3)
            final = await tokens_from.last_seq()
            resumed = RealtimeClient()
            await resumed.connect(url_a, auth(tokens_from.token("viewer")))
            reply = await resumed.call("subscribe", subscribe(admitted.run_key, acked))
            assert reply["ok"] is True, reply
            await resumed.wait_for(lambda: bool(resumed.seqs()) and max(resumed.seqs()) == final)
            # Unacknowledged events (the cancel and later) are replayed, none twice.
            assert resumed.seqs() == list(range(acked + 1, final + 1))
            first_ids = {
                int(e["cursor"]["position"]): e["event_id"] for e in joiner.events["mission_event"]
            }
            for envelope in resumed.events["mission_event"]:
                seq = int(envelope["cursor"]["position"])
                if seq in first_ids:
                    assert first_ids[seq] == envelope["event_id"]
            await resumed.close()
            await watcher.close()
    finally:
        await pool.close()
