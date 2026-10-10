"""MP-14 on PostgreSQL 17: the `/missions` socket over the configured public API.

Everything here is real except the identity provider: `create_application` with its real
lifespan and restricted pools, the real `MissionTokenVerifier` (RS256 tokens minted with a
test key, like `test_mission_control_bootstrap_postgres.py`), the real registry check,
`PostgresStreamSource` over forced-RLS tables, the reducer journal written by the real
`RunControlService`, the real frame repository, uvicorn and python-socketio clients.
Stream services are registered the way the proposed `bootstrap/realtime.py` would
(bootstrap is frozen for MP-14; see the handoff).

V17: two clients disconnect and reconnect across concurrent commits with no fanout at all
(no hint is ever published), and still receive every committed event.
V18: expired, foreign-tenant, forged-cursor and forged-room requests are denied without
leaking data or applying commands; a client that stops acknowledging is resynced.
Redis fanout (two API servers sharing `AsyncRedisManager` and stream hints) needs
`MISSION_CONTROL_TEST_REDIS_URL`; without it that scenario is skipped as blocked.
"""

from __future__ import annotations

import asyncio
import json
import os
import random
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any
from uuid import UUID, uuid4

import asyncpg
import httpx
import pytest
import socketio
from joserfc import jwt
from joserfc.jwk import RSAKey
from redis.asyncio import Redis

import mission_control.bootstrap.api as api_module
from mission_control.adapters.auth.jwt import ActorGrant, ApplicationAuthentication
from mission_control.adapters.postgres.frames.repository import PostgresFrameRepository
from mission_control.adapters.realtime.stream_hints import RedisStreamHints
from mission_control.adapters.realtime.stream_source import PostgresStreamSource
from mission_control.application.frames.writer import FrameWriter
from mission_control.application.installations.registry import ApplicationBinding
from mission_control.application.streams.ports import StreamHint
from mission_control.application.streams.pump import PumpSettings
from mission_control.application.streams.service import MissionStreamService
from mission_control.bootstrap.api import ApplicationDeployment, MissionDeployment
from mission_control.domain.policies.contracts import CommandStatus, RecordUsageAction
from mission_control.interfaces.socketio.app import MissionSocketConfig, mount_mission_socketio
from mission_control.interfaces.socketio.server import STREAM_SERVICES, MissionSocketLimits
from tests.fixtures.mission_control_common_db import COMPONENT_VERSION, CommonDatabase
from tests.fixtures.provider_frames import StepClock, turn_observations
from tests.integration.postgres.frames_common import AdmittedUnit, admit_unit_attempt
from tests.integration.postgres.runtime_common import common_db as common_db
from tests.integration.postgres.runtime_common import owner_rows, scoped_command
from tests.unit.run_control.test_run_control import ALL_PERMISSIONS
from tests.unit.socketio.harness import MissionClient, serve

pytestmark = pytest.mark.common_db

DSN_ENV = "MC_SOCKET_TEST_DSN"
ISSUER = "https://issuer.invalid"
ORIGIN = "http://dashboard.test"
REDIS_ENV = "MISSION_CONTROL_TEST_REDIS_URL"
FAST = PumpSettings(page_size=50, poll_interval=0.2, slow_consumer_grace=1.0)


@dataclass
class Stack:
    db: CommonDatabase
    url: str
    key: RSAKey
    admitted: AdmittedUnit
    app: Any
    pool: asyncpg.Pool
    pool_opens: list[int]

    def token(
        self, subject: str, *, tenant: str = "tenant-1", ttl: float = 300, **claims: Any
    ) -> str:
        body = {
            "sub": subject,
            "iss": ISSUER,
            "aud": "authenticated",
            "exp": int(time.time() + ttl),
            "app_metadata": {
                "application_id": "biotech",
                "tenant_id": str(self.db.tenants[tenant]),
            },
        }
        body.update(claims)
        return jwt.encode({"alg": "RS256", "kid": "socket"}, body, self.key)

    async def last_seq(self) -> int:
        rows = await owner_rows(
            self.db,
            "SELECT m.last_event_seq FROM mission_control.mission m "
            "JOIN mission_control.mission_run r USING (installation_id, application_id, "
            "tenant_id, mission_id) WHERE r.run_key = $1",
            self.admitted.run_key,
        )
        return int(rows[0]["last_event_seq"])

    async def record_usage(self, count: int, *, pause: float = 0.0) -> list[int]:
        authority = self.admitted.run_control
        for _ in range(count):
            current = await authority.get_run(self.db.scope(), self.admitted.run_key)
            decision = await authority.execute(
                scoped_command(
                    self.db,
                    self.admitted.run_key,
                    current.version,
                    f"usage-{uuid4()}",
                    RecordUsageAction(
                        usage_id=f"usage:{uuid4()}", reservation_id="baseline", actual_amounts={}
                    ),
                )
            )
            assert decision.status == CommandStatus.ACCEPTED, decision
            if pause:
                await asyncio.sleep(random.uniform(0, pause))
        return [await self.last_seq()]


def binding(db: CommonDatabase) -> ApplicationBinding:
    return ApplicationBinding.seal(
        application_id=db.application_id,
        installation_id=db.installation_id,
        binding_version="1",
        supabase_project_ref=db.project_ref,
        database_secret_ref=DSN_ENV,
        accepted_issuers={ISSUER},
        accepted_audiences={"authenticated"},
        required_component_version=COMPONENT_VERSION,
    )


def deployment(db: CommonDatabase, public_jwks: Any) -> MissionDeployment:
    reader = {"workflow_run.read"}
    auth = ApplicationAuthentication(
        binding=binding(db),
        issuer=ISSUER,
        audience="authenticated",
        public_jwks_file=public_jwks,
        grants=(
            ActorGrant(
                subject="operator",
                actor_id="operator",
                tenant_ids={db.tenants["tenant-1"]},
                permissions=ALL_PERMISSIONS | reader,
                # The lifecycle authority a pause decision names (realtime receipt proof).
                authority_refs=frozenset({"authority:lifecycle"}),
            ),
            ActorGrant(
                subject="viewer",
                actor_id="viewer",
                tenant_ids={db.tenants["tenant-1"]},
                permissions=reader,
            ),
            ActorGrant(
                subject="intruder",
                actor_id="intruder",
                tenant_ids={db.tenants["tenant-2"]},
                permissions=ALL_PERMISSIONS | reader,
            ),
        ),
    )
    return MissionDeployment(
        storage_mode="production_common",
        max_request_bytes=1_000_000,
        applications=(ApplicationDeployment(authentication=auth),),
    )


def register_streams(app: Any, db: CommonDatabase, pool: asyncpg.Pool) -> None:
    """What the proposed `bootstrap/realtime.py` does per (installation, app, tenant)."""

    registry = getattr(app.state, STREAM_SERVICES)
    for tenant in ("tenant-1", "tenant-2"):
        scope = db.scope(tenant)
        registry[(db.installation_id, db.application_id, db.tenants[tenant])] = (
            MissionStreamService(PostgresStreamSource(pool, scope), request_scope=scope)
        )


@asynccontextmanager
async def stack(
    db: CommonDatabase,
    tmp_path: Any,
    monkeypatch: pytest.MonkeyPatch,
    *,
    limits: MissionSocketLimits | None = None,
    redis_url: str | None = None,
    redis_channel: str = "mission-control-socketio",
    hints: Any = None,
    key: RSAKey | None = None,
    admitted: AdmittedUnit | None = None,
) -> AsyncIterator[Stack]:
    monkeypatch.setenv(DSN_ENV, db.dsn("mission_control_runtime"))
    key = key or RSAKey.generate_key(2048, parameters={"kid": "socket"})
    public = tmp_path / f"keys-{uuid4().hex}.json"
    public.write_text(json.dumps({"keys": [key.as_dict(private=False)]}))
    opens: list[int] = []
    original = api_module.application_pool

    async def counted(secret_ref: str) -> asyncpg.Pool:
        opens.append(1)
        return await original(secret_ref)

    monkeypatch.setattr(api_module, "application_pool", counted)
    pool = await db.pool(max_size=6)
    try:
        admitted = admitted or await admit_unit_attempt(pool, db)
        app = api_module.create_application(deployment(db, public))
        config = MissionSocketConfig(
            allowed_origins=(ORIGIN,),
            redis_url=redis_url,
            redis_channel=redis_channel,
            limits=limits or MissionSocketLimits(pump=FAST),
        )
        asgi = mount_mission_socketio(app, config, hints=hints)
        register_streams(app, db, pool)
        async with serve(asgi) as (url, _server):
            yield Stack(db, url, key, admitted, app, pool, opens)
    finally:
        await pool.close()


def subscribe(run_key: str, cursor: int | None = None, **extra: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "request_id": f"req-{uuid4()}",
        "application_id": "biotech",
        "target": {"kind": "run", "id": run_key},
        "streams": ["mission_events"],
    }
    if cursor is not None:
        body["cursors"] = [mission(cursor)]
    body.update(extra)
    return body


def mission(seq: int) -> dict[str, Any]:
    return {"stream": "mission_events", "position": str(seq), "generation": None}


def auth(token: str) -> dict[str, str]:
    return {"application_id": "biotech", "token": token}


async def test_configured_api_lifespan_runs_once_through_the_socket_app(
    common_db: CommonDatabase, tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with stack(common_db, tmp_path, monkeypatch) as s:
        assert s.app.state.mission_control_ready is True
        assert len(s.pool_opens) == 1  # one application pool: lifespan entered once
        async with httpx.AsyncClient(base_url=s.url) as http:
            ready = await http.get("/health/ready")
            assert ready.status_code == 200, ready.text
            polling = await http.get("/socket.io/", params={"EIO": "4", "transport": "polling"})
            assert polling.status_code == 200 and polling.text.startswith("0")
        mounted = s.app.state.mission_control_socketio
        assert mounted.started == 1
    assert s.app.state.mission_control_ready is False
    assert (mounted.started, mounted.stopped) == (1, 1)
    assert len(s.pool_opens) == 1


async def test_two_clients_reconnect_across_commits_without_fanout_and_lose_nothing(
    common_db: CommonDatabase, tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """V17: no hint is ever published (total fanout failure); replay is from PostgreSQL."""

    async with stack(common_db, tmp_path, monkeypatch) as s:
        start = await s.last_seq()
        rng = random.Random(1408)
        received: dict[str, dict[int, set[str]]] = {"a": {}, "b": {}}
        reconnects = {"a": 0, "b": 0}
        writer_done = asyncio.Event()

        async def writer() -> None:
            await s.record_usage(24, pause=0.05)
            writer_done.set()

        async def reader(name: str) -> None:
            acked = start
            final: int | None = None
            while final is None or acked < final:
                client = MissionClient()
                await client.connect(s.url, auth(s.token("viewer")))
                ack = await client.call("subscribe", subscribe(s.admitted.run_key, acked))
                assert ack["ok"] is True, ack
                assert ack["replay_from"] == [mission(acked)]
                await asyncio.sleep(rng.uniform(0.1, 0.6))
                for envelope in client.events["mission_event"]:
                    assert envelope["scope"]["tenant_id"] == str(s.db.tenants["tenant-1"])
                    seq = int(envelope["cursor"]["position"])
                    received[name].setdefault(seq, set()).add(envelope["event_id"])
                seqs = sorted(received[name])
                contiguous = acked
                while contiguous + 1 in received[name]:
                    contiguous += 1
                # Sometimes drop the connection before acknowledging what arrived.
                if contiguous > acked and rng.random() < 0.7:
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
                reconnects[name] += 1
                if writer_done.is_set():
                    final = await s.last_seq()
                assert seqs == sorted(set(seqs))

        await asyncio.wait_for(asyncio.gather(writer(), reader("a"), reader("b")), timeout=120)
        final = await s.last_seq()
        assert final >= start + 24
        expected = set(range(start + 1, final + 1))
        for name in ("a", "b"):
            assert set(received[name]) == expected, name
            # Redelivery after a dropped ack repeats the same event id, never a new one.
            assert all(len(ids) == 1 for ids in received[name].values()), name
            assert reconnects[name] >= 2


async def test_forged_expired_and_foreign_requests_cannot_leak_or_apply_commands(
    common_db: CommonDatabase, tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """V18 on the real verifier, registry and forced-RLS store."""

    async with stack(common_db, tmp_path, monkeypatch) as s:
        repository = PostgresFrameRepository(s.pool)
        handle = await repository.open_execution(s.admitted.start())
        await FrameWriter(repository, handle, clock=StepClock()).write(turn_observations())
        execution = str(handle.harness_execution_id)
        await s.record_usage(2)
        before = await s.last_seq()

        # Credentials: expired, wrong audience, foreign key, and unknown subject are refused.
        other_key = RSAKey.generate_key(2048, parameters={"kid": "socket"})
        refused = [
            s.token("viewer", ttl=-30),
            s.token("viewer", aud="someone-else"),
            jwt.encode(
                {"alg": "RS256", "kid": "socket"},
                {
                    "sub": "viewer",
                    "iss": ISSUER,
                    "aud": "authenticated",
                    "exp": int(time.time()) + 60,
                    "app_metadata": {
                        "application_id": "biotech",
                        "tenant_id": str(s.db.tenants["tenant-1"]),
                    },
                },
                other_key,
            ),
            s.token("stranger"),
            s.token("viewer", tenant="tenant-2"),  # tenant not in the subject's grant
        ]
        for token in refused:
            client = MissionClient()
            with pytest.raises(socketio.exceptions.ConnectionError):
                await client.connect(s.url, auth(token))
            await client.close()

        intruder = MissionClient()
        await intruder.connect(s.url, auth(s.token("intruder", tenant="tenant-2")))
        absent = await intruder.call("subscribe", subscribe("run-that-does-not-exist", 0))
        foreign = await intruder.call("subscribe", subscribe(s.admitted.run_key, 0))
        assert absent["error"] == {**foreign["error"], "request_id": absent["error"]["request_id"]}
        assert foreign["error"]["code"] == "TARGET_NOT_FOUND"
        foreign_frames = await intruder.call(
            "subscribe",
            subscribe(
                execution,
                target={"kind": "execution", "id": execution},
                streams=["provider_frames"],
                cursors=[
                    {"stream": "provider_frames", "position": f"{execution}:0", "generation": 1}
                ],
            ),
        )
        assert foreign_frames["error"]["code"] == "TARGET_NOT_FOUND"
        command = cancel_command(s.admitted.run_key, version=99)
        rejected = await intruder.call("command", command)
        assert rejected["ok"] is False
        assert rejected["error"]["code"] in {"TARGET_NOT_FOUND", "COMMAND_CONFLICT"}
        forged_room = await intruder.call(
            "subscribe", subscribe(s.admitted.run_key, 0, room="mc-presence:tenant-1")
        )
        assert forged_room["error"]["code"] == "UNSUPPORTED_FILTER"
        await asyncio.sleep(0.5)
        assert intruder.events["mission_event"] == [] == intruder.events["provider_frame"]
        await intruder.close()

        viewer = MissionClient()
        await viewer.connect(s.url, auth(s.token("viewer")))
        scope_forgery = await viewer.call(
            "subscribe", {**subscribe(s.admitted.run_key, 0), "application_id": "another"}
        )
        assert scope_forgery["error"]["code"] == "SCOPE_MISMATCH"
        ahead = await viewer.call("subscribe", subscribe(s.admitted.run_key, before + 50))
        assert ahead["error"]["code"] == "CURSOR_AHEAD"
        forged_cursor = await viewer.call(
            "subscribe",
            subscribe(
                execution,
                target={"kind": "execution", "id": execution},
                streams=["provider_frames"],
                cursors=[
                    {"stream": "provider_frames", "position": f"{uuid4()}:0", "generation": 1}
                ],
            ),
        )
        assert forged_cursor["error"]["code"] == "SCOPE_MISMATCH"
        frames = await viewer.call(
            "subscribe",
            subscribe(
                execution,
                target={"kind": "execution", "id": execution},
                streams=["provider_frames"],
                cursors=[
                    {"stream": "provider_frames", "position": f"{execution}:3", "generation": 1}
                ],
            ),
        )
        assert frames["ok"] is True, frames
        await viewer.wait_for(lambda: len(viewer.events["provider_frame"]) >= 4)
        ordinals = [
            int(e["cursor"]["position"].rpartition(":")[2]) for e in viewer.events["provider_frame"]
        ]
        assert ordinals == sorted(ordinals) and min(ordinals) > 3
        assert all(
            e["execution_ref"] == f"harness_execution:{execution}"
            for e in viewer.events["provider_frame"]
        )
        # A read-only actor cannot apply a command; nothing is appended.
        denied = await viewer.call("command", cancel_command(s.admitted.run_key, version=1))
        assert denied["error"]["code"] in {"UNAUTHORIZED", "COMMAND_CONFLICT"}
        await viewer.close()
        assert await s.last_seq() == before

        # A credential that expires mid-connection detaches and disconnects.
        expiring = MissionClient()
        await expiring.connect(s.url, auth(s.token("viewer", ttl=3)))
        assert (await expiring.call("subscribe", subscribe(s.admitted.run_key, before)))["ok"]
        await expiring.wait_for(lambda: not expiring.sio.connected, within=15)
        await s.record_usage(1)
        await asyncio.sleep(0.5)
        assert expiring.events["mission_event"] == []
        assert [e["code"] for e in expiring.events["stream_error"]] == ["UNAUTHORIZED"]
        await expiring.close()


def cancel_command(run_key: str, *, version: int) -> dict[str, Any]:
    return {
        "request_id": f"req-{uuid4()}",
        "application_id": "biotech",
        "run_id": run_key,
        "command": {
            "request_id": str(uuid4()),
            "expected_version": version,
            "expected_generation": 1,
            "target": {"id": run_key},
            "kind": "cancel",
            "payload": {},
            "reason": "socket parity",
        },
    }


async def test_socket_command_is_the_http_command_and_its_event_streams_live(
    common_db: CommonDatabase, tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with stack(common_db, tmp_path, monkeypatch) as s:
        before = await s.last_seq()
        operator = MissionClient()
        await operator.connect(s.url, auth(s.token("operator")))
        assert (await operator.call("subscribe", subscribe(s.admitted.run_key, before)))["ok"]
        projection = await s.admitted.run_control.get_run(s.db.scope(), s.admitted.run_key)
        command = cancel_command(s.admitted.run_key, version=projection.version)
        receipt = await operator.call("command", command)
        assert receipt["ok"] is True, receipt
        assert receipt["outcome"] == "accepted" and receipt["replay"] is False
        assert operator.events["command_receipt"][0]["receipt"] == receipt["receipt"]
        await operator.wait_for(lambda: bool(operator.seqs()))
        assert any("cancel" in e["kind"] for e in operator.events["mission_event"]), [
            e["kind"] for e in operator.events["mission_event"]
        ]
        async with httpx.AsyncClient(base_url=s.url) as http:
            replay = await http.post(
                f"/v1/applications/biotech/runs/{s.admitted.run_key}/commands",
                json=command["command"],
                headers={"Authorization": f"Bearer {s.token('operator')}"},
            )
        assert replay.status_code == 200, replay.text
        assert replay.json()["replay"] is True
        assert replay.json()["admission"] == receipt["receipt"]["admission"]
        await operator.close()


async def test_a_slow_consumer_on_the_real_store_is_resynced_with_its_acked_cursor(
    common_db: CommonDatabase, tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    limits = MissionSocketLimits(max_connection_queue_bytes=65_536, pump=FAST)
    async with stack(common_db, tmp_path, monkeypatch, limits=limits) as s:
        start = await s.last_seq()
        await s.record_usage(160)
        client = MissionClient()
        await client.connect(s.url, auth(s.token("viewer")))
        assert (await client.call("subscribe", subscribe(s.admitted.run_key, start)))["ok"]
        await client.wait_for(lambda: bool(client.events["resync_required"]), within=20)
        notice = client.events["resync_required"][0]
        assert notice["code"] == "SLOW_CONSUMER" and notice["detached"] is True
        assert notice["replay_from"] == [mission(start)]
        delivered = len(client.events["mission_event"])
        assert 0 < delivered < await s.last_seq() - start
        total = sum(
            len(json.dumps(e, separators=(",", ":"))) for e in client.events["mission_event"]
        )
        assert total <= 65_536 + 4_096
        await client.close()


@pytest.mark.skipif(not os.environ.get(REDIS_ENV), reason=f"blocked: {REDIS_ENV} unset")
async def test_redis_fanout_between_two_api_servers_and_hints_wake_pumps(
    common_db: CommonDatabase, tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    redis_url = os.environ[REDIS_ENV]
    channel = f"mc-socketio-test-{uuid4().hex}"
    hint_channel = f"mc-stream-hints-test-{uuid4().hex}"
    publisher = Redis.from_url(redis_url)
    listener = Redis.from_url(redis_url)
    slow_poll = MissionSocketLimits(
        pump=PumpSettings(page_size=50, poll_interval=30.0, slow_consumer_grace=30.0)
    )
    key = RSAKey.generate_key(2048, parameters={"kid": "socket"})
    try:
        async with stack(
            common_db,
            tmp_path,
            monkeypatch,
            redis_url=redis_url,
            redis_channel=channel,
            key=key,
        ) as first:
            async with stack(
                common_db,
                tmp_path,
                monkeypatch,
                redis_url=redis_url,
                redis_channel=channel,
                limits=slow_poll,
                hints=RedisStreamHints(listener, channel=hint_channel),
                key=key,
                admitted=first.admitted,
            ) as second:
                watcher, joiner = MissionClient(), MissionClient()
                await watcher.connect(first.url, auth(first.token("viewer")))
                await joiner.connect(second.url, auth(second.token("viewer")))
                start = await first.last_seq()
                presence = subscribe(
                    first.admitted.run_key, start, streams=["mission_events", "presence"]
                )
                assert (await watcher.call("subscribe", presence))["ok"]

                def joined() -> int:
                    return sum(e["kind"] == "presence.joined" for e in watcher.events["presence"])

                await watcher.wait_for(lambda: joined() == 1, within=10)  # its own join
                assert (await joiner.call("subscribe", presence))["ok"]
                # The join emitted by server two reaches the client of server one via Redis.
                await watcher.wait_for(lambda: joined() == 2, within=10)
                # Server two polls every 30 s; a published hint delivers the commit now.
                await first.record_usage(1)
                mission_id = (
                    await owner_rows(
                        first.db,
                        "SELECT mission_id FROM mission_control.mission_run WHERE run_key = $1",
                        first.admitted.run_key,
                    )
                )[0]["mission_id"]
                began = time.monotonic()
                await RedisStreamHints(publisher, channel=hint_channel).publish(
                    StreamHint(request_scope=first.db.scope(), mission_id=UUID(str(mission_id)))
                )
                await joiner.wait_for(lambda: bool(joiner.seqs()), within=10)
                assert time.monotonic() - began < 10
                await watcher.close()
                await joiner.close()
    finally:
        await publisher.aclose()
        await listener.aclose()
