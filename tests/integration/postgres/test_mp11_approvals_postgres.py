"""MP-11 native approval bindings on a disposable PostgreSQL 17 (restricted runtime login, RLS).

SCHEMA: every test runs on a scratch database built from the released common migration
chain (0001..0033, component 1.2.0); ``approval_correlation`` and ``governed_effect_intent``
come from migration 0033 section 1, exactly as an installation receives them.

Proves: approval tasks on the common ``human_task``/``human_resolution`` rows with their
canonical events and outbox rows; two concurrent reviewers give exactly one attributed
resolution; a callback timeout expires the native correlation while the durable task stays
open; restart/timeout never replays an approval into a different request or modified
arguments; stale approvals after compaction, cancel and stop fence are refused; review with
feedback reaches the native reply as an instruction; an HTTP and a Socket.IO retry of the
same resolution (real uvicorn server, real socket.io client) are idempotent; deny and cancel
stay distinct; closed correlations never reopen; cross-tenant reads see nothing.

The context probe is a FIXTURE (``StaticApprovalContext``); the native requests are the
fixture request objects a lane adapter would build. No provider is called.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import asyncpg
import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI

from mission_control.adapters.postgres.approvals.correlations import (
    PostgresApprovalCorrelationRepository,
)
from mission_control.adapters.postgres.approvals.tasks import PostgresApprovalTaskRepository
from mission_control.adapters.postgres.human_tasks.repository import PostgresHumanTaskRepository
from mission_control.adapters.postgres.run_control.run_control_repository import (
    PostgresRunControlRepository,
)
from mission_control.adapters.postgres.run_control.stop_fence import PostgresStopFenceRepository
from mission_control.application.execution.approvals_broker import ApprovalBroker
from mission_control.application.execution.approvals_memory import StaticApprovalContext
from mission_control.application.human_tasks.service import HumanTaskRejected, HumanTaskService
from mission_control.application.installations.registry import (
    ApplicationBinding,
    ApplicationRegistry,
    InstallationObservation,
)
from mission_control.domain.execution.approvals import NativeApprovalCorrelation
from mission_control.domain.policies.contracts import ActorContext
from mission_control.domain.policies.stop_fence import StopFence
from mission_control.interfaces.http.human_tasks import router
from mission_control.interfaces.http.mission_control import MissionPrincipal, get_mission_principal
from mission_control.interfaces.socketio.app import MissionSocketConfig, mount_mission_socketio
from mission_control.interfaces.socketio.auth import SocketAuthRejected, token_expiry
from tests.fixtures.mission_control_common_db import (
    CommonDatabase,
    create_common_database,
    drop_common_database,
    tenant_id,
)
from tests.integration.postgres.runtime_common import owner_rows
from tests.unit.approvals.fixtures import (
    POLICY,
    REVIEWER,
    answer,
    connection_scoped_request,
    elicitation_request,
    permission_request,
)
from tests.unit.run_control.test_run_control import request, service
from tests.unit.socketio.harness import MissionClient, fixture_token, serve

pytestmark = pytest.mark.common_db

OWNER = ActorContext(actor_id=REVIEWER)


@pytest_asyncio.fixture
async def mp11_db() -> AsyncIterator[CommonDatabase]:
    """Scratch database with the released chain 0001..0033 (MP-11 tables from 0033)."""

    database = await create_common_database()
    try:
        yield database
    finally:
        await drop_common_database(database)


async def admitted_run(pool: asyncpg.Pool, scope: str) -> str:
    authority, _ = service(PostgresRunControlRepository(pool))  # type: ignore[arg-type]
    admission = await authority.admit(request(request_scope=scope, request_id=str(uuid4())))
    assert admission.run_id is not None
    return admission.run_id


class Stack:
    """One process's composition on a shared pool (a 'restart' is a new Stack)."""

    def __init__(
        self,
        pool: asyncpg.Pool,
        scope: str,
        connection_ref: str,
        context: StaticApprovalContext,
    ) -> None:
        self.tasks = PostgresApprovalTaskRepository(pool)
        self.correlations = PostgresApprovalCorrelationRepository(pool)
        self.fences = PostgresStopFenceRepository(pool)
        self.broker = ApprovalBroker(
            self.tasks,
            self.correlations,
            probe=context,
            connection_ref=connection_ref,
            fences=self.fences,
            poll_seconds=0.05,
        )
        self.service = HumanTaskService(
            PostgresHumanTaskRepository(pool),
            request_scope=scope,
            approvals=self.tasks,
            approval_wake=self.broker.hub,
        )


async def _events(db: CommonDatabase, task_id: str) -> list[tuple[str, int, bool, str]]:
    rows = await owner_rows(
        db,
        """
        SELECT e.event_type, o.aggregate_version,
               o.ledger_commit_id = e.ledger_commit_id AS linked,
               jsonb_path_query_first(e.payload, 'strict $.**.origin') #>> '{}' AS origin
        FROM mission_control.mission_event e
        JOIN mission_control.outbox o
          ON o.installation_id = e.installation_id AND o.application_id = e.application_id
         AND o.tenant_id = e.tenant_id AND o.event_id = e.event_id
        WHERE o.aggregate_key = $1 ORDER BY e.seq
        """,
        f"human_task:{task_id}",
    )
    return [(r["event_type"], r["aggregate_version"], r["linked"], r["origin"]) for r in rows]


async def _correlation_states(db: CommonDatabase, task_id: str) -> list[str]:
    rows = await owner_rows(
        db,
        "SELECT state FROM mission_control.approval_correlation WHERE human_task_id = $1 "
        "ORDER BY opened_at, approval_correlation_id",
        task_id,
    )
    return [row["state"] for row in rows]


async def test_two_concurrent_reviewers_one_resolution_with_events(
    mp11_db: CommonDatabase,
) -> None:
    pool = await mp11_db.pool(max_size=8)
    try:
        scope = mp11_db.scope()
        run_id = await admitted_run(pool, scope)
        context = StaticApprovalContext(1, POLICY)
        stack = Stack(pool, scope, "worker-a#1", context)
        bound = await stack.broker.bind(permission_request(request_scope=scope, run_id=run_id))
        again = await stack.broker.bind(permission_request(request_scope=scope, run_id=run_id))
        assert again.task.human_task_id == bound.task.human_task_id  # one task per binding
        services = [Stack(pool, scope, f"api-{i}", context).service for i in range(2)]

        async def resolve(index: int, actor: str, decision: str) -> str:
            try:
                receipt = await services[index].resolve(
                    bound.task.human_task_id,
                    answer(bound.task, request_id=f"{actor}-1", decision=decision),
                    ActorContext(actor_id=actor, permissions=frozenset({f"reviewer:{REVIEWER}"})),
                )
            except HumanTaskRejected as rejected:
                return rejected.code
            return receipt.status

        results = await asyncio.gather(resolve(0, "alice", "approve"), resolve(1, "bob", "deny"))
        assert sorted(results) == ["accepted", "already_resolved"]
        rows = await owner_rows(
            mp11_db,
            "SELECT actor_ref, answer->>'decision' AS decision, answer->>'schema_version' AS s "
            "FROM mission_control.human_resolution WHERE human_task_id = $1",
            bound.task.human_task_id,
        )
        assert len(rows) == 1 and rows[0]["s"] == "mc.approval_resolution.v1"
        kind = await owner_rows(
            mp11_db,
            "SELECT kind, request_packet->'binding'->>'schema_version' AS binding "
            "FROM mission_control.human_task WHERE human_task_id = $1",
            bound.task.human_task_id,
        )
        assert kind[0]["kind"] == "approval:provider_permission"
        assert kind[0]["binding"] == "mc.approval_binding.v1"
        assert await _events(mp11_db, bound.task.human_task_id) == [
            ("human_task.created", 1, True, "provider_permission"),
            ("human_task.resolved", 2, True, "provider_permission"),
        ]
        # Event payloads never carry native transport handles.
        payloads = await owner_rows(
            mp11_db,
            "SELECT e.payload::text AS body FROM mission_control.mission_event e "
            "WHERE e.payload::text LIKE '%' || $1 || '%'",
            bound.task.human_task_id,
        )
        assert len(payloads) == 2
        for row in payloads:
            for handle in ("toolu_fixture_1", "fixture-session", "worker-a#1"):
                assert handle not in row["body"]
        winner = rows[0]["decision"]
        # The re-delivery superseded the first native handle; the live one gets the answer.
        superseded = await stack.broker.wait(bound, wait_seconds=2)
        assert superseded.status == "superseded" and superseded.reply.action == "deny"
        outcome = await stack.broker.wait(again, wait_seconds=2)
        assert outcome.status == ("approved" if winner == "approve" else "denied")
    finally:
        await pool.close()


async def test_restart_and_timeout_never_replay_into_another_request_or_arguments(
    mp11_db: CommonDatabase,
) -> None:
    pool = await mp11_db.pool(max_size=8)
    try:
        scope = mp11_db.scope()
        run_id = await admitted_run(pool, scope)
        context = StaticApprovalContext(1, POLICY)
        before = Stack(pool, scope, "worker-a#1", context)

        # Callback timeout: the native correlation expires, the durable task stays open.
        stable = await before.broker.bind(permission_request(request_scope=scope, run_id=run_id))
        timed_out = await before.broker.wait(stable, wait_seconds=0.2)
        assert timed_out.status == "expired" and timed_out.reply.reason == "wait_expired"
        task = await before.tasks.get_task(scope, stable.task.human_task_id)
        assert task is not None and task.lifecycle == "open"
        assert await _correlation_states(mp11_db, stable.task.human_task_id) == ["expired"]

        # A connection-scoped request is pending when the process dies.
        doomed = await before.broker.bind(
            connection_scoped_request("7", request_scope=scope, run_id=run_id)
        )
        await before.service.resolve(stable.task.human_task_id, answer(stable.task), OWNER)
        await before.service.resolve(doomed.task.human_task_id, answer(doomed.task), OWNER)

        # Restart: a new process owns the native connection.
        after = Stack(pool, scope, "worker-a#2", context)
        recovered = await after.broker.recover(scope, "harness-exec-1")
        assert [item.correlation.correlation_id for item in recovered] == [
            doomed.correlation.correlation_id
        ]
        assert recovered[0].action == "restart_at_safe_boundary"
        assert recovered[0].decision_ref is not None  # the human decision stays as evidence
        assert await _correlation_states(mp11_db, doomed.task.human_task_id) == ["lost"]

        # The provider reuses request id "7" on the new connection: a different task, open.
        coincidence = await after.broker.bind(
            connection_scoped_request("7", request_scope=scope, run_id=run_id)
        )
        assert coincidence.task.human_task_id != doomed.task.human_task_id
        assert coincidence.task.lifecycle == "open" and coincidence.immediate is None

        # Modified arguments on the approved stable call: a new digest, a new review.
        modified = await after.broker.bind(
            permission_request(
                request_scope=scope,
                run_id=run_id,
                arguments={"command": "git push --force origin main", "timeout": 30},
            )
        )
        assert modified.task.human_task_id != stable.task.human_task_id
        assert modified.immediate is None

        # The same call and arguments reissued fresh (reissue strategy): replay after
        # revalidation, recorded with its origin decision; the expired handle stays expired.
        reissued = await after.broker.bind(permission_request(request_scope=scope, run_id=run_id))
        assert reissued.immediate is not None and reissued.immediate.status == "approved"
        assert reissued.immediate.replayed
        assert await _correlation_states(mp11_db, stable.task.human_task_id) == [
            "expired",
            "answered",
        ]
        replay = await owner_rows(
            mp11_db,
            "SELECT replayed_from, reply->>'reason' AS reason, connection_ref "
            "FROM mission_control.approval_correlation WHERE approval_correlation_id = $1",
            reissued.correlation.correlation_id,
        )
        assert replay[0]["replayed_from"] == reissued.task.resolution.resolution_ref  # type: ignore[union-attr]
        assert replay[0]["connection_ref"] == "worker-a#2"

        # A closed correlation never reopens, even for the owner.
        connection = await asyncpg.connect(mp11_db.owner_dsn)
        try:
            with pytest.raises(asyncpg.RestrictViolationError, match="does not move"):
                await connection.execute(
                    "UPDATE mission_control.approval_correlation SET state = 'live', "
                    "closed_at = NULL WHERE approval_correlation_id = $1",
                    stable.correlation.correlation_id,
                )
        finally:
            await connection.close()
        # The runtime role cannot delete correlations.
        async with pool.acquire() as raw, raw.transaction():
            from mission_control.adapters.postgres.scope import apply_scope

            await apply_scope(raw, scope)
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await raw.execute("DELETE FROM mission_control.approval_correlation")
    finally:
        await pool.close()


async def test_stale_approval_after_compaction_cancel_and_stop_fence(
    mp11_db: CommonDatabase,
) -> None:
    pool = await mp11_db.pool(max_size=8)
    try:
        scope = mp11_db.scope()
        run_id = await admitted_run(pool, scope)
        context = StaticApprovalContext(1, POLICY)
        stack = Stack(pool, scope, "worker-a#1", context)

        compacted = await stack.broker.bind(permission_request(request_scope=scope, run_id=run_id))
        await stack.service.resolve(compacted.task.human_task_id, answer(compacted.task), OWNER)
        context.state = context.state.model_copy(update={"generation": 2})
        stale = await stack.broker.wait(compacted, wait_seconds=2)
        assert stale.status == "stale" and stale.reply.reason == "stale_generation"
        context.state = context.state.model_copy(update={"generation": 1})

        waiting = await stack.broker.bind(
            permission_request(
                request_scope=scope,
                run_id=run_id,
                native=NativeApprovalCorrelation(tool_call_ref="toolu_cancelled"),
            )
        )
        waiter = asyncio.create_task(stack.broker.wait(waiting, wait_seconds=5))
        await asyncio.sleep(0.1)
        cancelled = await stack.service.cancel_run_approvals(run_id, actor_ref="operator:cancel")
        assert [task.human_task_id for task in cancelled] == [waiting.task.human_task_id]
        outcome = await asyncio.wait_for(waiter, 5)
        assert outcome.status == "task_cancelled" and outcome.reply.action == "deny"
        with pytest.raises(HumanTaskRejected) as late:
            await stack.service.resolve(waiting.task.human_task_id, answer(waiting.task), OWNER)
        assert late.value.code == "task_cancelled"

        await stack.fences.persist(
            StopFence(
                request_scope=scope,
                run_id=run_id,
                generation=1,
                command_id="cancel-immediate-1",
                reason="operator stop",
                requested_at=datetime.now(UTC),
            )
        )
        fenced = await stack.broker.bind(permission_request(request_scope=scope, run_id=run_id))
        assert fenced.immediate is not None and fenced.immediate.status == "fenced"
        admissions = await owner_rows(
            mp11_db,
            "SELECT decision, reason_code, effect_ref FROM "
            "mission_control.stop_fence_effect_admission WHERE run_key = $1",
            run_id,
        )
        assert [(r["decision"], r["reason_code"]) for r in admissions] == [("deny", "STOP_FENCED")]
        assert admissions[0]["effect_ref"] == "approval:call:toolu_fixture_1"
    finally:
        await pool.close()


async def test_deny_and_cancel_are_distinct_and_cross_tenant_reads_see_nothing(
    mp11_db: CommonDatabase,
) -> None:
    pool = await mp11_db.pool(max_size=8)
    try:
        scope = mp11_db.scope()
        run_id = await admitted_run(pool, scope)
        stack = Stack(pool, scope, "worker-a#1", StaticApprovalContext(1, POLICY))
        declined = await stack.broker.bind(elicitation_request(request_scope=scope, run_id=run_id))
        dismissed = await stack.broker.bind(
            elicitation_request(
                request_scope=scope,
                run_id=run_id,
                native=NativeApprovalCorrelation(native_request_ref="elicit-2"),
            )
        )
        await stack.service.resolve(
            declined.task.human_task_id, answer(declined.task, decision="deny"), OWNER
        )
        await stack.service.resolve(
            dismissed.task.human_task_id, answer(dismissed.task, decision="cancel"), OWNER
        )
        first = await stack.broker.wait(declined, wait_seconds=2)
        second = await stack.broker.wait(dismissed, wait_seconds=2)
        assert (first.status, first.reply.elicitation_action) == ("denied", "decline")
        assert (second.status, second.reply.elicitation_action) == ("cancelled", "cancel")
        actions = await owner_rows(
            mp11_db,
            "SELECT answer->>'resolution_action' AS action FROM mission_control.human_resolution "
            "WHERE human_task_id = ANY($1::uuid[]) ORDER BY answer->>'resolution_action'",
            [declined.task.human_task_id, dismissed.task.human_task_id],
        )
        assert [row["action"] for row in actions] == ["cancelled", "denied"]

        other = mp11_db.scope("tenant-2")
        assert await stack.tasks.get_task(other, declined.task.human_task_id) is None
        assert (
            await stack.correlations.get_correlation(other, declined.correlation.correlation_id)
            is None
        )
        assert await stack.tasks.list_tasks(other) == ()
    finally:
        await pool.close()


class _Resolver:
    """FIXTURE socket credential resolver: tokens issued here, never verified elsewhere."""

    def __init__(self, principal: MissionPrincipal) -> None:
        self.principal = principal
        self.tokens: set[str] = set()

    def grant(self) -> str:
        token = fixture_token(self.principal.actor.actor_id)
        self.tokens.add(token)
        return token

    def __call__(self, application_id: str, token: str) -> MissionPrincipal:
        expiry = token_expiry(token)
        if token not in self.tokens or expiry is None or application_id != "biotech":
            raise SocketAuthRejected("invalid_token")
        return self.principal


async def test_http_and_socket_retry_of_the_same_resolution_is_idempotent(
    mp11_db: CommonDatabase,
) -> None:
    pool = await mp11_db.pool(max_size=8)
    try:
        scope = mp11_db.scope()
        run_id = await admitted_run(pool, scope)
        stack = Stack(pool, scope, "worker-a#1", StaticApprovalContext(1, POLICY))
        bound = await stack.broker.bind(permission_request(request_scope=scope, run_id=run_id))
        principal = MissionPrincipal(
            installation_id=mp11_db.installation_id,
            application_id="biotech",
            tenant_id=tenant_id("tenant-1"),
            issuer="https://issuer.invalid",
            audiences=frozenset({"authenticated"}),
            actor=OWNER,
        )
        registry = ApplicationRegistry(
            (
                ApplicationBinding.seal(
                    application_id="biotech",
                    installation_id=mp11_db.installation_id,
                    binding_version="1",
                    supabase_project_ref="project",
                    database_secret_ref="TEST_DATABASE_URL",
                    accepted_issuers={"https://issuer.invalid"},
                    accepted_audiences={"authenticated"},
                    required_component_version="1",
                ),
            )
        )
        registry.observe(
            "biotech",
            InstallationObservation(
                mp11_db.installation_id, "biotech", "project", frozenset({"1"})
            ),
        )
        app = FastAPI()
        app.include_router(router)
        app.state.mission_control_registry = registry
        app.state.mission_control_human_task_services = {
            (mp11_db.installation_id, "biotech", tenant_id("tenant-1")): stack.service
        }
        app.dependency_overrides[get_mission_principal] = lambda: principal
        resolver = _Resolver(principal)
        asgi = mount_mission_socketio(
            app, MissionSocketConfig(allowed_origins=("http://dashboard.test",)), resolver=resolver
        )
        base = "/v1/applications/biotech/human-tasks"
        async with serve(asgi) as (url, _server):
            async with httpx.AsyncClient(base_url=url) as http:
                read = (await http.get(f"{base}/{bound.task.human_task_id}")).json()
                assert read["origin"] == "provider_permission"
                body: dict[str, Any] = {
                    "request_id": "shared-1",
                    "expected_task_version": read["version"],
                    "decision": "deny",
                    "reviewed_packet_digest": read["packet_digest"],
                    "comment": "open a pull request instead",
                }
                first = await http.post(f"{base}/{bound.task.human_task_id}/resolutions", json=body)
                assert first.status_code == 200 and first.json()["status"] == "accepted"
                client = MissionClient()
                await client.connect(url, {"application_id": "biotech", "token": resolver.grant()})
                try:
                    replay = await client.call(
                        "resolve_human_task",
                        {
                            "application_id": "biotech",
                            "human_task_id": read["human_task_id"],
                            **body,
                        },
                    )
                    changed = await client.call(
                        "resolve_human_task",
                        {
                            "application_id": "biotech",
                            "human_task_id": read["human_task_id"],
                            **body,
                            "request_id": "shared-2",
                            "decision": "approve",
                        },
                    )
                finally:
                    await client.close()
                http_retry = await http.post(
                    f"{base}/{bound.task.human_task_id}/resolutions", json=body
                )
        assert replay["ok"] is True and replay["status"] == "duplicate"
        assert replay["task"]["resolution"] == first.json()["task"]["resolution"]
        assert changed["ok"] is False and changed["error"]["code"] == "COMMAND_CONFLICT"
        assert http_retry.json()["status"] == "duplicate"
        rows = await owner_rows(
            mp11_db,
            "SELECT count(*) AS n FROM mission_control.human_resolution WHERE human_task_id = $1",
            bound.task.human_task_id,
        )
        assert rows[0]["n"] == 1
        # Review with feedback: the denial reaches the native reply as an instruction.
        outcome = await stack.broker.wait(bound, wait_seconds=2)
        assert outcome.status == "denied" and outcome.reply.interrupt is False
        assert outcome.reply.message is not None
        assert "open a pull request instead" in outcome.reply.message
    finally:
        await pool.close()


async def test_production_context_probe_reads_generation_and_binding_from_harness_execution(
    mp11_db: CommonDatabase,
) -> None:
    """The real `PostgresApprovalContextProbe` on an MP-06 harness execution (fixture lane)."""

    from datetime import timedelta

    from mission_control.adapters.postgres.approvals.context import PostgresApprovalContextProbe
    from mission_control.application.execution.harness.lane_turns import execution_start
    from tests.fixtures.lane_turns import ScriptedSessionLane, scripted_frames
    from tests.integration.postgres.mp06_common import pg_lane_unit

    pool = await mp11_db.pool(max_size=8)
    try:
        unit = await pg_lane_unit(pool, mp11_db, ScriptedSessionLane(frames=scripted_frames()))
        identity = unit.identity
        scope, heid, run_key = (
            identity.request_scope,
            identity.harness_execution_id,
            identity.run_key,
        )
        probe = PostgresApprovalContextProbe(pool)
        missing = await probe.current(scope, run_id=run_key, harness_execution_id=str(heid))
        assert missing.granted is False  # not opened yet: nothing may be applied
        await unit.frames.open_execution(execution_start(unit.operation, identity, 1, "agent-1"))
        no_owner = await probe.current(scope, run_id=run_key, harness_execution_id=str(heid))
        assert no_owner.granted is False
        now = datetime.now(UTC)
        await unit.states.claim_owner(
            scope, heid, owner_ref="worker-a#1", generation=1, now=now, lease=timedelta(seconds=30)
        )
        state = await probe.current(scope, run_id=run_key, harness_execution_id=str(heid))
        assert state.granted and state.generation == 1
        binding = await owner_rows(
            mp11_db,
            "SELECT coalesce(actual_binding_digest, intended_binding_digest) AS digest "
            "FROM mission_control.harness_execution WHERE harness_execution_id = $1",
            heid,
        )
        assert state.policy_digest == binding[0]["digest"]
        wrong_run = await probe.current(scope, run_id="run-other", harness_execution_id=str(heid))
        assert wrong_run.granted is False

        stack = Stack(pool, scope, "worker-a#1", StaticApprovalContext(1, POLICY))
        stack.broker = ApprovalBroker(
            stack.tasks,
            stack.correlations,
            probe=probe,
            connection_ref="worker-a#1",
            fences=stack.fences,
            poll_seconds=0.05,
        )
        request_kwargs: dict[str, Any] = {
            "request_scope": scope,
            "run_id": run_key,
            "harness_execution_id": str(heid),
            "policy_digest": state.policy_digest,
        }
        bound = await stack.broker.bind(permission_request(**request_kwargs))
        await stack.service.resolve(bound.task.human_task_id, answer(bound.task), OWNER)
        approved = await stack.broker.wait(bound, wait_seconds=2)
        assert approved.status == "approved"
        # A takeover in the next generation (session transfer) makes the approval stale.
        await unit.states.claim_owner(
            scope,
            heid,
            owner_ref="worker-b#1",
            generation=2,
            now=now + timedelta(seconds=60),
            lease=timedelta(seconds=30),
        )
        replay = await stack.broker.bind(permission_request(**request_kwargs))
        assert replay.immediate is not None and replay.immediate.status == "stale"
        assert replay.immediate.reply.reason == "stale_generation"
    finally:
        await pool.close()
