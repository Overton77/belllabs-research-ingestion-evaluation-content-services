"""MP-11 governed prepare / review / execute on a disposable PostgreSQL 17 (runtime role, RLS).

SCHEMA: runs on the scratch database of ``test_mp11_approvals_postgres.mp11_db``: the
released chain 0001..0033 (component 1.2.0; MP-11 tables from 0033 section 1).

Proves: prepare persists the intent and its ``governed_effect`` Human Task without executing;
repeated prepare/execute calls return the same pending state or the same receipt; concurrent
execute calls from separate service instances (separate "processes" on one database) never
double-execute; edited arguments are a new intent and a new review; denied and cancelled
reviews settle distinctly and never execute; a Stop Fence after approval is a denied effect
recorded in the shared effect-admission table; a claim left by a crashed process is reported
until its lease passes and then reconciled or parked ``in_doubt``; the receipt is immutable;
a missing client elicitation capability is a typed rejection that writes nothing.

FIXTURE executors and probe; no provider or external system is called.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg
import pytest
from fastmcp import Client, FastMCP

from mission_control.adapters.postgres.approvals.intents import PostgresGovernedIntentRepository
from mission_control.adapters.postgres.approvals.tasks import PostgresApprovalTaskRepository
from mission_control.adapters.postgres.human_tasks.repository import PostgresHumanTaskRepository
from mission_control.adapters.postgres.run_control.stop_fence import PostgresStopFenceRepository
from mission_control.application.execution.approvals import ElicitationPrompt
from mission_control.application.execution.approvals_governed import (
    GOVERNED_EFFECT_PERMISSION,
    GovernedEffectResult,
    GovernedEffectService,
    GovernedIntent,
    GovernedPrepareRequest,
    GovernedRejected,
    GovernedTool,
    GovernedToolPolicy,
    GovernedToolRegistry,
)
from mission_control.application.execution.approvals_memory import StaticApprovalContext
from mission_control.application.human_tasks.service import HumanTaskService
from mission_control.domain.policies.contracts import ActorContext
from mission_control.domain.policies.stop_fence import StopFence
from mission_control.interfaces.mcp.coordinator_server import CoordinatorPrincipal, _principal_call
from mission_control.interfaces.mcp.governed_gateway import (
    GOVERNED_PREPARE_TOOL,
    ScopedGovernedEffects,
    register_governed_tools,
)
from tests.fixtures.mission_control_common_db import CommonDatabase
from tests.integration.postgres.runtime_common import owner_rows
from tests.integration.postgres.test_mp11_approvals_postgres import (  # noqa: F401
    admitted_run,
    mp11_db,
)
from tests.unit.approvals.fixtures import POLICY, REVIEWER, Clock, RecordingExecutor, answer

pytestmark = pytest.mark.common_db

AGENT = ActorContext(actor_id="agent:lane", permissions=frozenset({GOVERNED_EFFECT_PERMISSION}))
OWNER = ActorContext(actor_id=REVIEWER)


class ReconcilingExecutor(RecordingExecutor):
    """FIXTURE executor whose domain recorded the effect under the intent id."""

    def __init__(self) -> None:
        super().__init__()
        self.recorded: dict[str, GovernedEffectResult] = {}

    async def lookup(self, intent: GovernedIntent) -> GovernedEffectResult | None:
        return self.recorded.get(intent.intent_id)


class Gateway:
    def __init__(
        self,
        pool: asyncpg.Pool,
        scope: str,
        executor: RecordingExecutor,
        *,
        clock: Clock | None = None,
        **policy: Any,
    ) -> None:
        values: dict[str, Any] = {"reviewers": (REVIEWER,)}
        values.update(policy)
        self.tasks = PostgresApprovalTaskRepository(pool)
        self.intents = PostgresGovernedIntentRepository(pool)
        self.fences = PostgresStopFenceRepository(pool)
        self.context = StaticApprovalContext(1, POLICY)
        extra: dict[str, Any] = {"clock": clock} if clock is not None else {}
        self.service = GovernedEffectService(
            self.intents,
            self.tasks,
            GovernedToolRegistry(
                (
                    GovernedTool(
                        name="publish_report",
                        executor=executor,
                        policy=GovernedToolPolicy(**values),
                    ),
                )
            ),
            request_scope=scope,
            probe=self.context,
            fences=self.fences,
            **extra,
        )
        self.human = HumanTaskService(
            PostgresHumanTaskRepository(pool), request_scope=scope, approvals=self.tasks
        )

    async def review(self, task_id: str, decision: str = "approve", **extra: Any) -> None:
        task = await self.tasks.get_task(self.service.request_scope, task_id)
        assert task is not None
        await self.human.resolve(task_id, answer(task, decision=decision, **extra), OWNER)


def _prepare(run_id: str, **arguments: Any) -> GovernedPrepareRequest:
    return GovernedPrepareRequest(
        run_id=run_id,
        harness_execution_id="harness-exec-1",
        generation=1,
        lane_profile="codex",
        tool_name="publish_report",
        arguments=arguments or {"report": "q3"},
    )


async def _count(db: CommonDatabase, table: str) -> int:
    rows = await owner_rows(db, f"SELECT count(*) AS n FROM mission_control.{table}")
    return int(rows[0]["n"])


async def test_repeated_prepare_and_concurrent_execute_never_double_execute(
    mp11_db: CommonDatabase,  # noqa: F811
) -> None:
    pool = await mp11_db.pool(max_size=12)
    try:
        scope = mp11_db.scope()
        run_id = await admitted_run(pool, scope)
        executor = RecordingExecutor(delay=0.05)
        gateways = [Gateway(pool, scope, executor) for _ in range(4)]
        prepared = await asyncio.gather(
            *(g.service.prepare(_prepare(run_id), AGENT) for g in gateways)
        )
        assert len({state.intent_id for state in prepared}) == 1
        assert all(state.status == "pending_approval" for state in prepared)
        assert await _count(mp11_db, "governed_effect_intent") == 1
        tasks = await owner_rows(
            mp11_db,
            "SELECT kind, lifecycle FROM mission_control.human_task WHERE kind LIKE 'approval:%'",
        )
        assert [(row["kind"], row["lifecycle"]) for row in tasks] == [
            ("approval:governed_effect", "open")
        ]
        state = prepared[0]
        assert state.human_task is not None
        pending = await gateways[1].service.execute(state.intent_id, AGENT)
        assert pending.status == "pending_approval" and pending.human_task == state.human_task
        assert executor.calls == []

        await gateways[0].review(state.human_task["human_task_id"])
        results = await asyncio.gather(
            *(g.service.execute(state.intent_id, AGENT) for g in gateways for _ in range(3))
        )
        assert executor.calls == [state.intent_id]
        statuses = {item.status for item in results}
        assert statuses <= {"executed", "executing"} and "executed" in statuses
        final = await gateways[2].service.execute(state.intent_id, AGENT)
        receipts = {item.receipt.receipt_digest for item in results if item.receipt is not None}
        assert final.receipt is not None and receipts == {final.receipt.receipt_digest}
        row = await owner_rows(
            mp11_db,
            "SELECT state, receipt_digest, version FROM mission_control.governed_effect_intent",
        )
        assert row[0]["state"] == "executed"
        assert row[0]["receipt_digest"] == final.receipt.receipt_digest
        admissions = await owner_rows(
            mp11_db,
            "SELECT decision, effect_ref FROM mission_control.stop_fence_effect_admission",
        )
        assert [(r["decision"], r["effect_ref"]) for r in admissions] == [
            ("allow", f"governed:{state.intent_id}")
        ]
        # The receipt is immutable, even for the owner.
        connection = await asyncpg.connect(mp11_db.owner_dsn)
        try:
            with pytest.raises(asyncpg.RestrictViolationError, match="receipt is immutable"):
                await connection.execute(
                    "UPDATE mission_control.governed_effect_intent SET receipt = '{}'::jsonb"
                )
            with pytest.raises(asyncpg.RestrictViolationError, match="does not move"):
                await connection.execute(
                    "UPDATE mission_control.governed_effect_intent SET state = 'executing'"
                )
        finally:
            await connection.close()
    finally:
        await pool.close()


async def test_edited_arguments_denied_and_cancelled_effects(
    mp11_db: CommonDatabase,  # noqa: F811
) -> None:
    pool = await mp11_db.pool(max_size=8)
    try:
        scope = mp11_db.scope()
        run_id = await admitted_run(pool, scope)
        executor = RecordingExecutor()
        gateway = Gateway(pool, scope, executor)
        original = await gateway.service.prepare(_prepare(run_id, report="q3"), AGENT)
        assert original.human_task is not None
        await gateway.review(original.human_task["human_task_id"])
        with pytest.raises(GovernedRejected) as changed:
            await gateway.service.execute(original.intent_id, AGENT, arguments={"report": "q4"})
        assert changed.value.code == "arguments_changed"
        edited = await gateway.service.prepare(_prepare(run_id, report="q4"), AGENT)
        assert edited.intent_id != original.intent_id and edited.status == "pending_approval"
        assert edited.human_task is not None
        assert edited.human_task["human_task_id"] != original.human_task["human_task_id"]

        denied = await gateway.service.prepare(_prepare(run_id, report="deny-me"), AGENT)
        cancelled = await gateway.service.prepare(_prepare(run_id, report="cancel-me"), AGENT)
        assert denied.human_task is not None and cancelled.human_task is not None
        await gateway.review(denied.human_task["human_task_id"], "deny", comment="not public")
        await gateway.review(cancelled.human_task["human_task_id"], "cancel")
        first = await gateway.service.execute(denied.intent_id, AGENT)
        second = await gateway.service.execute(cancelled.intent_id, AGENT)
        assert (first.status, first.reason) == ("denied", "reviewer_denied")
        assert (second.status, second.reason) == ("cancelled", "reviewer_cancelled")
        assert (await gateway.service.execute(denied.intent_id, AGENT)).status == "denied"
        assert executor.calls == []
        rows = await owner_rows(
            mp11_db,
            "SELECT state FROM mission_control.governed_effect_intent ORDER BY state",
        )
        assert [row["state"] for row in rows] == [
            "cancelled",
            "denied",
            "pending_approval",
            "pending_approval",
        ]
    finally:
        await pool.close()


async def test_fence_after_approval_and_crashed_claims(
    mp11_db: CommonDatabase,  # noqa: F811
) -> None:
    pool = await mp11_db.pool(max_size=8)
    try:
        scope = mp11_db.scope()
        run_id = await admitted_run(pool, scope)
        clock = Clock(datetime.now(UTC))
        executor = ReconcilingExecutor()
        gateway = Gateway(pool, scope, executor, clock=clock)

        crashed = await gateway.service.prepare(_prepare(run_id, report="crash"), AGENT)
        parked = await gateway.service.prepare(_prepare(run_id, report="park"), AGENT)
        for state in (crashed, parked):
            assert state.human_task is not None
            await gateway.review(state.human_task["human_task_id"])
            # A process claimed the intent and died before recording the receipt.
            claimed = await gateway.intents.transition(
                scope,
                state.intent_id,
                from_states=frozenset({"pending_approval"}),
                to_state="executing",
                at=clock(),
                claimant_ref="worker-dead#1",
            )
            assert claimed is not None and claimed.state == "executing"
        within_lease = await gateway.service.execute(crashed.intent_id, AGENT)
        assert within_lease.status == "executing" and executor.calls == []
        executor.recorded[crashed.intent_id] = GovernedEffectResult(
            outcome="applied", output={"published": True}, external_ref="fixture:crash"
        )
        clock.at = clock.at + timedelta(seconds=600)
        reconciled = await gateway.service.execute(crashed.intent_id, AGENT)
        assert reconciled.status == "executed" and reconciled.receipt is not None
        assert reconciled.receipt.external_ref == "fixture:crash"
        doubtful = await gateway.service.execute(parked.intent_id, AGENT)
        assert (doubtful.status, doubtful.reason) == ("in_doubt", "claim_lease_expired")
        assert executor.calls == []  # neither claim was re-executed blindly

        fenced = await gateway.service.prepare(_prepare(run_id, report="fenced"), AGENT)
        assert fenced.human_task is not None
        await gateway.review(fenced.human_task["human_task_id"])
        await gateway.fences.persist(
            StopFence(
                request_scope=scope,
                run_id=run_id,
                generation=1,
                command_id="cancel-immediate-1",
                reason="operator stop",
                requested_at=datetime.now(UTC),
            )
        )
        outcome = await gateway.service.execute(fenced.intent_id, AGENT)
        assert (outcome.status, outcome.reason) == ("fenced", "stop_fenced")
        assert executor.calls == []
        admissions = await owner_rows(
            mp11_db,
            "SELECT decision, reason_code FROM mission_control.stop_fence_effect_admission "
            "WHERE effect_ref = $1",
            f"governed:{fenced.intent_id}",
        )
        assert [(r["decision"], r["reason_code"]) for r in admissions] == [("deny", "STOP_FENCED")]
        with pytest.raises(GovernedRejected) as refused:
            await gateway.service.prepare(_prepare(run_id, report="after"), AGENT)
        assert refused.value.code == "stop_fenced"
    finally:
        await pool.close()


async def test_missing_client_elicitation_capability_is_typed_and_writes_nothing(
    mp11_db: CommonDatabase,  # noqa: F811
) -> None:
    pool = await mp11_db.pool(max_size=4)
    try:
        scope = mp11_db.scope()
        run_id = await admitted_run(pool, scope)
        prompt = ElicitationPrompt(
            message="Which environment?",
            requested_schema={"type": "object", "properties": {"environment": {"type": "string"}}},
        )
        gateway = Gateway(pool, scope, RecordingExecutor(), elicitation=prompt)
        principal = CoordinatorPrincipal(
            actor_id="agent:lane",
            tenant_scope=scope,
            roles=frozenset(),
            permissions=frozenset({GOVERNED_EFFECT_PERMISSION}),
            request_scope=scope,
        )

        class Principals:
            async def resolve(self, context: Any) -> CoordinatorPrincipal:
                del context
                return principal

        server = FastMCP("mp11-governed-pg")
        register_governed_tools(
            server,
            ScopedGovernedEffects({scope: gateway.service}),
            Principals(),
            call=_principal_call,
        )
        async with Client(server) as client:  # this client declares no elicitation capability
            reply = await client.call_tool(
                GOVERNED_PREPARE_TOOL,
                {
                    "tool_name": "publish_report",
                    "arguments": {"report": "q3"},
                    "run_id": run_id,
                    "harness_execution_id": "harness-exec-1",
                    "generation": 1,
                    "lane_profile": "codex",
                },
                raise_on_error=False,
            )
        body = reply.structured_content
        assert body is not None and body["ok"] is False
        assert body["error"]["details"]["code"] == "elicitation_unsupported"
        assert await _count(mp11_db, "governed_effect_intent") == 0
        rows = await owner_rows(
            mp11_db,
            "SELECT count(*) AS n FROM mission_control.human_task WHERE kind LIKE 'approval:%'",
        )
        assert rows[0]["n"] == 0
    finally:
        await pool.close()
