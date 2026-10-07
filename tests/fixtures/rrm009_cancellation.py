"""RRM-009: running cancellation (RRM-008) inside the production composition.

Reusable drills over `open_production_stack` (the deployment's API, its workers built by
`ProductionWorkerActivityCompositionFactory`, a persistent `start_local` namespace, the
disposable PostgreSQL). A StageGraph run is admitted and launched through the
facade; its `draft` unit's Deep Agent is held at a chosen point; the cancel enters through
`POST /run-control/v1/runs/{run_id}/commands`, is journaled first, delivered root-first in
the `cancel` space, reaches the running Activity through its heartbeat and is reconciled by
the saga until the run is terminal `cancelled` and the cancel's receipts are `applied`.

Windows:
* `sync_child`: the in-process sync subagent's model call is in flight (deterministic).
* `cognition`: an async child is running on the Agent Server and the parent's next model call
  is in flight (deterministic parent, real hosted child).
* `completion_wait`: an async child is running and the parent's cognition has finished; the
  operation boundary is waiting for the child (`AsyncChildCompletion`).

The async windows need a real Agent Server with signed scope claims. Historical RRM-009
uses an opt-in provider model; Mission Control also qualifies the same protocol with a
distinct deterministic hosted model. RRM-010 reuses `run_cancellation_drill`.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from collections.abc import Awaitable, Callable
from contextlib import AsyncExitStack
from dataclasses import asdict, dataclass, field, replace
from datetime import UTC, datetime
from typing import Any, Literal, cast
from uuid import uuid4

from langchain_core.language_models import BaseChatModel
from langchain_core.messages import AIMessage, BaseMessage, ToolMessage
from langchain_core.outputs import ChatGeneration, ChatResult
from langgraph_sdk import get_client
from langgraph_sdk.errors import NotFoundError
from temporalio.api.enums.v1 import EventType
from tests.fixtures.rrm009_production_harness import (
    PRINCIPAL,
    ProductionStack,
    _admit,
    _command,
    _launch,
    _receipt_states,
    _release_wait,
    _replay,
    _run,
    _send,
    _wait_for,
)
from tests.fixtures.rrm009_production_stack import (
    CHILD_MARKER,
    OPERATOR,
    SCOPE,
    ChildModel,
    TechnicalBinding,
    TechnicalCatalog,
    TechnicalModel,
    call_usage,
    publish_technical_catalog,
    stage_input,
    stage_templates,
    technical_binding,
)

from mission_control.adapters.agent_server.async_subagents.auth import mint_scope_claim
from mission_control.adapters.agent_server.async_subagents.bindings import (
    technical_child_definition,
)
from mission_control.adapters.deep_agents.async_subagents import (
    PROVIDER_USAGE_STATE_KEY,
    REQUEST_SCOPE_HEADER,
    SPAWN_KEY_METADATA,
    attribute_usage,
)
from mission_control.adapters.postgres.async_subagents.async_subagent_detail_repository import (
    PostgresAsyncSubagentDetailRepository,
)
from mission_control.adapters.postgres.async_subagents.async_subagents import (
    PostgresAsyncSubagentAuthority,
)
from mission_control.adapters.postgres.orchestration.stagegraph_repository import (
    PostgresStageGraphOperationTemplateRepository,
)
from mission_control.adapters.temporal.deployment_composition import DeploymentCapabilityComponents
from mission_control.adapters.temporal.worker import create_production_workers
from mission_control.application.subordinates.parent_effects import (
    async_child_effect_id,
    async_child_usage_id,
)
from mission_control.bootstrap.technical_api import api
from mission_control.domain.authoring.contracts import SecretRef
from mission_control.domain.execution.async_subagent_reconciliation import (
    ASYNC_CHILD_RECONCILE_PERMISSION,
)
from mission_control.domain.execution.contracts import (
    AsyncSubagentContract,
    AsyncSubagentDependencyClass,
    AsyncSubagentLifecycle,
    AsyncSubagentUsage,
    DeepAgentExecutionBinding,
    OperationExecutionRequest,
)
from mission_control.domain.policies.contracts import ActorContext, CancelAction
from mission_control.interfaces.http.control_plane import get_control_plane_principal

CancellationWindow = Literal["sync_child", "cognition", "completion_wait"]
TOKEN_ENV = "BELLABS_ASYNC_SUBAGENT_SERVER_TOKEN"
TOKEN_REF = SecretRef(provider="environment", key=TOKEN_ENV)
ASYNC_CHILD_NAME = "async-child"
# A real hosted child that stays running long enough for the cancel to land.
ASYNC_CHILD_OBJECTIVE = "Call wait_seconds with seconds=120, then reply with exactly PONG."
# Carved from the `draft` slot reservation (40 tokens): the parent's two calls use 10.
ASYNC_CHILD_LIMITS = {"tokens.total": 10}
# Ceilings large enough to record a cancelled child's actual usage (actuals are never
# dropped, REQ-CP-RUN-009), while every reservation stays the blueprint's.
CANCELLATION_CEILINGS = {
    "tokens.total": 400_000,
    "model.turns": 60,
    "operation.attempts": 12,
    "goal.iterations": 6,
}
# The deployment settings these drills run with (on top of `runtime_environment`): heartbeat
# timeouts per operation class and a worker drain shorter than all of them.
CANCELLATION_ENVIRONMENT = {
    "OPERATION_HEARTBEAT_TIMEOUT_SECONDS": "20",
    "OPERATION_ASYNC_CHILDREN_HEARTBEAT_TIMEOUT_SECONDS": "10",
    "OPERATION_BOUND_HEARTBEAT_TIMEOUT_SECONDS": "20",
    "WORKER_GRACEFUL_SHUTDOWN_SECONDS": "3",
}
DRAFT_OPERATION = (
    "execution-epoch:1:stage:draft:mapped:none:workflow-cycle:0:stage-cycle:0:slot:execute"
)
EXPECTED_TRANSITION: dict[CancellationWindow, str] = {
    "sync_child": "interrupted",
    "cognition": "interrupted",
    "completion_wait": "terminal_unobserved",
}
RECONCILER = PRINCIPAL.model_copy(
    update={"roles": frozenset({"reconciliation_operator", "auditor"})}
)


@dataclass
class CancellationGate:
    """Where cognition is held, and the event the drill waits on before cancelling."""

    window: CancellationWindow
    held: asyncio.Event = field(default_factory=asyncio.Event)
    # `cognition` only: replace the worker set while the child runs and the parent's call is
    # held, so the retried attempt reconnects to the same child before the cancel lands.
    restart_workers: bool = False


def _since_input(messages: list[BaseMessage]) -> list[BaseMessage]:
    human = [index for index, item in enumerate(messages) if item.type == "human"]
    return messages[human[-1] :] if human else messages


class GatedChildModel(ChildModel):
    """The sync subagent's model call that never returns: the cancel lands inside it."""

    gate: Any

    async def _agenerate(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None = None,
        run_manager: Any = None,
        **kwargs: Any,
    ) -> ChatResult:
        del stop, run_manager, kwargs
        self._record("child", messages)
        self.gate.held.set()
        await asyncio.Event().wait()
        raise AssertionError("a cancelled sync child call never returns")


class SpawningModel(TechnicalModel):
    """The parent's cognition: one `start_async_task` (a real hosted child), then either a
    held call (`cognition`) or the final answer (`completion_wait`)."""

    gate: Any

    async def _agenerate(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None = None,
        run_manager: Any = None,
        **kwargs: Any,
    ) -> ChatResult:
        del stop, run_manager, kwargs
        since = _since_input(messages)
        tools = sum(isinstance(item, ToolMessage) for item in since)
        self._record("parent", messages)
        if tools == 0:
            message = AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "start_async_task",
                        "args": {
                            "description": ASYNC_CHILD_OBJECTIVE,
                            "subagent_type": ASYNC_CHILD_NAME,
                        },
                        "id": f"rrm009-spawn-{self.operation_id[-12:]}",
                        "type": "tool_call",
                    }
                ],
                usage_metadata=call_usage(),
            )
            return ChatResult(generations=[ChatGeneration(message=message)])
        if self.gate.window == "cognition":
            self.gate.held.set()
            await asyncio.Event().wait()
            raise AssertionError("a cancelled parent call never returns")
        answer = {"answer": "RRM009-SPAWNED", "facts": {}, "output_refs": []}
        return ChatResult(
            generations=[
                ChatGeneration(
                    message=AIMessage(content=json.dumps(answer), usage_metadata=call_usage())
                )
            ]
        )


def async_child_binding(endpoint: str) -> tuple[TechnicalBinding, AsyncSubagentContract]:
    """The technical binding plus the hosted technical child's contract (RRM-013 shape)."""

    technical = technical_binding()
    contract = technical_child_definition().contract(
        agent_protocol_url=endpoint.rstrip("/"),
        name=ASYNC_CHILD_NAME,
        budget_limits=dict(ASYNC_CHILD_LIMITS),
        timeout_seconds=300,
        dependency_classes=frozenset({AsyncSubagentDependencyClass.REQUIRED_BLOCKING}),
    )
    binding = DeepAgentExecutionBinding.create(
        **{
            **technical.binding.model_dump(
                mode="python", exclude={"binding_digest", "async_subagents"}
            ),
            "async_subagents": (contract,),
        }
    )
    return replace(technical, binding=binding), contract


def cancellation_components(
    technical: TechnicalBinding, gate: CancellationGate, model_log: list[dict[str, Any]]
) -> DeploymentCapabilityComponents:
    """The deterministic models the drill registers beside the pins (exact digests)."""

    def parent(bound: Any, _secrets: Any) -> BaseChatModel:
        if gate.window == "sync_child":
            return TechnicalModel(
                run_id=bound.run_id, operation_id=bound.operation_id, log=model_log
            )
        return SpawningModel(
            run_id=bound.run_id, operation_id=bound.operation_id, log=model_log, gate=gate
        )

    def child(bound: Any, _secrets: Any) -> BaseChatModel:
        return GatedChildModel(
            run_id=bound.run_id, operation_id=bound.operation_id, log=model_log, gate=gate
        )

    return DeploymentCapabilityComponents(
        model_factories={
            technical.binding.model.ref.digest: parent,
            technical.child_model_ref.digest: child,
        },
        prompts={technical.child_prompt_ref.digest: f"Reply with exactly {CHILD_MARKER}."},
        skill_bundles={technical.bundle.bundle_digest: technical.bundle},
        mcp_servers={server.ref.digest: server for server in technical.binding.mcp_servers},
    )


def _with_token(template: OperationExecutionRequest) -> OperationExecutionRequest:
    return OperationExecutionRequest.model_validate(
        {
            **template.model_dump(mode="python"),
            "secret_refs": (*template.secret_refs, TOKEN_REF),
        }
    )


# --- Agent Server reads (scope-claim bearer) -------------------------------------------------


def _sdk() -> Any:
    return get_client(
        url=os.environ["AGENT_SERVER_ENDPOINT"].rstrip("/"),
        headers={
            "Authorization": f"Bearer {mint_scope_claim(os.environ[TOKEN_ENV], SCOPE)}",
            REQUEST_SCOPE_HEADER: SCOPE,
        },
    )


async def provider_runs(child_id: str) -> list[dict[str, Any]]:
    try:
        runs = await _sdk().runs.list(child_id, limit=100)
    except NotFoundError:
        return []
    return [run for run in runs if (run.get("metadata") or {}).get(SPAWN_KEY_METADATA) == child_id]


async def provider_status(child_id: str, run_id: str) -> str:
    return str((await _sdk().runs.get(child_id, run_id)).get("status") or "")


async def attributed_usage(child_id: str, provider_run_id: str) -> AsyncSubagentUsage:
    """What the provider's durable thread state attributes to the cancelled run; when it
    attributes nothing, the operator's recorded decision is zero (never a default)."""

    state = await _sdk().threads.get_state(child_id)
    values = state.get("values") or {}
    observed = attribute_usage(
        provider_run_id,
        values.get("messages"),
        ASYNC_CHILD_LIMITS,
        provider_usage=values.get(PROVIDER_USAGE_STATE_KEY),
    )
    if observed.attribution != "provider_attributed":
        observed = AsyncSubagentUsage(
            provider_run_id=provider_run_id,
            attribution="provider_attributed",
            attributed_amounts={"tokens.total": 0},
        )
    return observed


# --- Durable reads --------------------------------------------------------------------------


async def operation_rows(stack: ProductionStack, run_id: str) -> list[dict[str, Any]]:
    async with stack.owner_pool.acquire() as connection:
        rows = await connection.fetch(
            """
            SELECT DISTINCT ON (c.semantic_attempt_key)
                   c.semantic_attempt_key, s.status, s.failure_code, s.usage_payload
            FROM mission_control.operation_claim c
            JOIN mission_control.operation_settlement s
              ON (s.installation_id, s.application_id, s.tenant_id, s.claim_key)
               = (c.installation_id, c.application_id, c.tenant_id, c.claim_key)
            WHERE c.run_key = $1
            ORDER BY c.semantic_attempt_key, s.settlement_revision DESC
            """,
            run_id,
        )
    return [
        {
            "operation": row["semantic_attempt_key"].split(":operation:")[1],
            "status": row["status"],
            "failure_code": row["failure_code"],
            "usage": json.loads(row["usage_payload"]),
        }
        for row in rows
    ]


async def transitions(
    stack: ProductionStack, run_id: str, operation: str | None = None
) -> list[str]:
    """The run's recorded checkpoint transitions, or only those of one semantic operation."""

    attempt_pattern = f"%:operation:{operation}:attempt:%" if operation is not None else "%"
    async with stack.owner_pool.acquire() as connection:
        rows = await connection.fetch(
            """
            SELECT t.classification FROM mission_control.checkpoint_transition t
            WHERE (t.installation_id, t.application_id, t.tenant_id, t.unit_key) IN (
                SELECT c.installation_id, c.application_id, c.tenant_id, c.unit_key
                FROM mission_control.operation_claim c
                WHERE c.run_key = $1 AND c.semantic_attempt_key LIKE $2
            )
            ORDER BY t.observed_at
            """,
            run_id,
            attempt_pattern,
        )
    return [str(row["classification"]) for row in rows]


async def scheduled_activities(
    stack: ProductionStack, workflow_id: str
) -> list[tuple[str, float | None]]:
    """(activity type, heartbeat timeout in seconds) of every scheduled Activity."""

    history = await stack.client.get_workflow_handle(workflow_id).fetch_history()
    scheduled: list[tuple[str, float | None]] = []
    for event in history.events:
        if event.event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:
            attributes = event.activity_task_scheduled_event_attributes
            timeout = (
                attributes.heartbeat_timeout.ToTimedelta().total_seconds()
                if attributes.HasField("heartbeat_timeout")
                else None
            )
            scheduled.append((attributes.activity_type.name, timeout))
    return scheduled


async def completed_activities(stack: ProductionStack, workflow_id: str, name: str) -> int:
    """How many `name` Activities of the execution completed (returned a result)."""

    history = await stack.client.get_workflow_handle(workflow_id).fetch_history()
    scheduled = {
        event.event_id
        for event in history.events
        if event.event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED
        and event.activity_task_scheduled_event_attributes.activity_type.name == name
    }
    return sum(
        1
        for event in history.events
        if event.event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_COMPLETED
        and event.activity_task_completed_event_attributes.scheduled_event_id in scheduled
    )


async def signals(stack: ProductionStack, workflow_id: str) -> list[str]:
    history = await stack.client.get_workflow_handle(workflow_id).fetch_history()
    return [
        event.workflow_execution_signaled_event_attributes.signal_name
        for event in history.events
        if event.event_type == EventType.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED
    ]


async def workflow_state(stack: ProductionStack, workflow_id: str) -> str:
    """The execution's status, with the close event's failure when it failed."""

    handle = stack.client.get_workflow_handle(workflow_id)
    description = await handle.describe()
    status = description.status.name if description.status is not None else "UNKNOWN"
    if status == "RUNNING":
        return status
    history = await handle.fetch_history()
    return f"{status}: {str(history.events[-1])[:1500]}"


async def _json(stack: ProductionStack, path: str) -> dict[str, Any]:
    response = await stack.http.get(path, params={"request_scope": SCOPE})
    assert response.status_code == 200, response.text
    return cast(dict[str, Any], response.json())


# --- The drill ------------------------------------------------------------------------------


async def publish_cancellation_catalog(stack: ProductionStack) -> TechnicalCatalog:
    """The StageGraph catalog the drills run under; publish it once per stack."""

    return await publish_technical_catalog(
        stack.control_plane,
        family="StageGraph",
        now=datetime.now(UTC),
        ceilings=CANCELLATION_CEILINGS,
    )


async def launch_stagegraph(
    stack: ProductionStack,
    technical: TechnicalBinding,
    *,
    async_children: bool,
    catalog: TechnicalCatalog | None = None,
    draft_technical: TechnicalBinding | None = None,
) -> str:
    """Admit and launch a StageGraph run; `draft_technical`, when given, binds the `draft`
    stage instead of `technical` (which then binds `review` only)."""

    if catalog is None:
        catalog = await publish_cancellation_catalog(stack)
    run_id = await _admit(stack, catalog, f"rrm009-cancel-{uuid4().hex[:12]}")
    binding_ref = f"semantic-input:rrm009-cancel:{run_id}"
    templates = stage_templates(technical, catalog)
    if draft_technical is not None:
        templates["draft/execute/default"] = stage_templates(draft_technical, catalog)[
            "draft/execute/default"
        ]
    if async_children:
        templates = {key: _with_token(value) for key, value in templates.items()}
    await PostgresStageGraphOperationTemplateRepository(stack.worker_pool).persist_templates(
        request_scope=SCOPE,
        semantic_input_binding_ref=binding_ref,
        templates=templates,
        recorded_at=datetime.now(UTC),
    )
    await _launch(
        stack,
        run_id,
        {
            "request_scope": SCOPE,
            "run_id": run_id,
            "family": "StageGraph",
            "stagegraph": asdict(stage_input(catalog, run_id, binding_ref, 1)),
        },
    )
    return run_id


async def restart_workers(stack: ProductionStack) -> None:
    """Shut the deployment's worker set down (graceful drain, then the in-flight Activity is
    cancelled with `worker_shutdown`) and start a new one over the same composition."""

    await stack.worker_stack.aclose()
    replacement = AsyncExitStack()
    workers = create_production_workers(stack.client, stack.settings, stack.composition)
    for worker in workers.workers:
        await replacement.enter_async_context(worker)
    # The fixture's own exit closes the replacement set.
    stack.worker_stack.push_async_callback(replacement.aclose)


def _parent_calls(stack: ProductionStack, run_id: str, stage: str) -> int:
    """The parent model calls of the run's operations whose ID contains `stage`."""

    return sum(
        1
        for item in stack.model_log
        if item["run_id"] == run_id and item["model"] == "parent" and stage in item["operation"]
    )


async def cancel_through_the_facade(stack: ProductionStack, run_id: str) -> str:
    run = await _run(stack, run_id)
    command_id = f"cancel:{run_id[:8]}"
    accepted = await _send(
        stack,
        run_id,
        _command(run_id, run["version"], command_id, CancelAction(), "workflow_run.cancel"),
    )
    assert (accepted["status"], accepted["phase"]) == ("accepted", "cancelling"), accepted
    return command_id


async def _child(stack: ProductionStack, run_id: str) -> tuple[str, str]:
    """The run's single async child and its provider run, once submitted."""

    async def submitted() -> bool:
        children = await PostgresAsyncSubagentAuthority(stack.worker_pool).list_children(
            SCOPE, run_id
        )
        return len(children) == 1 and children[0].provider_run_id is not None

    await _wait_for(stack, run_id, submitted, 120)
    (view,) = await PostgresAsyncSubagentAuthority(stack.worker_pool).list_children(SCOPE, run_id)
    assert view.provider_run_id is not None
    return view.child_execution_id, view.provider_run_id


async def reconcile_child_usage(
    stack: ProductionStack, run_id: str, child_id: str, usage: AsyncSubagentUsage
) -> Any:
    """The privileged API route, as the reconciliation operator."""

    path = f"/run-control/v1/runs/{run_id}/async-children/{child_id}/reconcile-usage"
    body = {
        "request_scope": SCOPE,
        "actor": ActorContext(
            actor_id=OPERATOR, permissions=frozenset({ASYNC_CHILD_RECONCILE_PERMISSION})
        ).model_dump(mode="json"),
        "run_usage": {usage.provider_run_id: usage.model_dump(mode="json")},
        "settlement_ref": f"settlement:{child_id}:reconciled",
    }
    # The plain operator role does not hold the privilege.
    refused = await stack.http.post(path, json=body)
    assert refused.status_code == 403, refused.text
    api.dependency_overrides[get_control_plane_principal] = lambda: RECONCILER
    try:
        return await stack.http.post(path, json=body)
    finally:
        api.dependency_overrides[get_control_plane_principal] = lambda: PRINCIPAL


BeforeCancel = Callable[[str, str | None, str | None], Awaitable[dict[str, Any]]]


async def run_cancellation_drill(
    stack: ProductionStack,
    technical: TechnicalBinding,
    gate: CancellationGate,
    *,
    catalog: TechnicalCatalog | None = None,
    before_cancel: BeforeCancel | None = None,
    settled_draft: TechnicalBinding | None = None,
) -> dict[str, Any]:
    """Cancel a launched StageGraph run while its held unit is held at `gate.window`;
    return the evidence after the run is terminal. Asserts every saga step on the way.

    The held unit is `draft`. With `settled_draft` (a binding without the held behaviour)
    the `draft` stage settles first, the run's declared wait is released through the facade
    and the held unit is `review`, so the run already has an accepted stage while it is held.
    `catalog` reuses a catalog already published on the stack (a stack publishes each
    Workflow Type once). `before_cancel(run_id, child_id, provider_run_id)` runs while the
    unit is held (and an async child is running), just before the cancel is sent; its
    result is recorded under `evidence["before_cancel"]`."""

    async_children = gate.window != "sync_child"
    run_id = await launch_stagegraph(
        stack,
        technical,
        async_children=async_children,
        catalog=catalog,
        draft_technical=settled_draft,
    )
    held_operation = (
        DRAFT_OPERATION
        if settled_draft is None
        else DRAFT_OPERATION.replace(":stage:draft:", ":stage:review:")
    )
    root_id = f"belllabs-run/{run_id}"
    family_id = f"family/{run_id}/1"
    operation_id = f"operation/{run_id}:operation:{held_operation}:attempt:1"
    held_stage = held_operation.split(":slot:")[0]
    settled_rows = [] if settled_draft is None else [(f"{DRAFT_OPERATION}:attempt:1", "completed")]
    evidence: dict[str, Any] = {"run_id": run_id, "window": gate.window}
    child_id = provider_run_id = None
    refused_at = 0.0
    if settled_draft is not None:
        # `draft` settles and the run holds its declared wait; the wait is released through
        # the facade (`applied`) and `review` is admitted and held.
        await _release_wait(stack, run_id)
        evidence["wait_release_receipts"] = await _receipt_states(
            stack, run_id, f"release:{run_id[:8]}"
        )
    if gate.window in {"sync_child", "cognition"}:
        await asyncio.wait_for(gate.held.wait(), timeout=180)
    if async_children:
        child_id, provider_run_id = await _child(stack, run_id)

        async def child_running() -> bool:
            return await provider_status(child_id, provider_run_id) == "running"

        await _wait_for(stack, run_id, child_running, 120)
    if gate.restart_workers:
        assert gate.window == "cognition"
        await restart_workers(stack)

        # The retried attempt resumes the interrupted lineage (the held call never completed,
        # so it is asked again) and is held again; nothing was spawned twice.
        async def resumed() -> bool:
            return _parent_calls(stack, run_id, held_stage) == 3

        await _wait_for(stack, run_id, resumed, 180)
        # A worker shutdown is not a cancel of the unit (RRM-008 F1): nothing settled.
        assert [
            (row["operation"], row["status"]) for row in await operation_rows(stack, run_id)
        ] == settled_rows
        assert (await _run(stack, run_id))["phase"] == "active"
        assert child_id is not None and provider_run_id is not None
        children = await PostgresAsyncSubagentAuthority(stack.worker_pool).list_children(
            SCOPE, run_id
        )
        assert [item.child_execution_id for item in children] == [child_id]
        assert await provider_status(child_id, provider_run_id) == "running"
        assert len(await provider_runs(child_id)) == 1
        evidence["restart"] = {
            "parent_calls_before_cancel": _parent_calls(stack, run_id, held_stage)
        }
    if gate.window == "completion_wait":
        # The parent's cognition finished (both calls answered); the boundary waits.
        async def cognition_finished() -> bool:
            return _parent_calls(stack, run_id, held_stage) == 2

        await _wait_for(stack, run_id, cognition_finished, 120)
        await asyncio.sleep(1)
        assert (await _run(stack, run_id))["phase"] == "active"
    if before_cancel is not None:
        evidence["before_cancel"] = await before_cancel(run_id, child_id, provider_run_id)
    command_id = await cancel_through_the_facade(stack, run_id)

    async def delivered() -> bool:
        return "delivered" in await _receipt_states(stack, run_id, command_id)

    await _wait_for(stack, run_id, delivered, 60)
    if async_children:
        assert child_id is not None and provider_run_id is not None
        details = PostgresAsyncSubagentDetailRepository(stack.worker_pool)

        async def child_settled_by_the_parent() -> bool:
            link = await details.get_link(SCOPE, child_id)
            return link.usage_disposition is not None

        await _wait_for(stack, run_id, child_settled_by_the_parent, 120)
        execution = await details.get_execution(SCOPE, child_id)
        link = await details.get_link(SCOPE, child_id)
        # Cancelled at the provider and acknowledged; rejected before settlement (a late
        # result can never mutate the cancelled parent); its usage pending.
        assert execution.lifecycle == AsyncSubagentLifecycle.CANCELLED, execution
        assert link.cancellation_receipt == "provider_acknowledged", link
        assert link.result_decision == "reject", link
        assert link.usage_disposition == "pending_usage", link
        runs = await provider_runs(child_id)
        assert len(runs) == 1 and runs[0]["status"] in {"interrupted", "cancelled"}, runs
        budget = await _json(stack, f"/run-control/v1/runs/{run_id}/budget")
        pending = budget["usage_records"][async_child_usage_id(child_id)]
        evidence["child"] = {
            "child_execution_id": child_id,
            "provider_run_id": provider_run_id,
            "provider_status": runs[0]["status"],
            "lifecycle": execution.lifecycle.value,
            "cancellation_receipt": link.cancellation_receipt,
            "result_decision": link.result_decision,
            "usage_disposition": link.usage_disposition,
            "pending_before": pending["pending_external_amounts"],
        }
        # The pending usage is a liability: the run stays `cancelling` and the family waits
        # on it (liability backoff or the `liability_reconciled` hint).
        await asyncio.sleep(3)
        assert (await _run(stack, run_id))["phase"] == "cancelling"
        family_state = await workflow_state(stack, family_id)
        assert family_state == "RUNNING", (
            family_state,
            await workflow_state(stack, operation_id),
            await operation_rows(stack, run_id),
        )

        # The family proposed terminalization and the reducer refused it on the liability:
        # the family now waits on its backoff (30 s) or the `liability_reconciled` hint.
        async def terminal_proposal_refused() -> bool:
            return await completed_activities(stack, family_id, "stagegraph.complete") >= 1

        await _wait_for(stack, run_id, terminal_proposal_refused, 120)
        assert (await _run(stack, run_id))["phase"] == "cancelling"
        refused_at = time.monotonic()
        usage = await attributed_usage(child_id, provider_run_id)
        response = await reconcile_child_usage(stack, run_id, child_id, usage)
        assert response.status_code == 200, response.text
        receipt = response.json()
        assert receipt["link"]["settled"] is True, receipt
        assert receipt["link"]["usage_disposition"] == "settled", receipt
        assert receipt["liability_hint_sent"] is True, receipt
        evidence["usage_reconciliation"] = {
            "attributed": dict(usage.attributed_amounts),
            "settlement_revision": receipt["link"]["settlement_revision"],
        }

    async def terminal() -> bool:
        return (await _run(stack, run_id))["phase"] == "terminal"

    await _wait_for(stack, run_id, terminal, 180)
    if async_children:
        # Woken by the hint, not by the family's 30 s liability timer.
        evidence["terminal_after_refusal_seconds"] = round(time.monotonic() - refused_at, 1)
        assert evidence["terminal_after_refusal_seconds"] < 25, evidence
    run = await _run(stack, run_id)
    assert run["terminal_outcome"] == "cancelled", run
    receipts = await _receipt_states(stack, run_id, command_id)
    assert receipts == ["accepted", "delivered", "applied"], receipts
    rows = await operation_rows(stack, run_id)
    assert [(row["operation"], row["status"]) for row in rows] == [
        *settled_rows,
        (f"{held_operation}:attempt:1", "cancelled"),
    ], rows
    # The head advances over the partial lineage by one recorded transition: the held call
    # left the leaf interrupted; a finished cognition left a terminal leaf nobody observed.
    assert await transitions(stack, run_id, held_operation) == [EXPECTED_TRANSITION[gate.window]]
    effects = await _json(stack, f"/run-control/v1/runs/{run_id}/effects")
    claims = {claim["effect_kind"]: claim for claim in effects["claims"].values()}
    runtime_dispositions = sorted(
        claim["disposition"]
        for claim in effects["claims"].values()
        if claim["effect_kind"] == "operation.runtime"
    )
    assert runtime_dispositions == (
        ["cancelled"] if settled_draft is None else ["cancelled", "succeeded"]
    ), effects["claims"]
    assert all(claim["settlement"] is not None for claim in effects["claims"].values()), claims
    budget = await _json(stack, f"/run-control/v1/runs/{run_id}/budget")
    assert budget["reservations"] == {}, budget["reservations"]
    assert not any(budget["pending_settlement"].values()), budget["pending_settlement"]
    scheduled = await scheduled_activities(stack, operation_id)
    names = [name for name, _ in scheduled]
    assert names[0] == "operation.execute" and "operation.cancel" in names, scheduled
    expected_heartbeat = 10.0 if async_children else 20.0
    assert {timeout for _, timeout in scheduled} == {expected_heartbeat}, scheduled
    run_calls = [item for item in stack.model_log if item["run_id"] == run_id]
    parent_calls = [
        item for item in run_calls if item["model"] == "parent" and held_stage in item["operation"]
    ]
    child_calls = [
        item for item in run_calls if item["model"] == "child" and held_stage in item["operation"]
    ]
    # Nothing resumed after the cancel and no stage after the held one was admitted; a
    # settled `draft` made its own four parent calls and one sync-child call.
    other_calls = [item for item in run_calls if held_stage not in item["operation"]]
    if settled_draft is None:
        assert other_calls == [], other_calls
    else:
        assert all(
            DRAFT_OPERATION.split(":slot:", maxsplit=1)[0] in item["operation"]
            for item in other_calls
        )
        assert sorted(item["model"] for item in other_calls) == ["child", *["parent"] * 4]
    # sync_child: the parent's `task` call, then the child's held call; async windows: the
    # spawn call, then the held (cognition) or final (completion_wait) parent call.
    expected_calls = (
        (1, 1) if gate.window == "sync_child" else (3, 0) if gate.restart_workers else (2, 0)
    )
    assert (len(parent_calls), len(child_calls)) == expected_calls, stack.model_log
    if async_children:
        assert child_id is not None
        child_claim = effects["claims"][async_child_effect_id(child_id)]
        assert child_claim["disposition"] == "cancelled" and child_claim["settlement"], child_claim
        assert "liability_reconciled" in await signals(stack, family_id)
    replayed = await _replay(stack.client, [root_id, family_id])
    evidence.update(
        {
            "terminal_outcome": run["terminal_outcome"],
            "receipts": receipts,
            "operations": rows,
            "transitions": await transitions(stack, run_id, held_operation),
            "effects": sorted(
                {
                    (claim["effect_kind"], claim["disposition"])
                    for claim in effects["claims"].values()
                }
            ),
            "budget_consumed": budget["consumed"],
            "scheduled_activities": scheduled,
            "model_calls": {"parent": len(parent_calls), "child": len(child_calls)},
            "replayed_events": replayed,
        }
    )
    return evidence


__all__ = [
    "CANCELLATION_ENVIRONMENT",
    "BeforeCancel",
    "CancellationGate",
    "async_child_binding",
    "cancellation_components",
    "publish_cancellation_catalog",
    "run_cancellation_drill",
]
