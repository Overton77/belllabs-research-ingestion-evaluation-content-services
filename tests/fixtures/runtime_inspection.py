"""RRM-005 inspection fixtures shared by the unit (in-memory) and PostgreSQL suites.

The seeded world is technical only: one active run with a settled and an `in_doubt` unit,
one terminal run, and one run of another request scope. Checkpoints carry sensitive-looking
content so that redaction is observable.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.base import BaseCheckpointSaver, empty_checkpoint
from tests.fixtures.checkpoint_lineage import (
    BINDING,
    LINEAGE_NOW,
    OTHER_SCHEMA,
    SCHEMA,
    activity_attempt,
    in_doubt_incident,
    namespace_claim,
    stage_unit,
    transition,
    unit_result,
)
from tests.unit.run_control.test_run_control import (
    EMPTY_EVIDENCE_DIGEST,
    INITIAL_EVIDENCE_FRONTIER,
    NOW,
    WORKFLOW_DIGEST,
    command,
    operator_wait,
    request,
)

from mission_control.application.execution.operations.checkpoint_lineage import (
    CheckpointLineageRepository,
)
from mission_control.application.execution.service import RunControlService
from mission_control.domain.execution.checkpoint_lineage import (
    STAMP_BINDING_DIGEST,
    STAMP_EXECUTION_GENERATION,
    STAMP_INVOCATION_ID,
    STAMP_STATE_SCHEMA_DIGEST,
    STAMP_UNIT_KEY,
    cognitive_session_namespace,
    submission_invocation_id,
)
from mission_control.domain.graph_runtime.identities import RuntimeUnitIdentity
from mission_control.domain.policies.contracts import (
    ClaimEffectAction,
    CommandResult,
    CommandStatus,
    ObserveEffectAction,
    RecordUsageAction,
    ReserveBudgetAction,
    SetWaitAction,
    StartAction,
    TerminalizationProposal,
    TerminalizeAction,
)

READ_AT = LINEAGE_NOW + timedelta(hours=1)
SECRET_TEXT = "sk-rrm005-secret-value"
PROMPT_TEXT = "RRM005 confidential prompt body"
TOOL_ARGUMENT = "rrm005-tool-argument-payload"


class MutableClock:
    def __init__(self, now: datetime) -> None:
        self.now = now

    def __call__(self) -> datetime:
        return self.now


@dataclass(frozen=True)
class SeededRuns:
    active_run: str
    terminal_run: str
    other_scope_run: str
    settled: RuntimeUnitIdentity
    in_doubt: RuntimeUnitIdentity


def _config(namespace: str, parent: str | None) -> RunnableConfig:
    configurable: dict[str, Any] = {"thread_id": namespace, "checkpoint_ns": ""}
    if parent is not None:
        configurable["checkpoint_id"] = parent
    return {"configurable": configurable}


async def put_checkpoint(
    saver: BaseCheckpointSaver[Any],
    namespace: str,
    checkpoint_id: str,
    parent: str | None,
    *,
    step: int,
    stamps: dict[str, Any],
    values: dict[str, Any],
    pending: str | None = None,
) -> None:
    checkpoint = empty_checkpoint()
    checkpoint["id"] = checkpoint_id
    values = dict(values)
    if pending is not None:
        # An available trigger the node has not seen yet: the node is the next task.
        values[f"branch:to:{pending}"] = None
    checkpoint["channel_values"] = values
    versions: dict[str, Any] = dict.fromkeys(values, f"{step:032}.0.1")
    checkpoint["channel_versions"] = versions
    await saver.aput(
        _config(namespace, parent),
        checkpoint,
        {"step": step, "source": "loop", "writes": {"model": PROMPT_TEXT}, **stamps},
        versions,
    )


def stamps_for(unit: RuntimeUnitIdentity, *, schema: str = SCHEMA) -> dict[str, Any]:
    return {
        STAMP_UNIT_KEY: unit.unit_key,
        STAMP_EXECUTION_GENERATION: 1,
        STAMP_INVOCATION_ID: submission_invocation_id(unit.unit_key, 1),
        STAMP_BINDING_DIGEST: BINDING,
        STAMP_STATE_SCHEMA_DIGEST: schema,
        "belllabs_attempt_ref": "activity-attempt:fixture",
    }


def sensitive_state() -> dict[str, Any]:
    return {
        "messages": [
            HumanMessage(content=PROMPT_TEXT),
            AIMessage(
                content="",
                tool_calls=[
                    {"name": "write_todos", "args": {"secret": TOOL_ARGUMENT}, "id": "call-1"}
                ],
            ),
            ToolMessage(content=SECRET_TEXT, tool_call_id="call-1"),
        ],
        "todos": [{"content": PROMPT_TEXT, "status": "pending"}],
        "files": {"/workspace/notes.md": SECRET_TEXT},
        "artifact_index": {"report": {"digest": "sha256:" + "1" * 64}},
        "structured_response": {"answer": SECRET_TEXT},
    }


def _command(scope: str, run_id: str, version: int, command_id: str, action: Any) -> Any:
    """The unit-test lifecycle command addressed to `scope` (literal or canonical)."""

    return command(run_id, version, command_id, action).model_copy(update={"request_scope": scope})


async def terminal_run(
    run_service: RunControlService, request_id: str, *, scope: str = "tenant-1"
) -> str:
    admitted = await run_service.admit(request(request_scope=scope, request_id=request_id))
    assert admitted.run_id is not None
    run_id = admitted.run_id
    await run_service.execute(_command(scope, run_id, 1, f"{request_id}-start", StartAction()))
    await run_service.execute(
        _command(
            scope,
            run_id,
            2,
            f"{request_id}-release",
            RecordUsageAction(
                usage_id=f"{request_id}-release",
                reservation_id="baseline",
                actual_amounts={},
                release_amounts={"tokens.total": 20},
            ),
        )
    )
    terminal = await run_service.execute(
        _command(
            scope,
            run_id,
            3,
            f"{request_id}-terminalize",
            TerminalizeAction(
                proposal=TerminalizationProposal(
                    proposal_id=f"{request_id}-terminal",
                    expected_run_version=3,
                    workflow_type_digest=WORKFLOW_DIGEST,
                    obligation_revision="obligations:1",
                    evidence_frontier_digest=INITIAL_EVIDENCE_FRONTIER,
                    accepted_obligation_evidence_digest=EMPTY_EVIDENCE_DIGEST,
                    proposing_execution_binding_ref="execution:stagegraph",
                    required_obligations_accepted=True,
                    execution_failure_refs=("evaluation:workflow:failed",),
                    budget_settled=True,
                    effects_settled=True,
                    proposed_at=NOW,
                )
            ),
        )
    )
    assert terminal.status == CommandStatus.ACCEPTED
    return run_id


async def execute_command(
    run_service: RunControlService,
    run_id: str,
    command_id: str,
    action: Any,
    *,
    scope: str = "tenant-1",
) -> CommandResult:
    run = await run_service.get_run(scope, run_id)
    result = await run_service.execute(_command(scope, run_id, run.version, command_id, action))
    assert result.status == CommandStatus.ACCEPTED, result.reason
    return result


async def seed_inspection_world(
    run_service: RunControlService,
    lineage: CheckpointLineageRepository,
    saver: BaseCheckpointSaver[Any],
    *,
    scope: str = "tenant-1",
    other_scope: str = "tenant-2",
) -> SeededRuns:
    """Seed one active run (a settled and an in-doubt unit), one terminal run, and one
    run of another scope, with checkpoint lineage in `saver`. The in-memory unit tests use
    the literal scopes; the PostgreSQL proof passes two canonical tenant scopes."""

    admitted = await run_service.admit(request(request_scope=scope, request_id="inspection-active"))
    assert admitted.run_id is not None
    active = admitted.run_id
    await run_service.execute(_command(scope, active, 1, "inspection-start", StartAction()))
    terminal = await terminal_run(run_service, "inspection-terminal", scope=scope)
    other = await run_service.admit(request(request_scope=other_scope, request_id="other-scope"))
    assert other.run_id is not None

    settled = stage_unit(request_scope=scope, run_id=active, operation_id="op-settled")
    in_doubt = stage_unit(
        request_scope=scope, run_id=active, operation_id="op-in-doubt", stage_id="other"
    )

    # Settled unit: attempt (lease since expired), transition + fenced result.
    await lineage.record_attempt(
        unit=settled,
        execution_generation=1,
        attempt=activity_attempt(1, workflow_id=f"operation/{settled.semantic_operation_id}"),
        binding_id=f"binding:{settled.unit_key}:1",
        binding_digest=BINDING,
        namespace=namespace_claim(settled),
        dispatching=True,
        observed_at=LINEAGE_NOW,
        lease_expires_at=LINEAGE_NOW + timedelta(minutes=5),
    )
    observed = transition(settled, source=None, result_id="c3")
    await lineage.record_result(
        unit_result(settled, fence=1, transition_id=observed.transition_id),
        transition=observed,
    )
    namespace = cognitive_session_namespace(settled, 1)
    stamps = stamps_for(settled)
    await put_checkpoint(saver, namespace, "c1", None, step=-1, stamps=stamps, values={})
    await put_checkpoint(
        saver,
        namespace,
        "c3-parent",
        "c1",
        step=0,
        stamps=stamps,
        values=sensitive_state(),
        pending="tools",
    )
    await put_checkpoint(
        saver, namespace, "c3", "c3-parent", step=1, stamps=stamps, values=sensitive_state()
    )
    # A drifted stamp on the lineage, and a foreign branch in the same namespace.
    await put_checkpoint(
        saver,
        namespace,
        "drifted",
        "c3",
        step=2,
        stamps=stamps_for(settled, schema=OTHER_SCHEMA),
        values={"messages": []},
    )
    foreign = stage_unit(request_scope=scope, run_id=active, operation_id="op-foreign")
    await put_checkpoint(
        saver, namespace, "foreign", "c1", step=0, stamps=stamps_for(foreign), values={}
    )

    # In-doubt unit: dispatching attempt (lease held), incident, operator wait, ambiguous
    # effect on its binding.
    await lineage.record_attempt(
        unit=in_doubt,
        execution_generation=1,
        attempt=activity_attempt(2, workflow_id=f"operation/{in_doubt.semantic_operation_id}"),
        binding_id=f"binding:{in_doubt.unit_key}:1",
        binding_digest=BINDING,
        namespace=namespace_claim(in_doubt),
        dispatching=True,
        observed_at=LINEAGE_NOW,
        lease_expires_at=LINEAGE_NOW + timedelta(hours=2),
    )
    await lineage.open_incident(in_doubt_incident(in_doubt))
    await execute_command(
        run_service,
        active,
        "park-in-doubt",
        SetWaitAction(condition=operator_wait(in_doubt.unit_key), runnable_work_remains=False),
        scope=scope,
    )
    await execute_command(
        run_service,
        active,
        "reserve-effect",
        ReserveBudgetAction(reservation_id="reservation:effect", amounts={"tokens.total": 5}),
        scope=scope,
    )
    await execute_command(
        run_service,
        active,
        "claim-effect",
        ClaimEffectAction(
            effect_id="tool-effect:send",
            effect_kind="tool",
            operation_ref=f"binding:{in_doubt.unit_key}:1",
            provider_idempotency_key="send:1",
            reservation_id="reservation:effect",
        ),
        scope=scope,
    )
    await execute_command(
        run_service,
        active,
        "observe-effect",
        ObserveEffectAction(
            effect_id="tool-effect:send", observation_id="observation:1", disposition="ambiguous"
        ),
        scope=scope,
    )
    return SeededRuns(
        active_run=active,
        terminal_run=terminal,
        other_scope_run=other.run_id,
        settled=settled,
        in_doubt=in_doubt,
    )
