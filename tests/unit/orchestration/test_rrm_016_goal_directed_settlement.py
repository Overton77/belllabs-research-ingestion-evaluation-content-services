"""RRM-016: GoalDirected operations compose with run-control authority and the journal.

Deterministic proofs (in-memory run control, journal and lineage; a real `create_deep_agent`
graph with a scripted model) of:

* preparation binds the revision its own admission produces, the goal-context segment is
  non-privileged, and the compiled workspace slots are bound under the role-scoped root;
* the real `RunControlOperationAuthority` admits both roles (first binding and
  continuation) and rejects a privileged goal-context, a foreign root and the pre-change
  revision;
* one operation is claimed, observed and settled once; the family consumes that settlement
  (`RunControlGoalOperationSettlements`) and its pre-change `record_usage` is now rejected;
* the usage accounting of the pre-change composition (journal absent, RRM-006's shape) and
  of the governed one, for the same operations.
"""

from __future__ import annotations

import json
from typing import Any

import pytest
from langgraph.checkpoint.memory import InMemorySaver

from mission_control.adapters.storage.artifact_payloads import InMemoryArtifactPayloadStore
from mission_control.application.execution.operations.checkpoint_lineage import (
    CheckpointLineageService,
    InMemoryCheckpointLineageRepository,
)
from mission_control.application.execution.operations.operation_execution import (
    InMemoryOperationBindingRepository,
    OperationExecutionService,
    bind_operation_execution_request,
    operation_settlement_id,
)
from mission_control.application.programs.goal_directed import (
    GoalDirectedOperationResultService,
    GoalOperationSettlementUnavailable,
)
from mission_control.domain.execution.contracts import (
    DeepAgentExecutionBinding,
    OperationExecutionRequest,
    OperationExecutionResult,
    OperationWorkflowResult,
    PromptTrustClass,
    WorkspaceOwnerKind,
)
from mission_control.domain.policies.contracts import (
    CommandStatus,
    EffectDisposition,
    ExecutionTarget,
    LifecycleCommand,
    RecordUsageAction,
    StartAction,
)
from mission_control.domain.programs.contracts import GoalExecutionResult
from mission_control.domain.programs.goal_directed import GoalDirectedInterpreter
from mission_control.domain.programs.goal_directed_runtime import (
    GoalOperationDispatch,
    GoalOperationPreparationRequest,
    GoalOperationReconciliationRequest,
    GoalOperationReconciliationResult,
)
from mission_control.domain.programs.runtime_units import goal_runtime_unit
from tests.fixtures.checkpoint_lineage import activity_attempt, bind_unit, stage_unit
from tests.fixtures.checkpoint_recovery import AcceptingAuthority, MemoryOperationJournal
from tests.fixtures.goal_directed_journaled import (
    SCOPE,
    TOKENS_PER_OPERATION,
    GoalComposition,
    GoalScriptedModel,
    admit_goal_run,
    compose_goal_directed,
    goal_authority,
    goal_blueprint,
    goal_run_control,
    goal_run_input,
    goal_start_action,
)
from tests.unit.run_control.test_run_control import actor, command

NOW_ISSUER = "goal-directed-worker"


async def _composition(*, journaled: bool = True) -> tuple[GoalComposition, str]:
    run_control = goal_run_control()
    composition = await compose_goal_directed(
        run_control=run_control,
        journal=MemoryOperationJournal(),
        lineage=CheckpointLineageService(InMemoryCheckpointLineageRepository()),
        results=InMemoryArtifactPayloadStore(),
        bindings=InMemoryOperationBindingRepository(),
        saver=InMemorySaver(),
        model=GoalScriptedModel(),
        blueprint=goal_blueprint(),
    )
    if not journaled:
        # The pre-change composition RRM-006 had to use: no journal, an accepting authority.
        prior = composition.service
        composition.service = OperationExecutionService(
            authority=AcceptingAuthority(),
            bindings=prior._bindings,
            runtime=prior._runtime,
            sandbox=prior._sandbox,
            assets=prior._assets,
            mcp=prior._mcp,
            secrets=prior._secrets,
            events=prior._events,
            budget=prior._budget,
            journal=None,
            lineage=prior._lineage,
        )
        # ... and a family that records each operation's usage itself (no settlement port).
        composition.family._results = GoalDirectedOperationResultService(composition.documents)
    run_id = await admit_goal_run(run_control, f"rrm-016-unit-{'journal' if journaled else 'none'}")
    started = await run_control.execute(
        command(run_id, 1, "rrm-016-start", goal_start_action(run_id))
    )
    assert started.status == CommandStatus.ACCEPTED
    return composition, run_id


def _preparation(
    run_id: str,
    claim: Any,
    role: str,
    run_version: int,
    family_version: int,
    executor: GoalExecutionResult | None = None,
) -> GoalOperationPreparationRequest:
    blueprint = goal_blueprint()
    run_input = goal_run_input(run_id, blueprint, baseline={})
    revision = run_input.initial_revision
    verifier = role == "verifier"
    return GoalOperationPreparationRequest(
        request_scope=SCOPE,
        run_id=run_id,
        effective_configuration_digest=run_input.effective_configuration_digest,
        semantic_input_binding_ref=run_input.semantic_input_binding_ref,
        goal_revision_id=revision.revision_id,
        goal_revision_digest=revision.canonical_digest,
        goal_revision=revision,
        goal_iteration=claim.identity.iteration.goal_iteration,
        operation_role=role,  # type: ignore[arg-type]
        operation_attempt=1,
        execution_generation=1,
        expected_run_version=run_version,
        expected_family_version=family_version,
        reservation_id=(f"{claim.reservation_id}:verifier" if verifier else claim.reservation_id),
        reservation=claim.reservation,
        session_id=(f"{claim.session_id}:verifier" if verifier else claim.session_id),
        workspace_id=(
            f"{claim.workspace_namespace}:verifier" if verifier else claim.workspace_namespace
        ),
        read_workspace_id=executor.workspace_id if executor is not None else None,
        verifier_input_refs=executor.output_refs if executor is not None else (),
        decided_at=_now(),
        execution_epoch=claim.identity.iteration.execution_epoch,
        agent_run=claim.identity.agent_run,
        session_generation=claim.identity.session_generation,
    )


def _now() -> Any:
    from tests.unit.run_control.test_run_control import NOW

    return NOW


def _reconciliation(
    claim: Any,
    role: str,
    dispatch: GoalOperationDispatch,
    result: OperationExecutionResult,
    executor: GoalExecutionResult | None = None,
) -> GoalOperationReconciliationRequest:
    blueprint = goal_blueprint()
    verifier = role == "verifier"
    return GoalOperationReconciliationRequest(
        request_scope=SCOPE,
        goal_revision_id=claim.identity.iteration.goal_revision_id,
        operation_role=role,  # type: ignore[arg-type]
        operation_binding_ref=dispatch.operation_binding_ref,
        required_output_contract_refs=tuple(sorted(blueprint.required_output_contracts)),
        operation_request=dispatch.workflow_request,
        claim=claim,
        executor_result=executor,
        operation_result=OperationWorkflowResult(
            semantic_attempt_id=dispatch.workflow_request.semantic_attempt_id,
            execution_generation=1,
            disposition="completed",
            result=result.model_dump(mode="json"),
            message_cursor=0,
        ),
        remaining_iterations=1,
        protected_fact_classes=(
            () if verifier else tuple(sorted(blueprint.session_policy.protected_fact_classes))
        ),
        context_selection_policy_ref=(
            None if verifier else blueprint.session_policy.context_selection_policy_ref
        ),
        context_compaction_policy_ref=(
            None if verifier else blueprint.session_policy.context_compaction_policy_ref
        ),
        workspace_ref_class=(
            None if verifier else sorted(blueprint.handoff_policy.allowed_workspace_ref_classes)[0]
        ),
        compaction_failure_action=(
            None if verifier else blueprint.session_policy.compaction_failure_action
        ),
        verifier_policy_binding_ref=(blueprint.verifier_policy.binding_ref if verifier else None),
        verifier_rubric_ref=blueprint.verifier_policy.rubric_ref if verifier else None,
        verifier_rubric_version=(blueprint.verifier_policy.rubric_version if verifier else None),
        acceptance_contract_ref=blueprint.acceptance_contract if verifier else None,
        acceptance_version=(blueprint.verifier_policy.acceptance_version if verifier else None),
        recorded_at=_now(),
    )


async def _claim(run_id: str) -> Any:
    blueprint = goal_blueprint()
    interpreter = GoalDirectedInterpreter(blueprint)
    state = interpreter.initial_state(goal_run_input(run_id, blueprint, baseline={}))
    _claimed, claim = interpreter.claim_execution(state)
    return claim


async def _run_iteration_one(composition: GoalComposition, run_id: str) -> dict[str, Any]:
    """Executor then verifier of iteration 1, each reconciled by the family service."""

    claim = await _claim(run_id)
    run = await composition.run_control.get_run(SCOPE, run_id)
    observed: dict[str, Any] = {"claim": claim, "versions": [run.version]}
    executor_dispatch = await composition.family.execute_iteration(
        _preparation(run_id, claim, "executor", run.version, 0)
    )
    executor_result = await composition.service.execute(
        executor_dispatch.workflow_request.operation,
        activity_attempt(workflow_id=executor_dispatch.workflow_request.workflow_id),
    )
    assert executor_result.status == "completed", executor_result
    executor = await composition.family.prepare_handoff(
        _reconciliation(claim, "executor", executor_dispatch, executor_result)
    )
    assert executor.execution_result is not None
    observed.update(
        executor_dispatch=executor_dispatch,
        executor_result=executor_result,
        executor=executor,
    )
    run = await composition.run_control.get_run(SCOPE, run_id)
    next_version = (
        executor.settlement.settled_run_version if executor.settlement is not None else run.version
    )
    verifier_dispatch = await composition.family.verify_iteration(
        _preparation(
            run_id,
            claim,
            "verifier",
            next_version,
            executor_dispatch.resulting_family_version,
            executor.execution_result,
        )
    )
    verifier_result = await composition.service.execute(
        verifier_dispatch.workflow_request.operation,
        activity_attempt(workflow_id=verifier_dispatch.workflow_request.workflow_id),
    )
    assert verifier_result.status == "completed", verifier_result
    verifier = await composition.family.prepare_handoff(
        _reconciliation(
            claim, "verifier", verifier_dispatch, verifier_result, executor.execution_result
        )
    )
    observed.update(
        verifier_dispatch=verifier_dispatch,
        verifier_result=verifier_result,
        verifier=verifier,
    )
    return observed


@pytest.mark.asyncio
async def test_preparation_binds_the_admission_revision_role_slots_and_untrusted_context() -> None:
    composition, run_id = await _composition()
    claim = await _claim(run_id)
    executor = await composition.family.execute_iteration(
        _preparation(run_id, claim, "executor", 2, 0)
    )
    operation = executor.workflow_request.operation

    # REQ-CP-EXEC-014: the binding carries the revision its own admission produced.
    assert executor.resulting_run_version == 3
    assert operation.run_control_revision == 3
    assert operation.deep_agent_binding is not None
    assert operation.deep_agent_binding.control_revision == 3
    assert (await composition.run_control.get_run(SCOPE, run_id)).version == 3

    # The goal context is non-privileged run state; the only privileged segment is the
    # configured system prompt.
    segments = {item.source_ref: item.trust_class for item in operation.prompt_segments}
    assert segments[f"goal-context:{run_id}:1:executor"] == PromptTrustClass.UNTRUSTED_CONTENT
    assert {
        ref
        for ref, trust in segments.items()
        if trust in {PromptTrustClass.SYSTEM_AUTHORITY, PromptTrustClass.AUTHORED_INSTRUCTION}
    } == {"prompt:system@1"}

    # REQ-CP-DA-013: exact compiled slots under the role-scoped root, owned by the iteration.
    assert operation.workspace.exclusive_write_paths == ("/goal/1/executor/work",)
    [slot] = operation.workspace.slot_bindings
    assert (slot.slot_name, slot.logical_path, slot.owner.kind) == (
        "work",
        "/goal/1/executor/work",
        WorkspaceOwnerKind.ITERATION,
    )


@pytest.mark.asyncio
async def test_real_authority_admits_goal_operations_and_rejects_privileged_or_foreign_shapes() -> (
    None
):
    composition, run_id = await _composition()
    claim = await _claim(run_id)
    dispatch = await composition.family.execute_iteration(
        _preparation(run_id, claim, "executor", 2, 0)
    )
    operation = dispatch.workflow_request.operation
    authority = goal_authority(composition.run_control, composition.templates["executor"])

    # First binding (exact revision) and continuation of the prepared binding both admit.
    await authority.verify(operation)
    await authority.verify_continuation(operation, bind_operation_execution_request(operation))

    # A privileged goal-context is a privileged prompt bypass: rejected.
    privileged = operation.model_copy(
        update={
            "prompt_segments": tuple(
                item.model_copy(update={"trust_class": PromptTrustClass.AUTHORED_INSTRUCTION})
                if item.source_ref.startswith("goal-context:")
                else item
                for item in operation.prompt_segments
            )
        }
    )
    with pytest.raises(ValueError, match="privileged prompt segments"):
        await authority.verify(privileged)

    # The executor's slots under the verifier root (another unit's root) are foreign.
    foreign_slot = operation.workspace.slot_bindings[0].model_copy(
        update={"logical_path": "/goal/1/verifier/work"}
    )
    foreign = operation.model_copy(
        update={
            "workspace": operation.workspace.model_copy(
                update={
                    "slot_bindings": (foreign_slot,),
                    "exclusive_write_paths": ("/goal/1/verifier/work",),
                }
            )
        }
    )
    with pytest.raises(ValueError, match="workspace slots do not exactly match"):
        await authority.verify(foreign)

    # The pre-change binding revision (the version before its own admission) is stale.
    stale = operation.model_copy(update={"run_control_revision": 2})
    with pytest.raises(ValueError, match="not bound to the accepted Run Control revision"):
        await authority.verify(stale)


@pytest.mark.asyncio
async def test_operations_settle_once_and_the_family_consumes_the_settlement() -> None:
    composition, run_id = await _composition()
    observed = await _run_iteration_one(composition, run_id)
    run_control = composition.run_control
    budget = await run_control.get_budget(SCOPE, run_id)
    effects = await run_control.get_effects(SCOPE, run_id)
    run = await run_control.get_run(SCOPE, run_id)

    settlements = [observed["executor"].settlement, observed["verifier"].settlement]
    assert all(item is not None for item in settlements)
    executor_settlement, verifier_settlement = settlements
    executor_binding = observed["executor_dispatch"].operation_binding_ref
    verifier_binding = observed["verifier_dispatch"].operation_binding_ref
    assert executor_settlement.settlement_id == operation_settlement_id(executor_binding)
    assert verifier_settlement.settlement_id == operation_settlement_id(verifier_binding)
    assert executor_settlement.usage == {"tokens.total": TOKENS_PER_OPERATION}
    # Each settlement advanced the run version past its operation's bound revision by the
    # journal's three facts (claim, observation, authority settlement).
    assert executor_settlement.settled_run_version == (
        observed["executor_dispatch"].resulting_run_version + 3
    )
    assert verifier_settlement.settled_run_version == run.version

    # REQ-CP-RUN-006/009: exactly one usage record per operation, by its own settlement.
    operation_usage = {
        key: (item.authority_ref, item.reservation_id, item.actual_amounts)
        for key, item in budget.usage_records.items()
    }
    claim = observed["claim"]
    assert operation_usage == {
        executor_settlement.settlement_id: (
            executor_binding,
            claim.reservation_id,
            {"tokens.total": TOKENS_PER_OPERATION},
        ),
        verifier_settlement.settlement_id: (
            verifier_binding,
            f"{claim.reservation_id}:verifier",
            {"tokens.total": TOKENS_PER_OPERATION},
        ),
    }
    assert budget.consumed.get("tokens.total") == 2 * TOKENS_PER_OPERATION
    assert claim.reservation_id not in budget.reservations
    # REQ-CP-RUN-007: both effects claimed, observed and settled; evidence accepted.
    assert sorted(item.operation_ref for item in effects.claims.values()) == sorted(
        [executor_binding, verifier_binding]
    )
    assert {item.disposition for item in effects.claims.values()} == {EffectDisposition.SUCCEEDED}
    assert {
        (item.settlement_id, item.accepted_by_authority_ref)
        for item in run.accepted_operation_settlement_evidence
    } == {
        (executor_settlement.settlement_id, executor_binding),
        (verifier_settlement.settlement_id, verifier_binding),
    }

    # The family's pre-change usage command would be a second record: it is now rejected.
    second = await run_control.execute(
        LifecycleCommand(
            command_id=f"goal:usage:{claim.reservation_id}",
            idempotency_issuer=NOW_ISSUER,
            request_scope=SCOPE,
            run_id=run_id,
            expected_run_version=run.version,
            actor=actor(),
            action=RecordUsageAction(
                usage_id=f"goal-usage:{claim.reservation_id}",
                actual_amounts={"tokens.total": TOKENS_PER_OPERATION},
                reservation_id=claim.reservation_id,
            ),
            reason="pre-RRM-016 family usage recording",
            occurred_at=_now(),
            correlation_id="rrm-016",
        )
    )
    assert (second.status, second.reason_code) == (
        CommandStatus.REJECTED,
        "reservation_required",
    )
    assert (await run_control.get_budget(SCOPE, run_id)).consumed.get("tokens.total") == (
        2 * TOKENS_PER_OPERATION
    )

    # Reconciliation is a read of authority: repeating it consumes the same settlement.
    again = await composition.family.prepare_handoff(
        _reconciliation(
            claim,
            "executor",
            observed["executor_dispatch"],
            observed["executor_result"],
        )
    )
    assert again.settlement is not None
    assert again.settlement.settlement_id == executor_settlement.settlement_id
    assert again.settlement.usage == executor_settlement.usage

    # The goal context reached the model only as user content, never the system prompt.
    assert not any(item["system_has_goal_context"] for item in composition.model.turns)


@pytest.mark.asyncio
async def test_settlement_consumption_fails_closed() -> None:
    composition, run_id = await _composition()
    claim = await _claim(run_id)
    run = await composition.run_control.get_run(SCOPE, run_id)
    dispatch = await composition.family.execute_iteration(
        _preparation(run_id, claim, "executor", run.version, 0)
    )
    binding = bind_operation_execution_request(dispatch.workflow_request.operation)
    unsettled = OperationExecutionResult(
        binding_id=binding.binding_id,
        semantic_attempt_key=binding.semantic_attempt_key,
        status="completed",
        output_text="{}",
        structured_output={
            "schema_version": "belllabs.goal-executor-observation.v1",
            "disposition": "completed",
            "output_refs": ["artifact:rrm016:1"],
            "completion_claim": False,
            "evidence_refs": ["evidence:executor:1"],
            "output_contract_ref": "fixture-output",
        },
    )
    # No operation ran: there is no run-control settlement to consume.
    with pytest.raises(GoalOperationSettlementUnavailable, match="usage is not recorded"):
        await composition.family.prepare_handoff(
            _reconciliation(claim, "executor", dispatch, unsettled)
        )
    assert composition.documents.iterations == []

    settled = await composition.service.execute(
        dispatch.workflow_request.operation,
        activity_attempt(workflow_id=dispatch.workflow_request.workflow_id),
    )
    # A result whose usage differs from the settled usage is not the settled operation.
    drifted = settled.model_copy(
        update={"usage": settled.usage.model_copy(update={"amounts": {"tokens.total": 1}})}
    )
    with pytest.raises(GoalOperationSettlementUnavailable, match="differs"):
        await composition.family.prepare_handoff(
            _reconciliation(claim, "executor", dispatch, drifted)
        )
    reconciled = await composition.family.prepare_handoff(
        _reconciliation(claim, "executor", dispatch, settled)
    )
    assert isinstance(reconciled, GoalOperationReconciliationResult)
    assert reconciled.settlement is not None


@pytest.mark.asyncio
async def test_usage_accounting_before_and_after_the_governed_composition() -> None:
    """The same iteration under the pre-change composition (no journal; the family records
    usage itself) and under the journaled composition."""

    records: dict[str, Any] = {}
    for label, journaled in (("before", False), ("after", True)):
        composition, run_id = await _composition(journaled=journaled)
        observed = await _run_iteration_one(composition, run_id)
        run_control = composition.run_control
        if not journaled:
            # The pre-change family records each operation's usage (`goal:usage:{id}`).
            claim = observed["claim"]
            for reservation_id in (claim.reservation_id, f"{claim.reservation_id}:verifier"):
                run = await run_control.get_run(SCOPE, run_id)
                recorded = await run_control.execute(
                    LifecycleCommand(
                        command_id=f"goal:usage:{reservation_id}",
                        idempotency_issuer=NOW_ISSUER,
                        request_scope=SCOPE,
                        run_id=run_id,
                        expected_run_version=run.version,
                        actor=actor(),
                        action=RecordUsageAction(
                            usage_id=f"goal-usage:{reservation_id}",
                            actual_amounts={"tokens.total": TOKENS_PER_OPERATION},
                            reservation_id=reservation_id,
                            release_amounts={"goal.iterations": 1, "tokens.total": 5},
                        ),
                        reason="pre-RRM-016 family usage recording",
                        occurred_at=_now(),
                        correlation_id="rrm-016",
                    )
                )
                assert recorded.status == CommandStatus.ACCEPTED
        budget = await run_control.get_budget(SCOPE, run_id)
        effects = await run_control.get_effects(SCOPE, run_id)
        run = await run_control.get_run(SCOPE, run_id)
        records[label] = {
            "usage_ids": sorted(
                "operation-settlement" if item.authority_ref else item.usage_id.split(":")[0]
                for item in budget.usage_records.values()
            ),
            "consumed_tokens": budget.consumed.get("tokens.total", 0),
            "usage_records_with_operation_authority": sum(
                item.authority_ref is not None for item in budget.usage_records.values()
            ),
            "effect_claims": len(effects.claims),
            "settled_effect_claims": sum(
                item.settlement is not None for item in effects.claims.values()
            ),
            "accepted_operation_settlements": len(run.accepted_operation_settlement_evidence),
            "family_settlements_consumed": sum(
                observed[role].settlement is not None for role in ("executor", "verifier")
            ),
        }
    print("RRM-016 EVIDENCE accounting " + json.dumps(records, sort_keys=True))

    before, after = records["before"], records["after"]
    # The same usage is consumed once in both compositions ...
    assert before["consumed_tokens"] == after["consumed_tokens"] == 2 * TOKENS_PER_OPERATION
    # ... but only the governed one claims, settles and accepts each operation in run
    # control (RRM-006 saw the former as `not_accepted`), with usage bound to the binding.
    assert before == {
        "usage_ids": ["goal-usage", "goal-usage"],
        "consumed_tokens": 20,
        "usage_records_with_operation_authority": 0,
        "effect_claims": 0,
        "settled_effect_claims": 0,
        "accepted_operation_settlements": 0,
        "family_settlements_consumed": 0,
    }
    assert after == {
        "usage_ids": ["operation-settlement", "operation-settlement"],
        "consumed_tokens": 20,
        "usage_records_with_operation_authority": 2,
        "effect_claims": 2,
        "settled_effect_claims": 2,
        "accepted_operation_settlements": 2,
        "family_settlements_consumed": 2,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("target", ["StageGraph", None])
async def test_authority_rejects_a_goal_unit_on_a_run_that_is_not_goal_directed(
    target: str | None,
) -> None:
    """Review fix 1: a GoalDirected unit is admitted only on a GoalDirected run."""

    composition, _goal_run = await _composition()
    run_control = composition.run_control
    run_id = await admit_goal_run(run_control, f"rrm-016-foreign-family-{target}")
    start = (
        StartAction(
            execution_target=ExecutionTarget(family="StageGraph", family_workflow_id="family/x/1")
        )
        if target == "StageGraph"
        else StartAction()
    )
    assert (
        await run_control.execute(command(run_id, 1, "start", start))
    ).status == CommandStatus.ACCEPTED
    claim = await _claim(run_id)
    dispatch = await composition.family.execute_iteration(
        _preparation(run_id, claim, "executor", 2, 0)
    )
    authority = goal_authority(run_control, composition.templates["executor"])
    with pytest.raises(ValueError, match="requires a GoalDirected Workflow Run"):
        await authority.verify(dispatch.workflow_request.operation)


@pytest.mark.asyncio
@pytest.mark.parametrize(("iteration", "role"), [(2, "executor"), (1, "verifier")])
async def test_authority_rejects_a_goal_unit_whose_location_is_not_its_operation(
    iteration: int, role: str
) -> None:
    """Review fix 1: the unit location (iteration, role) must name the operation; otherwise
    the workspace root it implies belongs to another unit."""

    composition, run_id = await _composition()
    claim = await _claim(run_id)
    dispatch = await composition.family.execute_iteration(
        _preparation(run_id, claim, "executor", 2, 0)
    )
    operation = dispatch.workflow_request.operation
    assert operation.runtime_unit is not None and operation.deep_agent_binding is not None
    location = operation.runtime_unit.location
    foreign_unit = goal_runtime_unit(
        request_scope=SCOPE,
        run_id=run_id,
        execution_epoch=1,
        operation_id=operation.identity.operation_id,  # still goal-iteration/1/executor
        operation_attempt=1,
        goal_iteration=iteration,
        goal_revision_id=location.goal_revision_id,  # type: ignore[union-attr]
        operation_role=role,  # type: ignore[arg-type]
        agent_run=location.agent_run,  # type: ignore[union-attr]
        session_generation=location.session_generation,  # type: ignore[union-attr]
    )
    foreign = OperationExecutionRequest.model_validate(
        {
            **operation.model_dump(mode="python"),
            "runtime_unit": foreign_unit,
            "deep_agent_binding": bind_unit(operation.deep_agent_binding, foreign_unit),
        }
    )
    authority = goal_authority(composition.run_control, composition.templates["executor"])
    await authority.verify(operation)
    with pytest.raises(ValueError, match="location does not match its operation"):
        await authority.verify(foreign)


@pytest.mark.asyncio
@pytest.mark.parametrize("unit", ["none", "stage_graph"])
async def test_goal_directed_run_rejects_an_operation_without_a_goal_unit(unit: str) -> None:
    """Re-check (a): on a GoalDirected run every operation carries a GoalDirected unit, so
    its slots are always rebased under its role root and executor and verifier stay
    disjoint. An operation with no unit, or with another family's unit, is rejected."""

    composition, run_id = await _composition()
    claim = await _claim(run_id)
    dispatch = await composition.family.execute_iteration(
        _preparation(run_id, claim, "executor", 2, 0)
    )
    operation = dispatch.workflow_request.operation
    assert operation.deep_agent_binding is not None
    if unit == "none":
        replacement = None
        deep_binding = DeepAgentExecutionBinding.create(
            **{
                **operation.deep_agent_binding.model_dump(
                    mode="python", exclude={"binding_digest"}
                ),
                "runtime_unit": None,
                "cognitive_session_namespace": None,
            }
        )
    else:
        replacement = stage_unit(
            request_scope=SCOPE,
            run_id=run_id,
            operation_id=operation.identity.operation_id,
        )
        deep_binding = bind_unit(operation.deep_agent_binding, replacement)
    unitless = OperationExecutionRequest.model_validate(
        {
            **operation.model_dump(mode="python"),
            "runtime_unit": replacement,
            "deep_agent_binding": deep_binding,
        }
    )
    authority = goal_authority(composition.run_control, composition.templates["executor"])
    with pytest.raises(ValueError, match="admits only GoalDirected runtime units"):
        await authority.verify(unitless)
