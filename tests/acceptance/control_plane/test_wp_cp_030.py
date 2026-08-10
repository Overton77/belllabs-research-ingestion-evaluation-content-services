from __future__ import annotations

import inspect

import pytest

from app.domain.operation_execution.contracts import OperationWorkflowRequest
from app.domain.orchestration.contracts import (
    RunContinuityState,
    SemanticForkRequest,
    WorkflowMessage,
    create_semantic_fork,
)
from app.temporal.registration.task_queues import BellLabsTaskQueues
from app.temporal.registration.workflows import registered_workflows
from app.temporal.workflows.belllabs_run import BellLabsRunWorkflow
from app.temporal.workflows.operation import OperationWorkflow

DIGEST = "sha256:" + "a" * 64


def test_ordered_message_receipts_reject_gaps_duplicates_and_late_generations() -> None:
    root = BellLabsRunWorkflow()
    accepted = root._accept_message(  # noqa: SLF001 - deterministic contract qualification
        WorkflowMessage("message-1", 1, "fact", "fact:1")
    )
    duplicate = root._accept_message(  # noqa: SLF001
        WorkflowMessage("message-1", 1, "fact", "fact:1")
    )
    gap = root._accept_message(  # noqa: SLF001
        WorkflowMessage("message-3", 3, "fact", "fact:3")
    )
    stale = root._accept_message(  # noqa: SLF001
        WorkflowMessage("message-2-old", 2, "fact", "fact:2", execution_generation=2)
    )

    assert accepted.status == "accepted"
    assert duplicate.status == "duplicate"
    assert gap.status == "gap"
    assert stale.status == "stale_generation"
    assert root.continuity().last_message_sequence == 1


def test_continue_as_new_advances_only_technical_segment_and_preserves_semantics() -> None:
    continuity = RunContinuityState(
        execution_epoch=4,
        technical_segment=7,
        execution_generation=2,
        family_workflow_id="family/run-a/4",
        active_operation_ids=("operation/attempt-a",),
        pending_message_ids=("message-2",),
        last_message_sequence=1,
        reservation_balances={"tokens": 42},
        linked_run_ids=("run-child",),
    )
    continued = continuity.next_technical_segment()

    assert continued.technical_segment == 8
    assert continued.execution_epoch == 4
    assert continued.execution_generation == 2
    assert continued.active_operation_ids == continuity.active_operation_ids
    assert continued.pending_message_ids == continuity.pending_message_ids
    assert continued.reservation_balances == continuity.reservation_balances
    assert continued.linked_run_ids == continuity.linked_run_ids


def test_semantic_fork_starts_epoch_one_without_live_children_or_messages() -> None:
    fork = create_semantic_fork(
        SemanticForkRequest(
            source_run_id="run-source",
            new_run_id="run-fork",
            request_scope="tenant-a",
            snapshot_ref="snapshot:accepted:1",
            effective_configuration_digest=DIGEST,
        )
    )
    assert fork.execution_epoch == 1
    assert fork.technical_segment == 1
    assert fork.active_operation_ids == ()
    assert fork.pending_message_ids == ()


def test_operation_identity_and_single_registries_are_versioned_and_complete() -> None:
    with pytest.raises(ValueError, match="operation_kind"):
        OperationWorkflowRequest.model_validate(
            {
                "semantic_attempt_id": "run-a:stage:collect:attempt:1",
                "operation_kind": "stage_operation",
                "payload": {"objective": "collect"},
                "task_queue": "belllabs-agent-cognitive",
            }
        )

    workflow_types = tuple(registered_workflows())
    assert BellLabsRunWorkflow in workflow_types
    assert OperationWorkflow in workflow_types
    assert len(workflow_types) == len(set(workflow_types))
    assert 'name="belllabs.operation.v2"' in inspect.getsource(OperationWorkflow)

    queues = BellLabsTaskQueues.from_base("belllabs")
    assert len(set(queues.__dict__.values())) == 5


def test_workflow_owners_have_no_forbidden_nondeterministic_io_imports() -> None:
    sources = "\n".join(
        (
            inspect.getsource(BellLabsRunWorkflow),
            inspect.getsource(OperationWorkflow),
        )
    )
    for forbidden in (
        "asyncpg",
        "pymongo",
        "requests.",
        "httpx.",
        "boto3",
        "openai.",
        "socket.",
        "datetime.now",
    ):
        assert forbidden not in sources
