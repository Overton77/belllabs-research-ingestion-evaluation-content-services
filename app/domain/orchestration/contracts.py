from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal

StageStatus = Literal[
    "pending",
    "running",
    "waiting",
    "paused",
    "completed",
    "degraded",
    "skipped",
    "failed",
]


@dataclass(frozen=True)
class ExecutionIdentity:
    run_id: str
    execution_epoch: int = 1


@dataclass(frozen=True)
class StageExecutionIdentity:
    run_id: str
    stage_id: str
    workflow_cycle: int
    stage_cycle: int
    operation_attempt: int
    execution_epoch: int

    @property
    def semantic_key(self) -> str:
        return (
            f"{self.run_id}:execution-epoch:{self.execution_epoch}:"
            f"workflow-cycle:{self.workflow_cycle}:stage:{self.stage_id}:"
            f"stage-cycle:{self.stage_cycle}:operation-attempt:{self.operation_attempt}"
        )


@dataclass(frozen=True)
class StageOperationRequest:
    identity: StageExecutionIdentity
    idempotency_key: str
    objective: str
    input_refs: tuple[str, ...]
    reservation_id: str
    reservation: dict[str, int]
    workspace_namespace: str
    cycle_evaluation_contract_ref: str = ""
    cycle_objective_contract_ref: str = ""


@dataclass(frozen=True)
class StageOperationResult:
    identity: StageExecutionIdentity
    disposition: Literal["completed", "skipped", "failed", "waiting", "paused"]
    output_refs: tuple[str, ...] = ()
    evaluation: Literal["accept", "cycle", "degrade", "escalate"] = "accept"
    evaluation_ref: str = ""
    next_objective: str = ""
    evaluation_contract_ref: str = ""
    objective_contract_ref: str = ""
    wait_condition_id: str = ""
    pause_decision_id: str = ""
    handoff_ref: str = ""
    temporal_activity_attempt: int = 1
    actual_usage: dict[str, int] = field(default_factory=dict)
    pending_external_usage: dict[str, int] = field(default_factory=dict)


@dataclass(frozen=True)
class WorkflowEvaluationRequest:
    run_id: str
    workflow_cycle: int
    objective: str
    current_output_refs: dict[str, tuple[str, ...]]
    execution_lineage: tuple[StageOperationResult, ...]
    evaluation_contract_ref: str = ""
    objective_contract_ref: str = ""


@dataclass(frozen=True)
class WorkflowEvaluationResult:
    action: Literal["accept", "cycle", "fail"]
    evaluation_ref: str
    invalidation_frontier: tuple[str, ...] = ()
    next_objective: str = ""
    evaluation_contract_ref: str = ""
    objective_contract_ref: str = ""


@dataclass(frozen=True)
class LifecycleCommandRequest:
    command_id: str
    expected_run_version: int
    action: dict[str, Any]
    reason: str
    evidence_refs: tuple[str, ...] = ()
    occurred_at: datetime | None = None
    run_id: str = ""
    request_scope: str = ""
    effective_configuration_digest: str = ""
    idempotency_issuer: str = ""
    correlation_id: str = ""
    blueprint_digest: str = ""


@dataclass(frozen=True)
class LifecycleCommandOutcome:
    accepted: bool
    resulting_run_version: int
    phase: str
    reason_code: str
    evidence_frontier_digest: str = ""
    obligation_revision: str = ""
    accepted_obligation_evidence_digest: str = ""
    required_obligations_accepted: bool = False


@dataclass
class StageExecutionState:
    status: StageStatus = "pending"
    stage_cycle: int = 0
    operation_attempt: int = 0
    objective: str = "execute declared stage objective"
    output_refs: tuple[str, ...] = ()
    wait_condition_id: str = ""
    pause_decision_id: str = ""


@dataclass
class StageGraphExecutionState:
    identity: ExecutionIdentity
    workflow_cycle: int = 0
    run_version: int = 1
    stages: dict[str, StageExecutionState] = field(default_factory=dict)
    lineage: list[StageOperationResult] = field(default_factory=list)
    schedule_trace: list[str] = field(default_factory=list)
    fairness_cursor: dict[str, int] = field(default_factory=dict)
    workflow_objective: str = "satisfy the frozen StageGraph"


@dataclass(frozen=True)
class StageGraphRunInput:
    run_id: str
    request_scope: str
    effective_configuration_digest: str
    blueprint_digest: str
    blueprint: dict[str, Any]
    initial_run_version: int = 1
    execution_epoch: int = 1
    max_concurrency: int = 1
    task_timeout_seconds: int = 30
    orchestration_authority_ref: str = "orchestration-authority"
    lifecycle_idempotency_issuer: str = "stagegraph-worker"
    correlation_id: str = ""
    baseline_reservation: dict[str, int] = field(default_factory=dict)


@dataclass(frozen=True)
class StageGraphRunResult:
    run_id: str
    workflow_cycles: int
    execution_epoch: int
    stage_cycles: dict[str, int]
    operation_attempts: dict[str, int]
    output_refs: dict[str, tuple[str, ...]]
    reused_output_refs: dict[str, tuple[str, ...]]
    schedule_trace: tuple[str, ...]
    lineage: tuple[StageOperationResult, ...]
