from __future__ import annotations

from typing import Final, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from app.domain.control_plane.canonical import sha256_digest

IDENTIFIER_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,511}$"
DIGEST_PATTERN = r"^sha256:[0-9a-f]{64}$"


class Identity(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class BellLabsRunKey(Identity):
    request_scope: str = Field(min_length=1, max_length=256)
    belllabs_run_id: str = Field(pattern=IDENTIFIER_PATTERN)


class ExecutionEpochKey(BellLabsRunKey):
    execution_epoch: int = Field(ge=1)

    @property
    def canonical_key(self) -> str:
        return (
            f"belllabs:{self.request_scope}:run:{self.belllabs_run_id}:"
            f"execution-epoch:{self.execution_epoch}"
        )


class GraphIdentity(Identity):
    graph_family: Literal["StageGraph", "GoalDirected", "deep_agent", "operation"]
    graph_id: str = Field(pattern=IDENTIFIER_PATTERN)
    graph_assembly_digest: str = Field(pattern=DIGEST_PATTERN)


class DeploymentIdentity(Identity):
    runtime_provider: Literal["langgraph_agent_server"] = "langgraph_agent_server"
    assistant_id: str = Field(pattern=IDENTIFIER_PATTERN)
    deployment_id: str = Field(pattern=IDENTIFIER_PATTERN)
    deployment_revision: str = Field(pattern=IDENTIFIER_PATTERN)
    deployment_endpoint_id: str = Field(pattern=IDENTIFIER_PATTERN)


class AgentThreadKey(ExecutionEpochKey):
    runtime_provider: Literal["langgraph"] = "langgraph"
    agent_server_thread_id: str = Field(pattern=IDENTIFIER_PATTERN)
    relationship: Literal["parent", "fork", "linked_run", "async_subagent"]
    parent_belllabs_run_id: str | None = Field(default=None, pattern=IDENTIFIER_PATTERN)

    @model_validator(mode="after")
    def child_relationship_requires_parent(self) -> AgentThreadKey:
        child = self.relationship in {"fork", "linked_run", "async_subagent"}
        if child != (self.parent_belllabs_run_id is not None):
            raise ValueError("child threads require one qualified parent BellLabs run")
        if self.parent_belllabs_run_id == self.belllabs_run_id:
            raise ValueError("child thread parent and child BellLabs runs must differ")
        return self


class AgentRunKey(Identity):
    runtime_provider: Literal["langgraph"] = "langgraph"
    deployment_endpoint_id: str = Field(pattern=IDENTIFIER_PATTERN)
    agent_server_run_id: str = Field(pattern=IDENTIFIER_PATTERN)


class LangGraphCheckpointKey(Identity):
    """Agent Server-shaped checkpoint key; versioned by `QualifiedCheckpointKey` (AMD-RRM-001)."""

    runtime_provider: Literal["langgraph"] = "langgraph"
    deployment_endpoint_id: str = Field(pattern=IDENTIFIER_PATTERN)
    agent_server_thread_id: str = Field(pattern=IDENTIFIER_PATTERN)
    langgraph_checkpoint_id: str = Field(pattern=IDENTIFIER_PATTERN)


class GoalHandoffCheckpointKey(BellLabsRunKey):
    goal_handoff_checkpoint_id: str = Field(pattern=IDENTIFIER_PATTERN)
    goal_iteration: int = Field(ge=1)


class SemanticOperationAttemptKey(BellLabsRunKey):
    """Delimiter-keyed attempt identity; versioned by `RuntimeUnitIdentity` (REQ-CP-EXEC-013)."""

    operation_id: str = Field(pattern=IDENTIFIER_PATTERN)
    semantic_attempt: int = Field(ge=1)
    stage_id: str | None = Field(default=None, pattern=IDENTIFIER_PATTERN)
    stage_cycle: int | None = Field(default=None, ge=0)
    goal_iteration: int | None = Field(default=None, ge=1)

    @model_validator(mode="after")
    def semantic_location_is_unambiguous(self) -> SemanticOperationAttemptKey:
        stage = self.stage_id is not None or self.stage_cycle is not None
        goal = self.goal_iteration is not None
        if stage and goal:
            raise ValueError("semantic attempts cannot be both stage and goal attempts")
        if (self.stage_id is None) != (self.stage_cycle is None):
            raise ValueError("stage attempts require both stage_id and stage_cycle")
        return self

    @property
    def canonical_key(self) -> str:
        location = ""
        if self.stage_id is not None:
            location = f":stage:{self.stage_id}:cycle:{self.stage_cycle}"
        elif self.goal_iteration is not None:
            location = f":goal-iteration:{self.goal_iteration}"
        return (
            f"belllabs:{self.request_scope}:run:{self.belllabs_run_id}{location}:"
            f"operation:{self.operation_id}:semantic-attempt:{self.semantic_attempt}"
        )


class RuntimeTransportAttemptKey(ExecutionEpochKey):
    runtime_attempt: int = Field(ge=1)
    submission_id: str = Field(pattern=IDENTIFIER_PATTERN)


class SubagentProfileKey(Identity):
    provider: Literal["deepagents"] = "deepagents"
    profile_id: str = Field(pattern=IDENTIFIER_PATTERN)
    profile_revision: int = Field(ge=1)
    profile_digest: str = Field(pattern=DIGEST_PATTERN)


class LinkedBellLabsRunKey(Identity):
    request_scope: str = Field(min_length=1, max_length=256)
    parent_belllabs_run_id: str = Field(pattern=IDENTIFIER_PATTERN)
    child_belllabs_run_id: str = Field(pattern=IDENTIFIER_PATTERN)
    linked_run_slot_id: str = Field(pattern=IDENTIFIER_PATTERN)

    @model_validator(mode="after")
    def parent_and_child_differ(self) -> LinkedBellLabsRunKey:
        if self.parent_belllabs_run_id == self.child_belllabs_run_id:
            raise ValueError("linked BellLabs runs require distinct parent and child identities")
        return self


RUNTIME_UNIT_SCHEMA_VERSION: Final = "belllabs.runtime-unit.v1"
UNIT_KEY_PREFIX = "bl-unit-v1:"
UNIT_KEY_PATTERN = r"^bl-unit-v1:[0-9a-f]{64}$"
NO_MAPPED_INSTANCE = "NO_MAPPED_INSTANCE"
QUALIFIED_CHECKPOINT_KEY_SCHEMA_VERSION: Final = "belllabs.qualified-checkpoint-key.v1"


class StageGraphUnitLocation(Identity):
    """StageGraph semantic location of one runtime unit (`CON-CP-RUNTIME-UNIT-V1`)."""

    stage_id: str = Field(min_length=1, max_length=512)
    mapped_instance_id: str = Field(min_length=1, max_length=512)
    workflow_cycle_ordinal: int = Field(ge=0)
    stage_cycle_ordinal: int = Field(ge=0)
    operation_slot_id: str = Field(min_length=1, max_length=512)


class GoalDirectedUnitLocation(Identity):
    """GoalDirected semantic location of one runtime unit (`CON-CP-RUNTIME-UNIT-V1`)."""

    goal_iteration: int = Field(ge=1)
    goal_revision_id: str = Field(min_length=1, max_length=512)
    operation_role: Literal["executor", "verifier"]
    agent_run: int = Field(ge=1)
    session_generation: int = Field(ge=1)


class RuntimeUnitIdentity(Identity):
    """Structured identity of one semantic operation attempt (REQ-CP-EXEC-013).

    Temporal workflow, run, and activity IDs, Activity attempts, worker identity,
    timestamps, and the execution generation are deliberately absent: technical retries,
    worker restarts, and Continue-As-New must yield a byte-identical `unit_key`.
    """

    schema_version: Literal["belllabs.runtime-unit.v1"] = RUNTIME_UNIT_SCHEMA_VERSION
    request_scope: str = Field(min_length=1, max_length=256)
    belllabs_run_id: str = Field(min_length=1, max_length=512)
    execution_epoch: int = Field(ge=1)
    family: Literal["stage_graph", "goal_directed"]
    unit_kind: Literal["stage_operation", "goal_executor", "goal_verifier"]
    semantic_operation_id: str = Field(min_length=1, max_length=1024)
    semantic_attempt: int = Field(ge=1)
    location: StageGraphUnitLocation | GoalDirectedUnitLocation

    @model_validator(mode="after")
    def location_matches_family(self) -> RuntimeUnitIdentity:
        if self.family == "stage_graph":
            if not isinstance(self.location, StageGraphUnitLocation):
                raise ValueError("stage_graph runtime units require a StageGraph location")
            if self.unit_kind != "stage_operation":
                raise ValueError("stage_graph runtime units are stage_operation units")
        else:
            if not isinstance(self.location, GoalDirectedUnitLocation):
                raise ValueError("goal_directed runtime units require a GoalDirected location")
            expected = f"goal_{self.location.operation_role}"
            if self.unit_kind != expected:
                raise ValueError("goal_directed unit kind must match its operation role")
        return self

    @property
    def unit_key(self) -> str:
        return UNIT_KEY_PREFIX + sha256_digest(self).removeprefix("sha256:")


class QualifiedCheckpointKey(Identity):
    """The only form in which BellLabs records refer to a LangGraph checkpoint (REQ-CP-DA-016).

    Successor of `LangGraphCheckpointKey`: it names the registered checkpointer by digest,
    the thread (the cognitive session namespace), LangGraph's `checkpoint_ns`, the
    checkpoint, and its parent. It never carries checkpoint bodies or transcripts.
    """

    schema_version: Literal["belllabs.qualified-checkpoint-key.v1"] = (
        QUALIFIED_CHECKPOINT_KEY_SCHEMA_VERSION
    )
    checkpointer_ref_digest: str = Field(pattern=DIGEST_PATTERN)
    thread_id: str = Field(min_length=1, max_length=1024)
    checkpoint_ns: str = Field(default="", max_length=1024)
    checkpoint_id: str = Field(min_length=1, max_length=256)
    parent_checkpoint_id: str | None = Field(default=None, min_length=1, max_length=256)

    @property
    def is_root(self) -> bool:
        return self.checkpoint_ns == ""
