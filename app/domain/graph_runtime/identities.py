from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

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
    runtime_provider: Literal["langgraph"] = "langgraph"
    deployment_endpoint_id: str = Field(pattern=IDENTIFIER_PATTERN)
    agent_server_thread_id: str = Field(pattern=IDENTIFIER_PATTERN)
    langgraph_checkpoint_id: str = Field(pattern=IDENTIFIER_PATTERN)


class GoalHandoffCheckpointKey(BellLabsRunKey):
    goal_handoff_checkpoint_id: str = Field(pattern=IDENTIFIER_PATTERN)
    goal_iteration: int = Field(ge=1)


class SemanticOperationAttemptKey(BellLabsRunKey):
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


class AsyncTaskKey(Identity):
    provider: Literal["deepagents"] = "deepagents"
    deployment_endpoint_id: str = Field(pattern=IDENTIFIER_PATTERN)
    async_task_id: str = Field(pattern=IDENTIFIER_PATTERN)
    child_thread: AgentThreadKey
    child_run: AgentRunKey | None = None


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
