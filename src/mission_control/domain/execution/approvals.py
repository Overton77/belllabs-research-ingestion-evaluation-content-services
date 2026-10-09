"""`mc.approval_binding.v1`: the correlation between one durable Human Task and the native
provider request (or workflow gate) it answers (multi-provider SPEC-03; MP-01).

Two origins of human interaction share one Human Task service: the explicit Human Gate
program node and native provider requests (permission callbacks, user questions, MCP
elicitation, governed domain effects). The binding records which native request a task
answers, under which generation, for which exact normalized tool input and policy, with a
deadline and the replay strategy to use if the native correlation is lost. Approval applies
to the exact `input_digest`; edited arguments are a new digest and, where policy says so, a
new review. An opaque connection-scoped request handle is never reused after restart.

Pure contracts; the Human Task tables and runtime approval gateway consume them.
"""

from __future__ import annotations

from typing import Any, Final, Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

from mission_control.domain.execution.lanes import DIGEST_PATTERN, ApprovalMode, LaneProfileName

APPROVAL_BINDING_SCHEMA: Final = "mc.approval_binding.v1"

ApprovalOrigin = ApprovalMode
ReplayStrategy = Literal[
    "reissue_native_request",
    "restart_at_safe_boundary",
    "park_for_reconciliation",
    "deny_and_park",
]
ApprovalDecision = Literal["approve", "deny", "approve_edited", "request_changes", "cancel"]
ElicitationAction = Literal["accept", "decline", "cancel"]

# What a decision may become on each origin (SPEC-03 "Native provider requests").
ADMITTED_DECISIONS: Final[dict[str, frozenset[str]]] = {
    "workflow_gate": frozenset({"approve", "deny", "request_changes", "cancel"}),
    "provider_permission": frozenset({"approve", "deny", "approve_edited", "cancel"}),
    "provider_question": frozenset({"approve", "cancel"}),
    "mcp_elicitation": frozenset({"approve", "deny", "cancel"}),
    "governed_effect": frozenset({"approve", "deny", "cancel"}),
}


class ApprovalContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class NativeApprovalCorrelation(ApprovalContract):
    """The provider-side identities a pending request has; none of them is a credential."""

    native_session_ref: str | None = Field(default=None, min_length=1, max_length=1_024)
    native_turn_ref: str | None = Field(default=None, min_length=1, max_length=1_024)
    native_request_ref: str | None = Field(default=None, min_length=1, max_length=1_024)
    tool_call_ref: str | None = Field(default=None, min_length=1, max_length=1_024)
    connection_scoped: bool = True


class ApprovalBinding(ApprovalContract):
    """`mc.approval_binding.v1`: one Human Task bound to its origin and native request."""

    schema_version: Literal["mc.approval_binding.v1"] = APPROVAL_BINDING_SCHEMA
    human_task_id: str = Field(min_length=1, max_length=512)
    origin: ApprovalOrigin
    lane_profile: LaneProfileName
    harness_execution_id: str = Field(min_length=1, max_length=512)
    generation: int = Field(ge=1)
    native: NativeApprovalCorrelation = Field(default_factory=NativeApprovalCorrelation)
    tool_name: str | None = Field(default=None, min_length=1, max_length=256)
    input_digest: str = Field(pattern=DIGEST_PATTERN)
    policy_digest: str = Field(pattern=DIGEST_PATTERN)
    review_packet_ref: str | None = Field(default=None, min_length=1, max_length=2_048)
    opened_at: AwareDatetime
    deadline: AwareDatetime | None = None
    replay_strategy: ReplayStrategy
    task_version: int = Field(default=1, ge=1)

    @model_validator(mode="after")
    def origin_shape(self) -> ApprovalBinding:
        if self.origin == "workflow_gate":
            if self.native.native_request_ref is not None:
                raise ValueError("a workflow gate has no native request to correlate")
            if self.review_packet_ref is None:
                raise ValueError("a workflow gate binds the review packet it authorizes")
        else:
            if self.native.native_request_ref is None and self.native.tool_call_ref is None:
                raise ValueError(
                    f"a {self.origin} binding names the native request or tool call it answers"
                )
            if self.origin in {"provider_permission", "governed_effect"} and self.tool_name is None:
                raise ValueError(f"a {self.origin} binding names the tool")
        if self.deadline is not None and self.deadline <= self.opened_at:
            raise ValueError("the approval deadline follows opened_at")
        return self

    def admits(self, decision: str) -> bool:
        return decision in ADMITTED_DECISIONS[self.origin]


class ApprovalResolutionIntent(ApprovalContract):
    """A human decision about one binding, before it is applied to the provider.

    `expected_task_version` and `request_id` make retries idempotent and stale answers
    typed failures. `edited_input_digest` is required for `approve_edited` and must differ
    from the bound `input_digest`. Feedback is an attributed artifact, not permission.
    """

    human_task_id: str = Field(min_length=1, max_length=512)
    request_id: str = Field(min_length=1, max_length=512)
    expected_task_version: int = Field(ge=1)
    decision: ApprovalDecision
    actor_ref: str = Field(min_length=1, max_length=256)
    edited_input_digest: str | None = Field(default=None, pattern=DIGEST_PATTERN)
    feedback_artifact_refs: tuple[str, ...] = ()
    elicitation_action: ElicitationAction | None = None
    decided_at: AwareDatetime

    def validate_against(self, binding: ApprovalBinding) -> None:
        """Raise `ValueError` when the intent cannot apply to the binding as it stands."""

        if self.human_task_id != binding.human_task_id:
            raise ValueError("resolution names a different human task")
        if self.expected_task_version != binding.task_version:
            raise ValueError(
                f"stale resolution: expected task version {self.expected_task_version}, "
                f"binding is at {binding.task_version}"
            )
        if not binding.admits(self.decision):
            raise ValueError(f"decision {self.decision} is not admitted for {binding.origin}")
        if self.decision == "approve_edited":
            if self.edited_input_digest is None:
                raise ValueError("approve_edited carries the edited input digest")
            if self.edited_input_digest == binding.input_digest:
                raise ValueError("approve_edited must change the input digest")
        elif self.edited_input_digest is not None:
            raise ValueError("only approve_edited carries an edited input digest")
        if binding.origin == "mcp_elicitation" and self.elicitation_action is None:
            raise ValueError("an mcp_elicitation resolution states accept, decline or cancel")


def approval_contract_schemas() -> dict[str, dict[str, Any]]:
    return {
        "approval_binding": ApprovalBinding.model_json_schema(),
        "approval_resolution_intent": ApprovalResolutionIntent.model_json_schema(),
    }


__all__ = [
    "ADMITTED_DECISIONS",
    "APPROVAL_BINDING_SCHEMA",
    "ApprovalBinding",
    "ApprovalDecision",
    "ApprovalOrigin",
    "ApprovalResolutionIntent",
    "NativeApprovalCorrelation",
    "ReplayStrategy",
    "approval_contract_schemas",
]
