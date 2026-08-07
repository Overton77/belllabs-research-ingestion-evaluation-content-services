from __future__ import annotations

from typing import Any, Literal, Protocol

from pydantic import BaseModel, ConfigDict

from app.application.runtime_decisions import DurableDecisionService
from app.domain.graph_runtime.contracts import (
    AppendInputIntervention,
    CancelAsyncTaskIntervention,
    CancelRunIntervention,
    ForkFromCheckpointIntervention,
    PrivilegedOperatorReconcileIntervention,
    RespondToInterruptIntervention,
    ResumePauseIntervention,
    RuntimeExecutionBinding,
    RuntimeIntervention,
    SatisfyWaitIntervention,
    UpdateAsyncTaskIntervention,
)


class ResolvedAgentServerAction(BaseModel):
    """Provider action compiled only after the BellLabs command is durable."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    kind: Literal["input", "resume", "cancel"]
    input: dict[str, Any] | None = None
    command: dict[str, Any] | None = None
    multitask_strategy: Literal["reject", "enqueue"] = "reject"
    enqueue_authorized: bool = False

    def model_post_init(self, _context: Any) -> None:
        if self.kind == "input" and self.input is None:
            raise ValueError("input actions require compact resolved input")
        if self.kind == "resume" and self.command is None:
            raise ValueError("resume actions require a resolved Command payload")
        if self.kind == "cancel" and (self.input is not None or self.command is not None):
            raise ValueError("cancel actions cannot carry input or resume commands")
        if self.multitask_strategy == "enqueue" and not self.enqueue_authorized:
            raise ValueError("enqueue requires an authored workflow authorization")


class RuntimeActionPolicy(Protocol):
    async def enqueue_allowed(self, binding: RuntimeExecutionBinding) -> bool: ...


class RuntimeInterruptBindingRepository(Protocol):
    async def runtime_interrupt_map(
        self,
        request_scope: str,
        belllabs_decision_id: str,
    ) -> dict[str, str]: ...


class BellLabsAgentServerActionResolver:
    """Maps durable typed commands to compact SDK actions without state mutation."""

    def __init__(
        self,
        *,
        decisions: DurableDecisionService,
        interrupt_bindings: RuntimeInterruptBindingRepository,
        policy: RuntimeActionPolicy,
    ) -> None:
        self._decisions = decisions
        self._interrupt_bindings = interrupt_bindings
        self._policy = policy

    async def resolve(
        self,
        intervention: RuntimeIntervention,
        binding: RuntimeExecutionBinding,
    ) -> ResolvedAgentServerAction:
        if intervention.epoch != binding.epoch:
            raise ValueError("intervention epoch does not match the runtime binding")
        if isinstance(intervention, CancelRunIntervention):
            return ResolvedAgentServerAction(kind="cancel")
        if isinstance(intervention, AppendInputIntervention):
            enqueue = await self._policy.enqueue_allowed(binding)
            return ResolvedAgentServerAction(
                kind="input",
                input={
                    "input_manifest_ref": intervention.input_manifest_ref,
                    "input_digest": intervention.input_digest,
                    "command_id": intervention.command_id,
                },
                multitask_strategy="enqueue" if enqueue else "reject",
                enqueue_authorized=enqueue,
            )
        if isinstance(intervention, RespondToInterruptIntervention):
            runtime_map = await self._interrupt_bindings.runtime_interrupt_map(
                intervention.epoch.request_scope,
                intervention.interrupt_request_id,
            )
            resume = await self._decisions.resume_map(
                request_scope=intervention.epoch.request_scope,
                runtime_interrupt_to_decision=runtime_map,
            )
            return ResolvedAgentServerAction(
                kind="resume",
                command={"resume": resume},
            )
        if isinstance(intervention, SatisfyWaitIntervention):
            return ResolvedAgentServerAction(
                kind="resume",
                command={
                    "resume": {
                        "wait_condition_id": intervention.wait_condition_id,
                        "satisfaction_ref": intervention.satisfaction_ref,
                    }
                },
            )
        if isinstance(intervention, ResumePauseIntervention):
            return ResolvedAgentServerAction(
                kind="resume",
                command={
                    "resume": {
                        "pause_decision_id": intervention.pause_decision_id,
                    }
                },
            )
        if isinstance(
            intervention,
            UpdateAsyncTaskIntervention | CancelAsyncTaskIntervention,
        ):
            raise ValueError("async task interventions remain disabled until Stage 6")
        if isinstance(intervention, ForkFromCheckpointIntervention):
            raise ValueError("fork commands require the admitted RuntimeForkService path")
        if isinstance(intervention, PrivilegedOperatorReconcileIntervention):
            raise ValueError("privileged repair cannot map to unchecked Agent Server state updates")
        raise TypeError("unsupported runtime intervention")
