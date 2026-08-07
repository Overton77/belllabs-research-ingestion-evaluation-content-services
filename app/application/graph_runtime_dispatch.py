from __future__ import annotations

from datetime import UTC, datetime
from typing import Protocol
from uuid import NAMESPACE_URL, uuid5

from app.application.runtime_execution_bindings import (
    RuntimeBindingConflict,
    RuntimeExecutionBindingRepository,
    touch_binding,
)
from app.domain.control_plane.contracts import ExactDefinitionRef
from app.domain.graph_runtime.contracts import (
    AttemptDisposition,
    GraphExecutionReceipt,
    GraphExecutionSubmission,
    RuntimeExecutionAttempt,
    RuntimeExecutionBinding,
    RuntimeExecutionStatus,
)
from app.domain.graph_runtime.definitions import RunPlan, RunPlanV3
from app.domain.graph_runtime.identities import (
    AgentThreadKey,
    DeploymentIdentity,
    RuntimeTransportAttemptKey,
)


class GraphRuntimeClient(Protocol):
    runtime_provider: str

    def deployment_for(
        self,
        submission: GraphExecutionSubmission,
    ) -> DeploymentIdentity | None: ...

    def thread_for(
        self,
        submission: GraphExecutionSubmission,
    ) -> AgentThreadKey | None: ...

    async def submit(
        self,
        submission: GraphExecutionSubmission,
        binding: RuntimeExecutionBinding,
    ) -> GraphExecutionReceipt: ...

    async def reconcile_submission(
        self,
        submission: GraphExecutionSubmission,
        binding: RuntimeExecutionBinding,
    ) -> GraphExecutionReceipt | None: ...


class RuntimeSelector(Protocol):
    def select(self, workflow_implementation_ref: ExactDefinitionRef) -> GraphRuntimeClient: ...


class ExactRuntimeSelector:
    """Selects a runtime only by the admitted Workflow Implementation exact ref."""

    def __init__(
        self,
        clients: dict[tuple[str, int, str], GraphRuntimeClient],
    ) -> None:
        self._clients = clients

    def select(self, workflow_implementation_ref: ExactDefinitionRef) -> GraphRuntimeClient:
        key = (
            workflow_implementation_ref.logical_id,
            workflow_implementation_ref.revision,
            workflow_implementation_ref.digest,
        )
        try:
            return self._clients[key]
        except KeyError as error:
            raise LookupError(
                "no runtime is admitted for the exact Workflow Implementation"
            ) from error


class GraphRuntimeDispatchService:
    """Binds immutable runtime intent before dispatch and reconciles ambiguous launches."""

    def __init__(
        self,
        *,
        repository: RuntimeExecutionBindingRepository,
        selector: RuntimeSelector,
        allow_legacy_plan: bool = False,
    ) -> None:
        self._repository = repository
        self._selector = selector
        self._allow_legacy_plan = allow_legacy_plan

    async def submit(
        self,
        submission: GraphExecutionSubmission,
        run_plan: RunPlan | RunPlanV3,
    ) -> GraphExecutionReceipt:
        self._validate_plan(submission, run_plan)
        existing = await self._repository.get_by_submission(
            submission.epoch.request_scope,
            submission.submission_id,
        )
        if existing is not None:
            if existing.submission_digest != submission.request_digest:
                raise RuntimeBindingConflict(
                    "runtime submission identity was reused with a conflicting request"
                )
            return await self._existing_receipt(submission, existing)

        client = self._selector.select(run_plan.workflow_implementation_ref)
        deployment = client.deployment_for(submission)
        if submission.target_deployment is not None and deployment != submission.target_deployment:
            raise RuntimeBindingConflict(
                "selected runtime does not match the frozen deployment route"
            )
        observed_at = submission.submitted_at
        binding = RuntimeExecutionBinding(
            binding_id=_stable_id("runtime-binding", submission.epoch.canonical_key),
            epoch=submission.epoch,
            submission_id=submission.submission_id,
            submission_idempotency_key=submission.idempotency_key,
            submission_digest=submission.request_digest,
            run_plan_digest=run_plan.plan_digest,
            graph_assembly_digest=submission.graph_assembly_digest,
            state_schema_digest=submission.state_schema_digest,
            runtime_provider=client.runtime_provider,
            deployment=deployment,
            agent_thread=client.thread_for(submission),
            graph_id=submission.target_graph_id,
            status=RuntimeExecutionStatus.SUBMITTING,
            created_at=observed_at,
            updated_at=observed_at,
        )
        reservation = await self._repository.create_binding(submission, binding)
        binding = reservation.binding
        if not reservation.created:
            return await self._existing_receipt(submission, binding)
        attempt = RuntimeExecutionAttempt(
            attempt_key=RuntimeTransportAttemptKey(
                **submission.epoch.model_dump(),
                runtime_attempt=1,
                submission_id=submission.submission_id,
            ),
            binding_id=binding.binding_id,
            disposition=AttemptDisposition.CREATED,
            provider_request_digest=submission.request_digest,
            started_at=observed_at,
        )
        try:
            receipt = await client.submit(submission, binding)
        except Exception:
            failed_at = datetime.now(UTC)
            await self._repository.append_attempt(
                attempt.model_copy(
                    update={
                        "disposition": AttemptDisposition.AMBIGUOUS,
                        "finished_at": failed_at,
                        "failure_code": "provider_acceptance_ambiguous",
                    }
                )
            )
            ambiguous = touch_binding(
                binding,
                observed_at=failed_at,
                status=RuntimeExecutionStatus.RECONCILIATION_REQUIRED,
            )
            await self._repository.update_binding(ambiguous, expected_version=binding.version)
            raise
        self._validate_receipt(submission, binding, receipt)
        await self._repository.append_attempt(
            attempt.model_copy(
                update={
                    "disposition": AttemptDisposition.ACCEPTED,
                    "agent_run": receipt.agent_run,
                    "finished_at": receipt.accepted_at,
                }
            )
        )
        accepted = touch_binding(
            binding,
            observed_at=receipt.accepted_at,
            status=(
                RuntimeExecutionStatus.RECONCILIATION_REQUIRED
                if receipt.status == "reconciliation_required"
                else RuntimeExecutionStatus.ACCEPTED
            ),
        )
        await self._repository.update_binding(accepted, expected_version=binding.version)
        return receipt

    async def reconcile(
        self,
        submission: GraphExecutionSubmission,
        run_plan: RunPlan | RunPlanV3,
    ) -> GraphExecutionReceipt | None:
        self._validate_plan(submission, run_plan)
        binding = await self._repository.get_by_submission(
            submission.epoch.request_scope,
            submission.submission_id,
        )
        if binding is None:
            return None
        client = self._selector.select(run_plan.workflow_implementation_ref)
        receipt = await client.reconcile_submission(submission, binding)
        if receipt is None:
            return None
        self._validate_receipt(submission, binding, receipt)
        if binding.status == RuntimeExecutionStatus.RECONCILIATION_REQUIRED:
            await self._repository.append_attempt(
                RuntimeExecutionAttempt(
                    attempt_key=RuntimeTransportAttemptKey(
                        **submission.epoch.model_dump(),
                        runtime_attempt=2,
                        submission_id=submission.submission_id,
                    ),
                    binding_id=binding.binding_id,
                    disposition=AttemptDisposition.ACCEPTED,
                    agent_run=receipt.agent_run,
                    provider_request_digest=submission.request_digest,
                    started_at=receipt.accepted_at,
                    finished_at=receipt.accepted_at,
                    provider_metadata={"source": "reconciliation"},
                )
            )
            accepted = touch_binding(
                binding,
                observed_at=receipt.accepted_at,
                status=RuntimeExecutionStatus.ACCEPTED,
            )
            await self._repository.update_binding(accepted, expected_version=binding.version)
        return receipt

    async def _existing_receipt(
        self,
        submission: GraphExecutionSubmission,
        binding: RuntimeExecutionBinding,
    ) -> GraphExecutionReceipt:
        projection = await self._repository.projection(submission.epoch)
        agent_run = None
        if projection is not None:
            agent_run = next(
                (
                    attempt.agent_run
                    for attempt in reversed(projection.attempts)
                    if attempt.agent_run
                ),
                None,
            )
        return GraphExecutionReceipt(
            submission_id=submission.submission_id,
            request_digest=submission.request_digest,
            epoch=submission.epoch,
            status=(
                "reconciliation_required"
                if binding.status == RuntimeExecutionStatus.RECONCILIATION_REQUIRED
                else "existing"
            ),
            binding_id=binding.binding_id,
            agent_thread=binding.agent_thread,
            agent_run=agent_run,
            accepted_at=binding.updated_at,
        )

    def _validate_plan(
        self,
        submission: GraphExecutionSubmission,
        run_plan: RunPlan | RunPlanV3,
    ) -> None:
        if isinstance(run_plan, RunPlan) and not self._allow_legacy_plan:
            raise ValueError("production graph dispatch requires a frozen RunPlanV3")
        if isinstance(run_plan, RunPlanV3) and (
            submission.target_deployment is None or submission.target_graph_id is None
        ):
            raise ValueError("RunPlanV3 dispatch requires an exact deployment and graph route")
        if (
            submission.run_plan_digest != run_plan.plan_digest
            or submission.run_plan_ref.digest != run_plan.plan_digest
        ):
            raise ValueError("submission RunPlan digest differs from the frozen plan")
        if (
            submission.graph_assembly_digest
            != run_plan.graph_assembly.graph_assembly_ref.digest
            or submission.state_schema_digest != run_plan.graph_assembly.state_schema_digest
        ):
            raise ValueError("runtime submission cannot widen or reinterpret graph assembly")

    @staticmethod
    def _validate_receipt(
        submission: GraphExecutionSubmission,
        binding: RuntimeExecutionBinding,
        receipt: GraphExecutionReceipt,
    ) -> None:
        if (
            receipt.submission_id != submission.submission_id
            or receipt.request_digest != submission.request_digest
            or receipt.epoch != submission.epoch
            or receipt.binding_id != binding.binding_id
        ):
            raise RuntimeBindingConflict("runtime receipt does not match the frozen submission")
        if receipt.agent_thread != binding.agent_thread:
            raise RuntimeBindingConflict("runtime changed the pre-bound thread identity")
        if receipt.agent_run is not None:
            if binding.deployment is None:
                raise RuntimeBindingConflict("legacy bindings cannot accept Agent Server run IDs")
            if (
                receipt.agent_run.deployment_endpoint_id
                != binding.deployment.deployment_endpoint_id
            ):
                raise RuntimeBindingConflict(
                    "Agent Server run belongs to a different deployment endpoint"
                )


def _stable_id(*parts: str) -> str:
    return str(uuid5(NAMESPACE_URL, ":".join(parts)))
