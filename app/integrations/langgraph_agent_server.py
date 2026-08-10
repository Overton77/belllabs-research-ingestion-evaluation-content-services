from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any, Literal, Protocol
from uuid import NAMESPACE_URL, uuid5

from langgraph.types import Overwrite
from pydantic import BaseModel, ConfigDict, Field

from app.application.agent_server_actions import ResolvedAgentServerAction
from app.application.runtime_execution_bindings import RuntimeExecutionBindingRepository
from app.application.runtime_repairs import (
    PrivilegedRepairObservation,
)
from app.domain.control_plane.canonical import sha256_digest
from app.domain.graph_runtime.contracts import (
    CancelRunIntervention,
    GraphExecutionReceipt,
    GraphExecutionSubmission,
    InterventionReceipt,
    PrivilegedOperatorReconcileIntervention,
    RuntimeExecutionBinding,
    RuntimeIntervention,
)
from app.domain.graph_runtime.identities import (
    AgentRunKey,
    AgentThreadKey,
    DeploymentIdentity,
)


class AgentServerSDKClient(Protocol):
    threads: Any
    runs: Any


class RuntimeInputResolver(Protocol):
    async def resolve(
        self,
        submission: GraphExecutionSubmission,
        binding: RuntimeExecutionBinding,
    ) -> Mapping[str, Any]: ...


class AgentServerRuntimeConfig(BaseModel):
    """Exact non-secret route facts for one admitted Agent Server deployment."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    deployment: DeploymentIdentity
    graph_id: str = Field(min_length=1)


class AgentServerActionResolver(Protocol):
    async def resolve(
        self,
        intervention: RuntimeIntervention,
        binding: RuntimeExecutionBinding,
    ) -> ResolvedAgentServerAction: ...


class LangGraphAgentServerClient:
    """Pinned SDK adapter that routes only to one exact deployment and graph."""

    runtime_provider = "langgraph_agent_server"

    def __init__(
        self,
        *,
        client: AgentServerSDKClient,
        config: AgentServerRuntimeConfig,
        input_resolver: RuntimeInputResolver,
    ) -> None:
        self._client = client
        self._config = config
        self._input_resolver = input_resolver

    def deployment_for(
        self,
        submission: GraphExecutionSubmission,
    ) -> DeploymentIdentity:
        self._require_route(submission)
        return self._config.deployment

    def thread_for(self, submission: GraphExecutionSubmission) -> AgentThreadKey:
        self._require_route(submission)
        thread_id = str(
            uuid5(
                NAMESPACE_URL,
                ":".join(
                    (
                        "belllabs-agent-thread",
                        self._config.deployment.deployment_endpoint_id,
                        submission.epoch.canonical_key,
                    )
                ),
            )
        )
        return AgentThreadKey(
            **submission.epoch.model_dump(),
            agent_server_thread_id=thread_id,
            relationship="parent",
        )

    async def submit(
        self,
        submission: GraphExecutionSubmission,
        binding: RuntimeExecutionBinding,
    ) -> GraphExecutionReceipt:
        self._require_binding(submission, binding)
        assert binding.agent_thread is not None
        metadata = self._metadata(submission, binding)
        thread = await self._client.threads.create(
            thread_id=binding.agent_thread.agent_server_thread_id,
            if_exists="do_nothing",
            graph_id=self._config.graph_id,
            metadata=metadata,
        )
        if thread["thread_id"] != binding.agent_thread.agent_server_thread_id:
            raise ValueError("Agent Server returned a different thread identity")
        runtime_input = dict(await self._input_resolver.resolve(submission, binding))
        run = await self._client.runs.create(
            binding.agent_thread.agent_server_thread_id,
            self._config.deployment.assistant_id,
            input=runtime_input,
            metadata=metadata,
            multitask_strategy="reject",
            durability="sync",
        )
        agent_run = self._agent_run(run)
        return GraphExecutionReceipt(
            submission_id=submission.submission_id,
            request_digest=submission.request_digest,
            epoch=submission.epoch,
            status="accepted",
            binding_id=binding.binding_id,
            agent_thread=binding.agent_thread,
            agent_run=agent_run,
            accepted_at=_timestamp(run.get("created_at")),
        )

    async def reconcile_submission(
        self,
        submission: GraphExecutionSubmission,
        binding: RuntimeExecutionBinding,
    ) -> GraphExecutionReceipt | None:
        self._require_binding(submission, binding)
        assert binding.agent_thread is not None
        expected = self._metadata(submission, binding)
        try:
            runs = await self._client.runs.list(
                binding.agent_thread.agent_server_thread_id,
                limit=100,
            )
        except Exception:
            # Transport/auth/not-found distinctions are classified by the outer reconciler.
            # This adapter never turns an ambiguous read into a second submission.
            return None
        matches = [
            run
            for run in runs
            if all((run.get("metadata") or {}).get(key) == value for key, value in expected.items())
        ]
        if not matches:
            return None
        if len(matches) != 1:
            raise ValueError("submission metadata resolves to multiple Agent Server runs")
        run = matches[0]
        return GraphExecutionReceipt(
            submission_id=submission.submission_id,
            request_digest=submission.request_digest,
            epoch=submission.epoch,
            status="accepted",
            binding_id=binding.binding_id,
            agent_thread=binding.agent_thread,
            agent_run=self._agent_run(run),
            accepted_at=_timestamp(run.get("created_at")),
        )

    def _require_route(self, submission: GraphExecutionSubmission) -> None:
        if submission.target_deployment != self._config.deployment:
            raise ValueError("submission is not bound to this exact Agent Server deployment")
        if submission.target_graph_id != self._config.graph_id:
            raise ValueError("submission is not bound to this exact Agent Server graph")

    def _require_binding(
        self,
        submission: GraphExecutionSubmission,
        binding: RuntimeExecutionBinding,
    ) -> None:
        self._require_route(submission)
        if binding.deployment != self._config.deployment:
            raise ValueError("runtime binding deployment route changed")
        if binding.graph_id != self._config.graph_id:
            raise ValueError("runtime binding graph route changed")
        if binding.agent_thread != self.thread_for(submission):
            raise ValueError("runtime binding thread route changed")

    @staticmethod
    def _metadata(
        submission: GraphExecutionSubmission,
        binding: RuntimeExecutionBinding,
    ) -> dict[str, str]:
        return {
            "belllabs_submission_id": submission.submission_id,
            "belllabs_submission_digest": submission.request_digest,
            "belllabs_binding_id": binding.binding_id,
            "belllabs_run_plan_digest": submission.run_plan_digest,
            "belllabs_graph_assembly_digest": submission.graph_assembly_digest,
        }

    def _agent_run(self, run: Mapping[str, Any]) -> AgentRunKey:
        run_id = run.get("run_id")
        if not isinstance(run_id, str) or not run_id:
            raise ValueError("Agent Server run response is missing its run identity")
        return AgentRunKey(
            deployment_endpoint_id=self._config.deployment.deployment_endpoint_id,
            agent_server_run_id=run_id,
        )


class LangGraphAgentServerInterventionClient:
    """Applies pre-persisted typed commands to one exact bound deployment."""

    def __init__(
        self,
        *,
        client: AgentServerSDKClient,
        config: AgentServerRuntimeConfig,
        bindings: RuntimeExecutionBindingRepository,
        resolver: AgentServerActionResolver,
    ) -> None:
        self._client = client
        self._config = config
        self._bindings = bindings
        self._resolver = resolver

    async def apply(
        self,
        intervention: RuntimeIntervention,
        *,
        binding_id: str,
    ) -> InterventionReceipt:
        binding = await self._bindings.get_binding(intervention.epoch)
        if binding is None or binding.binding_id != binding_id:
            raise LookupError("runtime binding is unavailable for Agent Server intervention")
        self._require_route(binding)
        assert binding.agent_thread is not None
        action = await self._resolver.resolve(intervention, binding)
        if isinstance(intervention, CancelRunIntervention) != (action.kind == "cancel"):
            raise ValueError("resolved Agent Server action does not match the typed intervention")
        if action.kind == "cancel":
            runs = await self._client.runs.list(
                binding.agent_thread.agent_server_thread_id,
                limit=100,
            )
            for run in runs:
                if run.get("status") in {"pending", "running"}:
                    await self._client.runs.cancel(
                        binding.agent_thread.agent_server_thread_id,
                        run["run_id"],
                        wait=False,
                        action="interrupt",
                    )
            reason_code = "cancellation_dispatched"
        else:
            await self._client.runs.create(
                binding.agent_thread.agent_server_thread_id,
                self._config.deployment.assistant_id,
                input=action.input,
                command=action.command,
                metadata={
                    "belllabs_command_id": intervention.command_id,
                    "belllabs_command_digest": intervention.request_digest,
                    "belllabs_binding_id": binding.binding_id,
                },
                multitask_strategy=action.multitask_strategy,
                durability="sync",
            )
            reason_code = "intervention_dispatched"
        return InterventionReceipt(
            command_id=intervention.command_id,
            status="accepted",
            binding_id=binding.binding_id,
            resulting_belllabs_version=intervention.expected_belllabs_version,
            reason_code=reason_code,
            recorded_at=intervention.requested_at,
        )

    async def reconcile(
        self,
        intervention: RuntimeIntervention,
        *,
        binding_id: str,
    ) -> InterventionReceipt | None:
        binding = await self._bindings.get_binding(intervention.epoch)
        if binding is None or binding.binding_id != binding_id:
            raise LookupError("runtime binding is unavailable for intervention reconciliation")
        self._require_route(binding)
        assert binding.agent_thread is not None
        runs = await self._client.runs.list(
            binding.agent_thread.agent_server_thread_id,
            limit=100,
        )
        if isinstance(intervention, CancelRunIntervention):
            active = [run for run in runs if run.get("status") in {"pending", "running"}]
            if active:
                return await self.apply(intervention, binding_id=binding_id)
            return self._receipt(
                intervention,
                binding_id,
                "cancellation_observed",
                status="existing",
            )
        matches = [
            run
            for run in runs
            if (run.get("metadata") or {}).get("belllabs_command_id") == intervention.command_id
            and (run.get("metadata") or {}).get("belllabs_command_digest")
            == intervention.request_digest
            and (run.get("metadata") or {}).get("belllabs_binding_id") == binding_id
        ]
        if len(matches) > 1:
            raise ValueError("intervention metadata resolves to multiple Agent Server runs")
        if not matches:
            return None
        return self._receipt(
            intervention,
            binding_id,
            "intervention_observed",
            status="existing",
        )

    @staticmethod
    def _receipt(
        intervention: RuntimeIntervention,
        binding_id: str,
        reason_code: str,
        *,
        status: Literal["accepted", "existing"] = "accepted",
    ) -> InterventionReceipt:
        return InterventionReceipt(
            command_id=intervention.command_id,
            status=status,
            binding_id=binding_id,
            resulting_belllabs_version=intervention.expected_belllabs_version,
            reason_code=reason_code,
            recorded_at=intervention.requested_at,
        )

    def _require_route(self, binding: RuntimeExecutionBinding) -> None:
        if binding.deployment != self._config.deployment:
            raise ValueError("intervention cannot change deployment endpoint or revision")
        if binding.graph_id != self._config.graph_id:
            raise ValueError("intervention cannot change the bound graph")
        if binding.agent_thread is None:
            raise ValueError("intervention requires the persisted Agent Server thread")


class LangGraphAgentServerRepairClient:
    """Exact-route privileged Overwrite adapter; outer service owns authorization/audit."""

    def __init__(
        self,
        *,
        client: AgentServerSDKClient,
        config: AgentServerRuntimeConfig,
    ) -> None:
        self._client = client
        self._config = config

    async def apply_overwrite(
        self,
        intervention: PrivilegedOperatorReconcileIntervention,
        binding: RuntimeExecutionBinding,
        overwrite: Overwrite,
    ) -> PrivilegedRepairObservation:
        self._require_route(intervention, binding)
        assert binding.agent_thread is not None
        assert intervention.expected_checkpoint is not None
        thread_id = binding.agent_thread.agent_server_thread_id
        checkpoint_id = intervention.expected_checkpoint.langgraph_checkpoint_id
        before = await self._client.threads.get_state(
            thread_id,
            checkpoint_id=checkpoint_id,
        )
        before_digest = sha256_digest(before.get("values") or {})
        replacement = {
            **dict(overwrite.value),
            "before_digest": before_digest,
        }
        await self._client.threads.update_state(
            thread_id,
            {"runtime_reconciliation": Overwrite(replacement)},
            checkpoint_id=checkpoint_id,
            as_node="bootstrap_runtime_authority",
        )
        after = await self._client.threads.get_state(thread_id)
        return PrivilegedRepairObservation(
            before_digest=before_digest,
            after_digest=sha256_digest(after.get("values") or {}),
        )

    async def reconcile_overwrite(
        self,
        intervention: PrivilegedOperatorReconcileIntervention,
        binding: RuntimeExecutionBinding,
    ) -> PrivilegedRepairObservation | None:
        self._require_route(intervention, binding)
        assert binding.agent_thread is not None
        latest = await self._client.threads.get_state(binding.agent_thread.agent_server_thread_id)
        repair = (latest.get("values") or {}).get("runtime_reconciliation")
        if not isinstance(repair, Mapping) or repair.get("command_id") != intervention.command_id:
            return None
        before_digest = repair.get("before_digest")
        if not isinstance(before_digest, str):
            raise ValueError("observed privileged repair is missing its before digest")
        digest = sha256_digest(latest.get("values") or {})
        return PrivilegedRepairObservation(
            before_digest=before_digest,
            after_digest=digest,
        )

    def _require_route(
        self,
        intervention: PrivilegedOperatorReconcileIntervention,
        binding: RuntimeExecutionBinding,
    ) -> None:
        if binding.epoch != intervention.epoch:
            raise ValueError("privileged repair epoch does not match runtime binding")
        if binding.deployment != self._config.deployment:
            raise ValueError("privileged repair cannot change deployment route")
        if binding.graph_id != self._config.graph_id or binding.agent_thread is None:
            raise ValueError("privileged repair requires exact graph and thread")
        assert intervention.expected_checkpoint is not None
        if (
            intervention.expected_checkpoint.deployment_endpoint_id
            != self._config.deployment.deployment_endpoint_id
            or intervention.expected_checkpoint.agent_server_thread_id
            != binding.agent_thread.agent_server_thread_id
        ):
            raise ValueError("privileged repair checkpoint is outside the bound route")


def _timestamp(value: object) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    if isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)
    return datetime.now(UTC)
