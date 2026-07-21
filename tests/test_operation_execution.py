from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from app.application.operation_execution import (
    InMemoryOperationBindingRepository,
    OperationBudgetReconciliationInProgress,
    OperationExecutionInProgress,
    OperationExecutionService,
    RunControlOperationAuthority,
    RunControlOperationBudgetAuthority,
)
from app.domain.control_plane.canonical import sha256_digest
from app.domain.control_plane.contracts import (
    DefinitionKind,
    ExactDefinitionRef,
    SecretRef,
    WorkflowWorkspaceContract,
    WorkspaceSlot,
)
from app.domain.operation_execution.contracts import (
    CapabilityGrant,
    ImmutableAssetBinding,
    MCPServerBinding,
    ModelPolicy,
    OperationAttemptIdentity,
    OperationExecutionRequest,
    OperationSettlement,
    PromptSegment,
    PromptTrustClass,
    RuntimeUsage,
    WorkspaceContract,
    WorkspaceOwner,
    WorkspaceOwnerKind,
    WorkspaceSlotBinding,
)
from app.domain.run_control.contracts import (
    ActorContext,
    CommandResult,
    CommandStatus,
    LifecycleCommand,
    RunPhase,
)
from app.domain.run_control.errors import IdempotencyConflict
from app.integrations.conformance_operation_runtime import (
    ConformanceAssetVerifier,
    ConformanceAuthority,
    ConformanceBudgetAuthority,
    ConformanceEventSink,
    ConformanceRuntime,
    ConformanceSandbox,
    ConformanceSecretResolver,
)
from app.temporal.operation_activities import (
    OperationExecutionActivities,
    parse_operation_result,
)
from app.temporal.operation_workflow import OperationExecutionWorkflow

NOW = datetime(2026, 7, 19, 20, 0, tzinfo=UTC)
DIGEST = "sha256:" + "a" * 64
MCP_DIGEST = "sha256:" + "b" * 64
SKILL_DIGEST = "sha256:" + "c" * 64
SECRET_VALUE = "must-never-be-persisted"


def exact(kind: DefinitionKind, logical_id: str, digest: str = DIGEST) -> ExactDefinitionRef:
    return ExactDefinitionRef(kind=kind, logical_id=logical_id, revision=1, digest=digest)


def operation_request(
    *, attempt: int = 1, prompt: str = "Return BINDING-OK"
) -> OperationExecutionRequest:
    return OperationExecutionRequest(
        identity=OperationAttemptIdentity(
            run_id="run-operation",
            operation_id="sandbox-agent",
            operation_attempt=attempt,
        ),
        request_scope="tenant-1",
        effective_configuration_digest=DIGEST,
        run_control_revision=4,
        operation_contract_ref="operation:sandbox-agent@1",
        prompt_segments=(
            PromptSegment(
                source_ref="prompt:system@1",
                source_revision=1,
                trust_class=PromptTrustClass.SYSTEM_AUTHORITY,
                content="Use only the exact bound capabilities.",
                rendered_digest=sha256_digest("Use only the exact bound capabilities."),
            ),
            PromptSegment(
                source_ref="input:manifest@1",
                source_revision=1,
                trust_class=PromptTrustClass.ADMITTED_INPUT,
                content=prompt,
                rendered_digest=sha256_digest(prompt),
            ),
        ),
        model_policy=ModelPolicy(
            provider="openai",
            model="gpt-5-mini",
            reasoning_effort="minimal",
            verbosity="low",
            max_turns=2,
        ),
        mcp_servers=(
            MCPServerBinding(
                server_id="fixture-mcp",
                revision=1,
                transport="streamable_http",
                endpoint_ref="secretless-fixture://mcp",
                allowed_tools=frozenset({"lookup_fixture"}),
                schema_digest=MCP_DIGEST,
                approval_policy="always",
            ),
        ),
        skills=(
            ImmutableAssetBinding(
                ref=exact(DefinitionKind.SKILL, "fixture.skill", SKILL_DIGEST),
                manifest_digest=SKILL_DIGEST,
                mount_path="/skills/fixture/SKILL.md",
            ),
        ),
        agent_profile_ref=exact(DefinitionKind.AGENT_PROFILE, "fixture.agent"),
        capability_grant=CapabilityGrant(
            capabilities=frozenset({"model.invoke", "sandbox.execute", "mcp.call"}),
            mcp_server_ids=frozenset({"fixture-mcp"}),
        ),
        workspace=WorkspaceContract(
            namespace_id="workspace-namespace:run-operation",
            workspace_id=f"workspace:run-operation:attempt:{attempt}",
            provider="conformance-sandbox",
            template_ref=exact(DefinitionKind.WORKSPACE_TEMPLATE, "fixture.workspace"),
            exclusive_write_paths=("/workspace/output",),
            runtime_digest=DIGEST,
            image_digest=DIGEST,
            package_digest=DIGEST,
            environment_digest=DIGEST,
        ),
        secret_refs=(SecretRef(provider="environment", key="OPENAI_API_KEY"),),
        budget_reservation_id="reservation:operation",
        budget_limits={"model.turns": 2, "tokens.total": 20},
        tracing_policy_ref="tracing:no-sensitive-data@1",
        sensitive_data_policy_ref="sensitive:redact@1",
        snapshot_policy_ref="snapshot:on-failure@1",
        prior_binding_id=None if attempt == 1 else "prior-binding",
        requested_at=NOW,
        idempotency_key=f"side-effect:run-operation:sandbox-agent:{attempt}",
    )


def service_fixture(
    *,
    assets: ConformanceAssetVerifier | None = None,
    runtime: ConformanceRuntime | None = None,
) -> tuple[
    OperationExecutionService,
    InMemoryOperationBindingRepository,
    ConformanceRuntime,
    ConformanceEventSink,
    ConformanceBudgetAuthority,
]:
    request = operation_request()
    bindings = InMemoryOperationBindingRepository()
    runtime = runtime or ConformanceRuntime()
    events = ConformanceEventSink()
    budget = ConformanceBudgetAuthority()
    asset_verifier = assets or ConformanceAssetVerifier(
        mcp_schema_digests={"fixture-mcp": MCP_DIGEST},
        asset_manifest_digests={"skill:fixture.skill:1": SKILL_DIGEST},
    )
    service = OperationExecutionService(
        authority=ConformanceAuthority(
            accepted_run_id=request.identity.run_id,
            configuration_digest=request.effective_configuration_digest,
            control_revision=request.run_control_revision,
            reservation_id=request.budget_reservation_id,
        ),
        bindings=bindings,
        runtime=runtime,
        sandbox=ConformanceSandbox(),
        assets=asset_verifier,
        mcp=asset_verifier,
        secrets=ConformanceSecretResolver({"environment:OPENAI_API_KEY": SECRET_VALUE}),
        events=events,
        budget=budget,
    )
    return service, bindings, runtime, events, budget


@pytest.mark.asyncio
async def test_binding_precedes_effects_and_retry_is_exactly_idempotent() -> None:
    service, bindings, runtime, events, budget = service_fixture()
    request = operation_request()

    first = await service.execute(request)
    replayed = await service.execute(request)

    assert first == replayed
    assert first.status == "completed"
    assert len(runtime.invocations) == 1
    binding = await bindings.get_binding(request.identity.semantic_key)
    assert binding is not None
    assert binding.binding_id == first.binding_id
    assert len(events.events) == 1
    assert len(budget.settlements) == 1
    persisted = repr((binding, await bindings.get_settlement(binding.binding_id), events.events))
    assert SECRET_VALUE not in persisted
    assert SECRET_VALUE not in repr(runtime.invocations)


@pytest.mark.asyncio
async def test_preparation_failure_is_recorded_without_runtime_invocation() -> None:
    service, bindings, runtime, _events, budget = service_fixture(
        assets=ConformanceAssetVerifier(
            mcp_schema_digests={"fixture-mcp": DIGEST},
            asset_manifest_digests={"skill:fixture.skill:1": SKILL_DIGEST},
        )
    )
    request = operation_request()

    result = await service.execute(request)

    assert result.status == "failed"
    assert result.failure_code == "preparation_failed"
    assert runtime.invocations == []
    binding = await bindings.get_binding(request.identity.semantic_key)
    assert binding is not None
    assert await bindings.get_settlement(binding.binding_id) is not None
    assert len(budget.settlements) == 1


@pytest.mark.asyncio
async def test_run_control_authority_validates_exact_run_workspace_and_reservation() -> None:
    workspace_contract = WorkflowWorkspaceContract(
        slots=(
            WorkspaceSlot(
                name="output",
                path="/workspace/output",
                access="exclusive_write",
                purpose="operation output",
            ),
        )
    )
    request = operation_request()
    request = request.model_copy(
        update={
            "workspace": request.workspace.model_copy(
                update={
                    "workflow_contract_digest": sha256_digest(
                        workspace_contract.model_dump(mode="json")
                    ),
                    "slot_bindings": (
                        WorkspaceSlotBinding(
                            slot_name="output",
                            logical_path="/workspace/output",
                            access="exclusive_write",
                            owner=WorkspaceOwner(
                                kind=WorkspaceOwnerKind.STAGE,
                                owner_id="stage:operation",
                            ),
                        ),
                    ),
                }
            )
        }
    )

    class FakeRunControl:
        async def get_run(self, _scope: str, _run_id: str) -> SimpleNamespace:
            return SimpleNamespace(
                version=request.run_control_revision,
                phase=RunPhase.ACTIVE,
                effective_configuration_digest=request.effective_configuration_digest,
            )

        async def get_budget(self, _scope: str, _run_id: str) -> SimpleNamespace:
            return SimpleNamespace(
                reservations={request.budget_reservation_id: dict(request.budget_limits)}
            )

    class FakeControlPlane:
        async def retrieve_for_admission(self, _digest: str) -> SimpleNamespace:
            return SimpleNamespace(
                effective_authority=SimpleNamespace(
                    capabilities=request.capability_grant.capabilities
                ),
                source_refs=(
                    request.workspace.template_ref,
                    exact(DefinitionKind.PROMPT, "system"),
                ),
                workflow_workspace_contract=workspace_contract,
            )

    authority = RunControlOperationAuthority(
        FakeRunControl(),  # type: ignore[arg-type]
        FakeControlPlane(),  # type: ignore[arg-type]
    )
    await authority.verify(request)

    with pytest.raises(ValueError, match="Run Control revision"):
        await authority.verify(
            request.model_copy(update={"run_control_revision": request.run_control_revision + 1})
        )

    with pytest.raises(ValueError, match="outside the reservation"):
        await authority.verify(
            request.model_copy(
                update={
                    "budget_limits": {
                        **request.budget_limits,
                        "currency.actual_micros": 1,
                    }
                }
            )
        )


@pytest.mark.asyncio
async def test_runtime_budget_violation_fails_before_completed_settlement() -> None:
    class ExcessRuntime(ConformanceRuntime):
        async def execute(self, invocation, resolved_secrets):  # type: ignore[no-untyped-def]
            result = await super().execute(invocation, resolved_secrets)
            return result.model_copy(update={"usage": RuntimeUsage(amounts={"tokens.total": 21})})

    service, bindings, runtime, events, budget = service_fixture(runtime=ExcessRuntime())
    request = operation_request()

    result = await service.execute(request)
    settlement = await bindings.get_settlement(result.binding_id)

    assert result.status == "failed"
    assert result.failure_code == "budget_exceeded"
    assert settlement is not None
    assert settlement.status == "failed"
    assert settlement.usage.amounts == {"tokens.total": 21}
    assert runtime.effect_count == 1
    assert events.events == {}
    assert len(budget.settlements) == 1
    assert budget.settlements[settlement.settlement_id][2] is True


@pytest.mark.asyncio
async def test_budget_adapter_maps_settlement_to_stable_run_control_usage_command() -> None:
    service, bindings, _runtime, _events, _budget = service_fixture()
    request = operation_request()
    result = await service.execute(request)
    binding = await bindings.get_binding(request.identity.semantic_key)
    settlement = await bindings.get_settlement(result.binding_id)
    assert binding is not None
    assert settlement is not None

    class FakeRunControl:
        def __init__(self) -> None:
            self.commands: list[LifecycleCommand] = []
            self.version = 7
            self.usage_ids: set[str] = set()

        async def get_run(self, _scope: str, _run_id: str) -> SimpleNamespace:
            return SimpleNamespace(version=self.version)

        async def get_budget(self, _scope: str, _run_id: str) -> SimpleNamespace:
            return SimpleNamespace(usage_ids=frozenset(self.usage_ids))

        async def execute(self, command: LifecycleCommand) -> CommandResult:
            self.commands.append(command)
            if len(self.commands) == 1:
                self.version += 1
                return CommandResult(
                    command_id=command.command_id,
                    idempotency_issuer=command.idempotency_issuer,
                    run_id=command.run_id,
                    command_fingerprint=DIGEST,
                    status=CommandStatus.STALE,
                    resulting_run_version=self.version,
                    phase=RunPhase.ACTIVE,
                    reason_code="stale_run_version",
                    reason="simulated concurrent command",
                    recorded_at=NOW,
                )
            self.usage_ids.add(command.action.usage_id)
            self.version += 1
            return CommandResult(
                command_id=command.command_id,
                idempotency_issuer=command.idempotency_issuer,
                run_id=command.run_id,
                command_fingerprint=DIGEST,
                status=CommandStatus.ACCEPTED,
                resulting_run_version=self.version,
                phase=RunPhase.ACTIVE,
                reason_code="accepted",
                reason="usage reconciled",
                recorded_at=NOW,
            )

    run_control = FakeRunControl()
    adapter = RunControlOperationBudgetAuthority(
        run_control,  # type: ignore[arg-type]
        actor=ActorContext(
            actor_id="operation-runtime",
            permissions=frozenset({"workflow_run.report_usage"}),
        ),
    )
    await adapter.reconcile(
        binding=binding,
        settlement_id=settlement.settlement_id,
        usage=settlement.usage,
    )
    await adapter.reconcile(
        binding=binding,
        settlement_id=settlement.settlement_id,
        usage=settlement.usage,
    )

    first, second = run_control.commands
    assert first.command_id != second.command_id
    assert first.expected_run_version == 7
    assert second.expected_run_version == 8
    assert first.action.kind == "record_usage"
    assert first.action.usage_id == settlement.settlement_id
    assert second.action.usage_id == settlement.settlement_id


@pytest.mark.asyncio
async def test_budget_reconciliation_stale_exhaustion_remains_retryable() -> None:
    service, bindings, _runtime, _events, _budget = service_fixture()
    result = await service.execute(operation_request())
    binding = await bindings.get_binding(operation_request().identity.semantic_key)
    settlement = await bindings.get_settlement(result.binding_id)
    assert binding is not None
    assert settlement is not None

    class AlwaysStaleRunControl:
        def __init__(self) -> None:
            self.version = 10

        async def get_run(self, _scope: str, _run_id: str) -> SimpleNamespace:
            self.version += 1
            return SimpleNamespace(version=self.version)

        async def get_budget(self, _scope: str, _run_id: str) -> SimpleNamespace:
            return SimpleNamespace(usage_ids=frozenset())

        async def execute(self, command: LifecycleCommand) -> CommandResult:
            return CommandResult(
                command_id=command.command_id,
                idempotency_issuer=command.idempotency_issuer,
                run_id=command.run_id,
                command_fingerprint=DIGEST,
                status=CommandStatus.STALE,
                resulting_run_version=self.version + 1,
                phase=RunPhase.ACTIVE,
                reason_code="stale_run_version",
                reason="simulated continuous concurrency",
                recorded_at=NOW,
            )

    adapter = RunControlOperationBudgetAuthority(
        AlwaysStaleRunControl(),  # type: ignore[arg-type]
        actor=ActorContext(
            actor_id="operation-runtime",
            permissions=frozenset({"workflow_run.report_usage"}),
        ),
    )
    with pytest.raises(OperationBudgetReconciliationInProgress):
        await adapter.reconcile(
            binding=binding,
            settlement_id=settlement.settlement_id,
            usage=settlement.usage,
        )


@pytest.mark.asyncio
async def test_budget_adapter_rejects_usage_outside_immutable_binding() -> None:
    service, bindings, _runtime, _events, _budget = service_fixture()
    result = await service.execute(operation_request())
    binding = await bindings.get_binding(operation_request().identity.semantic_key)
    assert binding is not None

    adapter = RunControlOperationBudgetAuthority(
        SimpleNamespace(),  # type: ignore[arg-type]
        actor=ActorContext(
            actor_id="operation-runtime",
            permissions=frozenset({"workflow_run.report_usage"}),
        ),
    )
    with pytest.raises(ValueError, match="exceeds immutable"):
        await adapter.reconcile(
            binding=binding,
            settlement_id=f"{result.binding_id}:excess",
            usage=RuntimeUsage(
                amounts={"tokens.total": 15},
                pending_external_amounts={"tokens.total": 10},
            ),
        )
    with pytest.raises(ValueError, match="unbound budget dimensions"):
        await adapter.reconcile(
            binding=binding,
            settlement_id=f"{result.binding_id}:unbound",
            usage=RuntimeUsage(amounts={"currency.actual_micros": 1}),
        )


@pytest.mark.asyncio
async def test_conflicting_retry_is_rejected_and_new_attempt_has_lineage() -> None:
    service, bindings, runtime, _events, _budget = service_fixture()
    first = await service.execute(operation_request())

    with pytest.raises(IdempotencyConflict):
        await service.execute(operation_request(prompt="changed semantic input"))

    second_request = operation_request(attempt=2)
    second_request = second_request.model_copy(update={"prior_binding_id": first.binding_id})
    second = await service.execute(second_request)
    second_binding = await bindings.get_binding(second_request.identity.semantic_key)

    assert second.binding_id != first.binding_id
    assert second_binding is not None
    assert second_binding.prior_binding_id == first.binding_id
    assert len(runtime.invocations) == 2


@pytest.mark.asyncio
async def test_activity_redelivery_never_repeats_claimed_unsettled_provider_work() -> None:
    class FailSettlementOnce(InMemoryOperationBindingRepository):
        def __init__(self) -> None:
            super().__init__()
            self.failed = False

        async def settle(self, settlement: OperationSettlement) -> OperationSettlement:
            if not self.failed:
                self.failed = True
                raise RuntimeError("simulated worker loss before settlement commit")
            return await super().settle(settlement)

    request = operation_request()
    bindings = FailSettlementOnce()
    runtime = ConformanceRuntime()
    events = ConformanceEventSink()
    budget = ConformanceBudgetAuthority()
    asset_verifier = ConformanceAssetVerifier(
        mcp_schema_digests={"fixture-mcp": MCP_DIGEST},
        asset_manifest_digests={"skill:fixture.skill:1": SKILL_DIGEST},
    )
    service = OperationExecutionService(
        authority=ConformanceAuthority(
            accepted_run_id=request.identity.run_id,
            configuration_digest=request.effective_configuration_digest,
            control_revision=request.run_control_revision,
            reservation_id=request.budget_reservation_id,
        ),
        bindings=bindings,
        runtime=runtime,
        sandbox=ConformanceSandbox(),
        assets=asset_verifier,
        mcp=asset_verifier,
        secrets=ConformanceSecretResolver({"environment:OPENAI_API_KEY": SECRET_VALUE}),
        events=events,
        budget=budget,
    )

    with pytest.raises(RuntimeError, match="simulated worker loss"):
        await service.execute(request)
    with pytest.raises(OperationExecutionInProgress):
        await service.execute(request)
    assert len(runtime.invocations) == 1
    assert runtime.effect_count == 1
    assert len(events.events) == 0
    assert len(budget.settlements) == 0


@pytest.mark.asyncio
async def test_post_settlement_retry_completes_events_without_provider_reexecution() -> None:
    class FailEventOnce(ConformanceEventSink):
        def __init__(self) -> None:
            super().__init__()
            self.failed = False

        async def publish(
            self, *, event_key: str, binding_id: str, payload: dict[str, object]
        ) -> None:
            if not self.failed:
                self.failed = True
                raise RuntimeError("simulated event relay outage")
            await super().publish(
                event_key=event_key,
                binding_id=binding_id,
                payload=payload,
            )

    request = operation_request()
    bindings = InMemoryOperationBindingRepository()
    runtime = ConformanceRuntime()
    events = FailEventOnce()
    budget = ConformanceBudgetAuthority()
    asset_verifier = ConformanceAssetVerifier(
        mcp_schema_digests={"fixture-mcp": MCP_DIGEST},
        asset_manifest_digests={"skill:fixture.skill:1": SKILL_DIGEST},
    )
    service = OperationExecutionService(
        authority=ConformanceAuthority(
            accepted_run_id=request.identity.run_id,
            configuration_digest=request.effective_configuration_digest,
            control_revision=request.run_control_revision,
            reservation_id=request.budget_reservation_id,
        ),
        bindings=bindings,
        runtime=runtime,
        sandbox=ConformanceSandbox(),
        assets=asset_verifier,
        mcp=asset_verifier,
        secrets=ConformanceSecretResolver({"environment:OPENAI_API_KEY": SECRET_VALUE}),
        events=events,
        budget=budget,
    )

    with pytest.raises(RuntimeError, match="simulated event relay outage"):
        await service.execute(request)
    result = await service.execute(request)

    assert result.status == "completed"
    assert len(runtime.invocations) == 1
    assert runtime.effect_count == 1
    assert len(events.events) == 1
    assert len(budget.settlements) == 1


@pytest.mark.asyncio
async def test_real_temporal_activity_uses_public_operation_result_seam() -> None:
    service, _bindings, runtime, events, budget = service_fixture()
    activities = OperationExecutionActivities(service)
    try:
        environment = await WorkflowEnvironment.start_time_skipping()
    except RuntimeError as error:
        pytest.skip(f"Temporal test server is unavailable: {error}")

    async with environment:
        async with Worker(
            environment.client,
            task_queue="operation-execution-conformance",
            workflows=[OperationExecutionWorkflow],
            activities=[activities.execute],
        ):
            payload = await environment.client.execute_workflow(
                OperationExecutionWorkflow.run,
                operation_request().model_dump(mode="json"),
                id="operation-execution-conformance",
                task_queue="operation-execution-conformance",
            )

    result = parse_operation_result(payload)
    assert result.status == "completed"
    assert result.output_text == "conformance-ok"
    assert len(runtime.invocations) == 1
    assert len(events.events) == 1
    assert len(budget.settlements) == 1
