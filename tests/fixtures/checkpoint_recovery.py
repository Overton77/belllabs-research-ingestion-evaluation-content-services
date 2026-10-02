"""RRM-004 crash-window harness: a real Deep Agent behind the full operation boundary.

`OperationExecutionService` runs a real `create_deep_agent` graph (deterministic scripted
chat model) through `DeepAgentRuntimeAdapter`, the journaled coordinator over in-memory run
control, and the checkpoint lineage authority. Crashes are injected as `SimulatedWorkerCrash`,
a `BaseException` that no handler in the operation boundary intercepts: like a killed
process, it releases no lease and settles nothing. A new attempt then starts on the same
durable state after the lost holder's lease expires.

The persistent variant (real `AsyncPostgresSaver`, application PostgreSQL, real Temporal
worker restart) lives in `tests/integration/temporal/test_rrm_004_worker_restart_recovery.py`.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

from langchain_core.language_models import BaseChatModel
from langchain_core.messages import AIMessage, BaseMessage, ToolMessage
from langchain_core.outputs import ChatGeneration, ChatResult
from langchain_core.tools import BaseTool
from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.store.memory import InMemoryStore
from pydantic import PrivateAttr

from app.application.operations.checkpoint_lineage import (
    CheckpointLineageService,
    InMemoryCheckpointLineageRepository,
)
from app.application.operations.journaled_operation_execution import (
    JournaledOperationExecutionCoordinator,
)
from app.application.operations.operation_execution import (
    InMemoryOperationBindingRepository,
    OperationExecutionService,
    RunControlOperationAuthority,
    RuntimePort,
)
from app.application.operations.operation_journal import (
    OperationJournalMutation,
    OperationJournalService,
)
from app.application.operations.unit_reconciliation import UnitReconciliationService
from app.application.run_control.service import RunControlService
from app.domain.control_plane.canonical import sha256_digest
from app.domain.control_plane.contracts import (
    DefinitionKind,
    ExactDefinitionRef,
    WorkflowWorkspaceContract,
    WorkspaceSlot,
)
from app.domain.graph_runtime.identities import RuntimeUnitIdentity
from app.domain.operation_execution.checkpoint_lineage import OperationActivityAttempt
from app.domain.operation_execution.contracts import (
    DeepAgentExecutionBinding,
    OperationAttemptIdentity,
    OperationExecutionRequest,
    OperationExecutionResult,
    RuntimeInvocation,
    RuntimeResult,
    WorkspaceContract,
    WorkspaceOwner,
    WorkspaceOwnerKind,
    WorkspaceSlotBinding,
)
from app.domain.operation_execution.journal import (
    OperationClaimResult,
    OperationEffectClaim,
    OperationJournalSettlement,
)
from app.domain.run_control.contracts import (
    CommandResult,
    CommandStatus,
    LifecycleCommand,
    ObserveEffectAction,
    ReconcileUnitAction,
    ReserveBudgetAction,
    StartAction,
)
from app.domain.run_control.errors import IdempotencyConflict
from app.integrations.agents.deep_agents import (
    DeepAgentRuntimeAdapter,
    ExactComponentRegistry,
    ExactDeepAgentMaterializer,
    ResolvedSkillBundle,
    StateSandboxFactory,
)
from app.integrations.agents.deep_agents.checkpoint_verifier import (
    LangGraphCheckpointDescendantVerifier,
)
from app.integrations.artifact_payloads import InMemoryArtifactPayloadStore
from app.integrations.conformance_operation_runtime import (
    ConformanceAssetVerifier,
    ConformanceBudgetAuthority,
    ConformanceEventSink,
    ConformanceSandbox,
    ConformanceSecretResolver,
)
from tests.fixtures.checkpoint_lineage import bind_unit, stage_unit
from tests.unit.operations.test_operation_execution import (
    MCP_DIGEST,
    SKILL_DIGEST,
    operation_request,
)
from tests.unit.run_control.test_run_control import actor, command, reconciler_command
from tests.unit.run_control.test_run_control import request as run_request
from tests.unit.run_control.test_run_control import service as run_control_service

RESULT_MARKER = "RRM004-OK"
TOKENS_PER_CALL = 5


class SimulatedWorkerCrash(BaseException):  # noqa: N818 - a crash, not an error
    """A hard worker loss: nothing in the operation boundary handles or reports it."""


class ScriptedRecoveryModel(BaseChatModel):
    """Deterministic two-turn cognition: one `write_todos` tool call, then the answer.

    Every call is logged with the number of human and tool messages it observed, so tests
    assert model invocations, prompt (human-message) counts and tool executions directly.
    A call can fail like a provider, run a hook first, or block on a gate to model a slow
    (zombie) holder. Worker crashes are injected at the checkpointer (`CrashingSaver`):
    LangChain's model plumbing does not propagate a `BaseException` raised inside a call.
    """

    _log: list[tuple[int, int]] = PrivateAttr(default_factory=list)
    _fail_on: dict[int, Exception] = PrivateAttr(default_factory=dict)
    _before: dict[int, Callable[[], Awaitable[None]]] = PrivateAttr(default_factory=dict)
    _gate_on: int | None = PrivateAttr(default=None)
    _gate: asyncio.Event | None = PrivateAttr(default=None)
    _gate_entered: asyncio.Event | None = PrivateAttr(default=None)

    @property
    def _llm_type(self) -> str:
        return "rrm-004-scripted-recovery"

    @property
    def calls(self) -> list[tuple[int, int]]:
        return self._log

    def before_call(self, call: int, hook: Callable[[], Awaitable[None]]) -> None:
        self._before[call] = hook

    def fail_on(self, call: int, error: Exception) -> None:
        """An ordinary provider exception (not a worker loss) on the given call."""

        self._fail_on[call] = error

    def gate_on(self, call: int) -> tuple[asyncio.Event, asyncio.Event]:
        self._gate_on = call
        self._gate = asyncio.Event()
        self._gate_entered = asyncio.Event()
        return self._gate_entered, self._gate

    def bind_tools(
        self,
        tools: Sequence[BaseTool | dict[str, Any] | type | Any],
        *,
        tool_choice: str | None = None,
        **kwargs: Any,
    ) -> BaseChatModel:
        del tools, tool_choice, kwargs
        return self

    def _observe(self, messages: list[BaseMessage]) -> tuple[int, int]:
        human = sum(message.type == "human" for message in messages)
        tools = sum(isinstance(message, ToolMessage) for message in messages)
        self._log.append((human, tools))
        index = len(self._log)
        if index in self._fail_on:
            raise self._fail_on[index]
        return index, tools

    @staticmethod
    def _reply(tools: int) -> ChatResult:
        usage = {"input_tokens": 2, "output_tokens": 3, "total_tokens": TOKENS_PER_CALL}
        if tools == 0:
            message = AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "write_todos",
                        "args": {"todos": [{"content": "recover", "status": "completed"}]},
                        "id": "rrm004-write-todos",
                        "type": "tool_call",
                    }
                ],
                usage_metadata=usage,
            )
        else:
            message = AIMessage(
                content=json.dumps({"answer": RESULT_MARKER, "tool_results": tools}),
                usage_metadata=usage,
            )
        return ChatResult(generations=[ChatGeneration(message=message)])

    def _generate(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None = None,
        run_manager: Any = None,
        **kwargs: Any,
    ) -> ChatResult:
        del stop, run_manager, kwargs
        _index, tools = self._observe(messages)
        return self._reply(tools)

    async def _agenerate(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None = None,
        run_manager: Any = None,
        **kwargs: Any,
    ) -> ChatResult:
        del stop, run_manager, kwargs
        hook = self._before.get(len(self._log) + 1)
        if hook is not None:
            await hook()
        index, tools = self._observe(messages)
        if index == self._gate_on:
            assert self._gate is not None and self._gate_entered is not None
            self._gate_entered.set()
            await self._gate.wait()
        return self._reply(tools)


class CrashingSaver(InMemorySaver):
    """A checkpointer whose worker is lost right after its N-th checkpoint is durable.

    The checkpoint write completes, then the worker dies: every later write fails too, as
    if the process were gone, so nothing the crashed run would still write is persisted.
    """

    def __init__(self) -> None:
        super().__init__()
        self.crash_after_put: int | None = None
        self.puts = 0
        self._lost = False

    def crash_after(self, checkpoint_number: int) -> None:
        self.crash_after_put = checkpoint_number
        self.puts = 0
        self._lost = False

    def recover(self) -> None:
        """A new worker attaches to the same durable checkpoint store."""

        self.crash_after_put = None
        self._lost = False

    async def aput(self, config: Any, checkpoint: Any, metadata: Any, new_versions: Any) -> Any:
        if self._lost:
            raise SimulatedWorkerCrash("the worker is gone")
        written = await super().aput(config, checkpoint, metadata, new_versions)
        self.puts += 1
        if self.puts == self.crash_after_put:
            self._lost = True
            raise SimulatedWorkerCrash(f"worker lost after checkpoint {self.puts} was durable")
        return written

    async def aput_writes(
        self, config: Any, writes: Any, task_id: str, task_path: str = ""
    ) -> None:
        if self._lost:
            raise SimulatedWorkerCrash("the worker is gone")
        await super().aput_writes(config, writes, task_id, task_path)


class InjectingRuntime:
    """The runtime port with crash points around the real adapter."""

    def __init__(self, adapter: RuntimePort) -> None:
        self._adapter = adapter
        self.crash_before_invocation = False
        self.crash_after_invocation = False
        self.invocations = 0

    async def execute(self, invocation: RuntimeInvocation, secrets: Any) -> RuntimeResult:
        self.invocations += 1
        if self.crash_before_invocation:
            self.crash_before_invocation = False
            raise SimulatedWorkerCrash("worker lost before any checkpoint was written")
        result = await self._adapter.execute(invocation, secrets)
        if self.crash_after_invocation:
            self.crash_after_invocation = False
            raise SimulatedWorkerCrash("worker lost after the terminal checkpoint")
        return result


class CrashableRunControl:
    """Run control that can lose the worker after the result observation, mid-settlement."""

    def __init__(self, inner: RunControlService) -> None:
        self._inner = inner
        self.crash_before_settlement = False

    async def execute(self, command: LifecycleCommand) -> CommandResult:
        action = command.action
        if (
            self.crash_before_settlement
            and isinstance(action, ObserveEffectAction)
            and action.observation_id.startswith("observation:")
        ):
            self.crash_before_settlement = False
            raise SimulatedWorkerCrash("worker lost after observation, before settlement")
        return await self._inner.execute(command)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


class MemoryOperationJournal:
    """Atomic journal repository for unit tests: one claim and one settlement per effect."""

    async def record_reconciliation_applied(self, binding, decision) -> None:  # type: ignore[no-untyped-def]
        """RRM-007 receipt seam: the in-memory journal keeps no receipt ledger."""
        return None

    def __init__(self) -> None:
        self.claims: dict[str, OperationEffectClaim] = {}
        self.settlements: dict[str, OperationJournalSettlement] = {}
        self.technical_attempts: dict[str, list[int]] = {}

    async def commit(self, mutation: OperationJournalMutation) -> OperationClaimResult:
        claim_id = mutation.claim.effect_claim_id
        prior = self.claims.get(claim_id)
        if mutation.settlement is not None:
            existing = self.settlements.get(claim_id)
            if (
                existing is not None
                and existing.settlement_revision == mutation.settlement.settlement_revision
                and existing.settlement_digest != mutation.settlement.settlement_digest
            ):
                raise IdempotencyConflict("operation settlement replay conflicts")
            self.settlements[claim_id] = mutation.settlement
            if mutation.attempt is not None:
                self.technical_attempts.setdefault(claim_id, []).append(
                    mutation.attempt.technical_attempt
                )
            return OperationClaimResult(
                status="existing", claim=prior or mutation.claim, reason="settled"
            )
        if prior is not None:
            return OperationClaimResult(status="existing", claim=prior, reason="claim exists")
        self.claims[claim_id] = mutation.claim
        return OperationClaimResult(status="acquired", claim=mutation.claim, reason="acquired")

    async def get_claim(self, request_scope: str, effect_claim_id: str) -> OperationEffectClaim:
        del request_scope
        return self.claims[effect_claim_id]

    async def get_settlement(
        self, request_scope: str, effect_claim_id: str
    ) -> OperationJournalSettlement | None:
        del request_scope
        return self.settlements.get(effect_claim_id)


class MutableClock:
    def __init__(self) -> None:
        self.now = datetime.now(UTC)

    def __call__(self) -> datetime:
        return self.now

    def advance(self, delta: timedelta) -> None:
        self.now += delta


# --- REAL run-control operation authority (review fix 2) ---------------------------------

GOVERNED_WORKSPACE_CONTRACT = WorkflowWorkspaceContract(
    slots=(
        WorkspaceSlot(
            name="output",
            path="/workspace/output",
            access="exclusive_write",
            purpose="operation output",
        ),
    )
)


def governed_workspace(workspace: WorkspaceContract) -> WorkspaceContract:
    """The fixture workspace bound to the exact compiled workspace contract."""

    return workspace.model_copy(
        update={
            "workflow_contract_digest": sha256_digest(
                GOVERNED_WORKSPACE_CONTRACT.model_dump(mode="json")
            ),
            "slot_bindings": (
                WorkspaceSlotBinding(
                    slot_name="output",
                    logical_path="/workspace/output",
                    access="exclusive_write",
                    owner=WorkspaceOwner(kind=WorkspaceOwnerKind.STAGE, owner_id="stage:draft"),
                ),
            ),
        }
    )


class FixtureControlPlane:
    """The admitted run's exact effective configuration, as F1 would return it."""

    def __init__(self, request: OperationExecutionRequest) -> None:
        self._request = request

    async def retrieve_for_admission(self, digest: str) -> SimpleNamespace:
        del digest
        request = self._request
        return SimpleNamespace(
            effective_authority=SimpleNamespace(
                capabilities=request.capability_grant.capabilities
            ),
            source_refs=(
                request.workspace.template_ref,
                ExactDefinitionRef(
                    kind=DefinitionKind.PROMPT,
                    logical_id="system",
                    revision=1,
                    digest="sha256:" + "a" * 64,
                ),
            ),
            workflow_workspace_contract=GOVERNED_WORKSPACE_CONTRACT,
        )


def run_control_authority(run_control: RunControlService) -> RunControlOperationAuthority:
    return RunControlOperationAuthority(
        run_control,
        FixtureControlPlane(  # type: ignore[arg-type]
            operation_request().model_copy(
                update={"workspace": governed_workspace(operation_request().workspace)}
            )
        ),
    )


class AcceptingAuthority:
    async def verify(self, request: OperationExecutionRequest) -> None:
        del request

    async def verify_continuation(self, request: OperationExecutionRequest, binding: Any) -> None:
        del request, binding


@dataclass
class RecoveryHarness:
    run_control: RunControlService
    crashable: CrashableRunControl
    run_id: str
    journal: MemoryOperationJournal
    coordinator: JournaledOperationExecutionCoordinator
    lineage: InMemoryCheckpointLineageRepository
    clock: MutableClock
    saver: CrashingSaver
    model: ScriptedRecoveryModel
    runtime: InjectingRuntime
    service: OperationExecutionService
    reconciliation: UnitReconciliationService
    binding: DeepAgentExecutionBinding
    real_authority: bool = False
    _attempts: dict[str, int] = field(default_factory=dict)
    # RRM-006: the stores a fork-reuse resolver reads (same instances as the service's).
    repository: Any = None
    results: InMemoryArtifactPayloadStore | None = None
    bindings: InMemoryOperationBindingRepository | None = None

    async def request(self, unit: RuntimeUnitIdentity) -> OperationExecutionRequest:
        """Reserve a budget slice, then bind one Deep Agent unit at the current version."""

        run = await self.run_control.get_run("tenant-1", self.run_id)
        reservation_id = f"reservation:{unit.unit_key}"
        reserved = await self.run_control.execute(
            command(
                self.run_id,
                run.version,
                f"reserve:{unit.unit_key}",
                ReserveBudgetAction(reservation_id=reservation_id, amounts={"tokens.total": 10}),
            )
        )
        assert reserved.status == CommandStatus.ACCEPTED
        version = reserved.resulting_run_version
        workspace = (
            governed_workspace(self.binding.workspace)
            if self.real_authority
            else self.binding.workspace
        )
        deep_binding = bind_unit(
            self.binding,
            unit,
            control_revision=version,
            reservation_id=reservation_id,
            workspace=workspace,
        )
        return OperationExecutionRequest.model_validate(
            {
                **operation_request().model_dump(mode="python"),
                "workspace": workspace,
                "identity": OperationAttemptIdentity(
                    run_id=self.run_id,
                    operation_id=unit.semantic_operation_id,
                    operation_attempt=unit.semantic_attempt,
                ),
                "run_control_revision": version,
                "execution_runtime": "deep_agent",
                "native_placement": None,
                "deep_agent_binding": deep_binding,
                "runtime_unit": unit,
                "budget_reservation_id": reservation_id,
                "budget_limits": {"tokens.total": 10},
                "idempotency_key": f"rrm-004:{unit.unit_key}",
            }
        )

    def attempt(
        self, request: OperationExecutionRequest, *, lease: timedelta | None = None
    ) -> OperationActivityAttempt:
        """The next Temporal-shaped Activity attempt of the request's operation workflow."""

        workflow_id = f"operation/{request.identity.semantic_key}"
        number = self._attempts.get(workflow_id, 0) + 1
        self._attempts[workflow_id] = number
        return OperationActivityAttempt(
            workflow_id=workflow_id,
            workflow_run_id="temporal-run-1",
            activity_id="1",
            attempt=number,
            worker_identity=f"worker:rrm-004:{number}",
            lease_expires_at=self.clock() + lease if lease is not None else None,
        )

    async def crash(
        self, request: OperationExecutionRequest
    ) -> OperationActivityAttempt:
        """Run one attempt that is lost to an injected crash; then let its lease expire."""

        attempt = self.attempt(request)
        try:
            await self.service.execute(request, attempt)
        except SimulatedWorkerCrash:
            pass
        else:  # pragma: no cover - a crash window that did not crash is a harness bug
            raise AssertionError("the injected crash did not happen")
        self.saver.recover()
        self.clock.advance(timedelta(minutes=10))
        return attempt

    async def run(self, request: OperationExecutionRequest) -> OperationExecutionResult:
        return await self.service.execute(request, self.attempt(request))

    async def reconcile(
        self,
        request: OperationExecutionRequest,
        command_id: str,
        action: ReconcileUnitAction,
    ) -> CommandResult:
        run = await self.run_control.get_run("tenant-1", self.run_id)
        return await self.reconciliation.reconcile_unit(
            reconciler_command(self.run_id, run.version, command_id, action)
        )


async def recovery_harness(
    *,
    model: ScriptedRecoveryModel | None = None,
    real_authority: bool = False,
    fork_reuse: Any = None,
) -> RecoveryHarness:
    from tests.acceptance.control_plane.test_wp_cp_040 import exact_fixture

    run_control, repository = run_control_service()
    admitted = await run_control.admit(run_request(request_id="rrm-004-recovery"))
    assert admitted.run_id is not None
    started = await run_control.execute(
        command(admitted.run_id, 1, "rrm-004-start", StartAction())
    )
    assert started.status == CommandStatus.ACCEPTED
    binding, _profile, bundle = exact_fixture()
    model = model or ScriptedRecoveryModel()
    saver = CrashingSaver()
    runtime = InjectingRuntime(
        DeepAgentRuntimeAdapter(
            ExactDeepAgentMaterializer(_registry(binding, bundle, model, saver))
        )
    )
    crashable = CrashableRunControl(run_control)
    journal = MemoryOperationJournal()
    results = InMemoryArtifactPayloadStore()
    coordinator = JournaledOperationExecutionCoordinator(
        journal=OperationJournalService(journal),
        run_control=crashable,  # type: ignore[arg-type]
        results=results,
        actor=actor(),
    )
    clock = MutableClock()
    lineage = InMemoryCheckpointLineageRepository()
    assets = ConformanceAssetVerifier(
        mcp_schema_digests={"fixture-mcp": MCP_DIGEST},
        asset_manifest_digests={"skill:fixture.skill:1": SKILL_DIGEST},
    )
    bindings = InMemoryOperationBindingRepository()
    service = OperationExecutionService(
        authority=(
            run_control_authority(run_control) if real_authority else AcceptingAuthority()
        ),
        bindings=bindings,
        runtime=runtime,
        sandbox=ConformanceSandbox(),
        assets=assets,
        mcp=assets,
        secrets=ConformanceSecretResolver({"environment:OPENAI_API_KEY": "unused"}),
        events=ConformanceEventSink(),
        budget=ConformanceBudgetAuthority(),
        journal=coordinator,
        journal_claimed_by="worker:rrm-004",
        lineage=CheckpointLineageService(lineage, clock=clock),
        fork_reuse=fork_reuse,
    )
    return RecoveryHarness(
        run_control=run_control,
        crashable=crashable,
        run_id=admitted.run_id,
        journal=journal,
        coordinator=coordinator,
        lineage=lineage,
        clock=clock,
        saver=saver,
        model=model,
        runtime=runtime,
        service=service,
        reconciliation=UnitReconciliationService(
            run_control=run_control,
            lineage=lineage,
            verifier=LangGraphCheckpointDescendantVerifier(
                {binding.checkpointer_ref.digest: saver}
            ),
        ),
        binding=binding,
        real_authority=real_authority,
        repository=repository,
        results=results,
        bindings=bindings,
    )


def _registry(
    binding: DeepAgentExecutionBinding,
    bundle: ResolvedSkillBundle,
    model: BaseChatModel,
    saver: BaseCheckpointSaver[Any],
) -> ExactComponentRegistry:
    return ExactComponentRegistry(
        model_factories={binding.model.ref.digest: lambda _binding, _secrets: model},
        skill_bundles={bundle.bundle_digest: bundle},
        sandbox_factories={binding.sandbox.ref.digest: StateSandboxFactory()},
        checkpointers={binding.checkpointer_ref.digest: saver},
        stores={binding.store_ref.digest: InMemoryStore()},
    )


def stage_recovery_unit(run_id: str, name: str = "draft") -> RuntimeUnitIdentity:
    return stage_unit(
        request_scope="tenant-1",
        run_id=run_id,
        operation_id=(
            f"execution-epoch:1:stage:{name}:mapped:none:workflow-cycle:0:"
            "stage-cycle:0:slot:default"
        ),
        stage_id=name,
    )


def result_digest(result: OperationExecutionResult) -> str:
    """The recovered result's semantic digest: status, output, structured output, usage."""

    return sha256_digest(
        {
            "status": result.status,
            "output_text": result.output_text,
            "structured_output": result.structured_output,
            "usage": result.usage.model_dump(mode="json"),
        }
    )
