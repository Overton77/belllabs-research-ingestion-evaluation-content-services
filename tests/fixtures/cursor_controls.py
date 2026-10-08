"""FT-G4 fixtures: a `cursor_local` lane over the replaying bridge, inside a real Run.

`control_stack` admits and starts a Run on the in-memory run-control service (real reducer,
receipt ledger and command mailbox), binds a `cursor_local` operation to one StageGraph unit
of it, and composes `LaneTurnService` with the mailbox and the interrupt_and_inject service,
so tests drive the SPEC-07 section 7 controls exactly as the worker does: commands are
admitted through `MissionControlService.command`, delivered at the boundary, consumed at the
send that carries them and settled with the turn. No Cursor agent is created.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

from tests.fixtures.checkpoint_recovery import stage_recovery_unit
from tests.fixtures.cursor_local import LocalStack, local_stack
from tests.fixtures.lane_turns import SCOPE, LaneStack, lane_stack
from tests.unit.run_control.test_run_control import actor as control_actor
from tests.unit.run_control.test_run_control import request as run_request
from tests.unit.run_control.test_run_control import service as run_control_service

from mission_control.adapters.operations.conformance import ConformanceRuntime
from mission_control.application.execution.boundary_interventions import BoundaryInterventionService
from mission_control.application.execution.harness.deep_agents_harness import DeepAgentsHarness
from mission_control.application.execution.harness.inject import (
    InjectionSettings,
    InterruptAndInjectService,
)
from mission_control.application.execution.harness.lane_turns import (
    LaneExecutionIdentity,
    LaneTurnService,
    harness_scope,
)
from mission_control.application.execution.harness.registry import LaneRegistry
from mission_control.application.execution.mailbox import MailboxDeliveryService
from mission_control.application.execution.service import RunControlService
from mission_control.application.missions.service import MissionControlService
from mission_control.contracts.contracts import MissionCommandRequest
from mission_control.domain.context.render import INPUTS_MANIFEST_PATH, bytes_digest
from mission_control.domain.execution.contracts import (
    OperationAttemptIdentity,
    OperationExecutionRequest,
    WorkspaceOwner,
    WorkspaceOwnerKind,
    WorkspaceSlotBinding,
)
from mission_control.domain.execution.lane_turns import LaneTurnRequest
from mission_control.domain.execution.lanes import PrepareRequest, SessionHandle, StartRequest
from mission_control.domain.policies.contracts import (
    ActorContext,
    CommandStatus,
    ExecutionTarget,
    LifecycleCommand,
    StartAction,
)
from mission_control.domain.policies.mailbox import MailboxEntry

FAST = InjectionSettings(poll_seconds=0.01, settle_grace_seconds=0.3, settle_poll_seconds=0.02)
RUN_UUID = UUID("55555555-5555-4555-8555-555555555555")
ACTIVATION_UUID = UUID("66666666-6666-4666-8666-666666666666")
TARGET = ExecutionTarget(
    family="StageGraph",
    family_workflow_id="family/run/g4",
    root_workflow_id="root/run/g4",
    execution_epoch=1,
)


def operator() -> ActorContext:
    source = control_actor()
    return source.model_copy(
        update={"permissions": source.permissions | {"workflow_run.read", "workflow_run.control"}}
    )


@dataclass
class ControlStack:
    # The lane stack under test: a `LocalStack` (cursor_local) or a `CloudStack` (cursor_cloud).
    local: Any
    lanes: LaneStack
    service: LaneTurnService
    run_control: RunControlService
    repository: Any
    mailbox: MailboxDeliveryService
    injections: InterruptAndInjectService
    facade: MissionControlService
    run_id: str
    operation: OperationExecutionRequest
    profile: str = "cursor_local"

    @property
    def identity(self) -> LaneExecutionIdentity:
        return LaneExecutionIdentity.of(self.operation, self.profile, 1)

    def turn(self, **changes: Any) -> LaneTurnRequest:
        return LaneTurnRequest.model_validate(
            {
                "operation": self.operation,
                "lane_profile": self.profile,
                "generation": 1,
                **changes,
            }
        )

    async def command(self, kind: str, text: str, **payload: Any) -> Any:
        run = await self.run_control.get_run(SCOPE, self.run_id)
        return await self.facade.command(
            self.run_id,
            MissionCommandRequest.model_validate(
                {
                    "request_id": str(uuid4()),
                    "expected_version": run.version,
                    "expected_generation": 1,
                    "target": {"kind": "run", "id": self.run_id},
                    "kind": kind,
                    "payload": {"content": {"text": text}, **payload},
                    "reason": "operator steering",
                }
            ),
            operator(),
        )

    async def status(self, receipt: Any) -> Any:
        return await self.run_control.get_boundary_command(
            SCOPE,
            self.run_id,
            receipt.delivery.command.idempotency_issuer,
            str(receipt.request_id),
        )

    async def deliver(self, delivery_key: str | None = None) -> tuple[MailboxEntry, ...]:
        """The family boundary's claim (FT-F1): the unit's next turn takes queued content."""

        unit = self.operation.runtime_unit
        assert unit is not None
        return await self.mailbox.deliver(
            SCOPE,
            self.run_id,
            delivery_key=delivery_key or self.operation.idempotency_key,
            family="StageGraph",
            node_key=str(unit.location.stage_id),
            iteration_start=False,
            lane_profile=self.profile,
        )

    async def entries(self) -> tuple[MailboxEntry, ...]:
        return await self.mailbox.list_entries(SCOPE, self.run_id)


async def _admitted_unit(
    declared_outputs: tuple[str, ...], operation_changes: Mapping[str, Any] | None
) -> tuple[RunControlService, Any, str, Any, dict[str, Any]]:
    """A started Run, one StageGraph unit of it, and the operation fields that bind it."""

    run_control, repository = run_control_service()
    admitted = await run_control.admit(
        run_request(request_scope=SCOPE, request_id=f"ft-g4-{uuid4()}")
    )
    assert admitted.run_id is not None
    run_id = admitted.run_id
    started = await run_control.execute(
        LifecycleCommand(
            command_id="ft-g4-start",
            idempotency_issuer="operator",
            request_scope=SCOPE,
            run_id=run_id,
            expected_run_version=1,
            actor=control_actor(),
            action=StartAction(execution_target=TARGET),
            reason="test start",
            occurred_at=admitted_at(),
            correlation_id="correlation-g4",
        )
    )
    assert started.status == CommandStatus.ACCEPTED
    unit = stage_recovery_unit(run_id, "implement", request_scope=SCOPE)
    changes: dict[str, Any] = {
        "identity": OperationAttemptIdentity(
            run_id=run_id,
            operation_id=unit.semantic_operation_id,
            operation_attempt=unit.semantic_attempt,
        ),
        "runtime_unit": unit,
        "idempotency_key": f"ft-g4:{unit.unit_key}",
    }
    if declared_outputs:
        changes["workspace"] = {
            **_base_workspace(),
            "exclusive_write_paths": tuple(f"/outputs/{name}" for name in declared_outputs),
        }
    changes.update(operation_changes or {})
    return run_control, repository, run_id, unit, changes


def _compose(
    stack: Any,
    harness: Any,
    frames: Any,
    operation: OperationExecutionRequest,
    *,
    run_control: RunControlService,
    repository: Any,
    run_id: str,
    unit: Any,
    settings: InjectionSettings,
    profile: str,
) -> ControlStack:
    lanes = lane_stack(harness, frames=frames, operation=operation)
    frames.register_run(SCOPE, run_id, RUN_UUID, {unit.unit_key: ACTIVATION_UUID})
    mailbox = MailboxDeliveryService(repository.mailbox, run_control)
    injections = InterruptAndInjectService(mailbox, settings=settings)
    service = LaneTurnService(
        lanes=LaneRegistry(
            [DeepAgentsHarness(ConformanceRuntime()), harness], allow_unqualified=True
        ),
        boundary=lanes.boundary,
        frames=frames,
        states=lanes.states,
        mailbox=mailbox,
        injections=injections,
    )
    facade = MissionControlService(
        run_control,
        BoundaryInterventionService(run_control),
        request_scope=SCOPE,
        mailbox=mailbox,
    )
    return ControlStack(
        local=stack,
        lanes=lanes,
        service=service,
        run_control=run_control,
        repository=repository,
        mailbox=mailbox,
        injections=injections,
        facade=facade,
        run_id=run_id,
        operation=operation,
        profile=profile,
    )


async def control_stack(
    tmp_path: Path,
    fixture: str = "full_run",
    *,
    launcher_changes: Mapping[str, Any] | None = None,
    declared_outputs: tuple[str, ...] = (),
    operation_changes: Mapping[str, Any] | None = None,
    settings: InjectionSettings = FAST,
) -> ControlStack:
    run_control, repository, run_id, unit, changes = await _admitted_unit(
        declared_outputs, operation_changes
    )
    local = local_stack(
        tmp_path, fixture, launcher_changes=launcher_changes, operation_changes=changes
    )
    return _compose(
        local,
        local.harness,
        local.frames,
        local.operation,
        run_control=run_control,
        repository=repository,
        run_id=run_id,
        unit=unit,
        settings=settings,
        profile="cursor_local",
    )


async def cloud_control_stack(
    tmp_path: Path,
    *,
    stream: str = "run_stream",
    record: str = "run_record",
    api_changes: Mapping[str, Any] | None = None,
    declared_outputs: tuple[str, ...] = (),
    operation_changes: Mapping[str, Any] | None = None,
    settings: InjectionSettings = FAST,
    inputs: Any = None,
    artifacts: Any = None,
) -> ControlStack:
    """The `cursor_cloud` lane (fake Cloud Agents API, bare remote) inside a real Run."""

    from tests.fixtures.cursor_cloud import cloud_stack

    from mission_control.application.frames.sink import InMemoryFrameStore

    run_control, repository, run_id, unit, changes = await _admitted_unit(
        declared_outputs, operation_changes
    )
    cloud = cloud_stack(
        tmp_path,
        stream=stream,
        record=record,
        api_changes=dict(api_changes or {}),
        operation_changes=changes,
        inputs=inputs,
        artifacts=artifacts,
    )
    return _compose(
        cloud,
        cloud.harness,
        InMemoryFrameStore(),
        cloud.operation,
        run_control=run_control,
        repository=repository,
        run_id=run_id,
        unit=unit,
        settings=settings,
        profile="cursor_cloud",
    )


# --- fork and continuation helpers --------------------------------------------------------------


class StaticInputs:
    """A `DurableInputReader` over fixed objects (the derived run's packet files)."""

    def __init__(self, objects: dict[str, bytes]) -> None:
        self.objects = objects

    async def retrieve(self, durable_ref: str) -> bytes:
        return self.objects[durable_ref]


def register_run(stack: LocalStack) -> None:
    """Record a plain (unit-less) operation's run and activation in the frame store."""

    operation = stack.operation
    stack.frames.register_run(
        SCOPE,
        operation.identity.run_id,
        RUN_UUID,
        {operation.identity.operation_id: ACTIVATION_UUID},
    )


def harness_fields(
    operation: OperationExecutionRequest, heid: str, profile: str | None = None
) -> dict[str, Any]:
    assert operation.cursor_binding is not None
    return {
        "scope": harness_scope(operation.request_scope),
        "lane_profile": profile or operation.cursor_binding.lane_profile,
        "harness_execution_id": heid,
        "binding_digest": operation.cursor_binding.binding_digest,
        "idempotency_key": f"{heid}:1:test",
        "generation": 1,
    }


async def started_session(stack: Any) -> tuple[str, SessionHandle]:
    """Stage, prepare and start the stack's operation directly on its harness (either lane)."""

    harness = stack.harness
    operation = stack.operation
    assert operation.cursor_binding is not None
    profile = operation.cursor_binding.lane_profile
    heid = str(LaneExecutionIdentity.of(operation, profile, 1).harness_execution_id)
    fields = harness_fields(operation, heid, profile)
    harness.stage(heid, operation)
    prepared = await harness.prepare(
        PrepareRequest(
            **fields,
            run_id=operation.identity.run_id,
            operation_id=operation.identity.operation_id,
            attempt_no=1,
        )
    )
    return heid, await harness.start(StartRequest(**fields, prepared=prepared))


def fork_stack_from(
    tmp_path: Path, source: LocalStack, snapshot_ref: str, *, agent_base: str
) -> tuple[LocalStack, bytes]:
    """The derived run of a fork: its first packet carries one `workspace` item naming
    `snapshot_ref` (restore `/`, FT-F4), served through the context input slot."""

    inputs_manifest = json.dumps(
        {
            "schema_version": "mc.mission_inputs.v1",
            "packet_digest": "sha256:" + "d" * 64,
            "inputs": [],
            "workspace": {"snapshot_ref": snapshot_ref, "restore_paths": ["/"]},
        }
    ).encode("utf-8")
    durable = "file-artifact://fork-inputs"
    owner = WorkspaceOwner(kind=WorkspaceOwnerKind.STAGE, owner_id="implement")
    workspace = {
        **source.operation.workspace.model_dump(mode="python"),
        "workflow_contract_digest": "sha256:" + "e" * 64,
        "slot_bindings": (
            WorkspaceSlotBinding(
                slot_name="ctx-mission-inputs",
                logical_path=f"/{INPUTS_MANIFEST_PATH}",
                access="read_only",
                owner=owner,
                durable_ref=durable,
                content_digest=bytes_digest(inputs_manifest),
            ),
            WorkspaceSlotBinding(
                slot_name="output",
                logical_path="/workspace/output",
                access="exclusive_write",
                owner=owner,
            ),
        ),
    }
    fork = local_stack(
        tmp_path,
        operation_changes={"workspace": workspace},
        artifacts=source.artifacts,
        inputs=StaticInputs({durable: inputs_manifest}),
        launcher_changes={"agent_base": agent_base},
    )
    return fork, inputs_manifest


def source_agent(stack: ControlStack) -> str:
    """The agent the stack's first turn runs on (either lane)."""

    if stack.profile == "cursor_cloud":
        return next(iter(stack.local.api.agents))
    return str(stack.local.launcher.meta["agent_id"])


async def seal_and_transfer(stack: ControlStack) -> tuple[dict[str, Any], Any]:
    """`request_continuation` through B4: request, seal from the live session's workspace
    (the lease, or the run branch), transfer with the lane's hydrator (a new agent)."""

    from tests.fixtures.continuation import CONT_SCOPE, build_service, facts, seal_target, trigger

    from mission_control.adapters.cursor.controls import (
        CursorCloudSessionHydrator,
        CursorCloudWorkspaceSnapshots,
        CursorSessionHydrator,
        CursorWorkspaceSnapshots,
    )
    from mission_control.domain.context.checkpoint import CheckpointIdentities

    harness = stack.local.harness
    artifacts = stack.local.artifacts
    agent = source_agent(stack)
    snapshots: Any
    hydrator: Any
    if stack.profile == "cursor_cloud":
        snapshots = CursorCloudWorkspaceSnapshots(harness, artifacts)
        hydrator = CursorCloudSessionHydrator(harness, artifacts)
    else:
        snapshots = CursorWorkspaceSnapshots(harness, artifacts)
        hydrator = CursorSessionHydrator(harness)
    wired = build_service(snapshots=snapshots)
    service = wired["service"]
    transfer = await service.request(
        trigger(),
        request_scope=CONT_SCOPE,
        run_key="run-continuation-1",
        activation_key="unit-collect-1",
        logical_execution_id="logical-collect-1",
        lane_profile=stack.profile,
        source_session_ref=agent,
    )
    base = facts()
    sealed = await service.seal(
        transfer.transfer_id,
        facts(
            lane_profile=stack.profile,
            identities=CheckpointIdentities(
                **{**base.identities.model_dump(), "source_agent_session_ref": agent}
            ),
        ),
        seal_target(),
        request_scope=CONT_SCOPE,
    )
    assert sealed.checkpoint is not None, sealed.transfer
    outcome = await service.transfer(transfer.transfer_id, hydrator, request_scope=CONT_SCOPE)
    return wired, outcome


def admitted_at() -> Any:
    from tests.unit.run_control.test_run_control import NOW

    return NOW + timedelta(minutes=1)


def _base_workspace() -> dict[str, Any]:
    from tests.fixtures.lane_turns import cursor_operation

    return cursor_operation().workspace.model_dump(mode="python")


def states(status: Any) -> list[str]:
    return [item.state.value for item in status.receipts]


__all__ = [
    "ACTIVATION_UUID",
    "FAST",
    "RUN_UUID",
    "ControlStack",
    "StaticInputs",
    "cloud_control_stack",
    "control_stack",
    "fork_stack_from",
    "harness_fields",
    "operator",
    "register_run",
    "seal_and_transfer",
    "source_agent",
    "started_session",
    "states",
]
