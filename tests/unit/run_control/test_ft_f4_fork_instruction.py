"""FT-F4: fork from a Snapshot with a queued instruction on the facade, HTTP, CLI and MCP.

The real snapshot validator, semantic fork saga and in-memory run control; the forked Run's
own mailbox receives the instruction (its first entry) and the Snapshot restore, which the
first boundary seals into a `fork`-purpose packet as its single `workspace` item. The
source Run's mailbox, children and Commands are never copied. No provider is called.
"""

from __future__ import annotations

import json
from typing import Any
from uuid import uuid4

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from fastmcp import Client, Context
from langgraph.checkpoint.memory import InMemorySaver

from mission_control.adapters.storage.artifact_payloads import InMemoryArtifactPayloadStore
from mission_control.application.context.pack_service import ContextPackService
from mission_control.application.execution.boundary_interventions import BoundaryInterventionService
from mission_control.application.execution.mailbox import MailboxDeliveryService
from mission_control.application.execution.operations.checkpoint_lineage import (
    CheckpointLineageService,
    InMemoryCheckpointLineageRepository,
)
from mission_control.application.execution.operations.operation_execution import (
    InMemoryOperationBindingRepository,
)
from mission_control.application.execution.run_control_repository import (
    InMemoryRunControlRepository,
)
from mission_control.application.installations.registry import (
    ApplicationBinding,
    ApplicationRegistry,
    InstallationObservation,
)
from mission_control.application.missions.runtime import MissionControlRuntimeService
from mission_control.application.missions.service import MissionControlService
from mission_control.application.recovery.fork_seed import (
    FORK_SEED_ACTOR,
    ForkSeedService,
    seed_command_id,
)
from mission_control.application.recovery.run_forks import ForkPatchPolicyRegistry
from mission_control.contracts.contracts import (
    InlineText,
    MissionCommandRequest,
    MissionControlRejected,
)
from mission_control.contracts.identities import parse_request_scope
from mission_control.contracts.runtime_contracts import MissionForkRequest
from mission_control.domain.context.packet import ContextPurpose, ContextSourceKind, ExpansionTier
from mission_control.domain.policies.contracts import (
    ActorContext,
    AddContextAction,
    CommandStatus,
    LifecycleCommand,
)
from mission_control.domain.policies.forks import ForkRejected
from mission_control.domain.policies.mailbox import MailboxState
from mission_control.interfaces.cli.main import main
from mission_control.interfaces.http.mission_control import (
    MissionPrincipal,
    get_mission_principal,
    router,
)
from mission_control.interfaces.mcp.coordinator_server import (
    CoordinatorPrincipal,
    create_coordinator_server,
)
from mission_control.interfaces.mcp.run_control_tools import RUN_FORK_TOOL, ScopedRunControl
from tests.fixtures.checkpoint_recovery import (
    MemoryOperationJournal,
    recovery_harness,
    stage_recovery_unit,
)
from tests.fixtures.goal_directed_journaled import (
    SCOPE as GOAL_SCOPE,
)
from tests.fixtures.goal_directed_journaled import (
    GoalScriptedModel,
    admit_goal_run,
    compose_goal_directed,
    goal_blueprint,
    goal_run_control,
    goal_start_action,
)
from tests.fixtures.run_forks import (
    FakeForkSourceReader,
    compose_in_memory_forks,
    inspection_reads,
    stage_policy,
    stagegraph_head,
)
from tests.unit.coordinator.test_coordinator_mcp_read_surface import FakeFacade
from tests.unit.operations.test_ft_b2_stage_handoff import (
    FakeArtifacts,
    FakeSelections,
    FakeStaging,
)
from tests.unit.orchestration.test_rrm_016_goal_directed_settlement import _run_iteration_one
from tests.unit.run_control.test_run_control import WORKFLOW_DIGEST, actor
from tests.unit.run_control.test_run_control import command as lifecycle
from tests.unit.run_control.test_run_control import request as run_request

SCOPE = "mc/00000000-0000-7000-8000-0000000000aa/biotech/00000000-0000-7000-8000-0000000000bb"
GRANTS = {
    "sponsorship_refs": frozenset({"sponsorship:test"}),
    "approval_refs": frozenset({"approval:test"}),
}
INSTRUCTION = "Retry the synthesis with the stricter review rubric."


def caller() -> ActorContext:
    return actor().model_copy(
        update={
            "permissions": actor().permissions
            | {
                "workflow_run.snapshot",
                "workflow_run.fork",
                "workflow_run.read",
                "workflow_run.control",
            }
        }
    )


class World:
    """A settled StageGraph source run with fork authority, seeds and inspection."""

    async def build(self, request_scope: str = SCOPE) -> World:
        self.harness = await recovery_harness(request_scope=request_scope)
        unit = stage_recovery_unit(self.harness.run_id, "draft", request_scope=request_scope)
        result = await self.harness.run(await self.harness.request(unit))
        assert result.status == "completed"
        policies = ForkPatchPolicyRegistry()
        policies.register(WORKFLOW_DIGEST, stage_policy())
        self.forks = compose_in_memory_forks(
            self.harness.run_control,
            self.harness.repository,
            inspection_reads(self.harness.repository, self.harness.lineage, self.harness.journal),
            FakeForkSourceReader(
                self.harness.run_control,
                heads={self.harness.run_id: (stagegraph_head(stages={"draft": "completed"}),)},
            ),
            policies=policies,
        )
        self.scope = request_scope
        self.runtime = MissionControlRuntimeService(
            self.forks.snapshots,
            self.forks.forks,
            request_scope=request_scope,
            seeds=ForkSeedService(self.harness.run_control),
        )
        self.lifecycle = MissionControlService(
            self.harness.run_control,
            BoundaryInterventionService(self.harness.run_control),
            request_scope=request_scope,
            mailbox=MailboxDeliveryService(
                self.harness.repository.mailbox, self.harness.run_control
            ),
            forks=self.forks.store,
        )
        return self

    @property
    def run_id(self) -> str:
        return self.harness.run_id

    @property
    def repository(self) -> InMemoryRunControlRepository:
        return self.harness.repository

    def request(self, **overrides: Any) -> MissionForkRequest:
        values: dict[str, Any] = {
            "request_id": str(uuid4()),
            "baseline_reservations": {"tokens.total": 20},
            "sponsorship_ref": "sponsorship:test",
            "approval_refs": ["approval:test"],
            "instruction": {"text": INSTRUCTION},
            "reason": "explore a stricter review",
        }
        values.update(overrides)
        return MissionForkRequest.model_validate(values)


async def _counts(world: World, run_id: str) -> tuple[int, int, int]:
    return (
        len(await world.repository.mailbox.list_entries(world.scope, run_id)),
        len(await world.harness.run_control.list_boundary_commands(world.scope, run_id)),
        len((await world.harness.run_control.get_run(world.scope, run_id)).async_children),
    )


@pytest.mark.asyncio
async def test_fork_without_a_snapshot_takes_the_latest_safe_one_and_seeds_the_new_run() -> None:
    world = await World().build()
    # The source holds its own queued instruction: a fork never copies it.
    lifecycle_version = (await world.harness.run_control.get_run(world.scope, world.run_id)).version
    queued = await world.lifecycle.command(
        world.run_id,
        MissionCommandRequest.model_validate(
            {
                "request_id": str(uuid4()),
                "expected_version": lifecycle_version,
                "expected_generation": 1,
                "target": {"kind": "run", "id": world.run_id},
                "kind": "queue_instruction",
                "payload": {"content": {"text": "source-only note"}},
                "reason": "operator note on the source",
            }
        ),
        caller(),
    )
    assert queued.admission.status == CommandStatus.ACCEPTED
    before = await _counts(world, world.run_id)

    request = world.request()
    receipt = await world.runtime.fork(world.run_id, request, caller(), **GRANTS)

    assert receipt.seed is not None
    derived = receipt.receipt.target_run_id
    assert receipt.seed.derived_run_id == derived != world.run_id
    snapshot = await world.forks.snapshot_store.latest(world.scope, world.run_id)
    assert snapshot is not None and receipt.seed.snapshot_id == snapshot.snapshot_id
    assert receipt.receipt.snapshot_digest == snapshot.snapshot_digest
    instruction, workspace = await world.repository.mailbox.list_entries(world.scope, derived)
    # The instruction is the forked Run's first mailbox entry, queued, next_turn.
    assert (instruction.kind, instruction.admission_sequence, instruction.state) == (
        "queue_instruction",
        1,
        MailboxState.QUEUED,
    )
    assert (instruction.boundary, instruction.content_inline) == ("next_turn", INSTRUCTION)
    assert instruction.command_id == receipt.seed.instruction_command_id
    assert (workspace.kind, workspace.expand, workspace.admission_sequence) == (
        "add_context",
        "workspace",
        2,
    )
    assert workspace.content_digest == snapshot.snapshot_digest
    assert workspace.command_issuer.endswith(f'"{FORK_SEED_ACTOR.actor_id}"]')
    # Nothing of the source was copied or changed.
    assert await _counts(world, world.run_id) == before
    derived_commands = await world.harness.run_control.list_boundary_commands(world.scope, derived)
    assert {item.command.command_id for item in derived_commands} == {
        instruction.command_id,
        workspace.command_id,
    }
    assert (await world.harness.run_control.get_run(world.scope, derived)).async_children == ()

    # An identical retry returns the same forked Run and writes nothing twice.
    assert await world.runtime.fork(world.run_id, request, caller(), **GRANTS) == receipt
    assert len(await world.repository.mailbox.list_entries(world.scope, derived)) == 2

    # Lineage is visible from both Runs.
    source_view = await world.lifecycle.inspect(world.run_id, caller())
    derived_view = await world.lifecycle.inspect(derived, caller())
    assert source_view.lineage is not None and derived_view.lineage is not None
    (edge,) = source_view.lineage.forks
    assert derived_view.lineage.forked_from == edge
    assert (edge.source_run_id, edge.target_run_id, edge.snapshot_id) == (
        world.run_id,
        derived,
        snapshot.snapshot_id,
    )
    assert source_view.lineage.forked_from is None and derived_view.lineage.forks == ()


@pytest.mark.asyncio
async def test_fork_names_a_snapshot_or_reuses_the_latest_and_accepts_from_snapshot_id() -> None:
    world = await World().build()
    version = (await world.harness.run_control.get_run(world.scope, world.run_id)).version
    snapshot = await world.forks.snapshots.take(
        world.scope, world.run_id, expected_run_version=version
    )
    named = await world.runtime.fork(
        world.run_id,
        world.request(from_snapshot_id=snapshot.snapshot_id, instruction=None),
        caller(),
        **GRANTS,
    )
    assert named.receipt.snapshot_id == snapshot.snapshot_id
    assert named.seed is not None and named.seed.instruction_command_id is None
    (workspace,) = await world.repository.mailbox.list_entries(
        world.scope, named.receipt.target_run_id
    )
    assert workspace.expand == "workspace"
    latest = await world.runtime.fork(world.run_id, world.request(), caller(), **GRANTS)
    assert latest.receipt.snapshot_id == snapshot.snapshot_id


@pytest.mark.asyncio
async def test_a_run_without_a_safe_snapshot_is_checkpoint_invalid() -> None:
    world = await World().build()
    admitted = await world.harness.run_control.admit(
        run_request(request_scope=world.scope, request_id="never-started")
    )
    assert admitted.run_id is not None
    with pytest.raises(ForkRejected) as invalid:
        await world.runtime.fork(admitted.run_id, world.request(), caller(), **GRANTS)
    assert invalid.value.code == "CHECKPOINT_INVALID"


@pytest.mark.asyncio
async def test_instruction_needs_the_control_grant_and_respects_the_cap() -> None:
    world = await World().build()
    no_control = caller().model_copy(
        update={"permissions": caller().permissions - {"workflow_run.control"}}
    )
    with pytest.raises(MissionControlRejected) as denied:
        await world.runtime.fork(world.run_id, world.request(), no_control, **GRANTS)
    assert denied.value.code == "unauthorized"
    with pytest.raises(MissionControlRejected) as too_large:
        await world.runtime.fork(
            world.run_id, world.request(instruction={"text": "x" * 9_000}), caller(), **GRANTS
        )
    assert too_large.value.code == "content_too_large"
    # Nothing was forked by the refused requests.
    assert world.forks.store.materializations == {}


@pytest.mark.asyncio
async def test_the_forked_runs_first_packet_restores_the_snapshot_and_carries_the_instruction() -> (
    None
):
    """GoalDirected boundary: the seeded entries enter the executor's first packet, which is
    sealed with purpose `fork` and exactly one `workspace` item."""

    repository = InMemoryRunControlRepository()
    run_control = goal_run_control(repository)
    selections, staging = FakeSelections(), FakeStaging()
    packs = ContextPackService(
        artifacts=FakeArtifacts({}, scope=None), selections=selections, staging=staging
    )
    mailbox = MailboxDeliveryService(repository.mailbox, run_control)
    composition = await compose_goal_directed(
        run_control=run_control,
        journal=MemoryOperationJournal(),
        lineage=CheckpointLineageService(InMemoryCheckpointLineageRepository()),
        results=InMemoryArtifactPayloadStore(),
        bindings=InMemoryOperationBindingRepository(),
        saver=InMemorySaver(),
        model=GoalScriptedModel(),
        blueprint=goal_blueprint(),
        context_packs=packs,
        context_inputs=staging,
        mailbox=mailbox,
    )
    run_id = await admit_goal_run(run_control, "ft-f4-derived")
    started = await run_control.execute(
        lifecycle(run_id, 1, "ft-f4-start", goal_start_action(run_id))
    )
    assert started.status == CommandStatus.ACCEPTED
    # The seed exactly as ForkSeedService writes it on a derived run.
    lifecycle_service = MissionControlService(
        run_control,
        BoundaryInterventionService(run_control),
        request_scope=GOAL_SCOPE,
        mailbox=mailbox,
    )
    version = (await run_control.get_run(GOAL_SCOPE, run_id)).version
    await lifecycle_service.command(
        run_id,
        MissionCommandRequest.model_validate(
            {
                "request_id": seed_command_id(GOAL_SCOPE, "fork-1", "instruction"),
                "expected_version": version,
                "expected_generation": 1,
                "target": {"kind": "run", "id": run_id},
                "kind": "queue_instruction",
                "payload": {"content": {"text": INSTRUCTION}},
                "reason": "fork instruction",
            }
        ),
        caller(),
    )
    seeded = await run_control.execute(
        LifecycleCommand(
            command_id=seed_command_id(GOAL_SCOPE, "fork-1", "workspace"),
            idempotency_issuer='["mc.fork_seed.v1","mission-control-fork-seed"]',
            request_scope=GOAL_SCOPE,
            run_id=run_id,
            expected_run_version=version,
            actor=FORK_SEED_ACTOR,
            action=AddContextAction(
                generation=1,
                content_ref="sandbox-snapshot://run-1/snap-3",
                content_digest="sha256:" + "d" * 64,
                media_type="application/x-mission-control-run-snapshot",
                content_bytes=0,
                expand="workspace",
            ),
            reason="fork workspace restore",
            occurred_at=started.recorded_at,
            correlation_id="fork:fork-1",
        )
    )
    assert seeded.status == CommandStatus.ACCEPTED

    await _run_iteration_one(composition, run_id)

    executor = next(
        packet
        for packet, _selection in selections.rows.values()
        if packet.target.activation_id == "goal-iteration/1/executor"
    )
    verifier = next(
        packet
        for packet, _selection in selections.rows.values()
        if packet.target.activation_id == "goal-iteration/1/verifier"
    )
    assert executor.target.purpose == ContextPurpose.FORK
    assert verifier.target.purpose == ContextPurpose.ITERATION_START
    (restore,) = [item for item in executor.items if item.tier == ExpansionTier.WORKSPACE]
    assert executor.workspace_snapshot_ref == "sandbox-snapshot://run-1/snap-3"
    assert restore.mandatory
    (queued,) = [
        item for item in executor.items if item.source_kind == ContextSourceKind.QUEUED_INSTRUCTION
    ]
    assert queued.inline is not None and queued.inline.text == INSTRUCTION
    assert {entry.state for entry in await repository.mailbox.list_entries(GOAL_SCOPE, run_id)} == {
        MailboxState.CONSUMED
    }


# --- Interfaces: identical results on HTTP, CLI and MCP --------------------------------------


def _app(world: World, principal_actor: ActorContext) -> FastAPI:
    scope = parse_request_scope(world.scope)
    registry = ApplicationRegistry(
        (
            ApplicationBinding.seal(
                application_id="biotech",
                installation_id=scope.installation_id,
                binding_version="1",
                supabase_project_ref="project",
                database_secret_ref="TEST_DATABASE_URL",
                accepted_issuers={"https://issuer.invalid"},
                accepted_audiences={"authenticated"},
                required_component_version="1",
            ),
        )
    )
    registry.observe(
        "biotech",
        InstallationObservation(scope.installation_id, "biotech", "project", frozenset({"1"})),
    )
    app = FastAPI()
    app.include_router(router)
    app.state.mission_control_registry = registry
    key = (scope.installation_id, "biotech", scope.tenant_id)
    app.state.mission_control_services = {key: world.lifecycle}
    app.state.mission_control_runtime_services = {key: world.runtime}
    app.dependency_overrides[get_mission_principal] = lambda: MissionPrincipal(
        installation_id=scope.installation_id,
        application_id="biotech",
        tenant_id=scope.tenant_id,
        issuer="https://issuer.invalid",
        audiences={"authenticated"},
        actor=principal_actor,
        sponsorship_refs=GRANTS["sponsorship_refs"],
        approval_refs=GRANTS["approval_refs"],
    )
    return app


class Resolver:
    def __init__(self, permissions: frozenset[str]) -> None:
        self.principal = CoordinatorPrincipal(
            actor_id="operator",
            tenant_scope="tenant",
            roles=frozenset({"operator"}),
            permissions=permissions,
            request_scope=SCOPE,
            **GRANTS,
        )

    async def resolve(self, _context: Context) -> CoordinatorPrincipal:
        return self.principal


@pytest.mark.asyncio
async def test_http_cli_and_mcp_fork_with_an_instruction_identically(tmp_path, monkeypatch) -> None:
    world = await World().build()
    principal_actor = ActorContext(actor_id="operator", permissions=caller().permissions)
    app = _app(world, principal_actor)
    http = TestClient(app)
    request_id = str(uuid4())
    body = {
        "schema_version": "mc.runtime_fork.v1",
        "request_id": request_id,
        "baseline_reservations": {"tokens.total": 20},
        "instruction": {"text": INSTRUCTION},
        "sponsorship_ref": "sponsorship:test",
        "approval_refs": ["approval:test"],
        "reason": "explore a stricter review",
    }
    first = http.post(f"/v1/applications/biotech/runs/{world.run_id}/forks", json=body)
    assert first.status_code == 202, first.text
    http_receipt = {key: value for key, value in first.json().items() if key != "application_id"}
    assert http_receipt["seed"]["instruction_command_id"] is not None

    server = create_coordinator_server(
        FakeFacade(),
        Resolver(principal_actor.permissions),
        run_control=ScopedRunControl({SCOPE: world.lifecycle}, {SCOPE: world.runtime}),
    )
    async with Client(server) as mcp:
        result = await mcp.call_tool(RUN_FORK_TOOL, {"run_id": world.run_id, "request": body})
    envelope = result.structured_content
    assert envelope is not None and envelope["ok"] is True, envelope
    assert envelope["data"] == http_receipt

    instruction = tmp_path / "retry-with-tests.json"
    instruction.write_text(json.dumps({"text": INSTRUCTION}))
    sent: list[dict[str, Any]] = []

    def receive(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            sent.append(json.loads(request.content))
        forwarded = http.request(
            request.method,
            request.url.path,
            content=request.content,
            headers={key: value for key, value in request.headers.items() if key != "host"},
        )
        return httpx.Response(forwarded.status_code, content=forwarded.content)

    original = httpx.Client
    monkeypatch.setattr(
        httpx, "Client", lambda **kwargs: original(**kwargs, transport=httpx.MockTransport(receive))
    )
    monkeypatch.setenv("MISSION_CONTROL_TOKEN", "token")
    request_file = tmp_path / "fork.json"
    request_file.write_text(json.dumps({"baseline_reservations": {"tokens.total": 20}}))
    argv = [
        "run",
        "fork",
        world.run_id,
        "--request-file",
        str(request_file),
        "--instruction-file",
        str(instruction),
        "--sponsorship-ref",
        "sponsorship:test",
        "--approval-ref",
        "approval:test",
        "--reason",
        "explore a stricter review",
        "--request-id",
        request_id,
        "--application",
        "biotech",
        "--url",
        "http://127.0.0.1:8000",
    ]
    assert main(argv) == 0
    (cli_body,) = sent
    assert cli_body["instruction"] == {"text": INSTRUCTION}
    assert {key: cli_body[key] for key in body if key != "schema_version"} == {
        key: value for key, value in body.items() if key != "schema_version"
    }
    # One forked Run for the one request across the three surfaces.
    assert len(world.forks.store.materializations) == 1
    assert InlineText(text=INSTRUCTION).text == INSTRUCTION


@pytest.mark.asyncio
async def test_a_unit_the_fork_reuses_takes_no_queued_content() -> None:
    """Reused units settle by reference without a turn; the next executed unit takes it."""

    class Reused:
        def __init__(self, keys: set[str]) -> None:
            self.keys = keys

        async def reused(self, request_scope: str, run_id: str, unit_key: str) -> bool:
            return unit_key in self.keys

    world = await World().build()
    mailbox = MailboxDeliveryService(
        world.repository.mailbox, world.harness.run_control, reuse=Reused({"unit:reused"})
    )
    receipt = await world.runtime.fork(world.run_id, world.request(), caller(), **GRANTS)
    derived = receipt.receipt.target_run_id
    skipped = await mailbox.deliver(
        world.scope,
        derived,
        delivery_key="stagegraph:reused",
        family="StageGraph",
        node_key="draft",
        iteration_start=True,
        lane_profile="deep_agents",
        unit_key="unit:reused",
    )
    assert skipped == ()
    taken = await mailbox.deliver(
        world.scope,
        derived,
        delivery_key="stagegraph:executed",
        family="StageGraph",
        node_key="review",
        iteration_start=True,
        lane_profile="deep_agents",
        unit_key="unit:executed",
    )
    assert [entry.kind for entry in taken] == ["queue_instruction", "add_context"]
