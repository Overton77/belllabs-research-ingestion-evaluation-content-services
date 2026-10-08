"""FT-B4 integration on the real local stack: compaction, seal, request_continuation, hydrate.

A compiled Deep Agents graph (offline scripted model) checkpoints through the real
``AsyncPostgresSaver`` in ``mission_control_runtime``; frames go to the PostgreSQL Native
Event Store; transfers, checkpoints, the continuation packet and the ``session.*`` events
go to the common component, all as restricted logins under forced RLS. Forced
summarization (``trigger=("messages", 3)``) yields both compaction frames;
``request_continuation`` (the command path's trigger) seals a valid checkpoint; the fresh
thread's first turn lists ``/inputs`` exactly as the snapshot manifest holds it.
"""

from __future__ import annotations

from typing import Any

import pytest
from deepagents.backends.utils import create_file_data
from langchain_core.messages import HumanMessage, ToolMessage
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from tests.fixtures.continuation import FakeStaging, NoArtifacts, StepClock, facts, seal_target
from tests.fixtures.continuation_graph import TASK, compaction_agent, stream_turn
from tests.fixtures.mission_control_common_db import CommonDatabase, provision_runtime
from tests.fixtures.provider_frames import StepClock as FrameClock
from tests.integration.postgres.frames_common import admit_unit_attempt
from tests.integration.postgres.runtime_common import common_db as common_db
from tests.integration.postgres.runtime_common import owner_rows
from tests.unit.run_control.test_run_control import ALL_PERMISSIONS, actor

from mission_control.adapters.deep_agents.compaction import (
    DeepAgentsSessionHydrator,
    StateBackendWorkspaceSnapshots,
)
from mission_control.adapters.deep_agents.frames import DeepAgentFrameRecorder
from mission_control.adapters.deep_agents.persistence import runtime_checkpoint_conninfo
from mission_control.adapters.postgres.context.continuation_repository import (
    PostgresCheckpointRepository,
    PostgresContinuationRepository,
    PostgresRunIds,
)
from mission_control.adapters.postgres.context.selection_repository import (
    PostgresContextSelectionRepository,
)
from mission_control.adapters.postgres.frames.repository import PostgresFrameRepository
from mission_control.application.context.continuation import (
    ContinuationCommands,
    ContinuationService,
    ContinuationTriggers,
    FrameHydrationConfirmation,
    FrameSessionLocator,
    InMemoryMailbox,
    RunControlContinuationEvents,
    TransferStatus,
)
from mission_control.application.context.pack_service import ContextPackService
from mission_control.application.frames.writer import FrameWriter
from mission_control.domain.context.checkpoint import CheckpointIdentities
from mission_control.domain.context.packet import PacketScope
from mission_control.domain.frames.contracts import FrameKind

pytestmark = pytest.mark.common_db

SAVER_SCHEMA = "mission_control_runtime"
MANIFEST = '{"records": 180, "source": "pubmed"}'


async def _turn(frames: Any, admitted: Any, agent: Any, invoke_input: Any, thread: str) -> Any:
    handle = await frames.open_execution(admitted.start(native_session_ref=thread))
    writer = FrameWriter(frames, handle, clock=FrameClock())
    recorder = DeepAgentFrameRecorder(writer, thread_id=thread, invocation_id=f"turn:{thread}")
    await recorder.begin(body={})
    result = await stream_turn(
        agent, invoke_input, {"configurable": {"thread_id": thread}}, recorder
    )
    messages = result.get("messages", [])
    await recorder.finish(
        messages=messages,
        own_messages=messages,
        output_text=str(messages[-1].content) if messages else "",
        structured_keys=(),
        checkpoint_id=None,
    )
    return result


def _scope_of(scope: str) -> PacketScope:
    parts = scope.split("/")
    return PacketScope(installation_id=parts[1], application_id=parts[2], tenant_id=parts[3])


@pytest.mark.asyncio
async def test_continuation_on_the_local_stack(common_db: CommonDatabase) -> None:
    checkpoint_dsn = await provision_runtime(common_db)
    pool = await common_db.pool(max_size=6)
    try:
        admitted = await admit_unit_attempt(pool, common_db)
        scope = admitted.unit.request_scope
        source = f"thread:{admitted.unit.unit_key}"
        frames = PostgresFrameRepository(pool)
        saver_dsn = runtime_checkpoint_conninfo(checkpoint_dsn, SAVER_SCHEMA)
        async with AsyncPostgresSaver.from_conn_string(saver_dsn) as saver:
            agent, middleware, backend, _model = compaction_agent(saver)
            await _turn(
                frames,
                admitted,
                agent,
                {
                    "messages": [HumanMessage(content=TASK)],
                    "files": {"/inputs/sources/source_manifest.json": create_file_data(MANIFEST)},
                },
                source,
            )
            run_ids = PostgresRunIds(pool)
            run_id = await run_ids(scope, admitted.run_key)
            assert run_id is not None
            stored_frames = await frames.frames_for_run(scope, run_id)
            kinds = [frame.kind for frame in stored_frames]
            assert FrameKind.BEFORE_COMPACTION in kinds and FrameKind.AFTER_COMPACTION in kinds
            assert middleware.observations

            staging = FakeStaging()
            selections = PostgresContextSelectionRepository(pool)
            transfers = PostgresContinuationRepository(pool)

            async def thread_values(thread: str) -> dict[str, Any]:
                state = await agent.aget_state({"configurable": {"thread_id": thread}})
                return dict(state.values)

            snapshots = StateBackendWorkspaceSnapshots(
                thread_values=thread_values, staging=staging, bytes_source=staging
            )
            mailbox = InMemoryMailbox(pending={admitted.run_key: ["cmd-held-1"]})
            events = RunControlContinuationEvents(
                admitted.run_control,
                actor=actor().model_copy(
                    update={"permissions": ALL_PERMISSIONS | {"workflow_run.record_continuation"}}
                ),
                clock=StepClock(),
            )
            service = ContinuationService(
                transfers=transfers,
                checkpoints=PostgresCheckpointRepository(pool),
                packets=ContextPackService(
                    artifacts=NoArtifacts(), selections=selections, staging=staging
                ),
                packet_reader=selections,
                snapshots=snapshots,
                events=events,
                mailbox=mailbox,
                confirmation=FrameHydrationConfirmation(frames, run_ids),
                clock=StepClock(),
            )
            commands = ContinuationCommands(
                ContinuationTriggers(transfers),
                FrameSessionLocator(frames, run_ids),
                request_scope=scope,
            )
            planned = await commands.plan(admitted.run_key, "cmd-continue-1", None)
            assert planned.source_session_ref == source
            await commands.record(admitted.run_key, planned, "cmd-continue-1")

            sealed = await service.seal(
                planned.transfer_id,
                facts(
                    scope=_scope_of(scope),
                    identities=CheckpointIdentities(
                        mission_id="mission-1",
                        run_id=admitted.run_key,
                        revision_id="rev-1",
                        node_key="collect",
                        activation_id="unit-collect-1",
                        logical_execution_id=planned.logical_execution_id,
                        source_agent_session_ref=source,
                    ),
                ),
                seal_target(),
                request_scope=scope,
            )
            assert sealed.checkpoint is not None and sealed.checkpoint.valid
            snapshot = await snapshots.load(
                request_scope=scope, snapshot_ref=sealed.checkpoint.workspace_snapshot_ref
            )
            assert snapshot is not None
            assert "/inputs/sources/source_manifest.json" in snapshot.manifest

            outcome = await service.transfer(
                planned.transfer_id,
                DeepAgentsSessionHydrator(graph=agent, backend=backend, bytes_source=staging),
                request_scope=scope,
            )
            target = outcome.transfer.target_session_ref
            assert outcome.transfer.status == TransferStatus.TRANSFERRED and target
            continued = await _turn(frames, admitted, agent, None, target)
            listing = next(
                message
                for message in continued["messages"]
                if isinstance(message, ToolMessage) and message.tool_call_id == "cont-ls"
            )
            expected_inputs = sorted(
                {
                    "/inputs/" + path.removeprefix("/inputs/").split("/", 1)[0]
                    for path in snapshot.manifest
                    if path.startswith("/inputs/")
                }
            )
            assert expected_inputs
            assert all(item in str(listing.content) for item in expected_inputs)
            released = await service.release_if_hydrated(planned.transfer_id, request_scope=scope)
            assert released.released
            assert mailbox.released[planned.transfer_id] == ("cmd-held-1",)

        rows = await owner_rows(
            common_db,
            """
            SELECT e.event_type FROM mission_control.mission_event e
            JOIN mission_control.mission_run r
              ON r.installation_id = e.installation_id AND r.application_id = e.application_id
             AND r.tenant_id = e.tenant_id AND r.run_id = e.run_id
            WHERE r.run_key = $1 AND e.event_type LIKE 'session.%'
            ORDER BY e.seq
            """,
            admitted.run_key,
        )
        assert [row["event_type"] for row in rows] == [
            "session.checkpoint_sealed",
            "session.transferred",
        ]
    finally:
        await pool.close()
