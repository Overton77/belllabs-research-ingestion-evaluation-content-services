"""Real local production qualification of migrated artifact and subordinate persistence."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from dataclasses import replace
from pathlib import Path

import pytest
from temporalio.api.enums.v1 import EventType
from tests.fixtures import rrm009_cancellation
from tests.fixtures.mission_control_local_agent_server import (
    deterministic_child_definition,
    local_agent_server,
)
from tests.fixtures.mission_control_production_stack import open_postgres_production_stack
from tests.fixtures.rrm009_production_harness import (
    ProductionStack,
    _calls,
    _holds_wait,
    _release_wait,
    _run,
    _terminal,
    _wait_for,
    canonical_evidence,
    promote_generic_artifact,
    record_parity_trace,
)
from tests.fixtures.rrm009_production_stack import ANSWER_MARKER, SCOPE, technical_binding

from mission_control.adapters.postgres.async_subagents.async_subagent_detail_repository import (
    PostgresAsyncSubagentDetailRepository,
)
from mission_control.adapters.postgres.async_subagents.async_subagents import (
    PostgresAsyncSubagentAuthority,
)
from mission_control.domain.execution.contracts import (
    AsyncSubagentDependencyClass,
    AsyncSubagentLifecycle,
    DeepAgentExecutionBinding,
)

pytestmark = pytest.mark.common_db

_ARTIFACTS_OF_RUN = """
    SELECT count(*) FROM mission_control.artifact a
    JOIN mission_control.mission_run r
      ON (r.installation_id, r.application_id, r.tenant_id, r.run_id)
       = (a.installation_id, a.application_id, a.tenant_id, a.producer_run_id)
    WHERE r.run_key = $1
"""
_METADATA_REVISIONS = """
    SELECT count(*) FROM mission_control.artifact_metadata_revision
    WHERE request_scope = $1 AND artifact_key = $2
"""


@pytest.fixture
async def stack(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[ProductionStack]:
    async with open_postgres_production_stack(root=tmp_path, monkeypatch=monkeypatch) as production:
        yield production


async def test_generic_artifact_real_operation_and_promotion_retry(stack: ProductionStack) -> None:
    run_id, result = await promote_generic_artifact(stack)
    artifact = result["artifact"]
    object_path = stack.payload_root / artifact["object_ref"].split("://")[1]
    assert b"RRM-009 report" in object_path.read_bytes()
    assert ANSWER_MARKER in result["operation"]["output_text"]
    assert json.loads(result["operation"]["output_text"])["facts"] == {
        "child": True,
        "mcp": True,
    }
    assert list(_calls(stack, run_id).values()) == [{"parent": 4, "child": 1}]
    async with stack.owner_pool.acquire() as connection:
        assert await connection.fetchval(_ARTIFACTS_OF_RUN, run_id) == 1
        assert await connection.fetchval(_METADATA_REVISIONS, SCOPE, artifact["artifact_id"]) == 4

    # Re-run the actual activity with its recorded Temporal input after the workspace
    # manifest already links the promoted artifact. No mocked DB or candidate provider.
    promotion_payload = None
    async for execution in stack.client.list_workflows():
        history = await stack.client.get_workflow_handle(execution.id).fetch_history()
        for event in history.events:
            if event.event_type != EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:
                continue
            scheduled = event.activity_task_scheduled_event_attributes
            if scheduled.activity_type.name == "artifact.promote":
                promotion_payload = (
                    await stack.client.data_converter.decode(scheduled.input.payloads)
                )[0]
    assert promotion_payload is not None, "real artifact activity was not scheduled"
    assert stack.composition.artifacts is not None
    replayed = await stack.composition.artifacts.promote(promotion_payload)
    assert replayed == artifact
    assert list(_calls(stack, run_id).values()) == [{"parent": 4, "child": 1}]
    async with stack.owner_pool.acquire() as connection:
        assert await connection.fetchval(_METADATA_REVISIONS, SCOPE, artifact["artifact_id"]) == 4
        assert await connection.fetchval(_ARTIFACTS_OF_RUN, run_id) == 1
    rows = await canonical_evidence(stack, run_id, artifacts=True)
    assert rows["counts"]["artifact"] == 1 and rows["counts"]["settled_claims"] == 1, rows
    record_parity_trace("generic_artifact_promotion_retry", rows)
    print(
        "POSTGRES ARTIFACT EVIDENCE",
        json.dumps(
            {
                "run_id": run_id,
                "artifact_id": artifact["artifact_id"],
                "metadata_revisions": 4,
                "activity_replay": "same artifact, no duplicate revisions",
                "model_calls": _calls(stack, run_id),
            },
            sort_keys=True,
        ),
    )


def local_async_binding(endpoint: str):
    technical = technical_binding()
    contract = deterministic_child_definition().contract(
        agent_protocol_url=endpoint,
        name=rrm009_cancellation.ASYNC_CHILD_NAME,
        budget_limits=dict(rrm009_cancellation.ASYNC_CHILD_LIMITS),
        timeout_seconds=300,
        dependency_classes=frozenset({AsyncSubagentDependencyClass.REQUIRED_BLOCKING}),
    )
    binding = DeepAgentExecutionBinding.create(
        **{
            **technical.binding.model_dump(
                mode="python", exclude={"binding_digest", "async_subagents"}
            ),
            "async_subagents": (contract,),
        }
    )
    return replace(technical, binding=binding)


@pytest.mark.parametrize("window", ["cognition", "completion_wait"])
async def test_real_hosted_async_child_cancellation_uses_postgres_detail(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    window: str,
) -> None:
    async with local_agent_server(tmp_path / "agent-server", monkeypatch) as endpoint:
        technical = local_async_binding(endpoint)
        gate = rrm009_cancellation.CancellationGate(window)  # type: ignore[arg-type]
        model_log = []
        async with open_postgres_production_stack(
            root=tmp_path,
            monkeypatch=monkeypatch,
            technical_override=technical,
            components=rrm009_cancellation.cancellation_components(technical, gate, model_log),
            model_log=model_log,
            extra_environment={
                **rrm009_cancellation.CANCELLATION_ENVIRONMENT,
                "ASYNC_SUBAGENT_SPAWNING_ENABLED": "true",
                "AGENT_SERVER_ENDPOINT": endpoint,
                "ASYNC_SUBAGENT_COMPLETION_WAIT_SECONDS": "240",
            },
        ) as production:
            evidence = await rrm009_cancellation.run_cancellation_drill(production, technical, gate)
            assert evidence["child"]["cancellation_receipt"] == "provider_acknowledged"
            rows = await canonical_evidence(production, evidence["run_id"], children=True)
            assert rows["terminal_outcome"] == "cancelled", rows
            record_parity_trace(f"async_child_cancellation_{window}", rows)
            print("POSTGRES ASYNC CANCELLATION EVIDENCE", json.dumps(evidence, sort_keys=True))


async def test_real_hosted_async_children_complete_and_settle_in_postgres(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(rrm009_cancellation, "ASYNC_CHILD_OBJECTIVE", "Reply with exactly PONG.")
    async with local_agent_server(tmp_path / "agent-server", monkeypatch) as endpoint:
        technical = local_async_binding(endpoint)
        gate = rrm009_cancellation.CancellationGate("completion_wait")
        model_log = []
        async with open_postgres_production_stack(
            root=tmp_path,
            monkeypatch=monkeypatch,
            technical_override=technical,
            components=rrm009_cancellation.cancellation_components(technical, gate, model_log),
            model_log=model_log,
            extra_environment={
                **rrm009_cancellation.CANCELLATION_ENVIRONMENT,
                "ASYNC_SUBAGENT_SPAWNING_ENABLED": "true",
                "AGENT_SERVER_ENDPOINT": endpoint,
                "ASYNC_SUBAGENT_COMPLETION_WAIT_SECONDS": "120",
            },
        ) as production:
            run_id = await rrm009_cancellation.launch_stagegraph(
                production, technical, async_children=True
            )
            await _wait_for(production, run_id, lambda: _holds_wait(production, run_id), 180)
            await _release_wait(production, run_id)
            await _wait_for(production, run_id, lambda: _terminal(production, run_id), 180)
            assert (await _run(production, run_id))["terminal_outcome"] == "completed"
            children = await PostgresAsyncSubagentAuthority(production.worker_pool).list_children(
                SCOPE, run_id
            )
            assert len(children) == 2
            details = PostgresAsyncSubagentDetailRepository(production.worker_pool)
            for child in children:
                execution = await details.get_execution(SCOPE, child.child_execution_id)
                link = await details.get_link(SCOPE, child.child_execution_id)
                assert execution.lifecycle == AsyncSubagentLifecycle.COMPLETED
                assert link.result_decision == "admit" and link.settled
                assert link.usage_disposition == "settled"
                assert len(await rrm009_cancellation.provider_runs(child.child_execution_id)) == 1
            rows = await canonical_evidence(production, run_id, children=True)
            assert rows["terminal_outcome"] == "succeeded", rows
            assert rows["counts"]["subordinate_execution"] >= 2, rows
            record_parity_trace("async_children_completion", rows)
            print(
                "POSTGRES ASYNC COMPLETION EVIDENCE",
                json.dumps(
                    {
                        "run_id": run_id,
                        "children": len(children),
                        "result_decisions": "admit",
                        "usage_disposition": "settled",
                        "provider_runs_per_child": 1,
                    },
                    sort_keys=True,
                ),
            )
