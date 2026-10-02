"""RRM-009 live qualification: pinned research capabilities in the production composition.

Opt-in only (`BELLABS_RUN_RRM_009_LIVE=1`; see `tests/fixtures/rrm009_live_capabilities.py`
for the environment). The deployment's API and workers (`open_production_stack`, the same
composition as the technical qualification, with no deterministic component: every model,
MCP server, Skill and tool resolves from the pins) run one governed operation through
`POST /run-control/v1/runs/{run_id}/operations` → `GenericArtifactWorkflow` →
`operation.execute` → governed promotion. The real model reads the pinned Skill, searches
through the pinned Tavily MCP server, opens one public page with the pinned browser tool,
delegates to the in-process sync subagent and to the async subagent hosted on the BellLabs
Agent Server (scope-bound claim), and writes its report into the governed output slot. The
parent boundary admits and settles the async child; the sanitized lineage, the child's
authority rows and the run budget are read back from the stores. Tiny technical input only.
"""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast
from urllib.parse import urlsplit
from uuid import uuid4

import pytest

from app.application.async_subagents.mongo_async_subagent_repository import (
    MongoAsyncSubagentDetailRepository,
)
from app.application.async_subagents.postgres_async_subagents import PostgresAsyncSubagentAuthority
from app.config import PROJECT_ROOT
from app.domain.operation_execution.contracts import (
    ArtifactPromotionPlan,
    GenericArtifactWorkflowRequest,
    OperationAttemptIdentity,
    OperationExecutionRequest,
    WorkspaceOwner,
    WorkspaceOwnerKind,
)
from app.domain.run_control.contracts import ReserveBudgetAction, StartAction
from app.integrations.capability_pins import CapabilityPins
from app.temporal.deployment_composition import ASYNC_CHILD_COMPLETION_KIND
from tests.acceptance.control_plane.test_rrm_009_production_composition import (
    _admit,
    _command,
    _operation_payloads,
    _run,
    _send,
    mongo_database,  # noqa: F401 - the per-test Mongo database fixture
    open_production_stack,
)
from tests.fixtures.checkpoint_lineage import bind_unit, stage_unit
from tests.fixtures.rrm009_live_capabilities import (
    ASYNC_CHILD_MARKER,
    LIVE_CEILINGS,
    NETWORK_HOSTS,
    OPERATION_LIMITS,
    PAGE_URL,
    SYNC_CHILD_MARKER,
    TOKEN_ENV,
    live_binding,
    live_opt_in,
    live_template,
)
from tests.fixtures.rrm009_production_stack import (
    REPORT_PATH,
    SCOPE,
    _workspace,
    publish_technical_catalog,
    stage_workspace_contract,
    technical_binding,
)

PINS_PATH = PROJECT_ROOT / "infra" / "capability-pins" / "research-capabilities.json"
EXPECTED_KINDS = {"skill_read", "mcp", "tool", "sync_subagent", "async_subagent", "framework"}


def _lineage_and_completion(
    payloads: list[dict[str, Any]],
) -> tuple[dict[str, Any], dict[str, Any]]:
    (payload,) = payloads
    events = cast(list[dict[str, Any]], payload["event_payloads"])
    lineage = next(item["capability_lineage"] for item in events if "capability_lineage" in item)
    completion = next(item for item in events if item.get("kind") == ASYNC_CHILD_COMPLETION_KIND)
    return lineage, completion


@pytest.mark.asyncio
async def test_pinned_capabilities_and_both_subagents_run_in_the_production_composition(
    test_application_postgres_dsn: str,
    test_mongodb_uri: str,
    mongo_database: str,  # noqa: F811
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    enabled, reason = live_opt_in()
    if not enabled:
        pytest.skip(reason)
    pins = CapabilityPins.load(PINS_PATH)
    endpoint = os.environ["AGENT_SERVER_ENDPOINT"]
    live = live_binding(pins, endpoint=endpoint)
    async with open_production_stack(
        dsn=test_application_postgres_dsn,
        mongo_uri=test_mongodb_uri,
        mongo_database=mongo_database,
        root=tmp_path,
        monkeypatch=monkeypatch,
        technical=technical_binding(),
        components=live.components(),
        model_log=[],
        extra_environment={
            "ASYNC_SUBAGENT_SPAWNING_ENABLED": "true",
            "AGENT_SERVER_ENDPOINT": endpoint,
            "ASYNC_SUBAGENT_COMPLETION_WAIT_SECONDS": "240",
            "LANGSMITH_TRACING": "false",
        },
    ) as stack:
        catalog = await publish_technical_catalog(
            stack.control_plane, family="StageGraph", now=datetime.now(UTC), ceilings=LIVE_CEILINGS
        )
        run_id = await _admit(stack, catalog, f"rrm009-live-{uuid4().hex[:12]}")
        run = await _run(stack, run_id)
        started = await _send(
            stack,
            run_id,
            _command(
                run_id, run["version"], f"start:{run_id[:8]}", StartAction(), "workflow_run.start"
            ),
        )
        assert started["status"] == "accepted", started
        unit = stage_unit(
            request_scope=SCOPE,
            run_id=run_id,
            operation_id=(
                "execution-epoch:1:stage:report:mapped:none:workflow-cycle:0:stage-cycle:0:"
                "slot:default"
            ),
            stage_id="report",
        )
        reservation_id = f"reservation:{unit.unit_key}"
        run = await _run(stack, run_id)
        reserved = await _send(
            stack,
            run_id,
            _command(
                run_id,
                run["version"],
                f"reserve:{run_id[:8]}",
                ReserveBudgetAction(reservation_id=reservation_id, amounts=dict(OPERATION_LIMITS)),
                "workflow_run.reserve_budget",
            ),
        )
        assert reserved["status"] == "accepted", reserved
        workspace = _workspace(
            catalog.workspace_ref,
            namespace=f"workspace-namespace:{run_id}",
            workspace_id=f"workspace:{run_id}:report",
            contract=stage_workspace_contract(),
            owner=WorkspaceOwner(kind=WorkspaceOwnerKind.STAGE, owner_id="stage:report"),
        )
        template = live_template(live, workspace)
        revision = reserved["resulting_run_version"]
        operation = OperationExecutionRequest.model_validate(
            {
                **template.model_dump(mode="python"),
                "identity": OperationAttemptIdentity(
                    run_id=run_id,
                    operation_id=unit.semantic_operation_id,
                    operation_attempt=unit.semantic_attempt,
                ),
                "effective_configuration_digest": catalog.erc.digest,
                "run_control_revision": revision,
                "deep_agent_binding": bind_unit(
                    cast(Any, template.deep_agent_binding),
                    unit,
                    control_revision=revision,
                    reservation_id=reservation_id,
                    workspace=workspace,
                    erc_digest=catalog.erc.digest,
                ),
                "runtime_unit": unit,
                "budget_reservation_id": reservation_id,
                "requested_at": datetime.now(UTC),
                "idempotency_key": f"rrm009-live:{unit.unit_key}",
            }
        )
        submission = GenericArtifactWorkflowRequest(
            request_scope=SCOPE,
            run_id=run_id,
            operation=operation,
            promotion=ArtifactPromotionPlan(
                namespace_id=workspace.namespace_id,
                workspace_id=workspace.workspace_id,
                output_slot="output",
                logical_path=REPORT_PATH,
                owner=WorkspaceOwner(kind=WorkspaceOwnerKind.STAGE, owner_id="stage:report"),
                permission_ref="permission:rrm009",
                permission_outcome="allowed",
                output_contract_ref=operation.operation_contract_ref,
            ),
        )
        try:
            response = await stack.http.post(
                f"/run-control/v1/runs/{run_id}/operations",
                json=submission.model_dump(mode="json"),
            )
        except Exception:
            # Diagnostics only: the settlement row's typed failure (never provider text).
            async with stack.owner_pool.acquire() as connection:
                rows = await connection.fetch(
                    "SELECT status, failure_code, settlement_payload "
                    "FROM belllabs_control.operation_settlements"
                )
            print("RRM-009 LIVE FAILURE:", [dict(row) for row in rows])
            raise
        assert response.status_code == 201, response.text
        result = response.json()
        payloads = await _operation_payloads(stack, run_id)
        assert result["operation"]["status"] == "completed", (result, payloads)

        # Durable output: the report the agent wrote into its slot is a promoted artifact.
        artifact = result["artifact"]
        assert artifact["status"] == "admitted"
        report = (stack.payload_root / artifact["object_ref"].split("://")[1]).read_text(
            encoding="utf-8"
        )
        assert "Example Domain" in report and SYNC_CHILD_MARKER in report, report

        lineage, completion = _lineage_and_completion(payloads)
        # Every pinned capability was actually invoked by the bounded runtime agent.
        assert EXPECTED_KINDS <= set(lineage["invoked"]), lineage["invoked"]
        assert "tavily_search" in lineage["invoked"]["mcp"]
        assert lineage["invoked"]["tool"] == ["agent_browser_page"]
        assert {"start_async_task", "check_async_task"} <= set(lineage["invoked"]["async_subagent"])
        calls = cast(list[dict[str, Any]], lineage["invocations"])
        browser = [item for item in calls if item["tool_name"] == "agent_browser_page"]
        assert browser and all(item["status"] == "success" for item in browser), browser
        assert {urlsplit(str(item["requested_url"])).hostname for item in browser} == {
            urlsplit(PAGE_URL).hostname
        }
        searches = [item for item in calls if item["tool_name"] == "tavily_search"]
        assert searches and all(
            item["status"] == "success" and item["mcp_server"] == "tavily" for item in searches
        ), searches
        assert any(item["kind"] == "skill_read" for item in calls)
        # Disclosed and mounted exactly as pinned.
        skill = pins.skill("agent-browser")
        assert [item["bundle_digest"] for item in lineage["disclosed_skills"]] == [
            skill.bundle_digest
        ]
        (mounted_server,) = lineage["mounted"]["mcp_servers"]
        assert mounted_server["schema_digest"] == pins.mcp_server("mcp.tavily").schema_digest
        assert {
            item["tool_name"]: item["schema_digest"] for item in mounted_server["tool_filter"]
        } == {tool.tool_name: tool.schema_digest for tool in pins.mcp_server("mcp.tavily").tools}
        assert (
            lineage["mounted"]["tools"][0]["schema_digest"]
            == pins.tool("agent_browser_page").schema_digest
        )
        # Constrained egress: the grant the governed browser was bound by, as disclosed.
        assert lineage["mounted"]["capability_grant"]["network_hosts"] == sorted(NETWORK_HOSTS)
        assert (
            lineage["placement"]["checkpointer_ref"]["digest"] == pins.checkpointers[0].ref.digest
        )
        assert sorted(lineage["credential_refs"]) == sorted(
            {
                "environment:OPENAI_API_KEY",
                "environment:TAVILY_API_KEY",
                f"environment:{TOKEN_ENV}",
            }
        )
        # The sync child's model calls are charged to the operation.
        scopes = [item["scope"] for item in lineage["usage"]["model_calls"]]
        assert scopes.count("subordinate") >= 1, scopes
        assert lineage["usage"]["amounts"]["tokens.total"] > 0
        # The async child completed on the Agent Server, was admitted and settled at the boundary.
        (child_record,) = completion["children"]
        assert (child_record["lifecycle"], child_record["result_decision"]) == (
            "completed",
            "admit",
        ), child_record
        assert child_record["usage_disposition"] == "settled", child_record
        (child,) = await PostgresAsyncSubagentAuthority(stack.owner_pool).list_children(
            SCOPE, run_id
        )
        assert child.child_execution_id == child_record["child_execution_id"]
        assert child.result_decision == "admit" and child.settlement_ref is not None
        assert [item.disposition for item in child.provider_runs] == ["bound"]
        assert child.graph_binding_digest == live.async_contract.graph_binding_digest
        budget = (
            await stack.http.get(
                f"/run-control/v1/runs/{run_id}/budget", params={"request_scope": SCOPE}
            )
        ).json()
        child_usage = budget["usage_records"][f"async-child-usage:{child.child_execution_id}"]
        assert child_usage["actual_amounts"]["tokens.total"] > 0
        # Sanitized: no credential value reached the durable payloads or the lineage.
        durable = json.dumps(payloads)
        for name in ("OPENAI_API_KEY", "TAVILY_API_KEY", TOKEN_ENV):
            assert os.environ[name] not in durable, name
        detail = await MongoAsyncSubagentDetailRepository().get_execution(
            SCOPE, child.child_execution_id
        )
        assert ASYNC_CHILD_MARKER in (detail.result_output_text or ""), detail.result_output_text
        print(
            "RRM-009 LIVE EVIDENCE:",
            json.dumps(
                {
                    "run": run_id,
                    "artifact": {
                        "durable_reference": artifact["durable_reference"],
                        "content_digest": artifact["content_digest"],
                    },
                    "invoked": lineage["invoked"],
                    "invocations": [
                        {
                            key: item.get(key)
                            for key in (
                                "tool_name",
                                "kind",
                                "status",
                                "requested_url",
                                "mcp_server",
                                "arguments_digest",
                                "result_digest",
                            )
                        }
                        for item in calls
                    ],
                    "disclosed_skills": lineage["disclosed_skills"],
                    "credential_refs": lineage["credential_refs"],
                    "usage": lineage["usage"],
                    "async_child": {
                        **child_record,
                        "provider_run_id": child.provider_run_id,
                        "graph_binding_digest": child.graph_binding_digest,
                        "usage": child_usage["actual_amounts"],
                    },
                    "model": live.binding.model.model_name,
                },
                sort_keys=True,
                default=str,
            ),
        )
