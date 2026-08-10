from __future__ import annotations

import json
import os
from typing import Any, cast

import pytest
from temporalio import activity
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from app.application.operation_execution import bind_operation_execution_request
from app.config import Settings
from app.domain.control_plane.canonical import sha256_digest
from app.domain.control_plane.contracts import SecretRef
from app.domain.operation_execution.contracts import (
    MaterializedWorkspace,
    OperationExecutionRequest,
    OperationWorkflowRequest,
    RuntimeInvocation,
)
from app.integrations.agents.deep_agents import (
    DeepAgentRuntimeAdapter,
    ExactComponentRegistry,
    ExactDeepAgentMaterializer,
    LangSmithSandboxFactory,
    OpenAIExactModelFactory,
)
from app.temporal.workflows.operation import OperationWorkflow
from tests.acceptance.control_plane.test_wp_cp_040 import exact_fixture
from tests.test_operation_execution import operation_request


class QualificationActivities:
    def __init__(
        self,
        adapter: DeepAgentRuntimeAdapter,
        secrets: dict[str, str],
    ) -> None:
        self._adapter = adapter
        self._secrets = secrets

    @activity.defn(name="operation.execute")
    async def execute(self, payload: dict[str, Any]) -> dict[str, Any]:
        request = OperationExecutionRequest.model_validate(payload)
        binding = bind_operation_execution_request(request)
        invocation = RuntimeInvocation(
            binding=binding,
            prompt_segments=request.prompt_segments,
            workspace=MaterializedWorkspace(
                workspace_id=request.workspace.workspace_id,
                namespace_id=request.workspace.namespace_id,
                provider=request.workspace.provider,
                runtime_digest=request.workspace.runtime_digest,
                image_digest=request.workspace.image_digest,
                mount_manifest_digest=sha256_digest("wp-cp-040-live-mounts"),
            ),
            resolved_secret_names=tuple(sorted(self._secrets)),
        )
        result = await self._adapter.execute(invocation, self._secrets)
        return result.model_dump(mode="json")


@pytest.mark.asyncio
async def test_live_temporal_deep_agent_mcp_skill_and_langsmith_sandbox() -> None:
    if os.getenv("BELLABS_RUN_WP_CP_040_LIVE") != "1":
        pytest.skip("set BELLABS_RUN_WP_CP_040_LIVE=1 for external qualification")

    settings = Settings()
    openai_key = settings.openai_api_key.get_secret_value().strip()
    langsmith_key = (
        settings.langsmith_api_key.get_secret_value().strip()
        if settings.langsmith_api_key is not None
        else ""
    )
    if not openai_key or not langsmith_key:
        pytest.skip("OpenAI and LangSmith credentials are required")

    langsmith_ref = SecretRef(provider="environment", key="LANGSMITH_API_KEY")
    package_versions = {
        "deepagents": "0.7.5",
        "langchain": "1.3.14",
        "langchain-openai": "1.3.4",
        "langchain-mcp-adapters": "0.3.1",
        "langgraph": "1.2.10",
        "langsmith": "0.10.15",
        "temporalio": "1.30.0",
    }
    deep_binding, _profile, bundle = exact_fixture(
        include_mcp=True,
        model_name="gpt-5.6-luna",
        model_settings={
            "reasoning_effort": "low",
            "verbosity": "low",
            "use_responses_api": True,
        },
        sandbox_backend="langsmith",
        sandbox_credentials=(langsmith_ref,),
        package_versions=package_versions,
    )
    objective = (
        "Perform this exact qualification sequence before answering: "
        "(1) inspect the available Skill metadata, then read "
        "/skills/exact-binding-proof/SKILL.md with read_file; "
        "(2) call lookup_binding_marker with code LIVE040; "
        "(3) call execute with command `printf SANDBOX-LIVE-040`; "
        "(4) answer with the Skill proof marker, MCP result, and sandbox output."
    )
    base = operation_request(prompt=objective)
    payload = base.model_dump(mode="python")
    payload.update(
        execution_runtime="deep_agent",
        deep_agent_binding=deep_binding,
        secret_refs=(
            SecretRef(provider="environment", key="OPENAI_API_KEY"),
            langsmith_ref,
        ),
        budget_limits={"model.turns": 8, "tokens.total": 50_000},
    )
    request = OperationExecutionRequest.model_validate(payload)
    secrets = {
        "environment:OPENAI_API_KEY": openai_key,
        "environment:LANGSMITH_API_KEY": langsmith_key,
    }
    registry = ExactComponentRegistry(
        model_factories={deep_binding.model.ref.digest: OpenAIExactModelFactory()},
        skill_bundles={bundle.bundle_digest: bundle},
        sandbox_factories={deep_binding.sandbox.ref.digest: LangSmithSandboxFactory()},
        checkpointers={},
        stores={},
    )
    # The exact checkpointer/store digests are populated below without aliases.
    from langgraph.checkpoint.memory import InMemorySaver
    from langgraph.store.memory import InMemoryStore

    registry = ExactComponentRegistry(
        model_factories=registry.model_factories,
        skill_bundles=registry.skill_bundles,
        sandbox_factories=registry.sandbox_factories,
        checkpointers={deep_binding.checkpointer_ref.digest: InMemorySaver()},
        stores={deep_binding.store_ref.digest: InMemoryStore()},
    )
    activities = QualificationActivities(
        DeepAgentRuntimeAdapter(ExactDeepAgentMaterializer(registry)),
        secrets,
    )

    async with await WorkflowEnvironment.start_time_skipping() as environment:
        async with Worker(
            environment.client,
            task_queue="wp-cp-040-live",
            workflows=[OperationWorkflow],
            activities=[activities.execute],
        ):
            workflow_result = await environment.client.execute_workflow(
                OperationWorkflow.run,
                OperationWorkflowRequest(
                    semantic_attempt_id=request.identity.semantic_key,
                    operation_kind="bound_operation",
                    payload=request.model_dump(mode="json"),
                    task_queue="wp-cp-040-live",
                    timeout_seconds=300,
                ),
                id="qualification/wp-cp-040/live",
                task_queue="wp-cp-040-live",
            )

    runtime_result = cast(dict[str, Any], workflow_result.result or {})
    inspection = runtime_result["event_payloads"][0]
    assert inspection["skills_metadata"][0]["name"] == "exact-binding-proof"
    assert "SKILL-MD-IN-MESSAGES-040" in inspection["skill_instruction_messages"][0]["content"]
    assert inspection["mcp_tools_called"] == ["lookup_binding_marker"]
    assert inspection["sandbox_execute_called"] is True
    assert {"artifact_index", "context_manifest", "child_result_index"} <= set(
        inspection["state_keys"]
    )
    assert "SKILL-MD-IN-MESSAGES-040" in runtime_result["output_text"]
    assert "MCP-BOUND::LIVE040::EXACT" in runtime_result["output_text"]
    assert "SANDBOX-LIVE-040" in runtime_result["output_text"]

    print(
        "WP_CP_040_LIVE_EVIDENCE="
        + json.dumps(
            {
                "workflow_id": "qualification/wp-cp-040/live",
                "workflow_disposition": workflow_result.disposition,
                "model": deep_binding.model.model_name,
                "binding_id": deep_binding.binding_id,
                "binding_digest": deep_binding.binding_digest,
                "state_schema_digest": deep_binding.cognitive_state_schema.schema_digest,
                "context_schema_digest": deep_binding.cognitive_context_schema.schema_digest,
                "inspection": inspection,
                "output_text": runtime_result["output_text"],
            },
            sort_keys=True,
        )
    )
