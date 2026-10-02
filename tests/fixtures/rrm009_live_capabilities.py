"""RRM-009 live capability binding: the pinned research capabilities on a real model.

One exact Deep Agent binding over the deployment pins (`infra/capability-pins/
research-capabilities.json`): the pinned OpenAI model definition, the pinned Tavily MCP
server with its exact five-tool filter, the pinned `agent-browser` Skill bundle, the pinned
`agent_browser_page` host tool, one in-process sync subagent and one async subagent hosted on
the BellLabs Agent Server. The operation's objective is a tiny technical procedure over
`example.com`; no company, fixture or research mission input (RRM-009 §2).

Environment (names only; never commit values): `BELLABS_RUN_RRM_009_LIVE=1`,
`OPENAI_API_KEY`, `TAVILY_API_KEY` (from `--env-file`), `AGENT_SERVER_ENDPOINT`,
`BELLABS_ASYNC_SUBAGENT_SERVER_TOKEN`, `TEST_APPLICATION_POSTGRES_DSN`, `TEST_MONGODB_URI`.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

from app.agent_server.async_subagents.bindings import technical_child_definition
from app.domain.control_plane.canonical import sha256_digest
from app.domain.control_plane.contracts import DefinitionKind, ExactDefinitionRef, SecretRef
from app.domain.operation_execution.contracts import (
    AsyncSubagentContract,
    AsyncSubagentDependencyClass,
    CapabilityGrant,
    CognitiveChannelPackRef,
    DeepAgentExecutionBinding,
    DeepAgentExecutionPlacementProfile,
    DeepAgentModelComponent,
    DeepAgentProfile,
    DeepAgentSandboxComponent,
    ImmutableAssetBinding,
    MCPServerBinding,
    OperationExecutionRequest,
    PromptSegment,
    PromptTrustClass,
    SyncSubagentProfile,
    WorkspaceContract,
)
from app.domain.operation_execution.materialization import compile_deep_agent_execution_binding
from app.integrations.capability_pins import CapabilityPins
from app.temporal.deployment_composition import DeploymentCapabilityComponents
from tests.acceptance.control_plane.test_wp_cp_040 import cognitive_schemas, exact
from tests.fixtures.rrm009_production_stack import (
    AGENT_COGNITIVE_QUEUE,
    CAPABILITIES,
    NODE_EXECUTABLE,
    REPORT_PATH,
)
from tests.unit.operations.test_operation_execution import operation_request

LIVE_FLAG = "BELLABS_RUN_RRM_009_LIVE"
TOKEN_ENV = "BELLABS_ASYNC_SUBAGENT_SERVER_TOKEN"
LIVE_MODEL = "gpt-5.6-luna"
LIVE_MODEL_SETTINGS: dict[str, object] = {
    "reasoning_effort": "low",
    "verbosity": "low",
    "use_responses_api": True,
    "max_completion_tokens": 2_000,
}
SYNC_CHILD = "technical-child"
ASYNC_CHILD = "async-child"
SYNC_CHILD_MARKER = "CHILD-OK"
ASYNC_CHILD_MARKER = "PONG"
PAGE_URL = "https://example.com/"
SEARCH_QUERY = "example.com reserved domain IANA"
NETWORK_HOSTS = frozenset({"api.tavily.com", "example.com"})
# Live ceilings: the technical run's ceilings sized for one real research-shaped operation
# plus its hosted child; the reservation below is the operation's hard bound.
LIVE_CEILINGS = {
    "tokens.total": 400_000,
    "model.turns": 60,
    "operation.attempts": 12,
    "goal.iterations": 6,
}
OPERATION_LIMITS = {"tokens.total": 300_000, "model.turns": 40}
ASYNC_CHILD_LIMITS = {"tokens.total": 8_000, "model.turns": 4}
LIVE_SECRET_REFS = (
    SecretRef(provider="environment", key="OPENAI_API_KEY"),
    SecretRef(provider="environment", key="TAVILY_API_KEY"),
    SecretRef(provider="environment", key=TOKEN_ENV),
)
OBJECTIVE = "\n".join(
    (
        "RRM-009 technical capability check. Do these steps in order, one tool call each:",
        "1. read_file /skills/agent-browser/SKILL.md (the attached browser Skill).",
        f'2. tavily_search with query "{SEARCH_QUERY}" and max_results 2.',
        f"3. agent_browser_page with url {PAGE_URL}",
        f'4. task with subagent_type "{SYNC_CHILD}" and description '
        f'"Reply with exactly {SYNC_CHILD_MARKER}".',
        f'5. start_async_task with subagent_type "{ASYNC_CHILD}" and description '
        f'"Reply with exactly the word {ASYNC_CHILD_MARKER}."',
        "6. check_async_task with that task_id; repeat at most 4 times while its status is "
        "running or pending.",
        f"7. write_file {REPORT_PATH} with three lines: "
        '"title: <page title from step 3>", "search: <first result URL from step 2>", '
        '"child: <reply from step 4>".',
        "Then answer with one line of JSON: "
        '{"title": ..., "search_url": ..., "child": ..., "async_status": ...}.',
    )
)


def live_opt_in() -> tuple[bool, str]:
    if os.getenv(LIVE_FLAG) != "1":
        return False, f"{LIVE_FLAG}=1 is required for the live capability qualification"
    for name in (
        "OPENAI_API_KEY",
        "TAVILY_API_KEY",
        "AGENT_SERVER_ENDPOINT",
        TOKEN_ENV,
        "TEST_APPLICATION_POSTGRES_DSN",
        "TEST_MONGODB_URI",
    ):
        if not os.getenv(name, "").strip():
            return False, f"{name} is required for the live capability qualification"
    if not NODE_EXECUTABLE.exists():
        return False, f"the pinned MCP servers and browser tool require node at {NODE_EXECUTABLE}"
    return True, ""


@dataclass(frozen=True)
class LiveBinding:
    binding: DeepAgentExecutionBinding
    pins: CapabilityPins
    async_contract: AsyncSubagentContract
    child_prompt_ref: ExactDefinitionRef

    def components(self) -> DeploymentCapabilityComponents:
        """Only the child prompt: every model, MCP server, Skill and tool is a pinned one."""

        return DeploymentCapabilityComponents(
            prompts={
                self.child_prompt_ref.digest: (
                    "You are a bounded in-process technical child. Reply with exactly the text "
                    "the task asks for and nothing else."
                )
            }
        )


def _model(pins: CapabilityPins) -> DeepAgentModelComponent:
    return DeepAgentModelComponent(
        ref=pins.models[0].ref.as_ref(),
        provider="openai",
        model_name=LIVE_MODEL,
        settings=dict(LIVE_MODEL_SETTINGS),
    )


def _grant() -> CapabilityGrant:
    return CapabilityGrant(
        capabilities=CAPABILITIES,
        tool_ids=frozenset({"agent_browser_page"}),
        mcp_server_ids=frozenset({"mcp.tavily"}),
        network_hosts=NETWORK_HOSTS,
    )


def live_binding(pins: CapabilityPins, *, endpoint: str) -> LiveBinding:
    """The exact binding of the pinned capabilities (compiled like the WP-CP-040 profile)."""

    request = operation_request()
    state_packs, context_pack, state_schema, context_schema = cognitive_schemas(
        with_child_slices=True
    )
    tavily = pins.mcp_server("mcp.tavily").component(node_executable=NODE_EXECUTABLE)
    skill = pins.skill("agent-browser")
    browser = pins.tool("agent_browser_page")
    child_prompt_ref = exact(DefinitionKind.PROMPT, "prompt.rrm009-live-child", "live-child")
    child = SyncSubagentProfile(
        name=SYNC_CHILD,
        description="An in-process technical child that replies with the exact requested text.",
        system_prompt_ref=child_prompt_ref,
        model=_model(pins),
        state_slice_id="child-state",
        context_slice_id="child-context",
        workspace_id="workspace-child",
        writable_paths=("/workspace/child",),
    )
    sandbox = pins.sandboxes[0]
    profile = DeepAgentProfile.create(
        logical_id="agent.rrm009-live-capabilities",
        revision=1,
        model=_model(pins),
        prompt_refs=(exact(DefinitionKind.PROMPT, "prompt.wp-cp-040", "prompt"),),
        context_assembly_ref=exact(
            DefinitionKind.WORKFLOW_CONFIGURATION, "context.wp-cp-040", "context-assembly"
        ),
        backend_ref=exact(DefinitionKind.RUNTIME_PROFILE, "backend.wp-cp-040", "backend"),
        store_ref=pins.stores[0].ref.as_ref(),
        checkpointer_ref=pins.checkpointers[0].ref.as_ref(),
        tools=(browser.component(),),
        mcp_servers=(tavily,),
        skills=(skill.component(),),
        sandbox=DeepAgentSandboxComponent(
            ref=sandbox.ref.as_ref(),
            backend=sandbox.backend,
            runtime_digest=sha256_digest(f"{sandbox.backend}-backend-runtime"),
        ),
        sync_subagents=(child,),
        tracing_policy_ref=exact(DefinitionKind.EVALUATION_PROFILE, "trace.wp-cp-040", "trace"),
        cognitive_state_pack_refs=tuple(
            CognitiveChannelPackRef(
                logical_id=pack.logical_id,
                revision=pack.revision,
                digest=pack.digest,
                contributor=pack.contributor,
            )
            for pack in state_packs
        ),
        cognitive_context_pack_refs=(
            CognitiveChannelPackRef(
                logical_id=context_pack.logical_id,
                revision=context_pack.revision,
                digest=context_pack.digest,
                contributor=context_pack.contributor,
            ),
        ),
        compatible_placement_ids=frozenset({"placement.rrm009-live"}),
    )
    placement = DeepAgentExecutionPlacementProfile.create(
        logical_id="placement.rrm009-live",
        revision=1,
        placement="local_in_worker",
        python_runtime="3.12",
        package_versions={"deepagents": "0.7.5"},
        task_queue=AGENT_COGNITIVE_QUEUE,
        checkpoint_behavior="local_checkpointer",
        cancellation_behavior="cooperative",
        streaming_behavior="state_updates",
        message_injection_behavior="invoke_only",
        reconnect_behavior="checkpoint_resume",
        sandbox_backends=frozenset({sandbox.backend}),
        qualification_refs=("QUAL-CP-DEEP-AGENT-MATERIALIZATION",),
    )
    compiled = compile_deep_agent_execution_binding(
        profile=profile,
        placement=placement,
        state_schema=state_schema,
        context_schema=context_schema,
        context_values={
            "run_id": request.identity.run_id,
            "operation_id": request.identity.operation_id,
            "operation_attempt": request.identity.operation_attempt,
            "execution_generation": 1,
            "capability_grant_ref": "ref:capability-grant:rrm009-live",
            "workspace_handle": "handle:workspace:rrm009-live",
        },
        run_id=request.identity.run_id,
        operation_id=request.identity.operation_id,
        operation_attempt=request.identity.operation_attempt,
        execution_generation=1,
        erc_digest=request.effective_configuration_digest,
        control_revision=request.run_control_revision,
        workspace=request.workspace,
        capability_grant=_grant(),
        reservation_id=request.budget_reservation_id,
        authority_refs=("authority:rrm009-live",),
        redaction_policy_ref=request.sensitive_data_policy_ref,
        initial_context_manifest={"digest": sha256_digest("context-manifest"), "entries": []},
    )
    contract = technical_child_definition().contract(
        agent_protocol_url=endpoint,
        name=ASYNC_CHILD,
        budget_limits=dict(ASYNC_CHILD_LIMITS),
        timeout_seconds=300,
        dependency_classes=frozenset({AsyncSubagentDependencyClass.REQUIRED_BLOCKING}),
    )
    # The hosted contract is added to the compiled binding exactly as RRM-013's live gate does.
    binding = DeepAgentExecutionBinding.create(
        **{
            **compiled.model_dump(mode="python", exclude={"binding_digest", "async_subagents"}),
            "async_subagents": (contract,),
        }
    )
    return LiveBinding(
        binding=binding, pins=pins, async_contract=contract, child_prompt_ref=child_prompt_ref
    )


def live_template(live: LiveBinding, workspace: WorkspaceContract) -> OperationExecutionRequest:
    """The operation request: pinned MCP and Skill bindings the worker verifies, the live
    objective, the credential references by name, and the operation's hard limits."""

    base = operation_request()
    tavily = live.pins.mcp_server("mcp.tavily")
    skill = live.pins.skill("agent-browser")
    grant: dict[str, Any] = _grant().model_dump(mode="python")
    return OperationExecutionRequest.model_validate(
        {
            **base.model_dump(mode="python"),
            "prompt_segments": (
                PromptSegment(
                    source_ref="input:rrm009-live@1",
                    source_revision=1,
                    trust_class=PromptTrustClass.ADMITTED_INPUT,
                    content=OBJECTIVE,
                    rendered_digest=sha256_digest(OBJECTIVE),
                ),
            ),
            "mcp_servers": (
                MCPServerBinding(
                    server_id=tavily.server_id,
                    revision=tavily.ref.revision,
                    transport="stdio",
                    endpoint_ref=tavily.module_locator,
                    allowed_tools=frozenset(tool.tool_name for tool in tavily.tools),
                    schema_digest=tavily.schema_digest,
                ),
            ),
            "skills": (
                ImmutableAssetBinding(
                    ref=skill.ref.as_ref(),
                    manifest_digest=skill.manifest_digest,
                    mount_path=skill.mount_root,
                ),
            ),
            "capability_grant": grant,
            "workspace": workspace,
            "execution_runtime": "deep_agent",
            "native_placement": None,
            "deep_agent_binding": DeepAgentExecutionBinding.create(
                **{
                    **live.binding.model_dump(mode="python", exclude={"binding_digest"}),
                    "workspace": workspace,
                    "capability_grant": grant,
                }
            ),
            "budget_limits": dict(OPERATION_LIMITS),
            "secret_refs": LIVE_SECRET_REFS,
            "output_schema": None,
        }
    )


__all__ = [
    "ASYNC_CHILD",
    "ASYNC_CHILD_MARKER",
    "LIVE_CEILINGS",
    "LIVE_FLAG",
    "OPERATION_LIMITS",
    "PAGE_URL",
    "SYNC_CHILD_MARKER",
    "TOKEN_ENV",
    "LiveBinding",
    "live_binding",
    "live_opt_in",
    "live_template",
]
