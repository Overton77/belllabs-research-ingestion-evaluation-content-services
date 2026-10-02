"""The exact BellLabs bindings the dedicated Agent Server hosts (REQ-CP-DA-019).

Every hosted graph is compiled from a `DeepAgentExecutionBinding` built here with the same
domain compiler the worker uses. Its graph ID, revision and binding digest are frozen into the
`AsyncSubagentContract` a parent spawns against, so the served identity the server reports
and stamps into thread state can be verified exactly. The definitions are import-safe: no
network client, model call or secret is touched at import.

The technical child is a mission-horizon fixture: a bounded subordinate with one exact tool
(`wait_seconds`, which gives the restart drills a window) and no company specifics.
"""

from __future__ import annotations

import asyncio
import importlib.metadata
from dataclasses import dataclass

from langchain_core.tools import BaseTool, tool

from app.domain.control_plane.canonical import sha256_digest
from app.domain.control_plane.contracts import DefinitionKind, ExactDefinitionRef
from app.domain.operation_execution.async_subagent_reconciliation import (
    AsyncServedGraphIdentity,
)
from app.domain.operation_execution.contracts import (
    AsyncSubagentContract,
    AsyncSubagentDependencyClass,
    CapabilityGrant,
    CognitiveChannelDefinition,
    CognitiveChannelPack,
    CognitiveChannelPackRef,
    CognitiveRuntimeContextPack,
    CognitiveRuntimeField,
    DeepAgentExecutionBinding,
    DeepAgentExecutionPlacementProfile,
    DeepAgentModelComponent,
    DeepAgentProfile,
    DeepAgentSandboxComponent,
    DeepAgentToolComponent,
    WorkspaceContract,
)
from app.domain.operation_execution.materialization import (
    compile_deep_agent_execution_binding,
    compose_cognitive_context_schema,
    compose_cognitive_state_schema,
)

TECHNICAL_CHILD_GRAPH_ID = "belllabs_async_technical_child"
TECHNICAL_CHILD_PROFILE_ID = "agent.async-technical-child"
TECHNICAL_CHILD_PROFILE_REVISION = 1
TECHNICAL_CHILD_PLACEMENT_ID = "placement.async-agent-server-local"
TECHNICAL_CHILD_MODEL = "gpt-5.6-luna"
TECHNICAL_CHILD_MODEL_SETTINGS: dict[str, object] = {
    "reasoning_effort": "low",
    "verbosity": "low",
    "use_responses_api": True,
    # The server streams the run; streamed usage is what makes the thread's final message
    # carry provider-attributed token counts (REQ-CP-DA-011).
    "stream_usage": True,
}
TECHNICAL_CHILD_SYSTEM_PROMPT = (
    "You are a bounded BellLabs technical child agent hosted on an Agent Server. "
    "Follow the parent's objective exactly and briefly. If the objective asks you to wait, "
    "call the wait_seconds tool once with the requested seconds, then answer. Reply with "
    "the exact text the objective requests and nothing else. You cannot access any "
    "BellLabs authority, budget or artifact store; your output is evidence the parent admits."
)
DEPLOYMENT_CREDENTIAL_REF = "environment:BELLABS_ASYNC_SUBAGENT_SERVER_TOKEN"
MAX_WAIT_SECONDS = 90


@tool
async def wait_seconds(seconds: int) -> str:
    """Wait for the given number of seconds (1 to 90), then confirm. Use it only when asked."""

    bounded = max(1, min(int(seconds), MAX_WAIT_SECONDS))
    await asyncio.sleep(bounded)
    return f"waited {bounded} seconds"


def _ref(kind: DefinitionKind, logical_id: str, seed: str) -> ExactDefinitionRef:
    return ExactDefinitionRef(
        kind=kind, logical_id=logical_id, revision=1, digest=sha256_digest(seed)
    )


def tool_schema_digest(item: BaseTool) -> str:
    schema_type = item.get_input_schema()
    schema = (
        schema_type.model_json_schema()
        if hasattr(schema_type, "model_json_schema")
        else schema_type.schema()
    )
    return sha256_digest(schema)


@dataclass(frozen=True)
class HostedAsyncSubagentDefinition:
    """One graph the server hosts: its exact binding, prompt, tools and served identity."""

    graph_id: str
    binding: DeepAgentExecutionBinding
    profile: DeepAgentProfile
    system_prompt: str
    tools: tuple[BaseTool, ...]

    @property
    def graph_revision(self) -> str:
        return f"{self.profile.logical_id}@{self.profile.revision}"

    @property
    def served(self) -> AsyncServedGraphIdentity:
        return AsyncServedGraphIdentity(
            graph_id=self.graph_id,
            graph_revision=self.graph_revision,
            graph_binding_digest=self.binding.binding_digest,
            deepagents_version=importlib.metadata.version("deepagents"),
        )

    def contract(
        self,
        *,
        agent_protocol_url: str,
        name: str = "technical-child",
        budget_limits: dict[str, int] | None = None,
        timeout_seconds: int = 600,
        dependency_classes: frozenset[AsyncSubagentDependencyClass] = frozenset(
            AsyncSubagentDependencyClass
        ),
    ) -> AsyncSubagentContract:
        """The immutable contract a parent binds to spawn this hosted graph (DA-019 fields)."""

        return AsyncSubagentContract.create(
            contract_id=f"{self.graph_id}:{self.graph_revision}",
            name=name,
            description=(
                "Bounded technical child hosted on the BellLabs async Agent Server; it "
                "follows a tiny objective and returns exact text."
            ),
            graph_id=self.graph_id,
            graph_revision=self.graph_revision,
            graph_binding_digest=self.binding.binding_digest,
            agent_protocol_url=agent_protocol_url,
            deployment_credential_ref=DEPLOYMENT_CREDENTIAL_REF,
            objective_schema_ref="schema:async-objective:text@1",
            result_schema_ref="schema:async-result:text@1",
            context_slice_id="child-context",
            state_slice_id="child-state",
            capability_ceiling=CapabilityGrant(
                capabilities=frozenset({"model.invoke"}), tool_ids=frozenset({"wait_seconds"})
            ),
            authority_refs=("authority:async-subagent-hosting",),
            budget_limits=budget_limits or {"tokens.total": 4_000, "model.turns": 4},
            dependency_classes=dependency_classes,
            timeout_seconds=timeout_seconds,
            cancellation_propagation="required",
            late_result_policy="quarantine",
            fallback_policy="degrade",
            result_admission_policy_ref="policy:async-result:technical-child@1",
        )


def technical_child_definition() -> HostedAsyncSubagentDefinition:
    """The exact hosted technical child, identical in every process that builds it."""

    state_pack = CognitiveChannelPack.create(
        logical_id="pack.belllabs.base-state",
        revision=1,
        contributor="base",
        channels=(
            CognitiveChannelDefinition(
                name="artifact_index",
                value_kind="map",
                value_schema_ref="schema:artifact-index@1",
                reducer="merge_by_key",
            ),
            CognitiveChannelDefinition(
                name="context_manifest",
                value_kind="object",
                value_schema_ref="schema:context-manifest@1",
                reducer="replace",
            ),
            CognitiveChannelDefinition(
                name="child_result_index",
                value_kind="append_list",
                value_schema_ref="schema:child-result-index@1",
                reducer="append_unique_by_id",
            ),
        ),
    )
    context_pack = CognitiveRuntimeContextPack.create(
        logical_id="pack.belllabs.async-child-context",
        revision=1,
        contributor="base",
        fields=(CognitiveRuntimeField(name="hosted_graph_id", value_kind="string"),),
    )
    state_schema = compose_cognitive_state_schema(
        schema_id="state.async-technical-child", packs=(state_pack,)
    )
    context_schema = compose_cognitive_context_schema(
        schema_id="context.async-technical-child", packs=(context_pack,)
    )
    model_ref = _ref(
        DefinitionKind.MODEL, "model.async-technical-child", "model.async-technical-child"
    )
    sandbox_ref = _ref(DefinitionKind.SANDBOX_PROFILE, "sandbox.async-state", "sandbox.async-state")
    tool_ref = _ref(DefinitionKind.TOOL, "tool.wait-seconds", "tool.wait-seconds")
    profile = DeepAgentProfile.create(
        logical_id=TECHNICAL_CHILD_PROFILE_ID,
        revision=TECHNICAL_CHILD_PROFILE_REVISION,
        model=DeepAgentModelComponent(
            ref=model_ref,
            provider="openai",
            model_name=TECHNICAL_CHILD_MODEL,
            settings=dict(TECHNICAL_CHILD_MODEL_SETTINGS),
        ),
        prompt_refs=(
            ExactDefinitionRef(
                kind=DefinitionKind.PROMPT,
                logical_id="prompt.async-technical-child",
                revision=1,
                digest=sha256_digest(TECHNICAL_CHILD_SYSTEM_PROMPT),
            ),
        ),
        context_assembly_ref=_ref(
            DefinitionKind.WORKFLOW_CONFIGURATION, "context.async-technical-child", "context"
        ),
        backend_ref=_ref(DefinitionKind.RUNTIME_PROFILE, "backend.async-state", "backend"),
        store_ref=_ref(DefinitionKind.MEMORY_POLICY, "store.agent-server-managed", "store"),
        checkpointer_ref=_ref(
            DefinitionKind.RUNTIME_PROFILE, "checkpointer.agent-server-managed", "checkpointer"
        ),
        tools=(
            DeepAgentToolComponent(
                ref=tool_ref,
                tool_name="wait_seconds",
                schema_digest=tool_schema_digest(wait_seconds),
                attachment_target="agent.main",
            ),
        ),
        sandbox=DeepAgentSandboxComponent(
            ref=sandbox_ref,
            backend="state",
            runtime_digest=sha256_digest("state-backend-runtime"),
        ),
        tracing_policy_ref=_ref(
            DefinitionKind.EVALUATION_PROFILE, "trace.async-technical-child", "trace"
        ),
        cognitive_state_pack_refs=(
            CognitiveChannelPackRef(
                logical_id=state_pack.logical_id,
                revision=state_pack.revision,
                digest=state_pack.digest,
                contributor=state_pack.contributor,
            ),
        ),
        cognitive_context_pack_refs=(
            CognitiveChannelPackRef(
                logical_id=context_pack.logical_id,
                revision=context_pack.revision,
                digest=context_pack.digest,
                contributor=context_pack.contributor,
            ),
        ),
        compatible_placement_ids=frozenset({TECHNICAL_CHILD_PLACEMENT_ID}),
    )
    placement = DeepAgentExecutionPlacementProfile.create(
        logical_id=TECHNICAL_CHILD_PLACEMENT_ID,
        revision=1,
        placement="remote_langsmith_deployment",
        python_runtime="3.12",
        package_versions={"deepagents": "0.7.5"},
        deployment_id="belllabs-async-agent-server-local",
        task_queue="agent-server-hosted",
        checkpoint_behavior="remote_managed",
        cancellation_behavior="remote_reconcile",
        streaming_behavior="state_updates",
        message_injection_behavior="remote_thread",
        reconnect_behavior="remote_run_reconnect",
        sandbox_backends=frozenset({"state"}),
        qualification_refs=("QUAL-CP-ASYNC-SUBAGENT-LIFECYCLE",),
    )
    digest = sha256_digest("async-technical-child-workspace")
    binding = compile_deep_agent_execution_binding(
        profile=profile,
        placement=placement,
        state_schema=state_schema,
        context_schema=context_schema,
        context_values={"hosted_graph_id": TECHNICAL_CHILD_GRAPH_ID},
        run_id="belllabs-async-hosting",
        operation_id=TECHNICAL_CHILD_GRAPH_ID,
        operation_attempt=1,
        execution_generation=1,
        erc_digest=sha256_digest(f"async-hosting:{TECHNICAL_CHILD_GRAPH_ID}"),
        control_revision=1,
        workspace=WorkspaceContract(
            namespace_id="workspace-namespace:async-technical-child",
            workspace_id="workspace:async-technical-child",
            provider="state-backend",
            template_ref=_ref(
                DefinitionKind.WORKSPACE_TEMPLATE, "workspace.async-technical-child", "workspace"
            ),
            exclusive_write_paths=("/workspace/child-output",),
            runtime_digest=digest,
            image_digest=digest,
            package_digest=digest,
            environment_digest=digest,
        ),
        capability_grant=CapabilityGrant(
            capabilities=frozenset({"model.invoke"}), tool_ids=frozenset({"wait_seconds"})
        ),
        reservation_id="reservation:async-hosting:none",
        authority_refs=("authority:async-subagent-hosting",),
        redaction_policy_ref="sensitive:redact@1",
        initial_context_manifest={"digest": sha256_digest("async-child-context"), "entries": []},
    )
    return HostedAsyncSubagentDefinition(
        graph_id=TECHNICAL_CHILD_GRAPH_ID,
        binding=binding,
        profile=profile,
        system_prompt=TECHNICAL_CHILD_SYSTEM_PROMPT,
        tools=(wait_seconds,),
    )


def hosted_definitions() -> tuple[HostedAsyncSubagentDefinition, ...]:
    """Every graph the dedicated server serves; the config file must list exactly these."""

    return (technical_child_definition(),)


def served_graph_identities() -> tuple[AsyncServedGraphIdentity, ...]:
    return tuple(definition.served for definition in hosted_definitions())
