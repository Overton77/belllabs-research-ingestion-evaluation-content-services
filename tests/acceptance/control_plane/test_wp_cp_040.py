from __future__ import annotations

import inspect
import sys
from collections.abc import Sequence
from typing import Any, Literal

import pytest
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import AIMessage, BaseMessage, ToolMessage
from langchain_core.outputs import ChatGeneration, ChatResult
from langchain_core.tools import BaseTool
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.store.memory import InMemoryStore
from pydantic import ValidationError

from mission_control.adapters.deep_agents import (
    DeepAgentRuntimeAdapter,
    ExactComponentRegistry,
    ExactDeepAgentMaterializer,
    ResolvedSkillBundle,
    StateSandboxFactory,
)
from mission_control.application.execution.operations.checkpoint_lineage import (
    CheckpointLineageService,
    InMemoryCheckpointLineageRepository,
)
from mission_control.application.execution.operations.operation_execution import (
    bind_operation_execution_request,
)
from mission_control.domain.authoring.canonical import sha256_digest
from mission_control.domain.authoring.contracts import DefinitionKind, ExactDefinitionRef, SecretRef
from mission_control.domain.execution.checkpoint_lineage import (
    STAMP_BINDING_DIGEST,
    STAMP_EXECUTION_GENERATION,
    STAMP_INVOCATION_ID,
    STAMP_STATE_SCHEMA_DIGEST,
    STAMP_UNIT_KEY,
    CheckpointLineageInDoubt,
    IncompatibleCheckpointSchema,
)
from mission_control.domain.execution.contracts import (
    CapabilityGrant,
    CognitiveChannelDefinition,
    CognitiveChannelPack,
    CognitiveChannelPackRef,
    CognitiveRuntimeContextPack,
    CognitiveRuntimeField,
    DeepAgentExecutionBinding,
    DeepAgentExecutionPlacementProfile,
    DeepAgentMCPServerComponent,
    DeepAgentMCPToolComponent,
    DeepAgentModelComponent,
    DeepAgentProfile,
    DeepAgentSandboxComponent,
    DeepAgentSkillComponent,
    MaterializedWorkspace,
    OperationAttemptIdentity,
    OperationExecutionRequest,
    OperationWorkflowRequest,
    RuntimeInvocation,
    SubagentContextSlice,
    SubagentStateSlice,
    SyncSubagentProfile,
)
from mission_control.domain.execution.errors import (
    DeepAgentMaterializationError,
    DeepAgentRuntimeDrift,
)
from mission_control.domain.execution.materialization import (
    compile_deep_agent_execution_binding,
    compose_cognitive_context_schema,
    compose_cognitive_state_schema,
)
from tests.fixtures.checkpoint_lineage import (
    activity_attempt,
    bind_unit,
    goal_unit,
    stage_unit,
)
from tests.unit.operations.test_operation_execution import operation_request

DIGEST_A = "sha256:" + "a" * 64
MCP_TOOL_SCHEMA_DIGEST = "sha256:bb30ffeeaa9cc8d145c2160ac76df146df61820df24db1078c4db98573714a99"
SKILL_TEXT = """---
name: exact-binding-proof
description: Use for the WP-CP-040 exact binding proof.
---

Read this full instruction before acting. The proof marker is SKILL-MD-IN-MESSAGES-040.
"""


def exact(kind: DefinitionKind, logical_id: str, seed: str) -> ExactDefinitionRef:
    return ExactDefinitionRef(
        kind=kind,
        logical_id=logical_id,
        revision=1,
        digest=sha256_digest(seed),
    )


def skill_bundle() -> ResolvedSkillBundle:
    content = SKILL_TEXT.encode()
    manifest = [
        {
            "path": "SKILL.md",
            "digest": sha256_digest(SKILL_TEXT),
            "size_bytes": len(content),
        }
    ]
    return ResolvedSkillBundle(
        bundle_digest=sha256_digest(manifest),
        files=(("SKILL.md", content),),
    )


def cognitive_schemas(*, with_child_slices: bool = False):
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
    skills_pack = CognitiveChannelPack.create(
        logical_id="pack.deepagents.skills-middleware",
        revision=1,
        contributor="middleware",
        channels=(
            CognitiveChannelDefinition(
                name="skills_metadata",
                value_kind="append_list",
                value_schema_ref="schema:deepagents-skill-metadata@0.7.5",
                reducer="replace",
            ),
            CognitiveChannelDefinition(
                name="skills_load_errors",
                value_kind="string_list",
                value_schema_ref="schema:deepagents-skill-load-errors@0.7.5",
                reducer="replace",
            ),
        ),
    )
    context_pack = CognitiveRuntimeContextPack.create(
        logical_id="pack.belllabs.base-context",
        revision=1,
        contributor="base",
        fields=(
            CognitiveRuntimeField(name="run_id", value_kind="string"),
            CognitiveRuntimeField(name="operation_id", value_kind="string"),
            CognitiveRuntimeField(name="operation_attempt", value_kind="integer"),
            CognitiveRuntimeField(name="execution_generation", value_kind="integer"),
            CognitiveRuntimeField(
                name="capability_grant_ref",
                value_kind="string",
                reference_only=True,
            ),
            CognitiveRuntimeField(
                name="workspace_handle",
                value_kind="string",
                reference_only=True,
            ),
        ),
    )
    state_slices = (
        (
            SubagentStateSlice(
                slice_id="child-state",
                channel_names=frozenset({"artifact_index", "context_manifest"}),
            ),
        )
        if with_child_slices
        else ()
    )
    context_slices = (
        (
            SubagentContextSlice(
                slice_id="child-context",
                field_names=frozenset({"run_id", "operation_id", "workspace_handle"}),
            ),
        )
        if with_child_slices
        else ()
    )
    return (
        (state_pack, skills_pack),
        context_pack,
        compose_cognitive_state_schema(
            schema_id="state.wp-cp-040",
            packs=(state_pack, skills_pack),
            subagent_slices=state_slices,
        ),
        compose_cognitive_context_schema(
            schema_id="context.wp-cp-040",
            packs=(context_pack,),
            subagent_slices=context_slices,
        ),
    )


def exact_fixture(
    *,
    include_mcp: bool = False,
    package_versions: dict[str, str] | None = None,
    sync_subagents: tuple[SyncSubagentProfile, ...] = (),
    with_child_slices: bool = False,
    model_name: str = "fixture-model",
    model_settings: dict[str, object] | None = None,
    sandbox_backend: Literal["langsmith", "daytona", "docker", "state"] = "state",
    sandbox_credentials: tuple[SecretRef, ...] = (),
) -> tuple[DeepAgentExecutionBinding, DeepAgentProfile, ResolvedSkillBundle]:
    request = operation_request()
    state_packs, context_pack, state_schema, context_schema = cognitive_schemas(
        with_child_slices=with_child_slices
    )
    model_ref = exact(DefinitionKind.MODEL, "model.wp-cp-040", "model")
    sandbox_ref = exact(DefinitionKind.SANDBOX_PROFILE, "sandbox.wp-cp-040", "sandbox")
    skill_ref = exact(DefinitionKind.SKILL, "skill.wp-cp-040", "skill")
    bundle = skill_bundle()
    mcp_servers: tuple[DeepAgentMCPServerComponent, ...] = ()
    if include_mcp:
        mcp_servers = (
            DeepAgentMCPServerComponent(
                ref=exact(DefinitionKind.MCP_SERVER, "mcp.wp-cp-040", "mcp"),
                server_name="qualification",
                transport="stdio",
                command=sys.executable,
                arguments=(
                    "-m",
                    "mission_control.adapters.agent_server.qualification.wp_cp_040_mcp",
                ),
                tools=(
                    DeepAgentMCPToolComponent(
                        tool_name="lookup_binding_marker",
                        schema_digest=MCP_TOOL_SCHEMA_DIGEST,
                    ),
                ),
                schema_digest=sha256_digest("qualification-mcp-schema-v1"),
                attachment_target="agent.main",
            ),
        )
    profile = DeepAgentProfile.create(
        logical_id="agent.wp-cp-040",
        revision=1,
        model=DeepAgentModelComponent(
            ref=model_ref,
            provider="openai",
            model_name=model_name,
            settings=model_settings or {},
        ),
        prompt_refs=(exact(DefinitionKind.PROMPT, "prompt.wp-cp-040", "prompt"),),
        context_assembly_ref=exact(
            DefinitionKind.WORKFLOW_CONFIGURATION,
            "context.wp-cp-040",
            "context-assembly",
        ),
        backend_ref=exact(DefinitionKind.RUNTIME_PROFILE, "backend.wp-cp-040", "backend"),
        store_ref=exact(DefinitionKind.MEMORY_POLICY, "store.wp-cp-040", "store"),
        checkpointer_ref=exact(
            DefinitionKind.RUNTIME_PROFILE,
            "checkpointer.wp-cp-040",
            "checkpointer",
        ),
        mcp_servers=mcp_servers,
        skills=(
            DeepAgentSkillComponent(
                ref=skill_ref,
                skill_name="exact-binding-proof",
                bundle_digest=bundle.bundle_digest,
                skill_md_digest=sha256_digest(SKILL_TEXT),
                mount_root="/skills/exact-binding-proof",
                attachment_target="agent.main",
            ),
        ),
        sandbox=DeepAgentSandboxComponent(
            ref=sandbox_ref,
            backend=sandbox_backend,
            runtime_digest=sha256_digest(f"{sandbox_backend}-backend-runtime"),
            credential_refs=sandbox_credentials,
        ),
        sync_subagents=sync_subagents,
        tracing_policy_ref=exact(
            DefinitionKind.EVALUATION_PROFILE,
            "trace.wp-cp-040",
            "trace",
        ),
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
        compatible_placement_ids=frozenset({"placement.wp-cp-040"}),
    )
    placement = DeepAgentExecutionPlacementProfile.create(
        logical_id="placement.wp-cp-040",
        revision=1,
        placement="local_in_worker",
        python_runtime="3.12",
        package_versions=package_versions or {"deepagents": "0.7.5"},
        task_queue="agent-cognitive",
        checkpoint_behavior="local_checkpointer",
        cancellation_behavior="cooperative",
        streaming_behavior="state_updates",
        message_injection_behavior="invoke_only",
        reconnect_behavior="checkpoint_resume",
        sandbox_backends=frozenset({sandbox_backend}),
        qualification_refs=("QUAL-CP-DEEP-AGENT-MATERIALIZATION",),
    )
    binding = compile_deep_agent_execution_binding(
        profile=profile,
        placement=placement,
        state_schema=state_schema,
        context_schema=context_schema,
        context_values={
            "run_id": request.identity.run_id,
            "operation_id": request.identity.operation_id,
            "operation_attempt": request.identity.operation_attempt,
            "execution_generation": 1,
            "capability_grant_ref": "ref:capability-grant:wp-cp-040",
            "workspace_handle": "handle:workspace:wp-cp-040",
        },
        run_id=request.identity.run_id,
        operation_id=request.identity.operation_id,
        operation_attempt=request.identity.operation_attempt,
        execution_generation=1,
        erc_digest=request.effective_configuration_digest,
        control_revision=request.run_control_revision,
        workspace=request.workspace,
        capability_grant=request.capability_grant,
        reservation_id=request.budget_reservation_id,
        authority_refs=("authority:wp-cp-040",),
        redaction_policy_ref=request.sensitive_data_policy_ref,
        initial_context_manifest={"digest": sha256_digest("context-manifest"), "entries": []},
    )
    return binding, profile, bundle


class SkillReadingModel(BaseChatModel):
    calls: int = 0

    @property
    def _llm_type(self) -> str:
        return "wp-cp-040-scripted"

    def bind_tools(
        self,
        tools: Sequence[BaseTool | dict[str, Any] | type | Any],
        *,
        tool_choice: str | None = None,
        **kwargs: Any,
    ) -> BaseChatModel:
        del tools, tool_choice, kwargs
        return self

    def _generate(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None = None,
        run_manager: Any = None,
        **kwargs: Any,
    ) -> ChatResult:
        del stop, run_manager, kwargs
        self.calls += 1
        read = next((item for item in messages if isinstance(item, ToolMessage)), None)
        if read is None:
            message = AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "read_file",
                        "args": {
                            "file_path": "/skills/exact-binding-proof/SKILL.md",
                            "limit": 1000,
                        },
                        "id": "skill-read-040",
                        "type": "tool_call",
                    }
                ],
            )
        else:
            message = AIMessage(content="SKILL-MD-IN-MESSAGES-040 observed and followed.")
        return ChatResult(generations=[ChatGeneration(message=message)])


class SessionProbeModel(BaseChatModel):
    observed_human_counts: list[int] = []

    @property
    def _llm_type(self) -> str:
        return "wp-cp-040-session-probe"

    def bind_tools(
        self,
        tools: Sequence[BaseTool | dict[str, Any] | type | Any],
        *,
        tool_choice: str | None = None,
        **kwargs: Any,
    ) -> BaseChatModel:
        del tools, tool_choice, kwargs
        return self

    def _generate(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None = None,
        run_manager: Any = None,
        **kwargs: Any,
    ) -> ChatResult:
        del stop, run_manager, kwargs
        human_count = sum(message.type == "human" for message in messages)
        self.observed_human_counts.append(human_count)
        return ChatResult(
            generations=[
                ChatGeneration(
                    message=AIMessage(
                        content=f"human-count:{human_count}",
                        usage_metadata={
                            "input_tokens": 2,
                            "output_tokens": 3,
                            "total_tokens": 5,
                        },
                    )
                )
            ]
        )


def unit_request(binding: DeepAgentExecutionBinding) -> OperationExecutionRequest:
    payload = operation_request().model_dump(mode="python")
    payload.update(
        identity=OperationAttemptIdentity(
            run_id=binding.run_id,
            operation_id=binding.operation_id,
            operation_attempt=binding.operation_attempt,
        ),
        execution_runtime="deep_agent",
        native_placement=None,
        deep_agent_binding=binding,
        runtime_unit=binding.runtime_unit,
        idempotency_key=f"side-effect:{binding.operation_id}",
    )
    return OperationExecutionRequest.model_validate(payload)


def runtime_invocation(binding: DeepAgentExecutionBinding) -> RuntimeInvocation:
    base = operation_request()
    payload = base.model_dump(mode="python")
    payload.update(
        identity=OperationAttemptIdentity(
            run_id=binding.run_id,
            operation_id=binding.operation_id,
            operation_attempt=binding.operation_attempt,
        ),
        execution_runtime="deep_agent",
        native_placement=None,
        deep_agent_binding=binding,
        runtime_unit=binding.runtime_unit,
    )
    request = OperationExecutionRequest.model_validate(payload)
    operation_binding = bind_operation_execution_request(request)
    return RuntimeInvocation(
        binding=operation_binding,
        prompt_segments=request.prompt_segments,
        workspace=MaterializedWorkspace(
            workspace_id=request.workspace.workspace_id,
            namespace_id=request.workspace.namespace_id,
            provider=request.workspace.provider,
            runtime_digest=request.workspace.runtime_digest,
            image_digest=request.workspace.image_digest,
            mount_manifest_digest=sha256_digest("mounts"),
        ),
    )


def unit_bound(
    binding: DeepAgentExecutionBinding,
    *,
    iteration: int | None = None,
    session_generation: int = 1,
) -> DeepAgentExecutionBinding:
    """Freeze a fixture binding to a StageGraph unit, or to a GoalDirected executor unit."""

    if iteration is None:
        unit = stage_unit(
            request_scope="tenant-1",
            run_id=binding.run_id,
            operation_id=binding.operation_id,
        )
    else:
        unit = goal_unit(
            request_scope="tenant-1",
            run_id=binding.run_id,
            operation_id=f"goal-iteration/{iteration}/executor",
            goal_iteration=iteration,
            session_generation=session_generation,
        )
    return bind_unit(binding, unit)


async def planned_invocation(
    binding: DeepAgentExecutionBinding,
    lineage: CheckpointLineageService,
    *,
    attempt: int = 1,
) -> RuntimeInvocation:
    invocation = runtime_invocation(binding)
    plan = await lineage.observe_attempt(
        invocation.binding, activity_attempt(attempt), dispatching=True
    )
    return invocation.model_copy(update={"checkpoint_plan": plan})


async def execute_and_observe(
    adapter: DeepAgentRuntimeAdapter,
    lineage: CheckpointLineageService,
    binding: DeepAgentExecutionBinding,
):  # type: ignore[no-untyped-def]
    invocation = await planned_invocation(binding, lineage)
    result = await adapter.execute(invocation, {})
    assert invocation.checkpoint_plan is not None and result.checkpoint is not None
    transition = await lineage.record_transition(
        invocation.checkpoint_plan,
        result.checkpoint,
        result_manifest_ref=f"manifest:{binding.binding_id}",
        result_manifest_digest=sha256_digest(result.output_text),
    )
    return invocation, result, transition


def test_operation_workflow_derives_queue_and_rejects_binding_generation_drift() -> None:
    binding, _profile, _bundle = exact_fixture()
    payload = operation_request().model_dump(mode="python")
    payload.update(
        execution_runtime="deep_agent",
        native_placement=None,
        deep_agent_binding=binding,
    )
    operation = OperationExecutionRequest.model_validate(payload)
    request = OperationWorkflowRequest(
        semantic_attempt_id=operation.identity.semantic_key,
        execution_generation=binding.execution_generation,
        operation_kind="bound_operation",
        operation=operation,
    )

    assert request.activity_task_queue == binding.task_queue
    with pytest.raises(ValueError, match="generation"):
        OperationWorkflowRequest(
            semantic_attempt_id=operation.identity.semantic_key,
            execution_generation=binding.execution_generation + 1,
            operation_kind="bound_operation",
            operation=operation,
        )


def registry(
    binding: DeepAgentExecutionBinding,
    bundle: ResolvedSkillBundle,
    model: BaseChatModel | None = None,
) -> ExactComponentRegistry:
    selected = model or SkillReadingModel()
    return ExactComponentRegistry(
        model_factories={binding.model.ref.digest: lambda _binding, _secrets: selected},
        skill_bundles={bundle.bundle_digest: bundle},
        sandbox_factories={binding.sandbox.ref.digest: StateSandboxFactory()},
        checkpointers={binding.checkpointer_ref.digest: InMemorySaver()},
        stores={binding.store_ref.digest: InMemoryStore()},
    )


def test_profile_placement_and_cognitive_schema_digests_are_strict_and_stable() -> None:
    first, profile, _bundle = exact_fixture()
    second, second_profile, _second_bundle = exact_fixture()
    assert first == second
    assert profile.profile_digest == second_profile.profile_digest
    assert first.binding_digest.startswith("sha256:")
    with pytest.raises(ValidationError, match="profile digest mismatch"):
        DeepAgentProfile.model_validate(
            {**profile.model_dump(mode="python"), "profile_digest": DIGEST_A}
        )
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        DeepAgentProfile.model_validate(
            {**profile.model_dump(mode="python"), "runtime_alias": "latest"}
        )


def test_nested_frozenset_projections_have_one_canonical_digest() -> None:
    observed = {
        (
            binding.binding_digest,
            binding.cognitive_state_schema.schema_digest,
            binding.cognitive_context_schema.schema_digest,
        )
        for binding, _profile, _bundle in (exact_fixture(with_child_slices=True) for _ in range(25))
    }
    assert len(observed) == 1


def test_channel_collision_and_context_secret_material_fail_closed() -> None:
    state_packs, context_pack, _state, _context = cognitive_schemas()
    collision = CognitiveChannelPack.create(
        logical_id="pack.collision",
        revision=1,
        contributor="middleware",
        channels=(
            CognitiveChannelDefinition(
                name="artifact_index",
                value_kind="object",
                value_schema_ref="schema:different@1",
                reducer="replace",
            ),
        ),
    )
    with pytest.raises(ValueError, match="channel collision"):
        compose_cognitive_state_schema(
            schema_id="state.collision",
            packs=(state_packs[0], collision),
        )
    unsafe = CognitiveRuntimeContextPack.create(
        logical_id="pack.unsafe",
        revision=1,
        contributor="operation_role",
        fields=(CognitiveRuntimeField(name="api_key", value_kind="string"),),
    )
    with pytest.raises(ValidationError, match="references only"):
        compose_cognitive_context_schema(
            schema_id="context.unsafe",
            packs=(context_pack, unsafe),
        )


@pytest.mark.asyncio
async def test_actual_deep_agent_progressively_loads_skill_md_into_messages() -> None:
    binding, _profile, bundle = exact_fixture()
    model = SkillReadingModel()
    adapter = DeepAgentRuntimeAdapter(ExactDeepAgentMaterializer(registry(binding, bundle, model)))
    lineage = CheckpointLineageService(InMemoryCheckpointLineageRepository())
    result = await adapter.execute(await planned_invocation(unit_bound(binding), lineage), {})

    assert result.output_text == "SKILL-MD-IN-MESSAGES-040 observed and followed."
    inspection = result.event_payloads[0]
    assert "artifact_index" in inspection["state_keys"]
    assert inspection["skills_metadata"][0]["name"] == "exact-binding-proof"
    assert "SKILL-MD-IN-MESSAGES-040" in (inspection["skill_instruction_messages"][0]["content"])
    assert inspection["skill_instruction_messages"][0]["path"].endswith("SKILL.md")
    assert model.calls == 2


@pytest.mark.asyncio
async def test_shared_session_reuse_is_ordered_and_same_unit_never_reappends_prompt() -> None:
    """REQ-CP-DA-018 / REQ-BP-GD-012 / RRM-001 section 7 #3.

    The WP-CP-040 test asserted `[1, 2, 1]` by executing the *same* invocation twice, which
    re-appended its prompt on the same thread. Under the accepted checkpoint protocol a
    checkpoint written by the current submission never authorizes appending that input
    again: the same-unit re-execution now fails closed before the model is called. The
    intentional cross-iteration session reuse (next unit, pinned to the namespace head) and
    the empty rollover session keep their accepted human-message counts.

    RRM-004 (REQ-CP-DA-018): RRM-003 failed the same-unit re-execution closed because only
    `not_submitted` was implemented. The full classifier now recognizes the unit's own
    stamped terminal lineage as `terminal_unobserved` and reconstructs the identical result
    from it, still with no model call and no re-appended prompt.
    """

    binding, _profile, bundle = exact_fixture()
    model = SessionProbeModel(observed_human_counts=[])
    adapter = DeepAgentRuntimeAdapter(ExactDeepAgentMaterializer(registry(binding, bundle, model)))
    lineage = CheckpointLineageService(InMemoryCheckpointLineageRepository())
    first_unit = unit_bound(binding, iteration=1)
    first_invocation, first_result, first_transition = await execute_and_observe(
        adapter, lineage, first_unit
    )

    reconstructed = await adapter.execute(first_invocation, {})
    assert reconstructed.checkpoint is not None and first_result.checkpoint is not None
    assert reconstructed.checkpoint.classification == "terminal_unobserved"
    assert reconstructed.checkpoint.result_key == first_result.checkpoint.result_key
    assert reconstructed.output_text == first_result.output_text
    assert reconstructed.usage == first_result.usage
    assert model.observed_human_counts == [1]
    with pytest.raises(CheckpointLineageInDoubt, match="unsettled transition"):
        await planned_invocation(first_unit, lineage, attempt=2)

    _second_invocation, second_result, second_transition = await execute_and_observe(
        adapter, lineage, unit_bound(binding, iteration=2)
    )
    _fresh_invocation, fresh_result, fresh_transition = await execute_and_observe(
        adapter, lineage, unit_bound(binding, iteration=3, session_generation=2)
    )

    assert model.observed_human_counts == [1, 2, 1]
    assert second_transition.namespace == first_transition.namespace
    assert second_transition.source_key == first_transition.result_key
    assert fresh_transition.namespace != first_transition.namespace
    assert fresh_transition.source_key is None
    assert first_result.usage.amounts["tokens.total"] == 5
    assert second_result.usage.amounts["tokens.total"] == 5
    assert fresh_result.usage.amounts["tokens.total"] == 5


@pytest.mark.asyncio
async def test_invocation_is_root_namespaced_pinned_sync_durable_and_fully_stamped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """REQ-CP-DA-016/017: exact thread, root `checkpoint_ns`, source pin, stamps, sync."""

    import mission_control.adapters.deep_agents.adapter as adapter_module

    invoke_kwargs: list[dict[str, Any]] = []
    real_create = adapter_module.create_deep_agent

    def recording_create(*args: Any, **kwargs: Any) -> Any:
        agent = real_create(*args, **kwargs)
        real_ainvoke = agent.ainvoke

        async def ainvoke(state: Any, **invoke: Any) -> Any:
            invoke_kwargs.append(invoke)
            return await real_ainvoke(state, **invoke)

        object.__setattr__(agent, "ainvoke", ainvoke)
        return agent

    monkeypatch.setattr(adapter_module, "create_deep_agent", recording_create)
    binding, _profile, bundle = exact_fixture()
    saver = InMemorySaver()
    exact_registry = registry(binding, bundle, SessionProbeModel(observed_human_counts=[]))
    exact_registry = ExactComponentRegistry(
        model_factories=exact_registry.model_factories,
        skill_bundles=exact_registry.skill_bundles,
        sandbox_factories=exact_registry.sandbox_factories,
        checkpointers={binding.checkpointer_ref.digest: saver},
        stores=exact_registry.stores,
    )
    adapter = DeepAgentRuntimeAdapter(ExactDeepAgentMaterializer(exact_registry))
    lineage = CheckpointLineageService(InMemoryCheckpointLineageRepository())
    first_binding = unit_bound(binding, iteration=1)
    second_binding = unit_bound(binding, iteration=2)
    first, first_result, first_transition = await execute_and_observe(
        adapter, lineage, first_binding
    )
    second, second_result, second_transition = await execute_and_observe(
        adapter, lineage, second_binding
    )
    assert first.checkpoint_plan is not None and second.checkpoint_plan is not None
    assert first_result.checkpoint is not None and second_result.checkpoint is not None

    namespace = first_transition.namespace
    assert namespace == "belllabs/goal/run-operation/epoch/1/session/1/role/executor"
    assert [call["config"]["configurable"] for call in invoke_kwargs] == [
        {"thread_id": namespace, "checkpoint_ns": ""},
        {
            "thread_id": namespace,
            "checkpoint_ns": "",
            "checkpoint_id": first_transition.result_key.checkpoint_id,
        },
    ]
    assert [call["durability"] for call in invoke_kwargs] == ["sync", "sync"]
    thread = {"configurable": {"thread_id": namespace, "checkpoint_ns": ""}}
    root = [item async for item in saver.alist(thread)]
    plans = {plan.invocation_id: plan for plan in (first.checkpoint_plan, second.checkpoint_plan)}
    stamped_per_invocation: dict[str, int] = {}
    for item in root:
        plan = plans[item.metadata[STAMP_INVOCATION_ID]]
        assert item.metadata[STAMP_UNIT_KEY] == plan.unit_key
        assert item.metadata[STAMP_EXECUTION_GENERATION] == 1
        assert item.metadata[STAMP_BINDING_DIGEST] == plan.binding_digest
        assert item.metadata[STAMP_STATE_SCHEMA_DIGEST] == (
            binding.cognitive_state_schema.schema_digest
        )
        assert "Return BINDING-OK" not in str(item.metadata)
        stamped_per_invocation[plan.invocation_id] = (
            stamped_per_invocation.get(plan.invocation_id, 0) + 1
        )
    assert stamped_per_invocation == {
        first_transition.invocation_id: first_result.checkpoint.stamped_checkpoint_count,
        second_transition.invocation_id: second_result.checkpoint.stamped_checkpoint_count,
    }
    assert second_transition.result_key.parent_checkpoint_id is not None
    assert await lineage.repository.get_namespace_head("tenant-1", namespace) == (
        second_transition.result_key
    )


@pytest.mark.asyncio
async def test_source_state_schema_mismatch_fails_closed_before_model_invocation() -> None:
    """REQ-CP-CS-007 (amended): the stamped source schema digest gates every resume."""

    binding, _profile, bundle = exact_fixture()
    drifted, _drifted_profile, _drifted_bundle = exact_fixture(with_child_slices=True)
    assert drifted.cognitive_state_schema.schema_digest != (
        binding.cognitive_state_schema.schema_digest
    )
    model = SessionProbeModel(observed_human_counts=[])
    adapter = DeepAgentRuntimeAdapter(ExactDeepAgentMaterializer(registry(binding, bundle, model)))
    lineage = CheckpointLineageService(InMemoryCheckpointLineageRepository())
    await execute_and_observe(adapter, lineage, unit_bound(binding, iteration=1))
    second = await planned_invocation(unit_bound(drifted, iteration=2), lineage)

    with pytest.raises(IncompatibleCheckpointSchema, match="REQ-CP-CS-007"):
        await adapter.execute(second, {})
    assert model.observed_human_counts == [1]


@pytest.mark.asyncio
async def test_unplanned_or_drifted_invocation_is_refused_before_materialization() -> None:
    binding, _profile, bundle = exact_fixture()
    model = SessionProbeModel(observed_human_counts=[])
    adapter = DeepAgentRuntimeAdapter(ExactDeepAgentMaterializer(registry(binding, bundle, model)))
    lineage = CheckpointLineageService(InMemoryCheckpointLineageRepository())
    planned = await planned_invocation(unit_bound(binding), lineage)
    assert planned.checkpoint_plan is not None

    with pytest.raises(DeepAgentMaterializationError, match="lineage plan"):
        await adapter.execute(planned.model_copy(update={"checkpoint_plan": None}), {})
    with pytest.raises(DeepAgentMaterializationError, match="lineage plan"):
        await adapter.execute(runtime_invocation(binding), {})
    drifted_plan = planned.checkpoint_plan.model_copy(update={"checkpointer_ref_digest": DIGEST_A})
    with pytest.raises(DeepAgentMaterializationError, match="does not match"):
        await adapter.execute(planned.model_copy(update={"checkpoint_plan": drifted_plan}), {})
    assert model.observed_human_counts == []


@pytest.mark.asyncio
async def test_exact_mcp_server_and_tool_surface_materialize() -> None:
    binding, _profile, bundle = exact_fixture(include_mcp=True)
    materializer = ExactDeepAgentMaterializer(registry(binding, bundle))
    async with materializer.prepare(binding, {}) as resolved:
        assert [tool.name for tool in resolved.tools] == ["lookup_binding_marker"]
        assert await resolved.tools[0].ainvoke({"code": "UNIT"})


@pytest.mark.asyncio
async def test_runtime_package_drift_fails_before_model_or_sandbox_effects() -> None:
    binding, _profile, bundle = exact_fixture(package_versions={"deepagents": "999.0.0"})
    calls = 0

    def model_factory(_binding, _secrets):  # type: ignore[no-untyped-def]
        nonlocal calls
        calls += 1
        return SkillReadingModel()

    exact_registry = registry(binding, bundle)
    exact_registry = ExactComponentRegistry(
        model_factories={binding.model.ref.digest: model_factory},
        skill_bundles=exact_registry.skill_bundles,
        sandbox_factories=exact_registry.sandbox_factories,
        checkpointers=exact_registry.checkpointers,
        stores=exact_registry.stores,
    )
    with pytest.raises(DeepAgentRuntimeDrift, match="package drift"):
        async with ExactDeepAgentMaterializer(exact_registry).prepare(binding, {}):
            pass
    assert calls == 0


def test_sync_subagent_cannot_exceed_parent_tool_or_workspace_ceiling() -> None:
    _state_packs, _context_pack, state_schema, context_schema = cognitive_schemas(
        with_child_slices=True
    )
    model = DeepAgentModelComponent(
        ref=exact(DefinitionKind.MODEL, "model.child", "child-model"),
        provider="openai",
        model_name="fixture-model",
    )
    child = SyncSubagentProfile(
        name="bounded-child",
        description="A bounded child fixture.",
        system_prompt_ref=exact(DefinitionKind.PROMPT, "prompt.child", "child-prompt"),
        model=model,
        tool_refs=(exact(DefinitionKind.TOOL, "tool.not-parent", "not-parent"),),
        state_slice_id="child-state",
        context_slice_id="child-context",
        workspace_id="workspace-child",
        writable_paths=("/workspace/child",),
    )
    _binding, base_profile, _bundle = exact_fixture(with_child_slices=True)
    profile_payload = base_profile.model_dump(mode="python", exclude={"profile_digest"})
    profile_payload["sync_subagents"] = (child,)
    profile = DeepAgentProfile.create(**profile_payload)
    del state_schema, context_schema
    request = operation_request()
    _, _, effective_state, effective_context = cognitive_schemas(with_child_slices=True)
    placement = DeepAgentExecutionPlacementProfile.create(
        logical_id="placement.wp-cp-040",
        revision=1,
        placement="local_in_worker",
        python_runtime="3.12",
        package_versions={"deepagents": "0.7.5"},
        task_queue="agent-cognitive",
        checkpoint_behavior="local_checkpointer",
        cancellation_behavior="cooperative",
        streaming_behavior="state_updates",
        message_injection_behavior="invoke_only",
        reconnect_behavior="checkpoint_resume",
        sandbox_backends=frozenset({"state"}),
        qualification_refs=("QUAL-CP-DEEP-AGENT-MATERIALIZATION",),
    )
    with pytest.raises(ValueError, match="tools exceed"):
        compile_deep_agent_execution_binding(
            profile=profile,
            placement=placement,
            state_schema=effective_state,
            context_schema=effective_context,
            context_values={
                "run_id": request.identity.run_id,
                "operation_id": request.identity.operation_id,
                "operation_attempt": 1,
                "execution_generation": 1,
                "capability_grant_ref": "ref:grant",
                "workspace_handle": "handle:workspace",
            },
            run_id=request.identity.run_id,
            operation_id=request.identity.operation_id,
            operation_attempt=1,
            execution_generation=1,
            erc_digest=request.effective_configuration_digest,
            control_revision=request.run_control_revision,
            workspace=request.workspace,
            capability_grant=CapabilityGrant(capabilities=frozenset()),
            reservation_id=request.budget_reservation_id,
            authority_refs=("authority:test",),
            redaction_policy_ref="redaction:test",
            initial_context_manifest={"digest": sha256_digest("context")},
        )


def test_create_deep_agent_has_one_non_experiment_production_call_site() -> None:
    import mission_control.adapters.deep_agents.adapter as adapter_module

    source = inspect.getsource(adapter_module)
    assert source.count("create_deep_agent(") == 1


@pytest.mark.asyncio
async def test_operation_service_pins_records_and_links_the_result_checkpoint() -> None:
    """Production seam: `OperationExecutionService` → adapter → transition → settlement."""

    from mission_control.adapters.operations.conformance import (
        ConformanceAssetVerifier,
        ConformanceBudgetAuthority,
        ConformanceEventSink,
        ConformanceSandbox,
        ConformanceSecretResolver,
    )
    from mission_control.application.execution.operations.operation_execution import (
        InMemoryOperationBindingRepository,
        OperationExecutionService,
    )
    from tests.unit.operations.test_operation_execution import MCP_DIGEST, SKILL_DIGEST

    class AcceptingAuthority:
        async def verify(self, request: OperationExecutionRequest) -> None:
            del request

        async def verify_continuation(
            self, request: OperationExecutionRequest, binding: Any
        ) -> None:
            del request, binding

        async def verify_cancellation(
            self, request: OperationExecutionRequest, binding: Any
        ) -> None:
            del request, binding

    binding, _profile, bundle = exact_fixture()
    model = SessionProbeModel(observed_human_counts=[])
    repository = InMemoryCheckpointLineageRepository()
    assets = ConformanceAssetVerifier(
        mcp_schema_digests={"fixture-mcp": MCP_DIGEST},
        asset_manifest_digests={"skill:fixture.skill:1": SKILL_DIGEST},
    )

    def compose(lineage: CheckpointLineageService | None) -> OperationExecutionService:
        return OperationExecutionService(
            authority=AcceptingAuthority(),
            bindings=InMemoryOperationBindingRepository(),
            runtime=DeepAgentRuntimeAdapter(
                ExactDeepAgentMaterializer(registry(binding, bundle, model))
            ),
            sandbox=ConformanceSandbox(),
            assets=assets,
            mcp=assets,
            secrets=ConformanceSecretResolver({"environment:OPENAI_API_KEY": "unused"}),
            events=ConformanceEventSink(),
            budget=ConformanceBudgetAuthority(),
            lineage=lineage,
        )

    request = unit_request(unit_bound(binding))
    with pytest.raises(ValueError, match="checkpoint lineage composition"):
        await compose(None).execute(request, activity_attempt())
    service = compose(CheckpointLineageService(repository))

    result = await service.execute(request, activity_attempt(attempt=1))
    replay = await service.execute(request, activity_attempt(attempt=2))

    assert request.runtime_unit is not None
    transition = await repository.get_transition("tenant-1", request.runtime_unit.unit_key, 1)
    assert transition is not None
    assert result.status == "completed"
    assert result.result_checkpoint == transition.result_key
    assert result.checkpoint_transition_id == transition.transition_id
    assert transition.result_manifest_ref.startswith("operation-settlement:")
    assert replay == result
    assert model.observed_human_counts == [1]
    attempts = await repository.list_attempts("tenant-1", request.runtime_unit.unit_key)
    assert [(item.attempt.attempt, item.dispatching) for item in attempts] == [(1, True)]
