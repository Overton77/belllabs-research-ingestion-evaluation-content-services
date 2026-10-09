"""FIXTURE deployment launch bindings (``mc.manifest_launch_bindings.v1``) for tests only.

Not production configuration: every model profile the example manifests select maps to the
WP-CP-040 qualification model (``model.wp-cp-040``, served in tests by deterministic local
cognition) and every sandbox profile to its ``state`` sandbox. The scaffold is the
qualification's Deep Agent profile with its Skill, MCP server and subagent removed (manifest
capabilities bind those) and a placement on the test stack's agent-cognitive queue.
"""

from __future__ import annotations

from mission_control.application.authoring.manifest_launch_inputs import (
    CapabilityComponentBinding,
    DeepAgentScaffold,
    ManifestLaunchBindings,
    ServedComponents,
    WorkspaceProvision,
)
from mission_control.domain.authoring.canonical import sha256_digest
from mission_control.domain.authoring.contracts import DefinitionKind, ExactDefinitionRef
from mission_control.domain.authoring.manifest import canonical_manifest_bytes, load_manifest_yaml
from mission_control.domain.execution.contracts import (
    DeepAgentExecutionPlacementProfile,
    DeepAgentMCPServerComponent,
    DeepAgentProfile,
    StructuredOutputBinding,
)
from tests.acceptance.control_plane.test_wp_cp_040 import exact_fixture
from tests.unit.operations.test_operation_execution import operation_request

FIXTURE_MODEL_PROFILES = ("frontier.default", "frontier.long_context")
FIXTURE_SANDBOX_PROFILES = ("research.standard", "ingestion.standard")


def fixture_bindings(
    *,
    task_queue: str = "agent-cognitive",
    model_profiles: tuple[str, ...] = FIXTURE_MODEL_PROFILES,
    sandbox_profiles: tuple[str, ...] = FIXTURE_SANDBOX_PROFILES,
) -> ManifestLaunchBindings:
    binding, profile, _bundle = exact_fixture()
    scaffold_profile = DeepAgentProfile.create(
        **{
            **profile.model_dump(mode="python", exclude={"profile_digest"}),
            "mcp_servers": (),
            "skills": (),
            "tools": (),
            "sync_subagents": (),
            "compatible_placement_ids": frozenset({"placement.manifest-fixture"}),
        }
    )
    placement = DeepAgentExecutionPlacementProfile.create(
        logical_id="placement.manifest-fixture",
        revision=1,
        placement="local_in_worker",
        python_runtime="3.12",
        package_versions=dict(binding.package_versions),
        task_queue=task_queue,
        checkpoint_behavior="local_checkpointer",
        cancellation_behavior="cooperative",
        streaming_behavior="state_updates",
        message_injection_behavior="invoke_only",
        reconnect_behavior="checkpoint_resume",
        sandbox_backends=frozenset({binding.sandbox.backend}),
        qualification_refs=("QUAL-CP-DEEP-AGENT-MATERIALIZATION",),
    )
    base = operation_request()
    workspace = base.workspace
    return ManifestLaunchBindings(
        deep_agents=DeepAgentScaffold(
            profile=scaffold_profile,
            placement=placement,
            state_schema=binding.cognitive_state_schema,
            context_schema=binding.cognitive_context_schema,
            context_values=dict(binding.cognitive_context_values),
            initial_context_manifest=dict(binding.initial_context_manifest),
            authority_refs=binding.authority_refs,
            redaction_policy_ref=binding.redaction_policy_ref,
            agent_profile_ref=base.agent_profile_ref,
            tracing_policy_ref=base.tracing_policy_ref,
            sensitive_data_policy_ref=base.sensitive_data_policy_ref,
            snapshot_policy_ref=base.snapshot_policy_ref,
            goal_output_schemas={
                role: StructuredOutputBinding(
                    schema_id=f"goal-{role}-observation",
                    revision=1,
                    schema_digest=sha256_digest(f"goal-{role}-observation-schema"),
                )
                for role in ("executor", "verifier")
            },
            workspace=WorkspaceProvision(
                provider=workspace.provider,
                runtime_digest=workspace.runtime_digest,
                image_digest=workspace.image_digest,
                package_digest=workspace.package_digest,
                environment_digest=workspace.environment_digest,
            ),
        ),
        model_profiles=dict.fromkeys(model_profiles, binding.model),
        sandbox_profiles=dict.fromkeys(sandbox_profiles, binding.sandbox),
    )


def fixture_served(bindings: ManifestLaunchBindings) -> ServedComponents:
    """Exactly the fixture's own components, as a test deployment registers them."""

    profile = bindings.deep_agents.profile
    return ServedComponents(
        models=frozenset(item.ref.digest for item in bindings.model_profiles.values()),
        sandboxes=frozenset(item.ref.digest for item in bindings.sandbox_profiles.values()),
        checkpointers=frozenset({profile.checkpointer_ref.digest}),
        stores=frozenset({profile.store_ref.digest}),
    )


def unserved_model_ref() -> ExactDefinitionRef:
    return ExactDefinitionRef(
        kind=DefinitionKind.MODEL,
        logical_id="model.not-served",
        revision=1,
        digest=sha256_digest("model.not-served"),
    )


def deep_agents_chain(manifest_yaml: str) -> str:
    """FIXTURE transform of Mission 2: both missions on `deep_agents` with only what this
    deployment fixture binds (model, sandbox and the pubmed MCP server); catalog capabilities
    without a Deep Agents launch binding (hooks, subagents, context bundles, executors) are
    dropped, and the action space narrows to pubmed."""

    document = load_manifest_yaml(manifest_yaml)
    pubmed = {"search": "pubmed literature retrieval", "kind": "mcp_server", "as": "pubmed"}
    for mission in document["missions"]:
        environment = mission["environment"]
        environment["lane"] = "deep_agents"
        environment["model"] = {"profile": "frontier.default"}
        environment.setdefault("sandbox", {"profile": "research.standard"})
        for name in ("workspace", "agents", "hooks", "plugins"):
            environment.pop(name, None)
        environment["capabilities"] = [pubmed]
        program = mission["program"]
        program["action_space"] = ["pubmed"]
        program["inputs"] = [
            item for item in program.get("inputs", ()) if not item["from"].startswith("root.")
        ]
        if not program["inputs"]:
            program.pop("inputs")
    return canonical_manifest_bytes(document).decode("utf-8")


def pubmed_bound(
    bindings: ManifestLaunchBindings,
    digest: str,
    server: DeepAgentMCPServerComponent | None = None,
) -> ManifestLaunchBindings:
    """FIXTURE: the catalog's pubmed revision served by the qualification MCP component."""

    if server is None:
        (server,) = exact_fixture(include_mcp=True)[0].mcp_servers
    return bindings.model_copy(
        update={
            "capabilities": {
                "mcp.pubmed": CapabilityComponentBinding(
                    catalog_digest=digest, kind="mcp_server", mcp_server=server
                )
            }
        }
    )
