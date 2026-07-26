from __future__ import annotations

import argparse
import asyncio
import os
import shutil
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import docker
from temporalio.contrib.openai_agents import OpenAIAgentsPlugin

from app.application.control_plane import ControlPlaneService
from app.application.control_plane_repository import InMemoryDefinitionRepository
from app.application.goal_directed_live import (
    GOAL_RESEARCH_SKILL,
    GoalWorkspaceHandoffPreparer,
    LiveGoalIterationExecutor,
    OpenAIGoalIndependentVerifier,
    create_live_goal_workspace,
    digest_bytes,
    write_live_acceptance_manifest,
)
from app.application.orchestration import (
    F1OrchestrationBindingVerifier,
    GoalDirectedLaunchService,
    RunControlLifecycleGateway,
    WorkflowLaunchDispatcher,
)
from app.application.run_control import (
    AdmissionPolicyRegistry,
    F1RunConfigurationVerifier,
    RunControlService,
)
from app.application.run_control_repository import InMemoryRunControlRepository
from app.config import get_settings
from app.domain.control_plane.canonical import sha256_digest
from app.domain.control_plane.contracts import (
    AuthorityCeiling,
    BudgetCeiling,
    CompilationContext,
    CompileInvocation,
    ControlProfileDefinition,
    Definition,
    DefinitionSelector,
    EffectiveRunConfiguration,
    EnvironmentAvailability,
    EvaluationProfileDefinition,
    GoalConvergencePolicy,
    GoalDirectedBlueprint,
    GoalSessionRolloverPolicy,
    GoalWorkspaceSnapshotPolicy,
    PublishedDefinition,
    PublishRequest,
    RunInputManifestRef,
    RuntimeProfileDefinition,
    SecretRef,
    WorkflowTypeDefinition,
    WorkflowWorkspaceContract,
    WorkspaceSlot,
    WorkspaceTemplateDefinition,
)
from app.domain.control_plane.extensions import ExtensionRegistry
from app.domain.orchestration.contracts import GoalDirectedRunInput
from app.domain.run_control.contracts import (
    ActorContext,
    BudgetApplicability,
    BudgetDimensionLimit,
    BudgetEnvelope,
    DecisionStatus,
    RunRequest,
)
from app.domain.run_control.reducer import ACTION_PERMISSIONS
from app.integrations.control_plane_payloads import InMemoryPayloadStore
from app.integrations.temporal import create_temporal_client
from app.temporal.agentic_probe_assets import TAVILY_BEST_PRACTICES_SKILL
from app.temporal.goal_directed_activities import (
    GoalDirectedActivities,
    create_goal_directed_worker,
)
from app.temporal.goal_directed_workflow import GoalDirectedWorkflow

PROBE_IMAGE = "belllabs-agentic-probe:local"
ARTIFACT_ROOT = Path("sandbox-work/goal-directed-live")
REQUEST_SCOPE = "local-goal-directed-live"
SMOKE_ITERATION_BUDGET = {
    "goal.iterations": 1,
    "tokens.input": 8_000,
    "tokens.output": 2_000,
    "tokens.total": 10_000,
    "model.turns": 2,
}
DAVE_ITERATION_BUDGET = {
    "goal.iterations": 1,
    "tokens.input": 120_000,
    "tokens.output": 15_000,
    "tokens.total": 135_000,
    "model.turns": 14,
}
ALL_BUDGET_DIMENSIONS = frozenset(
    {
        "currency.estimated_micros",
        "currency.actual_micros",
        "tokens.input",
        "tokens.output",
        "tokens.total",
        "time.elapsed_ms",
        "time.active_compute_ms",
        "model.turns",
        "tool.calls.total",
        "mcp.calls.total",
        "external.quotas.total",
        "stage.cycles",
        "workflow.cycles",
        "goal.iterations",
        "operation.attempts",
        "subagent.spawns",
        "concurrency.slots",
    }
)

SMOKE_GOAL = """\
As of 2026-07-25, use current web evidence to produce a compact cited report on
OpenAI: identify its current CEO, what the company does, its official site, one
material publicly supportable business fundamental, dated official role
evidence, one independent corroborating source, confidence, checked date, and
the limits of open-web exhaustiveness. Preserve any rejected leadership
candidates with reasons.
"""
SMOKE_ACCEPTANCE = """\
Accept only a complete Markdown report with the company identity, current CEO,
company purpose, official site, at least one material public business
fundamental, dated official current-role evidence, independent corroboration,
confidence, checked date 2026-07-25, rejected candidates, working source URLs,
and an explicit open-web completeness limitation.
"""

DAVE_GOAL = """\
This is a bounded best-effort test. As of 2026-07-25, use a small set of
high-value Tavily searches to identify and verify the strongest publicly
supported candidates for companies or operating brands that Dave Asprey
currently runs. "Currently runs" means a present active executive, operator, or
controlling leadership role; exclude founder-only historical affiliations,
investments, advisory roles, sold companies, and closed projects. Do your best,
preserve investigated exclusions and unresolved candidates, and do not claim
provable completeness. Produce the complete source-cited Markdown report at
/workspace/output/company-report.md.
"""
DAVE_ACCEPTANCE = """\
Independently accept this bounded test only when the report: (1) defines
"currently runs" and the as-of date 2026-07-25; (2) deduplicates discovered
companies and brands; (3) lists each supported included candidate with Dave
Asprey's current active role, what it does, official site, material publicly
supportable fundamentals, dated current-status evidence, independent
corroboration where available, confidence, and checked date; (4) lists
investigated historical, sold, closed, investor-only, advisor-only, or
unresolved candidates with reasons; (5) evaluates source quality and bounded
search coverage; (6) uses working source URLs; and (7) clearly states that this
best-effort open-web test cannot prove logical exhaustiveness.
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=("smoke", "dave"), default="smoke")
    return parser.parse_args()


def iteration_budget(run_label: str) -> dict[str, int]:
    return (
        dict(SMOKE_ITERATION_BUDGET)
        if run_label.startswith("smoke")
        else dict(DAVE_ITERATION_BUDGET)
    )


def run_budget(run_label: str) -> dict[str, int]:
    return {dimension: amount * 4 for dimension, amount in iteration_budget(run_label).items()} | {
        "concurrency.slots": 1
    }


def authority(run_label: str) -> AuthorityCeiling:
    return AuthorityCeiling(
        capabilities=frozenset(
            {"model.invoke", "sandbox.filesystem", "sandbox.shell", "skill.read", "evaluate"}
        ),
        budgets=BudgetCeiling(dimensions=run_budget(run_label)),
        max_concurrency=1,
    )


async def publish(
    service: ControlPlaneService,
    definition: Definition,
    *,
    published_at: datetime,
) -> PublishedDefinition:
    return await service.publish(
        PublishRequest(
            definition=definition,
            actor_id="goal-directed-live-author",
            published_at=published_at,
            expected_head_revision=0,
        )
    )


async def publish_compile_admit(
    *,
    objective: str,
    acceptance: str,
    run_label: str,
) -> tuple[
    ControlPlaneService,
    RunControlService,
    str,
    EffectiveRunConfiguration,
    PublishedDefinition,
]:
    now = datetime.now(UTC)
    control_plane = ControlPlaneService(
        InMemoryDefinitionRepository(),
        ExtensionRegistry(),
        InMemoryPayloadStore(),
    )
    blueprint = await publish(
        control_plane,
        GoalDirectedBlueprint(
            logical_id=f"live.{run_label}.goal-directed",
            title=f"Live {run_label} GoalDirected blueprint",
            description="Low-threshold durable Ralph loop acceptance blueprint.",
            objective_contract=f"objective:live-{run_label}@1",
            acceptance_contract=f"acceptance:live-{run_label}@1",
            independent_verifier_ref=f"verifier:live-{run_label}@1",
            allowed_operation_classes=frozenset({"research"}),
            session_policy=GoalSessionRolloverPolicy(
                session_mode="reuse",
                fresh_agent_token_threshold=1,
                handoff_token_reserve=1_000,
                rollover_mode="fresh_from_handoff",
            ),
            workspace_policy=GoalWorkspaceSnapshotPolicy(
                workspace_mode="shared",
                snapshot_mode="on_rollover",
            ),
            convergence_policy=GoalConvergencePolicy(
                max_no_progress_iterations=4,
                max_repeated_blockers=3,
            ),
            iteration_reservation=iteration_budget(run_label),
            max_iterations=3 if run_label.startswith("dave") else 4,
        ),
        published_at=now,
    )
    control = await publish(
        control_plane,
        ControlProfileDefinition(
            logical_id=f"live.{run_label}.control",
            title=f"Live {run_label} control",
            description="Pins the GoalDirected blueprint and bounded authority.",
            blueprint_ref=blueprint.ref,
            authority_ceiling=authority(run_label),
        ),
        published_at=now,
    )
    runtime = await publish(
        control_plane,
        RuntimeProfileDefinition(
            logical_id=f"live.{run_label}.runtime",
            title=f"Live {run_label} runtime",
            description="Pinned Python, OpenAI Agents, Docker, and Tavily runtime.",
            binding="python-3.12-openai-agents-0.17.8",
            required_capabilities=frozenset(
                {"model.invoke", "sandbox.filesystem", "sandbox.shell", "skill.read"}
            ),
            required_secrets=(
                SecretRef(provider="environment", key="OPENAI_API_KEY"),
                SecretRef(provider="environment", key="TAVILY_API_KEY"),
            ),
        ),
        published_at=now,
    )
    workspace = await publish(
        control_plane,
        WorkspaceTemplateDefinition(
            logical_id=f"live.{run_label}.workspace",
            title=f"Live {run_label} workspace",
            description="Read-only governed goal and one sequential output owner.",
            slots=(
                WorkspaceSlot(
                    name="goal",
                    path="/goal",
                    access="read_only",
                    purpose="Host-authored immutable goal projection",
                ),
                WorkspaceSlot(
                    name="output",
                    path="/workspace/output",
                    access="exclusive_write",
                    purpose="Sequential GoalDirected research outputs",
                ),
            ),
            required_capabilities=frozenset({"sandbox.filesystem", "sandbox.shell"}),
        ),
        published_at=now,
    )
    evaluation = await publish(
        control_plane,
        EvaluationProfileDefinition(
            logical_id=f"live.{run_label}.evaluation",
            title=f"Live {run_label} independent evaluation",
            description="Read-only independent model verification.",
            gate_contract_refs=frozenset({f"acceptance:live-{run_label}@1"}),
            required_capabilities=frozenset({"evaluate", "model.invoke"}),
        ),
        published_at=now,
    )
    workflow_type = await publish(
        control_plane,
        WorkflowTypeDefinition(
            logical_id=f"live.{run_label}.workflow",
            title=f"Live {run_label} GoalDirected workflow",
            description="Private immutable live acceptance workflow.",
            purpose=objective,
            non_goals=frozenset(
                {
                    "Medical advice",
                    "Secret persistence",
                    "Claims of logically provable open-web exhaustiveness",
                }
            ),
            input_admission_contract=f"input:live-{run_label}@1",
            invariants=frozenset({f"invariant:live-{run_label}-scope@1"}),
            obligations=frozenset({f"obligation:live-{run_label}-report@1"}),
            output_contracts=frozenset({f"output:live-{run_label}-report@1"}),
            allowed_blueprints=frozenset({blueprint.ref}),
            allowed_control_profiles=frozenset({control.ref}),
            allowed_runtime_profiles=frozenset({runtime.ref}),
            allowed_workspace_templates=frozenset({workspace.ref}),
            allowed_evaluation_profiles=frozenset({evaluation.ref}),
            authority_ceiling=authority(run_label),
            workspace_contract=WorkflowWorkspaceContract(
                slots=(
                    WorkspaceSlot(
                        name="goal",
                        path="/goal",
                        access="read_only",
                        purpose="Host-authored immutable goal projection",
                    ),
                    WorkspaceSlot(
                        name="output",
                        path="/workspace/output",
                        access="exclusive_write",
                        purpose="Sequential GoalDirected research outputs",
                    ),
                )
            ),
        ),
        published_at=now,
    )
    manifest = RunInputManifestRef(
        manifest_id=f"live-{run_label}-goal",
        revision=1,
        digest=sha256_digest({"objective": objective, "acceptance": acceptance}),
    )

    def selector(record: object) -> DefinitionSelector:
        return DefinitionSelector(exact=record.ref)  # type: ignore[attr-defined]

    configuration = await control_plane.compile(
        CompileInvocation(
            workflow_type=selector(workflow_type),
            blueprint=selector(blueprint),
            control_profile=selector(control),
            runtime_profile=selector(runtime),
            workspace_template=selector(workspace),
            evaluation_profile=selector(evaluation),
            input_manifest=manifest,
            caller_authority=authority(run_label),
            parent_authority=authority(run_label),
            environment=EnvironmentAvailability(
                capabilities=authority(run_label).capabilities,
                runtime_bindings=frozenset({"python-3.12-openai-agents-0.17.8"}),
                secret_refs=(
                    SecretRef(provider="environment", key="OPENAI_API_KEY"),
                    SecretRef(provider="environment", key="TAVILY_API_KEY"),
                ),
            ),
            context=CompilationContext(
                compilation_id=f"compile-{run_label}-{uuid4()}",
                compiled_at=now,
                actor_id="goal-directed-live-worker",
                authority_subject_id="goal-directed-live-worker",
                authority_scope=REQUEST_SCOPE,
            ),
        )
    )
    policies = AdmissionPolicyRegistry()
    policies.register(
        f"input:live-{run_label}@1",
        lambda _request, _configuration: None,
    )
    policies.register(
        f"invariant:live-{run_label}-scope@1",
        lambda _request, _configuration: None,
    )
    run_control = RunControlService(
        InMemoryRunControlRepository(),
        F1RunConfigurationVerifier(control_plane),
        policies,
    )
    actor = ActorContext(
        actor_id="goal-directed-live-worker",
        authority_refs=frozenset({"authority:goal-directed-live"}),
        permissions=frozenset({"workflow_run.admit", *ACTION_PERMISSIONS.values()}),
    )
    decision = await run_control.admit(
        RunRequest(
            request_scope=REQUEST_SCOPE,
            idempotency_issuer="goal-directed-live-worker",
            request_id=f"request-{run_label}-{uuid4()}",
            actor=actor,
            effective_configuration_digest=configuration.digest,
            workflow_type_ref=workflow_type.ref,
            input_manifest=manifest,
            budget_envelope=BudgetEnvelope(
                dimensions=tuple(
                    BudgetDimensionLimit(
                        dimension=dimension,
                        applicability=(
                            BudgetApplicability.BOUNDED
                            if dimension in run_budget(run_label)
                            else BudgetApplicability.NOT_APPLICABLE
                        ),
                        hard_cap=run_budget(run_label).get(dimension),
                    )
                    for dimension in sorted(ALL_BUDGET_DIMENSIONS)
                )
            ),
            requested_at=now,
            correlation_id=f"live-{run_label}-{uuid4()}",
            sponsorship_ref="sponsorship:local-live-acceptance",
            approval_refs=("approval:user-existing-openai-key",),
            delegation_authority_refs=actor.authority_refs,
            admission_evidence_refs=(
                f"skill:tavily.best-practices:{digest_bytes(TAVILY_BEST_PRACTICES_SKILL)}",
                f"skill:goal-directed.company-research:{digest_bytes(GOAL_RESEARCH_SKILL)}",
            ),
        )
    )
    if decision.status != DecisionStatus.ACCEPTED or decision.run_id is None:
        raise RuntimeError(
            f"GoalDirected live admission failed: {decision.reason_code}: {decision.reason}"
        )
    return (
        control_plane,
        run_control,
        decision.run_id,
        configuration,
        workspace,
    )


async def main() -> None:
    args = parse_args()
    objective = SMOKE_GOAL if args.mode == "smoke" else DAVE_GOAL
    acceptance = SMOKE_ACCEPTANCE if args.mode == "smoke" else DAVE_ACCEPTANCE
    run_label = f"{args.mode}-{uuid4().hex[:8]}"
    settings = get_settings()
    tavily_secret = settings.tavily_api_key
    if tavily_secret is None:
        raise RuntimeError("TAVILY_API_KEY is required for the live GoalDirected run")
    os.environ.setdefault("OPENAI_API_KEY", settings.openai_api_key.get_secret_value())

    docker_client = docker.from_env()
    image = docker_client.images.get(PROBE_IMAGE)
    (
        control_plane,
        run_control,
        run_id,
        configuration,
        workspace_record,
    ) = await publish_compile_admit(
        objective=objective,
        acceptance=acceptance,
        run_label=run_label,
    )
    dispatcher = WorkflowLaunchDispatcher(
        stagegraph=None,
        goal_directed=GoalDirectedLaunchService(run_control, control_plane),
        run_control=run_control,
        control_plane=control_plane,
    )
    prepared = await dispatcher.prepare(
        REQUEST_SCOPE,
        run_id,
        initial_goal=objective,
        orchestration_authority_ref="authority:goal-directed-live",
    )
    if not isinstance(prepared, GoalDirectedRunInput):
        raise RuntimeError("dispatcher did not select the admitted GoalDirected family")
    run_root = ARTIFACT_ROOT / run_label
    workspace_service, goal_workspace = create_live_goal_workspace(
        base_path=run_root / "host-workspace",
        namespace_id=f"run/{run_id}/goal",
        run_id=run_id,
        objective=objective,
        acceptance_contract=acceptance,
        protected_scope_digest=prepared.protected_scope_digest,
    )
    session_database = run_root / "agent-sessions.sqlite"
    session_database.parent.mkdir(parents=True, exist_ok=True)
    executor = LiveGoalIterationExecutor(
        run_id=run_id,
        request_scope=REQUEST_SCOPE,
        configuration_digest=configuration.digest,
        workspace_template_ref=workspace_record.ref,
        image_digest=image.id,
        openai_api_key=settings.openai_api_key.get_secret_value(),
        tavily_api_key=tavily_secret.get_secret_value(),
        workspace_service=workspace_service,
        workspace=goal_workspace,
        session_database=session_database,
        direct_smoke=args.mode == "smoke",
    )
    verifier = OpenAIGoalIndependentVerifier(
        executor=executor,
        acceptance_contract=acceptance,
        minimum_iterations=2,
    )
    actor = ActorContext(
        actor_id="goal-directed-live-worker",
        authority_refs=frozenset({"authority:goal-directed-live"}),
        permissions=frozenset(ACTION_PERMISSIONS.values()),
    )
    lifecycle = RunControlLifecycleGateway(
        run_control,
        F1OrchestrationBindingVerifier(control_plane),
        actor,
    )
    activities = GoalDirectedActivities(
        executor=executor,
        verifier=verifier,
        handoffs=GoalWorkspaceHandoffPreparer(executor),
        lifecycle=lifecycle,
    )
    plugin = OpenAIAgentsPlugin(register_activities=False)
    client = await create_temporal_client(settings, plugins=[plugin])
    task_queue = f"{settings.temporal_task_queue}-goal-directed-live"
    workflow_id = f"goal-directed-{run_label}"
    worker = create_goal_directed_worker(
        client,
        task_queue=task_queue,
        activities=activities,
    )
    try:
        async with worker:
            handle = await client.start_workflow(
                GoalDirectedWorkflow.run,
                prepared,
                id=workflow_id,
                task_queue=task_queue,
            )
            result = await handle.result()
            temporal_run_id = handle.result_run_id or handle.first_execution_run_id or ""
    finally:
        await executor.aclose()

    if result.stop_reason != "verified_completion":
        failures = tuple(
            item.irrecoverable_failure_ref
            for item in result.execution_results
            if item.irrecoverable_failure_ref
        )
        raise RuntimeError(
            f"live GoalDirected run stopped as {result.stop_reason}; failures={failures}"
        )
    if result.goal_iterations < 2 or result.rollover_count < 1:
        raise RuntimeError("live GoalDirected run did not prove a fresh-session handoff")
    if not result.execution_results or not all(
        item.actual_usage.get("tokens.total", 0) > 0 for item in result.execution_results
    ):
        raise RuntimeError("live GoalDirected run has no provider-observed token usage")
    final_identity = result.execution_results[-1].identity.semantic_key
    report_source = executor.report_paths[final_identity]
    report_path = run_root / "final-report.md"
    shutil.copyfile(report_source, report_path)
    manifest_path = run_root / "acceptance.json"
    write_live_acceptance_manifest(
        manifest_path,
        workflow_id=workflow_id,
        temporal_run_id=temporal_run_id,
        configuration_digest=configuration.digest,
        blueprint_digest=prepared.blueprint_digest,
        tavily_skill_digest=digest_bytes(TAVILY_BEST_PRACTICES_SKILL),
        result=asdict(result),
        report_path=report_path.resolve(),
    )
    print(f"GOAL_DIRECTED_LIVE_OK mode={args.mode}")
    print(f"workflow_id={workflow_id}")
    print(f"temporal_run_id={temporal_run_id}")
    print(f"goal_iterations={result.goal_iterations}")
    print(f"rollover_count={result.rollover_count}")
    print(f"report={report_path.resolve()}")
    print(f"acceptance={manifest_path.resolve()}")


if __name__ == "__main__":
    asyncio.run(main())
