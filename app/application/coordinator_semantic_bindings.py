from __future__ import annotations

from collections.abc import Mapping

from app.application.coordinator_launch import SemanticBindingProvider
from app.application.orchestration_routing import SemanticRoutingError
from app.domain.control_plane.contracts import EffectiveRunConfiguration
from app.domain.coordinator.launch import (
    PreparedLaunchTicket,
    SemanticBindingPlan,
    WorkflowLaunchProposal,
)
from app.domain.orchestration.bindings import RunSemanticInputBinding

SCHEMA_CONTEXT_WORKFLOW = "schema-context-selection"
SUPPORTING_GRAPH_WORKFLOW = "supporting-graph-reconciliation"
WEB_RESEARCH_WORKFLOW = "web-research-browser-verification"


class WorkflowSemanticBindingProviderRouter:
    """Route semantic binding authorship only by frozen Workflow Type identity."""

    def __init__(self, providers: Mapping[str, SemanticBindingProvider]) -> None:
        if not providers or any(not workflow_id for workflow_id in providers):
            raise ValueError("semantic binding routes require non-empty Workflow Type ids")
        self._providers = dict(providers)

    async def prepare(
        self,
        proposal: WorkflowLaunchProposal,
        configuration: EffectiveRunConfiguration,
    ) -> SemanticBindingPlan:
        workflow_id = configuration.workflow_type.logical_id
        provider = self._provider(workflow_id)
        plan = await provider.prepare(proposal, configuration)
        if plan.blueprint_family.value != configuration.selected_blueprint.family:
            raise SemanticRoutingError(
                "routed semantic binding provider returned a different blueprint family"
            )
        return plan

    async def author(
        self,
        plan: SemanticBindingPlan,
        ticket: PreparedLaunchTicket,
        *,
        run_id: str,
    ) -> RunSemanticInputBinding:
        provider = self._provider(ticket.workflow_type_ref.logical_id)
        binding = await provider.author(plan, ticket, run_id=run_id)
        if (
            binding.request_scope != ticket.request_scope
            or binding.run_id != run_id
            or binding.effective_configuration_digest != ticket.effective_configuration_digest
            or binding.blueprint_digest != ticket.blueprint_ref.digest
            or binding.blueprint_family != ticket.blueprint_family.value
        ):
            raise SemanticRoutingError(
                "routed semantic binding provider returned authority for a different run"
            )
        return binding

    def _provider(self, workflow_id: str) -> SemanticBindingProvider:
        provider = self._providers.get(workflow_id)
        if provider is None:
            raise SemanticRoutingError(
                f"no semantic binding provider is registered for Workflow Type: {workflow_id}"
            )
        return provider


def build_experiment_semantic_binding_provider(
    *,
    schema_context: SemanticBindingProvider,
    supporting_graph: SemanticBindingProvider,
    web_research: SemanticBindingProvider,
) -> WorkflowSemanticBindingProviderRouter:
    """Compose the production Scenario A/C/D providers behind one launch port."""

    return WorkflowSemanticBindingProviderRouter(
        {
            SCHEMA_CONTEXT_WORKFLOW: schema_context,
            SUPPORTING_GRAPH_WORKFLOW: supporting_graph,
            WEB_RESEARCH_WORKFLOW: web_research,
        }
    )


__all__ = [
    "SCHEMA_CONTEXT_WORKFLOW",
    "SUPPORTING_GRAPH_WORKFLOW",
    "WEB_RESEARCH_WORKFLOW",
    "WorkflowSemanticBindingProviderRouter",
    "build_experiment_semantic_binding_provider",
]
