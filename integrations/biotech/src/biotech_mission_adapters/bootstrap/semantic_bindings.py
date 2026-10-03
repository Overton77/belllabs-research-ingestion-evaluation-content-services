from mission_control.application.coordinator.coordinator_launch import SemanticBindingProvider
from mission_control.application.coordinator.coordinator_semantic_bindings import (
    WorkflowSemanticBindingProviderRouter,
)

SCHEMA_CONTEXT_WORKFLOW = "schema-context-selection"
SUPPORTING_GRAPH_WORKFLOW = "supporting-graph-reconciliation"
WEB_RESEARCH_WORKFLOW = "web-research-browser-verification"


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
