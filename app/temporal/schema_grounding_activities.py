from __future__ import annotations

from pydantic import BaseModel, ConfigDict
from temporalio import activity
from temporalio.client import Client
from temporalio.worker import Worker

from app.application.schema_catalog import SchemaCatalog
from app.application.schema_catalog_build import SchemaCatalogBuildService
from app.application.schema_context_derivation import SchemaContextDerivationService
from app.application.supporting_graph_reconciliation import (
    SupportingGraphReconciliationWorkflow,
)
from app.domain.schema_context.contracts import (
    AcceptedSchemaContextSelection,
    GraphReconciliationEvidence,
)
from app.domain.schema_grounding.contracts import (
    SchemaCatalogBuildRecord,
    SchemaCatalogBuildRequest,
    SchemaContextDerivationResult,
    SupportingGraphReconciliationRecord,
    SupportingGraphReconciliationRequest,
)
from app.temporal.workflow_sandbox import coordinator_workflow_runner


class ActivityInput(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class CatalogBuildActivityInput(ActivityInput):
    request: SchemaCatalogBuildRequest
    schema_definition: bytes
    semantic_overlay: bytes
    report_seed: bytes = b""


class SchemaDerivationActivityInput(ActivityInput):
    request_scope: str
    run_id: str
    accepted: AcceptedSchemaContextSelection
    catalog: SchemaCatalog
    purpose: str = "read_query_reconciliation"
    live_indexes: tuple[dict, ...] = ()
    allow_vector: bool = False


class SupportingReconciliationActivityInput(ActivityInput):
    request: SupportingGraphReconciliationRequest
    evidence: GraphReconciliationEvidence | None = None


class SchemaGroundingActivities:
    """Nondeterministic services invoked by the published StageGraph operations."""

    def __init__(
        self,
        *,
        catalog_builds: SchemaCatalogBuildService,
        derivations: SchemaContextDerivationService,
        reconciliations: SupportingGraphReconciliationWorkflow,
    ) -> None:
        self._catalog_builds = catalog_builds
        self._derivations = derivations
        self._reconciliations = reconciliations

    @activity.defn(name="schema_grounding.build_catalog")
    async def build_catalog(
        self,
        value: CatalogBuildActivityInput,
    ) -> SchemaCatalogBuildRecord:
        return await self._catalog_builds.build(
            value.request,
            schema_definition=value.schema_definition,
            semantic_overlay=value.semantic_overlay,
            report_seed=value.report_seed,
        )

    @activity.defn(name="schema_grounding.derive_context")
    async def derive_context(
        self,
        value: SchemaDerivationActivityInput,
    ) -> SchemaContextDerivationResult:
        return await self._derivations.derive(
            request_scope=value.request_scope,
            run_id=value.run_id,
            accepted=value.accepted,
            catalog=value.catalog,
            purpose=value.purpose,
            live_indexes=value.live_indexes,
            allow_vector=value.allow_vector,
        )

    @activity.defn(name="schema_grounding.reconcile")
    async def reconcile(
        self,
        value: SupportingReconciliationActivityInput,
    ) -> SupportingGraphReconciliationRecord:
        return await self._reconciliations.run(
            value.request,
            evidence=value.evidence,
        )


def create_schema_grounding_activity_worker(
    client: Client,
    *,
    task_queue: str,
    activities: SchemaGroundingActivities,
) -> Worker:
    return Worker(
        client,
        task_queue=task_queue,
        workflows=[],
        workflow_runner=coordinator_workflow_runner(),
        activities=[
            activities.build_catalog,
            activities.derive_context,
            activities.reconcile,
        ],
    )
