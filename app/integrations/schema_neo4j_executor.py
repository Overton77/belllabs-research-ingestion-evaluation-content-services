from __future__ import annotations

from typing import Any

from app.config import Settings
from app.domain.schema_grounding.contracts import (
    GraphAdmissionDecision,
    GraphAdmissionRequest,
)
from app.integrations.neo4j import create_neo4j
from app.integrations.neo4j_read_executor import Neo4jReadExecutor


class ManagedNeo4jReadExecutor:
    def __init__(self, driver: Any, *, database: str) -> None:
        self._driver = driver
        self._executor = Neo4jReadExecutor(driver, database=database)

    async def execute(self, intent: Any, projection: Any) -> Any:
        return await self._executor.execute(intent, projection)

    async def close(self) -> None:
        await self._driver.close()


class Neo4jBoundedReadExecutorFactory:
    """Create the graph client only after canonical admission accepted all three gates."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    async def create(
        self,
        admission: GraphAdmissionRequest,
        decision: GraphAdmissionDecision,
    ) -> ManagedNeo4jReadExecutor:
        capability = admission.graph_capability
        if (
            not decision.admitted
            or admission.deployment_manifest is None
            or admission.workspace_binding is None
            or capability is None
            or not capability.admitted
            or decision.deployment_manifest_id != admission.deployment_manifest.manifest_id
            or decision.workspace_binding_id != admission.workspace_binding.binding_id
            or decision.graph_capability_grant_id != capability.grant_id
            or decision.schema_definition_digest != admission.schema_definition_digest
            or decision.projection_digest != admission.projection_digest
        ):
            raise RuntimeError("Neo4j client creation requires a fully admitted graph request")
        driver = await create_neo4j(self._settings)
        return ManagedNeo4jReadExecutor(driver, database=admission.database)
