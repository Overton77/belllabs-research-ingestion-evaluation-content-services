"""Biotech-only credentials and schema authority configuration (explicit opt-in)."""

from pydantic import SecretStr

from mission_control.bootstrap.settings import Settings


class BiotechSettings(Settings):
    neo4j_uri: str | None = None
    neo4j_aura_username: str | None = None
    neo4j_aura_password: SecretStr | None = None
    schema_deployment_issuer_authority_ref: str = "issue-12:graph-schema-deployment-service"
    schema_workspace_issuer_authority_ref: str = "issue-13:schema-workspace-materialization-service"
    graph_capability_authority_ref: str = "graph-authority:read-capability-service"
    schema_workspace_materializer_version: str = "issue-13-materializer-v1"


def get_settings() -> BiotechSettings:
    """Construct explicit domain settings without mutating the kernel singleton."""
    return BiotechSettings()
