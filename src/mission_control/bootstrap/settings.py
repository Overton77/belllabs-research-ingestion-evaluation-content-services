from __future__ import annotations

import hmac
import os
from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# Deployment resources are selected by the operator, never relative to site-packages.
# Local commands use their working directory; installed deployments set this root.
PROJECT_ROOT = Path(os.environ.get("MISSION_CONTROL_HOME", str(Path.cwd()))).resolve()
# The private LangGraph saver/store schema (adapters/deep_agents/persistence.RUNTIME_SCHEMA;
# imported lazily there because the deep_agents package imports these settings).
RUNTIME_CHECKPOINT_SCHEMA = "mission_control_runtime"


class IntegrationConfigurationError(ValueError):
    """A selected optional adapter lacks its deployment configuration."""


class Settings(BaseSettings):
    """Deployment settings; optional integrations validate credentials when selected."""

    model_config = SettingsConfigDict(
        env_file=PROJECT_ROOT / ".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    database_direct: SecretStr | None = None
    database_url: SecretStr | None = None
    application_database_direct: SecretStr | None = None
    application_database_url: SecretStr | None = None
    application_migration_database_direct: SecretStr | None = None
    application_backfill_database_direct: SecretStr | None = None
    application_family_writer_database_direct: SecretStr | None = None

    supabase_url: str | None = None
    supabase_publishable_key: SecretStr | None = None
    supabase_secret_key: SecretStr | None = None

    openai_api_key: SecretStr | None = None
    anthropic_api_key: SecretStr | None = None
    anthropic_model: str = "claude-haiku-4.5"
    openai_model: str = "gpt-5.4-nano"
    firecrawl_api_key: SecretStr | None = None
    tavily_api_key: SecretStr | None = None

    langsmith_api_key: SecretStr | None = None
    langsmith_tracing: bool = False
    langsmith_endpoint: str = "https://api.smith.langchain.com"
    langsmith_project: str = "BellLabsBiotech"
    langsmith_workspace_id: str | None = None
    agent_server_langsmith_project: str = "BellLabsBiotech-AgentServer-Local"
    bell_labs_trace_pseudonym_key: SecretStr | None = None

    langgraph_runtime_enabled: bool = False
    async_subagent_spawning_enabled: bool = False
    bell_labs_environment: Literal["development", "staging", "production"] = "development"
    agent_server_endpoint: str = "http://127.0.0.1:2024"
    agent_server_api_key: SecretStr | None = None
    agent_server_goal_directed_id: Literal["belllabs_goal_directed"] = "belllabs_goal_directed"
    agent_server_deployment_endpoint_id: str | None = None
    agent_server_deployment_revision: str | None = None
    bell_labs_agent_auth_issuer: str | None = None
    bell_labs_agent_auth_audience: str = "authenticated"
    bell_labs_agent_auth_jwks_uri: str | None = None
    bell_labs_agent_auth_public_key: SecretStr | None = None
    bell_labs_agent_auth_algorithm: Literal["RS256", "ES256"] = "RS256"

    coordinator_mcp_enabled: bool = False
    coordinator_mcp_mount_path: str = Field(
        default="/mcp/coordinator",
        min_length=2,
    )
    coordinator_mcp_jwt_issuer: str | None = None
    coordinator_mcp_jwt_audience: str = Field(default="authenticated", min_length=1)
    coordinator_standalone_mode: Literal["read-only"] = "read-only"
    coordinator_request_timeout_seconds: float = Field(default=30, ge=1, le=120)
    coordinator_max_request_bytes: int = Field(
        default=131_072,
        ge=1_024,
        le=1_000_000,
    )
    coordinator_max_response_bytes: int = Field(
        default=1_000_000,
        ge=1_024,
        le=4_000_000,
    )
    coordinator_max_concurrency: int = Field(default=16, ge=1, le=256)
    coordinator_requests_per_minute: int = Field(default=120, ge=1, le=10_000)
    capability_search_enabled: bool = False
    external_capability_discovery_enabled: bool = False
    coordinator_launch_enabled: bool = False
    capability_embedding_model: Literal["text-embedding-3-small"] = "text-embedding-3-small"
    capability_embedding_dimensions: Literal[1536] = 1536
    # FT-A3: the Model Profile naming the catalog search embedding route. Unset means the
    # public catalog search runs lexical-only (never unavailable for a missing route).
    capability_embedding_profile: Literal["embedding.openai.text-embedding-3-small"] | None = None
    capability_projection_lease_seconds: int = Field(
        default=120,
        ge=15,
        le=900,
    )
    capability_projection_max_attempts: int = Field(default=6, ge=1, le=20)
    capability_projection_base_backoff_seconds: int = Field(
        default=5,
        ge=1,
        le=300,
    )
    capability_projection_max_backoff_seconds: int = Field(
        default=900,
        ge=5,
        le=86_400,
    )
    capability_projection_batch_size: int = Field(default=64, ge=1, le=256)
    coordinator_launch_ticket_ttl_seconds: int = Field(
        default=900,
        ge=60,
        le=3_600,
    )
    external_discovery_request_timeout_seconds: float = Field(
        default=10.0,
        ge=1.0,
        le=60.0,
    )
    external_discovery_command_timeout_seconds: float = Field(
        default=30.0,
        ge=1.0,
        le=120.0,
    )
    external_discovery_max_output_bytes: int = Field(
        default=1_000_000,
        ge=1_024,
        le=10_000_000,
    )
    external_discovery_max_results: int = Field(default=25, ge=1, le=100)
    external_discovery_max_pages: int = Field(default=5, ge=1, le=20)
    external_discovery_max_retries: int = Field(default=2, ge=0, le=5)
    mcp_registry_base_url: str = "https://registry.modelcontextprotocol.io"
    mcp_registry_api_version: Literal["v0.1"] = "v0.1"
    npx_skills_executable: str = Field(default="npx", min_length=1)
    npx_skills_package_version: str = Field(
        default="1.5.20",
        pattern=r"^\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?$",
    )
    web_research_firecrawl_mcp_command: Path | None = None
    web_research_firecrawl_mcp_arguments: tuple[str, ...] = ()
    web_research_firecrawl_mcp_module: Path = (
        PROJECT_ROOT.parent
        / ".tools"
        / "reviewed"
        / "firecrawl-mcp-7232b6d1cdd80335107d53a33b80c902b515a334"
        / "dist"
        / "index.js"
    )
    web_research_tavily_mcp_command: Path | None = None
    web_research_tavily_mcp_arguments: tuple[str, ...] = ()
    web_research_tavily_mcp_module: Path = (
        PROJECT_ROOT.parent / ".tools" / "node_modules" / "tavily-mcp" / "build" / "index.js"
    )
    web_research_agent_browser_node: Path | None = None
    web_research_agent_browser_entrypoint: Path = (
        PROJECT_ROOT.parent
        / ".tools"
        / "node_modules"
        / "agent-browser"
        / "bin"
        / "agent-browser.js"
    )
    web_research_mcp_timeout_seconds: float = Field(default=30, ge=5, le=120)
    web_research_browser_timeout_seconds: float = Field(default=90, ge=10, le=300)
    web_research_browser_command_timeout_seconds: float = Field(
        default=25,
        ge=5,
        le=60,
    )
    web_research_max_provider_output_bytes: int = Field(
        default=1_000_000,
        ge=16_384,
        le=10_000_000,
    )
    web_research_max_browser_output_bytes: int = Field(
        default=250_000,
        ge=16_384,
        le=2_000_000,
    )

    # RRM-009: production runtime composition. The API composes Temporal-backed inspection,
    # boundary delivery (with its relay), the governed launch path and the generic artifact
    # submitter only when this is set; the worker verifies the Search Attributes at
    # readiness and composes the deployment `WorkerActivityCompositionFactory` when
    # COORDINATOR_LAUNCH_ENABLED is set.
    run_control_temporal_enabled: bool = False
    boundary_relay_interval_seconds: float = Field(default=5.0, ge=0.5, le=300)
    # Scopes the delivery relay re-drives; forced RLS confines every read to one scope.
    boundary_relay_request_scopes: tuple[str, ...] = ()
    # One shared key for multi-replica inspection pagination (>= 16 bytes). When absent the
    # checkpoint signing key derives it, which is also shared by every replica.
    inspection_cursor_key: SecretStr | None = None
    # REQ-CP-DA-004 (clarified): the persistent, registered LangGraph saver and store live
    # in the private `mission_control_runtime` schema, provisioned only by
    # `mission-db runtime-apply` (packages/mission-control-db-contract/runtime). There is no
    # fallback to `belllabs_langgraph` or `public`. `setup` is a TEST-ONLY vendor setup()
    # switch for disposable schemas; it is refused for the runtime schema and in production.
    langgraph_checkpoint_database_direct: SecretStr | None = None
    langgraph_checkpoint_schema: str = Field(
        default=RUNTIME_CHECKPOINT_SCHEMA, pattern=r"^[a-z_][a-z0-9_]{0,62}$"
    )
    langgraph_checkpoint_setup: bool = False
    # The exact checkpointer/store definition digests the deployment serves with that saver
    # and store (`ExactComponentRegistry` keys); bindings naming any other digest refuse.
    deep_agent_checkpointer_digests: tuple[str, ...] = ()
    deep_agent_store_digests: tuple[str, ...] = ()
    # Deployment-stable journal claimant (RRM-004): a per-worker value would make the
    # replacement worker's claim replay conflict.
    operation_journal_claimed_by: str = Field(default="operation-runtime:belllabs", min_length=1)
    # Content-addressed artifact and result payloads when no S3 bucket is configured: a
    # directory every worker and the API can reach (a local object-store stand-in).
    artifact_payload_root: Path | None = None
    async_subagent_submitter_identity: str = Field(default="belllabs-async-submitter", min_length=1)
    # How long the parent operation boundary waits for its async children to finish before
    # settling the parent; an unfinished child stays an unsettled effect of the run.
    async_subagent_completion_wait_seconds: float = Field(default=120.0, ge=0, le=3_600)
    # RRM-008 composed by RRM-009: the heartbeat timeout of `operation.execute`/`cancel` per
    # operation class. A cancel reaches running cognition within about 0.8 * timeout (the SDK
    # heartbeat throttle); a unit holding async children is cancelled sooner because its
    # children keep spending until they are cancelled. Every worker's graceful shutdown must
    # be shorter than the shortest of them (checked when the worker set is composed).
    operation_heartbeat_timeout_seconds: int = Field(default=30, ge=1, le=3_600)
    operation_async_children_heartbeat_timeout_seconds: int = Field(default=15, ge=1, le=3_600)
    operation_bound_heartbeat_timeout_seconds: int = Field(default=30, ge=1, le=3_600)
    worker_graceful_shutdown_seconds: float = Field(default=10.0, ge=0, le=3_600)
    # Digest/revision pins of the search and browser capabilities the deployment mounts.
    capability_pins_path: Path = (
        PROJECT_ROOT / "infra" / "capability-pins" / "research-capabilities.json"
    )
    capability_bundle_backend: Literal["local", "supabase"] = "local"
    capability_bundle_namespace: str | None = None
    # FT-A2: custody credentials (never the service key). The publisher holds INSERT+SELECT
    # on the application prefix of `capability-bundles`; the reader holds SELECT only.
    capability_bundle_publisher_token: SecretStr | None = None
    capability_bundle_reader_token: SecretStr | None = None
    capability_bundle_local_root: Path | None = None
    deep_agent_sandbox_workspace_root: Path | None = None

    mission_control_catalog_scope: str | None = None
    redis_url: SecretStr = SecretStr("redis://localhost:16379/0")
    runtime_realtime_required: bool = False
    runtime_approval_timeout_seconds: int = Field(default=900, ge=30, le=86_400)
    runtime_checkpoint_signing_key: SecretStr | None = None

    aws_region: str = "us-east-1"
    aws_profile: str | None = "default"
    s3_bucket: str | None = None

    temporal_address: str = "localhost:7233"
    temporal_namespace: str = "default"
    temporal_task_queue: str = "biotech-research-ingestion"
    sandbox_image: str = "python:3.12-slim"

    api_host: str = "127.0.0.1"
    api_port: int = 8000
    socketio_cors_origins: str = "http://localhost:3000,http://127.0.0.1:3000"

    @property
    def postgres_dsn(self) -> str:
        value = self.database_direct or self.database_url
        if value is None:
            raise ValueError("DATABASE_DIRECT or DATABASE_URL is required")
        return value.get_secret_value()

    @property
    def application_postgres_dsn(self) -> str:
        value = self.application_database_direct or self.application_database_url
        if value is None:
            raise ValueError("APPLICATION_DATABASE_DIRECT or APPLICATION_DATABASE_URL is required")
        return value.get_secret_value()

    @property
    def has_application_postgres(self) -> bool:
        return (
            self.application_database_direct is not None
            or self.application_database_url is not None
        )

    @property
    def application_migration_postgres_dsn(self) -> str:
        # Migration authority is never inferred from the restricted runtime credential.
        if self.application_migration_database_direct is None:
            raise ValueError("APPLICATION_MIGRATION_DATABASE_DIRECT is required for migrations")
        return self.application_migration_database_direct.get_secret_value()

    @property
    def application_backfill_postgres_dsn(self) -> str:
        if self.application_backfill_database_direct is None:
            raise ValueError("APPLICATION_BACKFILL_DATABASE_DIRECT is required for backfill")
        return self.application_backfill_database_direct.get_secret_value()

    @property
    def has_application_family_writer_postgres(self) -> bool:
        return self.application_family_writer_database_direct is not None

    @property
    def application_family_writer_postgres_dsn(self) -> str:
        if self.application_family_writer_database_direct is None:
            raise ValueError(
                "APPLICATION_FAMILY_WRITER_DATABASE_DIRECT is required for atomic family admission"
            )
        return self.application_family_writer_database_direct.get_secret_value()

    @property
    def cors_origins(self) -> list[str]:
        return [
            origin.strip() for origin in self.socketio_cors_origins.split(",") if origin.strip()
        ]

    @property
    def coordinator_jwt_issuer(self) -> str:
        if self.coordinator_mcp_jwt_issuer and self.coordinator_mcp_jwt_issuer.strip():
            return self.coordinator_mcp_jwt_issuer
        if self.supabase_url and self.supabase_url.strip():
            return f"{self.supabase_url.rstrip('/')}/auth/v1"
        raise IntegrationConfigurationError(
            "COORDINATOR_MCP_JWT_ISSUER or SUPABASE_URL is required for coordinator authentication"
        )

    @property
    def checkpoint_signing_key(self) -> bytes:
        secret = self.runtime_checkpoint_signing_key or self.supabase_secret_key
        if secret is None or not secret.get_secret_value().strip():
            raise IntegrationConfigurationError(
                "RUNTIME_CHECKPOINT_SIGNING_KEY is required for checkpoint signing "
                "when no Supabase signing fallback is configured"
            )
        return secret.get_secret_value().encode()

    @model_validator(mode="after")
    def _checkpoint_runtime_binding(self) -> Settings:
        from mission_control.adapters.deep_agents.persistence import (
            FORBIDDEN_CHECKPOINT_SCHEMAS,
            RUNTIME_SCHEMA,
        )

        schema = self.langgraph_checkpoint_schema
        if schema in FORBIDDEN_CHECKPOINT_SCHEMAS:
            raise ValueError("LANGGRAPH_CHECKPOINT_SCHEMA names a forbidden fallback schema")
        if self.langgraph_checkpoint_setup and schema == RUNTIME_SCHEMA:
            raise ValueError(
                "mission_control_runtime is provisioned only by mission-db runtime-apply"
            )
        if self.bell_labs_environment == "production" and (
            schema != RUNTIME_SCHEMA or self.langgraph_checkpoint_setup
        ):
            raise ValueError(
                "production checkpoints require mission_control_runtime without vendor setup"
            )
        return self

    @property
    def langgraph_checkpoint_dsn(self) -> str:
        """The saver/store conninfo with exactly one `options=-c search_path=<schema>,pg_temp`.

        A configured DSN that already carries `options` or a search_path is rejected
        (`RuntimeConninfoError`), never silently overridden.
        """

        from mission_control.adapters.deep_agents.persistence import (
            runtime_checkpoint_conninfo,
        )

        value = (
            self.langgraph_checkpoint_database_direct.get_secret_value()
            if self.langgraph_checkpoint_database_direct is not None
            else self.application_postgres_dsn
        )
        return runtime_checkpoint_conninfo(value, self.langgraph_checkpoint_schema)

    @property
    def inspection_cursor_secret(self) -> bytes:
        """The shared inspection cursor key: configured, or derived from the signing key."""

        if self.inspection_cursor_key is not None:
            return self.inspection_cursor_key.get_secret_value().encode()
        return hmac.new(
            self.checkpoint_signing_key, b"belllabs.inspection-cursor.v1", "sha256"
        ).digest()


@lru_cache
def get_settings() -> Settings:
    return Settings()
