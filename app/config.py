from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import AliasChoices, Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_ROOT = Path(__file__).resolve().parents[1]


class Settings(BaseSettings):
    """PRE-EMPTIVE SETUP: typed environment contract for future implementation agents."""

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

    supabase_url: str
    supabase_publishable_key: SecretStr
    supabase_secret_key: SecretStr

    openai_api_key: SecretStr
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

    mongodb_uri: SecretStr
    mongodb_database: str = "belllabsbiotech"
    operation_binding_write_authority: Literal["legacy", "v2"] = "legacy"
    operation_binding_legacy_read_fallback: bool = False
    legacy_operation_journal_read_fallback: bool = False
    redis_url: SecretStr = SecretStr("redis://localhost:16379/0")
    runtime_realtime_required: bool = False
    runtime_approval_timeout_seconds: int = Field(default=900, ge=30, le=86_400)
    runtime_checkpoint_signing_key: SecretStr | None = None

    neo4j_uri: str = Field(validation_alias=AliasChoices("NEO4J_URI", "NEO$J_URI"))
    neo4j_aura_username: str
    neo4j_aura_password: SecretStr
    schema_deployment_issuer_authority_ref: str = "issue-12:graph-schema-deployment-service"
    schema_workspace_issuer_authority_ref: str = "issue-13:schema-workspace-materialization-service"
    graph_capability_authority_ref: str = "graph-authority:read-capability-service"
    schema_workspace_materializer_version: str = "issue-13-materializer-v1"

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
        if self.application_migration_database_direct is not None:
            return self.application_migration_database_direct.get_secret_value()
        return self.application_postgres_dsn

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
        return self.coordinator_mcp_jwt_issuer or (f"{self.supabase_url.rstrip('/')}/auth/v1")

    @property
    def checkpoint_signing_key(self) -> bytes:
        secret = self.runtime_checkpoint_signing_key or self.supabase_secret_key
        return secret.get_secret_value().encode()


@lru_cache
def get_settings() -> Settings:
    return Settings()
