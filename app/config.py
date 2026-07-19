from __future__ import annotations

from functools import lru_cache
from pathlib import Path

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

    supabase_url: str
    supabase_publishable_key: SecretStr
    supabase_secret_key: SecretStr

    openai_api_key: SecretStr
    openai_model: str = "gpt-5.4-nano"

    mongodb_uri: SecretStr
    mongodb_database: str = "belllabsbiotech"

    neo4j_uri: str = Field(validation_alias=AliasChoices("NEO4J_URI", "NEO$J_URI"))
    neo4j_aura_username: str
    neo4j_aura_password: SecretStr

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
    def cors_origins(self) -> list[str]:
        return [
            origin.strip() for origin in self.socketio_cors_origins.split(",") if origin.strip()
        ]


@lru_cache
def get_settings() -> Settings:
    return Settings()
