"""A second API process for the multi-process realtime proof (G6).

`uvicorn tests.integration.postgres.realtime_process_app:create --factory` builds the public
API exactly as production does: `bootstrap.api.create_application` for a test deployment
(real `MissionTokenVerifier`, registry, restricted pool, per-tenant stream services) wrapped
by `bootstrap.realtime.create_asgi_app` with Redis fanout and PostgreSQL LISTEN hints. Only
the identity provider is a test key (its public JWKS file is passed in). The database DSN
arrives in the process environment under the deployment's secret ref and is never printed.

`MC_REALTIME_PROCESS_SPEC` (JSON): application_id, installation_id, project_ref, tenants
(name -> uuid), jwks (path), redis_url, origin.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from uuid import UUID

from pydantic import SecretStr

from mission_control.bootstrap.api import MissionDeployment, create_application
from mission_control.bootstrap.realtime import create_asgi_app
from mission_control.bootstrap.settings import Settings

SPEC_ENV = "MC_REALTIME_PROCESS_SPEC"


def spec_database(spec: dict[str, Any]) -> Any:
    """The attributes `test_mission_socket_postgres.deployment` reads from a database."""

    return SimpleNamespace(
        application_id=spec["application_id"],
        installation_id=UUID(spec["installation_id"]),
        project_ref=spec["project_ref"],
        tenants={name: UUID(value) for name, value in spec["tenants"].items()},
    )


def realtime_settings(redis_url: str, origin: str) -> Settings:
    base = Settings(_env_file=None)  # type: ignore[call-arg]
    return base.model_copy(
        update={
            "mission_socket_redis_fanout": True,
            "mission_socket_postgres_hints": True,
            "redis_url": SecretStr(redis_url),
            "socketio_cors_origins": origin,
        }
    )


def build(spec: dict[str, Any]) -> Any:
    from tests.integration.postgres.test_mission_socket_postgres import deployment

    composed: MissionDeployment = deployment(spec_database(spec), Path(spec["jwks"]))
    app = create_application(composed)
    return create_asgi_app(
        settings=realtime_settings(spec["redis_url"], spec["origin"]),
        app=app,
        deployment=composed,
    )


def create() -> Any:
    return build(json.loads(os.environ[SPEC_ENV]))


__all__ = ["SPEC_ENV", "build", "create", "realtime_settings", "spec_database"]
