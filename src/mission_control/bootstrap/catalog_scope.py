"""Resolve operator catalog scope from trusted deployment configuration."""

from mission_control.bootstrap.settings import IntegrationConfigurationError, Settings


def configured_catalog_scope(settings: Settings, *, requested: str | None = None) -> str:
    scope = settings.mission_control_catalog_scope
    if scope is None or not scope.strip():
        raise IntegrationConfigurationError(
            "MISSION_CONTROL_CATALOG_SCOPE is required for catalog operations"
        )
    if requested is not None and requested != scope:
        raise IntegrationConfigurationError(
            "requested catalog scope differs from the configured installation"
        )
    return scope
