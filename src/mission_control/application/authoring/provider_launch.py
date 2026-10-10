"""Deployment launch bindings of the local provider lanes (MP-02 for MP-07/MP-08).

``mc.manifest_launch_bindings.v2`` is ``mc.manifest_launch_bindings.v1`` plus a ``providers``
section with one operator-reviewed entry per local provider lane profile
(``claude_agent_sdk``, ``codex``): the lane activity task queue, model profile ->
:class:`ModelPin`, auth profile -> :class:`AuthPin`, worker host profile ->
``mc.environment_binding.v1``, the exact sdk/cli/schema pins the adapter implements, typed
provider-option defaults (``ClaudeSdkOptions`` / ``CodexAppServerOptions``, never a free-form
bag) and the per-session budgets. Hosted profiles have no entry: their launch is refused
(MP-18/19 are evidence-blocked). Secrets are never named here; credentials resolve through the
auth route admission at lane start (MP-05).

The materialization digest a binding pins is the MP-03 Host Projection digest of the node's
capability rows for its profile, rendered exactly as the lanes render it at ``prepare``
(``RenderedProjectionSource``: ``render_host_files(rows, profile, operating_contract(op),
None, DEFAULT_KERNEL_HOOKS)`` then ``projection_digest``). The capability pins the rows come
from travel in the binding's ``pins`` under ``capability.<role>.<id>`` keys, so the worker
re-resolves the same rows (:class:`ProviderBindingRows`) and drift is ``CAPABILITY_DRIFT``.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping, Sequence
from typing import Final

from pydantic import BaseModel, ConfigDict, Field, model_validator

from mission_control.application.agentic_components.materialization import projection_digest
from mission_control.application.agentic_components.projections import render_host_files
from mission_control.application.authoring.cursor_launch import (
    CURSOR_PROFILES,
    CursorCloudLane,
    CursorLocalLane,
    WorkspaceProvision,
)
from mission_control.application.capabilities.catalog import resolved_capability
from mission_control.application.execution.harness.describe import (
    CLAUDE_BUNDLED_CLI_VERSION,
    CLAUDE_SDK_VERSION,
    CODEX_APP_SERVER_SCHEMA_SHA256,
    CODEX_CLI_VERSION,
)
from mission_control.domain.agentic_components.projection import (
    DEFAULT_KERNEL_HOOKS,
    ResolvedCapability,
)
from mission_control.domain.authoring.contracts import ExactDefinitionRef
from mission_control.domain.capabilities.pins import CapabilityPin as CatalogPin
from mission_control.domain.execution.bindings import (
    AuthPin,
    BindingBudgets,
    ClaudeSdkOptions,
    CodexAppServerOptions,
    EnvironmentBinding,
    ModelPin,
)
from mission_control.domain.execution.contracts import (
    OperationExecutionRequest,
    PromptSegment,
    PromptTrustClass,
)

LAUNCH_BINDINGS_SCHEMA_V2: Final = "mc.manifest_launch_bindings.v2"
LOCAL_PROVIDER_PROFILES: Final = ("claude_agent_sdk", "codex")
HOSTED_PROVIDER_PROFILES: Final = ("claude_cloud", "codex_cloud")
# The exact versions the composed adapters implement (application/execution/harness/describe):
# a deployment cannot pin another version than the lane was built and fixture-proven against.
REQUIRED_PINS: Final[dict[str, dict[str, str]]] = {
    "claude_agent_sdk": {
        "sdk.claude_agent_sdk": CLAUDE_SDK_VERSION,
        "cli.claude_code": CLAUDE_BUNDLED_CLI_VERSION,
    },
    "codex": {
        "cli.codex": CODEX_CLI_VERSION,
        "schema.app_server_v2": CODEX_APP_SERVER_SCHEMA_SHA256,
    },
}
CODEX_APP_SERVER_SCHEMA_VERSION: Final = f"v2@{CODEX_CLI_VERSION}"
MODEL_PROVIDER: Final[dict[str, str]] = {"claude_agent_sdk": "anthropic", "codex": "openai"}
CAPABILITY_PIN_PREFIX: Final = "capability."
# `adapters/cursor/projection.py` (the lanes' instruction channel); pinned equal by a test.
MISSION_CONTEXT_POINTER: Final = (
    "Mission context: read .mission/context.md (the context index) and .mission/inputs.json "
    "before acting; inputs are under inputs/ (read-only); write declared outputs under outputs/."
)
_INSTRUCTION_CLASSES: Final = frozenset(
    {PromptTrustClass.SYSTEM_AUTHORITY, PromptTrustClass.AUTHORED_INSTRUCTION}
)


class _File(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ProviderLaneBinding(_File):
    """One local provider lane profile's deployment binding (see module doc)."""

    task_queue: str = Field(min_length=1, max_length=255)
    model_profiles: dict[str, ModelPin] = Field(default_factory=dict)
    auth_profiles: dict[str, AuthPin] = Field(default_factory=dict)
    host_profiles: dict[str, EnvironmentBinding] = Field(default_factory=dict)
    pins: dict[str, str]
    provider_options: ClaudeSdkOptions | CodexAppServerOptions = Field(discriminator="provider")
    budgets: BindingBudgets
    agent_profile_ref: ExactDefinitionRef
    tracing_policy_ref: str = Field(min_length=1)
    sensitive_data_policy_ref: str = Field(min_length=1)
    snapshot_policy_ref: str = Field(min_length=1)
    workspace: WorkspaceProvision

    @model_validator(mode="after")
    def keyed_by_their_own_names(self) -> ProviderLaneBinding:
        for name, model in self.model_profiles.items():
            if model.profile != name:
                raise ValueError(f"model_profiles[{name}] pins profile {model.profile}")
        for name, auth in self.auth_profiles.items():
            if auth.profile != name:
                raise ValueError(f"auth_profiles[{name}] pins profile {auth.profile}")
        for name, host in self.host_profiles.items():
            if host.kind != "local_workspace" or host.host_profile != name:
                raise ValueError(
                    f"host_profiles[{name}] must be a local_workspace binding of host {name}"
                )
        if any(key.startswith(CAPABILITY_PIN_PREFIX) for key in self.pins):
            raise ValueError("capability pins come from the manifest, not the deployment file")
        return self


class ProviderLanes(_File):
    """The ``providers`` section: the local provider lanes (claude/codex, sealed into
    ``mc.execution_binding.v2``) and the Cursor lanes (sealed into ``mc.cursor_binding.v1``,
    ``cursor_launch``). Hosted claude/codex profiles have no entry. Each entry is additive and
    omitted from dumps when absent, so a file without Cursor entries reads and dumps unchanged."""

    claude_agent_sdk: ProviderLaneBinding | None = None
    codex: ProviderLaneBinding | None = None
    cursor_local: CursorLocalLane | None = Field(
        default=None, exclude_if=lambda value: value is None
    )
    cursor_cloud: CursorCloudLane | None = Field(
        default=None, exclude_if=lambda value: value is None
    )

    @model_validator(mode="after")
    def options_and_pins_match_the_adapter(self) -> ProviderLanes:
        for profile in LOCAL_PROVIDER_PROFILES:
            lane: ProviderLaneBinding | None = getattr(self, profile)
            if lane is None:
                continue
            expected = ClaudeSdkOptions if profile == "claude_agent_sdk" else CodexAppServerOptions
            if not isinstance(lane.provider_options, expected):
                raise ValueError(
                    f"providers.{profile} carries {lane.provider_options.provider} options"
                )
            if (
                isinstance(lane.provider_options, CodexAppServerOptions)
                and lane.provider_options.app_server_schema_version
                != CODEX_APP_SERVER_SCHEMA_VERSION
            ):
                raise ValueError(
                    f"providers.codex pins app-server schema "
                    f"{lane.provider_options.app_server_schema_version}; the adapter implements "
                    f"{CODEX_APP_SERVER_SCHEMA_VERSION}"
                )
            for key, version in REQUIRED_PINS[profile].items():
                if lane.pins.get(key) != version:
                    raise ValueError(
                        f"providers.{profile}.pins.{key} must be {version} (the composed "
                        f"adapter's version), not {lane.pins.get(key)}"
                    )
        return self

    def lane(self, profile: str) -> ProviderLaneBinding | None:
        if profile not in LOCAL_PROVIDER_PROFILES:
            return None
        binding: ProviderLaneBinding | None = getattr(self, profile)
        return binding

    def cursor(self, profile: str) -> CursorLocalLane | CursorCloudLane | None:
        if profile not in CURSOR_PROFILES:
            return None
        binding: CursorLocalLane | CursorCloudLane | None = getattr(self, profile)
        return binding


# --- The lanes' instruction channel and projection digest ----------------------------------------


def operating_contract_text(segments: Sequence[PromptSegment]) -> str:
    """The lanes' operating contract (``adapters.cursor.projection.operating_contract``)."""

    parts = [
        segment.content.strip()
        for segment in segments
        if segment.trust_class in _INSTRUCTION_CLASSES and segment.content.strip()
    ]
    parts.append(MISSION_CONTEXT_POINTER)
    return "\n\n".join(parts)


def materialization_digest(
    rows: Sequence[ResolvedCapability], profile: str, segments: Sequence[PromptSegment]
) -> str:
    """The MP-03 projection digest the lane recomputes at ``prepare`` (no packet index)."""

    projection = render_host_files(
        rows, profile, operating_contract_text(segments), None, DEFAULT_KERNEL_HOOKS
    )
    return projection_digest(projection)


def capability_pin_key(role: str, capability_id: str) -> str:
    return f"{CAPABILITY_PIN_PREFIX}{role}.{capability_id}"


def binding_capability_pins(pins: Mapping[str, str]) -> tuple[str, ...]:
    """The projected capability pins of a binding, in their rendering order (sorted keys)."""

    return tuple(
        dict.fromkeys(pins[key] for key in sorted(pins) if key.startswith(CAPABILITY_PIN_PREFIX))
    )


ProjectionRowsPort = Callable[[Sequence[str]], Awaitable[Mapping[str, ResolvedCapability]]]
"""Resolve exact pin texts (``id@version#sha256:...``) to projection rows (catalog + custody)."""


class CatalogProjectionRows:
    """:data:`ProjectionRowsPort` over the catalog (``resolved_capability``: definition,
    verified bundle bytes, plugin members)."""

    def __init__(self, definitions: object, *, custody: object | None = None) -> None:
        self._definitions = definitions
        self._custody = custody

    async def __call__(self, pins: Sequence[str]) -> Mapping[str, ResolvedCapability]:
        rows: dict[str, ResolvedCapability] = {}
        for text in dict.fromkeys(pins):
            rows[text] = await resolved_capability(
                self._definitions,
                CatalogPin.parse(text),
                custody=self._custody,  # type: ignore[arg-type]
            )
        return rows


class ProviderBindingRows:
    """The worker-side ``RowsResolver`` of the claude/codex lanes: the rows of the capability
    pins a ``mc.execution_binding.v2`` carries, in the order the launch author rendered them."""

    def __init__(self, rows: ProjectionRowsPort) -> None:
        self._rows = rows

    async def __call__(self, operation: OperationExecutionRequest) -> Sequence[ResolvedCapability]:
        binding = operation.provider_binding
        if binding is None:
            return ()
        pins = binding_capability_pins(binding.pins)
        if not pins:
            return ()
        resolved = await self._rows(pins)
        return tuple(resolved[text] for text in pins)


__all__ = [
    "CAPABILITY_PIN_PREFIX",
    "CODEX_APP_SERVER_SCHEMA_VERSION",
    "HOSTED_PROVIDER_PROFILES",
    "LAUNCH_BINDINGS_SCHEMA_V2",
    "LOCAL_PROVIDER_PROFILES",
    "MODEL_PROVIDER",
    "REQUIRED_PINS",
    "CatalogProjectionRows",
    "ProjectionRowsPort",
    "ProviderBindingRows",
    "ProviderLaneBinding",
    "ProviderLanes",
    "WorkspaceProvision",
    "binding_capability_pins",
    "capability_pin_key",
    "materialization_digest",
    "operating_contract_text",
]
