"""Deployment launch bindings of the Cursor lanes (``cursor_local``, ``cursor_cloud``).

``mc.manifest_launch_bindings.v2`` carries, beside the claude/codex entries of its ``providers``
section, one operator-reviewed entry per Cursor lane profile: the worker task queue that serves
the lane's ``lane.*`` activities, model profile -> Cursor model ID (:class:`ModelPin`), the
admitted auth profile names (the Cursor credential itself is the worker's, never named here),
the repositories (and refs) a mission may bind and the clone URL or worker path the lane uses for
each, the ``cursor-sdk`` pins the adapter implements, the session budgets and, per profile, the
loopback hook callback (local) or the published cloud environments and their typed options
(cloud). From these and the committed definition the launch author seals an exact
``mc.cursor_binding.v1`` (``domain.execution.lanes.CursorExecutionBinding``).

The binding pins the projection the Cursor harness re-renders at ``prepare``
(``adapters/cursor/projection.py``): ``RenderedProjectionSource`` over ``CatalogRows`` resolves
the binding's ``projections.skills``, ``projections.mcp_servers`` and ``inline_subagents`` (in
that order) and renders ``render_host_files(rows, profile, operating_contract(op), None,
kernel_hooks)``, with the Kernel Hooks on ``cursor_local`` and none on ``cursor_cloud`` (the VM
cannot reach the worker; ``deployment_composition.compose_cursor_cloud``). ``projection_digests``
of that rendering (rule, agents, hooks) must equal the binding's or the lane refuses with
``CAPABILITY_DRIFT``. :func:`cursor_projection_digests` is that function, pinned equal to the
adapter's by a unit test (application code does not import adapters).

What the binding cannot carry is refused at the manifest pointer, never dropped: hook scripts
and plugins (``mc.cursor_binding.v1`` has no slot for their pins and ``CatalogRows`` never
resolves them), context profiles and any other capability kind.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Final, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from mission_control.application.agentic_components.projections import render_host_files
from mission_control.domain.agentic_components.projection import (
    DEFAULT_KERNEL_HOOKS,
    HostProjection,
    KernelHook,
    ResolvedCapability,
)
from mission_control.domain.authoring.canonical import sha256_digest
from mission_control.domain.authoring.contracts import DefinitionKind, ExactDefinitionRef
from mission_control.domain.context.render import bytes_digest
from mission_control.domain.execution.bindings import AuthPin, EnvironmentBinding, ModelPin
from mission_control.domain.execution.contracts import PromptSegment, PromptTrustClass
from mission_control.domain.execution.lanes import (
    CursorBudgets,
    CursorCloudOptions,
    CursorHookCallback,
    CursorPins,
)

CURSOR_PROFILES: Final = ("cursor_local", "cursor_cloud")
# The versions the composed Cursor adapters implement (`adapters/cursor/bridge.py`
# `CURSOR_SDK_PIN`/`BRIDGE_PROTOCOL`, `adapters/cursor/cloud_api.py` `/v1`): a deployment cannot
# pin another version than the lane was built and fixture-proven against (pinned by a test).
REQUIRED_CURSOR_PINS: Final = CursorPins(
    cursor_sdk="1.0.37", bridge="1.0.37", protocol="sdk.v1", cloud_api="v1"
)
# The Kernel Hooks each composed Cursor harness renders (`deployment_composition`).
CURSOR_KERNEL_HOOKS: Final[dict[str, tuple[KernelHook, ...]]] = {
    "cursor_local": tuple(DEFAULT_KERNEL_HOOKS),
    "cursor_cloud": (),
}
# Row kinds `CatalogRows` resolves from a Cursor binding, and the binding slot each fills.
CURSOR_PROJECTED_KINDS: Final[dict[DefinitionKind, str]] = {
    DefinitionKind.SKILL: "skills",
    DefinitionKind.MCP_SERVER: "mcp_servers",
    DefinitionKind.SUBAGENT_PROFILE: "inline_subagents",
}
# `adapters/cursor/projection.py` (the lanes' instruction channel); pinned equal by a test.
_MISSION_CONTEXT_POINTER: Final = (
    "Mission context: read .mission/context.md (the context index) and .mission/inputs.json "
    "before acting; inputs are under inputs/ (read-only); write declared outputs under outputs/."
)
_INSTRUCTION_CLASSES: Final = frozenset(
    {PromptTrustClass.SYSTEM_AUTHORITY, PromptTrustClass.AUTHORED_INSTRUCTION}
)
_RULE_PATH: Final = ".cursor/rules/mc-mission.mdc"
_HOOKS_PATH: Final = ".cursor/hooks.json"


class _File(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class WorkspaceProvision(_File):
    """The workspace provider and image digests a manifest template's workspace contract
    declares (a lane leases its own worktree or provider workspace under its binding; for it
    these identify the worker image). Re-exported by ``provider_launch``."""

    provider: str = Field(min_length=1)
    runtime_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    image_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    package_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    environment_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    network_policy: Literal["none", "allowlisted"] = "none"


class CursorRepository(_File):
    """A repository a manifest may bind, keyed in the deployment by the manifest's repository
    identity (``cursor_local``: ``workspace.repo.path``, which compile requires; ``cursor_cloud``:
    ``workspace.repo.url``): the worker path (local) or clone URL (cloud) the lane works from,
    and the refs a mission may start from (the first is the default when the manifest names
    none). The lane creates ``<branch_prefix><run>`` from that ref."""

    repo_url: str = Field(min_length=1)
    base_refs: tuple[str, ...] = Field(min_length=1)
    branch_prefix: str = Field(default="mc/", min_length=1)


class CursorCloudEnvironment(_File):
    """A published Cursor cloud environment (by ``environment_ref``) and the typed options the
    cloud agent is created with."""

    environment: EnvironmentBinding
    options: CursorCloudOptions = Field(default_factory=CursorCloudOptions)

    @model_validator(mode="after")
    def hosted_by_cursor(self) -> CursorCloudEnvironment:
        if self.environment.kind != "provider_hosted" or self.environment.provider != "cursor":
            raise ValueError("a Cursor cloud environment is provider_hosted by cursor")
        if self.environment.environment_ref is None:
            raise ValueError("a Cursor cloud environment names its environment_ref")
        return self


class _CursorLane(_File):
    task_queue: str = Field(min_length=1, max_length=255)
    model_profiles: dict[str, ModelPin] = Field(default_factory=dict)
    auth_profiles: dict[str, AuthPin] = Field(default_factory=dict)
    repositories: dict[str, CursorRepository] = Field(default_factory=dict)
    pins: CursorPins
    mode: Literal["agent", "plan"] = "agent"
    budgets: CursorBudgets
    agent_profile_ref: ExactDefinitionRef
    tracing_policy_ref: str = Field(min_length=1)
    sensitive_data_policy_ref: str = Field(min_length=1)
    snapshot_policy_ref: str = Field(min_length=1)
    # The workspace provider and image digests of the manifest template's workspace contract.
    workspace: WorkspaceProvision

    @model_validator(mode="after")
    def reviewed_and_pinned(self) -> _CursorLane:
        for name, model in self.model_profiles.items():
            if model.profile != name:
                raise ValueError(f"model_profiles[{name}] pins profile {model.profile}")
        for name, auth in self.auth_profiles.items():
            if auth.profile != name:
                raise ValueError(f"auth_profiles[{name}] pins profile {auth.profile}")
        if self.pins != REQUIRED_CURSOR_PINS:
            raise ValueError(
                f"Cursor pins must be {REQUIRED_CURSOR_PINS.model_dump()} (the composed "
                f"adapter's versions), not {self.pins.model_dump()}"
            )
        return self


class CursorLocalLane(_CursorLane):
    """``providers.cursor_local``: the bridge on a worker host, leasing a git worktree from the
    repository path; the loopback Kernel Hook callback is required."""

    host_profiles: dict[str, EnvironmentBinding] = Field(default_factory=dict)
    hook_callback: CursorHookCallback
    sandbox_enabled: bool = False
    disallowed_tools: tuple[str, ...] = ()
    lease_root: str | None = Field(default=None, min_length=1)

    @model_validator(mode="after")
    def local_hosts(self) -> CursorLocalLane:
        for name, host in self.host_profiles.items():
            if host.kind != "local_workspace" or host.host_profile != name:
                raise ValueError(
                    f"host_profiles[{name}] must be a local_workspace binding of host {name}"
                )
        return self


class CursorCloudLane(_CursorLane):
    """``providers.cursor_cloud``: a Cursor-hosted agent on the run branch of a bound
    repository. ``v1_environment`` names the environment a mission/v1 node (which cannot
    select one) runs in; without it a v1 ``cursor_cloud`` node is refused."""

    environments: dict[str, CursorCloudEnvironment] = Field(default_factory=dict)
    v1_environment: str | None = Field(default=None, min_length=1)

    @model_validator(mode="after")
    def keyed_environments(self) -> CursorCloudLane:
        for name, item in self.environments.items():
            if item.environment.environment_ref != name:
                raise ValueError(f"environments[{name}] names {item.environment.environment_ref}")
        if self.v1_environment is not None and self.v1_environment not in self.environments:
            raise ValueError(f"v1_environment {self.v1_environment} is not a bound environment")
        return self


# --- The projection the Cursor harness re-renders at `prepare` ----------------------------------


def cursor_operating_contract(segments: Sequence[PromptSegment]) -> str:
    """``adapters.cursor.projection.operating_contract`` over a template's prompt segments."""

    parts = [
        segment.content.strip()
        for segment in segments
        if segment.trust_class in _INSTRUCTION_CLASSES and segment.content.strip()
    ]
    parts.append(_MISSION_CONTEXT_POINTER)
    return "\n\n".join(parts)


def cursor_projection(
    rows: Sequence[ResolvedCapability], profile: str, segments: Sequence[PromptSegment]
) -> HostProjection:
    """What ``RenderedProjectionSource`` renders for the binding at ``prepare`` (no packet
    index: the pinned projection points at ``.mission/context.md`` on disk)."""

    return render_host_files(
        rows, profile, cursor_operating_contract(segments), None, CURSOR_KERNEL_HOOKS[profile]
    )


def cursor_projection_digests(projection: HostProjection) -> dict[str, str]:
    """``adapters.cursor.projection.projection_digests``: rule, agents (with ``AGENTS.md``)
    and hooks digests (pinned equal by a test)."""

    files = {item.path: item.content for item in projection.files}
    agents = sorted(
        (path, bytes_digest(content))
        for path, content in files.items()
        if path.startswith(".cursor/agents/") or path == "AGENTS.md"
    )
    return {
        "rules_digest": bytes_digest(files.get(_RULE_PATH, b"")),
        "agents_digest": sha256_digest([list(item) for item in agents]),
        "hooks_digest": bytes_digest(files.get(_HOOKS_PATH, b"")),
    }


def cursor_rows_order(
    skills: Sequence[str], mcp_servers: Sequence[str], inline_subagents: Sequence[str]
) -> tuple[str, ...]:
    """The pin order ``CatalogRows`` resolves a binding's rows in (de-duplicated)."""

    return tuple(dict.fromkeys((*skills, *mcp_servers, *inline_subagents)))


__all__ = [
    "CURSOR_KERNEL_HOOKS",
    "CURSOR_PROFILES",
    "CURSOR_PROJECTED_KINDS",
    "REQUIRED_CURSOR_PINS",
    "CursorCloudEnvironment",
    "CursorCloudLane",
    "CursorLocalLane",
    "CursorRepository",
    "WorkspaceProvision",
    "cursor_operating_contract",
    "cursor_projection",
    "cursor_projection_digests",
    "cursor_rows_order",
]
