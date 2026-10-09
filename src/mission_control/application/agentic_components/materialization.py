from __future__ import annotations

import json
import shutil
from collections.abc import Callable, Iterable
from hashlib import sha256
from pathlib import Path, PurePosixPath
from typing import Final, Literal

from pydantic import BaseModel, ConfigDict, Field

from mission_control.application.agentic_components.projections import (
    HOOK_RUNNER_SCRIPT,
    render_release_host_files,
    skill_target_path,
)
from mission_control.application.agentic_components.repository import AgenticComponentRepository
from mission_control.domain.agentic_components.contracts import (
    AgenticComponentRelease,
    ComponentKind,
    MaterializationPlan,
    MaterializationRequest,
    MaterializationStep,
    TrustStage,
)
from mission_control.domain.agentic_components.projection import (
    HostProjection,
    ResolvedCapability,
)
from mission_control.domain.authoring.canonical import stable_json_dump
from mission_control.domain.authoring.contracts import (
    HookScriptDefinition,
    MCPServerDefinition,
    PluginDefinition,
)
from mission_control.domain.capabilities.host_support import HostSupportStatus, LaneProfile

PROJECTION_MATERIALIZATION_SCHEMA: Final = "mc.projection_materialization.v1"
_DIGEST = r"^sha256:[0-9a-f]{64}$"


class MaterializationRejected(ValueError):
    pass


# --- Host Projection into an admitted workspace (SPEC-02 "Local providers") -----------------


class MaterializedFile(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    path: str = Field(min_length=1)
    digest: str = Field(pattern=_DIGEST)
    size_bytes: int = Field(ge=0)
    mode: int = Field(ge=0, le=0o777)


class ExecutableCheck(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    command: str = Field(min_length=1)
    required_by: tuple[str, ...]
    available: bool


class ProjectionMaterialization(BaseModel):
    """`mc.projection_materialization.v1`: what was written before the session started.

    Secret-free and path-free (workspace-relative paths only). It proves which bytes are on
    disk and which launch executables resolved; it does not prove that a provider loaded them,
    which takes adapter discovery evidence (SPEC-02 "Capabilities and hook scripts").
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["mc.projection_materialization.v1"] = PROJECTION_MATERIALIZATION_SCHEMA
    lane_profile: LaneProfile
    projection_digest: str = Field(pattern=_DIGEST)
    files: tuple[MaterializedFile, ...]
    unchanged: tuple[str, ...] = ()
    executables: tuple[ExecutableCheck, ...] = ()

    @property
    def missing_executables(self) -> tuple[str, ...]:
        return tuple(item.command for item in self.executables if not item.available)


def projection_digest(projection: HostProjection) -> str:
    """Content identity of a projection: every path, mode and byte plus its send options.

    Digest-identical rows render identical bytes, so equal inputs give equal digests and any
    change to a projected file, mode or per-turn option changes the digest.
    """
    hasher = sha256()
    hasher.update(projection.profile.value.encode() + b"\0")
    for item in projection.files:
        hasher.update(item.path.encode() + b"\0" + oct(item.mode).encode() + b"\0")
        hasher.update(sha256(item.content).digest())
    hasher.update(json.dumps(projection.send_options, sort_keys=True, default=str).encode())
    return "sha256:" + hasher.hexdigest()


def required_executables(
    rows: Iterable[ResolvedCapability], profile: LaneProfile | str
) -> dict[str, tuple[str, ...]]:
    """Commands a profile's session must find on PATH: hook interpreters (and the hook runner's
    ``python`` on file lanes) and stdio MCP launchers, mapped to the capabilities needing them.
    Rows that the profile does not run (unsupported or unqualified optional members) are
    excluded the same way the Host Projection excludes them."""
    lane = LaneProfile(profile)
    needed: dict[str, list[str]] = {}

    def need(command: str, logical_id: str) -> None:
        owners = needed.setdefault(command, [])
        if logical_id not in owners:
            owners.append(logical_id)

    def visit(row: ResolvedCapability) -> None:
        definition = row.definition
        if isinstance(definition, PluginDefinition):
            optional = {member.pin for member in definition.manifest.members if member.optional}
            for member in row.members:
                support = getattr(member.definition, "host_support", None)
                runs = support is not None and support.status(lane) is HostSupportStatus.SUPPORTED
                if member.pin in optional and not runs:
                    continue
                visit(member)
            return
        if isinstance(definition, HookScriptDefinition):
            need(definition.interpreter.value, definition.logical_id)
            if lane is not LaneProfile.DEEP_AGENTS:
                need("python", f"{definition.logical_id} ({HOOK_RUNNER_SCRIPT})")
        elif isinstance(definition, MCPServerDefinition):
            overlay = definition.host_support.overlay(lane)
            transport = str(overlay.get("transport", definition.transport))
            if transport == "stdio" and definition.launch_template:
                need(definition.launch_template[0], definition.logical_id)

    for row in rows:
        visit(row)
    if lane is LaneProfile.CODEX_CLOUD:
        return {}
    return {command: tuple(owners) for command, owners in sorted(needed.items())}


def _target(root: Path, relative: str) -> Path:
    """Resolve a projected path under ``root``; refuse traversal and symlinked components."""
    raw = relative.replace("\\", "/")
    pure = PurePosixPath(raw)
    if (
        not pure.parts
        or pure.is_absolute()
        or raw.startswith("~")
        or ".." in pure.parts
        or ":" in pure.parts[0]
    ):
        raise MaterializationRejected(f"projected path escapes the workspace: {relative}")
    current = root
    for part in pure.parts:
        current = current / part
        if current.is_symlink():
            raise MaterializationRejected(f"projected path crosses a symlink: {relative}")
    resolved = current.resolve(strict=False)
    if root != resolved and root not in resolved.parents:
        raise MaterializationRejected(f"projected path resolves outside the workspace: {relative}")
    return current


def materialize_projection(
    projection: HostProjection,
    workspace_root: Path,
    *,
    expected_digest: str | None = None,
    executables: dict[str, tuple[str, ...]] | None = None,
    which: Callable[[str], str | None] = shutil.which,
    require_executables: bool = True,
) -> ProjectionMaterialization:
    """Write a Host Projection into an admitted workspace before the session starts.

    Fails closed before writing anything when the projection digest differs from the pinned
    one, a path would escape the root (traversal, absolute, drive, symlinked component), an
    existing file holds different bytes (collision; identical bytes are an idempotent re-run),
    or a required launch executable is missing.
    """
    digest = projection_digest(projection)
    if expected_digest is not None and digest != expected_digest:
        raise MaterializationRejected(
            f"projection digest {digest} differs from the pinned {expected_digest}"
        )
    root = workspace_root.resolve(strict=True)
    if not root.is_dir():
        raise MaterializationRejected("workspace root is not a directory")
    checks = tuple(
        ExecutableCheck(command=command, required_by=owners, available=which(command) is not None)
        for command, owners in sorted((executables or {}).items())
    )
    missing = [item.command for item in checks if not item.available]
    if missing and require_executables:
        raise MaterializationRejected(f"launch executable(s) not found: {', '.join(missing)}")
    plan: list[tuple[Path, bytes, int, str]] = []
    unchanged: list[str] = []
    for item in projection.files:
        target = _target(root, item.path)
        if target.exists():
            if not target.is_file() or target.read_bytes() != item.content:
                raise MaterializationRejected(
                    f"projected path collides with different existing content: {item.path}"
                )
            unchanged.append(item.path)
            continue
        plan.append((target, item.content, item.mode, item.path))
    for target, content, mode, _ in plan:
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)
        target.chmod(mode)
    return ProjectionMaterialization(
        lane_profile=projection.profile,
        projection_digest=digest,
        files=tuple(
            MaterializedFile(
                path=item.path,
                digest="sha256:" + sha256(item.content).hexdigest(),
                size_bytes=len(item.content),
                mode=item.mode,
            )
            for item in projection.files
        ),
        unchanged=tuple(unchanged),
        executables=checks,
    )


class MaterializationPlanner:
    """Compile exact component releases into a non-secret, replayable execution plan."""

    def __init__(self, repository: AgenticComponentRepository) -> None:
        self._repository = repository

    async def plan(self, request: MaterializationRequest) -> MaterializationPlan:
        releases = await self._resolve(request)
        selected_diff = (
            await self._repository.select_diff_qualification(request.model_id)
            if request.model_id
            else None
        )
        generated_files = render_release_host_files(request.host, releases)
        steps: list[MaterializationStep] = []

        def add(
            kind: str,
            description: str,
            *,
            release: AgenticComponentRelease | None = None,
            command: tuple[str, ...] = (),
            secret_refs: tuple[str, ...] = (),
        ) -> None:
            steps.append(
                MaterializationStep(
                    ordinal=len(steps) + 1,
                    kind=kind,
                    component_digest=release.coordinate.digest if release else None,
                    description=description,
                    command=command,
                    secret_refs=secret_refs,
                )
            )

        for release in releases:
            if release.plugin is not None:
                members = ", ".join(
                    f"{member.component_id}@{member.version}" for member in release.plugin.members
                )
                add(
                    "stage",
                    f"Expand plugin {release.coordinate.component_id} into members in position "
                    f"order: {members}",
                    release=release,
                )
                continue
            add(
                "retrieve",
                f"Retrieve immutable payloads for {release.coordinate.component_id}",
                release=release,
            )
            add(
                "verify",
                f"Verify release and payload digests for {release.coordinate.component_id}",
                release=release,
            )
            if release.payloads:
                add(
                    "stage",
                    f"Stage {len(release.payloads)} payload(s) in the workspace",
                    release=release,
                )
            if release.skill is not None:
                target = skill_target_path(request.host, release.skill.skill_name)
                add("stage", f"Install and verify skill manifest at {target}", release=release)
            if release.workspace is not None:
                add("provision_workspace", release.workspace.description, release=release)
            if release.hook_script is not None:
                events = ", ".join(event.value for event in release.hook_script.events)
                add(
                    "configure_host",
                    f"Install hook script {release.hook_script.hook_id} for {events} "
                    f"behind {HOOK_RUNNER_SCRIPT}; kernel hooks stay first",
                    release=release,
                )
            if release.subagent_profile is not None:
                add(
                    "configure_host",
                    f"Render subagent profile {release.subagent_profile.name}",
                    release=release,
                )
            if release.mcp is not None:
                refs = tuple(item.secret_ref for item in release.mcp.secret_environment)
                if refs:
                    add(
                        "inject_secrets",
                        "Resolve credential handles into the ephemeral process environment",
                        release=release,
                        secret_refs=refs,
                    )
                if release.mcp.transport == "stdio":
                    add(
                        "start_server",
                        f"Start MCP server {release.mcp.server_name}",
                        release=release,
                        command=(release.mcp.command or "", *release.mcp.arguments),
                    )
                add(
                    "probe_readiness",
                    f"Initialize MCP and verify frozen tool schema {release.mcp.schema_digest}",
                    release=release,
                )
            if release.sandbox is not None:
                add(
                    "seal_snapshot",
                    f"Verify sandbox snapshot {release.sandbox.snapshot_digest}",
                    release=release,
                )
        if generated_files:
            add("configure_host", f"Render {request.host.value} project configuration")

        canonical = {
            "request": stable_json_dump(request),
            "release_digests": [release.coordinate.digest for release in releases],
            "generated_file_digests": [item.digest for item in generated_files],
            "diff_qualification": (
                selected_diff.qualification_id if selected_diff is not None else None
            ),
        }
        digest = sha256(
            json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        return MaterializationPlan(
            plan_id=f"plan:sha256:{digest}",
            request=request,
            releases=tuple(release.coordinate for release in releases),
            generated_files=generated_files,
            steps=tuple(steps),
            selected_diff_qualification=selected_diff,
        )

    async def _resolve(
        self,
        request: MaterializationRequest,
    ) -> tuple[AgenticComponentRelease, ...]:
        if len(request.component_digests) != len(set(request.component_digests)):
            raise MaterializationRejected("component digests must be unique")
        releases: list[AgenticComponentRelease] = []
        seen: set[str] = set()

        async def load(digest: str, *, optional: bool = False) -> AgenticComponentRelease | None:
            release = await self._repository.get_by_digest(digest)
            if release is None:
                raise MaterializationRejected(f"unknown component digest: {digest}")
            if release.trust_stage in {TrustStage.QUARANTINED, TrustStage.REVIEWED}:
                raise MaterializationRejected(
                    f"component is not qualified for execution: {release.coordinate.component_id}"
                )
            compatible = [
                item
                for item in release.compatibility
                if item.host == request.host
                and request.operating_system in item.operating_systems
                and request.architecture in item.architectures
            ]
            if not compatible:
                if optional:
                    return None
                raise MaterializationRejected(
                    f"component is incompatible with the requested host: "
                    f"{release.coordinate.component_id}"
                )
            return release

        for digest in request.component_digests:
            release = await load(digest)
            assert release is not None
            if digest in seen:
                continue
            seen.add(digest)
            releases.append(release)
            if release.plugin is None:
                continue
            for member in release.plugin.members:
                if member.digest in seen:
                    continue
                expanded = await load(
                    member.digest, optional=member.digest in release.plugin.optional_digests
                )
                if expanded is None:
                    continue
                if expanded.coordinate != member:
                    raise MaterializationRejected(
                        f"plugin member drifted from its pinned coordinate: {member.component_id}"
                    )
                if expanded.plugin is not None:
                    raise MaterializationRejected("plugins cannot nest other plugins")
                seen.add(member.digest)
                releases.append(expanded)
        workspace_ids = {
            release.workspace.sandbox_snapshot_id for release in releases if release.workspace
        }
        snapshot_ids = {release.sandbox.snapshot_id for release in releases if release.sandbox}
        if not workspace_ids <= snapshot_ids:
            missing = sorted(workspace_ids - snapshot_ids)
            raise MaterializationRejected(
                f"workspace setup requires missing sandbox snapshots: {', '.join(missing)}"
            )
        server_names = [release.mcp.server_name for release in releases if release.mcp]
        if len(server_names) != len(set(server_names)):
            raise MaterializationRejected("MCP server names must be unique per materialization")
        if any(release.kind == ComponentKind.DIFF_CODEC for release in releases):
            raise MaterializationRejected(
                "diff codecs are selected by model qualification, not directly materialized"
            )
        return tuple(releases)
