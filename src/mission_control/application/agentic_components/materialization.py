from __future__ import annotations

import json
from hashlib import sha256

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
from mission_control.domain.authoring.canonical import stable_json_dump


class MaterializationRejected(ValueError):
    pass


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
