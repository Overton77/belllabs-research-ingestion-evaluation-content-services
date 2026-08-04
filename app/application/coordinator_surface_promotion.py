from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256
from pathlib import Path

import yaml

from app.application.control_plane import ControlPlaneService
from app.domain.control_plane.canonical import sha256_digest
from app.domain.control_plane.contracts import (
    CatalogPayloadRef,
    Definition,
    ExactDefinitionRef,
    PromptDefinition,
    PromptVariable,
    PublishedDefinition,
    PublishRequest,
    SkillCompatibility,
    SkillDefinition,
    SkillFileManifestEntry,
    SourceProvenance,
)


class CoordinatorSurfacePromotionError(RuntimeError):
    pass


@dataclass(frozen=True)
class CoordinatorSurfacePromotionPlan:
    definitions: tuple[Definition, ...]
    expected_head_revisions: tuple[int, ...]
    reused: tuple[ExactDefinitionRef, ...]


def build_coordinator_surface(skill_root: Path) -> tuple[Definition, ...]:
    root = skill_root.resolve(strict=True)
    skill_path = root / "SKILL.md"
    manifest = _file_manifest(root)
    frontmatter = _frontmatter(skill_path)
    if frontmatter.get("name") != "belllabs-workflow-coordinator":
        raise CoordinatorSurfacePromotionError("coordinator skill name is not exact")
    description = frontmatter.get("description")
    if not isinstance(description, str) or not description.strip():
        raise CoordinatorSurfacePromotionError("coordinator skill description is missing")
    manifest_digest = sha256_digest(manifest)
    skill = SkillDefinition(
        logical_id="skill.belllabs-workflow-coordinator",
        title="BellLabs Workflow Coordinator",
        description=" ".join(description.split()),
        skill_name="belllabs-workflow-coordinator",
        frontmatter=frontmatter,
        body_summary=(
            "Search Workflow Types and governed capabilities progressively, quarantine "
            "external discoveries, and prepare exact admitted launches."
        ),
        bundle_ref=CatalogPayloadRef(
            uri=(
                "workspace://.agents/skills/belllabs-workflow-coordinator/"
                f"{manifest_digest.removeprefix('sha256:')}"
            ),
            digest=manifest_digest,
            media_type="application/vnd.belllabs.skill-manifest+json",
            size_bytes=sum(item.size_bytes for item in manifest),
        ),
        manifest_digest=manifest_digest,
        file_manifest=manifest,
        required_capabilities=frozenset(
            {
                "catalog.search",
                "workflow.prepare",
                "workflow.launch",
                "workflow.result.read",
            }
        ),
        compatibility=SkillCompatibility(
            runtimes=frozenset({"codex", "governed-agent-runtime"}),
            network_capabilities=frozenset({"network.coordinator-mcp"}),
            workspace_capabilities=frozenset({"workspace.skill.read"}),
        ),
        source_provenance=SourceProvenance(
            source="belllabs",
            locator="workspace:.agents/skills/belllabs-workflow-coordinator",
            upstream_identity="belllabs-workflow-coordinator",
            upstream_version=manifest_digest,
        ),
        review_status="approved",
    )
    prompt = PromptDefinition(
        logical_id="prompt.coordinator.propose-workflow",
        title="Propose governed workflow",
        description=(
            "Normalize an operator objective, then search existing Workflow Types and "
            "their exact contracts before proposing any new topology."
        ),
        format="markdown",
        template_engine="format",
        variables=(
            PromptVariable(
                name="objective",
                description="The operator's requested outcome.",
            ),
        ),
        body=(
            "Normalize this objective, then search Workflow Types first. Retrieve exact "
            "input, invariant, obligation, output, workspace, and linked-run contracts "
            "before selecting capabilities or drafting topology.\n\nObjective: {objective}"
        ),
        trust_class="reviewed",
    )
    return skill, prompt


def plan_coordinator_surface_promotion(
    definitions: tuple[Definition, ...],
    records: tuple[PublishedDefinition, ...],
) -> CoordinatorSurfacePromotionPlan:
    by_identity: dict[tuple[str, str], list[PublishedDefinition]] = {}
    for record in records:
        key = (record.ref.kind.value, record.ref.logical_id)
        by_identity.setdefault(key, []).append(record)

    publish: list[Definition] = []
    expected: list[int] = []
    reused: list[ExactDefinitionRef] = []
    for definition in definitions:
        key = (definition.kind.value, definition.logical_id)
        revisions = by_identity.get(key, [])
        head = max(revisions, key=lambda item: item.ref.revision) if revisions else None
        if (
            head is not None
            and head.retired_at is None
            and head.definition == definition
        ):
            reused.append(head.ref)
            continue
        publish.append(definition)
        expected.append(head.ref.revision if head is not None else 0)
    return CoordinatorSurfacePromotionPlan(
        definitions=tuple(publish),
        expected_head_revisions=tuple(expected),
        reused=tuple(reused),
    )


async def publish_coordinator_surface(
    *,
    service: ControlPlaneService,
    plan: CoordinatorSurfacePromotionPlan,
    actor_id: str,
    published_at: datetime,
) -> tuple[ExactDefinitionRef, ...]:
    published: list[ExactDefinitionRef] = []
    for definition, expected_revision in zip(
        plan.definitions,
        plan.expected_head_revisions,
        strict=True,
    ):
        record = await service.publish(
            PublishRequest(
                definition=definition,
                actor_id=actor_id,
                published_at=published_at,
                expected_head_revision=expected_revision,
            )
        )
        published.append(record.ref)
    return tuple(published)


def _file_manifest(root: Path) -> tuple[SkillFileManifestEntry, ...]:
    entries: list[SkillFileManifestEntry] = []
    for path in sorted(item for item in root.rglob("*") if item.is_file()):
        resolved = path.resolve(strict=True)
        if root not in resolved.parents:
            raise CoordinatorSurfacePromotionError("skill file escapes its bundle root")
        content = resolved.read_bytes()
        entries.append(
            SkillFileManifestEntry(
                path=resolved.relative_to(root).as_posix(),
                digest=f"sha256:{sha256(content).hexdigest()}",
                size_bytes=len(content),
            )
        )
    if not entries:
        raise CoordinatorSurfacePromotionError("coordinator skill bundle is empty")
    return tuple(entries)


def _frontmatter(path: Path) -> dict[str, object]:
    text = path.read_text(encoding="utf-8")
    if not text.startswith("---\n"):
        raise CoordinatorSurfacePromotionError("coordinator skill frontmatter is missing")
    _, raw, _body = text.split("---", maxsplit=2)
    parsed = yaml.safe_load(raw)
    if not isinstance(parsed, dict):
        raise CoordinatorSurfacePromotionError("coordinator skill frontmatter is invalid")
    return {str(key): value for key, value in parsed.items()}
