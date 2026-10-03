from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from mission_control.adapters.storage.control_plane_payloads import InMemoryPayloadStore
from mission_control.application.authoring.control_plane_repository import (
    InMemoryDefinitionRepository,
)
from mission_control.application.authoring.service import ControlPlaneService
from mission_control.application.coordinator.coordinator_surface_promotion import (
    build_coordinator_surface,
    plan_coordinator_surface_promotion,
    publish_coordinator_surface,
)
from mission_control.domain.authoring.contracts import (
    DefinitionKind,
    PromptDefinition,
    SkillDefinition,
)
from mission_control.domain.authoring.extensions import ExtensionRegistry

PROJECT_ROOT = Path(__file__).resolve().parents[3]
SKILL_ROOT = PROJECT_ROOT / ".agents" / "skills" / "mission-control-coordinator"


@pytest.mark.asyncio
async def test_coordinator_surface_is_content_addressed_and_resumable() -> None:
    definitions = build_coordinator_surface(SKILL_ROOT)
    skill = next(item for item in definitions if isinstance(item, SkillDefinition))
    prompt = next(item for item in definitions if isinstance(item, PromptDefinition))

    assert skill.logical_id == "skill.mission-control-coordinator"
    assert skill.manifest_digest == skill.bundle_ref.digest
    assert {item.path for item in skill.file_manifest} >= {
        "SKILL.md",
        "references/coordinator-protocol.md",
        "scripts/validate_launch_proposal.py",
    }
    assert prompt.logical_id == "prompt.coordinator.propose-workflow"
    assert prompt.trust_class == "reviewed"
    assert "Workflow Types first" in prompt.body

    repository = InMemoryDefinitionRepository()
    service = ControlPlaneService(
        repository,
        ExtensionRegistry(),
        InMemoryPayloadStore(),
    )
    plan = plan_coordinator_surface_promotion(definitions, ())
    published = await publish_coordinator_surface(
        service=service,
        plan=plan,
        actor_id="test-publisher",
        published_at=datetime(2026, 7, 26, 15, 0, tzinfo=UTC),
    )
    assert {ref.kind for ref in published} == {
        DefinitionKind.SKILL,
        DefinitionKind.PROMPT,
    }

    records = tuple([await repository.get(ref) for ref in published])
    resumed = plan_coordinator_surface_promotion(definitions, records)
    assert resumed.definitions == ()
    assert resumed.reused == published
