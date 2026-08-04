from __future__ import annotations

import asyncio
import os
import subprocess
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from app.application.artifact_promotion import ArtifactPayloadAddress
from app.application.orchestration import (
    ORCHESTRATION_AUTHORITY_REF,
    orchestration_lifecycle_actor,
)
from app.application.run_control import ACTION_PERMISSIONS
from app.application.web_research_coordinator_live import (
    EXTERNAL_MCP_DISCOVERY_QUERY,
    EXTERNAL_SKILL_DISCOVERY_QUERY,
    SEARCH_PLAN,
    ReadOnlyAdmissionPreview,
    VerifiedS3ScreenshotStore,
    _live_settings,
    _profile_derived_selection,
    _retrieve_exact_capabilities,
)
from app.application.web_research_semantic_binding import (
    REQUIRED_SELECTED_IDENTITIES,
)
from app.domain.control_plane.canonical import sha256_digest
from app.domain.control_plane.contracts import (
    AgentProfileDefinition,
    AuthorityCeiling,
    DefinitionKind,
    ExactDefinitionRef,
    ModelPolicy,
    PublishedDefinition,
)
from app.domain.coordinator.contracts import AuthorizationState
from app.domain.run_control.contracts import ActorContext
from scripts.run_web_research_coordinator_live import parse_args

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "run_web_research_coordinator_live.py"


def test_viome_gate_defaults_to_two_independent_browser_pages(tmp_path: Path) -> None:
    args = parse_args(["--artifact-dir", str(tmp_path)])

    assert args.browser_verification_limit == 2


def _ref(kind: DefinitionKind, logical_id: str) -> ExactDefinitionRef:
    return ExactDefinitionRef(
        kind=kind,
        logical_id=logical_id,
        revision=3,
        digest=sha256_digest({"kind": kind.value, "logical_id": logical_id}),
    )


@dataclass(frozen=True)
class Hit:
    kind: DefinitionKind
    exact_ref: ExactDefinitionRef | None
    candidate_id: str | None = None
    authorization_state: AuthorizationState = AuthorizationState.SELECTABLE


class Search:
    def __init__(
        self,
        current: dict[tuple[DefinitionKind, str], PublishedDefinition],
    ) -> None:
        self.requests = []
        self.current = current

    async def search(self, request):
        self.requests.append(request)
        if request.kinds == frozenset({DefinitionKind.WORKFLOW_TYPE}):
            current = self.current[
                (
                    DefinitionKind.WORKFLOW_TYPE,
                    "web-research-browser-verification",
                )
            ].ref
            return SimpleNamespace(
                hits=(
                    Hit(
                        kind=DefinitionKind.WORKFLOW_TYPE,
                        exact_ref=current.model_copy(
                            update={
                                "revision": current.revision - 1,
                                "digest": sha256_digest("older-workflow-revision"),
                            }
                        ),
                    ),
                    Hit(
                        kind=DefinitionKind.WORKFLOW_TYPE,
                        exact_ref=current,
                    ),
                )
            )
        exact = tuple(
            Hit(kind=kind, exact_ref=record.ref)
            for (kind, _logical_id), record in self.current.items()
            if kind in request.kinds
        )
        candidate = Hit(
            kind=next(iter(request.kinds)),
            exact_ref=None,
            candidate_id="candidate:sha256:" + "a" * 64,
            authorization_state=AuthorizationState.CANDIDATE_ONLY,
        )
        return SimpleNamespace(hits=(*exact, candidate))


def _selection_catalog(
    *,
    replacement_skill: str | None = None,
) -> dict[tuple[DefinitionKind, str], PublishedDefinition]:
    current: dict[tuple[DefinitionKind, str], PublishedDefinition] = {}

    def add(
        kind: DefinitionKind,
        logical_id: str,
        definition: object,
    ) -> ExactDefinitionRef:
        ref = ExactDefinitionRef(
            kind=kind,
            logical_id=logical_id,
            revision=3,
            digest=sha256_digest(definition),
        )
        current[(kind, logical_id)] = PublishedDefinition.model_construct(
            ref=ref,
            definition=definition,
            published_at=datetime.now(UTC),
            retired_at=None,
        )
        return ref

    add(
        DefinitionKind.WORKFLOW_TYPE,
        "web-research-browser-verification",
        {"fixture": "workflow"},
    )
    server_refs = frozenset(
        add(kind, logical_id, {"fixture": logical_id})
        for kind, logical_id in REQUIRED_SELECTED_IDENTITIES
        if kind == DefinitionKind.MCP_SERVER
    )
    tool_refs = frozenset(
        add(kind, logical_id, {"fixture": logical_id})
        for kind, logical_id in REQUIRED_SELECTED_IDENTITIES
        if kind == DefinitionKind.MCP_TOOL
    )
    skill_ids = [
        logical_id
        for kind, logical_id in REQUIRED_SELECTED_IDENTITIES
        if kind == DefinitionKind.SKILL
    ]
    if replacement_skill is not None:
        skill_ids[skill_ids.index("skill.agent-browser")] = replacement_skill
    skill_refs = frozenset(
        add(DefinitionKind.SKILL, logical_id, {"fixture": logical_id})
        for logical_id in skill_ids
    )
    profile = AgentProfileDefinition(
        logical_id="agent-profile.web-research-browser-verification",
        title="Profile-derived research fixture",
        description="A profile whose immutable refs drive exact selection.",
        skill_refs=skill_refs,
        mcp_server_refs=server_refs,
        tool_refs=tool_refs,
        model_policy=ModelPolicy(provider="openai", model="test-model"),
        maximum_capability_request=AuthorityCeiling(),
    )
    add(DefinitionKind.AGENT_PROFILE, profile.logical_id, profile)
    return current


def test_help_is_side_effect_free() -> None:
    completed = subprocess.run(
        [sys.executable, str(SCRIPT), "--help"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0
    assert "--goal" in completed.stdout
    assert "--artifact-dir" in completed.stdout
    assert "--skip-external-discovery" in completed.stdout
    assert "PostgreSQL admission" in completed.stdout


def test_internal_search_plan_is_workflow_first_and_has_no_provider_name_hints() -> None:
    assert SEARCH_PLAN[0][1] == frozenset({DefinitionKind.WORKFLOW_TYPE})
    assert [kinds for _query, kinds in SEARCH_PLAN[1:]] == [
        frozenset({DefinitionKind.MCP_SERVER}),
        frozenset({DefinitionKind.MCP_TOOL}),
        frozenset({DefinitionKind.SKILL}),
        frozenset({DefinitionKind.AGENT_PROFILE}),
    ]
    queries = " ".join(query for query, _kinds in SEARCH_PLAN).casefold()
    assert "firecrawl" not in queries
    assert "tavily" not in queries
    assert "vercel" not in queries


def test_web_live_lifecycle_actor_is_separate_and_least_privilege() -> None:
    coordinator = ActorContext(
        actor_id="coordinator-live",
        authority_refs=frozenset({"authority:coordinator-live"}),
        permissions=frozenset({"workflow_run.admit"}),
    )
    lifecycle = orchestration_lifecycle_actor()

    assert lifecycle.actor_id == ORCHESTRATION_AUTHORITY_REF
    assert lifecycle.authority_refs == frozenset({ORCHESTRATION_AUTHORITY_REF})
    assert lifecycle.permissions == frozenset(ACTION_PERMISSIONS.values())
    assert "workflow_run.admit" not in lifecycle.permissions
    assert ORCHESTRATION_AUTHORITY_REF not in coordinator.authority_refs


def test_live_settings_pin_workspace_npx_and_bundled_node(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PATH", r"C:\Windows\System32")

    settings = _live_settings()
    node = Path(settings.web_research_agent_browser_node or "").resolve()
    npx = Path(settings.npx_skills_executable).resolve()

    assert node == Path(sys.base_prefix).resolve().parent / "node" / "bin" / "node.exe"
    assert npx == ROOT.parent / ".tools" / "node_modules" / ".bin" / "npx.CMD"
    assert (settings.npx_skills_package_version) == "1.5.20"
    assert settings.external_discovery_request_timeout_seconds == 30.0
    assert settings.external_discovery_command_timeout_seconds == 60.0
    assert settings.external_discovery_max_retries == 4
    assert settings.web_research_browser_command_timeout_seconds == 60.0
    assert settings.web_research_browser_timeout_seconds == 240.0
    assert Path(os.environ["PATH"].split(os.pathsep)[0]) == node.parent


def test_external_discovery_queries_are_provider_name_free_and_intent_derived() -> None:
    queries = (
        EXTERNAL_MCP_DISCOVERY_QUERY,
        EXTERNAL_SKILL_DISCOVERY_QUERY,
    )
    normalized = " ".join(queries).casefold()

    assert "search" in EXTERNAL_MCP_DISCOVERY_QUERY.casefold()
    assert "research" in EXTERNAL_SKILL_DISCOVERY_QUERY.casefold()
    assert "browser" in EXTERNAL_SKILL_DISCOVERY_QUERY.casefold()
    assert all(
        provider not in normalized
        for provider in ("firecrawl", "tavily", "vercel", "skills.sh")
    )


@pytest.mark.asyncio
async def test_internal_search_selects_only_exact_evidence_and_quarantines_candidates() -> None:
    current = _selection_catalog()
    search = Search(current)

    selection = await _retrieve_exact_capabilities(
        search,  # type: ignore[arg-type]
        tenant_scope="tenant-live",
        current=current,
    )

    assert selection.workflow_hit.exact_ref is not None
    assert (
        selection.workflow_hit.exact_ref.logical_id
        == "web-research-browser-verification"
    )
    assert selection.workflow_hit.exact_ref.revision == 3
    assert {
        (hit.exact_ref.kind, hit.exact_ref.logical_id)
        for hit in selection.selected_hits
        if hit.exact_ref is not None
    } == REQUIRED_SELECTED_IDENTITIES
    assert all(hit.candidate_id is None for hit in selection.selected_hits)
    assert len(selection.requests) == len(SEARCH_PLAN)
    assert all(
        request.workflow_type_ref == selection.workflow_hit.exact_ref
        for request in selection.requests[1:]
    )


def test_agent_profile_refs_drive_selection_and_missing_retrieval_fails_closed() -> None:
    replacement = "skill.browser-replacement"
    current = _selection_catalog(replacement_skill=replacement)
    profile_record = current[
        (
            DefinitionKind.AGENT_PROFILE,
            "agent-profile.web-research-browser-verification",
        )
    ]
    profile_hit = Hit(
        kind=DefinitionKind.AGENT_PROFILE,
        exact_ref=profile_record.ref,
    )
    hits = tuple(
        Hit(kind=kind, exact_ref=record.ref)
        for (kind, _logical_id), record in current.items()
        if kind != DefinitionKind.WORKFLOW_TYPE
    )

    selected = _profile_derived_selection(
        profile_hit,  # type: ignore[arg-type]
        hits,  # type: ignore[arg-type]
        current=current,
    )

    assert replacement in {
        hit.exact_ref.logical_id for hit in selected if hit.exact_ref is not None
    }
    assert "skill.agent-browser" not in {
        hit.exact_ref.logical_id for hit in selected if hit.exact_ref is not None
    }
    missing_replacement = tuple(
        hit
        for hit in hits
        if hit.exact_ref is None or hit.exact_ref.logical_id != replacement
    )
    with pytest.raises(RuntimeError, match="derived from the selected Agent Profile"):
        _profile_derived_selection(
            profile_hit,  # type: ignore[arg-type]
            missing_replacement,  # type: ignore[arg-type]
            current=current,
        )


@pytest.mark.asyncio
async def test_read_only_preview_runs_f1_then_scenario_policy_without_mutation() -> None:
    calls: list[str] = []

    class Verifier:
        async def verify(self, request):
            calls.append("verify")
            return SimpleNamespace(workflow_type_ref=request.workflow_type_ref)

    class Policies:
        async def validate(self, request, configuration):
            calls.append("policy")
            assert configuration.workflow_type_ref == request.workflow_type_ref

    request = SimpleNamespace(
        workflow_type_ref=_ref(
            DefinitionKind.WORKFLOW_TYPE,
            "web-research-browser-verification",
        )
    )
    preview = ReadOnlyAdmissionPreview(
        Verifier(),  # type: ignore[arg-type]
        Policies(),  # type: ignore[arg-type]
    )

    decision = await preview.preview(request)

    assert decision.accepted
    assert calls == ["verify", "policy"]


class _FakeS3Payloads:
    def __init__(self) -> None:
        self.address: ArtifactPayloadAddress | None = None
        self.content = b""

    async def stage(
        self,
        *,
        artifact_id: str,
        content: bytes,
        content_digest: str,
        media_type: str,
    ) -> ArtifactPayloadAddress:
        assert artifact_id.startswith("browser-screenshot:")
        assert media_type == "image/png"
        self.content = content
        self.address = ArtifactPayloadAddress(
            object_ref="s3://private-test/web-research/screenshots/proof.png",
            content_digest=content_digest,
            size_bytes=len(content),
        )
        return self.address

    async def retrieve(self, address: ArtifactPayloadAddress) -> bytes:
        assert address == self.address
        return self.content


@pytest.mark.asyncio
async def test_screenshot_store_requires_s3_and_verifies_retrieval(
    tmp_path: Path,
) -> None:
    payloads = _FakeS3Payloads()
    store = VerifiedS3ScreenshotStore(  # type: ignore[arg-type]
        payloads,
        mirror_root=tmp_path,
    )

    ref = await store.store(
        request_scope="global",
        run_id="run-1",
        idempotency_key="run-1:browser",
        source_url="https://example.com",
        content=b"\x89PNG\r\n\x1a\nacceptance-proof",
        media_type="image/png",
    )

    assert ref.startswith("s3://")
    assert store.refs[ref]["retrieval_verified"] is True
    assert await asyncio.to_thread(
        Path(str(store.refs[ref]["local_qa_mirror"])).read_bytes
    ) == (
        b"\x89PNG\r\n\x1a\nacceptance-proof"
    )
