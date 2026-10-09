"""MP-13: transcript `full=true` reads bodies through the artifact grant; refresh re-projects."""

from __future__ import annotations

from datetime import UTC, datetime
from hashlib import sha256
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from mission_control.adapters.storage.artifact_payloads import InMemoryArtifactPayloadStore
from mission_control.application.artifacts.artifact_promotion import artifact_durable_reference
from mission_control.application.frames.artifact_bodies import (
    ArtifactBodyDenied,
    ArtifactBodyMissing,
    GrantedArtifactBodyReader,
    parse_artifact_ref,
)
from mission_control.application.frames.search import TranscriptSearchService
from mission_control.application.frames.transcript import (
    MissionEventRecord,
    TranscriptDenied,
    TranscriptService,
)
from mission_control.domain.execution.contracts import (
    ArtifactMetadataRevision,
    ArtifactPromotionState,
    WorkspaceOwner,
    WorkspaceOwnerKind,
)
from mission_control.domain.policies.contracts import ActorContext
from mission_control.interfaces.http.mission_control import MissionPrincipal, get_mission_principal
from mission_control.interfaces.http.transcript import ARTIFACT_BODY_READERS
from tests.fixtures.provider_frames import INSTALLATION, OTHER_SCOPE, SCOPE, TENANT
from tests.fixtures.transcripts import (
    RUN_KEY,
    SECRET_VALUE,
    InMemoryMissionEvents,
    StaticFrames,
    transcript_events,
    transcript_frames,
)
from tests.unit.frames.test_run_list_and_search import MemoryDocuments
from tests.unit.frames.test_transcript_interfaces import http_app

READ = frozenset({"workflow_run.read"})
FULL = READ | {"workflow.result.read"}
BODY = f"full report body with {SECRET_VALUE} inside".encode()
ARTIFACT_ID = "artifact-report-1"
DURABLE = artifact_durable_reference(SCOPE, RUN_KEY, ARTIFACT_ID)


def revision(**overrides: Any) -> ArtifactMetadataRevision:
    digest = "sha256:" + sha256(BODY).hexdigest()
    values: dict[str, Any] = {
        "promotion_id": "promotion-1",
        "artifact_id": ARTIFACT_ID,
        "intent_key": "intent-1",
        "promotion_identity": "sha256:" + "a" * 64,
        "revision": 3,
        "state": ArtifactPromotionState.ADMITTED,
        "request_scope": SCOPE,
        "run_id": RUN_KEY,
        "semantic_attempt_key": "attempt-1",
        "producer_binding_id": "binding-1",
        "namespace_id": "ns-1",
        "workspace_id": "ws-1",
        "output_slot": "report",
        "logical_path": "report.md",
        "owner": WorkspaceOwner(kind=WorkspaceOwnerKind.RUN, owner_id=RUN_KEY),
        "candidate_id": "candidate-1",
        "content_digest": digest,
        "media_type": "text/markdown",
        "size_bytes": len(BODY),
        "permission_ref": "permission-1",
        "permission_outcome": "allowed",
        "output_contract_ref": "contract-1",
        "object_ref": "memory://artifacts/" + digest.removeprefix("sha256:"),
        "recorded_at": datetime(2026, 10, 8, tzinfo=UTC),
    }
    values.update(overrides)
    return ArtifactMetadataRevision.model_validate(values)


class Metadata:
    def __init__(self, item: ArtifactMetadataRevision | None) -> None:
        self.item = item

    async def get_by_artifact(self, artifact_id: str) -> ArtifactMetadataRevision | None:
        return self.item if self.item is not None and self.item.artifact_id == artifact_id else None


async def reader(item: ArtifactMetadataRevision | None = None) -> GrantedArtifactBodyReader:
    payloads = InMemoryArtifactPayloadStore()
    await payloads.stage(
        artifact_id=ARTIFACT_ID,
        content=BODY,
        content_digest="sha256:" + sha256(BODY).hexdigest(),
        media_type="text/markdown",
    )
    return GrantedArtifactBodyReader(Metadata(item or revision()), payloads, request_scope=SCOPE)


def actor(permissions: frozenset[str]) -> ActorContext:
    return ActorContext(actor_id="reader", permissions=permissions)


def test_artifact_refs_parse_durable_and_short_forms():
    assert parse_artifact_ref(DURABLE) == (SCOPE, RUN_KEY, ARTIFACT_ID)
    assert parse_artifact_ref("artifact:abc") == (None, None, "abc")
    with pytest.raises(ArtifactBodyMissing):
        parse_artifact_ref("mc://artifacts/biotech/run/x.json")


async def test_reader_enforces_grant_scope_run_state_and_permission_outcome():
    granted = await reader()
    assert await granted.read(SCOPE, actor(FULL), DURABLE) == BODY.decode()
    with pytest.raises(ArtifactBodyDenied):
        await granted.read(SCOPE, actor(READ), DURABLE)
    with pytest.raises(ArtifactBodyDenied):
        await granted.read(OTHER_SCOPE, actor(FULL), DURABLE)
    with pytest.raises(ArtifactBodyDenied):
        await granted.read(
            SCOPE, actor(FULL), artifact_durable_reference(OTHER_SCOPE, RUN_KEY, ARTIFACT_ID)
        )
    with pytest.raises(ArtifactBodyMissing):
        await granted.read(
            SCOPE, actor(FULL), artifact_durable_reference(SCOPE, "other-run", ARTIFACT_ID)
        )
    with pytest.raises(ArtifactBodyDenied):
        await (await reader(revision(permission_outcome="prohibited"))).read(
            SCOPE, actor(FULL), DURABLE
        )
    with pytest.raises(ArtifactBodyMissing):
        await (await reader(revision(state=ArtifactPromotionState.PAYLOAD_STAGED))).read(
            SCOPE, actor(FULL), DURABLE
        )
    with pytest.raises(ArtifactBodyMissing):
        await (await reader(revision(request_scope=OTHER_SCOPE))).read(SCOPE, actor(FULL), DURABLE)


def events_with_durable_artifact() -> list[MissionEventRecord]:
    frames = transcript_frames()
    events = list(transcript_events(frames))
    last = events[-1]
    events.append(
        MissionEventRecord(
            seq=last.seq + 1,
            event_id=f"00000000-0000-7000-8000-{last.seq + 1:012d}",
            event_type="artifact.registered",
            recorded_at=last.recorded_at,
            actor_ref="mission-control",
            payload={
                "event_type": "artifact.registered",
                "payload": {"artifact_ref": DURABLE, "media_type": "text/markdown"},
            },
        )
    )
    return events


async def service_with(artifacts: GrantedArtifactBodyReader | None) -> TranscriptService:
    service = TranscriptService(
        InMemoryMissionEvents(events_with_durable_artifact()),
        StaticFrames(transcript_frames()),  # type: ignore[arg-type]
        request_scope=SCOPE,
        secret_values=(SECRET_VALUE,),
    )
    return service.with_artifacts(artifacts) if artifacts is not None else service


async def test_full_transcript_reads_admitted_bodies_redacted_and_keeps_other_excerpts():
    service = await service_with(await reader())
    page = await service.materialize(RUN_KEY, actor=actor(FULL), full=True)
    durable = [entry for entry in page.entries if entry.refs.artifact_ref == DURABLE]
    assert len(durable) == 1
    assert durable[0].body_excerpt is not None
    assert durable[0].body_excerpt.startswith("full report body with ")
    assert SECRET_VALUE not in durable[0].body_excerpt
    legacy = [
        entry
        for entry in page.entries
        if entry.refs.artifact_ref and entry.refs.artifact_ref.startswith("mc://")
    ]
    plain = await (await service_with(None)).materialize(RUN_KEY, actor=actor(FULL))
    by_cursor = {entry.cursor: entry for entry in plain.entries}
    assert legacy and all(by_cursor[e.cursor].body_excerpt == e.body_excerpt for e in legacy)
    with pytest.raises(TranscriptDenied):
        await service.materialize(RUN_KEY, actor=actor(READ), full=True)


def app_with_reader(permissions: frozenset[str], artifacts: Any) -> FastAPI:
    app = http_app(permissions)
    key = (INSTALLATION, "biotech", TENANT)
    app.state.mission_control_transcript_services = {key: _sync(service_with(None))}
    setattr(app.state, ARTIFACT_BODY_READERS, {key: artifacts})
    app.state.mission_control_transcript_search_services = {
        key: TranscriptSearchService(_sync(service_with(None)), MemoryDocuments())
    }
    app.dependency_overrides[get_mission_principal] = lambda: MissionPrincipal(
        installation_id=INSTALLATION,
        application_id="biotech",
        tenant_id=TENANT,
        issuer="https://issuer.invalid",
        audiences=frozenset({"authenticated"}),
        actor=actor(permissions),
    )
    return app


def _sync(awaitable: Any) -> Any:
    import asyncio

    return asyncio.run(awaitable)


PATH = f"/v1/applications/biotech/runs/{RUN_KEY}/transcript"


def test_http_composes_the_registered_reader_for_full_bodies_and_refresh():
    granted = _sync(reader())
    client = TestClient(app_with_reader(FULL, granted))
    full = client.get(PATH, params={"full": "true", "format": "json"})
    assert full.status_code == 200
    bodies = [
        entry["body_excerpt"]
        for entry in full.json()["entries"]
        if entry.get("refs", {}).get("artifact_ref") == DURABLE
    ]
    assert len(bodies) == 1 and bodies[0].startswith("full report body")
    denied = TestClient(app_with_reader(READ, granted)).get(PATH, params={"full": "true"})
    assert denied.status_code == 403
    foreign = GrantedArtifactBodyReader(
        Metadata(None), InMemoryArtifactPayloadStore(), request_scope=OTHER_SCOPE
    )
    mismatch = TestClient(app_with_reader(FULL, foreign)).get(PATH, params={"full": "true"})
    assert mismatch.status_code == 503
    refresh = client.post(f"/v1/applications/biotech/runs/{RUN_KEY}/transcript/refresh")
    assert refresh.status_code == 200
    receipt = refresh.json()
    assert receipt["run_id"] == RUN_KEY and receipt["documents"] > 0
    again = client.post(f"/v1/applications/biotech/runs/{RUN_KEY}/transcript/refresh").json()
    assert again["upserted"] == 0 and again["documents"] == receipt["documents"]
    missing = client.post("/v1/applications/biotech/runs/missing/transcript/refresh")
    assert missing.status_code == 404
    forbidden = TestClient(app_with_reader(frozenset(), granted)).post(
        f"/v1/applications/biotech/runs/{RUN_KEY}/transcript/refresh"
    )
    assert forbidden.status_code == 403
