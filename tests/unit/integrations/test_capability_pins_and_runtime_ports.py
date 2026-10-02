"""RRM-009: digest pins, the pinned asset verifier and the deployment runtime ports."""

from __future__ import annotations

import json
from hashlib import sha256
from pathlib import Path

import pytest

from app.application.workspaces.artifact_promotion import ArtifactPayloadAddress
from app.config import PROJECT_ROOT
from app.domain.control_plane.contracts import SecretRef
from app.domain.operation_execution.errors import WorkspaceDigestMismatch
from app.integrations.capability_pins import (
    CapabilityPinError,
    CapabilityPins,
    read_skill_bundle,
    workspace_path,
)
from app.integrations.operation_runtime_ports import (
    EnvironmentSecretResolver,
    FilesystemArtifactPayloadStore,
    PinnedCapabilityAssetVerifier,
    RecordedOperationEventSink,
    SecretReferenceUnavailable,
)
from tests.unit.operations.test_operation_execution import (
    MCP_DIGEST,
    SKILL_DIGEST,
    operation_request,
    service_fixture,
)

PINS = PROJECT_ROOT / "infra" / "capability-pins" / "research-capabilities.json"


def test_pin_file_is_exact_and_discloses_no_secret() -> None:
    pins = CapabilityPins.load(PINS)
    assert {server.server_id for server in pins.mcp_servers} == {"mcp.firecrawl", "mcp.tavily"}
    firecrawl = pins.mcp_server("mcp.firecrawl")
    assert {tool.tool_name for tool in firecrawl.tools} >= {"firecrawl_search", "firecrawl_scrape"}
    assert {tool.tool_name for tool in pins.mcp_server("mcp.tavily").tools} >= {"tavily_search"}
    assert all(
        tool.schema_digest.startswith("sha256:")
        for server in pins.mcp_servers
        for tool in server.tools
    )
    assert pins.skill("agent-browser").mount_root == "/skills/agent-browser"
    assert pins.tool("agent_browser_page").kind == "agent_browser_page"
    assert pins.checkpointers and pins.stores and pins.models and pins.sandboxes
    disclosure = json.dumps(pins.disclosure())
    assert "credential_ref" in disclosure and "environment:FIRECRAWL_API_KEY" in disclosure
    for forbidden in ("fc-", "tvly-", "sk-", "password"):
        assert forbidden not in disclosure.lower().replace("environment:", "")
    assert pins.mcp_schema_digests()["mcp.firecrawl"] == firecrawl.schema_digest
    assert pins.asset_manifest_digests() == {
        "skill:skill.agent-browser:1": pins.skill("agent-browser").bundle_digest
    }
    with pytest.raises(CapabilityPinError, match="not pinned"):
        pins.mcp_server("mcp.other")


def test_workspace_artifacts_verify_against_their_pins_when_present() -> None:
    pins = CapabilityPins.load(PINS)
    skill = pins.skill("agent-browser")
    directory = workspace_path(skill.source_locator)
    if not directory.is_dir():
        pytest.skip("workspace Skill directory is not laid out beside this checkout")
    bundle = read_skill_bundle(directory)
    assert bundle.bundle_digest == skill.bundle_digest
    assert skill.bundle().files == bundle.files
    tool = pins.tool("agent_browser_page")
    entrypoint = workspace_path(tool.entrypoint_locator)
    if not entrypoint.exists():
        pytest.skip("workspace agent-browser entrypoint is not laid out beside this checkout")
    assert tool.verify_entrypoint().read_bytes() == entrypoint.read_bytes()
    tampered = tool.model_copy(update={"entrypoint_digest": "sha256:" + "0" * 64})
    with pytest.raises(CapabilityPinError, match="reviewed digest"):
        tampered.verify_entrypoint()


def test_workspace_locators_cannot_escape_the_workspace() -> None:
    for locator in ("workspace://../secrets", "workspace:///etc/passwd", "file://x"):
        with pytest.raises(CapabilityPinError):
            workspace_path(locator)
    assert workspace_path("workspace://.tools/x.js") == PROJECT_ROOT.parent / ".tools" / "x.js"


@pytest.mark.asyncio
async def test_pinned_verifier_admits_exact_bindings_and_refuses_drift() -> None:
    pins = CapabilityPins.model_validate(
        {
            "mcp_servers": [
                {
                    "server_id": "fixture-mcp",
                    "server_name": "fixture",
                    "ref": {
                        "kind": "mcp_server",
                        "logical_id": "mcp.fixture",
                        "revision": 1,
                        "digest": MCP_DIGEST,
                    },
                    "package_name": "fixture",
                    "package_version": "1.0.0",
                    "module_locator": "workspace://.tools/fixture.js",
                    "module_digest": "sha256:" + "1" * 64,
                    "schema_digest": MCP_DIGEST,
                    "credential_env": None,
                    "tools": [{"tool_name": "lookup_fixture", "schema_digest": MCP_DIGEST}],
                }
            ],
            "skills": [
                {
                    "ref": {
                        "kind": "skill",
                        "logical_id": "fixture.skill",
                        "revision": 1,
                        "digest": SKILL_DIGEST,
                    },
                    "skill_name": "fixture",
                    "source_locator": "workspace://.agents/skills/fixture",
                    "bundle_digest": SKILL_DIGEST,
                    "skill_md_digest": "sha256:" + "2" * 64,
                    "mount_root": "/skills/fixture",
                }
            ],
        }
    )
    verifier = PinnedCapabilityAssetVerifier(pins)
    service, bindings, _runtime, _events, _budget = service_fixture()
    del service
    request = operation_request()
    from app.application.operations.operation_execution import bind_operation_execution_request

    binding = bind_operation_execution_request(request)
    await verifier.verify(binding)
    await verifier.verify_servers(binding)
    drifted = request.model_copy(
        update={
            "mcp_servers": (
                request.mcp_servers[0].model_copy(update={"schema_digest": "sha256:" + "9" * 64}),
            )
        }
    )
    with pytest.raises(ValueError, match="not pinned"):
        await verifier.verify(bind_operation_execution_request(drifted))
    with pytest.raises(ValueError, match="not pinned"):
        await PinnedCapabilityAssetVerifier(CapabilityPins.empty()).verify(binding)
    del bindings


@pytest.mark.asyncio
async def test_environment_secrets_resolve_by_reference_only() -> None:
    resolver = EnvironmentSecretResolver({"OPENAI_API_KEY": "value-1", "EMPTY": ""})
    resolved = await resolver.resolve((SecretRef(provider="environment", key="OPENAI_API_KEY"),))
    assert resolved == {"environment:OPENAI_API_KEY": "value-1"}
    with pytest.raises(SecretReferenceUnavailable, match="no value"):
        await resolver.resolve((SecretRef(provider="environment", key="EMPTY"),))
    with pytest.raises(SecretReferenceUnavailable, match="not composed"):
        await resolver.resolve((SecretRef(provider="vault", key="OPENAI_API_KEY"),))


@pytest.mark.asyncio
async def test_filesystem_payload_store_is_content_addressed_and_verified(tmp_path: Path) -> None:
    store = FilesystemArtifactPayloadStore(tmp_path / "payloads")
    content = b"# report\n"
    digest = "sha256:" + sha256(content).hexdigest()
    address = await store.stage(
        artifact_id="a", content=content, content_digest=digest, media_type="text/markdown"
    )
    assert address.object_ref == f"file-artifacts://{digest.removeprefix('sha256:')}"
    assert await store.retrieve(address) == content
    again = await store.stage(
        artifact_id="b", content=content, content_digest=digest, media_type="text/markdown"
    )
    assert again == address
    with pytest.raises(WorkspaceDigestMismatch):
        await store.stage(
            artifact_id="c", content=b"other", content_digest=digest, media_type="text/plain"
        )
    with pytest.raises(WorkspaceDigestMismatch):
        await store.retrieve(
            ArtifactPayloadAddress(
                object_ref="s3://elsewhere/x", content_digest=digest, size_bytes=9
            )
        )
    with pytest.raises(WorkspaceDigestMismatch):
        await store.retrieve(
            ArtifactPayloadAddress(
                object_ref=f"file-artifacts://{'0' * 64}", content_digest=digest, size_bytes=9
            )
        )


@pytest.mark.asyncio
async def test_recorded_events_are_idempotent_by_key() -> None:
    sink = RecordedOperationEventSink()
    await sink.publish(event_key="k", binding_id="b", payload={"kind": "x"})
    await sink.publish(event_key="k", binding_id="b", payload={"kind": "x"})
    with pytest.raises(ValueError, match="conflicting"):
        await sink.publish(event_key="k", binding_id="b", payload={"kind": "y"})
