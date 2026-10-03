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


def test_browser_tool_reaches_public_hosts_by_name_only() -> None:
    from app.integrations.agents.deep_agents.browser_tool import _public_host
    from app.integrations.web_research_runtime import WebResearchRuntimeDependencyError

    assert _public_host("https://Example.COM/path?q=1") == "example.com"
    for url in (
        "http://localhost/",
        "http://127.0.0.1:8080/",
        "http://[::1]/",
        "http://[fe80::1]/admin",
        "http://printer.local/",
        "file:///etc/hosts",
        "ftp://example.com/",
        # RRM-009 review: non-canonical IPv4 forms are IP literals too.
        "http://2130706433/",
        "http://0x7f000001/",
        "http://0177.0.0.1/",
        "http://127.1/",
        "http://10.1/",
        "http://169.254.169.254/",
        "http://example.123/",
        "http://localhost./",
        "http://api.localhost/",
    ):
        with pytest.raises(WebResearchRuntimeDependencyError):
            _public_host(url)


@pytest.mark.asyncio
async def test_browser_tool_opens_only_hosts_the_operation_was_granted(tmp_path: Path) -> None:
    import sys

    from app.integrations.agents.deep_agents.browser_tool import (
        AgentBrowserPageTool,
        granted_network_hosts,
    )
    from app.integrations.web_research_runtime import WebResearchRuntimeDependencyError

    class NoSubprocess:
        calls = 0

        async def run(self, request: object) -> object:
            del request
            NoSubprocess.calls += 1
            raise AssertionError("no subprocess outside the grant")

    entrypoint = tmp_path / "agent-browser.js"
    entrypoint.write_text("// pinned entrypoint stand-in", encoding="utf-8")
    tool = AgentBrowserPageTool(
        node_executable=Path(sys.executable), entrypoint=entrypoint, runner=NoSubprocess()
    )
    with granted_network_hosts(frozenset({"example.com"})):
        with pytest.raises(WebResearchRuntimeDependencyError, match="granted network hosts"):
            await tool.ainvoke({"url": "https://other.example.org/"})
    with granted_network_hosts(frozenset()):
        with pytest.raises(WebResearchRuntimeDependencyError, match="granted network hosts"):
            await tool.ainvoke({"url": "https://example.com/"})
    # RRM-009 review: egress is deny-by-default; with no grant bound nothing is reachable.
    with pytest.raises(WebResearchRuntimeDependencyError, match="deny by default"):
        await tool.ainvoke({"url": "https://example.com/"})
    assert NoSubprocess.calls == 0


@pytest.mark.asyncio
async def test_browser_tool_refuses_a_granted_name_that_resolves_to_a_private_address(
    tmp_path: Path,
) -> None:
    import sys

    from app.integrations.agents.deep_agents.browser_tool import (
        AgentBrowserPageTool,
        granted_network_hosts,
    )
    from app.integrations.web_research_runtime import WebResearchRuntimeDependencyError

    answers = {
        "internal.example.com": ("93.184.215.14", "10.0.0.7"),
        "meta.example.com": ("169.254.169.254",),
        "v6.example.com": ("::1",),
        "gone.example.com": (),
    }
    resolved: list[str] = []

    async def resolver(host: str) -> tuple[str, ...]:
        resolved.append(host)
        return answers[host]

    class NoSubprocess:
        async def run(self, request: object) -> object:
            raise AssertionError("no subprocess for a non-public resolution")

    entrypoint = tmp_path / "agent-browser.js"
    entrypoint.write_text("// pinned entrypoint stand-in", encoding="utf-8")
    tool = AgentBrowserPageTool(
        node_executable=Path(sys.executable),
        entrypoint=entrypoint,
        runner=NoSubprocess(),
        resolver=resolver,
    )
    with granted_network_hosts(frozenset(answers)):
        for host in answers:
            with pytest.raises(WebResearchRuntimeDependencyError, match="resolve"):
                await tool.ainvoke({"url": f"https://{host}/"})
    assert resolved == list(answers)


def test_worker_launches_only_the_pinned_mcp_module_command_and_arguments(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """RRM-009 review: the worker re-verifies the pinned module digest and requires the
    pinned command and arguments; a binding that keeps the pinned schema digest but names
    another command or module, or a tampered module, is refused before any launch."""

    import sys
    from types import SimpleNamespace

    import app.integrations.capability_pins as capability_pins

    monkeypatch.setattr(capability_pins, "workspace_root", lambda: tmp_path)
    package = tmp_path / ".tools" / "fixture-mcp"
    (package / "dist").mkdir(parents=True)
    module = package / "dist" / "index.js"
    module.write_bytes(b"// reviewed fixture MCP server")
    (package / "package.json").write_text(
        json.dumps({"name": "fixture-mcp", "version": "1.0.0"}), encoding="utf-8"
    )
    pin = capability_pins.PinnedMCPServer.model_validate(
        {
            "server_id": "fixture-mcp",
            "server_name": "fixture",
            "ref": {
                "kind": "mcp_server",
                "logical_id": "mcp.fixture",
                "revision": 1,
                "digest": MCP_DIGEST,
            },
            "package_name": "fixture-mcp",
            "package_version": "1.0.0",
            "module_locator": "workspace://.tools/fixture-mcp/dist/index.js",
            "module_digest": "sha256:" + sha256(module.read_bytes()).hexdigest(),
            "schema_digest": MCP_DIGEST,
            "credential_env": "FIXTURE_API_KEY",
            "tools": [{"tool_name": "lookup_fixture", "schema_digest": MCP_DIGEST}],
        }
    )
    node = Path(sys.executable)
    pins = CapabilityPins(mcp_servers=(pin,))
    verifier = PinnedCapabilityAssetVerifier(pins, node_executable=node)
    exact = pin.component(node_executable=node)

    def binding(*components: object) -> object:
        return SimpleNamespace(deep_agent_binding=SimpleNamespace(mcp_servers=components))

    verifier.verify_launch(binding(exact))  # type: ignore[arg-type]
    other_command = exact.model_copy(update={"command": str(tmp_path / "evil.exe")})
    other_module = exact.model_copy(update={"arguments": (str(tmp_path / "evil.js"),)})
    no_credential = exact.model_copy(update={"credential_refs": ()})
    for drifted in (other_command, other_module, no_credential):
        assert drifted.schema_digest == pin.schema_digest
        with pytest.raises(ValueError, match="launch differs from its pin"):
            verifier.verify_launch(binding(drifted))  # type: ignore[arg-type]
    unpinned = exact.model_copy(
        update={"ref": exact.ref.model_copy(update={"digest": "sha256:" + "7" * 64})}
    )
    with pytest.raises(ValueError, match="is not pinned"):
        verifier.verify_launch(binding(unpinned))  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="requires the deployment's node"):
        PinnedCapabilityAssetVerifier(pins).verify_launch(binding(exact))  # type: ignore[arg-type]
    # A tampered module is refused even though the binding is unchanged.
    module.write_bytes(b"// tampered")
    with pytest.raises(ValueError, match="failed verification"):
        verifier.verify_launch(binding(exact))  # type: ignore[arg-type]
    # A component the deployment registered exactly is compared by full equality.
    registered = PinnedCapabilityAssetVerifier(
        CapabilityPins.empty(), registered_mcp_servers={exact.ref.digest: exact}
    )
    registered.verify_launch(binding(exact))  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="registered component"):
        registered.verify_launch(binding(other_command))  # type: ignore[arg-type]
    # Missing pins fail closed in the production compositions.
    from app.config import get_settings

    monkeypatch.setenv("CAPABILITY_PINS_PATH", str(tmp_path / "absent.json"))
    get_settings.cache_clear()
    try:
        with pytest.raises(capability_pins.CapabilityPinError, match="required but absent"):
            CapabilityPins.from_settings(get_settings())
        assert CapabilityPins.from_settings(get_settings(), required=False) == CapabilityPins()
    finally:
        get_settings.cache_clear()
