"""FT-D2: `missionctl chain inspect`, `GET /chains/{id}` and `mission_chain_inspect` return the
same `mc.chain.v1` projection under the same scope; the MCP tool is read-only annotated."""

from __future__ import annotations

import json
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from fastmcp import Client, FastMCP

from mission_control.application.chains.service import (
    ChainInspection,
    ChainInspectionService,
    ChainMemberRun,
)
from mission_control.application.installations.registry import (
    ApplicationBinding,
    ApplicationRegistry,
    InstallationObservation,
)
from mission_control.domain.composition.chain import ChainLifecycle, MissionChain
from mission_control.domain.policies.contracts import ActorContext
from mission_control.interfaces.cli import main as cli
from mission_control.interfaces.http.chains import router
from mission_control.interfaces.http.mission_control import MissionPrincipal, get_mission_principal
from mission_control.interfaces.mcp.coordinator_server import CoordinatorPrincipal, _principal_call
from mission_control.interfaces.mcp.mission_tools import (
    CHAIN_TOOL,
    ScopedChains,
    register_chain_tools,
)
from tests.fixtures.provider_frames import INSTALLATION, SCOPE, TENANT
from tests.unit.chains.test_chain_reducer import accepted, apply, build, document, reduce


def inspection(*, completed: bool = False) -> ChainInspection:
    chain = build(document())
    after = apply(chain, reduce(chain, "research", accepted("research", "evidence_map")))
    if completed:
        after = MissionChain.model_validate(
            {
                **after.model_dump(),
                "lifecycle": ChainLifecycle.COMPLETED,
                "terminal_outcome": "accepted",
            }
        )
    return ChainInspection(
        chain=after,
        members=tuple(
            ChainMemberRun(
                mission_key=member.mission_key,
                mission_id=member.mission_id,
                order=member.order,
                status="terminal" if member.order == 0 or completed else "active",
                run_key=f"run-{member.mission_key}",
                phase="terminal" if member.order == 0 or completed else "active",
            )
            for member in after.members
        ),
    )


class Reader:
    def __init__(self, found: ChainInspection, *, completes_after: int | None = None) -> None:
        self.found = found
        self.calls = 0
        self.completes_after = completes_after

    async def inspect(self, request_scope: str, chain_id: UUID) -> ChainInspection | None:
        assert request_scope == SCOPE
        self.calls += 1
        if chain_id != self.found.chain.chain_id:
            return None
        if self.completes_after is not None and self.calls > self.completes_after:
            return inspection(completed=True)
        return self.found

    async def chains_for_mission(
        self, request_scope: str, mission_id: UUID
    ) -> tuple[ChainInspection, ...]:
        assert request_scope == SCOPE
        members = {member.mission_id for member in self.found.chain.members}
        return (self.found,) if mission_id in members else ()


def http_app(
    reader: Reader, permissions: frozenset[str] = frozenset({"workflow_run.read"})
) -> FastAPI:
    registry = ApplicationRegistry(
        (
            ApplicationBinding.seal(
                application_id="biotech",
                installation_id=INSTALLATION,
                binding_version="1",
                supabase_project_ref="project",
                database_secret_ref="TEST_DATABASE_URL",
                accepted_issuers={"https://issuer.invalid"},
                accepted_audiences={"authenticated"},
                required_component_version="1",
            ),
        )
    )
    registry.observe(
        "biotech", InstallationObservation(INSTALLATION, "biotech", "project", frozenset({"1"}))
    )
    app = FastAPI()
    app.include_router(router)
    app.state.mission_control_registry = registry
    app.state.mission_control_chain_services = {
        (INSTALLATION, "biotech", TENANT): ChainInspectionService(reader, request_scope=SCOPE)
    }
    app.dependency_overrides[get_mission_principal] = lambda: MissionPrincipal(
        installation_id=INSTALLATION,
        application_id="biotech",
        tenant_id=TENANT,
        issuer="https://issuer.invalid",
        audiences=frozenset({"authenticated"}),
        actor=ActorContext(actor_id="reader", permissions=permissions),
    )
    return app


def test_http_projection_listing_and_errors() -> None:
    found = inspection()
    client = TestClient(http_app(Reader(found)))
    chain_id = found.chain.chain_id
    response = client.get(f"/v1/applications/biotech/chains/{chain_id}")
    assert response.status_code == 200
    body = response.json()
    assert body["schema_version"] == "mc.chain_inspection.v1"
    assert body["chain"]["schema_version"] == "mc.chain.v1"
    assert {link["state"] for link in body["chain"]["links"]} == {"released"}
    assert [member["status"] for member in body["members"]] == ["terminal", "active"]
    listing = client.get(
        "/v1/applications/biotech/chains",
        params={"mission_id": str(found.chain.members[1].mission_id)},
    )
    assert [item["chain"]["chain_id"] for item in listing.json()["chains"]] == [str(chain_id)]
    assert client.get(f"/v1/applications/biotech/chains/{uuid4()}").status_code == 404
    denied = TestClient(http_app(Reader(found), permissions=frozenset()))
    assert denied.get(f"/v1/applications/biotech/chains/{chain_id}").status_code == 403
    other = client.get(f"/v1/applications/ai-engineer/chains/{chain_id}")
    assert other.status_code == 403


class _ASGIBridge(httpx.BaseTransport):
    def __init__(self, app: FastAPI) -> None:
        self._client = TestClient(app)

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        response = self._client.request(
            request.method, str(request.url), headers=dict(request.headers), content=request.content
        )
        return httpx.Response(
            response.status_code, headers=response.headers, content=response.content
        )


def _cli(monkeypatch: pytest.MonkeyPatch, app: FastAPI) -> None:
    real_client = httpx.Client

    def factory(*args: Any, **kwargs: Any) -> httpx.Client:
        kwargs["transport"] = _ASGIBridge(app)
        return real_client(*args, **kwargs)

    monkeypatch.setenv("MISSION_CONTROL_TOKEN", "token")
    monkeypatch.setenv("MISSION_CONTROL_APPLICATION_ID", "biotech")
    monkeypatch.setattr(cli.httpx, "Client", factory)


def test_cli_chain_inspect_json_and_wait_for_completion(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    found = inspection()
    reader = Reader(found, completes_after=1)
    _cli(monkeypatch, http_app(reader))
    chain_id = str(found.chain.chain_id)
    assert cli.main(["chain", "inspect", chain_id, "--json"]) == 0
    printed = json.loads(capsys.readouterr().out)
    assert printed["chain"]["chain_id"] == chain_id
    assert printed["chain"]["lifecycle"] == "running"
    # --wait polls until the chain completes; accepted exits 0.
    assert cli.main(["chain", "inspect", chain_id, "--wait", "5"]) == 0
    done = json.loads(capsys.readouterr().out)
    assert done["chain"]["lifecycle"] == "completed"
    assert cli.main(["chain", "inspect", str(uuid4())]) == 2  # 404 -> exit 2
    capsys.readouterr()


@pytest.mark.asyncio
async def test_mcp_tool_and_resource_match_http_and_are_read_only() -> None:
    found = inspection()
    principal = CoordinatorPrincipal(
        actor_id="coordinator",
        tenant_scope=SCOPE,
        roles=frozenset(),
        permissions=frozenset({"workflow.result.read"}),
        request_scope=SCOPE,
    )

    class Principals:
        async def resolve(self, context: Any) -> CoordinatorPrincipal:
            del context
            return principal

    service = ChainInspectionService(Reader(found), request_scope=SCOPE)
    server = FastMCP("chain-test")
    register_chain_tools(server, ScopedChains({SCOPE: service}), Principals(), call=_principal_call)
    http_body = (
        TestClient(http_app(Reader(found)))
        .get(f"/v1/applications/biotech/chains/{found.chain.chain_id}")
        .json()
    )
    http_body.pop("application_id")
    async with Client(server) as client:
        tools = {tool.name: tool for tool in await client.list_tools()}
        assert tools[CHAIN_TOOL].annotations is not None
        assert tools[CHAIN_TOOL].annotations.readOnlyHint is True
        result = await client.call_tool(CHAIN_TOOL, {"chain_id": str(found.chain.chain_id)})
        envelope = result.structured_content
        assert envelope is not None and envelope["ok"] is True
        assert envelope["data"] == http_body
        missing = await client.call_tool(CHAIN_TOOL, {"chain_id": str(uuid4())})
        assert missing.structured_content is not None
        assert missing.structured_content["error"]["code"] == "NOT_FOUND"
        resource = await client.read_resource(
            f"mc://applications/biotech/chains/{found.chain.chain_id}"
        )
        assert json.loads(resource[0].text) == http_body  # type: ignore[union-attr]
        with pytest.raises(Exception, match="denied"):
            await client.read_resource(
                f"mc://applications/ai-engineer/chains/{found.chain.chain_id}"
            )
