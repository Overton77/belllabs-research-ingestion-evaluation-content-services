from __future__ import annotations

import pytest
from fastmcp import Client, Context

from app.mcp.coordinator_server import (
    CoordinatorPrincipal,
    create_coordinator_server,
)


class StaticPrincipalResolver:
    def __init__(self, *, launch: bool = False) -> None:
        permissions = frozenset({"workflow.launch"}) if launch else frozenset()
        self.principal = CoordinatorPrincipal(
            actor_id="operator-1",
            tenant_scope="tenant-a",
            roles=frozenset({"operator"}),
            permissions=permissions,
        )

    async def resolve(self, _context: Context) -> CoordinatorPrincipal:
        return self.principal


class FakeFacade:
    def __init__(self) -> None:
        self.search_requests: list[dict[str, object]] = []

    async def bootstrap(self, principal: CoordinatorPrincipal) -> object:
        return {
            "actor_id": principal.actor_id,
            "supported_blueprint_families": ["StageGraph", "GoalDirected"],
            "executable_blueprint_families": ["StageGraph", "GoalDirected"],
        }

    async def search(
        self,
        principal: CoordinatorPrincipal,
        request: dict[str, object],
    ) -> object:
        assert request["tenant_scope"] == principal.tenant_scope
        self.search_requests.append(request)
        return [{"exact_ref": {"kind": "workflow_type", "logical_id": "schema-context"}}]

    async def get_capability(
        self,
        _principal: CoordinatorPrincipal,
        exact_ref: dict[str, object],
    ) -> object:
        return exact_ref

    async def discover_mcp_servers(
        self,
        _principal: CoordinatorPrincipal,
        query: str,
    ) -> object:
        return [{"candidate_id": f"mcp:{query}", "authorization_state": "candidate_only"}]

    async def discover_agent_skills(
        self,
        _principal: CoordinatorPrincipal,
        query: str,
    ) -> object:
        return [{"candidate_id": f"skill:{query}", "authorization_state": "candidate_only"}]

    async def inspect_external_candidate(
        self,
        _principal: CoordinatorPrincipal,
        candidate_id: str,
    ) -> object:
        return {"candidate_id": candidate_id, "inspection_status": "pending"}

    async def validate_workflow_design(
        self,
        _principal: CoordinatorPrincipal,
        draft: dict[str, object],
    ) -> object:
        return {"valid": bool(draft)}

    async def prepare_workflow_launch(
        self,
        _principal: CoordinatorPrincipal,
        proposal: dict[str, object],
    ) -> object:
        return {"ticket_id": "ticket-1", "request_scope": proposal["request_scope"]}

    async def launch_workflow(
        self,
        _principal: CoordinatorPrincipal,
        ticket_id: str,
        idempotency_issuer: str,
        idempotency_key: str,
    ) -> object:
        return {
            "ticket_id": ticket_id,
            "run_id": f"{idempotency_issuer}:{idempotency_key}",
        }

    async def get_workflow_result(
        self,
        _principal: CoordinatorPrincipal,
        run_id: str,
    ) -> object:
        return {"run_id": run_id, "phase": "completed"}

    async def resource(
        self,
        principal: CoordinatorPrincipal,
        uri: str,
    ) -> str | dict[str, object]:
        return {"uri": uri, "tenant_scope": principal.tenant_scope}

    async def prompt(
        self,
        _principal: CoordinatorPrincipal,
        name: str,
        arguments: dict[str, str],
    ) -> str:
        return f"prompt:{name}:{arguments}"


@pytest.mark.asyncio
async def test_in_memory_mcp_bootstrap_search_resources_and_prompts() -> None:
    facade = FakeFacade()
    server = create_coordinator_server(facade, StaticPrincipalResolver())

    async with Client(server) as client:
        tools = {tool.name: tool for tool in await client.list_tools()}
        assert tools["discover_mcp_servers"].annotations.readOnlyHint is False
        assert tools["discover_agent_skills"].annotations.readOnlyHint is False
        assert tools["discover_mcp_servers"].annotations.openWorldHint is True

        bootstrap = await client.call_tool("coordinator_bootstrap")
        assert bootstrap.data["ok"] is True
        assert bootstrap.data["data"]["supported_blueprint_families"] == [
            "StageGraph",
            "GoalDirected",
        ]

        search = await client.call_tool(
            "search_capabilities",
            {
                "query": "select relevant schema context",
                "kinds": ["workflow_type"],
                "limit": 5,
            },
        )
        assert search.data["ok"] is True
        assert facade.search_requests[0]["tenant_scope"] == "tenant-a"

        resources = await client.read_resource(
            "belllabs://workflow-types/schema-context-selection/1/contract"
        )
        assert "tenant-a" in resources[0].text

        prompt = await client.get_prompt(
            "propose_workflow",
            {"objective": "research a current claim"},
        )
        assert "prompt:propose_workflow" in prompt.messages[0].content.text


@pytest.mark.asyncio
async def test_launch_is_separately_authorized_and_preparation_is_tenant_bound() -> None:
    facade = FakeFacade()
    server = create_coordinator_server(facade, StaticPrincipalResolver())

    async with Client(server) as client:
        launch = await client.call_tool(
            "launch_workflow",
            {
                "ticket_id": "ticket-1",
                "idempotency_issuer": "operator",
                "idempotency_key": "once",
            },
        )
        assert launch.data["ok"] is False
        assert launch.data["error"]["code"] == "FORBIDDEN"

        preparation = await client.call_tool(
            "prepare_workflow_launch",
            {"proposal": {"request_scope": "tenant-b"}},
        )
        assert preparation.data["ok"] is False
        assert preparation.data["error"]["code"] == "INVALID_ARGUMENT"
