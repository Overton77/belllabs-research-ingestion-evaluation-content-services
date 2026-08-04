from __future__ import annotations

from typing import Any, Protocol

from fastmcp import Context, FastMCP

COORDINATOR_PROMPT_NAMES = (
    "propose_workflow",
    "review_workflow_design",
    "explain_launch_blocker",
    "summarize_workflow_result",
)


class PromptFacade(Protocol):
    async def prompt(
        self,
        principal: Any,
        name: str,
        arguments: dict[str, str],
    ) -> str: ...


class PromptPrincipalResolver(Protocol):
    async def resolve(self, context: Context) -> Any: ...


def register_prompts(
    server: FastMCP,
    facade: PromptFacade,
    principals: PromptPrincipalResolver,
) -> None:
    async def render(context: Context, name: str, arguments: dict[str, str]) -> str:
        return await facade.prompt(await principals.resolve(context), name, arguments)

    @server.prompt
    async def propose_workflow(objective: str, context: Context) -> str:
        return await render(context, "propose_workflow", {"objective": objective})

    @server.prompt
    async def review_workflow_design(design: str, context: Context) -> str:
        return await render(context, "review_workflow_design", {"design": design})

    @server.prompt
    async def explain_launch_blocker(blocker: str, context: Context) -> str:
        return await render(context, "explain_launch_blocker", {"blocker": blocker})

    @server.prompt
    async def summarize_workflow_result(result: str, context: Context) -> str:
        return await render(context, "summarize_workflow_result", {"result": result})
