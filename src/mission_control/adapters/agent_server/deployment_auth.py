"""One authentication entrypoint; scoped credentials never choose server topology."""

from typing import Any

from langgraph_sdk import Auth

from mission_control.adapters.agent_server import auth as runtime_auth
from mission_control.adapters.agent_server.async_subagents import auth as subordinate_auth
from mission_control.adapters.agent_server.block_c_qualification import auth as qualification_auth
from mission_control.adapters.agent_server.deployment import profile, require_assistant

auth = Auth()


@auth.authenticate
async def authenticate(headers: dict[bytes, bytes]) -> Auth.types.MinimalUserDict:
    if profile() != "runtime":
        return await qualification_auth.authenticate(headers)
    authorization = headers.get(b"authorization", b"").lower()
    if authorization.startswith(b"bearer bl1."):
        user = await subordinate_auth.authenticate(headers)
        user["permissions"] = (*user["permissions"], "credential:subordinate")
        return user
    return await runtime_auth.authenticate(headers)


def subordinate(ctx: Auth.types.AuthContext) -> bool:
    return "credential:subordinate" in (getattr(ctx.user, "permissions", ()) or ())


@auth.on
async def default_deny(ctx: Auth.types.AuthContext, value: Any) -> bool:
    return await runtime_auth.default_deny(ctx, value)


@auth.on.threads
async def threads(ctx: Auth.types.AuthContext, value: dict[str, Any]) -> dict[str, str]:
    # Native Agent Protocol emits runs as threads/create_run (including inmem).
    if str(ctx.action).lower().endswith("create_run"):
        require_assistant(value.get("assistant_id"))
    return await runtime_auth.authorize_threads(ctx, value)


@auth.on(resources="runs")
async def runs(ctx: Auth.types.AuthContext, value: dict[str, Any]) -> dict[str, str]:
    if "create" in str(ctx.action).lower():
        require_assistant(value.get("assistant_id"))
    return await runtime_auth.authorize_runs(ctx, value)


@auth.on.assistants
async def assistants(ctx: Auth.types.AuthContext, value: dict[str, Any]) -> dict[str, str]:
    if value.get("assistant_id") is not None:
        require_assistant(value["assistant_id"])
    return await runtime_auth.authorize_assistants(ctx, value)


@auth.on.store()
async def store(ctx: Auth.types.AuthContext, value: dict[str, Any]) -> bool:
    if subordinate(ctx) or profile() != "runtime":
        return await subordinate_auth.deny_store(ctx, value)
    return await runtime_auth.authorize_store(ctx, value)


@auth.on.crons
async def crons(ctx: Auth.types.AuthContext, value: Any) -> bool:
    return await runtime_auth.deny_crons(ctx, value)
