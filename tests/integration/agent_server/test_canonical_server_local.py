"""Actual canonical Agent Server config, authentication, persistence and cancellation."""

import asyncio
import os
from pathlib import Path

import httpx
import pytest
from langgraph_sdk import get_client
from langgraph_sdk.errors import APIStatusError

from mission_control.adapters.agent_server.async_subagents.auth import mint_scope_claim
from tests.fixtures.mission_control_local_agent_server import (
    deterministic_child_definition,
    local_agent_server,
)
from tests.fixtures.rrm009_production_stack import SCOPE


async def test_canonical_runtime_config_runs_and_cancels_bounded_child(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with local_agent_server(tmp_path, monkeypatch) as endpoint:
        token = mint_scope_claim(os.environ["BELLABS_ASYNC_SUBAGENT_SERVER_TOKEN"], SCOPE)
        client = get_client(url=endpoint, headers={"Authorization": "Bearer " + token})
        async with httpx.AsyncClient() as transport:
            response = await transport.post(endpoint + "/threads", json={})
            assert response.status_code == 401
        thread = await client.threads.create(metadata={"request_scope": SCOPE})
        definition = deterministic_child_definition()
        result = await client.runs.wait(
            thread["thread_id"],
            definition.graph_id,
            input={"messages": [{"role": "user", "content": "say PONG"}]},
        )
        assert result["messages"][-1]["content"] == "PONG"
        assert result["belllabs_served_graph"] == definition.served.model_dump(mode="json")
        state = await client.threads.get_state(thread["thread_id"])
        assert state["checkpoint"]["checkpoint_id"]
        with pytest.raises(APIStatusError) as denied:
            await client.runs.create(
                thread["thread_id"], "block_c_qualification", input={"scenario": "single_interrupt"}
            )
        assert denied.value.status_code == 403

        other = get_client(
            url=endpoint,
            headers={
                "Authorization": "Bearer "
                + mint_scope_claim(
                    os.environ["BELLABS_ASYNC_SUBAGENT_SERVER_TOKEN"], "other-tenant"
                )
            },
        )
        with pytest.raises(APIStatusError):
            await other.threads.get(thread["thread_id"])
        waiting = await client.runs.create(
            thread["thread_id"],
            definition.graph_id,
            input={"messages": [{"role": "user", "content": "wait then PONG"}]},
        )
        async with asyncio.timeout(20):
            while (await client.runs.get(thread["thread_id"], waiting["run_id"]))[  # noqa: ASYNC110
                "status"
            ] == "pending":
                await asyncio.sleep(0.1)
        await client.runs.cancel(
            thread["thread_id"], waiting["run_id"], wait=True, action="interrupt"
        )
        cancelled = await client.runs.get(thread["thread_id"], waiting["run_id"])
        assert cancelled["status"] == "interrupted"


async def test_same_config_n1_profile_denies_old_graph_and_runs_new_graph(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    authorization: list[str] = []
    async with local_agent_server(
        tmp_path, monkeypatch, profile="qualification_n1", authorization_out=authorization
    ) as endpoint:
        client = get_client(url=endpoint, headers={"Authorization": authorization[0]})
        thread = await client.threads.create(metadata={"request_scope": SCOPE})
        with pytest.raises(APIStatusError) as denied:
            await client.runs.create(
                thread["thread_id"], "block_c_qualification", input={"scenario": "single_interrupt"}
            )
        assert denied.value.status_code == 403
        result = await client.runs.wait(
            thread["thread_id"], "block_c_qualification_n1", input={"request_scope": SCOPE}
        )
        assert result["__interrupt__"]
        result = await client.runs.wait(
            thread["thread_id"], "block_c_qualification_n1", command={"resume": "n1-approved"}
        )
        assert result["decisions"] == ["n1-approved"]


async def test_same_config_preserves_authenticated_interrupt_resume_and_thread_copy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    authorization: list[str] = []
    async with local_agent_server(
        tmp_path, monkeypatch, profile="qualification", authorization_out=authorization
    ) as endpoint:
        client = get_client(url=endpoint, headers={"Authorization": authorization[0]})
        thread = await client.threads.create(metadata={"request_scope": SCOPE})
        result = await client.runs.wait(
            thread["thread_id"],
            "block_c_qualification",
            input={"request_scope": SCOPE, "scenario": "single_interrupt"},
        )
        assert result["__interrupt__"]
        snapshot = await client.threads.get_state(thread["thread_id"])
        assert snapshot["checkpoint"]["checkpoint_id"]
        copied = await client.threads.copy(thread["thread_id"])
        assert copied["thread_id"] != thread["thread_id"]
        resumed = await client.runs.wait(
            thread["thread_id"], "block_c_qualification", command={"resume": "approved"}
        )
        assert resumed["decisions"] == ["approved"]
        assert resumed["claim_tokens"] == ["stable-claim:block-c-single"]
        with pytest.raises(APIStatusError) as denied:
            await client.runs.create(
                thread["thread_id"],
                "belllabs_async_technical_child",
                input={"messages": [{"role": "user", "content": "denied"}]},
            )
        assert denied.value.status_code == 403
