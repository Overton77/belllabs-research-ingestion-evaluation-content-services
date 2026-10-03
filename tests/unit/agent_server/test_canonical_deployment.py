"""Profile and credential boundaries of the single Agent Server deployment."""

import json
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid5

import pytest
from langgraph_sdk import Auth

from mission_control.adapters.agent_server import deployment, deployment_auth, entrypoints
from mission_control.adapters.agent_server.async_subagents.auth import mint_scope_claim
from mission_control.adapters.agent_server.hosting import hosted_graph


def test_only_one_deployment_configuration_and_exact_registered_graphs():
    root = Path(__file__).resolve().parents[3]
    canonical = root / "agent_server" / "langgraph.json"
    configurations = [*root.glob("langgraph*.json"), *canonical.parent.glob("*.json")]
    assert configurations == [canonical]
    declared = json.loads(canonical.read_text(encoding="utf-8"))
    assert set(declared["graphs"]) == set().union(*deployment.PROFILES.values())


@pytest.mark.parametrize("profile,graphs", list(deployment.PROFILES.items()))
def test_profiles_select_exact_graphs(monkeypatch, profile, graphs):
    monkeypatch.setenv("MISSION_CONTROL_AGENT_SERVER_PROFILE", profile)
    assert deployment.allowed_graphs() == graphs
    for graph in graphs:
        deployment.require_assistant(uuid5(deployment.GRAPH_NAMESPACE, graph))
    with pytest.raises(Auth.exceptions.HTTPException):
        deployment.require_assistant("unregistered-assistant")


def test_unknown_profile_fails_closed(monkeypatch):
    monkeypatch.setenv("MISSION_CONTROL_AGENT_SERVER_PROFILE", "typo")
    with pytest.raises(RuntimeError, match="unknown"):
        deployment.profile()


async def test_auditor_cannot_copy_threads(monkeypatch):
    monkeypatch.setenv("MISSION_CONTROL_AGENT_SERVER_PROFILE", "qualification")
    ctx = SimpleNamespace(
        action="copy",
        user=SimpleNamespace(identity="auditor", permissions=("role:auditor", "request_scope:a")),
    )
    with pytest.raises(Auth.exceptions.HTTPException):
        await deployment_auth.threads(ctx, {})


async def test_runtime_auth_scopes_child_claim_and_denies_other_graphs(monkeypatch):
    monkeypatch.setenv("MISSION_CONTROL_AGENT_SERVER_PROFILE", "runtime")
    monkeypatch.setenv("BELLABS_ASYNC_SUBAGENT_SERVER_TOKEN", "unit-secret")
    token = mint_scope_claim("unit-secret", "tenant-a")
    user = await deployment_auth.authenticate({b"authorization": ("Bearer " + token).encode()})
    ctx = SimpleNamespace(action="create_run", user=SimpleNamespace(**user))
    allowed = await deployment_auth.runs(ctx, {"assistant_id": deployment.CHILD_GRAPH})
    assert allowed == {"request_scope": "tenant-a"}
    for target in deployment.QUALIFICATION_GRAPHS:
        with pytest.raises(Auth.exceptions.HTTPException):
            await deployment_auth.runs(
                ctx, {"assistant_id": uuid5(deployment.GRAPH_NAMESPACE, target)}
            )
        with pytest.raises(Auth.exceptions.HTTPException):
            await deployment_auth.threads(
                ctx, {"assistant_id": uuid5(deployment.GRAPH_NAMESPACE, target)}
            )
    with pytest.raises(Auth.exceptions.HTTPException):
        await deployment_auth.threads(ctx, {"metadata": {"request_scope": "tenant-b"}})
    with pytest.raises(Auth.exceptions.HTTPException):
        await deployment_auth.store(
            ctx, {"namespace": ["tenant-a", "development", "procedural_memory"]}
        )


def test_qualification_factories_are_disabled_in_runtime(monkeypatch):
    monkeypatch.setenv("MISSION_CONTROL_AGENT_SERVER_PROFILE", "runtime")
    for factory in (entrypoints.qualification, entrypoints.qualification_n1, entrypoints.wait):
        with pytest.raises(Auth.exceptions.HTTPException):
            factory({})
    monkeypatch.setenv("MISSION_CONTROL_AGENT_SERVER_PROFILE", "qualification_n1")
    assert entrypoints.qualification_n1({}) is not None
    with pytest.raises(Auth.exceptions.HTTPException):
        entrypoints.qualification({})


def test_hosting_hook_cannot_be_an_expression_or_path(monkeypatch):
    hosted_graph.cache_clear()
    monkeypatch.setenv("MISSION_CONTROL_AGENT_HOSTING_FACTORY", "../../module:factory()")
    try:
        with pytest.raises(ValueError, match="installed module"):
            hosted_graph()
    finally:
        hosted_graph.cache_clear()
