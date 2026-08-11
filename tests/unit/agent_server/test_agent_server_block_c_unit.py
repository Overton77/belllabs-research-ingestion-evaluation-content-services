"""Offline Block C qualification graph/auth harness tests (no live server)."""

from __future__ import annotations

import base64
import contextlib
import json
from pathlib import Path

import pytest
import yaml
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, START, StateGraph
from langgraph.types import Command, Send
from langgraph_cli.cli import prepare_args_and_stdin
from langgraph_cli.config import validate_config_file
from langgraph_cli.docker import DockerCapabilities

from app.agent_server import block_c_qualification as block_c_pkg
from app.agent_server.block_c_qualification import graph as graph_mod
from app.agent_server.block_c_qualification import graph_n1 as n1_mod
from app.agent_server.block_c_qualification import wait_graph as wait_mod
from app.agent_server.block_c_qualification.auth import resolve_public_key_pem
from app.agent_server.block_c_qualification.compat import (
    ASSEMBLY_N,
    ASSEMBLY_ROLE_N,
    ASSEMBLY_ROLE_N1,
    COMPAT_VERSION_N,
    COMPAT_VERSION_N1,
    GRAPH_ID_N,
    GRAPH_ID_N1,
    GRAPH_ID_WAIT,
)
from app.agent_server.block_c_qualification.compat_route import (
    IncompatibleResumeRouteError,
    decide_resume_route,
    require_compatible_resume_route,
)
from app.agent_server.block_c_qualification.deployment_route import (
    decide_deployment_resume_route,
)
from app.agent_server.block_c_qualification.guarded_resume import (
    guarded_deployment_runs_wait,
    guarded_runs_wait,
)
from app.agent_server.block_c_qualification.state import QualificationState, WaitState
from tests.fixtures.agent_server_block_c import (
    BLOCK_C_CONFIG,
    BLOCK_C_N1_CONFIG,
    is_missing_n_graph_on_n1_error,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
ROOT_LANGGRAPH = PROJECT_ROOT / "langgraph.json"
QUAL_PKG_DIR = Path(block_c_pkg.__file__).resolve().parent
BLOCK_C_ENV_FILE = PROJECT_ROOT / "langgraph.block_c.env"


def test_root_langgraph_does_not_select_block_c_auth_or_graphs() -> None:
    config = json.loads(ROOT_LANGGRAPH.read_text(encoding="utf-8"))
    assert config["auth"]["path"] == "app.agent_server.auth:auth"
    assert "block_c_" not in str(config["graphs"])
    assert "block_c_qualification" not in json.dumps(config)
    assert BLOCK_C_CONFIG.name != ROOT_LANGGRAPH.name


def test_qualification_langgraph_config_registers_importable_module_paths_only() -> None:
    config = json.loads(BLOCK_C_CONFIG.read_text(encoding="utf-8"))
    assert set(config["graphs"]) == {
        "block_c_qualification",
        "block_c_qualification_n1",
        "block_c_wait",
    }
    assert config["dependencies"] == ["."]
    # Dict env is broken in langgraph-cli (double-indented Compose YAML).
    assert config["env"] == "langgraph.block_c.env"
    assert isinstance(config["env"], str)
    assert config["auth"]["path"] == "app.agent_server.block_c_qualification.auth:auth"
    assert config["auth"]["disable_studio_auth"] is True
    assert config["http"]["app"] == "app.agent_server.block_c_qualification.http_app:app"
    for graph_path in config["graphs"].values():
        assert isinstance(graph_path, str)
        assert ":" in graph_path
        assert not graph_path.startswith("./")
        assert graph_path.startswith("app.agent_server.block_c_qualification.")


def test_n1_deployment_config_registers_only_n1_graph_and_shared_env() -> None:
    config = json.loads(BLOCK_C_N1_CONFIG.read_text(encoding="utf-8"))
    assert config["dependencies"] == ["."]
    assert config["env"] == "langgraph.block_c.env"
    assert set(config["graphs"]) == {"block_c_qualification_n1"}
    assert config["graphs"]["block_c_qualification_n1"].startswith(
        "app.agent_server.block_c_qualification."
    )
    assert config["auth"]["path"] == "app.agent_server.block_c_qualification.auth:auth"
    assert GRAPH_ID_N not in config["graphs"]
    assert GRAPH_ID_WAIT not in config["graphs"]
    # Production root config remains untouched by the N+1 assembly file.
    root = json.loads(ROOT_LANGGRAPH.read_text(encoding="utf-8"))
    assert "block_c_" not in str(root["graphs"])


def _compose_stdin_for(config_path: Path, *, port: int) -> str:
    config = validate_config_file(config_path)
    caps = DockerCapabilities(
        version_docker=(24, 0, 0),
        version_compose=(2, 20, 0),
        healthcheck_start_interval=True,
        compose_type="plugin",
    )
    _args, stdin = prepare_args_and_stdin(
        capabilities=caps,
        config_path=config_path.resolve(),
        config=config,
        docker_compose=None,
        port=port,
        watch=False,
        postgres_uri=(
            "postgresql://postgres:postgres@127.0.0.1:5432/belllabs_langgraph_stage3"
        ),
    )
    return stdin


def test_block_c_compose_generation_accepts_env_file_with_dummy_values() -> None:
    """LangGraph CLI must emit parseable Compose when env is a file path."""

    assert BLOCK_C_ENV_FILE.is_file()
    stdin = _compose_stdin_for(BLOCK_C_CONFIG, port=8133)
    parsed = yaml.safe_load(stdin)
    assert "services" in parsed
    assert "langgraph-api" in parsed["services"]
    assert "env_file: langgraph.block_c.env" in stdin
    assert "LANGSMITH_API_KEY:" not in stdin
    assert "dockerfile_inline:" in stdin
    # External Postgres URI => no bundled postgres service; Redis remains local.
    assert "langgraph-redis" in parsed["services"]
    assert "langgraph-postgres" not in parsed["services"]


def test_block_c_n1_compose_generation_shares_env_and_external_postgres() -> None:
    assert BLOCK_C_ENV_FILE.is_file()
    stdin = _compose_stdin_for(BLOCK_C_N1_CONFIG, port=8134)
    parsed = yaml.safe_load(stdin)
    assert "langgraph-api" in parsed["services"]
    assert "env_file: langgraph.block_c.env" in stdin
    assert '"8134:8000"' in stdin or "8134:8000" in stdin
    assert "langgraph-redis" in parsed["services"]
    assert "langgraph-postgres" not in parsed["services"]
    assert "block_c_qualification_n1" in stdin or "graph_n1" in stdin


def test_resolve_public_key_pem_prefers_single_line_b64(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pem = "-----BEGIN PUBLIC KEY-----\nMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8A\n-----END PUBLIC KEY-----"
    monkeypatch.delenv("BELL_LABS_AGENT_AUTH_PUBLIC_KEY", raising=False)
    monkeypatch.setenv(
        "BELL_LABS_AGENT_AUTH_PUBLIC_KEY_B64",
        base64.b64encode(pem.encode("ascii")).decode("ascii"),
    )
    assert resolve_public_key_pem() == pem


def test_resolve_public_key_pem_accepts_direct_pem(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pem = "-----BEGIN PUBLIC KEY-----\nABC\n-----END PUBLIC KEY-----"
    monkeypatch.setenv("BELL_LABS_AGENT_AUTH_PUBLIC_KEY", pem)
    monkeypatch.delenv("BELL_LABS_AGENT_AUTH_PUBLIC_KEY_B64", raising=False)
    assert resolve_public_key_pem() == pem


def test_compat_route_allows_exact_n_on_n_and_rejects_n_to_n1() -> None:
    allow = decide_resume_route(
        source_graph_id=GRAPH_ID_N,
        source_compat_version=COMPAT_VERSION_N,
        target_graph_id=GRAPH_ID_N,
        target_compat_version=COMPAT_VERSION_N,
    )
    assert allow.allowed is True
    deny = decide_resume_route(
        source_graph_id=GRAPH_ID_N,
        source_compat_version=COMPAT_VERSION_N,
        target_graph_id=GRAPH_ID_N1,
        target_compat_version=COMPAT_VERSION_N1,
    )
    assert deny.allowed is False
    assert "fail open" in deny.reason
    with pytest.raises(IncompatibleResumeRouteError):
        require_compatible_resume_route(
            source_graph_id=GRAPH_ID_N,
            source_compat_version=COMPAT_VERSION_N,
            target_graph_id=GRAPH_ID_N1,
            target_compat_version=COMPAT_VERSION_N1,
        )
    # Non-vacuous: require still returns the allow decision for exact N-on-N.
    assert (
        require_compatible_resume_route(
            source_graph_id=GRAPH_ID_N,
            source_compat_version=COMPAT_VERSION_N,
            target_graph_id=GRAPH_ID_N,
            target_compat_version=COMPAT_VERSION_N,
        ).allowed
        is True
    )


@pytest.mark.asyncio
async def test_guarded_resume_never_invokes_provider_on_n_to_n1() -> None:
    calls = {"n": 0}

    async def provider(*_args: object, **_kwargs: object) -> dict[str, str]:
        calls["n"] += 1
        return {"ok": "should-not-run"}

    with pytest.raises(IncompatibleResumeRouteError) as raised:
        await guarded_runs_wait(
            source_graph_id=GRAPH_ID_N,
            source_compat_version=COMPAT_VERSION_N,
            target_graph_id=GRAPH_ID_N1,
            target_compat_version=COMPAT_VERSION_N1,
            runs_wait=provider,
            thread_id="thread-n",
            assistant_id="assistant-n1",
            command={"resume": "should-not-apply"},
        )
    assert raised.value.decision.allowed is False
    assert calls["n"] == 0


@pytest.mark.asyncio
async def test_guarded_resume_invokes_provider_only_after_n_on_n_allow() -> None:
    calls: list[tuple[object, ...]] = []

    async def provider(thread_id: str, assistant_id: str, **kwargs: object) -> dict[str, object]:
        calls.append((thread_id, assistant_id, kwargs.get("command")))
        return {"dispatched": True}

    result = await guarded_runs_wait(
        source_graph_id=GRAPH_ID_N,
        source_compat_version=COMPAT_VERSION_N,
        target_graph_id=GRAPH_ID_N,
        target_compat_version=COMPAT_VERSION_N,
        runs_wait=provider,
        thread_id="thread-n",
        assistant_id="assistant-n",
        command={"resume": "n-ok"},
    )
    assert result.decision.allowed is True
    assert result.provider_result == {"dispatched": True}
    assert calls == [("thread-n", "assistant-n", {"resume": "n-ok"})]


def test_missing_n_graph_on_n1_error_classifier_matches_pinned_message() -> None:
    message = (
        "Graph 'block_c_qualification' not found. "
        "Expected ['block_c_qualification_n1']"
    )
    assert is_missing_n_graph_on_n1_error(Exception(message))
    assert not is_missing_n_graph_on_n1_error(Exception("unrelated failure"))


def test_deployment_route_sends_n_checkpoint_to_n_assembly_from_n1_inspection() -> None:
    decision = decide_deployment_resume_route(
        source_graph_id=GRAPH_ID_N,
        source_compat_version=COMPAT_VERSION_N,
        inspection_assembly_role=ASSEMBLY_ROLE_N1,
    )
    assert decision.allowed is True
    assert decision.resume_assembly_role == ASSEMBLY_ROLE_N
    assert decision.resume_assembly_id == ASSEMBLY_N
    assert decision.resume_graph_id == GRAPH_ID_N
    assert decision.resume_compat_version == COMPAT_VERSION_N
    assert decision.inspection_assembly_role == ASSEMBLY_ROLE_N1


@pytest.mark.asyncio
async def test_guarded_deployment_resume_never_calls_n1_callback() -> None:
    n_calls = {"n": 0}
    n1_calls = {"n": 0}

    async def wait_n(thread_id: str, assistant_id: str, **kwargs: object) -> dict[str, object]:
        n_calls["n"] += 1
        return {
            "thread_id": thread_id,
            "assistant_id": assistant_id,
            "command": kwargs.get("command"),
        }

    async def wait_n1(*_args: object, **_kwargs: object) -> dict[str, str]:
        n1_calls["n"] += 1
        return {"ok": "should-not-run"}

    result = await guarded_deployment_runs_wait(
        source_graph_id=GRAPH_ID_N,
        source_compat_version=COMPAT_VERSION_N,
        inspection_assembly_role=ASSEMBLY_ROLE_N1,
        runs_wait_by_role={
            ASSEMBLY_ROLE_N: wait_n,
            ASSEMBLY_ROLE_N1: wait_n1,
        },
        assistant_id_by_graph={GRAPH_ID_N: "assistant-n"},
        thread_id="thread-shared",
        command={"resume": "deploy-n-ok"},
    )
    assert result.dispatched_assembly_role == ASSEMBLY_ROLE_N
    assert result.provider_result["assistant_id"] == "assistant-n"
    assert n_calls["n"] == 1
    assert n1_calls["n"] == 0


def test_qualification_modules_are_import_safe() -> None:
    assert graph_mod.graph is not None
    assert n1_mod.graph is not None
    assert wait_mod.graph is not None


def test_single_interrupt_records_idempotent_claim_and_resumes() -> None:
    builder = StateGraph(QualificationState)
    builder.add_node("record_claim", graph_mod.record_claim)
    builder.add_node("single_interrupt", graph_mod.single_interrupt)
    builder.add_edge(START, "record_claim")
    builder.add_edge("record_claim", "single_interrupt")
    builder.add_edge("single_interrupt", END)
    compiled = builder.compile(checkpointer=InMemorySaver())
    config = {"configurable": {"thread_id": "block-c-unit-single"}}
    first = compiled.invoke(
        {
            "request_scope": "tenant-a",
            "scenario": "single_interrupt",
            "compat_version": "",
            "claim_tokens": [],
            "decisions": [],
            "events": (),
            "decision_refs": (),
        },
        config,
    )
    assert first["__interrupt__"]
    state = compiled.get_state(config)
    assert graph_mod.STABLE_CLAIM in (state.values.get("claim_tokens") or [])
    resumed = compiled.invoke(Command(resume="approved"), config)
    assert resumed["decisions"] == ["approved"]
    assert resumed["claim_tokens"].count(graph_mod.STABLE_CLAIM) == 1
    # Idempotent re-entry of the claim node does not duplicate the token.
    again = graph_mod.record_claim(resumed)  # type: ignore[arg-type]
    assert "claim_tokens" not in again
    assert "claim-already-present" in again["events"]


def test_parallel_interrupt_ids_are_distinct() -> None:
    def route(state: dict) -> list[Send]:
        return [
            Send("parallel_lane_a", state),
            Send("parallel_lane_b", state),
        ]

    builder = StateGraph(QualificationState)
    builder.add_node("parallel_lane_a", graph_mod.parallel_lane_a)
    builder.add_node("parallel_lane_b", graph_mod.parallel_lane_b)
    builder.add_conditional_edges(START, route, ["parallel_lane_a", "parallel_lane_b"])
    builder.add_edge("parallel_lane_a", END)
    builder.add_edge("parallel_lane_b", END)
    compiled = builder.compile(checkpointer=InMemorySaver())
    config = {"configurable": {"thread_id": "block-c-unit-parallel"}}
    first = compiled.invoke(
        {
            "request_scope": "tenant-a",
            "scenario": "parallel_interrupts",
            "compat_version": "",
            "claim_tokens": [],
            "decisions": [],
            "events": (),
            "decision_refs": (),
        },
        config,
    )
    interrupts = first["__interrupt__"]
    assert len(interrupts) == 2
    ids = {item.id for item in interrupts}
    refs = {item.value["decision_ref"] for item in interrupts}
    assert len(ids) == 2
    assert refs == {
        graph_mod.DECISION_REF_PARALLEL_A,
        graph_mod.DECISION_REF_PARALLEL_B,
    }
    resume_map = {item.id: f"ok-{item.value['lane']}" for item in interrupts}
    resumed = compiled.invoke(Command(resume=resume_map), config)
    assert sorted(resumed["decisions"]) == ["a:ok-a", "b:ok-b"]


def test_n1_graph_uses_incompatible_claim_channel_and_node() -> None:
    assert "approve_v2" in n1_mod.graph.nodes
    assert "record_claim_v2" in n1_mod.graph.nodes
    assert "single_interrupt" in graph_mod.graph.nodes
    assert "record_claim" in graph_mod.graph.nodes
    assert "approve_v2" not in graph_mod.graph.nodes
    n_source = (QUAL_PKG_DIR / "graph.py").read_text(encoding="utf-8")
    n1_source = (QUAL_PKG_DIR / "graph_n1.py").read_text(encoding="utf-8")
    assert "claim_tokens_v2" in n1_source
    assert "claim_tokens_v2" not in n_source


@pytest.mark.asyncio
async def test_wait_graph_marks_waiting_before_hold() -> None:
    builder = StateGraph(WaitState)
    builder.add_node("enter_wait", wait_mod.enter_wait)
    builder.add_node("hold", wait_mod.hold)
    builder.add_edge(START, "enter_wait")
    builder.add_edge("enter_wait", "hold")
    builder.add_edge("hold", END)
    compiled = builder.compile(checkpointer=InMemorySaver())
    config = {"configurable": {"thread_id": "block-c-unit-wait"}}
    await compiled.ainvoke(
        {
            "request_scope": "tenant-a",
            "compat_version": "",
            "wait_status": "idle",
            "resource_open": False,
            "events": (),
            "hold_seconds": 0.01,
        },
        config,
        interrupt_after=["enter_wait"],
    )
    state = await compiled.aget_state(config)
    assert state.values["wait_status"] == "waiting"
    assert state.values["resource_open"] is True


@pytest.mark.asyncio
async def test_wait_graph_cancel_writes_typed_cancelled_cleanup() -> None:
    """Mirror Agent Server cancel: CancelledError(UserInterrupt) during hold."""

    import asyncio

    class UserInterrupt(Exception):
        pass

    builder = StateGraph(WaitState)
    builder.add_node("enter_wait", wait_mod.enter_wait)
    builder.add_node("hold", wait_mod.hold)
    builder.add_edge(START, "enter_wait")
    builder.add_edge("enter_wait", "hold")
    builder.add_edge("hold", END)
    compiled = builder.compile(checkpointer=InMemorySaver())
    config = {"configurable": {"thread_id": "block-c-unit-cancel"}}

    class Done:
        def __init__(self) -> None:
            self._event = asyncio.Event()
            self.value: Exception | None = None

        def set(self, value: Exception | None = None) -> None:
            self.value = value
            self._event.set()

        async def wait(self) -> Exception | None:
            await self._event.wait()
            return self.value

    done = Done()

    async def wait_if_not_done(coro: object) -> object:
        async with asyncio.TaskGroup() as tg:
            coro_task = tg.create_task(coro)  # type: ignore[arg-type]
            done_task = tg.create_task(done.wait())
            coro_task.add_done_callback(lambda _: done_task.cancel())
            done_task.add_done_callback(lambda _: coro_task.cancel(done.value))
            try:
                return await coro_task
            except asyncio.CancelledError as error:
                if error.args and isinstance(error.args[0], Exception):
                    raise error.args[0] from None
                raise

    async def run() -> object:
        return await compiled.ainvoke(
            {
                "request_scope": "tenant-a",
                "compat_version": "",
                "wait_status": "idle",
                "resource_open": False,
                "events": (),
                "hold_seconds": 30,
            },
            config,
        )

    task = asyncio.create_task(wait_if_not_done(run()))
    for _ in range(50):
        state = await compiled.aget_state(config)
        if state.values.get("wait_status") == "waiting" and state.values.get(
            "resource_open"
        ):
            break
        await asyncio.sleep(0.05)
    else:
        raise AssertionError("enter_wait never checkpointed waiting/open resource")

    done.set(UserInterrupt())
    with contextlib.suppress(UserInterrupt, ExceptionGroup):
        await task

    final = await compiled.aget_state(config)
    assert final.values["wait_status"] == "cancelled"
    assert final.values["resource_open"] is False
    assert "wait-resource-closed-cancelled" in final.values["events"]
