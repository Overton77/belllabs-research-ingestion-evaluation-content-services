"""Fixtures and helpers for pre-Stage-3 Block C persistent Agent Server drills.

Environment variables (names only; never commit values):
- ``AGENT_SERVER_ENDPOINT`` — N assembly base URL (default ``http://127.0.0.1:8133``)
- ``AGENT_SERVER_ENDPOINT_N1`` — N+1 assembly base URL (default ``http://127.0.0.1:8134``)
- ``BELL_LABS_AGENT_AUTH_ISSUER`` — JWT issuer embedded in minted tokens
- ``BELL_LABS_AGENT_AUTH_PUBLIC_KEY_B64`` — ASCII PEM as standard base64 (preferred)
- ``BELL_LABS_AGENT_AUTH_PUBLIC_KEY`` — PEM public key (tests only; avoid in Compose)
- ``BELL_LABS_AGENT_AUTH_PRIVATE_KEY`` — PEM private key (test mint only)
- ``BELL_LABS_AGENT_AUTH_AUDIENCE`` — optional, default ``authenticated``
- ``BELL_LABS_REQUIRE_STAGE3_ENTRY_SERVICES`` — ``1`` fails closed if missing
- ``BLOCK_C_POSTGRES_URI`` — shared disposable Postgres URI for both assemblies
- ``BLOCK_C_RUN_RESTART_PHASE`` — ``prepare`` | ``resume`` for restart drill
- ``BLOCK_C_RESTART_STATE_PATH`` — JSON path for restart handoff state
- ``BLOCK_C_RUN_NN1_PHASE`` — ``1`` enables same-server unsafe N→N1 fail-open evidence
- ``BLOCK_C_RUN_NN1_DEPLOYMENT`` — ``1`` enables two-endpoint N/N+1 deployment drills

Mint a throwaway local RSA pair (do not commit):

```bash
uv run python - <<'PY'
from joserfc.jwk import RSAKey
key = RSAKey.generate_key(2048)
print(key.as_pem(private=True).decode())
print(key.as_pem(private=False).decode())
PY
```

Two qualification deployments share one disposable Postgres DB and use distinct
Compose projects (each gets its own Redis/API container). Auth comes from the
tracked variable-reference file ``langgraph.block_c.env`` (no secret values).

Boundary (qualification topology only — not Stage 3 / production):
- N config ``langgraph.block_c.json`` registers ``block_c_qualification``,
  ``block_c_qualification_n1``, and ``block_c_wait``.
- N+1 config ``langgraph.block_c_n1.json`` registers only ``block_c_qualification_n1``.
- ``guarded_deployment_runs_wait`` may observe a thread id from N+1 (shared Postgres)
  but always resumes an N checkpoint on the exact N endpoint/assistant.
- Separate N+1 deployment fail-closes ``threads.get_state`` / incompatible resume for
  N graphs (``Graph 'block_c_qualification' not found``). That is accepted evidence,
  not a test failure. Same-server fail-open remains under ``BLOCK_C_RUN_NN1_PHASE=1``.

```bash
# Phase A — N assembly (8133)
export COMPOSE_PROJECT_NAME=belllabs-block-c-qualification
uv run langgraph up \\
  --config langgraph.block_c.json \\
  --postgres-uri \"$BLOCK_C_POSTGRES_URI\" \\
  --port 8133 --wait --verbose --no-pull

# Phase B — N+1 assembly (8134), same Postgres URI, distinct Compose project/Redis
export COMPOSE_PROJECT_NAME=belllabs-block-c-qualification-n1
uv run langgraph up \\
  --config langgraph.block_c_n1.json \\
  --postgres-uri \"$BLOCK_C_POSTGRES_URI\" \\
  --port 8134 --wait --verbose --no-pull
```

Use disposable DB ``belllabs_langgraph_stage3`` only (never primary).


Restart phase (after ``BLOCK_C_RUN_RESTART_PHASE=prepare`` test wrote state):

```bash
docker restart belllabs-block-c-qualification-langgraph-api-1
# wait healthy, then:
BLOCK_C_RUN_RESTART_PHASE=resume \\
BLOCK_C_RESTART_STATE_PATH=/path/to/state.json \\
AGENT_SERVER_ENDPOINT=http://127.0.0.1:8133 \\
BELL_LABS_REQUIRE_STAGE3_ENTRY_SERVICES=1 \\
uv run pytest -q tests/test_agent_server_block_c_persistent.py -m block_c_restart
```
"""

from __future__ import annotations

import hashlib
import json
import os
import time
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
from joserfc import jwt
from joserfc.jwk import RSAKey
from langgraph_sdk import get_client
from langgraph_sdk.client import LangGraphClient
from langgraph_sdk.errors import APIStatusError

from app.agent_server.block_c_qualification.compat import (
    GRAPH_ID_N,
    GRAPH_ID_N1,
    GRAPH_ID_WAIT,
)

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
BLOCK_C_CONFIG = _PROJECT_ROOT / "langgraph.block_c.json"
BLOCK_C_N1_CONFIG = _PROJECT_ROOT / "langgraph.block_c_n1.json"
BLOCK_C_ENV_FILE = _PROJECT_ROOT / "langgraph.block_c.env"


def _require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if value:
        return value
    if os.getenv("BELL_LABS_REQUIRE_STAGE3_ENTRY_SERVICES") == "1":
        pytest.fail(f"{name} is required for Block C persistent evidence")
    pytest.skip(f"{name} is not configured")


@dataclass(frozen=True)
class BlockCAuthMaterial:
    issuer: str
    audience: str
    private_key: RSAKey
    public_pem: str


def load_block_c_auth_material() -> BlockCAuthMaterial:
    issuer = _require_env("BELL_LABS_AGENT_AUTH_ISSUER")
    private_pem = _require_env("BELL_LABS_AGENT_AUTH_PRIVATE_KEY")
    public_pem = os.getenv("BELL_LABS_AGENT_AUTH_PUBLIC_KEY", "").strip()
    private_key = RSAKey.import_key(private_pem)
    if not public_pem:
        public_pem = private_key.as_pem(private=False).decode()
    return BlockCAuthMaterial(
        issuer=issuer.rstrip("/"),
        audience=os.getenv("BELL_LABS_AGENT_AUTH_AUDIENCE", "authenticated"),
        private_key=private_key,
        public_pem=public_pem,
    )


def mint_block_c_token(
    material: BlockCAuthMaterial,
    *,
    subject: str,
    request_scope: str,
    roles: list[str] | None = None,
    ttl_seconds: int = 600,
) -> str:
    """Mint a short-lived RS256 JWT. Never log or persist the token."""

    now = datetime.now(UTC)
    claims = {
        "sub": subject,
        "iss": material.issuer,
        "aud": material.audience,
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(seconds=ttl_seconds)).timestamp()),
        "request_scopes": [request_scope],
        "roles": roles or ["operator"],
    }
    return jwt.encode(
        {"alg": "RS256"},
        claims,
        material.private_key,
        algorithms=["RS256"],
    )


def agent_server_client(endpoint: str, token: str) -> LangGraphClient:
    return get_client(
        url=endpoint.rstrip("/"),
        api_key=None,
        headers={"Authorization": f"Bearer {token}"},
        timeout=60,
    )


def _checkpoint_ref(payload: Mapping[str, Any] | None) -> str:
    if not payload:
        return ""
    raw = payload.get("checkpoint")
    if isinstance(raw, dict):
        return json.dumps(raw, sort_keys=True, default=str)
    if raw:
        return str(raw)
    if payload.get("checkpoint_id"):
        return str(payload["checkpoint_id"])
    return ""


def _stable_digest(parts: Sequence[str]) -> str:
    joined = "\n".join(parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


async def capture_tenant_introspection_snapshot(
    client: LangGraphClient,
    *,
    thread_id: str,
    request_scope: str = "tenant-a",
) -> dict[str, Any]:
    """Capture exact tenant-scoped counts/digests available from the SDK."""

    assistants = await client.assistants.search(limit=50)
    assistant_ids = tuple(sorted(str(item["assistant_id"]) for item in assistants))
    assistant_count = await client.assistants.count()
    thread_count = await client.threads.count(metadata={"request_scope": request_scope})
    thread = await client.threads.get(thread_id)
    state = await client.threads.get_state(thread_id)
    history = await client.threads.get_history(thread_id, limit=50)
    runs = await client.runs.list(thread_id, limit=50)
    values = dict(state.get("values") or {})
    run_pairs = tuple(
        sorted((str(item["run_id"]), str(item.get("status") or "")) for item in runs)
    )
    history_checkpoint_refs = tuple(_checkpoint_ref(item) for item in history)
    store_denied = False
    store_item_count = 0
    try:
        store_page = await client.store.search_items((request_scope,), limit=20)
        items = getattr(store_page, "items", None)
        if items is None and isinstance(store_page, dict):
            items = store_page.get("items") or []
        store_item_count = len(list(items or []))
    except APIStatusError:
        store_denied = True

    return {
        "assistant_count": int(assistant_count),
        "assistant_ids": assistant_ids,
        "assistant_ids_digest": _stable_digest(assistant_ids),
        "thread_count_scope": int(thread_count),
        "thread_status": str(thread.get("status") or ""),
        "thread_metadata": dict(thread.get("metadata") or {}),
        "state_checkpoint_ref": _checkpoint_ref(state),
        "state_values_digest": _stable_digest(
            [json.dumps(values, sort_keys=True, default=str)]
        ),
        "state_value_keys": tuple(sorted(values)),
        "history_count": len(history),
        "history_checkpoint_digest": _stable_digest(history_checkpoint_refs),
        "run_count": len(runs),
        "run_digest": _stable_digest([f"{run_id}:{status}" for run_id, status in run_pairs]),
        "run_pairs": run_pairs,
        "store_denied": store_denied,
        "store_item_count": store_item_count,
    }


async def get_thread_status(client: LangGraphClient, thread_id: str) -> str:
    """Return thread status from ``threads.get`` (not ``get_state``).

    Agent Server 0.12 / SDK 0.4 ThreadState payloads omit top-level ``status``.
    """

    thread = await client.threads.get(thread_id)
    return str(thread.get("status") or "")


async def wait_thread_status(
    client: LangGraphClient,
    thread_id: str,
    *,
    statuses: set[str],
    timeout_seconds: float = 60.0,
) -> Any:
    deadline = time.monotonic() + timeout_seconds
    last = None
    while time.monotonic() < deadline:
        last = await client.threads.get(thread_id)
        if str(last.get("status") or "") in statuses:
            return last
        await _async_sleep(0.25)
    raise TimeoutError(f"thread {thread_id} not in {statuses}; last={last}")


async def wait_run_status(
    client: LangGraphClient,
    thread_id: str,
    run_id: str,
    *,
    statuses: set[str],
    timeout_seconds: float = 60.0,
) -> Any:
    deadline = time.monotonic() + timeout_seconds
    last = None
    while time.monotonic() < deadline:
        last = await client.runs.get(thread_id, run_id)
        if str(last.get("status") or "") in statuses:
            return last
        await _async_sleep(0.25)
    raise TimeoutError(f"run {run_id} not in {statuses}; last={last}")


async def _async_sleep(seconds: float) -> None:
    import asyncio

    await asyncio.sleep(seconds)


def interrupt_payloads(state: Any) -> list[dict[str, Any]]:
    tasks = list(state.get("tasks") or [])
    payloads: list[dict[str, Any]] = []
    for task in tasks:
        for item in task.get("interrupts") or []:
            if isinstance(item, dict):
                payloads.append(item)
            else:
                payloads.append(
                    {
                        "id": getattr(item, "id", None),
                        "value": getattr(item, "value", None),
                    }
                )
    # ThreadState may also expose top-level interrupts.
    top = state.get("interrupts") or []
    if isinstance(top, dict):
        for items in top.values():
            for item in items or []:
                if isinstance(item, dict):
                    payloads.append(item)
    elif isinstance(top, list):
        for item in top:
            if isinstance(item, dict):
                payloads.append(item)
    return payloads


def write_restart_state(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def read_restart_state(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


async def wait_thread_values(
    client: LangGraphClient,
    thread_id: str,
    *,
    predicate: Any,
    timeout_seconds: float = 30.0,
) -> dict[str, Any]:
    """Poll thread state until ``predicate(values, state)`` is true."""

    deadline = time.monotonic() + timeout_seconds
    last: dict[str, Any] | None = None
    while time.monotonic() < deadline:
        state = await client.threads.get_state(thread_id)
        values = dict(state.get("values") or {})
        last = {"values": values, "state": state}
        if predicate(values, state):
            return last
        await _async_sleep(0.1)
    raise TimeoutError(f"thread {thread_id} values predicate not met; last={last}")


async def copy_thread_strict(client: LangGraphClient, parent_id: str) -> str:
    """Copy a thread and require an explicit child thread identity.

    Refuses to guess among tenant threads when the API body is empty.
    """

    response = await client.threads.http.post(f"/threads/{parent_id}/copy", json={})
    child_id: str | None = None
    if isinstance(response, dict):
        raw = response.get("thread_id") or response.get("id")
        if raw:
            child_id = str(raw)
        # Some deployments nest the thread object.
        nested = response.get("thread")
        if child_id is None and isinstance(nested, dict) and nested.get("thread_id"):
            child_id = str(nested["thread_id"])
    if not child_id:
        raise AssertionError(
            "thread copy did not return a child thread_id; refusing to select an "
            "unrelated tenant thread from search results"
        )
    if child_id == parent_id:
        raise AssertionError("thread copy returned the parent thread_id unchanged")
    return child_id


async def lookup_assistant_id(client: LangGraphClient, graph_id: str) -> str:
    assistants = await client.assistants.search(graph_id=graph_id, limit=5)
    assert assistants, f"assistant for {graph_id} not found on endpoint"
    return str(assistants[0]["assistant_id"])


def is_missing_n_graph_on_n1_error(error: BaseException) -> bool:
    """True when N+1 assembly fail-closes because the N graph is not registered."""

    text = str(error)
    lowered = text.lower()
    return (
        GRAPH_ID_N in text
        and GRAPH_ID_N1 in text
        and ("not found" in lowered or "expected" in lowered)
    )


@pytest.fixture(scope="session")
def block_c_endpoint() -> str:
    return _require_env("AGENT_SERVER_ENDPOINT")


@pytest.fixture(scope="session")
def block_c_endpoint_n1() -> str:
    """N+1 assembly endpoint (8134). Required when deployment drills are enabled."""

    value = os.getenv("AGENT_SERVER_ENDPOINT_N1", "").strip()
    if value:
        return value
    if os.getenv("BLOCK_C_RUN_NN1_DEPLOYMENT") == "1":
        if os.getenv("BELL_LABS_REQUIRE_STAGE3_ENTRY_SERVICES") == "1":
            pytest.fail("AGENT_SERVER_ENDPOINT_N1 is required for N/N+1 deployment drills")
        pytest.skip("AGENT_SERVER_ENDPOINT_N1 is not configured")
    pytest.skip("Set BLOCK_C_RUN_NN1_DEPLOYMENT=1 and AGENT_SERVER_ENDPOINT_N1")


@pytest.fixture(scope="session")
def block_c_auth_material() -> BlockCAuthMaterial:
    return load_block_c_auth_material()


@pytest.fixture
def tenant_a_token(block_c_auth_material: BlockCAuthMaterial) -> str:
    return mint_block_c_token(
        block_c_auth_material,
        subject=f"block-c-a-{uuid4().hex[:8]}",
        request_scope="tenant-a",
        roles=["operator"],
    )


@pytest.fixture
def tenant_b_token(block_c_auth_material: BlockCAuthMaterial) -> str:
    return mint_block_c_token(
        block_c_auth_material,
        subject=f"block-c-b-{uuid4().hex[:8]}",
        request_scope="tenant-b",
        roles=["operator"],
    )


@pytest.fixture
def tenant_a_client(block_c_endpoint: str, tenant_a_token: str) -> Iterator[LangGraphClient]:
    yield agent_server_client(block_c_endpoint, tenant_a_token)


@pytest.fixture
def tenant_b_client(block_c_endpoint: str, tenant_b_token: str) -> Iterator[LangGraphClient]:
    yield agent_server_client(block_c_endpoint, tenant_b_token)


@pytest.fixture
def tenant_a_client_n1(
    block_c_endpoint_n1: str,
    tenant_a_token: str,
) -> Iterator[LangGraphClient]:
    yield agent_server_client(block_c_endpoint_n1, tenant_a_token)


@pytest.fixture
async def qualification_assistant_id(tenant_a_client: LangGraphClient) -> str:
    return await lookup_assistant_id(tenant_a_client, GRAPH_ID_N)


@pytest.fixture
async def wait_assistant_id(tenant_a_client: LangGraphClient) -> str:
    return await lookup_assistant_id(tenant_a_client, GRAPH_ID_WAIT)


@pytest.fixture
async def n1_assistant_id(tenant_a_client: LangGraphClient) -> str:
    """N1 assistant on the N assembly (same-server qualification)."""

    return await lookup_assistant_id(tenant_a_client, GRAPH_ID_N1)


@pytest.fixture
async def n1_deployment_assistant_id(tenant_a_client_n1: LangGraphClient) -> str:
    """N1 assistant on the dedicated N+1 assembly (8134)."""

    return await lookup_assistant_id(tenant_a_client_n1, GRAPH_ID_N1)
