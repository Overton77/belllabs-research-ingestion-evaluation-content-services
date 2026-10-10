"""FIXTURE app-server: an in-process stand-in for `codex app-server` (MP-08 tests).

FIXTURE ONLY. Nothing here is Codex. It speaks the pinned app-server protocol v2 shapes
(`tests/fixtures/provider_frames/codex/schema/`) over an in-memory message channel so the
transport, the harness and `lane.turn` are exercised end to end without a CLI, a login or a
model turn. Scripts under `tests/fixtures/provider_frames/codex/scripts/*.jsonl` (first line
`"_fixture"`) describe what one turn emits; the server persists threads and turns to a shared
`Disk` so a relaunch (`FixtureLauncher`) can `thread/resume` and `thread/read` history the
way the real server loads a thread from disk.

Behaviours the tests switch on:

- `lose_response`: the server does the work but never answers that request (lost receipt);
- `capacity_refusals`: `turn/start` is refused with `codexErrorInfo: usageLimitExceeded`;
- `complete_before_steer`: the active turn completes right before a `turn/steer` arrives, so
  the steer is refused (`expectedTurnId` no longer active): deterministic stale target;
- a `turn/start` while a turn is active is recorded in `steer_by_turn_start` (the real
  server treats it as a steer; the lane must never cause it);
- a `disconnect` script step closes the channel mid-turn while the turn finishes "offline"
  (the fixture process keeps writing the disk the way a surviving Codex process would).
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from mission_control.adapters.codex.launcher import LaunchedAppServer, LaunchSpec
from mission_control.adapters.codex.transport import AppServerConnection, MessageChannel

FIXTURE_ROOT = Path(__file__).resolve().parents[2] / "fixtures" / "provider_frames" / "codex"
SCRIPTS = FIXTURE_ROOT / "scripts"
FIXTURE_VERSION = "codex-cli 0.162.0 (FIXTURE app-server, not Codex)"
_EOF = object()


def load_script(name: str) -> list[dict[str, Any]]:
    """The steps of one scripted turn (the `_fixture` header line is checked, not replayed)."""

    lines = (SCRIPTS / f"{name}.jsonl").read_text(encoding="utf-8").splitlines()
    rows = [json.loads(line) for line in lines if line.strip()]
    assert rows and rows[0].get("_fixture"), f"{name}: the first line labels the fixture"
    assert rows[0].get("recorded") is False, f"{name}: a fixture is never claimed recorded"
    return rows[1:]


class MemoryChannel:
    """One side of an in-process duplex channel (dicts in queues; `None` is EOF)."""

    def __init__(self, inbox: asyncio.Queue[Any], outbox: asyncio.Queue[Any]) -> None:
        self._inbox = inbox
        self._outbox = outbox
        self.closed = False
        self.sent: list[dict[str, Any]] = []

    async def send(self, message: Mapping[str, Any]) -> None:
        if self.closed:
            raise ConnectionError("channel closed")
        copy = json.loads(json.dumps(dict(message)))  # wire-faithful: JSON values only
        self.sent.append(copy)
        await self._outbox.put(copy)

    async def receive(self) -> dict[str, Any] | None:
        if self.closed:
            return None
        item = await self._inbox.get()
        if item is _EOF:
            self.closed = True
            return None
        return dict(item)

    async def close(self) -> None:
        if not self.closed:
            self.closed = True
            await self._outbox.put(_EOF)
            await self._inbox.put(_EOF)


def channel_pair() -> tuple[MemoryChannel, MemoryChannel]:
    a_to_b: asyncio.Queue[Any] = asyncio.Queue()
    b_to_a: asyncio.Queue[Any] = asyncio.Queue()
    return MemoryChannel(b_to_a, a_to_b), MemoryChannel(a_to_b, b_to_a)


@dataclass
class Disk:
    """FIXTURE: what the real server persists under CODEX_HOME (threads and their turns)."""

    threads: dict[str, dict[str, Any]] = field(default_factory=dict)
    counter: int = 0

    def next_id(self, prefix: str) -> str:
        self.counter += 1
        return f"{prefix}-{self.counter:04d}"


@dataclass
class FixtureAppServer:
    disk: Disk
    scripts: list[list[dict[str, Any]]]
    lose_response: set[str] = field(default_factory=set)
    capacity_refusals: int = 0
    complete_before_steer: bool = False
    records: list[tuple[str, dict[str, Any]]] = field(default_factory=list)
    steer_by_turn_start: int = 0
    steers: list[dict[str, Any]] = field(default_factory=list)
    interrupts: list[str] = field(default_factory=list)
    approvals: list[tuple[str, Any]] = field(default_factory=list)
    compactions: int = 0
    release: asyncio.Event = field(default_factory=asyncio.Event)
    held: asyncio.Event = field(default_factory=asyncio.Event)
    steer_received: asyncio.Event = field(default_factory=asyncio.Event)
    offline: bool = False
    active: dict[str, str] = field(default_factory=dict)  # thread -> active turn
    _channel: MessageChannel | None = None
    _next_server_id: int = 1
    _answers: dict[int, asyncio.Future[dict[str, Any]]] = field(default_factory=dict)
    _interrupted: dict[str, asyncio.Event] = field(default_factory=dict)
    _tasks: list[asyncio.Task[None]] = field(default_factory=list)

    # --- wire ---------------------------------------------------------------------------------

    async def serve(self, channel: MessageChannel) -> None:
        self._channel = channel
        try:
            while True:
                message = await channel.receive()
                if message is None:
                    return
                await self._handle(message)
        finally:
            self.offline = True

    async def _send(self, message: dict[str, Any]) -> None:
        if self.offline or self._channel is None:
            return
        with suppress(ConnectionError):
            await self._channel.send(message)

    async def notify(self, method: str, params: dict[str, Any]) -> None:
        await self._send({"jsonrpc": "2.0", "method": method, "params": params})

    async def _respond(self, request_id: Any, result: dict[str, Any]) -> None:
        await self._send({"jsonrpc": "2.0", "id": request_id, "result": result})

    async def _error(
        self, request_id: Any, code: int, message: str, data: dict[str, Any] | None = None
    ) -> None:
        error: dict[str, Any] = {"code": code, "message": message}
        if data is not None:
            error["data"] = data
        await self._send({"jsonrpc": "2.0", "id": request_id, "error": error})

    async def server_request(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
        """Send a server-originated request and wait for the client's answer."""

        request_id = self._next_server_id
        self._next_server_id += 1
        loop = asyncio.get_running_loop()
        future: asyncio.Future[dict[str, Any]] = loop.create_future()
        self._answers[request_id] = future
        await self._send({"jsonrpc": "2.0", "id": request_id, "method": method, "params": params})
        return await future

    async def _handle(self, message: dict[str, Any]) -> None:
        method = message.get("method")
        if method is None and "id" in message:
            future = self._answers.pop(message["id"], None)
            if future is not None and not future.done():
                if "error" in message:
                    future.set_result({"error": message["error"]})
                else:
                    future.set_result({"result": message.get("result")})
            return
        if "id" not in message:
            self.records.append((str(method), dict(message.get("params") or {})))
            return  # client notification (`initialized`)
        request_id = message["id"]
        params = dict(message.get("params") or {})
        self.records.append((str(method), params))
        lose = method in self.lose_response
        if lose:
            self.lose_response.discard(str(method))

        async def reply(result: dict[str, Any]) -> None:
            if not lose:
                await self._respond(request_id, result)

        if method == "initialize":
            await reply(
                {
                    "userAgent": FIXTURE_VERSION,
                    "platformOs": "fixture",
                    "platformFamily": "unix",
                    "codexHome": "/fixture/codex-home",
                }
            )
        elif method == "thread/start":
            thread = self._new_thread(params)
            await reply(
                {
                    "thread": self._thread_view(thread["id"], include_turns=False),
                    "model": params.get("model") or "fixture-model",
                    "approvalPolicy": params.get("approvalPolicy") or "on-request",
                    "approvalsReviewer": "user",
                    "cwd": params.get("cwd") or "/fixture",
                    "modelProvider": "openai",
                    "sandbox": {"type": "workspaceWrite"},
                }
            )
            await self.notify("thread/started", {"thread": self._thread_view(thread["id"], False)})
        elif method == "thread/resume":
            thread_id = str(params.get("threadId"))
            if thread_id not in self.disk.threads:
                await self._error(request_id, -32602, f"unknown thread {thread_id}")
                return
            await reply(
                {
                    "thread": self._thread_view(thread_id, include_turns=True),
                    "model": params.get("model") or "fixture-model",
                    "approvalPolicy": "on-request",
                    "approvalsReviewer": "user",
                    "cwd": params.get("cwd") or "/fixture",
                    "modelProvider": "openai",
                    "sandbox": {"type": "workspaceWrite"},
                }
            )
        elif method == "thread/read":
            thread_id = str(params.get("threadId"))
            if thread_id not in self.disk.threads:
                await self._error(request_id, -32602, f"unknown thread {thread_id}")
                return
            await reply({"thread": self._thread_view(thread_id, bool(params.get("includeTurns")))})
        elif method == "turn/start":
            thread_id = str(params.get("threadId"))
            if thread_id not in self.disk.threads:
                await self._error(request_id, -32602, f"unknown thread {thread_id}")
                return
            if thread_id in self.active:
                # The real server steers an active turn on `turn/start`; the lane must not.
                self.steer_by_turn_start += 1
                await reply({"turn": self._turn_view(thread_id, self.active[thread_id], True)})
                return
            if self.capacity_refusals > 0:
                self.capacity_refusals -= 1
                await self._error(
                    request_id,
                    -32000,
                    "usage limit reached",
                    {"codexErrorInfo": "usageLimitExceeded"},
                )
                return
            turn = self._new_turn(thread_id, params)
            await reply({"turn": self._turn_view(thread_id, turn["id"], True)})
            self._tasks.append(asyncio.create_task(self._run_turn(thread_id, turn["id"])))
        elif method == "turn/steer":
            thread_id = str(params.get("threadId"))
            expected = str(params.get("expectedTurnId"))
            if self.complete_before_steer and self.active.get(thread_id) == expected:
                await self._complete_turn(thread_id, expected, "completed")
            if self.active.get(thread_id) != expected:
                await self._error(request_id, -32602, "expectedTurnId is not the active turn")
                return
            self.steers.append(params)
            text = "".join(
                str(item.get("text", ""))
                for item in params.get("input", [])
                if isinstance(item, dict)
            )
            item = {
                "type": "userMessage",
                "id": self.disk.next_id("item"),
                "content": [{"type": "text", "text": text}],
                "clientId": params.get("clientUserMessageId"),
            }
            self._append_item(thread_id, expected, item)
            await self.notify(
                "item/completed",
                {
                    "threadId": thread_id,
                    "turnId": expected,
                    "item": item,
                    "completedAtMs": 1760000000000,
                },
            )
            self.steer_received.set()
            await reply({"turnId": expected})
        elif method == "turn/interrupt":
            thread_id, turn_id = str(params.get("threadId")), str(params.get("turnId"))
            self.interrupts.append(turn_id)
            if self.active.get(thread_id) != turn_id:
                await self._error(request_id, -32602, "turn is not active")
                return
            await reply({})
            self._interrupted.setdefault(turn_id, asyncio.Event()).set()
            self.release.set()
        elif method == "thread/compact/start":
            thread_id = str(params.get("threadId"))
            self.compactions += 1
            await reply({})
            turn_id = self.active.get(thread_id) or "compaction"
            item = {"type": "contextCompaction", "id": self.disk.next_id("item")}
            await self.notify(
                "item/started",
                {
                    "threadId": thread_id,
                    "turnId": turn_id,
                    "item": item,
                    "startedAtMs": 1760000000000,
                },
            )
            await self.notify(
                "item/completed",
                {
                    "threadId": thread_id,
                    "turnId": turn_id,
                    "item": item,
                    "completedAtMs": 1760000000001,
                },
            )
            await self.notify("thread/compacted", {"threadId": thread_id, "turnId": turn_id})
        else:
            await self._error(request_id, -32601, f"method not found: {method}")

    # --- disk and views ---------------------------------------------------------------------

    def _new_thread(self, params: dict[str, Any]) -> dict[str, Any]:
        thread_id = self.disk.next_id("thr")
        self.disk.threads[thread_id] = {
            "id": thread_id,
            "cwd": params.get("cwd") or "/fixture",
            "model": params.get("model") or "fixture-model",
            "turns": [],
        }
        return self.disk.threads[thread_id]

    def _new_turn(self, thread_id: str, params: dict[str, Any]) -> dict[str, Any]:
        turn_id = self.disk.next_id("turn")
        text = "".join(
            str(item.get("text", "")) for item in params.get("input", []) if isinstance(item, dict)
        )
        user = {
            "type": "userMessage",
            "id": self.disk.next_id("item"),
            "content": [{"type": "text", "text": text}],
            "clientId": params.get("clientUserMessageId"),
        }
        turn = {
            "id": turn_id,
            "status": "inProgress",
            "items": [user],
            "error": None,
            "startedAt": 1760000000,
        }
        self.disk.threads[thread_id]["turns"].append(turn)
        self.active[thread_id] = turn_id
        return turn

    def _turn_record(self, thread_id: str, turn_id: str) -> dict[str, Any]:
        for turn in self.disk.threads[thread_id]["turns"]:
            if turn["id"] == turn_id:
                return turn
        raise KeyError(turn_id)

    def _append_item(self, thread_id: str, turn_id: str, item: dict[str, Any]) -> None:
        turn = self._turn_record(thread_id, turn_id)
        existing = [index for index, old in enumerate(turn["items"]) if old["id"] == item["id"]]
        if existing:
            turn["items"][existing[0]] = item
        else:
            turn["items"].append(item)

    def _turn_view(self, thread_id: str, turn_id: str, full: bool) -> dict[str, Any]:
        turn = self._turn_record(thread_id, turn_id)
        return {
            "id": turn["id"],
            "status": turn["status"],
            "items": list(turn["items"]) if full else [],
            "error": turn.get("error"),
            "startedAt": turn.get("startedAt"),
            "completedAt": turn.get("completedAt"),
            "durationMs": turn.get("durationMs"),
        }

    def _thread_view(self, thread_id: str, include_turns: bool) -> dict[str, Any]:
        thread = self.disk.threads[thread_id]
        active = self.active.get(thread_id)
        status: dict[str, Any] = (
            {"type": "active", "activeFlags": []} if active else {"type": "idle"}
        )
        return {
            "id": thread_id,
            "cliVersion": FIXTURE_VERSION,
            "createdAt": 1760000000,
            "updatedAt": 1760000000,
            "cwd": thread["cwd"],
            "ephemeral": False,
            "model": thread["model"],
            "modelProvider": "openai",
            "preview": "",
            "projectId": None,
            "sessionId": f"sess-{thread_id}",
            "source": "appServer",
            "status": status,
            "turns": (
                [self._turn_view(thread_id, turn["id"], True) for turn in thread["turns"]]
                if include_turns
                else []
            ),
        }

    # --- the scripted turn --------------------------------------------------------------------

    async def _run_turn(self, thread_id: str, turn_id: str) -> None:
        script: list[dict[str, Any]] = (
            self.scripts.pop(0) if self.scripts else [{"step": "complete"}]
        )
        await self.notify(
            "turn/started",
            {"threadId": thread_id, "turn": self._turn_view(thread_id, turn_id, False)},
        )
        interrupted = self._interrupted.setdefault(turn_id, asyncio.Event())
        for step in script:
            if interrupted.is_set():
                break
            kind = step.get("step")
            if kind == "item":
                item = dict(step["item"])
                item.setdefault("id", self.disk.next_id("item"))
                started = {**item, "status": "inProgress"} if "status" in item else dict(item)
                await self.notify(
                    "item/started",
                    {
                        "threadId": thread_id,
                        "turnId": turn_id,
                        "item": started,
                        "startedAtMs": 1760000000000,
                    },
                )
                self._append_item(thread_id, turn_id, item)
                await self.notify(
                    "item/completed",
                    {
                        "threadId": thread_id,
                        "turnId": turn_id,
                        "item": item,
                        "completedAtMs": 1760000000001,
                    },
                )
            elif kind == "delta":
                await self.notify(
                    step.get("method", "item/agentMessage/delta"),
                    {
                        "threadId": thread_id,
                        "turnId": turn_id,
                        "itemId": step["item_id"],
                        "delta": step["delta"],
                    },
                )
            elif kind == "usage":
                usage = {
                    "inputTokens": step["input"],
                    "cachedInputTokens": 0,
                    "outputTokens": step["output"],
                    "reasoningOutputTokens": 0,
                    "totalTokens": step["input"] + step["output"],
                }
                await self.notify(
                    "thread/tokenUsage/updated",
                    {
                        "threadId": thread_id,
                        "turnId": turn_id,
                        "tokenUsage": {
                            "last": usage,
                            "total": usage,
                            "modelContextWindow": step.get("window", 272000),
                        },
                    },
                )
            elif kind == "notify":
                await self.notify(
                    step["method"],
                    {"threadId": thread_id, "turnId": turn_id, **dict(step.get("params") or {})},
                )
            elif kind == "approval":
                item_id = self.disk.next_id("item")
                params = {
                    "threadId": thread_id,
                    "turnId": turn_id,
                    "itemId": item_id,
                    "startedAtMs": 1760000000000,
                    **dict(step.get("params") or {}),
                }
                answer = await self.server_request(step["method"], params)
                self.approvals.append((step["method"], answer))
                await self.notify(
                    "serverRequest/resolved",
                    {"threadId": thread_id, "requestId": self._next_server_id - 1},
                )
                decision = (
                    (answer.get("result") or {}).get("decision") if "result" in answer else None
                )
                status = "completed" if decision in {"accept", "acceptForSession"} else "declined"
                item = {
                    "type": step.get("item_type", "commandExecution"),
                    "id": item_id,
                    "command": params.get("command", ""),
                    "cwd": "/fixture",
                    "commandActions": [],
                    "status": status,
                    "exitCode": 0 if status == "completed" else None,
                }
                self._append_item(thread_id, turn_id, item)
                await self.notify(
                    "item/completed",
                    {
                        "threadId": thread_id,
                        "turnId": turn_id,
                        "item": item,
                        "completedAtMs": 1760000000002,
                    },
                )
            elif kind == "hold":
                self.held.set()
                waiter = asyncio.create_task(self.release.wait())
                stop = asyncio.create_task(interrupted.wait())
                await asyncio.wait({waiter, stop}, return_when=asyncio.FIRST_COMPLETED)
                for task in (waiter, stop):
                    task.cancel()
            elif kind == "wait_steer":
                self.held.set()
                waiter = asyncio.create_task(self.steer_received.wait())
                stop = asyncio.create_task(interrupted.wait())
                await asyncio.wait({waiter, stop}, return_when=asyncio.FIRST_COMPLETED)
                for task in (waiter, stop):
                    task.cancel()
            elif kind == "disconnect":
                self.held.set()
                assert self._channel is not None
                await self._channel.close()
                self.offline = True
            elif kind == "fail":
                info = step.get("codexErrorInfo", "other")
                await self.notify(
                    "error",
                    {
                        "threadId": thread_id,
                        "turnId": turn_id,
                        "error": {
                            "message": step.get("message", "fixture failure"),
                            "codexErrorInfo": info,
                        },
                        "willRetry": False,
                    },
                )
                await self._complete_turn(
                    thread_id,
                    turn_id,
                    "failed",
                    error={
                        "message": step.get("message", "fixture failure"),
                        "codexErrorInfo": info,
                    },
                )
                return
            elif kind == "complete":
                await self._complete_turn(thread_id, turn_id, step.get("status", "completed"))
                return
        if interrupted.is_set():
            await self._complete_turn(thread_id, turn_id, "interrupted")
        elif self.active.get(thread_id) == turn_id:
            await self._complete_turn(thread_id, turn_id, "completed")

    async def _complete_turn(
        self, thread_id: str, turn_id: str, status: str, *, error: dict[str, Any] | None = None
    ) -> None:
        turn = self._turn_record(thread_id, turn_id)
        if turn["status"] in {"completed", "interrupted", "failed"}:
            return
        turn["status"] = status
        turn["error"] = error
        turn["completedAt"] = 1760000010
        turn["durationMs"] = 10_000
        if self.active.get(thread_id) == turn_id:
            del self.active[thread_id]
        await self.notify(
            "turn/completed",
            {"threadId": thread_id, "turn": self._turn_view(thread_id, turn_id, True)},
        )
        await self.notify(
            "thread/status/changed", {"threadId": thread_id, "status": {"type": "idle"}}
        )


@dataclass
class FixtureLaunch:
    server: FixtureAppServer
    client: MemoryChannel
    spec: LaunchSpec
    task: asyncio.Task[None]
    connection: AppServerConnection


@dataclass
class FixtureLauncher:
    """`AppServerLauncher` FIXTURE: each launch is a fresh fixture server over the shared disk."""

    scripts: list[Sequence[dict[str, Any]]] = field(default_factory=list)
    # Per launch index (0 = first app-server): the scripts that process runs instead.
    launch_scripts: dict[int, list[Sequence[dict[str, Any]]]] = field(default_factory=dict)
    disk: Disk = field(default_factory=Disk)
    lose_response: set[str] = field(default_factory=set)
    capacity_refusals: int = 0
    complete_before_steer: bool = False
    request_timeout_s: float = 2.0
    launches: list[FixtureLaunch] = field(default_factory=list)
    fail_launch: bool = False
    launched: asyncio.Event = field(default_factory=asyncio.Event)

    @property
    def versions(self) -> Mapping[str, str]:
        return {"codex_cli": "0.162.0", "fixture": "true"}

    @property
    def server(self) -> FixtureAppServer:
        return self.launches[-1].server

    async def launch(self, spec: LaunchSpec) -> LaunchedAppServer:
        if self.fail_launch:
            raise OSError("fixture launcher refused to start")
        client, remote = channel_pair()
        server = FixtureAppServer(
            disk=self.disk,
            scripts=[
                list(script) for script in self.launch_scripts.get(len(self.launches), self.scripts)
            ],
            lose_response=set(self.lose_response),
            capacity_refusals=self.capacity_refusals,
            complete_before_steer=self.complete_before_steer,
        )
        self.lose_response = set()
        self.capacity_refusals = 0
        task = asyncio.create_task(server.serve(remote), name="fixture-app-server")
        connection = AppServerConnection(client, request_timeout_s=self.request_timeout_s)
        await connection.open()
        self.launches.append(FixtureLaunch(server, client, spec, task, connection))
        self.launched.set()
        return LaunchedAppServer(connection=connection, versions=dict(self.versions))

    async def shutdown(self) -> None:
        for launch in self.launches:
            launch.task.cancel()
            with suppress(asyncio.CancelledError, Exception):
                await launch.task


__all__ = [
    "FIXTURE_ROOT",
    "FIXTURE_VERSION",
    "SCRIPTS",
    "Disk",
    "FixtureAppServer",
    "FixtureLaunch",
    "FixtureLauncher",
    "MemoryChannel",
    "channel_pair",
    "load_script",
]
