"""One live Claude Agent SDK session: the single stream owner and its frame log (MP-07).

SPEC-01 "Claude local": use the Python SDK's interactive client, capture the session identity
and *one* stream owner. `LiveSession` wraps one `ClaudeSDKClient` (behind the `ClaudeClient`
protocol so a fixture client can stand in) and is the only reader of
`receive_messages()`. Every message is mapped to frames (`frames.py`) and appended to an
ordered session log whose ordinal is the lane's resumable cursor within this process; turns
are records over that log (start index, terminal index), so `observe(turn, after)` replays
exactly the frames of one turn and never the leftover frames of an interrupted one.

SDK facts this module relies on (claude_agent_sdk 0.2.165):

- `ClaudeSDKClient.connect()` without a prompt keeps the connection open for streaming input;
  `query(AsyncIterable)` writes each message dict as-is (adding `session_id`), so the lane
  stamps its own `uuid` on the user message: the CLI honours a client-supplied `uuid` on a
  streamed user message (`types.ClaudeAgentOptions.resume_drops_turn` docstring) and that
  uuid is the native turn reference of this lane (`describe.identity.turn_ref`).
- `receive_messages()` yields the whole stream across turns; a turn ends at its
  `ResultMessage`. `ResultMessage.origin` tells a turn this lane submitted (`None` or kind
  `human`) from one the session injected (`types.MessageOrigin`); only the former closes
  the lane's turn.
- `interrupt()` sends the `interrupt` control request; the interrupted turn still ends with
  its own `ResultMessage` (`terminal_reason` `aborted_streaming` / `aborted_tools`), which the
  lane drains before a replacement turn may be sent.
- `RateLimitEvent` (`types.RateLimitInfo`) is the provider's capacity signal; a `rejected`
  status with a future reset refuses the next send with `ProviderCapacityLimited` before
  anything is written (MP-05 classifier `classify_claude_rate_limit_info`).
- When the subprocess dies the reader raises (`ProcessError` / `ResultError`, `_errors.py`);
  a turn still running gets a synthesized terminal frame that says so.
"""

from __future__ import annotations

import asyncio
import dataclasses
import logging
from collections.abc import AsyncIterable, AsyncIterator, Callable, Mapping
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Final, Literal, Protocol

from claude_agent_sdk.types import (
    AssistantMessage,
    ClaudeAgentOptions,
    Message,
    RateLimitEvent,
    ResultMessage,
    SystemMessage,
)

from mission_control.adapters.claude.frames import (
    RESULT_RAW_KIND,
    lane_frame,
    message_payload,
    native_status,
    observations,
    session_id_of,
    synthesized_result,
)
from mission_control.adapters.provider_auth.limits import (
    classify_claude_assistant_error,
    classify_claude_rate_limit_info,
)
from mission_control.application.execution.harness.dispatch import ProviderCapacityLimited
from mission_control.application.execution.harness.protocol import NativeTurnLost
from mission_control.application.frames.kinds import UnknownKindCounter, bounded_key, classify
from mission_control.domain.execution.lanes import LaneFrame, LaneProfileName
from mission_control.domain.execution.usage_admission import ProviderLimitSignal
from mission_control.domain.frames.contracts import FrameObservation, LaneProfile

_LOGGER = logging.getLogger(__name__)
TurnStatus = Literal["running", "finished", "cancelled", "error", "lost"]
SendOutcome = Literal["accepted", "busy"]
DEFAULT_INIT_TIMEOUT_S: Final = 60.0
DEFAULT_DRAIN_TIMEOUT_S: Final = 20.0


class ClaudeClient(Protocol):
    """The subset of `claude_agent_sdk.client.ClaudeSDKClient` this lane drives."""

    async def connect(self, prompt: str | AsyncIterable[dict[str, Any]] | None = None) -> None: ...

    def receive_messages(self) -> AsyncIterator[Message]: ...

    async def query(
        self, prompt: str | AsyncIterable[dict[str, Any]], session_id: str = "default"
    ) -> None: ...

    async def interrupt(self) -> None: ...

    async def get_context_usage(self) -> Mapping[str, Any]: ...

    async def disconnect(self) -> None: ...


class ClaudeClientFactory(Protocol):
    """Builds the client of one session over an explicit child environment (the SDK's own
    transport merges `os.environ`; `transport.py` replaces it so credentials the admission
    unset never reach the CLI)."""

    @property
    def versions(self) -> Mapping[str, str]: ...

    def create(
        self, options: ClaudeAgentOptions, *, environment: Mapping[str, str]
    ) -> ClaudeClient: ...


class DispatchLedgerPort(Protocol):
    """The lane's own send journal under the leased state root (`workspace.DispatchLedger`)."""

    async def intend(self, kind: str, idempotency_key: str) -> None: ...

    async def acknowledge(self, kind: str, idempotency_key: str, native_ref: str) -> None: ...


class SessionEnded(NativeTurnLost):
    """The subprocess ended: no turn can be sent or observed on this connection."""


@dataclass
class TurnRecord:
    turn_ref: str
    idempotency_key: str
    start_index: int
    terminal_index: int | None = None
    status: TurnStatus = "running"
    result: dict[str, Any] | None = None
    sent_at: datetime | None = None

    @property
    def active(self) -> bool:
        return self.status == "running"


@dataclass
class SessionState:
    """Secret-free facts about the session, read by `status` and `usage`."""

    session_id: str | None = None
    init: dict[str, Any] = field(default_factory=dict)
    last_model: str | None = None
    last_result: dict[str, Any] | None = None
    limit: ProviderLimitSignal | None = None
    ended: bool = False
    failure: str | None = None


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _closes_lane_turn(message: ResultMessage) -> bool:
    origin = message.origin
    return origin is None or origin.get("kind") == "human"


async def _one_message(text: str, turn_ref: str) -> AsyncIterator[dict[str, Any]]:
    yield {
        "type": "user",
        "message": {"role": "user", "content": text},
        "parent_tool_use_id": None,
        "session_id": "default",
        "uuid": turn_ref,
    }


class LiveSession:
    """The single stream owner of one SDK session and its frame log."""

    def __init__(
        self,
        factory: ClaudeClientFactory,
        options: ClaudeAgentOptions,
        environment: Mapping[str, str],
        *,
        harness_execution_id: str,
        generation: int,
        lane_profile: LaneProfileName = "claude_agent_sdk",
        ledger: DispatchLedgerPort | None = None,
        clock: Callable[[], datetime] = _utc_now,
        init_timeout_s: float = DEFAULT_INIT_TIMEOUT_S,
        drain_timeout_s: float = DEFAULT_DRAIN_TIMEOUT_S,
        counter: UnknownKindCounter | None = None,
    ) -> None:
        self._factory = factory
        self._options = options
        self._environment = dict(environment)
        self._heid = harness_execution_id
        self._generation = generation
        self._lane_profile = lane_profile
        self._ledger = ledger
        self._clock = clock
        self._init_timeout = init_timeout_s
        self._drain_timeout = drain_timeout_s
        self._counter = counter
        self._client: ClaudeClient | None = None
        self._reader: asyncio.Task[None] | None = None
        self._changed = asyncio.Condition()
        self._emitted = 0
        self.log: list[LaneFrame] = []
        self.turns: dict[str, TurnRecord] = {}
        self.active: TurnRecord | None = None
        self.state = SessionState()
        self.sent: list[tuple[str, str]] = []

    # --- lifecycle ------------------------------------------------------------------------

    @property
    def options(self) -> ClaudeAgentOptions:
        return self._options

    def configure(self, options: ClaudeAgentOptions) -> None:
        """Replace the options before `start` (hook and permission callbacks emit frames into
        this session's log, so they are built after the session exists)."""

        if self._client is not None:
            raise RuntimeError("a started session cannot be reconfigured")
        self._options = options

    @property
    def environment(self) -> Mapping[str, str]:
        return dict(self._environment)

    @property
    def session_id(self) -> str | None:
        return self.state.session_id

    @property
    def ended(self) -> bool:
        return self.state.ended

    async def start(self) -> str:
        """Connect, start the single reader and wait for the `system/init` session identity."""

        self._client = self._factory.create(self._options, environment=self._environment)
        await self._client.connect()
        self._reader = asyncio.create_task(self._pump(self._client))
        try:
            async with asyncio.timeout(self._init_timeout):
                async with self._changed:
                    await self._changed.wait_for(
                        lambda: self.state.session_id is not None or self.state.ended
                    )
        except TimeoutError as error:
            await self.close()
            raise SessionEnded("the Claude session reported no init within the timeout") from error
        try:
            self._raise_if_refused_at_start()
        except ProviderCapacityLimited:
            await self.close()
            raise
        if self.state.session_id is None:
            await self.close()
            raise SessionEnded(f"the Claude session ended before init: {self.state.failure}")
        return self.state.session_id

    def _raise_if_refused_at_start(self) -> None:
        limit = self.state.limit
        if limit is not None and limit.kind in {"rate_limited", "quota_exhausted"}:
            raise ProviderCapacityLimited(limit)

    async def close(self) -> None:
        reader, self._reader = self._reader, None
        if reader is not None and not reader.done():
            reader.cancel()
            with suppress(asyncio.CancelledError, Exception):
                await reader
        client, self._client = self._client, None
        if client is not None:
            try:
                await client.disconnect()
            except Exception:
                _LOGGER.warning("the Claude client did not disconnect cleanly", exc_info=True)
        self.state.ended = True
        async with self._changed:
            self._changed.notify_all()

    # --- the reader -----------------------------------------------------------------------

    async def _pump(self, client: ClaudeClient) -> None:
        try:
            async for message in client.receive_messages():
                await self._ingest(message)
        except asyncio.CancelledError:
            raise
        except Exception as error:  # the subprocess died or the stream broke
            self.state.failure = f"{type(error).__name__}: {error}"[:1_024]
            _LOGGER.warning("the Claude session stream ended with %s", self.state.failure)
        finally:
            await self._stream_ended()

    async def _stream_ended(self) -> None:
        async with self._changed:
            active = self.active
            if active is not None:
                self._append_locked(
                    synthesized_result(
                        session_id=self.state.session_id,
                        turn_ref=active.turn_ref,
                        reason="process_exit",
                        detail=self.state.failure or "the Claude subprocess ended without a result",
                    ),
                    active.turn_ref,
                    closes=active,
                )
            # Marked ended only once the synthesized terminal frames are in the log, so an
            # observer never sees "ended" with the turn still open.
            self.state.ended = True
            self._changed.notify_all()

    async def _ingest(self, message: Message) -> None:
        if self.state.session_id is None:
            session_id = session_id_of(message)
            if session_id is not None:
                self.state.session_id = session_id
        if isinstance(message, SystemMessage) and message.subtype == "init":
            self.state.init = dict(message.data)
        if isinstance(message, RateLimitEvent):
            self.state.limit = classify_claude_rate_limit_info(
                self._lane_profile, dataclasses.asdict(message.rate_limit_info)
            )
        if isinstance(message, AssistantMessage):
            self.state.last_model = message.model or self.state.last_model
            if message.error is not None:
                signal = classify_claude_assistant_error(self._lane_profile, message.error)
                if signal is not None:
                    self.state.limit = signal
        active = self.active
        closes = (
            active
            if isinstance(message, ResultMessage)
            and active is not None
            and _closes_lane_turn(message)
            else None
        )
        await self._append(
            observations(message, counter=self._counter),
            active.turn_ref if active is not None else None,
            closes=closes,
        )
        if isinstance(message, ResultMessage) and closes is not None:
            self.state.last_result = message_payload(message)

    async def _append(
        self,
        items: tuple[FrameObservation, ...],
        turn_ref: str | None,
        *,
        closes: TurnRecord | None = None,
    ) -> None:
        async with self._changed:
            self._append_locked(items, turn_ref, closes=closes)
            self._changed.notify_all()

    def _append_locked(
        self,
        items: tuple[FrameObservation, ...],
        turn_ref: str | None,
        *,
        closes: TurnRecord | None,
    ) -> None:
        for observation in items:
            ordinal = len(self.log) + 1
            frame = lane_frame(
                observation,
                harness_execution_id=self._heid,
                generation=self._generation,
                ordinal=ordinal,
                turn_ref=turn_ref,
            )
            terminal = closes is not None and observation.raw_kind == RESULT_RAW_KIND
            frame = frame.model_copy(update={"terminal": terminal})
            self.log.append(frame)
            if terminal and closes is not None:
                closes.terminal_index = len(self.log) - 1
                body = observation.body if isinstance(observation.body, Mapping) else {}
                closes.result = dict(body)
                status = native_status(body)[0]
                closes.status = status if status in ("finished", "cancelled", "error") else "error"
                if self.active is closes:
                    self.active = None

    async def emit(
        self,
        raw_kind: str,
        body: Mapping[str, Any],
        *,
        tool_call_ref: str | None = None,
    ) -> LaneFrame:
        """A frame the lane itself observed (a Kernel Hook invocation, a permission request):
        keyed `claude:<session>:lane:<n>:<raw_kind>`, attributed to the active turn."""

        self._emitted += 1
        ref = self.state.session_id or self._heid
        observation = FrameObservation(
            provider_key=bounded_key("claude", ref, "lane", self._emitted, raw_kind),
            raw_kind=raw_kind,
            kind=classify(LaneProfile.CLAUDE_AGENT_SDK, raw_kind, body, counter=self._counter).kind,
            body=dict(body),
            native_session_ref=self.state.session_id,
            tool_call_ref=tool_call_ref,
        )
        active = self.active
        await self._append((observation,), active.turn_ref if active is not None else None)
        return self.log[-1]

    # --- turns ----------------------------------------------------------------------------

    def _check_capacity(self) -> None:
        limit = self.state.limit
        if limit is None or limit.kind not in {"rate_limited", "quota_exhausted"}:
            return
        if limit.resets_at is not None and limit.resets_at <= self._clock():
            return
        raise ProviderCapacityLimited(limit)

    async def send(self, text: str, *, turn_ref: str, idempotency_key: str) -> SendOutcome:
        """Write one user message as a new turn (`accepted`), or report `busy` while a turn
        (or its interrupted drain) is still running. Nothing is written when the provider's
        last capacity signal still rejects sends (`ProviderCapacityLimited`)."""

        if self._client is None or self.state.ended:
            raise SessionEnded("the Claude session is not connected")
        known = self.turns.get(turn_ref)
        if known is not None:
            return "accepted" if not known.active else "busy"
        if self.active is not None:
            return "busy"
        self._check_capacity()
        if self._ledger is not None:
            await self._ledger.intend("send", idempotency_key)
        record = TurnRecord(
            turn_ref=turn_ref,
            idempotency_key=idempotency_key,
            start_index=self._next_start_index(),
            sent_at=self._clock(),
        )
        self.turns[turn_ref] = record
        self.active = record
        try:
            await self._client.query(_one_message(text, turn_ref))
        except Exception:
            # The write may or may not have reached the CLI: the ledger keeps `intended`.
            self.active = None
            record.status = "lost"
            raise
        self.sent.append((idempotency_key, text))
        if self._ledger is not None:
            await self._ledger.acknowledge("send", idempotency_key, turn_ref)
        return "accepted"

    def _next_start_index(self) -> int:
        """Every logged frame belongs to exactly one turn's window: the first turn starts at
        the session's first frame (`system/init`), a later turn right after the terminal
        frame of the last closed turn."""

        closed = [record.terminal_index for record in self.turns.values()]
        ended = [index for index in closed if index is not None]
        return max(ended) + 1 if ended else 0

    def turn(self, turn_ref: str) -> TurnRecord | None:
        return self.turns.get(turn_ref)

    async def observe(self, turn_ref: str, after: int | None) -> AsyncIterator[LaneFrame]:
        """The frames of one turn from its start (or after the cursor), to its terminal frame."""

        record = self.turns.get(turn_ref)
        if record is None:
            raise NativeTurnLost(f"turn {turn_ref} is unknown to this session")
        index = record.start_index if after is None else max(record.start_index, after)
        while True:
            while index < len(self.log):
                frame = self.log[index]
                index += 1
                if record.terminal_index is not None and index - 1 > record.terminal_index:
                    return
                yield frame
                if frame.terminal and record.terminal_index == index - 1:
                    return
            if record.terminal_index is not None or self.state.ended:
                return
            await self._wait_past(record, index)

    async def _wait_past(self, record: TurnRecord, index: int) -> None:
        async with self._changed:
            await self._changed.wait_for(
                lambda: (
                    index < len(self.log) or record.terminal_index is not None or self.state.ended
                )
            )

    async def interrupt(self, turn_ref: str, *, drain: bool = True) -> tuple[bool, bool]:
        """`(already_terminal, drained)`: interrupt the turn and drain its response.

        The interrupted response (its remaining messages and the aborted `ResultMessage`) is
        read into the log before the method returns, so a replacement turn sent afterwards
        starts from a clean boundary (SPEC-01 "drain the interrupted response before
        consuming a replacement response"). `drained=False` means the bound passed first:
        the turn stays active and a later send reports `busy`.
        """

        record = self.turns.get(turn_ref)
        if record is None:
            raise NativeTurnLost(f"turn {turn_ref} is unknown to this session")
        if not record.active:
            return True, True
        if self._client is None or self.state.ended:
            return False, False
        await self._client.interrupt()
        if not drain:
            return False, False
        try:
            async with asyncio.timeout(self._drain_timeout):
                async with self._changed:
                    await self._changed.wait_for(lambda: not record.active or self.state.ended)
        except TimeoutError:
            return False, False
        return False, not record.active

    async def context_usage(self, *, timeout_s: float) -> Mapping[str, Any]:
        """`ClaudeSDKClient.get_context_usage()` (the `get_context_usage` control request,
        `_internal/query.py`): the live context window breakdown, bounded by `timeout_s`."""

        if self._client is None or self.state.ended:
            raise SessionEnded("the Claude session is not connected")
        async with asyncio.timeout(timeout_s):
            return dict(await self._client.get_context_usage())

    def status_of(self, turn_ref: str | None) -> tuple[str, bool, bool]:
        """`(status, terminal, idle)` of a turn or of the session."""

        if turn_ref is not None and turn_ref in self.turns:
            record = self.turns[turn_ref]
            if record.active:
                return "running", False, False
            return record.status, True, self.active is None and not self.state.ended
        if self.state.ended:
            return ("error" if self.state.failure else "ended"), True, False
        if self.active is not None:
            return "running", False, False
        return "idle", False, True


__all__ = [
    "DEFAULT_DRAIN_TIMEOUT_S",
    "DEFAULT_INIT_TIMEOUT_S",
    "ClaudeClient",
    "ClaudeClientFactory",
    "DispatchLedgerPort",
    "LiveSession",
    "SendOutcome",
    "SessionEnded",
    "SessionState",
    "TurnRecord",
    "TurnStatus",
]
