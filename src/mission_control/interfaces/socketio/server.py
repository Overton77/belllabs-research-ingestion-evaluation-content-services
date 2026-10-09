"""The `/missions` Socket.IO namespace: scoped subscriptions, replay, acks and commands.

Contract (SPEC-04 "Socket contract", ADR-0036):

- connect `auth={application_id, token}`; the token is verified exactly as the HTTP API
  verifies it (`interfaces.socketio.auth`). Rejections raise `ConnectionRefusedError`
  whose argument reaches the client (python-socketio server docs, "Connect and Disconnect
  Events"). Origins are checked by the server's `cors_allowed_origins`, independent of auth.
- client `subscribe`, `ack`, `unsubscribe`, `command`, `resolve_human_task`,
  `reauthenticate`; each one reauthorizes the stored credential first and is rate limited.
- server `subscribed` (the `SubscribeAck`, also returned as the event's ack), `snapshot`,
  `mission_event`, `provider_frame`, `presence`, `command_receipt`, `human_task_receipt`,
  `resync_required`, `stream_error` (a frozen `StreamError`; never exception text).

Room membership is never authorization: durable envelopes are emitted to the subscribing
`sid` by its own pump, which reads through the tenant-bound stream service. Rooms are used
only for best-effort presence and are derived from the server-resolved scope and mission,
never from a client-supplied name. The scope comes from the verified principal; a client
`application_id` that differs is `SCOPE_MISMATCH`.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Final
from uuid import uuid4

import socketio
from fastapi import FastAPI
from pydantic import BaseModel, ConfigDict, Field, ValidationError
from socketio.exceptions import ConnectionRefusedError as SocketConnectionRefused

from mission_control.application.streams.flow import ConnectionBudget, InflightWindow, TokenBucket
from mission_control.application.streams.hub import HintKey, StreamWakeups, target_keys
from mission_control.application.streams.pump import PumpSettings, SubscriptionPump
from mission_control.application.streams.service import (
    MissionStreamService,
    OpenedStream,
    SnapshotNotice,
    StreamFailure,
)
from mission_control.domain.subscriptions.streams import (
    StreamCursor,
    StreamEnvelope,
    StreamError,
    StreamFilters,
    StreamName,
    StreamSubscription,
    StreamTarget,
)
from mission_control.interfaces.socketio.auth import (
    PrincipalResolver,
    SocketAuthRejected,
    SocketCredential,
    authenticate,
    token_expiry,
)
from mission_control.interfaces.socketio.commands import (
    forward_command,
    forward_human_task_resolution,
    mission_service,
    parse_command,
    parse_human_task_resolution,
)

logger = logging.getLogger(__name__)

STREAM_SERVICES: Final = "mission_control_stream_services"
ENVELOPE_EVENTS: Final[dict[StreamName, str]] = {
    "mission_events": "mission_event",
    "provider_frames": "provider_frame",
    "presence": "presence",
}


@dataclass(frozen=True)
class MissionSocketLimits:
    max_subscriptions: int = 16
    max_connection_queue_bytes: int = 1_048_576
    min_delta_coalesce_ms: int = 250
    messages_per_second: float = 20.0
    message_burst: int = 40
    pump: PumpSettings = field(default_factory=PumpSettings)

    def __post_init__(self) -> None:
        if not 1 <= self.max_subscriptions <= 256:
            raise ValueError("max subscriptions per connection must be between 1 and 256")
        if not 65_536 <= self.max_connection_queue_bytes <= 67_108_864:
            raise ValueError("connection queue bound must be between 64 KiB and 64 MiB")


class SubscribeRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    request_id: str = Field(min_length=1, max_length=512)
    application_id: str = Field(min_length=1, max_length=63)
    target: StreamTarget
    streams: tuple[StreamName, ...] = Field(min_length=1, max_length=3)
    filters: StreamFilters = Field(default_factory=StreamFilters)
    cursors: tuple[StreamCursor, ...] = Field(default=(), max_length=3)
    include_descendants: bool = False
    max_queue_bytes: int | None = Field(default=None, ge=65_536, le=67_108_864)
    delta_coalesce_ms: int | None = Field(default=None, ge=0, le=10_000)


class AckRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    subscription_id: str = Field(min_length=1, max_length=512)
    cursors: tuple[StreamCursor, ...] = Field(min_length=1, max_length=3)


class UnsubscribeRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    subscription_id: str = Field(min_length=1, max_length=512)


class ReauthenticateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    token: str = Field(min_length=1, max_length=16_384)


class CredentialRevoked(StreamFailure):
    """The stored credential no longer verifies: the connection is closed."""

    def __init__(self, detail: str) -> None:
        super().__init__("UNAUTHORIZED", detail, retryable=True)


@dataclass
class _Subscription:
    pump: SubscriptionPump
    keys: tuple[HintKey, ...]
    presence_room: str | None
    task: asyncio.Task[str] | None = None


@dataclass
class _Connection:
    sid: str
    application_id: str
    credential: SocketCredential
    token: str
    budget: ConnectionBudget
    bucket: TokenBucket
    subscriptions: dict[str, _Subscription] = field(default_factory=dict)


class _SocketDelivery:
    def __init__(self, namespace: MissionNamespace, sid: str) -> None:
        self._namespace = namespace
        self._sid = sid

    async def envelope(self, envelope: StreamEnvelope) -> None:
        await self._namespace.emit(
            ENVELOPE_EVENTS[envelope.stream], envelope.model_dump(mode="json"), to=self._sid
        )

    async def resync_required(self, notice: dict[str, Any]) -> None:
        await self._namespace.emit("resync_required", notice, to=self._sid)

    async def error(self, error: StreamError) -> None:
        await self._namespace.emit("stream_error", error.model_dump(mode="json"), to=self._sid)


def presence_room(request_scope: str, mission_ref: str) -> str:
    digest = hashlib.sha256(f"{request_scope}|{mission_ref}".encode()).hexdigest()
    return f"mc-presence:{digest[:40]}"


Handler = Callable[[_Connection, Any], Awaitable[dict[str, Any]]]


class MissionNamespace(socketio.AsyncNamespace):
    def __init__(
        self,
        app: FastAPI,
        *,
        namespace: str,
        resolver: PrincipalResolver,
        limits: MissionSocketLimits,
        wakeups: StreamWakeups,
    ) -> None:
        super().__init__(namespace)
        self._app = app
        self._resolver = resolver
        self._limits = limits
        self._wakeups = wakeups
        self._connections: dict[str, _Connection] = {}
        self._closing = False
        self._presence_seq = 0

    # --- lifecycle -------------------------------------------------------------------

    @property
    def connections(self) -> int:
        return len(self._connections)

    async def close(self) -> None:
        """Detach every subscription and disconnect every client (API shutdown)."""

        self._closing = True
        for sid in list(self._connections):
            connection = self._connections.pop(sid, None)
            if connection is not None:
                await self._detach_all(connection)
                await self.disconnect(sid)

    # --- connect / disconnect --------------------------------------------------------

    async def on_connect(self, sid: str, environ: dict[str, Any], auth: Any = None) -> None:
        if self._closing:
            raise SocketConnectionRefused(_refusal("server is shutting down"))
        try:
            credential, token = authenticate(self._resolver, auth)
        except SocketAuthRejected as error:
            raise SocketConnectionRefused(_refusal(error.code)) from None
        if credential.expired():
            raise SocketConnectionRefused(_refusal("invalid_token"))
        self._connections[sid] = _Connection(
            sid=sid,
            application_id=credential.principal.application_id,
            credential=credential,
            token=token,
            budget=ConnectionBudget(self._limits.max_connection_queue_bytes),
            bucket=TokenBucket(self._limits.messages_per_second, self._limits.message_burst),
        )

    async def on_disconnect(self, sid: str, reason: Any = None) -> None:
        connection = self._connections.pop(sid, None)
        if connection is not None:
            await self._detach_all(connection)

    # --- client events ---------------------------------------------------------------

    async def on_subscribe(self, sid: str, data: Any = None) -> dict[str, Any]:
        return await self._handle(sid, data, self._subscribe)

    async def on_ack(self, sid: str, data: Any = None) -> dict[str, Any]:
        return await self._handle(sid, data, self._ack)

    async def on_unsubscribe(self, sid: str, data: Any = None) -> dict[str, Any]:
        return await self._handle(sid, data, self._unsubscribe)

    async def on_command(self, sid: str, data: Any = None) -> dict[str, Any]:
        return await self._handle(sid, data, self._command)

    async def on_resolve_human_task(self, sid: str, data: Any = None) -> dict[str, Any]:
        return await self._handle(sid, data, self._resolve_human_task)

    async def on_reauthenticate(self, sid: str, data: Any = None) -> dict[str, Any]:
        return await self._handle(sid, data, self._reauthenticate, reauthorize=False)

    async def _handle(
        self, sid: str, data: Any, handler: Handler, *, reauthorize: bool = True
    ) -> dict[str, Any]:
        connection = self._connections.get(sid)
        request_id = _field(data, "request_id")
        subscription_id = _field(data, "subscription_id")
        if connection is None:
            return _error_body(StreamFailure("UNAUTHORIZED", "not connected"), request_id)
        try:
            if not connection.bucket.take():
                raise StreamFailure("RATE_LIMITED", "too many messages", retryable=True)
            if reauthorize:
                self._reauthorize(connection)
            return await handler(connection, data)
        except StreamFailure as failure:
            error = failure.error(request_id=request_id, subscription_id=subscription_id)
            await self.emit("stream_error", error.model_dump(mode="json"), to=sid)
            if isinstance(failure, CredentialRevoked):
                self._connections.pop(sid, None)
                await self._detach_all(connection)
                await self.disconnect(sid)
            return {"ok": False, "error": error.model_dump(mode="json")}
        except Exception:
            logger.exception("mission socket handler failed")
            unavailable = StreamFailure("UNAVAILABLE", "temporarily unavailable", retryable=True)
            error = unavailable.error(request_id=request_id, subscription_id=subscription_id)
            await self.emit("stream_error", error.model_dump(mode="json"), to=sid)
            return {"ok": False, "error": error.model_dump(mode="json")}

    def _reauthorize(self, connection: _Connection) -> None:
        try:
            principal = self._resolver(connection.application_id, connection.token)
        except SocketAuthRejected as error:
            raise CredentialRevoked(error.code) from None
        fresh = SocketCredential(
            principal=principal,
            request_scope=connection.credential.request_scope,
            expires_at=connection.credential.expires_at,
        )
        if not fresh.same_identity(connection.credential) or fresh.expired():
            raise CredentialRevoked("credential identity changed")
        connection.credential = fresh

    async def _subscribe(self, connection: _Connection, data: Any) -> dict[str, Any]:
        try:
            request = SubscribeRequest.model_validate(data)
        except ValidationError:
            raise StreamFailure("UNSUPPORTED_FILTER", "invalid subscribe request") from None
        principal = connection.credential.principal
        if request.application_id != principal.application_id:
            raise StreamFailure("SCOPE_MISMATCH", "application is not the authenticated one")
        if len(connection.subscriptions) >= self._limits.max_subscriptions:
            raise StreamFailure("RATE_LIMITED", "too many subscriptions on this connection")
        service = self._stream_service(connection)
        queue_bytes = min(
            request.max_queue_bytes or self._limits.max_connection_queue_bytes,
            self._limits.max_connection_queue_bytes,
        )
        coalesce = max(
            request.delta_coalesce_ms or self._limits.min_delta_coalesce_ms,
            self._limits.min_delta_coalesce_ms,
        )
        try:
            subscription = StreamSubscription(
                subscription_id=f"sub_{uuid4().hex}",
                request_id=request.request_id,
                scope=service.scope,
                target=request.target,
                streams=request.streams,
                filters=request.filters,
                cursors=request.cursors,
                include_descendants=request.include_descendants,
                max_queue_bytes=queue_bytes,
                delta_coalesce_ms=coalesce,
            )
        except ValidationError:
            raise StreamFailure("UNSUPPORTED_FILTER", "incoherent subscription") from None
        opened = await service.open(subscription, principal.actor)
        delivery = _SocketDelivery(self, connection.sid)
        pump = SubscriptionPump(
            service,
            opened,
            delivery,
            InflightWindow(subscription.max_queue_bytes, connection.budget),
            self._limits.pump,
            authorized=lambda: not connection.credential.expired(),
        )
        mission_ref = f"mission:{opened.target.mission_id}"
        room = (
            presence_room(service.request_scope, mission_ref)
            if "presence" in subscription.streams
            else None
        )
        entry = _Subscription(pump, target_keys(service.request_scope, opened.target), room)
        connection.subscriptions[subscription.subscription_id] = entry
        ack = opened.ack.model_dump(mode="json")
        await self.emit("subscribed", {"request_id": request.request_id, **ack}, to=connection.sid)
        for notice in opened.snapshots:
            await self.emit(
                "snapshot",
                await self._snapshot(connection, service, opened, notice),
                to=connection.sid,
            )
        if room is not None:
            await self.enter_room(connection.sid, room)
            await self._presence(service, mission_ref, room, "presence.joined", connection)
        self._wakeups.register(entry.keys, pump.wake)
        entry.task = asyncio.create_task(self._run(connection, entry))
        return {"ok": True, "request_id": request.request_id, **ack}

    async def _snapshot(
        self,
        connection: _Connection,
        service: MissionStreamService,
        opened: OpenedStream,
        notice: SnapshotNotice,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {
            "subscription_id": opened.subscription.subscription_id,
            "stream": notice.stream,
            "reason": notice.reason,
            "covered": notice.covered.model_dump(mode="json"),
            "snapshot_ref": opened.ack.snapshot_ref,
            "projection": None,
        }
        run_key = opened.target.run_key
        if notice.stream == "mission_events" and run_key is not None:
            # The projection is read after the high-watermark, so it covers at least
            # `covered`; later events also arrive live and are idempotent by seq.
            try:
                inspection = await mission_service(
                    self._app, connection.credential.principal
                ).inspect(run_key, connection.credential.principal.actor)
            except Exception:
                logger.info("snapshot projection unavailable", exc_info=True)
            else:
                body["projection"] = inspection.model_dump(mode="json")
        return body

    async def _ack(self, connection: _Connection, data: Any) -> dict[str, Any]:
        try:
            request = AckRequest.model_validate(data)
        except ValidationError:
            raise StreamFailure("CURSOR_EXPIRED", "invalid ack") from None
        entry = connection.subscriptions.get(request.subscription_id)
        if entry is None:
            raise StreamFailure("TARGET_NOT_FOUND", "subscription not found")
        acked = entry.pump.acknowledge(request.cursors)
        return {"ok": True, "subscription_id": request.subscription_id, "acked": acked}

    async def _unsubscribe(self, connection: _Connection, data: Any) -> dict[str, Any]:
        try:
            request = UnsubscribeRequest.model_validate(data)
        except ValidationError:
            raise StreamFailure("TARGET_NOT_FOUND", "invalid unsubscribe") from None
        entry = connection.subscriptions.get(request.subscription_id)
        if entry is None:
            raise StreamFailure("TARGET_NOT_FOUND", "subscription not found")
        await self._detach(connection, request.subscription_id)
        return {"ok": True, "subscription_id": request.subscription_id, "unsubscribed": True}

    async def _command(self, connection: _Connection, data: Any) -> dict[str, Any]:
        command = parse_command(data)
        receipt = await forward_command(self._app, connection.credential.principal, command)
        await self.emit("command_receipt", receipt, to=connection.sid)
        return {"ok": True, **receipt}

    async def _resolve_human_task(self, connection: _Connection, data: Any) -> dict[str, Any]:
        # MP-10: the HTTP resolution body plus `human_task_id`, through the tenant's one
        # `HumanTaskService` (reviewer authorization lives there, never in a room).
        resolution = parse_human_task_resolution(data)
        receipt = await forward_human_task_resolution(
            self._app, connection.credential.principal, resolution
        )
        await self.emit("human_task_receipt", receipt, to=connection.sid)
        return {"ok": True, **receipt}

    async def _reauthenticate(self, connection: _Connection, data: Any) -> dict[str, Any]:
        try:
            request = ReauthenticateRequest.model_validate(data)
            principal = self._resolver(connection.application_id, request.token)
        except (ValidationError, SocketAuthRejected):
            raise CredentialRevoked("renewal rejected") from None
        fresh = SocketCredential(
            principal=principal,
            request_scope=connection.credential.request_scope,
            expires_at=token_expiry(request.token),
        )
        if not fresh.same_identity(connection.credential) or fresh.expired():
            raise CredentialRevoked("renewal names another identity")
        connection.credential = fresh
        connection.token = request.token
        return {"ok": True, "expires_at": fresh.expires_at}

    # --- subscriptions ---------------------------------------------------------------

    def _stream_service(self, connection: _Connection) -> MissionStreamService:
        principal = connection.credential.principal
        key = (principal.installation_id, principal.application_id, principal.tenant_id)
        service = getattr(self._app.state, STREAM_SERVICES, {}).get(key)
        if not isinstance(service, MissionStreamService):
            raise StreamFailure("TARGET_NOT_FOUND", "streams unavailable", retryable=True)
        if service.request_scope != connection.credential.request_scope:
            raise StreamFailure("SCOPE_MISMATCH", "stream service scope differs")
        return service

    async def _run(self, connection: _Connection, entry: _Subscription) -> str:
        reason = "unsubscribed"
        try:
            reason = await entry.pump.run()
        finally:
            subscription_id = entry.pump.subscription_id
            if connection.subscriptions.get(subscription_id) is entry:
                connection.subscriptions.pop(subscription_id, None)
                await self._release(connection, entry)
        if reason == "UNAUTHORIZED" and self._connections.get(connection.sid) is connection:
            self._connections.pop(connection.sid, None)
            await self._detach_all(connection)
            await self.disconnect(connection.sid)
        return reason

    async def _detach(self, connection: _Connection, subscription_id: str) -> None:
        entry = connection.subscriptions.pop(subscription_id, None)
        if entry is None:
            return
        task = entry.task
        if task is not None and task is not asyncio.current_task():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await self._release(connection, entry)

    async def _detach_all(self, connection: _Connection) -> None:
        for subscription_id in list(connection.subscriptions):
            await self._detach(connection, subscription_id)

    async def _release(self, connection: _Connection, entry: _Subscription) -> None:
        self._wakeups.unregister(entry.keys, entry.pump.wake)
        room = entry.presence_room
        if room is None:
            return
        if any(other.presence_room == room for other in connection.subscriptions.values()):
            return
        await self.leave_room(connection.sid, room)
        service = getattr(self._app.state, STREAM_SERVICES, {}).get(
            (
                connection.credential.principal.installation_id,
                connection.credential.principal.application_id,
                connection.credential.principal.tenant_id,
            )
        )
        if isinstance(service, MissionStreamService):
            mission_ref = f"mission:{entry.pump.opened.target.mission_id}"
            await self._presence(service, mission_ref, room, "presence.left", connection)

    async def _presence(
        self,
        service: MissionStreamService,
        mission_ref: str,
        room: str,
        kind: str,
        connection: _Connection,
    ) -> None:
        self._presence_seq += 1
        now = datetime.now(UTC).isoformat()
        envelope = StreamEnvelope(
            stream="presence",
            event_id=uuid4().hex,
            scope=service.scope,
            mission_ref=mission_ref,
            cursor=StreamCursor(stream="presence", position=str(self._presence_seq)),
            occurred_at=now,
            recorded_at=now,
            kind=kind,
            payload={"actor_ref": connection.credential.principal.actor.actor_id},
        )
        await self.emit("presence", envelope.model_dump(mode="json"), room=room)


def _refusal(detail: str) -> dict[str, Any]:
    error = StreamError(code="UNAUTHORIZED", retryable=True, detail=detail[:512])
    return error.model_dump(mode="json")


def _field(data: Any, name: str) -> str | None:
    value = data.get(name) if isinstance(data, dict) else None
    return value if isinstance(value, str) and 1 <= len(value) <= 512 else None


def _error_body(failure: StreamFailure, request_id: str | None) -> dict[str, Any]:
    return {"ok": False, "error": failure.error(request_id=request_id).model_dump(mode="json")}


__all__ = [
    "ENVELOPE_EVENTS",
    "STREAM_SERVICES",
    "MissionNamespace",
    "MissionSocketLimits",
    "SubscribeRequest",
    "presence_room",
]
