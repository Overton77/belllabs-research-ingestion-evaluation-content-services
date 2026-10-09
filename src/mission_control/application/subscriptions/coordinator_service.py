"""The durable coordinator inbox: subscribe, poll, acknowledge, callbacks and prompts (MP-15).

"Callback" means delivery to a registered transport endpoint or to the coordinator's own
inbox, never a function call inside a provider session (SPEC-04, ADR-0040):

- **Inbox (always).** Notifications are materialized from committed mission events by the
  pure planner (`coordinator.plan`) under the inbox row lock and kept until acknowledged. An
  offline coordinator polls after its acknowledged cursor and receives each pending
  notification once by id; acknowledging is idempotent and only moves the cursor forward.
- **Connected MCP session (qualified only while connected).** After new notifications seal,
  a connected session gets an `InboxHint`. A hint is a wake-up, not a delivery: it never
  acknowledges, and a disconnected client simply polls later.
- **Webhook.** Each sealed notification is POSTed once per attempt, signed with timestamp and
  stable delivery id (`webhook_signing`), in inbox order. A 2xx acknowledges through that
  notification; failures back off on the FT-F5 schedule and the twelfth (or an egress
  rejection) dead-letters the subscription. Retries re-send the stored notification; they
  never reach run control or agent work.
- **Prompting a coordinator** goes only through the admitted mailbox: a `queue_instruction`
  or `add_context` Command against the coordinator's own run, through the same
  `MissionControlService.command` path as HTTP (mailbox composition required), one request
  id per notification so a retry is a replay. Never a nested provider send.
- **Loops.** Commands admitted from a notification (prompts and `command_from_notification`)
  record their request id with depth = notification depth + 1; events those commands cause
  inherit the depth, and above `max_recursion_depth` neither notifications nor commands are
  admitted. Prompts and triggered commands are also capped per window, and notifications per
  run by the planner's rate cap.
"""

from __future__ import annotations

import random
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Final, Literal
from uuid import UUID, uuid4

from pydantic import Field, model_validator

from mission_control.application.execution.operations.operation_execution import (
    SecretResolutionPort,
)
from mission_control.application.subscriptions.coordinator import (
    INBOX_TICKET_PREFIX,
    CoordinatorContract,
    CoordinatorNotification,
    CoordinatorProfile,
    causation_tag,
    delivery_id,
    plan,
    prompt_request_id,
    render_prompt,
)
from mission_control.application.subscriptions.coordinator_ports import (
    AckOutcome,
    CausationRecord,
    CoordinatorCommandGateway,
    CoordinatorInbox,
    CoordinatorInboxStore,
    InboxDelivery,
    InboxHint,
    InboxNotifier,
    MaterializeResult,
    NewInbox,
    PollDelivery,
    PromptRecord,
)
from mission_control.application.subscriptions.ports import (
    NewSubscription,
    SubscriptionNotFound,
    SubscriptionStore,
    WebhookDestinationPolicy,
    WebhookEgressRejected,
    WebhookResponse,
    WebhookTransport,
    WebhookTransportError,
    error_class_of,
)
from mission_control.application.subscriptions.webhook_signing import (
    NON_RETRYABLE_ERRORS,
    signed_headers,
)
from mission_control.contracts.contracts import (
    MissionCommandReceipt,
    MissionCommandRequest,
    MissionControlRejected,
)
from mission_control.domain.policies.contracts import ActorContext
from mission_control.domain.subscriptions.contracts import (
    ADMIN_PERMISSION,
    SUBSCRIBE_PERMISSION,
    DeliveryStatus,
    McpSessionChannel,
    StreamTicketChannel,
    Subscription,
    SubscriptionDelivery,
    SubscriptionFilters,
    SubscriptionState,
    WebhookChannel,
    after_failure,
    secret_ref_name,
)

CONTROL_PERMISSION: Final = "workflow_run.control"
INBOX_EVENT_TYPES: Final = ("*",)  # the inbox reads the whole target journal and classifies
_STALE_CODES: Final = frozenset({"stale_version", "stale_generation"})
Clock = Callable[[], datetime]


class CoordinatorRejected(Exception):
    """Typed refusal: unauthorized, not_found, invalid, closed, egress_rejected,
    recursion_bound, rate_limited, unavailable."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


class CoordinatorSubscribeRequest(CoordinatorContract):
    target: Literal["mission", "run"]
    target_id: UUID
    profile: CoordinatorProfile = Field(default_factory=CoordinatorProfile)
    delivery: InboxDelivery = Field(default_factory=PollDelivery)
    coordinator_run_ref: str | None = Field(default=None, min_length=1, max_length=256)
    after_seq: int | None = Field(default=None, ge=0)

    @model_validator(mode="after")
    def _prompt_target(self) -> CoordinatorSubscribeRequest:
        if self.profile.prompt_mode != "off" and self.coordinator_run_ref is None:
            raise ValueError("prompting a coordinator names its own run (coordinator_run_ref)")
        return self


class InboxPage(CoordinatorContract):
    subscription_id: UUID
    notifications: tuple[CoordinatorNotification, ...]
    acked_inbox_seq: int
    high_inbox_seq: int
    has_more: bool
    state: SubscriptionState


@dataclass
class PromptReport:
    admitted: list[UUID] = field(default_factory=list)
    replayed: list[UUID] = field(default_factory=list)
    skipped: list[tuple[UUID, str]] = field(default_factory=list)
    failed: list[tuple[UUID, str]] = field(default_factory=list)
    deferred: int = 0


@dataclass
class CallbackReport:
    leased: int = 0
    delivered: list[tuple[str, int]] = field(default_factory=list)
    failed: list[tuple[str, int]] = field(default_factory=list)
    dead_lettered: list[str] = field(default_factory=list)


@dataclass
class InboxPassReport:
    callbacks: CallbackReport
    materialized: dict[UUID, int] = field(default_factory=dict)
    prompts: dict[UUID, PromptReport] = field(default_factory=dict)


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _require_read(actor: ActorContext) -> None:
    if SUBSCRIBE_PERMISSION not in actor.permissions:
        raise CoordinatorRejected("unauthorized", f"actor lacks {SUBSCRIBE_PERMISSION}")


class CoordinatorInboxService:
    def __init__(
        self,
        subscriptions: SubscriptionStore,
        inbox: CoordinatorInboxStore,
        *,
        commands: CoordinatorCommandGateway | None = None,
        notifier: InboxNotifier | None = None,
        transport: WebhookTransport | None = None,
        secrets: SecretResolutionPort | None = None,
        destinations: WebhookDestinationPolicy | None = None,
        mailbox_semantics: Callable[[str, str], str] | None = None,
        clock: Clock = _utc_now,
        jitter: Callable[[], float] = random.random,
        owner: str = "coordinator-inbox",
        page: int = 200,
        max_pages: int = 10,
        lease_seconds: int = 60,
        callback_batch: int = 50,
    ) -> None:
        if subscriptions.request_scope != inbox.request_scope:
            raise ValueError("subscription and inbox stores must share one request scope")
        self._subscriptions = subscriptions
        self._inbox = inbox
        self._commands = commands
        self._notifier = notifier
        self._transport = transport
        self._secrets = secrets
        self._destinations = destinations
        # `MailboxDeliveryService.semantics`: a lane that declares `unsupported` is not prompted.
        self._mailbox_semantics = mailbox_semantics
        self._clock = clock
        self._jitter = jitter
        self._owner = owner
        self._page = page
        self._max_pages = max_pages
        self._lease_seconds = lease_seconds
        self._callback_batch = callback_batch

    @property
    def request_scope(self) -> str:
        return self._inbox.request_scope

    # -- subscribe / read -------------------------------------------------------------------

    async def subscribe(
        self, request: CoordinatorSubscribeRequest, actor: ActorContext
    ) -> CoordinatorInbox:
        _require_read(actor)
        if request.profile.prompt_mode != "off" and CONTROL_PERMISSION not in actor.permissions:
            raise CoordinatorRejected(
                "unauthorized", f"prompting a coordinator run needs {CONTROL_PERMISSION}"
            )
        delivery = request.delivery
        if isinstance(delivery, WebhookChannel) and self._destinations is not None:
            try:
                await self._destinations.validate(delivery.url)
            except WebhookEgressRejected as rejected:
                raise CoordinatorRejected("egress_rejected", str(rejected)) from None
        try:
            target = await self._subscriptions.resolve_target(request.target, request.target_id)
        except SubscriptionNotFound:
            raise CoordinatorRejected("not_found", "subscription target not found") from None
        cursor = (
            request.after_seq
            if request.after_seq is not None
            else await self._subscriptions.last_seq(target.mission_id)
        )
        now = self._clock()
        subscription = await self._subscriptions.create(
            NewSubscription(
                target=target,
                filters=SubscriptionFilters(event_types=INBOX_EVENT_TYPES),
                channel=StreamTicketChannel(ticket_id=f"{INBOX_TICKET_PREFIX}{uuid4()}"),
                cursor_seq=cursor,
                actor_ref=actor.actor_id,
                now=now,
            )
        )
        return await self._inbox.create(
            NewInbox(
                subscription_id=subscription.subscription_id,
                coordinator_ref=actor.actor_id,
                coordinator_run_ref=request.coordinator_run_ref,
                profile=request.profile,
                delivery=delivery,
                now=now,
            )
        )

    async def _get(self, subscription_id: UUID) -> CoordinatorInbox:
        try:
            return await self._inbox.get(subscription_id)
        except SubscriptionNotFound:
            raise CoordinatorRejected("not_found", "coordinator inbox not found") from None

    async def _owned(self, subscription_id: UUID, actor: ActorContext) -> CoordinatorInbox:
        _require_read(actor)
        inbox = await self._get(subscription_id)
        if inbox.coordinator_ref != actor.actor_id and ADMIN_PERMISSION not in actor.permissions:
            # The same outward answer as an absent inbox: existence is not disclosed.
            raise CoordinatorRejected("not_found", "coordinator inbox not found")
        return inbox

    async def inbox(self, subscription_id: UUID, actor: ActorContext) -> CoordinatorInbox:
        return await self._owned(subscription_id, actor)

    async def materialize(self, subscription_id: UUID) -> MaterializeResult:
        result = await self._inbox.materialize(
            subscription_id,
            planner=plan,
            now=self._clock(),
            page=self._page,
            max_pages=self._max_pages,
        )
        if result.sealed:
            await self._hint(subscription_id)
        return result

    async def _hint(self, subscription_id: UUID) -> None:
        if self._notifier is None:
            return
        inbox = await self._inbox.get(subscription_id)
        delivery = inbox.delivery
        if not isinstance(delivery, McpSessionChannel):
            return
        if not self._notifier.connected(delivery.session_ref):
            return  # the durable inbox keeps everything; the client polls when it returns
        pending = await self._inbox.notifications(
            subscription_id, after_inbox_seq=inbox.acked_inbox_seq, limit=self._page
        )
        await self._notifier.notify_inbox(
            delivery.session_ref,
            InboxHint(
                subscription_id=subscription_id,
                high_inbox_seq=inbox.next_inbox_seq - 1,
                acked_inbox_seq=inbox.acked_inbox_seq,
                pending=len(pending),
            ),
        )

    async def poll(
        self,
        subscription_id: UUID,
        actor: ActorContext,
        *,
        after_inbox_seq: int | None = None,
        limit: int = 50,
        refresh: bool = True,
    ) -> InboxPage:
        """Pending notifications after the acknowledged cursor (the polling fallback)."""

        if not 1 <= limit <= 500:
            raise CoordinatorRejected("invalid", "limit is 1..500")
        inbox = await self._owned(subscription_id, actor)
        if inbox.state is SubscriptionState.CLOSED:
            raise CoordinatorRejected("closed", "the coordinator inbox is closed")
        if refresh:
            await self.materialize(subscription_id)
            inbox = await self._inbox.get(subscription_id)
        after = max(after_inbox_seq or 0, inbox.acked_inbox_seq)
        found = await self._inbox.notifications(
            subscription_id, after_inbox_seq=after, limit=limit + 1
        )
        return InboxPage(
            subscription_id=subscription_id,
            notifications=found[:limit],
            acked_inbox_seq=inbox.acked_inbox_seq,
            high_inbox_seq=inbox.next_inbox_seq - 1,
            has_more=len(found) > limit,
            state=inbox.state,
        )

    async def ack(
        self,
        subscription_id: UUID,
        actor: ActorContext,
        *,
        notification_ids: tuple[UUID, ...] = (),
        through_inbox_seq: int | None = None,
    ) -> AckOutcome:
        """Acknowledge by id (and/or through an inbox sequence); idempotent, forward only."""

        if not notification_ids and through_inbox_seq is None:
            raise CoordinatorRejected("invalid", "acknowledge names ids or through_inbox_seq")
        inbox = await self._owned(subscription_id, actor)
        if through_inbox_seq is not None and through_inbox_seq >= inbox.next_inbox_seq:
            raise CoordinatorRejected("invalid", "through_inbox_seq is past the inbox head")
        return await self._inbox.acknowledge(
            subscription_id,
            notification_ids=notification_ids,
            through_inbox_seq=through_inbox_seq,
            actor_ref=actor.actor_id,
            now=self._clock(),
        )

    async def close(self, subscription_id: UUID, actor: ActorContext) -> CoordinatorInbox:
        await self._owned(subscription_id, actor)
        await self._subscriptions.set_state(
            subscription_id, SubscriptionState.CLOSED, now=self._clock(), actor_ref=actor.actor_id
        )
        return await self._inbox.get(subscription_id)

    # -- commands caused by notifications ---------------------------------------------------

    async def command_from_notification(
        self,
        subscription_id: UUID,
        notification_id: UUID,
        run_id: str,
        request: MissionCommandRequest,
        actor: ActorContext,
    ) -> MissionCommandReceipt:
        """Admit a command the coordinator issues because of a notification, bounded.

        Depth = notification depth + 1, refused above `max_recursion_depth`; at most
        `rate_limit_per_run` triggered commands per target run and rate window. The request
        id is recorded before admission so the events it causes inherit the depth.
        """

        if self._commands is None:
            raise CoordinatorRejected("unavailable", "no command path is composed")
        inbox = await self._owned(subscription_id, actor)
        notification = await self._inbox.notification(subscription_id, notification_id)
        if notification is None or notification.state in {"open", "suppressed"}:
            raise CoordinatorRejected("not_found", "no delivered notification with that id")
        depth = notification.depth + 1
        profile = inbox.profile
        if depth > profile.max_recursion_depth:
            raise CoordinatorRejected(
                "recursion_bound",
                f"notification depth {notification.depth} is at the recursion bound "
                f"{profile.max_recursion_depth}",
            )
        now = self._clock()
        recent = await self._inbox.causations_since(
            subscription_id,
            origin="triggered",
            since=now - profile.rate_window,
            target_run_ref=run_id,
            exclude_request_id=request.request_id,
        )
        if recent >= profile.rate_limit_per_run:
            raise CoordinatorRejected(
                "rate_limited",
                f"{recent} notification-triggered commands on this run within "
                f"{profile.rate_window_seconds}s",
            )
        await self._inbox.record_causation(
            CausationRecord(
                command_request_id=request.request_id,
                subscription_id=subscription_id,
                notification_id=notification_id,
                origin="triggered",
                target_run_ref=run_id,
                depth=depth,
                recorded_at=now,
            )
        )
        return await self._commands.command(run_id, request, actor)

    # -- prompts through the coordinator's own mailbox ----------------------------------------

    async def dispatch_prompts(self, subscription_id: UUID, *, limit: int = 20) -> PromptReport:
        report = PromptReport()
        inbox = await self._get(subscription_id)
        profile = inbox.profile
        run_ref = inbox.coordinator_run_ref
        if profile.prompt_mode == "off" or run_ref is None or self._commands is None:
            return report
        if inbox.state is SubscriptionState.CLOSED:
            return report
        await self.materialize(subscription_id)
        actor = ActorContext(
            actor_id=inbox.coordinator_ref,
            permissions=frozenset({SUBSCRIBE_PERMISSION, CONTROL_PERMISSION}),
        )
        candidates = await self._inbox.prompt_candidates(subscription_id, limit=limit)
        for notification, record in candidates:
            nid = notification.notification_id
            depth = notification.depth + 1
            now = self._clock()
            if depth > profile.max_recursion_depth:
                await self._inbox.finish_prompt(
                    subscription_id, nid, state="skipped", detail="recursion_bound", now=now
                )
                report.skipped.append((nid, "recursion_bound"))
                continue
            request_id = prompt_request_id(subscription_id, nid)
            if record is None or record.state == "failed":
                prompted = await self._inbox.causations_since(
                    subscription_id,
                    origin="prompt",
                    since=now - profile.rate_window,
                    exclude_request_id=request_id,
                )
                if prompted >= profile.prompt_limit_per_window:
                    report.deferred += 1
                    break
            outcome = await self._prompt(
                inbox,
                notification,
                record,
                actor=actor,
                request_id=request_id,
                depth=depth,
                report=report,
            )
            if outcome == "stop":
                break
        return report

    async def _prompt(
        self,
        inbox: CoordinatorInbox,
        notification: CoordinatorNotification,
        record: PromptRecord | None,
        *,
        actor: ActorContext,
        request_id: UUID,
        depth: int,
        report: PromptReport,
    ) -> Literal["ok", "stop"]:
        assert self._commands is not None and inbox.coordinator_run_ref is not None
        subscription_id = inbox.subscription_id
        nid = notification.notification_id
        run_ref = inbox.coordinator_run_ref
        kind = inbox.profile.prompt_mode
        for _attempt in range(3):
            if record is not None and record.state == "requested" and record.request:
                # A pass died after recording the request: send the identical request again,
                # so run control answers with a replay instead of a second command.
                request = MissionCommandRequest.model_validate(record.request)
            else:
                inspection = await self._commands.inspect(run_ref, actor)
                lane = inspection.lane.lane_profile if inspection.lane is not None else None
                if (
                    lane is not None
                    and self._mailbox_semantics is not None
                    and self._mailbox_semantics(lane, kind) == "unsupported"
                ):
                    await self._inbox.finish_prompt(
                        subscription_id,
                        nid,
                        state="skipped",
                        detail="unsupported_semantics",
                        now=self._clock(),
                    )
                    report.skipped.append((nid, "unsupported_semantics"))
                    return "ok"
                request = MissionCommandRequest.model_validate(
                    {
                        "request_id": str(request_id),
                        "expected_version": inspection.version,
                        "expected_generation": inspection.execution_generation,
                        "target": {"kind": "run", "id": run_ref},
                        "kind": kind,
                        "payload": {
                            "boundary": "next_turn",
                            "content": {"text": render_prompt(notification)},
                        },
                        "reason": f"{causation_tag(nid)} depth={depth}",
                    }
                )
                record = await self._inbox.begin_prompt(
                    subscription_id,
                    nid,
                    request_id=request_id,
                    request=request.model_dump(mode="json"),
                    now=self._clock(),
                )
            await self._inbox.record_causation(
                CausationRecord(
                    command_request_id=request_id,
                    subscription_id=subscription_id,
                    notification_id=nid,
                    origin="prompt",
                    target_run_ref=run_ref,
                    depth=depth,
                    recorded_at=self._clock(),
                )
            )
            try:
                receipt = await self._commands.command(run_ref, request, actor)
            except MissionControlRejected as rejected:
                await self._inbox.finish_prompt(
                    subscription_id, nid, state="failed", detail=rejected.code, now=self._clock()
                )
                if rejected.code in _STALE_CODES:
                    # Nothing was admitted: rebuild the same request id at the new frontier.
                    record = None
                    continue
                report.failed.append((nid, rejected.code))
                return "stop" if rejected.code == "unsupported_control" else "ok"
            await self._inbox.finish_prompt(
                subscription_id,
                nid,
                state="admitted",
                detail=receipt.admission.status.value,
                now=self._clock(),
            )
            (report.replayed if receipt.replay else report.admitted).append(nid)
            return "ok"
        report.failed.append((nid, "stale_version"))
        return "ok"

    # -- the background pass ------------------------------------------------------------------

    async def run_once(self) -> InboxPassReport:
        """One pass (the proposed bootstrap loop): webhook callbacks, MCP hints, prompts."""

        report = InboxPassReport(callbacks=await self.deliver_callbacks())
        for subscription_id in await self._inbox.active_inboxes(delivery="mcp_session"):
            report.materialized[subscription_id] = len(
                (await self.materialize(subscription_id)).sealed
            )
        for subscription_id in await self._inbox.active_inboxes(prompting=True):
            report.prompts[subscription_id] = await self.dispatch_prompts(subscription_id)
        return report

    # -- webhook callbacks ----------------------------------------------------------------------

    async def deliver_callbacks(self, *, limit: int = 20) -> CallbackReport:
        report = CallbackReport()
        if self._transport is None or self._secrets is None:
            return report
        leased = await self._inbox.lease_callbacks(
            now=self._clock(), owner=self._owner, lease_seconds=self._lease_seconds, limit=limit
        )
        report.leased = len(leased)
        for subscription in leased:
            try:
                await self._deliver_inbox(subscription, report)
            finally:
                await self._subscriptions.release(subscription, owner=self._owner)
        return report

    async def _deliver_inbox(self, subscription: Subscription, report: CallbackReport) -> None:
        subscription_id = subscription.subscription_id
        await self.materialize(subscription_id)
        inbox = await self._inbox.get(subscription_id)
        channel = inbox.delivery
        if not isinstance(channel, WebhookChannel):
            return
        pending = await self._inbox.notifications(
            subscription_id, after_inbox_seq=inbox.acked_inbox_seq, limit=self._callback_batch
        )
        current = subscription
        name = str(subscription_id)
        for notification in pending:
            assert notification.inbox_seq is not None
            attempt = (
                await self._subscriptions.attempts(subscription_id, notification.anchor_event_id)
                + 1
            )
            response, error_class = await self._post(channel, inbox, notification, attempt)
            receipt = SubscriptionDelivery(
                subscription_id=subscription_id,
                event_id=notification.anchor_event_id,
                seq=notification.seq_to,
                attempt=attempt,
                status=DeliveryStatus.DELIVERED,
                response_code=response.status_code if response is not None else None,
                latency_ms=response.latency_ms if response is not None else None,
                recorded_at=self._clock(),
            )
            if response is not None and 200 <= response.status_code < 300:
                current = await self._subscriptions.record_success(
                    current, receipt, owner=self._owner
                )
                await self._inbox.mark_delivered(
                    subscription_id, inbox_seq=notification.inbox_seq, now=self._clock()
                )
                report.delivered.append((name, notification.inbox_seq))
                continue
            decision = after_failure(current.failure_count, self._clock(), self._jitter())
            if error_class in NON_RETRYABLE_ERRORS:
                decision = decision.model_copy(update={"dead_letter": True})
            await self._subscriptions.record_failure(
                current,
                receipt.model_copy(
                    update={
                        "status": DeliveryStatus.DEAD_LETTERED
                        if decision.dead_letter
                        else DeliveryStatus.FAILED,
                        "error_class": error_class
                        or (f"http_{response.status_code}" if response is not None else None),
                    }
                ),
                failure_count=decision.failure_count,
                next_attempt_at=decision.next_attempt_at,
                dead_letter=decision.dead_letter,
                owner=self._owner,
            )
            if decision.dead_letter:
                report.dead_lettered.append(name)
            else:
                report.failed.append((name, notification.inbox_seq))
            return

    async def _post(
        self,
        channel: WebhookChannel,
        inbox: CoordinatorInbox,
        notification: CoordinatorNotification,
        attempt: int,
    ) -> tuple[WebhookResponse | None, str | None]:
        assert self._transport is not None and self._secrets is not None
        try:
            resolved = await self._secrets.resolve((channel.secret_ref,))
            secret = resolved[secret_ref_name(channel.secret_ref)].encode("utf-8")
        except (LookupError, KeyError):
            return None, "secret_unavailable"
        body = notification.body()
        headers = signed_headers(
            secret,
            body,
            delivery=delivery_id(inbox.subscription_id, notification.notification_id),
            attempt=attempt,
            now=self._clock(),
            extra={
                "X-MC-Subscription-Id": str(inbox.subscription_id),
                "X-MC-Notification-Id": str(notification.notification_id),
                "X-MC-Inbox-Seq": str(notification.inbox_seq),
            },
        )
        try:
            return await self._transport.post(channel.url, body, headers), None
        except WebhookTransportError as error:
            return None, error_class_of(error)


__all__ = [
    "CONTROL_PERMISSION",
    "INBOX_EVENT_TYPES",
    "CallbackReport",
    "CoordinatorInboxService",
    "CoordinatorRejected",
    "CoordinatorSubscribeRequest",
    "InboxPage",
    "InboxPassReport",
    "PromptReport",
]
