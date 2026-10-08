# [FT-F5] Subscriptions: webhook, SSE events watch, MCP notification

Linear: OVE-48

**Epic:** Control (SPEC-06)
**Team:** T5
**Blocked by:** None (can start immediately)
**Status:** ready-for-agent

**What to build:** A consumer registers a Subscription (`missionctl subscribe --run RUN --webhook URL --secret-ref REF --events human_task.opened,run.completed`, or `POST /subscriptions`, or MCP `mission_subscribe`), the Outbox relay consumer delivers matching mission events at least once with HMAC-SHA256 signatures and a per-Subscription cursor, writes `subscription_delivery` receipts, backs off with jitter and dead-letters after twelve failures while emitting `subscription.dead_lettered`. `GET /missions/{id}/events?after_seq=` serves SSE with replay then live, heartbeats every 30 seconds and `resync_required` on an expired cursor; `missionctl events watch` consumes it. MCP sessions receive `notifications/mission/event`.

**Spec sections:** SPEC-06 "Subscriptions", "Contracts" (`mc.subscription.v1`, SSE frame), "Persistence" (`mission_subscription`, `subscription_delivery`), "Authority".

**Writable regions:** `src/mission_control/application/subscriptions/{service,relay}.py` (new), `interfaces/http/subscriptions.py` (new), SSE route in `interfaces/http/mission_control.py`, `interfaces/mcp/coordinator_server.py` (new tool and notification only), `migrations/0029_...sql` (subscription tables; coordinate the file with FT-F1 through the integrator), `interfaces/cli/main.py` (`subscribe`, `events watch`; integrator-coordinated).

**Acceptance criteria:**
- [ ] Subscription rows carry no secret values; `secret_ref` resolves through the existing secret resolution port at send time.
- [ ] Webhook body is the `mc.event.v1` envelope with references only; `X-MC-Signature: sha256=<hmac>` verifies against the raw body.
- [ ] At-least-once delivery proven by killing the relay mid-batch and observing duplicates with identical `event_id` and no gaps in `seq`.
- [ ] Dead-lettering after 12 failures emits `subscription.dead_lettered` and `subscribe list` shows the state.
- [ ] SSE replays from `after_seq`, then streams live events, sends heartbeats, and returns `resync_required` with `CURSOR_EXPIRED` for a cursor older than retention.
- [ ] Grant checks: `workflow_run.read` required to subscribe; another actor's Subscription can be closed only with `workflow_run.admin`.
- [ ] MCP `mission_subscribe` plus notification shape covered by a unit test on the coordinator server.
- [ ] `make check` passes; integration tests on the real stack.

**Verification:** `make check`; `uv run --group biotech pytest -m common_db tests/integration/postgres/test_subscriptions.py tests/unit/coordinator -q`.

**Notes:** Uses the existing `ConsumerCursor` and outbox tables; no new relay process is introduced (extend the outbox relay entrypoint). The ticket-based WebSocket adapter is out of scope. Webhook targets in tests are local fakes; no external endpoint.
