---
type: Decision Record
title: "The mission stream is a Socket.IO /missions namespace over the same application handlers as HTTP and MCP, replayed from PostgreSQL with Redis as fanout only"
description: "Proposed for the multi-provider packet (SPEC-04, MP-14): python-socketio wraps the public FastAPI app; three logical streams keep separate cursor domains; replay always reads the journal between the client cursor and the high-watermark; Redis fans out notifications and is never the ledger; socket mutations are thin parity over HTTP handlers; room membership is never authorization."
tags: [mission-control, adr, proposed, realtime, socketio, subscriptions]
status: proposed
source: docs/specs/multi-provider-2026-10/SPEC-04-realtime.md; docs/specs/multi-provider-2026-10/RESEARCH.md (Socket.IO delivery guarantees, python-socketio ASGI); ADR-0028; ADR-0032; bootstrap/technical_api.py (existing worker-side socket)
---

# The mission stream is a Socket.IO `/missions` namespace over the same application handlers as HTTP and MCP, replayed from PostgreSQL with Redis as fanout only

**Status: proposed.** Becomes accepted when the owner authorizes it; until then ADR-0028 and
ADR-0032 remain the accepted text for frames and subscriptions.

The dashboard must listen to a whole mission, including every subordinate, and occasionally send a
command or resolve a Human Task on the same connection. `python-socketio` is already a dependency
and `bootstrap/technical_api.py` already carries a socket, but that socket is a worker-side surface
keyed by operation for runtime approvals, not an application-scoped mission stream. We wrap the
public FastAPI application in `socketio.ASGIApp` with one `/missions` namespace whose handlers call
the existing application services (subscribe, command, resolve human task) and never a provider SDK
or SQL. Three logical streams keep separate cursor domains: mission events ordered by `mission_seq`,
provider frames ordered by `frame_seq` per execution and generation, and best-effort presence that is
not state. Replay is always read from PostgreSQL between the client's cursor and the journal
high-watermark, with a consistent snapshot when the cursor has expired; Redis (`AsyncRedisManager`)
only fans out notifications across API processes. We rejected raw WebSockets (we would rebuild rooms,
reconnection and acks), SSE alone (no acks or commands on the same connection) and promoting the
technical socket (wrong scope, wrong key, no tenant authorization). Socket.IO delivers at most once by
default, so persisted cursors, snapshot-at-high-watermark and deduplication by event ID provide
recoverable delivery; nothing promises exactly-once.

## Consequences

- Room membership is never authorization; every connect, subscribe, replay and mutation reauthorizes
  against the principal, and cross-tenant lookups behave like absent targets.
- Socket mutations are conveniences over the HTTP handlers; HTTP stays the simplest durable path.
- A slow consumer receives `resync_required` and is detached; workers are never blocked and no socket
  queue grows without bound.
- There is no global order across missions or across streams; a client must not merge native offsets
  with mission sequences.
- Provider token deltas may be coalesced for display, but control and terminal facts are durable before
  they are delivered.
