---
type: Runtime Fact Sheet
title: Eve fact sheet (Vercel durable agent framework)
description: "NO DEEP AGENTS NOW !"
tags: [mission-control, runtime, facts]
---
# Eve fact sheet (Vercel durable agent framework)

NO DEEP AGENTS NOW !

**Status:** fact sheet (verified 2026-09-07)

Scope: the `eve` npm package (Vercel), its default `/eve/v1` HTTP session API, the `eve/client` TypeScript SDK, and its relationship to the Workflow SDK. Facts come from the docs and type definitions shipped inside the `eve@0.52.2` tarball (`npm pack`, extracted under `%TEMP%`), which mirror https://eve.dev/docs. Anything not found there is marked **UNVERIFIED**.

## Sources

- npm registry: `eve` latest `0.52.2` (`npm view eve version`), Apache-2.0, repo `github.com/vercel/eve` (`packages/eve`), `bin: eve`. Description: "Filesystem-first framework for durable backend AI agents that run anywhere."
- Bundled docs (paths relative to `docs/` inside the tarball; also published at https://eve.dev/docs): `concepts/sessions-runs-and-streaming.md`, `concepts/execution-model-and-durability.mdx`, `channels/eve.mdx`, `guides/auth-and-route-protection.md`, `guides/hooks.md`, `guides/session-context.md`, `guides/client/{overview,streaming,continuations,messages}.mdx`, `guides/instrumentation.md`, `agent-config.md`, `sandbox.mdx`, `subagents/index.mdx`, `tools/workflows.mdx`, `instructions.mdx`, `guides/dynamic-capabilities.md`, `CHANGELOG.md`.
- Type definitions: `dist/src/protocol/message.d.ts` (stream event union), `dist/src/client/types.d.ts` (client result/status types).
- Dependencies (package.json 0.52.2): `@workflow/core@5.0.0-beta.48`, `@workflow/world@5.0.0-beta.33`, `@workflow/world-local@5.0.0-beta.42`, `@workflow/world-vercel@5.0.0-beta.44`, `ai@^7.0.82` (peer), `@ai-sdk/mcp@^2.0.29`, `@ai-sdk/otel@^1.0.58`; optional peers `just-bash@^3.1.0`, `microsandbox@^0.5.0`, `braintrust@^3.0.0`, `@opentelemetry/api`.
- Workflow SDK — https://workflow-sdk.dev/ ; Vercel Sandbox — https://vercel.com/docs/sandbox

## 1. Package names, versions, API surface

- Package `eve@0.52.2` (not `@vercel/eve`). Subpath exports used below: `eve/client`, `eve/hooks`, `eve/channels/auth`, `eve/channels/eve`, `eve/tools/workflow`, `eve/workflow-modules`. CLI: `eve dev`, `eve start`, `eve init`, `eve info`, `eve traces`.
- Each deployment hosts its own API; eve.dev is docs only. Default channel (`channels/eve.ts`) routes:
  - `GET /eve/v1/health` (public) → `{ ok, status: "ready", workflowId }`
  - `GET /eve/v1/info` (auth) → agent-info v4
  - `POST /eve/v1/session` (start + first message) → `202`, `sessionId` in body and `x-eve-session-id` header
  - `POST /eve/v1/session/:sessionId` (follow-up: exactly one of `message` | `inputResponses`)
  - `POST /eve/v1/session/:sessionId/cancel` (`{turnId?, tasks?}`) → `202 accepted` | `200 no_active_turn`
  - `POST /eve/v1/session/:sessionId/clear`, `/compact`, `/reset` (`{reason?}`)
  - `GET /eve/v1/session/:sessionId/stream?startIndex=&includeTailIndex=1` (NDJSON)
  - Workflow webhooks minted by `createWebhook()` are served under `/.well-known/workflow/v1/webhook/`.
- Session controls are ID-addressed only; channel continuation tokens (Slack thread etc.) never cross the HTTP API.

## 2. Identity hierarchy

- **Session** (`sessionId`, e.g. `wrun_A`) — the durable conversation; equals the Workflow run id of the session workflow. Stable for the session's life; never reused after `reset`. Default lifetime 30 days (`limits.sessionTimeoutMs`, or `false`).
- **Turn** (`turnId`, e.g. `turn_0`; `ctx.session.turn.id`, `.sequence`) — one user delivery → model loop; stamped on every turn-scoped event. Steering creates a _new_ turn id.
- **Step** (`stepIndex`) — one model call within a turn; `sequence` orders fragments inside a step.
- **Event** (`meta.id`, `evt_`-prefixed ULID, `meta.at`) — minted once at durable write; identical across reconnects and rewinds; absent on events written before stream version 20.
- **Tool call** (`callId`) — correlates `action.input.appended` → `actions.requested` → `action.partial*` → `action.result`.
- **Child session** — each subagent delegation is its own session (`subagent.called.data.childSessionId`); `ctx.session.parent` carries `{sessionId, callId, rootSessionId, turn}`.
- **HITL request** (`requestId`, e.g. `req_A`) — answered via `inputResponses`.
- **Sandbox** (`sandbox.id`) — stable per session.
- **Auth** — `ctx.session.auth.initiator` (started the session) and `auth.current` (active turn's caller); `null` on unprotected agents.
- Client cursor: `ClientSessionState { sessionId, streamIndex }`.

## 3. Control semantics

| Operation         | Eve behavior                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| ----------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | -------- | ----------- |
| start             | `POST /eve/v1/session` / `client.sessions.create({message})`. `202` as soon as Workflow accepts the run; inbox may still be starting → an immediate follow-up can get `409 session_not_active`; wait for `session.waiting`. Create-once: pass `operationId`; the same operation under the same authenticated principal returns the existing session once its owner is active. Simultaneous creates may receive different candidate ids; only the claimant runs the first turn. |
| resume (reattach) | `client.sessions.attach(sessionId, {streamIndex?})`; there is no "resume" verb — a parked session is woken by the next message. Parked turns (HITL, OAuth, workflow waits) resume where they left off without re-emitting events.                                                                                                                                                                                                                                              |
| fork              | **Not supported.** No fork/branch API; `reset` never mints a replacement id.                                                                                                                                                                                                                                                                                                                                                                                                   |
| follow-up         | `POST /eve/v1/session/:id` / `session.send(text, {turnPolicy?})`. Delivery goes to the session's _command inbox_; there is no durable FIFO message queue.                                                                                                                                                                                                                                                                                                                      |
| steer             | **Default** for message sends: `turnPolicy: "steer"` = buffer the replacement, cooperatively cancel the active turn (`turn.cancelled` → `session.waiting`), start a new turn with the new message. Partial output/side effects are not rolled back. Multiple replacements arriving before cancellation settles may fold into one turn (arrival order kept). `inputResponses` never steer.                                                                                      |
| queue             | `turnPolicy: "queue"` preserves the message until the active turn settles; adjacent ready messages may be folded into one turn. Per-channel default `turnPolicy` and per-send override.                                                                                                                                                                                                                                                                                        |
| interrupt         | = cancel (below) or steer.                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| pause             | **Not supported** as an explicit control. Durable parking is runtime-driven (HITL `input.requested`, `authorization.required`, workflow waits), not caller-driven.                                                                                                                                                                                                                                                                                                             |
| cancel            | `POST …/cancel {turnId?, tasks?}` / `session.cancel({turnId})` / `response.cancel()`. Asynchronous: `accepted` (202) or `no_active_turn` (200); confirm via `turn.cancelled` → `session.waiting`. `tasks: true` also cancels admitted background tasks (works while parked). Session stays alive and accepts the next message. Children report their own boundaries on child streams.                                                                                          |
| compact           | `POST …/compact` / `session.compact()`; waits for active turn; emits `compaction.requested` → `compaction.completed` → `session.waiting`; on failure history is preserved. `no_active_session` if inactive.                                                                                                                                                                                                                                                                    |
| clear             | `POST …/clear` / `session.clear()`; drops model-message history (incl. user-role instructions, recalled memory), keeps id, system-role instructions, tools, skills, durable state, limits, sandbox; emits `context.cleared` → `session.waiting`.                                                                                                                                                                                                                               |
| reset             | `POST …/reset {reason}` / `session.reset()`; terminally retires the id.                                                                                                                                                                                                                                                                                                                                                                                                        |
| completion        | Per turn: `turn.completed` (or `turn.failed`/`turn.cancelled`) then `session.waiting`. Session terminal: `session.completed` (incl. 30-day expiry, which lets the active turn settle) or `session.failed`. Client `MessageResponse.result()` → `status: "completed"                                                                                                                                                                                                            | "failed" | "waiting"`. |

## 4. Event stream, webhooks, polling, recovery

- **Live stream**: NDJSON over `GET …/stream`; typed union (`MessageStreamEvent` in `protocol/message.d.ts`). Full `type` vocabulary (0.52.2): `session.started`, `turn.started`, `message.received`, `step.started`, `action.input.appended`, `actions.requested`, `action.partial`, `action.result`, `approval.candidate`, `approval.settled`, `input.requested`, `input.resolved`, `subagent.called`, `subagent.started`, `subagent.event`, `subagent.completed`, `reasoning.appended`, `reasoning.completed`, `message.appended`, `message.completed`, `result.completed`, `compaction.requested`, `compaction.completed`, `context.cleared`, `authorization.required`, `authorization.completed`, `step.completed`, `step.failed`, `turn.completed`, `turn.failed`, `turn.cancelled`, `session.waiting`, `session.failed`, `session.completed`. (Docs table omits `approval.*`, `subagent.started`, `subagent.event`.)
- Envelope `{ type, data, meta: { id, at } }`; turn-scoped `data` carries `turnId`, `stepIndex`, `sequence`. `x-eve-stream-version` header (current v25, delta-only appends); client normalizes v21–v24.
- **Ordering**: stream order is authoritative; `startIndex` is an absolute event count. `meta.id` is time-ordered but not a total order across processes.
- **Reconnect**: `?startIndex=<n>` re-attaches to a live or finished session and replays from n; `0` rewinds; negative = tail-relative (no cursor advance). `includeTailIndex=1` → `x-eve-stream-tail-index` for catch-up reads (`stream({follow:false})`). No retention window is documented for stream history (stored with the workflow run).
- **Polling**: no session-status GET endpoint; state is derived from the stream (latest event via `startIndex=-1`) or `SessionSnapshot`. `GET /eve/v1/info` is static agent inspection.
- **Webhooks (outbound)**: **none built in.** Outbound notifications are authored via `defineHook` handlers (arbitrary code, may `fetch`) or channel event handlers. Inbound webhooks exist for workflow tools (`createWebhook`) and channel adapters.

## 5. Token/context visibility and compaction

- `step.completed.data.usage?: { inputTokens?, outputTokens?, cacheReadTokens?, cacheWriteTokens?, costUsd? }` plus `finishReason` (`"stop" | "tool-calls" | "length" | "content-filter" | "error" | "other"`); `costUsd` supplied by AI Gateway when available.
- `compaction.requested` carries `modelId`, `sessionId`, `turnId`, `usageInputTokens`; `compaction.completed` marks the checkpoint. Automatic compaction is part of the default harness (threshold config in `agent.ts`: **UNVERIFIED** exact option names).
- Manual compaction via `/compact`; `/clear` for hard reset of history.
- Runtime limits (`limits` in `agent.ts`): session token limits (default input budget `40_000_000` provider-reported input tokens), `maxTokenCostUsdPerSession` (USD, model tokens only), `sessionTimeoutMs` (30 days). Exceeding → tool/model call stops with `SESSION_TOKEN_LIMIT_REACHED` / `SESSION_TOKEN_COST_LIMIT_REACHED`; the crossing call is allowed to finish; a budget prompt is re-raised on next message until granted. Child usage counts against parent quota.
- No context-window-size or fill-percentage field is exposed on the stream (**UNVERIFIED** beyond `usageInputTokens`).

## 6. Workspace, filesystem, sandbox identity

- Sandbox is **per durable session** (`sandbox.id` stable across reconnects); a subagent gets its own sandbox. `/workspace` persists across turns for the same session.
- Backends: Vercel Sandbox (`vercel()`; idles after 30 min inactivity, filesystem preserved and resumed; snapshot `source` option), Docker (`docker()`, long-lived container per session), microsandbox (local VM, snapshot-backed templates), just-bash (virtual FS under `.eve/sandbox-cache/`), custom `SandboxBackend`. Default images `ghcr.io/vercel/eve` / `vcr.vercel.com/vercel/eve/base` tagged to eve version.
- `onSession({use, ctx})` runs once per session (network policy, resources, credentials). Provider-loss replacement reuses the sandbox key without rerunning `onSession`; post-create files are not restored — docs advise persisting artifacts outside the sandbox.
- Server stop halts all sandbox compute; sessions reattach from persisted container/VM/snapshot on restart. `ctx.getSandbox()` returns a handle with `stop()` / `delete()`; `setNetworkPolicy()` mid-turn.
- No git/branch/repo concept in core; GitHub is a channel (`eve/channels/github`) with `channel.repository.fullName`. Workflow state: local world persists under `.eve/.workflow-data`; on Vercel, Vercel Workflow.

## 7. Tool, command, subagent, and hook events

- Tools: `actions.requested` (calls stream before execution), `action.partial` (generator progress, last-write-wins), `action.result` (`ActionResultStatus: "completed" | "failed" | "rejected"`). HITL: `input.requested` / `input.resolved`, `approval.candidate` (`"pending" | "rejected" | "failed" | "timed-out" | "stale"`), `approval.settled`. OAuth: `authorization.required` / `.completed` (`"authorized" | "declined" | "failed" | "timed-out"`).
- Workflow tools (`defineWorkflowTool`, `"use workflow"`): durable waits via `createHook`, `createWebhook`, `sleep`, `ctx.ask`; background tools return task receipts; `yield task.postMessage(...)` requests a parent turn.
- Subagents: `subagent.called {childSessionId}`, `subagent.started`, `subagent.event` (parent re-emission with its own `meta.id`), `subagent.completed` (task receipt); child progress lives on the child session stream.
- Hooks: `defineHook({ events: { "<type>" | "*": handler } })` in `agent/hooks/` (`eve/hooks`). **Observe-only**: run after the event is durably recorded, cannot inject model context or block; see the same `meta.id`; observe retries as new events. `HookContext extends SessionContext` (+ `agent {name,nodeId}`, `channel {kind, continuationToken}`), has `ctx.getSandbox()`. Handlers are arbitrary async code, so they can POST to external endpoints. No channel filter on `defineHook`; channel `events` maps scope per channel and replace default handlers for the same key.
- Model-context contributions use `defineInstructions` / `defineDynamic` (`agent/instructions/`), and `defineDynamic` resolves models, subagents, connections, tools, skills, instructions at runtime.

## 8. Delivery guarantees and unsupported controls

- Stream is durable: every event recorded before the step completes; replay from any `startIndex`; duplicates across reconnect/rewind share `meta.id` (dedupe key). **Retried steps re-emit under new ids** (same `turnId`/`stepIndex`/`sequence`, no attempt id) — up to 4 attempts per durable step. Completed steps replay from the journal and emit nothing.
- Only one turn run can claim a session's turn inbox (no double-streaming). Hooks fire at-least-once per emitted event (retry-aware).
- Inbound messages: not a durable FIFO; `steer` replaces, `queue` folds. Concurrent senders share one inbox; cross-session independence.
- Unsupported: fork; explicit pause; per-message webhooks; session listing/status GET; editing history other than `compact`/`clear`/`reset`; cancelling a turn that has not started (`no_active_turn`).

## 9. Crash / reconnect behavior

- **Client**: `MessageResponse` and `session.stream()` reconnect by `streamIndex`; auth functions re-run on reconnect. `client.sessions` holds only `{sessionId, streamIndex}`; persist and `attach` later (continuations guide). Tail reads (negative `startIndex`) do not advance the cursor.
- **Agent side**: every turn is a Workflow run; state checkpointed at step boundaries; crash/redeploy/resume replays journal without new events; interrupted step re-runs (≤4 attempts) with re-emitted events; parked turns hold no compute. Provider failure after partial output keeps events from both attempts. On server stop, sandboxes stop and reattach on next start.
- Session expiry emits `session.completed`; stored data is not deleted.

## 10. Usage and cost fields

- Stream: `step.completed.data.usage` (`inputTokens`, `outputTokens`, `cacheReadTokens`, `cacheWriteTokens`, `costUsd?`). `compaction.requested.data.usageInputTokens`.
- Workflow run tags (Vercel Workflow dashboard, framework-owned, best-effort): `$eve.type` (`"session"|"turn"|"subagent"`), `$eve.parent`, `$eve.root`, `$eve.subagent`, `$eve.trigger`, `$eve.schedule`, `$eve.title`, `$eve.trace_id`, `$eve.model`, `$eve.input_tokens`, `$eve.output_tokens`, `$eve.cache_read_tokens`, `$eve.tool_count`. These power the "Agent Runs" tab under Vercel Observability (gated per team).
- Budgets: token and USD limits in `limits` (§5). No credits concept; dollars only via `costUsd`/AI Gateway.
- OpenTelemetry via `instrumentation.ts` (`@ai-sdk/otel`); local traces via `eve traces`.

## 11. Auth, rate limits, concurrency

- Auth is per deployment via `auth: AuthFn | AuthFn[]` on the eve channel. Built-ins (`eve/channels/auth`): `localDev()` (only under `eve dev`/`vercel dev`), `vercelOidc()` (Vercel OIDC bearer JWT), `httpBasic()`, `jwtHmac()`, `jwtEcdsa()`, `oidc()`, `placeholderAuth()` (401 in prod); low-level verifiers `verifyVercelOidc`, `verifyJwtHmac`, `extractBearerToken`, `withAuthChallenges`. Default with no file: `[vercelOidc(), localDev(), placeholderAuth()]` — rejects all production traffic until replaced. Custom `AuthFn` (API keys, Clerk, Auth.js) are first-class. Bearer/Basic client credentials may be functions re-evaluated per call.
- Rate limits: **none documented** in eve; bounded by host (Vercel Functions/Workflow) and model provider. **UNVERIFIED** beyond that.
- Concurrency: one active turn per session (steer/queue semantics); sessions independent; subagent batches share parent budgets. Platform-level session concurrency limits: **UNVERIFIED** (Vercel Workflow / Sandbox quotas apply).
- Error codes seen: `session_not_active` (409), `no_active_turn`, `no_active_session`, `SESSION_TOKEN_LIMIT_REACHED`, `SESSION_TOKEN_COST_LIMIT_REACHED`; client errors `ClientError`, `HealthResponseError`, `AgentInfoResponseError`.

## 12. Native status vocabularies (exact spellings)

- Session lifecycle events: `session.started`, `session.waiting`, `session.failed`, `session.completed`.
- Turn lifecycle events: `turn.started`, `turn.completed`, `turn.failed`, `turn.cancelled`.
- Step: `step.started`, `step.completed` (`finishReason`), `step.failed`.
- Client `MessageResult.status`: `"completed" | "failed" | "waiting"`.
- Cancel result `status`: `"accepted"` (202) | `"no_active_turn"` (200). Compact/clear/reset: `"no_active_session"` when inactive.
- `ActionResultStatus`: `"completed" | "failed" | "rejected"`. `ApprovalCandidateOutcome`: `"pending" | "rejected" | "failed" | "timed-out" | "stale"`. `AuthorizationOutcome`: `"authorized" | "declined" | "failed" | "timed-out"`.
- Client reducer message `status`: `"complete" | "failed" | "streaming" | "submitted"`; tool part `state: "input-streaming" | "input-available" | ...`.
- Health: `status: "ready"`. `$eve.type`: `"session" | "turn" | "subagent"`.

## Relationship to Vercel Workflow

Every eve turn is a durable workflow built on the open-source Workflow SDK (`@workflow/*` `5.0.0-beta` line vendored; "Vercel Workflow" when deployed on Vercel). Locally, the SDK's local world persists runs under `.eve/.workflow-data`; on Vercel it uses `@workflow/world-vercel`. Authored workflow tools import `createHook`, `createWebhook`, `sleep`, `FatalError` from `workflow`. **Eve does not expose or document `WorkflowAgent` / `@ai-sdk/workflow`**; Eve's own harness (built on AI SDK `ai@^7`) is the agent loop. `WorkflowAgent` (successor to `DurableAgent` from `@workflow/ai`) is a separate AI SDK construct for building agents directly on Workflow without Eve — **UNVERIFIED** whether Eve uses it internally (no reference in docs, types, or dependency list).

## Claims in MISSION_CONTROL_SPEC.md §10 confirmed / refuted / unverified

| Claim (§10.4 / §12.1)                                           | Verdict                    | Evidence                                                                                                   |
| --------------------------------------------------------------- | -------------------------- | ---------------------------------------------------------------------------------------------------------- |
| Eve version 0.52                                                | Confirmed                  | npm latest `0.52.2`                                                                                        |
| `POST /eve/v1/session` starts a session                         | Confirmed                  | channels/eve.mdx; `202` + `x-eve-session-id`                                                               |
| `turnPolicy: queue \| steer`                                    | Confirmed                  | default `"steer"`; `"queue"` opt-in; per-channel default + per-send override; `inputResponses` never steer |
| Cancel with `turnId`                                            | Confirmed                  | `POST …/cancel {turnId?, tasks?}`; async `accepted`/`no_active_turn`                                       |
| `compact` / `clear` controls                                    | Confirmed                  | plus `reset`; all ID-addressed, no continuation token                                                      |
| `defineHook` observe-only                                       | Confirmed                  | "Handlers are observe-only. They cannot inject model context." run after durable record                    |
| `defineInstructions` / `defineDynamic` for model context        | Confirmed                  | hooks.md, instructions.mdx, dynamic-capabilities.md                                                        |
| Stream `turn.completed` / `session.waiting`                     | Confirmed                  | plus `turn.failed`, `turn.cancelled`, `session.failed`, `session.completed`                                |
| Eve pause unsupported                                           | Confirmed                  | only runtime parking (HITL/OAuth/workflow waits)                                                           |
| Eve interrupt = cancel or steer                                 | Confirmed                  | steer is the default send policy                                                                           |
| Eve completion signal = `turn.completed` then `session.waiting` | Confirmed                  | terminal session = `session.completed`/`session.failed`                                                    |
| Eve snapshot = per-session sandbox + durable state              | Confirmed                  | `sandbox.id` per session; Vercel Sandbox snapshots; `defineState`                                          |
| Eve compaction observable via stream (§12.1)                    | Confirmed                  | `compaction.requested {usageInputTokens}` / `compaction.completed`; manual `/compact` available            |
| Eve built on `workflow` package / Vercel Workflow               | Confirmed                  | `@workflow/*` deps; every turn is a workflow run                                                           |
| Eve uses `WorkflowAgent`                                        | Unverified / not evidenced | no reference in eve 0.52.2 docs, types, or deps                                                            |
| Adapter can re-attach and get missed events                     | Confirmed                  | `startIndex` replay; `meta.id` dedupe; no documented retention limit                                       |
| Eve exposes outbound webhooks                                   | Refuted (none built in)    | notifications via `defineHook`/channel handlers                                                            |
