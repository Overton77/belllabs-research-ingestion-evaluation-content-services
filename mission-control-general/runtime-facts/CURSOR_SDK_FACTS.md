---
type: Runtime Fact Sheet
title: Cursor SDK fact sheet (cloud and local runtimes)
description: "Scope: @cursor/sdk (TypeScript) in both cloud and local runtimes, plus the Cloud Agents REST API v1 that the cloud runtime wraps. Facts below are taken from the official docs and from the published type definitions in…"
tags: [mission-control, runtime, facts]
---
# Cursor SDK fact sheet (cloud and local runtimes)

**Status:** fact sheet (verified 2026-09-07)

Scope: `@cursor/sdk` (TypeScript) in both `cloud` and `local` runtimes, plus the Cloud Agents REST API v1 that the cloud runtime wraps. Facts below are taken from the official docs and from the published type definitions in the `@cursor/sdk@1.0.31` tarball (`npm pack`, inspected under `%TEMP%`). Anything not found in those sources is marked **UNVERIFIED**.

## Sources

- npm registry: `@cursor/sdk` latest `1.0.31` (`dist-tags.latest`, published 2026-09-03; `next` = `1.0.27-beta.0`). Repo `github.com/cursor/cursor`. Node ≥ 22.13 required.
- Cursor TypeScript SDK reference — https://cursor.com/docs/sdk/typescript
- SDK changelog (1.0.20 → 1.0.31) — https://cursor.com/docs/sdk/changelog
- Cloud Agents API v1 endpoints (public beta) — https://cursor.com/docs/cloud-agent/api/endpoints
- Cursor APIs overview (auth, rate limits) — https://cursor.com/docs/api
- Webhooks (legacy v0) — https://cursor.com/docs/cloud-agent/api/webhooks
- Hooks (incl. cloud agent support matrix) — https://cursor.com/docs/agent/hooks
- In-VM agent metadata (preview) — https://cursor.com/docs/cloud-agent/metadata
- Type definitions inspected: `dist/cjs/run.d.ts`, `run-store-public-types.d.ts`, `run-event-tailer.d.ts`, `cloud-api-client.d.ts`, `executor-types.d.ts` (all `@cursor/sdk@1.0.31`).
- Cursor community forum (staff answers on concurrency; not official docs) — https://forum.cursor.com/t/clarification-on-cloud-agent-limits-simultaneous-agents-vs-environments-repos/157584

## 1. Package names, versions, API surface

- npm `@cursor/sdk@1.0.31` (TypeScript). PyPI `cursor-sdk` ships the same version number since 1.0.24. Per-platform native helper packages `@cursor/sdk-<platform>` carry sandbox + ripgrep binaries. Entry points: `@cursor/sdk`, `@cursor/sdk/sqlite` (`SqliteLocalAgentStore`), `@cursor/sdk/bundled`, `@cursor/sdk/bundled/sqlite`.
- Runtime is selected by passing `local: {...}` or `cloud: {...}` to `Agent.create()`. Same `CURSOR_API_KEY` for both. "Local" = agent loop + filesystem in your Node process; inference is always Cursor-hosted.
- REST (cloud only), base `https://api.cursor.com`, v1 public beta ("APIs may change before GA"):
  - `POST /v1/agents` (create agent + enqueue initial run), `GET /v1/agents`, `GET /v1/agents/{id}`
  - `POST /v1/agents/{id}/runs` (follow-up), `GET /v1/agents/{id}/runs`, `GET /v1/agents/{id}/runs/{runId}`, `GET /v1/agents/{id}/runs/{runId}/stream` (SSE), `POST /v1/agents/{id}/runs/{runId}/cancel`
  - `GET /v1/agents/{id}/usage[?runId=]`, `GET /v1/agents/{id}/artifacts`, `GET /v1/agents/{id}/artifacts/download?path=`
  - `POST /v1/agents/{id}/archive`, `POST /v1/agents/{id}/unarchive`, `DELETE /v1/agents/{id}`
  - `POST /v1/sub-tokens` (1-hour user-scoped worker token; needs agent-scoped team service-account key)
  - `GET /v1/me`, `GET /v1/models`, `GET /v1/repositories`
  - Self-hosted workers/pools: `/v0/private-workers/*` (list/claim/release; `GET /v0/private-workers/pending-requests/stream` SSE)
- Legacy v0 API still exists and is the only surface with webhooks (see §4).

## 2. Identity hierarchy

| Level | Cloud | Local |
|---|---|---|
| Agent (durable conversation + workspace) | `bc-<uuid>`; `agent.agentId` populated immediately; may be client-supplied (`agentId`) for idempotent create (`409 agent_id_conflict` on re-POST); client-supplied `agentId` cannot be combined with `envVars` | `agent-<...>`; stored in local agent store (SQLite default, or `JsonlLocalAgentStore` / custom `LocalAgentStore`) |
| Run (one `send()` / one prompt) | `run-<uuid>`; `agent.latestRunId` on the agent record | `run.id` from the local store |
| Request correlation | `requestId` (platform UUID) on `Run` and `RunResult`, and `error.requestId` | same; persists in local stores |
| Turn | Not exposed by SDK/REST as an ID. Inside the VM only: `turn/id`, `turn/started-at`, `turn/model`, `turn/user-id` via the metadata socket (`/run/cursor/api.sock`), present only while a turn is active. In hooks: `conversation_id` (stable) and `generation_id` (changes every user message). | Hooks receive `conversation_id` / `generation_id`. |
| Tool call | `call_id` (SDK) / `callId` (REST) stable across the running→completed pair | same |
| Idempotency | `idempotencyKey` on `Agent.create()` (auto-generated for cloud) and on `send()` | `idempotencyKey` on `send()` |

Stable: `agentId`, `runId`, `requestId`. Per-turn: `generation_id` (hooks), `turn/*` metadata keys. A run can contain multiple turns (background subagents return as follow-up turns on the same run; `usage` events fire once per turn).

## 3. Control semantics

| Operation | Cloud | Local |
|---|---|---|
| start | `Agent.create({cloud})` + `agent.send()` → `Run`; REST `POST /v1/agents` returns `{agent, run}` with run `CREATING` | `Agent.create({local})` + `agent.send()` |
| resume (reattach) | `Agent.resume("bc-…")` returns a fresh handle; conversation state loaded server-side. `Agent.getRun(runId, {runtime:"cloud", agentId})` returns a `Run` you can `stream()/wait()/cancel()` against a run already in flight. | `Agent.resume(agentId)` loads latest checkpoint from the local store; requires same `cwd`/`store`. `Agent.getRun(runId)` for detached handles. |
| fork | **Not supported.** No fork/branch-conversation API in SDK or REST. | Not supported. |
| follow-up | `agent.send()` / `POST /v1/agents/{id}/runs`. **Only one active run per agent**: while a run is `CREATING` or `RUNNING` the call fails with `409 agent_busy` (`AgentBusyError`, `isRetryable: false`). `409 agent_archived` → `ConfigurationError`. | `agent.send()`; no `agent_busy`. `send({local:{force:true}})` expires a stuck active run first. |
| steer (mid-turn inject) | `run.steer?.(text)` exists on the handle but **always resolves `revert_to_followup`** for cloud runs. | `run.steer(text)` → `"complete_delivered"` \| `"revert_to_followup"` (added in 1.0.31). Works while a foreground subagent runs (subagent moves to background). Detached local handles (from `getRun`) also always return `revert_to_followup`. `steer` is optional on `Run` and not a `RunOperation`. |
| interrupt | Only via cancel. | Only via cancel. |
| pause | **Not supported** (no pause/park API). Agent-level `IDLE` is the between-runs state, not a pause. | Not supported. |
| cancel | `run.cancel()`, `Agent.cancelRun()`, REST `POST …/cancel`. Terminal: run → `CANCELLED`, cannot be resumed; continue by creating a new run. Cancelling a non-active/terminal run → `409 run_not_cancellable` (SDK: no-op if finished). | `run.cancel()`; status → `"cancelled"`, stream aborts, in-flight tool calls stop, partial text stays on `Run`. |
| completion | `run.wait()` → `RunResult{status:"finished"\|"error"\|"cancelled", result?, error?{message,code?}, usage?, git?, durationMs?, model?}`. REST: `GET run` gains `result`, `durationMs`, `git` once terminal; SSE `result` then `done`. | `run.wait()`; `run.wait()` resolves after background-subagent follow-up turns with the last turn's text. |
| archive/delete | `Agent.archive/unarchive/delete`; archive idempotent; archived agents reject new runs. | same static methods route to the local store. |

## 4. Event stream, webhooks, polling, recovery

**SDK stream (`run.stream()`)**: typed discriminated union `SDKMessage`, every event carries `agent_id` and `run_id`. Types: `system` (subtype `init`; `model?`, `tools?`), `user`, `assistant` (`TextBlock | ToolUseBlock`), `thinking`, `tool_call` (`call_id`, `name`, `status: "running"|"completed"|"error"`, `args?`, `result?`, `truncated?`), `status` (cloud lifecycle: `CREATING|RUNNING|FINISHED|ERROR|CANCELLED|EXPIRED`), `task`, `request` (awaiting user input/approval; `request_id`), `usage` (per turn, `TokenUsage`). Tool `args`/`result` schemas are explicitly **not stable**; the envelope is. Finer-grained `InteractionUpdate` deltas via `send(..., { onDelta, onStep })`: `text-delta`, `thinking-delta`, `thinking-completed`, `tool-call-started`, `tool-call-completed`, `tool-call-delta` (one level of nested subagent/task updates), `partial-tool-call`, `token-delta`, `step-started`, `step-completed`, `turn-ended` (carries per-turn usage), `user-message-appended`, `summary`, `summary-started`, `summary-completed`, `shell-output-delta`. Callbacks are awaited (backpressure).

**Cloud SSE (REST `…/stream`)**: event types `status` (`{runId,status}`), `assistant` (`{text}` delta), `thinking`, `tool_call` (`{callId,name,status:"running"|"completed",args?,result?,truncated?}`), `interaction_update` (SDK-shape), `heartbeat`, `result` (`{runId,status,text?,durationMs?,git?}`), `error` (`{code,message}`), `done`. Scoped to one run; does not replay prior runs. Most events carry an opaque `id:` (looks like `1713033006000-0`); the leading `status` event has no id and is re-sent on every reconnect. **Reconnect**: send `Last-Event-ID`; id must belong to the requested run else `400 invalid_last_event_id`. **Retention**: header `X-Cursor-Stream-Retention-Seconds`; after the window `410 stream_expired` → read terminal state via `GET run`. Numeric retention value: **UNVERIFIED** (not published). The SDK's `cloud-api-client.d.ts` `streamRun({agentId, runId, lastEventId, signal})` exposes the same resume parameter; SDK docs state cloud streams "retain backlog for a window after the run starts" so multiple subscribers can `run.stream()`.

**Local stream**: events are persisted to the local store's append-only `runEvents` log; `RunEventTailer.streamRunEvents(runId, { afterOffset, mode: "replay" | "tail" | "replay-and-tail", pollIntervalMs })` (offsets `afterOffset`/`nextOffset`). A re-fetched local run that already finished does not support `stream` (`UnsupportedRunOperationError`; guard with `run.supports("stream")`).

**Polling**: `GET /v1/agents/{id}/runs/{runId}`, `Agent.listRuns()`, `Agent.getRun()`, `run.onDidChangeStatus(listener)`, `run.conversation()` (structured `ConversationTurn[]`). Agent-level `GET /v1/agents/{id}` gives `status` and `latestRunId` only.

**Webhooks**: v1 endpoints page states "Webhooks are coming soon. The legacy v0 API still supports them." v0 webhook: configured per agent create with a webhook URL; only event `statusChange`, fired on `FINISHED` or `ERROR`. Headers `X-Webhook-Signature` (`sha256=<hmac-hex>` over raw body), `X-Webhook-ID`, `X-Webhook-Event`, `User-Agent: Cursor-Agent-Webhook/1.0`. Payload: `{event:"statusChange", timestamp, id:"bc_…", status, source:{repository,ref}, target:{url,branchName,prUrl}, summary}` (optional fields omitted). "Webhooks may be retried if your endpoint returns an error status code." No delivery-count or ordering guarantee is documented.

## 5. Token/context visibility and compaction

- `TokenUsage { inputTokens, outputTokens, cacheReadTokens, cacheWriteTokens, totalTokens, reasoningTokens? }`; `totalTokens` excludes `reasoningTokens`. Live cumulative on `run.usage`; final on `result.usage`; per-turn on the `usage` stream event and `turn-ended` delta. `undefined` when no turn reported usage.
- **Context-window size and fill ratio are not exposed** by SDK or REST. The only signal is the `preCompact` hook input: `trigger: "auto"|"manual"`, `context_usage_percent`, `context_tokens`, `context_window_size`, `message_count`, `messages_to_compact`, `is_first_compaction`. `preCompact` is observational: it cannot block or alter compaction; output is `user_message` only.
- No public API to trigger compaction (`/compact` is an IDE/CLI command; no SDK/REST equivalent found — **UNVERIFIED** whether cloud agents accept a slash command in prompt text). The `summary-started` / `summary` / `summary-completed` `InteractionUpdate` deltas exist but their relation to compaction is undocumented (**UNVERIFIED**).
- Model params may include context size (`model.params` e.g. `{id:"context", value:"1m"}`, discoverable via `Cursor.models.list()` / `GET /v1/models`).

## 6. Workspace, branch, filesystem, sandbox identity

- **Cloud**: workspace is per **agent** (VM persists across runs; `IDLE` agents may be hibernated/snapshotted). `cloud.repos[] {url, startingRef?, prUrl?}` (≤20 repos; omit for a no-repo empty VM; `env.type: "cloud"|"pool"|"machine"`, `env.name`). `workOnCurrentBranch` (default false → new `cursor/...` branch), `autoCreatePR`, `openAsCursorGithubApp`, `skipReviewerRequest`. `git.branches[] {repoUrl (no scheme), branch?, prUrl?}` is **per-agent state returned identically on every run**; use `latestRunId`/SSE to attribute. Artifacts are agent-scoped (`artifacts/` dir; download via 15-minute presigned S3 URL). In-VM metadata socket exposes `workspace/repo-url`, `repo-urls`, `branch-name`, `environment-id`, `automation-id`. No filesystem snapshot/checkpoint API beyond git push + artifacts.
- **Local**: `local.cwd` (+ `local.dirs` for multi-root). Filesystem is the host's; no snapshot API. Optional sandbox (`local.sandboxOptions.enabled`: bubblewrap/seatbelt; writes limited to cwd/temp/`sandbox.json` allowlist; network denied by default). `local.autoReview` routes tool calls through the IDE classifier (best-effort). Conversation checkpoints are content-addressed blobs in the local store (`checkpoints` substore, `latestCheckpoint.rootBlobId`). `listArtifacts()` returns `[]`, `downloadArtifact()` throws.

## 7. Tool, command, subagent, and hook events

- Tool lifecycle: `tool_call` running→completed/error (SDK), `tool-call-started/-completed/-delta/partial-tool-call` deltas. Shell turns appear in `run.conversation()` as `shellConversationTurn {shellCommand, shellOutput{stdout,stderr,exitCode}}`.
- Subagents: inline `agents` on `Agent.create()` or `.cursor/agents/*.md` (cloud: `customSubagents[]`, ≤20). Nesting: top-level and direct subagents may spawn; a subagent launched by a subagent cannot (effective depth 2). Subagent progress surfaces as `tool-call-delta` nested updates and `task` events; background subagent results come back as follow-up turns on the same run (local only).
- **Hooks are file-based only; there is no programmatic hook callback in the SDK.** Sources: project `.cursor/hooks.json`, user `~/.cursor/hooks.json` (local only), team/enterprise (Enterprise plans). Hooks are spawned processes speaking JSON over stdio; they can call any HTTP endpoint (docs show a `stop` hook `fetch`-ing telemetry). Full Agent hook vocabulary: `sessionStart`, `sessionEnd`, `preToolUse`, `postToolUse`, `postToolUseFailure`, `subagentStart`, `subagentStop`, `beforeShellExecution`, `afterShellExecution`, `beforeMCPExecution`, `afterMCPExecution`, `beforeReadFile`, `afterFileEdit`, `beforeSubmitPrompt`, `preCompact`, `stop`, `afterAgentResponse`, `afterAgentThought`.
- **Cloud agents**: command-based hooks only (no prompt-based). Supported: `beforeShellExecution`, `afterShellExecution`, `beforeReadFile`, `afterFileEdit`, `preToolUse`, `postToolUse`, `postToolUseFailure`, `subagentStart`, `subagentStop`, `beforeSubmitPrompt`, `preCompact`, `afterAgentResponse`, `afterAgentThought`, `stop`. Not available: `sessionStart`, `sessionEnd`, `beforeMCPExecution`, `afterMCPExecution`, Tab hooks, `workspaceOpen`. Hooks do not run during early read-only exploratory turns. Self-hosted pool/machine workers do fire `sessionStart`/`sessionEnd` on claim/release.
- **Local SDK**: docs say hooks load from `.cursor/hooks.json` in `local.cwd` and `~/.cursor/hooks.json`, name `beforeShellExecution`/`preToolUse` as gating examples, and `agent.reload()` re-reads hooks. Which of the remaining Agent hooks (`sessionStart/End`, `preCompact`, `stop`, `subagentStart/Stop`, MCP hooks) actually fire in headless SDK runs is **UNVERIFIED** (no per-hook matrix published for the SDK local runtime).
- `stop` hook input `{status:"completed"|"aborted"|"error", loop_count}`; output `followup_message` auto-submits the next user message (default `loop_limit` 5). `subagentStop` likewise. Common hook input fields: `conversation_id`, `generation_id`, `model`, `model_id`, `model_params`, `hook_event_name`, `cursor_version`, `workspace_roots`, `user_email`, `transcript_path`.

## 8. Delivery guarantees and unsupported controls

- Cloud SSE: resumable by `Last-Event-ID` within retention; ids opaque; after `410 stream_expired` terminal state must come from `GET run`. No documented at-least-once/ordering guarantee wording for run streams. Webhook (v0): retried on non-2xx, no dedupe key beyond `X-Webhook-ID`.
- Pool watch stream (`/v0/private-workers/pending-requests/stream`, self-hosted workers only) is explicitly **best-effort**: "a rare failure can drop one, and a dropped event is never redelivered"; cursors expire 5 minutes after the issuing list; `410 cursor_expired`; max 4 concurrent streams per service account.
- Local: run events persisted append-only before/with emission (store-dependent); `enableAgentRetries` (default true) auto-retries transport/stall failures.
- Unsupported: fork; pause/park; cloud mid-run steer; programmatic hooks; `systemPrompt`, `tools`/`disallowedTools`, `customTools`, `autoReview`, custom `store` on cloud; artifacts on local; `local.settingSources` on cloud; inline `mcpServers` not persisted across `resume`; Team Admin API keys.

## 9. Crash / reconnect behavior

- **Client crash, cloud**: agent and run continue server-side ("runs need to survive the caller disconnecting"). Recover with `Agent.resume(bc-…)` + `Agent.getRun(runId, {runtime:"cloud", agentId})` then `stream()` (with backlog within retention) or `wait()`; or REST `GET run` + SSE with `Last-Event-ID`.
- **Client crash, local**: the agent loop runs in the client process, so the run dies with it. State (agent metadata, checkpoints, runs, run events) is in the local store; `Agent.resume(agentId)` continues the conversation from the last checkpoint; a stuck `running` row is cleared with `send({local:{force:true}})`. 1.0.23: run history "survives interrupted writes". 1.0.30: local runs refresh the access token before expiry (>1h runs).
- **Agent side, cloud**: run `ERROR`/`EXPIRED` are terminal run statuses; agent stays `IDLE` after recoverable errors ("run-level error detail stays on Get A Run"); `ARCHIVED` = archived or expired agent (terminal). VM may be hibernated between runs.
- Errors: all extend `CursorSdkError { isRetryable, code?, status?, cause?, endpoint?, requestId?, operation? }`; classes `AuthenticationError`, `RateLimitError` (retryable when transient), `ConfigurationError`, `AgentBusyError` (not retryable), `IntegrationNotConnectedError` (`helpUrl`), `NetworkError`, `UnsupportedRunOperationError`, `AgentNotFoundError`, `UnknownAgentError`.

## 10. Usage and cost fields

- Live tokens: §5 `TokenUsage`.
- Billed: `agent.getUsage({runId?})` / `Agent.getUsage(agentId)` → `AgentUsage { usage: TokenUsage, cost?: UsageCost, runs: RunUsage[] }`, `UsageCost { rawCostCents, chargedCents }` (`chargedCents` 0 for plan-included/BYOK/credit-grant; cost absent until it settles). Cloud: per-run; local: per-turn (since 1.0.27).
- REST `GET /v1/agents/{id}/usage` → `totalUsage` + `runs[] { id, usageUuid?, usage{inputTokens,outputTokens,cacheWriteTokens,cacheReadTokens,totalTokens} }`; no cost field on REST. `usageUuid` matches team usage-events endpoints.
- Billing: SDK runs appear under the "SDK" tag in the team usage dashboard; service-account keys bill the team, user keys bill the user.

## 11. Auth, rate limits, concurrency

- Auth: user API key or service-account API key (`CURSOR_API_KEY` / `apiKey`); Team Admin keys not supported. Resolution order: explicit `apiKey` → env → stored browser login (`Cursor.auth.login()` mints a 90-day key into `~/.cursor/sdk/auth.json`). REST accepts Basic (`key:` empty password) or Bearer. Repository-scoped keys cannot create no-repo agents. `POST /v1/sub-tokens` mints 1-hour user-scoped worker tokens.
- Rate limits: "Cloud Agents API — All endpoints — Standard rate limiting"; overview default is 20 req/min per endpoint per user/team/org unless documented; `429` with `Retry-After: 60` on Admin/Org APIs. Exact Cloud Agents numeric limit **UNVERIFIED**. Metadata socket: 120 req/min, burst 20, 8 connections.
- Concurrency: **one active run per agent** (`409 agent_busy`). Concurrent cloud agents per plan are not in official docs; Cursor staff on the forum state Pro = 8 simultaneous, Pro+/Ultra higher (unpublished) — treat as **UNVERIFIED (forum-sourced)**. Run-level rate-limit message observed in the field: "The run was rate-limited due to too many concurrent runs."
- Limits: 50 `envVars`, 50 inline MCP servers, 20 subagents, 20 repos, 5 images × 15 MB.

## 12. Native status vocabularies (exact spellings)

- REST agent `status`: `ACTIVE` (turn running / waiting on background work / about to start), `IDLE` (last turn finished, follow-ups accepted; also after recoverable run errors), `ARCHIVED` (terminal).
- REST run `status` / SDK `SDKStatusMessage.status`: `CREATING`, `RUNNING`, `FINISHED`, `ERROR`, `CANCELLED`, `EXPIRED`. Internal `RunLifecycleStatus` type additionally lists `QUEUED`.
- SDK `RunStatus`: `"running" | "finished" | "error" | "cancelled"`; `RunResult.status` excludes `running`.
- SDK `SDKAgentInfo.status?`: `"running" | "finished" | "error"`. Internal `AgentLifecycleStatus`: `"IDLE" | "RUNNING" | "ARCHIVED" | "ERROR"`.
- Tool call `status`: SDK `"running" | "completed" | "error"`; REST SSE `"running" | "completed"`.
- `SteerAckOutcome`: `"complete_delivered" | "revert_to_followup"` (internal third value `confirm_steering` never surfaces).
- Hook `stop.status`: `completed | aborted | error`; `subagentStop.status`: `completed | error | aborted`; `sessionEnd.reason`: `completed | aborted | error | window_close | user_close`; `postToolUseFailure.failure_type`: `error | timeout | permission_denied`.
- Webhook (v0) `status`: `FINISHED`, `ERROR`.
- Error codes seen: `agent_busy`, `agent_id_conflict`, `agent_archived`, `run_not_cancellable`, `run_not_found`, `invalid_last_event_id`, `stream_expired`, `cursor_expired`.

## Claims in MISSION_CONTROL_SPEC.md §10 confirmed / refuted / unverified

| Claim (§10.2 / §12.1) | Verdict | Evidence |
|---|---|---|
| Cursor SDK version 1.0.31 | Confirmed | npm `dist-tags.latest = 1.0.31` (2026-09-03) |
| `cursor_cloud`: no mid-run follow-up, active run → `409 agent_busy` | Confirmed | REST Create A Run; `AgentBusyError`, `isRetryable:false` |
| `cursor_cloud` interrupt = `run.cancel` → `agent.send` | Confirmed | cancel is terminal; continue via new run |
| `cursor_cloud` pause unsupported | Confirmed | no pause API in SDK or REST |
| `cursor_cloud` cancel native (`run.cancel` / REST cancel) | Confirmed | `POST …/runs/{runId}/cancel`; `409 run_not_cancellable` |
| `Agent.resume(bc-…)` native; re-pass MCP | Confirmed | inline `mcpServers` not persisted across resume |
| Snapshot = git branch/PR state per agent (not per run) | Confirmed | `git` "Per-agent state, not per-run" |
| Cloud hooks: project `.cursor/hooks.json`, command hooks only, no `sessionStart/End`, no MCP hooks | Confirmed | Hooks doc cloud matrix; team/enterprise hooks also load on Enterprise |
| Cloud subagents native, depth 2 | Confirmed | top-level + direct subagents may spawn; grandchildren cannot |
| Completion: poll `GET run`; v0 webhook `statusChange` (`FINISHED`/`ERROR`); v1 webhooks pending | Confirmed | v1 page: "Webhooks are coming soon" |
| `cursor_local` steer via `run.steer` → `complete_delivered` / `revert_to_followup` | Confirmed | added 1.0.31; local only |
| `cursor_local` queue = `wait_then_send` | Partially confirmed | no `agent_busy` locally; concurrent `send()` on one local agent behavior undocumented; `local.force` exists |
| `cursor_local` hooks "full `.cursor/hooks.json` incl. `preCompact`, `stop`, subagent events" | Unverified | SDK docs confirm file-based hooks + `preToolUse`/`beforeShellExecution`; no per-hook matrix for headless local runs |
| `cursor_local` completion = stream + `run.wait()` | Confirmed | |
| `cursor_local` snapshot = filesystem + git | Confirmed (by absence) | no SDK snapshot API; host FS only |
| Cursor `preCompact` observable as compaction signal (§12.1) | Confirmed for hooks; adapter cannot request compaction | `preCompact` is observe-only; no SDK/REST compact call |
| Adapter can re-attach to a running cloud run and get missed events | Confirmed within retention | `Last-Event-ID`; `410 stream_expired` after `X-Cursor-Stream-Retention-Seconds` (value unpublished) |
