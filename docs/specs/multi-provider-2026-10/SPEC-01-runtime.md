---
type: Specification
title: "Provider lifecycle, controls and continuation"
description: "Durable session ownership, command delivery, context rollover and workflow parity without conflating provider and Temporal lifecycles."
tags: [mission-control, runtime, temporal]
---

# Runtime contract

This is proposed behavior for the multi-provider increment. Reuse `AgentHarness`, `SessionLane`, `LaneTurnService`, closing-fact reducers and the existing segmented operation workflow. Do not wrap each provider in an independent Goal Loop.

## Session ownership and crash recovery

The stable identity is `(installation, application, tenant, run, activation, attempt, harness_execution, generation)`. Native session/turn/item/subagent IDs are mappings beneath it. Temporal Run ID is not Mission Control Run ID; Continue-As-New changes the former.

1. Admit a bounded operation, exact binding and Context Packet. Acquire a fenced workspace/session lease. Record prepare intent before materialization.
2. Materialize and attest the configuration; create/start through the provider adapter. Record the native session ID before sending work when the surface allows it.
3. Record send intent with idempotency key, expected generation and instruction digest. Send once; record the native turn ID and delivered receipt.
4. Observe with one writer per native session. Persist frames before advancing the durable provider cursor; heartbeat only a resume hint. Reducer output commits once under the generation fence.
5. End an observation segment without destroying a still-running local process. A worker-owned session manager retains the SDK client/app-server/bridge between activities. Activity cancellation is not automatically provider cancellation. Explicit lane cancel owns that action.
6. On process death or worker takeover, read persisted dispatch state and native status first. Reattach when supported; hydrate only after recording loss and reconciling effects. Do not call `send_turn` during reattachment.
7. A send acknowledged remotely but not recorded locally is an ambiguous dispatch. Query by idempotency/native identity when available. If identity cannot be recovered reliably, park `in_doubt` and block competing replacements.
8. On finish, collect outputs, usage and workspace snapshot, settle the attempt through existing application services, and release the lease only after artifact custody succeeds.

For local lanes, route all operations on a session to its owning worker/session manager. A random worker cannot read another machine's local CLI state. Persist owner identity, lease expiry and generation; reject wrong-owner control delivery. Takeover after an expired lease fences old control/effect admission, but cannot undo an external tool already running. Remote status uncertainty is an incident, not success.

The initial manager can live in the existing worker process. Its restart semantics must be explicit: local provider execution may die while a provider-hosted run continues. A separate supervised service is a later optimization, not another scheduler.

## Commands and “emit”

Use the current ordered command mailbox and application command receipts. Preserve accepted, delivered and applied as distinct facts.

| Command | Required behavior |
| --- | --- |
| `queue_instruction` | Persist with scope, target, deadline, expected generation and boundary; claim FIFO at `next_turn` or `next_iteration`; send once. Provider native queue is optional, not the durable queue authority |
| `add_context` | Register/pin content and deliver a new packet item at the requested boundary; do not mutate a consumed packet |
| `interrupt_and_inject` | Explicitly choose qualified cooperative steering or cancel-and-replace; report the actual mode. No automatic weaker fallback unless admitted by policy |
| `cancel` immediate | Persist Stop Fence, dispatch provider cancel, drain terminal/status observations, reconcile effects, record all timestamps independently |
| `pause` | Stop future admissions immediately; report whether current work continues to a safe boundary or is held at a tool gate. Never label boundary pause as a frozen VM |
| `resume` | Resume admitted work/waits; cannot clear a terminal cancel or revive an expired generation |
| `fork` | New execution identity + new workspace snapshot/hydration, with conversation fork only where qualified |
| `request_continuation` | Durable trigger fulfilled at a safe boundary using the existing seal/transfer service |

Interpret the brief's “emit” as event publication unless the owner means a separate command: `emit` is **not** a generic state-changing command. Provider frames enter the ingestion port; mission events come from reducers; coordinator messages use `queue_instruction`/`add_context`. No client may forge `mission.accepted` by emitting a socket event.

Cancel-and-replace sequence: fence the old turn's new effects, request interruption, drain/reconcile, seal its settled partial evidence, then create a replacement turn with a fresh delivery identity. If settlement exceeds the bound, `in_doubt`; no replacement. Pre-fence effects retain their disposition. Shell process-tree termination on local hosts needs its own verification; killing a CLI does not prove its child process stopped. Hosted cancel acknowledgement is likewise not proof of zero remaining effects.

Cooperative steer targets the exact active native turn; a concurrent completion produces a typed stale-target result. Policy may explicitly requeue at the next boundary with the same command lineage. A steering acceptance does not prove the agent used the instruction.

## Provider adapter rules

**Claude local:** use the Python SDK's interactive client, capture session identity and one stream owner, map tool/result/subagent messages into frames. Bind native permission requests to durable Human Tasks. When interrupting, drain the interrupted response before consuming a replacement response. Persist session files under the leased state root. Resuming history after process death is a new connection/generation; it is not proof that the previous tool execution never happened. Qualify callback support from the actual Python type surface. See [Python SDK](https://code.claude.com/docs/en/agent-sdk/python) and [sessions](https://code.claude.com/docs/en/agent-sdk/sessions).

**Codex local:** use a transport adapter over app-server, with generated schemas from the selected release. Correlate RPC responses and server requests separately; persist pending approval identity before displaying it. Serialize new-turn admission, explicitly use steering for the active turn, and observe terminal completion after interruption. Recover from documented thread reads/resume plus our journal; do not make unstable rollout-file parsing the sole recovery mechanism. A provider conversation fork is not a Git branch. See [app-server](https://learn.chatgpt.com/docs/app-server).

**Cursor:** preserve the existing SDK pin until a separate qualified upgrade. Cloud's busy response is a scheduling condition, not a reason to retry sends in a tight loop. Use durable observation offsets, distinguish stream-retention expiry from task failure, reconcile terminal state, and keep unknown usage unknown. Reapply required nonpersisted options on resume. `cursor_cloud` cannot inherit local fail-closed claims merely because both use the same SDK. See [Python SDK](https://cursor.com/docs/sdk/python).

**Claude/Codex hosted:** implement only qualified documented product operations. Complete the hosted evidence tickets first. No assumed app-server connection to a provider-hosted Codex chat, no assumed Claude SDK client for a `claude --cloud` session, and no reliance on this Codex desktop app's private task-management tools as a distributable Mission Control API.

## Four independent progress mechanisms

| Mechanism | Trigger | Durable state that advances |
| --- | --- | --- |
| GoalDirected iteration | Evaluated progress and unmet criteria | Iteration, attempts, accepted evidence and governor counters |
| Provider context compaction | Native context pressure or qualified explicit operation | Native context epoch/compaction observation; same goal iteration may continue |
| Mission continuation | Context health, planned session rotation or command | Sealed checkpoint, new session generation, transferred packet and snapshot |
| Temporal Continue-As-New | Workflow history/length policy | New Temporal execution with compact orchestration state; same mission identity |

Do not increment goal iterations on provider compaction. Do not reset budgets or retry counters when any context/session rotates. Native compaction does not replace the immutable Context Packet. A provider result declaring “done” still goes through completion evaluation.

Context policy uses fresh token occupancy only when the provider exposes it with a known model window. Define configurable soft/hard watermarks, reserved instruction/output headroom, max turns, max transfers and max compaction failures. Proposed defaults for qualification: soft 70%, hard 85%, reserve at least 15%; these are tuning inputs, not provider guarantees. Cumulative billed tokens are not context occupancy. When occupancy is unavailable use conservative turn-boundary/session budgets, and report `unknown` rather than inventing a percentage.

At soft pressure: prefer qualified native compaction; serialize it with turn admission. Record intent/start/completion and remeasure. At hard pressure or native failure: stop new actions at a safe boundary and call `ContinuationService.seal` then `transfer`. Keep stable system intent, exact grants, success criteria, unsettled liabilities, accepted artifacts, branch/patch state and mailbox frontier mandatory. Summaries are advisory and validated against structured state. Missing mandatory context rejects the transfer rather than truncating it.

Native compaction cannot be assumed manually triggerable everywhere. Codex documents explicit thread compaction; Claude/Cursor support must be qualified against their exact integration surface. An observed before-compaction hook is not a control API. Cloud adapters with no usable session rollover fail admission for missions that require it, or run a deliberately bounded job whose contract does not require rollover.

Continuation crash recovery is a persisted phase machine: requested → frozen → snapshotted → sealed → target prepared → hydrated → verified → activated. Each phase is idempotent. Only one target generation can activate; keep the old one fenced until activation is recorded. A failed target does not consume held mailbox entries. Verify packet, workspace and materialization digests before the first turn.

## Temporal integration

Register the existing continuation activities and hydrators; add versioned call sites to StageGraph/GoalDirected/operation workflows. Carry pending commands, current binding/checkpoint refs, lease owner, generation and reconciliation state across Continue-As-New. Drain message handlers and persist external intent before rollover. Use existing patch/versioning conventions and replay old histories. [Temporal message passing](https://docs.temporal.io/develop/python/workflows/message-passing) and [Continue-As-New](https://docs.temporal.io/develop/python/workflows/continue-as-new) establish the lifecycle primitives.

Provider network/process I/O, materialization, database writes and compaction calls belong in activities. Queries are read-only; Signals/Updates transport already-admitted commands. API acknowledgement must describe whether it is merely accepted or actually applied. Long observation activities heartbeat; human waits use durable wait state/timers. Reconcile before retrying any side-effecting provider call. Transport retries, provider turns, operation attempts, goal iterations and continuation transfers retain separate counters.

## Workflow parity

Stage Graph: an accepted output packet releases a dependent node; provider-local paths do not cross nodes without artifact registration. GoalDirected: each iteration consumes its own packet and produces typed progress/evidence; acceptance and governors decide continuation. Independent verifier bindings need the same admission checks as executors.

Mission Chains: compose `ChainIntentRelay` and production family-input author; release remains transactional with the supplier's accepted facts and outbox. Every consumer has its own budget, auth and environment. Transfer only declared artifact outputs and structured evidence. Repository changes transfer through an explicit snapshot/patch or commit artifact, not by sharing a mutable checkout. Replayed release intents start one consumer run. Cross-application links remain rejected unless separately specified and authorized; this packet does not expand that authority.
