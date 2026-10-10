---
type: Verification Reference
title: Codex local lane qualification
description: How the codex lane profile (Codex app-server on a Linux/WSL worker) is implemented, what is proven offline on fixtures and real local services, what only the owner-run bounded live drill can qualify, and the exact drill command with its preconditions.
tags: [mission-control, qualification, lanes, codex]
---

# Codex local lane (`codex`) qualification (MP-08)

`lane_profile.qualified` is `false` for `codex`. It flips only through a reviewed release that
cites a record in `docs/qualification/lanes/` written by the owner-run live drill
(`codex-<date>.md`, front matter `outcome: qualified`, `evidence: live_drill`). Fixtures never
flip it. Three facts are tracked separately for every control (ADR-0035): **implemented** (the
adapter exists), **qualified** (a recorded live drill on this exact pin) and
**account-enabled** (the owner's account and host actually expose it).

Pin: `codex-cli 0.162.0`, app-server protocol v2 over stdio, schema generated offline with
`codex app-server generate-json-schema` and committed under
`tests/fixtures/provider_frames/codex/schema/` (`PIN.json` carries every digest; the bundle is
`sha256:0bf5254b…`). The launcher probes `codex --version` and refuses any other CLI with the
typed `CodexVersionMismatch` (`PROVIDER_PIN_MISMATCH`) before an app-server starts, unless the
composition passes the explicit `allow_version_mismatch` override.

## Status per control

| Control | Implemented (`adapters/codex`) | Qualified | Account-enabled | Notes |
| --- | --- | --- | --- | --- |
| `prepare` | native: lease, packet, digest-checked projection (`materialize_projection`), required executables from the composition's `rows` resolver (MP-03), `CODEX_HOME` with project trust | no | n/a | trust is written to the isolated `CODEX_HOME/config.toml` (`[projects."<lease>"] trust_level = "trusted"`) or passed as a `-c` override in owner mode; the drill verifies the CLI honours it |
| `start` | native: `codex app-server` (pinned version), `initialize`/`initialized`, `thread/start` (cwd, model, approval policy, sandbox, `approvalsReviewer=user`, `ephemeral=false`) | no | unknown | child env is allow-listed + the admitted credential + `CODEX_HOME` only, built by the injected `provider_child_environment` (adapters never import bootstrap) |
| `reattach` | emulated: relaunch over the same lease, MP-11 `recover` (the dead connection's approval correlations become `lost`), `thread/resume` by thread id, history absorbed | no | unknown | a new process is a new connection epoch; cursors from another epoch resynchronize from `thread/read` |
| `send_turn` | native: serialized admission, `turn/start` only on an idle thread, `clientUserMessageId` = idempotency key, `busy` while active | no | unknown | a `turn/start` on an active thread would steer it (`turnTrigger` doc), so it is never issued |
| `steer` (`SteeringLane`) | native: `turn/steer` with `expectedTurnId`; server refusal / concurrent completion / mismatch → typed `stale_target` | no | unknown | the `interrupt_and_inject` describe cell follows the frozen `LANE_COMMAND_SEMANTICS` (`cancel_and_replace` today); `cooperative_inject` needs the drill |
| `cancel_turn` | native: `turn/interrupt`, terminal `turn/completed` (`interrupted`) observed before the receipt | no | unknown | |
| `observe` | native: every notification and server request → frame (MP-13 mapping), unknown methods stay `unknown`, deltas keyed by sequence | no | unknown | cursors: live `turn@epoch:seq` and `turn@epoch:seq/k` (k frames of a multi-frame event taken), history `turn@epoch:h<n>` |
| `reconcile_dispatch` | native: live journal, then `thread/read` history by `UserMessageThreadItem.clientId`; idle thread without the key = `not_received` | no | unknown | whether the server echoes `clientUserMessageId` as `clientId` is a drill item |
| `snapshot` | emulated: git patch + `mc.workspace_snapshot.v1` artifacts | no | n/a | |
| `usage` | native: last `thread/tokenUsage/updated` (`last`, never cumulative `total`); cost stays estimated | no | unknown | |
| `end_session` | native: patch and `outputs/` stored, app-server terminated, lease released after the patch | no | n/a | a `finished` turn whose declared `outputs/` are missing closes with `missing_outputs` (FT-G4) |
| `pause` | emulated: approval answers held at the tool gate (`hold_approvals`); every request is bound (persisted) before it is held | no | unknown | `pause_at_tool_gate` |
| `fork` | not built | no | n/a | `thread/fork` exists; a conversation fork is not a Git branch; unqualified |
| compaction (MP-12 `CompactingLane`) | native: `compact(CompactionRequest) -> CompactionReceipt`: `thread/compact/start` on an idle thread under the admission lock, bounded wait for `thread/compacted` / a `contextCompaction` item, `native_ref = <thread>@<epoch>:<seq>` | no | unknown | refused (`completed=False`) while a turn is active; MP-12 counts the epoch from max(its own, native) AFTER_COMPACTION frames |
| occupancy (MP-12 `ContextOccupancyLane`) | native: last `thread/tokenUsage/updated` `last.totalTokens` against `modelContextWindow` | no | unknown | `unknown` without a window, before any usage, and after a compaction until the next usage update; which `last` field is the occupancy is a drill item |
| approvals (MP-11 broker) | native RPCs: `item/commandExecution/requestApproval`, `item/fileChange/requestApproval`, `item/permissions/requestApproval`, `item/tool/requestUserInput`, `mcpServer/elicitation/request` → `NativeApprovalRequest` bound by `ApprovalBroker` before any wait or hold; each request served in its own task (the event pump never waits on a human); fail closed without a broker | no | unknown | other server requests (`item/tool/call`, `account/chatgptAuthTokens/refresh`, `attestation/generate`, legacy v1 approvals) get JSON-RPC -32001 |
| capacity | `ProviderCapacityLimited` from a JSON-RPC error carrying `codexErrorInfo` ∈ {usageLimitExceeded, rateLimitExceeded, serverOverloaded, unauthorized}; mid-turn limit failure closes with `error_code=capacity` | no | unknown | the error-data spelling is a drill item |

### Approval mapping (MP-08 x MP-11)

- origin: command / fileChange / permissions → `provider_permission`; requestUserInput →
  `provider_question`; elicitation → `mcp_elicitation`.
- identity: `native_session_ref` = threadId, `native_turn_ref` = turnId,
  `native_request_ref` = the JSON-RPC id, `tool_call_ref` = `itemId` (`itemId:approvalId` for
  zsh-exec-bridge callbacks), `connection_scoped=True`; the broker view's `connection_ref` is
  `<worker owner>:codex:<app-server epoch>`, so an elicitation (no item id) on a relaunched
  process is a different task even when the JSON-RPC id repeats.
- `replay_strategy = park_for_reconciliation`; `generation` = the turn request's generation;
  `policy_digest` = the execution's binding digest (`HarnessRequest.binding_digest` =
  `harness_execution.intended_binding_digest`, the value `PostgresApprovalContextProbe`
  revalidates against; proven equal on real PostgreSQL).
- replies: allow → `{"decision":"accept"}` (never `acceptForSession` or amendments);
  `approve_edited` → JSON-RPC -32001; deny → `{"decision":"decline"}`; cancel or any system
  reply → `{"decision":"cancel"}`; permissions allow → `{"permissions": <requested>}`, else
  -32002; user input answers → `{"answers": {...}}`, else -32002; elicitation →
  `{"action": accept|decline|cancel}` with `content` on accept.
- questions: Codex `questions[]` vs MP-11's single `QuestionPrompt`: one bound task per
  question (`tool_call_ref = <itemId>:q:<id>`), all bound before any wait, answered together
  only when every one is answered; any other outcome refuses the request (-32002). An
  `isSecret` question refuses the request without a task.

## Offline and real-service evidence (no provider, no credential, no paid call)

| Suite | What it proves |
| --- | --- |
| `tests/unit/codex/test_schema_pin.py` | the committed schema matches `PIN.json`; the method registries equal the generated registry files; outbound params, every native reply mapping and every fixture-emitted event validate against the pinned JSON Schema; the approval-policy contract equals the pinned `AskForApproval` |
| `tests/unit/codex/test_transport.py` | JSON-RPC correlation: out-of-order responses, separated notification/server-request stream, lost response (`ResponseLost`, nothing re-sent), disconnect (`TransportClosed`), bounded buffer (`CursorExpired`), stdio framing |
| `tests/unit/codex/test_harness.py` | full turn through `LaneTurnService`; queued sends never become steering; steer applied vs `stale_target`; interrupt with observed terminal completion; lost `turn/start` reconciled from history; disconnect → reattach from history; capacity refusal journaled `declined`; describe cells implemented; credential-free child env; Windows selector loop refused |
| `tests/unit/codex/test_segments.py` | failure B: every `max_frames` bound 1..14 settles with exactly one RUN_RESULT; a cursor past a finished turn resynchronizes; a new turn is observed from its own `turn/start`; a relaunch resync pages history at budgets 1..3 |
| `tests/unit/codex/test_approvals.py` | MP-11 broker over the in-memory FIXTURE stores: bound before the wait, the mapping above, decisions → pinned replies, fail closed without a broker, pause binds then holds, the pump keeps absorbing during a human wait, wait expiry expires the correlation not the task, questions, secret refusal, credential-collecting elicitation refusal, recover on relaunch |
| `tests/unit/codex/test_compaction.py` | `CompactingLane` / `ContextOccupancyLane` isinstance; occupancy never from `total`; compaction refused mid-turn and reported on an idle thread; soft pressure → native compaction through `LaneTurnService` with a continuation coordinator |
| `tests/unit/codex/test_launcher.py` | no bootstrap import in `adapters/codex`; version pin refusal (and the override); `rows` → required executables; `compose_codex_local` signature |
| `tests/integration/codex/test_codex_approvals_postgres.py` | real PostgreSQL 17 (127.0.0.1:55433, migration chain through 0033): task + live correlation rows before the decision, `approved` through the production `PostgresApprovalContextProbe` (policy digest = `harness_execution` binding digest), pinned native answer, `lost` correlation after a relaunch |
| `tests/integration/temporal/test_mp08_codex_lane_turns.py` | real local Temporal (127.0.0.1:7233) + disposable PostgreSQL 17: `OperationWorkflow` drives `lane.turn` segments over the FIXTURE app-server; full turn; reattach after a mid-turn app-server death; bounded segments (`max_frames=3`) complete; every history replayed with `Replayer` |

The app-server in every suite is `tests/unit/codex/fixture_app_server.py` (labelled FIXTURE)
replaying `tests/fixtures/provider_frames/codex/scripts/*.jsonl` (first line `_fixture`,
`recorded: false`). They prove Mission Control's reading of the pinned schema, not Codex.

## Worker constraint: Linux/WSL

The worker spawns `codex app-server` with `asyncio.create_subprocess_exec`. The production
worker on Windows runs a `SelectorEventLoop`, which cannot spawn (`launcher.subprocess_supported`
refuses with `HostUnsupported`, code `UNSUPPORTED_BEHAVIOR`); the event-loop policy is not
changed globally for one lane (VALIDATION "Local Temporal fallback"). Run the `codex` lane on a
Linux or WSL worker (`worker.linux.wsl` host profile in the binding) and make the refusal visible
in preflight on any other host.

## The bounded live drill (owner-run, paid; NOT run by MP-08)

Runner: `tests/integration/codex/test_codex_live_qualification.py` with
`tests/integration/codex/live_drill.py`. It is skipped unless every precondition holds:

1. A Linux/WSL worker with `codex` on `PATH` at exactly `codex-cli 0.162.0` (`codex --version`;
   `MC_CODEX_BINARY` names another binary path) and `git`; a different pin needs a regenerated
   schema and a re-run of the offline suites first (the launcher refuses it anyway).
2. An explicit auth route `MC_CODEX_AUTH_ROUTE`: `owner_cli_login` (the owner's `codex login` in
   the owner's `CODEX_HOME`, given as `MC_CODEX_OWNER_HOME`; nothing is copied) or `api_key`
   (`OPENAI_API_KEY`), admitted by MP-05 from the owner's profile document
   (`MISSION_CONTROL_AUTH_PROFILES_PATH`) and profile id (`MC_CODEX_AUTH_PROFILE`).
3. A finite, owner-approved budget recorded in a Linear comment on OVE-71 and exported as
   `MC_PAID_BUDGET_USD=<amount>` together with `MC_LIVE_CODEX_QUALIFICATION=1`. The runner caps
   itself at 4 paid turns; Codex reports no cost, so the record carries turns and estimated
   tokens and the paid amount as `unknown`.

Exact command (from the repository root on the Linux/WSL worker):

```bash
MC_LIVE_CODEX_QUALIFICATION=1 MC_PAID_BUDGET_USD=<owner-approved amount> \
MC_CODEX_AUTH_ROUTE=owner_cli_login MC_CODEX_OWNER_HOME="$HOME/.codex" \
MISSION_CONTROL_AUTH_PROFILES_PATH=<owner profile document> MC_CODEX_AUTH_PROFILE=<profile id> \
uv run --no-sync --group biotech --group dbcontract pytest \
  tests/integration/codex/test_codex_live_qualification.py -rs -p no:cacheprovider
```

After the integrator applies the proposed `scripts/lane_qualify.py` / `Makefile` change, the
same drill is `make lane-qualify PROFILE=codex LIVE=1` (with the variables above), which runs
`tests/unit/codex` and the describe-honesty suite first.

What the runner does (production composition: `compose_codex_local`, the pinned
`SubprocessAppServerLauncher` behind a recording tee, MP-05 admission, MP-11 `ApprovalBroker`
over the in-memory stores with the owner's scripted decisions, recorded as such):

1. **Session A, one turn through `LaneTurnService`** (`approvalPolicy=untrusted`): launch and
   configuration under the materialized project trust; approval requests bound before any
   answer, one declined then approved (`serverRequest/resolved` frames); the project subagent
   `.codex/agents/mc-drill-helper.toml` whose child thread must surface as `subordinate_ref`
   frames; the `collabAgentToolCall` field names; settled usage.
2. **Session B, steer / interrupt / compaction**: a long turn steered with the exact
   `expectedTurnId`, then `turn/interrupt` with the terminal `turn/completed` observed (latency
   measured); a late steer's refusal shape; the occupancy against `modelContextWindow`;
   `thread/compact/start` and its report.
3. **Session B, restart classification**: SIGTERM to the app-server mid-turn, `reattach`
   (relaunch, MP-11 `recover`, `thread/resume`), what Codex did with the turn, and whether
   `UserMessageThreadItem.clientId` echoes `clientUserMessageId` (history lookup with the live
   journal cleared).

It records every app-server event (scrubbed of bound secrets and e-mail addresses) under
`tests/integration/codex/recordings/<date>/drill.jsonl` and writes
`docs/qualification/lanes/codex-<date>.md` with the auth admission, the observed capability
matrix, checks, measurements, spend and each UNVERIFIED item as verified, refuted or open. Only
then may a reviewed change set `qualified=True` in `application/execution/harness/describe.py`
with a release migration row.

## UNVERIFIED items the drill settles

| Item (record key) | Offline status | How the drill settles it |
| --- | --- | --- |
| `project_trust`: project `.codex/` layers load via `CODEX_HOME/config.toml` `[projects]` / `-c` override | written, not loaded by a CLI | session A: the project subagent runs |
| `client_user_message_id_echo`: `clientUserMessageId` echoed as `UserMessageThreadItem.clientId` | assumed from the schema | session B restart: history lookup finds the send |
| `limit_error_data`: JSON-RPC error data carries `codexErrorInfo` on limit refusals | assumed; message text is never parsed | never provoked; only an observed refusal settles it |
| `history_after_restart`: `thread/read includeTurns` returns the turn after a process restart | fixture only | session B restart |
| `steer_refusal_shape`: `turn/steer` refusal when the turn completed first | fixture error | session B late steer |
| `collab_agent_tool_call_fields`: `senderThreadId`, `receiverThreadIds` | not confirmed (MP-13) | session A recorded items |
| `approval_rpc_coverage`: which approval RPCs this configuration sends | schema only | session A served requests |
| `compaction_report`: `thread/compacted` and/or `contextCompaction` after `thread/compact/start` | fixture only | session B compaction receipt |
| `occupancy_reading`: `last.totalTokens` vs `modelContextWindow` is the occupancy | assumed | session B values recorded against the CLI's own meter (stays `open` until compared) |

The contract no longer admits `approval_policy=on-failure` (absent from the pinned
`AskForApproval`): a binding naming it is refused at validation.
