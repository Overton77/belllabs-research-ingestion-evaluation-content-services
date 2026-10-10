---
type: Verification Reference
title: Claude Agent SDK lane qualification
description: What the local claude_agent_sdk lane profile implements, what is qualified, what the owner's account enables, the offline and real-service evidence, and the owner-run bounded live drill that records real fixtures before the profile can be qualified.
tags: [mission-control, qualification, lanes, claude]
---

# Claude Agent SDK lane qualification (MP-07 / OVE-70)

`lane_profile.qualified` is `false` for `claude_agent_sdk`. It flips only through a reviewed
release that cites a record in `docs/qualification/lanes/` written by the live drill
(`claude_agent_sdk-<date>.md`, front matter `outcome: qualified`, `evidence: live_drill`).
Fixture and local-service tests never flip it. No live Claude call was made while building or
integrating this lane.

Pins: `claude-agent-sdk==0.2.165` (Python), bundled Claude Code `2.1.294`
(`claude_agent_sdk._cli_version`). The adapter is `src/mission_control/adapters/claude/`; the
declared matrix is `application/execution/harness/describe.CLAUDE_AGENT_SDK_DESCRIBE`
(re-exported by `adapters/claude/describe.py`, which cites the SDK source behind each cell).

## Implemented, qualified, account-enabled

"Implemented" means the adapter code exists and is exercised by fixtures. "Qualified" means a
recorded live drill for this exact profile and pin proved it. "Account-enabled" means the
owner's bound account/route exposes it. None of the three proves another.

| Control / feature | Implemented | Qualified | Account-enabled | SDK surface (0.2.165) |
| --- | --- | --- | --- | --- |
| `prepare` (MP-04 lease, packet, MP-03 projection, state root) | yes (`native`) | no | n/a | `materialize_projection`; no SDK call |
| `start` (one subprocess per session, explicit child env) | yes (`native`) | no | unknown | `ClaudeSDKClient.connect`, `transport.py` |
| `send_turn` (client-stamped user `uuid` = native turn ref) | yes (`native`) | no | unknown | `ClaudeSDKClient.query(AsyncIterable)` |
| `observe` (session log, every `Message` type mapped) | yes (`native`) | no | unknown | `receive_messages`, `types.Message` |
| `cancel_turn` (interrupt, then drain the aborted response) | yes (`native`) | no | unknown | `interrupt`; `ResultMessage.terminal_reason` |
| `reattach` (live in-process; after death `resume=<id>` only without a running turn, after MP-11 `recover`) | yes (`emulated`) | no | unknown | `ClaudeAgentOptions.resume` |
| `status`, `usage` (tokens settled per turn; cost estimated) | yes (`native`) | no | unknown | `ResultMessage.usage`, `total_cost_usd` |
| `snapshot`, `end_session` (custody before release) | yes (`emulated` / `native`) | no | n/a | MP-04 allocator |
| `reconcile_dispatch` (MP-06) | yes | no | n/a | state-root `dispatch.json` |
| Production routing through `OperationWorkflow` (`provider_binding.task_queue`) | yes | no | n/a | n/a (real Temporal, see below) |
| Capacity refusal (`ProviderCapacityLimited`) | yes | no | unknown | `RateLimitEvent`, `api_error_status` |
| Kernel Hooks in-process (`mc.stop_fence`, `mc.operation_intent`, `mc.frame_capture`, `mc.usage`) | yes, fail closed | no | unknown | `ClaudeAgentOptions.hooks`, `PreToolUse` deny |
| Permission binding (`provider_permission` -> MP-11 durable approval task; bounded wait; `DenyWithoutGateway` fail-closed default) | yes (`approvals.BrokerPermissionBinding`) | no | unknown | `can_use_tool`, `ToolPermissionContext.tool_use_id` |
| Context occupancy (`ContextOccupancyLane`: `totalTokens` / `rawMaxTokens`) | yes | no | unknown | `ClaudeSDKClient.get_context_usage()` |
| Continuation into a fresh session (sealed checkpoint; no conversation fork) | yes (`continuation.py`; registration `qualified=False`) | no | unknown | a new `ClaudeSDKClient` (no `resume`, no `fork_session`) |
| Compaction observation (`PreCompact` -> `before_compaction` hook frame) | observation only | no | unknown | `types.HookEvent`; no `PostCompact` callback, no compaction control request |
| Subordinate lineage (`parent_tool_use_id`, `Task*Message`, `LaneFrame.subordinate_ref`) | yes (`lifecycle_only`) | no | unknown | MP-13 mapping |
| `pause` (`PreToolUse` defer), `fork` hydration | no (`unqualified`) | no | unknown | `permissionDecision: "defer"`, `fork_session` |
| `provider_question` (AskUserQuestion), `mcp_elicitation` | no (not claimed) | no | unknown | no user-question routing in the Python source; elicitation not forwarded (`types.McpSdkServerConfig`) |
| Auth routes | admission through MP-05; `owner_cli_login` is `policy_restricted` | no | **unknown** | `provider_child_environment` |
| Worker host | Linux / WSL 2 / macOS; Windows refused (`host.HostUnsupported`) | no | n/a | stdio subprocess |

Nothing above is account-enabled: no login, no `claude -p`, no status probe ran.

## Offline evidence (no network, no credentials)

| Suite | What it proves |
| --- | --- |
| `tests/unit/claude/test_claude_frames.py` | Every `claude_agent_sdk.types.Message` (parsed by the SDK's own `parse_message` from the labelled fixtures) maps to frames with the MP-13 keys and kinds; the SDK lifecycle facts with a kinds row are non-closing `STATUS`; a genuinely unmapped kind stays `UNKNOWN`, counted and non-closing; closing facts read `subtype` / `is_error` / `terminal_reason` / `api_error_status`, never the text; cost stays `estimated`. |
| `tests/unit/claude/test_claude_harness.py` | Describe honesty; a full turn through `lane.turn`; in-process resume from the cursor without a second send; history loss after process death is `NativeTurnLost` -> `in_doubt`; capacity refusal before any send; a dying subprocess ends with a synthesized `process_exit`; `reconcile_dispatch`; the child environment; the state root. |
| `tests/unit/claude/test_claude_controls.py` | The two-turn interrupt/replacement reads the intended response, never a leftover; Kernel Hooks deny on the Stop Fence and on disallowed tools; permission requests bind to `ApprovalBinding` (stable `tool_use_id`, not connection-scoped, `reissue_native_request`, the execution's binding digest) and default to deny without a broker; pause is a typed rejection. |
| `tests/unit/claude/test_claude_approvals.py` | `can_use_tool` through MP-11's `ApprovalBroker` + `HumanTaskService` (in-memory FIXTURE stores): approve, approve_edited (`updated_input`), deny (feedback, no interrupt), cancel (interrupt), bounded wait expiry (deny + interrupt, task stays open), policy/generation change and Stop Fence never allow, a reissued tool call replays only for the same input, `recover` closes the dead process's correlations before `resume`, wait bounds, composition refuses Windows and fails closed. |
| `tests/unit/claude/test_claude_continuation.py` | Occupancy from `get_context_usage` against the raw model window (`unknown` with the reason otherwise); a sealed continuation snapshot excludes the state root and hydrates a fresh SDK session (no `resume`, no `fork_session`) whose continuation turn runs through the production handover; the registration resolves this lane only and stays unqualified; the continuation service refuses this lane until its delivery row exists (integrator delta). |
| `tests/unit/claude/test_claude_transport.py` | The production transport spawns the CLI with exactly the admitted child environment plus the SDK markers, routes `can_use_tool` over `stdio`, and the host gate (injected platform facts) refuses Windows before anything is built. |
| `tests/unit/claude/test_claude_live_drill.py` | The paid drill refuses without every explicit precondition, its recorder drops control traffic and secrets, and the record it writes is what the qualification gate reads. |

Fixtures: `tests/fixtures/provider_frames/claude/*.jsonl` (first line `recorded: false`,
labelled FIXTURE) and `tests/unit/claude/fixtures.py` (`FixtureClient`, `FixtureWorkspace`).
They prove what the lane does with the pinned SDK's documented shapes, not live behaviour.

## Real local services (disposable PostgreSQL 17 at 127.0.0.1:55433, Temporal at 127.0.0.1:7233)

| Suite | What it proves |
| --- | --- |
| `tests/integration/temporal/test_mp07_claude_lane_turns.py` | A claude unit runs through the production `OperationWorkflow` (segment loop, `SEGMENT_LOOP_PATCH`): `activity_task_queue` routes every `lane.turn` to `provider_binding.task_queue`; frames and lane state in PostgreSQL under the runtime role; one live session across segments, one send; an error result settles `failed(capacity)`; every history replays with `Replayer`. |
| `tests/integration/claude/test_claude_broker_postgres.py` | The same production path with MP-11's PostgreSQL approval repositories (migration 0033): the native request waits on a persisted approval task, a reviewer resolves it through `HumanTaskService`, the production `PostgresApprovalContextProbe` revalidates (its policy digest and generation equal what the lane bound), the correlation is answered; an unanswered request expires its correlation while the task stays pending. |

Command (the DSN is injected by the workspace helper, never printed):

```
bash /c/Users/Pinda/Proyectos/BellLabs/.workspace-setup/mc-integration-20261009/with-test-db.sh \
  uv run --no-sync --group biotech --group dbcontract pytest -p no:cacheprovider -q \
  tests/integration/temporal/test_mp07_claude_lane_turns.py tests/integration/claude
```

## OS constraints

The worker that runs this lane must be a Linux / WSL 2 / macOS process: the SDK spawns the
Claude Code subprocess over stdio, and the Windows worker runs a `SelectorEventLoop` without
subprocess support. `adapters/claude/host.host_gate` refuses Windows where the lane is composed
(`compose.compose_claude_local`) and where it spawns (`transport.SdkClientFactory.create`) with
the typed `HostUnsupported` (`LANE_UNSUPPORTED_OS`). The unit and integration suites run
anywhere because the client is a fixture. `materialize_projection` checks launch executables
on the worker host when a rows resolver is composed (`rows=` on the harness).

## The bounded live drill (owner-run; not run)

Runner: `tests/integration/claude/test_claude_live_qualification.py` with
`tests/integration/claude/live_drill.py`. It is skipped unless every precondition holds and
names the missing ones in the skip reason:

1. `MC_LIVE_CLAUDE_QUALIFICATION=1` (the explicit opt-in; CI never sets it).
2. A finite budget approved by the owner (`MC_PAID_BUDGET_USD=<amount>`; `inf`, `0` or
   non-numbers refuse). The drill stops before each step once the SDK's estimated cost reaches
   it; the record keeps the estimate apart from the (unknown) settled bill.
3. An explicit auth route, `MC_CLAUDE_AUTH_ROUTE=api_key` or `owner_cli_login` (no default),
   admitted by MP-05 from the owner's profile document (`MISSION_CONTROL_AUTH_PROFILES_PATH`,
   `MC_CLAUDE_AUTH_PROFILE`); the drill fails if MP-05 admits a different route. `api_key`
   needs `ANTHROPIC_API_KEY` in the worker environment (it reaches the CLI only through the
   admitted child environment; the config dir moves under the lease). `owner_cli_login` is
   `policy_restricted` (Agent SDK docs): `MC_CLAUDE_OWNER_ATTESTATION_REF` must equal the
   profile's `owner_attestation_ref`; the lane keeps the owner's config dir and never copies a
   credential out of it.
4. A Linux / WSL 2 / macOS worker host.
5. Optional: `MC_LANE_DRILL_APPROVAL_URL` (the owner's Linear approval comment on OVE-70) is
   written into the record; `MC_CLAUDE_DRILL_MODEL` overrides the bound model id.

Command (once the integrator applies the `scripts/lane_qualify.py` profile, the same drill is
`make lane-qualify PROFILE=claude_agent_sdk LIVE=1`):

```
MC_LIVE_CLAUDE_QUALIFICATION=1 MC_PAID_BUDGET_USD=<amount> \
MC_CLAUDE_AUTH_ROUTE=<api_key|owner_cli_login> \
MISSION_CONTROL_AUTH_PROFILES_PATH=<profiles.json> MC_CLAUDE_AUTH_PROFILE=<profile id> \
MC_CLAUDE_OWNER_ATTESTATION_REF=<decision ref, owner_cli_login only> \
MC_LANE_DRILL_APPROVAL_URL=https://linear.app/overtonbell/issue/OVE-70#<comment> \
uv run --no-sync --group biotech --group dbcontract pytest -p no:cacheprovider -q -rs \
  tests/integration/claude/test_claude_live_qualification.py
```

What it drives (the production composition: `compose_claude_local`, the production
`SdkClientFactory` behind a recording tee, MP-05 admission, MP-11 broker):

1. One session, one turn through `LaneTurnService`: start, the client-stamped turn uuid, a
   Task subagent whose Bash call reaches `can_use_tool` and is bound to an approval Human Task
   (the drill's reviewer is the owner's scripted decision, `owner:drill`), subagent lifecycle
   frames, settled tokens and the estimated cost.
2. A second session: interrupt a long turn and replace it (drained `aborted_*` terminal
   reason, no leftover in the replacement) and read `get_context_usage` occupancy.
3. Simulated process death: a new harness runs MP-11 `recover`, resumes the session by id and
   runs one more turn on the resumed connection.

Outputs: scrubbed raw stream recordings under `tests/integration/claude/recordings/<date>/`
(first line `recorded: true`, secrets and e-mail addresses removed, control traffic dropped)
and `docs/qualification/lanes/claude_agent_sdk-<date>.md` with the auth route (names and
references only), the observed capability matrix, checks, measurements, paid units and the
items below. An ambiguous paid effect is `unknown` and never qualifies.

| Item | Offline status | How the drill settles it |
| --- | --- | --- |
| `client_uuid_is_turn_identity` | documented (`resume_drops_turn` docstring); fixture only | the stamped uuid appears in the recorded stream |
| `interrupt_terminal_reason` | documented; fixture only | the recorded interrupted turn |
| `rate_limit_event_shape` | documented; fixture only | only if a limit is hit (do not provoke one) |
| `resume_after_process_death` | documented; fixture only | step 3 |
| `can_use_tool_reaches_the_lane` | documented; fixture only | Bash outside `allowed_tools` in `default` mode |
| `pretooluse_deny_on_stop_fence` | documented; fixture only | not exercised by this drill (open) |
| `precompact_callback` | documented; not observed | only with a long session; open otherwise |
| `cost_is_an_estimate` | documented | compare the recorded estimate with the Console |
| `subagent_lifecycle_frames` | documented; fixture only | the recorded `Task*Message`s |
| `context_usage_control_request` | documented (`client.py`); fixture only | step 2 |
