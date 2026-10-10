---
type: Concept
title: Claude Agent SDK and Codex local lanes
description: The claude_agent_sdk and codex lane profiles as built - pinned SDK and app-server transports, worker composition behind opt-in settings and a Linux/WSL host gate, native approvals through the durable approval broker, continuation into a fresh session, Codex native compaction, output custody, and why both stay unqualified until an owner-run drill.
tags: [mission-control, lanes, harness, claude, codex, implementation, qualification]
---

# Claude Agent SDK and Codex local lanes

Two worker-hosted [Lane Profiles](../../GLOSSARY.md) added by the multi-provider packet (MP-07,
MP-08), integrated in the 2026-10-09 recovery session. Both implement the protocol in
[lanes and harness](lanes-and-harness.md), always run through the `lane.turn` segment loop, and
are `qualified=False`. They are local products on our own worker: neither counts toward the
provider-hosted `claude_cloud` / `codex_cloud` requirement.

## Claude Agent SDK (`claude_agent_sdk`)

`adapters/claude/` drives `claude-agent-sdk==0.2.165` (Python), which spawns its bundled Claude Code
CLI 2.1.294 over stdio, one subprocess per session with an explicit child environment
(`transport.py`). A turn's native identity is the client-stamped user message uuid; `observe`
maps every SDK `Message` to frames (`frames.py`); `cancel_turn` interrupts and drains the aborted
response; `reattach` is live in-process, or `resume=<session>` after a process death only when no
turn was running. Kernel Hooks run in-process as SDK callbacks and fail closed (`hooks.py`).
`reconcile_dispatch` reads the state-root dispatch record. `ContextOccupancyLane` reads
`get_context_usage()` against the raw model window. `PreCompact` is only observed, so compaction
control stays `unqualified`; `pause` and `fork` are `unqualified`; the SDK forwards no user
questions and no MCP elicitation, so `provider_question` and `mcp_elicitation` are not claimed.

## Codex (`codex`)

`adapters/codex/` speaks the app-server JSON-RPC protocol v2 of `codex-cli 0.162.0` over stdio
(`transport.py`, `protocol.py`; schema committed under `tests/fixtures/provider_frames/codex/schema/`
with `PIN.json`, bundle `sha256:0bf5254b…`). `launcher.py` probes `codex --version` and refuses any
other CLI with `PROVIDER_PIN_MISMATCH` unless `MISSION_CONTROL_CODEX_ALLOW_VERSION_MISMATCH`.
`turn/start` is sent only on an idle thread (`wait_then_send`); `turn/steer` implements
`SteeringLane` with `stale_target` refusals; `turn/interrupt` waits for the terminal
`turn/completed`. Live cursors are `turn@epoch:seq[/k]`, so a multi-frame event resumes exactly
(the recovered "failure B" livelock), and history resync pages `thread/read`. `CompactingLane.compact`
issues `thread/compact/start` on an idle thread: compaction control is `native`. The adapter
imports no bootstrap module; its child environment is injected.

## Composition

`adapters/temporal/provider_lane_composition.py` builds both lanes for a worker, each opt-in
(`MISSION_CONTROL_CLAUDE_LANE`, `MISSION_CONTROL_CODEX_LANE`, both requiring
`MISSION_CONTROL_AUTH_PROFILES_PATH`) and refused on Windows: the production
worker's `SelectorEventLoop` cannot spawn subprocesses (`adapters/claude/host.py`,
`launcher.subprocess_supported`), so the lanes need a Linux or WSL 2 worker. Each composition runs
MP-05 auth admission and builds the child environment through `provider_child_environment`, so an
API key never silently shadows a subscription route ([session ownership and dispatch](session-ownership-and-dispatch.md)).
`registered_lane_profiles` (`deployment_composition.py`) mirrors the opt-ins. Units route by `provider_binding.task_queue`; a
dedicated queue needs `MISSION_CONTROL_LANE_TASK_QUEUES`. Bindings are sealed at launch from the
`providers` section of `mc.manifest_launch_bindings.v2` ([mission manifest](mission-manifest.md)).

## Approvals

One `ApprovalBroker` per worker (connection ref = the session owner ref) binds each native
request to a persisted approval task before anyone waits ([Human Gates](human-gates.md)). Claude's
`can_use_tool` goes through `BrokerPermissionBinding` (keyed by `tool_use_id`,
`reissue_native_request`); Codex serves command, file-change and permission approvals, user-input
questions (one task per question; secret questions refused) and MCP elicitations, each in its own
task so the event pump never waits on a human, with connection ref `<owner>:codex:<epoch>`. Both
use the execution's binding digest as the policy digest that `PostgresApprovalContextProbe`
revalidates, wait at most `MISSION_CONTROL_APPROVAL_WAIT_S` (≤ 600 s) and run `recover` before a
resume. Without a broker both deny (`DenyWithoutGateway`, fail-closed replies).

## Continuation and outputs

Both register a hydrator and snapshot port with the MP-12 continuation stack
([continuation checkpoint](continuation-checkpoint.md)): a sealed checkpoint goes into a fresh
session or a fresh app-server thread (no conversation fork); snapshots exclude the state root and
`CODEX_HOME`. Delivery is `turn_boundary_guaranteed` for Claude and `wait_then_send` for Codex. The
final JSON answer is the Completion Candidate (`FinalTextLane.final_text`) and declared `outputs/`
files become digest-checked `workspace-candidate://` refs.

## Qualification

Evidence is FIXTURE clients plus real PostgreSQL and Temporal: production `OperationWorkflow`
routing with replay, broker tasks on release 1.2.0, continuation with worker loss, and the MP-20
parity rows. Owner-run drill runners exist (`tests/integration/{claude,codex}/test_*_live_qualification.py`)
and are skipped without an opt-in, finite budget, auth route and the right host
(`make lane-qualify PROFILE=claude_agent_sdk|codex LIVE=1`). Account enablement is unknown.

# Citations

- Spec: [SPEC-01](../specs/multi-provider-2026-10/SPEC-01-runtime.md),
  [SPEC-03](../specs/multi-provider-2026-10/SPEC-03-human-control.md);
  [MP-07](../specs/multi-provider-2026-10/issues/MP-07-implement-the-local-claude-agent-sdk-lane.md),
  [MP-08](../specs/multi-provider-2026-10/issues/MP-08-implement-the-local-codex-app-server-lane.md).
- ADRs: [0035](../adr/0035-seven-lane-profiles-versioned-provider-contracts-and-mission-v2.md),
  [0038](../adr/0038-two-origins-of-human-control-over-one-human-task-service.md),
  [0041](../adr/0041-continuation-is-a-persisted-phase-machine-driven-by-the-operation-workflow.md) (all proposed).
- Qualification: [claude_agent_sdk](../qualification/lanes/claude_agent_sdk/README.md),
  [codex](../qualification/lanes/codex/README.md),
  [release statement](../qualification/release/multi-provider-2026-10.md).
- Code: [Claude harness](../../src/mission_control/adapters/claude/harness.py),
  [Claude approvals](../../src/mission_control/adapters/claude/approvals.py),
  [Claude continuation](../../src/mission_control/adapters/claude/continuation.py),
  [Codex harness](../../src/mission_control/adapters/codex/harness.py),
  [Codex launcher](../../src/mission_control/adapters/codex/launcher.py),
  [Codex approvals](../../src/mission_control/adapters/codex/approvals.py),
  [Codex continuation](../../src/mission_control/adapters/codex/continuation.py),
  [provider lane composition](../../src/mission_control/adapters/temporal/provider_lane_composition.py),
  [approval broker](../../src/mission_control/application/execution/approvals_broker.py),
  [describe matrices](../../src/mission_control/application/execution/harness/describe.py).
- Tests: [Claude units](../../tests/unit/claude/test_claude_harness.py),
  [Claude approvals](../../tests/unit/claude/test_claude_approvals.py),
  [Codex units](../../tests/unit/codex/test_harness.py),
  [Codex segments](../../tests/unit/codex/test_segments.py),
  [Codex compaction](../../tests/unit/codex/test_compaction.py),
  [Claude on Temporal](../../tests/integration/temporal/test_mp07_claude_lane_turns.py),
  [Codex on Temporal](../../tests/integration/temporal/test_mp08_codex_lane_turns.py),
  [Claude broker in PostgreSQL](../../tests/integration/claude/test_claude_broker_postgres.py),
  [Codex approvals in PostgreSQL](../../tests/integration/codex/test_codex_approvals_postgres.py),
  [Codex continuation on Temporal](../../tests/integration/codex/test_codex_continuation_temporal.py).
