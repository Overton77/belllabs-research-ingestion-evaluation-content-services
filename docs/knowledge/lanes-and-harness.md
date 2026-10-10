---
type: Concept
title: Lanes and the harness protocol
description: The provider-neutral AgentHarness protocol and lane registry, the seven lane profiles and their v1 and v2 describe matrices, the lane.turn segment loop that drives every Session Lane on its binding's task queue, attempt recording and output custody, the Deep Agents lane behind it and the qualification flag. Cursor profiles are in cursor-lane, Claude and Codex in provider-lanes; session ownership, the dispatch journal and auth routes in session-ownership-and-dispatch.
tags: [mission-control, harness, lanes, deep-agents, agent-host, implementation]
---

# Lanes and the harness protocol

A [Harness](../../GLOSSARY.md) is the protocol; a [Lane](../../GLOSSARY.md) is one
qualified implementation of it; a [Lane Profile](../../GLOSSARY.md) is one placement of a
lane with its own control matrix; an [Agent Host](../../GLOSSARY.md) is a tool a human
works in that reads generated configuration. A host is where authoring happens; a lane
is where mission work executes. Keep the three apart when reading code, because the
package directory for the Deep Agents lane is called `adapters`.

## The protocol as implemented

`application/execution/harness/protocol.py::AgentHarness` is `describe` plus nine
operations: `prepare`, `start`, `reattach`, `send_turn`, `cancel_turn`, `observe`,
`snapshot`, `usage`, `end_session`. A lane that declares an operation `unsupported` or
`unqualified` in its describe raises `HarnessUnsupported` for it; any other error from
such an operation is a conformance failure. `observe` resumes from an opaque lane cursor
(LangGraph checkpoint id, bridge offset, SSE event id). The wire contracts are in
`domain/execution/lanes.py`: `mc.lane_describe.v1` and `.v2`, the request and handle contracts
and `mc.cursor_binding.v1`; `domain/execution/bindings.py` adds `mc.execution_binding.v2`,
`mc.environment_binding.v1` and `mc.workspace_snapshot.v1`, and `domain/execution/approvals.py`
`mc.approval_binding.v1` (MP-01, [ADR-0035](../adr/0035-seven-lane-profiles-versioned-provider-contracts-and-mission-v2.md),
`proposed`). Handles carry native identity, never credentials. `host_support.LaneProfile` is the
one profile vocabulary, seven values: `deep_agents`, `cursor_local`, `cursor_cloud`,
`claude_agent_sdk`, `codex`, `claude_cloud` and `codex_cloud`.

`application/execution/harness/describe.py` holds the declared matrix of each profile in
`DECLARED_LANE_MATRICES` (adapter `describe.py` modules only re-export): `deep_agents` keeps its
digest-stable `mc.lane_describe.v1` matrix (`V1_LANE_PROFILES`); `cursor_local`, `cursor_cloud`,
`claude_agent_sdk` and `codex` have implemented `mc.lane_describe.v2` matrices with per-feature
evidence (`V2_IMPLEMENTED_LANE_PROFILES`); `claude_cloud` and `codex_cloud` are v2 stubs whose
every control is `unqualified` (`V2_STUB_LANE_PROFILES`). Convention since the 2026-10-09
recovery: an implemented cell says how it is supported (`native` or `emulated`) with
`qualified=False`; `unqualified` only where nothing is implemented. Each control is
`native`, `emulated`, `unsupported` or `unqualified`, with per-command delivery
semantics, identity map, hook mechanism and events, instruction channels, subagent form,
usage dispositions and placement. `GET /v1/applications/{app}/lanes[/{profile}]`
(`interfaces/http/lanes.py`) and `missionctl lane list|describe` read them.

## Registry, admission and qualification

`application/execution/harness/registry.py::LaneRegistry` maps a profile to its harness
and caches describes. The worker composition (`adapters/temporal/deployment_composition.py`)
registers `deep_agents` always, the two Cursor profiles when `CURSOR_API_KEY` is bound, and
`claude_agent_sdk` / `codex` when their opt-in and an auth profiles path are set on a non-Windows
worker (`registered_lane_profiles`, `compose_lane_registry(claude_local=, codex_local=)`;
[provider lanes](provider-lanes.md)). No harness exists for `claude_cloud` or `codex_cloud`. The
API registers describe-only entries (`describe_only_registry(claude_bound=, codex_bound=)`) so
`lane list` works without executing.
Admission refuses a profile whose describe is not `qualified` unless policy allows
unqualified lanes (`MISSION_CONTROL_ALLOW_UNQUALIFIED_LANES`, a local-proof flag), and
the refusal names that remedy. Only `deep_agents` is `qualified=True` (WP-CP-040 parity
suite). Every other profile is `qualified=False`: nothing but a reviewed release citing a
live-drill record under `docs/qualification/lanes/` may flip that
([release-and-qualification](release-and-qualification.md)). Lane profile rows live in
`mission_control.lane_profile`: migration 0030 seeded three, 0031 adds four v2 stubs, and 0033
regenerates the four implemented v2 rows from `DECLARED_LANE_MATRICES`, all unqualified. Per-profile
status is in [qualification](qualification.md).

## One lifecycle synthesis: lane.turn

ADR-0031: the operation workflow schedules `lane.turn` segments, with `lane.status` to
reconcile and `lane.cancel` (idempotent) to stop. `application/execution/harness/lane_turns.py`
(`LaneTurnService`) admits the attempt at the operation boundary, then on `start` prepares,
starts and sends the turn, recording the native session and turn on the harness execution
before observing (`adapters/postgres/lanes/execution_state.py`, migration 0030); on
`resume` it reattaches and never sends again, because a lost native turn makes the unit
`in_doubt`. Every observed [Provider Frame](../../GLOSSARY.md) is persisted through the
FrameSink before the provider cursor is heartbeated, so persisted frames are the resume
truth and the throttled heartbeat only a hint
([events-and-commands](events-and-commands.md)). A segment ends at its bound or at a
terminal frame whose closing facts end the session and settle the operation once. The
activities are `adapters/temporal/activities/lane_turn.py`, registered on the
`-agent-cognitive` queue beside `operation.execute` and `operation.cancel`. Each segment also
claims fenced session ownership, journals every native dispatch and admits it against the Stop
Fence, and a provider capacity limit becomes a bounded wait
([session ownership and dispatch](session-ownership-and-dispatch.md)).

Session Lane units (`SESSION_LANE_RUNTIMES = {cursor, claude, codex}`,
`domain/execution/contracts.py`) always run through the segment loop with segment bounds from their
own binding, and `lane.*` runs on the binding's task queue (`OperationWorkflowRequest.activity_task_queue`
reads `provider_binding.task_queue`); a dedicated queue is served only when listed in
`MISSION_CONTROL_LANE_TASK_QUEUES` (`adapters/temporal/worker.py`). Admission records the attempt
row the frames hang off (`admit_lane_session(attempt=)`). A settlement carries the final JSON
answer as the Completion Candidate and registers declared `outputs/` files as
`workspace-candidate://` refs (`application/execution/operations/lane_outputs.py`). Deep Agents units still run `operation.execute`
unless `MISSION_CONTROL_LANE_SEGMENT_LOOP=true`, which moves them onto `lane.turn` with the
governed body unchanged (`OperationExecutionActivities.run_governed`); the default is
`false` (`bootstrap/settings.py`). Replay histories of the lane activities are captured under
`tests/integration/temporal/histories/ft_lanes/` and replayed by
`test_lane_replay_histories.py`.

## The Deep Agents lane

`adapters/deep_agents/adapter.py::DeepAgentRuntimeAdapter` is the sole production
`create_deep_agent` composition root and `DeepAgentsHarness` wraps it behind the protocol
without changing behavior. Materialization, frames, hook middleware, compaction observation,
hosted subordinates and the generated Agent Host configuration are in
[Deep Agents lane](deep-agents-lane.md).

## Other lanes and what is not built

- Cursor: [cursor lane](cursor-lane.md). Claude Agent SDK and Codex: [provider lanes](provider-lanes.md).
  The production launch author seals bindings for all five local profiles
  ([mission manifest](mission-manifest.md)); none is live-qualified.
- `claude_cloud` and `codex_cloud` are Outcome 3, revalidated 2026-10-09: the hosted products lack
  documented lifecycle operations ([claude_cloud](../qualification/lanes/claude_cloud/REVALIDATION-2026-10-09.md),
  [codex_cloud](../qualification/lanes/codex_cloud/REVALIDATION-2026-10-09.md)); the environment
  resolver (`application/environments/resolver.py`), the workspace allocator and launch refuse them.
- Direct Model lanes are not built.

# Citations

- Spec: [fast-track SPEC-07](../specs/fast-track-2026-10/SPEC-07-harness-and-cursor-lane.md);
  `../mission-control-general/general-mission-control/RUNTIME-CONTRACTS.md`
  (harness interface); `../mission-control-general/workflow-types/05-EXECUTORS_AND_DURABLE_CONTROLS.md`.
- ADRs: [0005](../adr/0005-deep-agents-first-extend-not-fork.md),
  [0006](../adr/0006-agent-server-required-execution-host-not-scheduler.md),
  [0017](../adr/0017-agent-server-runtime-persistence-separate-database.md),
  [0018](../adr/0018-coding-agents-are-execution-lanes.md),
  [0019](../adr/0019-coding-lanes-worker-hosted-local-first.md),
  [0030](../adr/0030-cursor-lane-two-profiles-reducer-from-frames-hydrated-fork.md),
  [0031](../adr/0031-temporal-lifecycle-synthesis-observe-activity-updates-seeded-forks.md).
- Code: [protocol](../../src/mission_control/application/execution/harness/protocol.py),
  [describe matrices](../../src/mission_control/application/execution/harness/describe.py),
  [registry](../../src/mission_control/application/execution/harness/registry.py),
  [lane turns](../../src/mission_control/application/execution/harness/lane_turns.py),
  [lane contracts](../../src/mission_control/domain/execution/lanes.py),
  [binding contracts](../../src/mission_control/domain/execution/bindings.py),
  [lane turn payloads](../../src/mission_control/domain/execution/lane_turns.py),
  [lane activities](../../src/mission_control/adapters/temporal/activities/lane_turn.py),
  [lane state store](../../src/mission_control/adapters/postgres/lanes/execution_state.py),
  [lane routes](../../src/mission_control/interfaces/http/lanes.py),
  [worker lane composition](../../src/mission_control/adapters/temporal/deployment_composition.py),
  [lane task queue workers](../../src/mission_control/adapters/temporal/worker.py),
  [lane outputs](../../src/mission_control/application/execution/operations/lane_outputs.py).
- Tests: [lane contracts](../../tests/unit/harness/test_lane_contracts.py),
  [multi-provider contracts](../../tests/unit/harness/test_multi_provider_contracts.py),
  [registry](../../tests/unit/harness/test_lane_registry.py),
  [dispatch](../../tests/unit/harness/test_lane_dispatch.py),
  [lane turn service](../../tests/unit/harness/test_lane_turn_service.py),
  [describe honesty](../../tests/unit/harness/test_describe_honesty.py),
  [lane state in PostgreSQL](../../tests/integration/postgres/test_lane_execution_state.py),
  [lane bindings in PostgreSQL](../../tests/integration/postgres/test_lane_bindings.py),
  [lane turn on Temporal](../../tests/integration/temporal/test_lane_turn.py),
  [lane replay histories](../../tests/integration/temporal/test_lane_replay_histories.py),
  [segment loop acceptance](../../tests/acceptance/mission_control/test_ft_g2_segment_loop.py),
  [Session Lane routing](../../tests/unit/operations/test_session_lane_routing.py),
  [lane task queue workers](../../tests/unit/runtime/test_lane_task_queue_workers.py),
  [lane final text](../../tests/unit/harness/test_lane_final_text.py).
