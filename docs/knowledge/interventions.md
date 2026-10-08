---
type: Concept
title: Interventions
description: The command mailbox for queued instructions and added context, interrupt and inject by lane delivery semantics, immediate cancel with a persisted Stop Fence, and request_continuation, with their delivery reports and what is and is not guaranteed.
tags: [mission-control, commands, mailbox, stop-fence, interventions, implementation]
---

# Interventions

An [Intervention](../../GLOSSARY.md) is a privileged operator [Command](../../GLOSSARY.md)
whose delivery semantics are declared by the lane and reported back in a
[Delivery Report](../../GLOSSARY.md). Receipt states, endpoints and events are in
[events and commands](events-and-commands.md); the lane matrices are in
[lanes and harness](lanes-and-harness.md). Decisions: ADR-0008 and ADR-0032.

**Mailbox** (FT-F1, `domain/policies/mailbox.py`, `application/execution/mailbox.py`,
`adapters/postgres/run_control/mailbox.py`, migration 0029). `queue_instruction` and
`add_context` are admitted as pending Commands and each writes one durable
`command_mailbox` entry for the run's current generation in its own `mailbox:<generation>`
space, in the transaction that admits the Command. The family boundary (Stage Graph
admission of the next operation, Goal Loop iteration boundary) claims undelivered entries in
admission order, once per delivery key, and hands them to the packer as mandatory
`queued_instruction` items ([context and continuation](context-and-continuation.md)); the lane
marks them consumed when the carrying turn starts. Entries are never deleted: a cancel
supersedes, a moved generation expires with `stale_generation`, a passed deadline with
`deadline_passed`. Boundaries are `next_turn` and `next_iteration`.

**Interrupt and inject** (FT-F2, `application/execution/harness/inject.py`). By the lane's
declared semantics: `cooperative_inject` (no first-wave lane), `cancel_and_replace` (Deep Agents
and both Cursor profiles: cancel the running turn, settle uncertain effects within a grace
period or park the unit `in_doubt` with an incident and run no replacement, then run a
replacement turn on the same session from a `follow_up_turn` packet), or `unsupported` (typed
rejection, the turn continues).

**Immediate cancel and the Stop Fence** (FT-F3, `domain/policies/stop_fence.py`,
`application/execution/stop_fence.py`, `adapters/postgres/run_control/stop_fence.py`).
`cancel` with `urgency: immediate` persists an insert-only fence for the run and generation
before any provider cancel; every Kernel Hook (Deep Agents `wrap_tool_call`, the Cursor hook
callback) asks the fence before admitting an effect and a fenced admission is denied
`STOP_FENCED`. Fence writes and effect admissions of one run serialize on an advisory lock, so
an effect admitted before the fence stays admitted and one after it is denied. It does not halt
a tool already dispatched, and every report says so. Permission: `workflow_run.admin` while
side-effecting work may be active, `workflow_run.cancel` otherwise. The report has four
separate timestamps (requested, fence persisted, provider acknowledged, settled) at
`GET /runs/{id}/stop-fence` and in inspection. On `cursor_cloud` there is no fail-closed
Kernel Hook, so the fence reaches the run only through the API cancel
([cursor lane](cursor-lane.md)).

**Request continuation** is admitted only where continuation is composed and recorded as
`session.continuation_requested`; nothing seals or transfers
([context and continuation](context-and-continuation.md)).

# Citations

- Spec: [SPEC-06](../specs/fast-track-2026-10/SPEC-06-interventions-inspection-subscriptions.md).
- ADRs: [0008](../adr/0008-ordered-commands-with-urgent-stop-fence.md),
  [0032](../adr/0032-interventions-real-queue-inject-cancel-fork-and-subscriptions.md).
- Code: [mailbox policy](../../src/mission_control/domain/policies/mailbox.py),
  [mailbox service](../../src/mission_control/application/execution/mailbox.py),
  [mailbox store](../../src/mission_control/adapters/postgres/run_control/mailbox.py),
  [stop fence policy](../../src/mission_control/domain/policies/stop_fence.py),
  [stop fence gate](../../src/mission_control/application/execution/stop_fence.py),
  [stop fence store](../../src/mission_control/adapters/postgres/run_control/stop_fence.py),
  [stop fence route](../../src/mission_control/interfaces/http/stop_fence.py),
  [interrupt and inject](../../src/mission_control/application/execution/harness/inject.py),
  [mission service](../../src/mission_control/application/missions/service.py).
- Tests: [mailbox](../../tests/unit/run_control/test_ft_f1_command_mailbox.py),
  [mailbox in PostgreSQL](../../tests/integration/postgres/test_command_mailbox_postgres.py),
  [mailbox on Temporal](../../tests/integration/temporal/test_command_mailbox.py),
  [interrupt and inject](../../tests/unit/operations/test_ft_f2_interrupt_and_inject.py),
  [inject in PostgreSQL](../../tests/integration/postgres/test_ft_f2_inject_postgres.py),
  [inject on Temporal](../../tests/integration/temporal/test_ft_f2_inject.py),
  [stop fence](../../tests/unit/run_control/test_stop_fence.py),
  [stop fence in PostgreSQL](../../tests/integration/postgres/test_stop_fence.py),
  [immediate cancel on Temporal](../../tests/integration/temporal/test_ft_f3_immediate_cancel.py),
  [inspection](../../tests/unit/run_control/test_ft_f6_inspection.py).
