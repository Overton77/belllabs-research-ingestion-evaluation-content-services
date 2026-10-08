---
type: Workflow
title: Snapshots forks and reconciliation
description: How recovery preserves immutable lineage and distinguishes uncertain work, including forks that carry a queued instruction and restore a Cursor Local lane snapshot, and what a Cursor Cloud fork still lacks.
tags: [mission-control, implementation]
---

# Snapshots forks and reconciliation

Snapshots capture validated safe boundaries and immutable manifests. A fork names
the exact snapshot digest, applies admitted patch policies, records lineage and
reuses only work that remains valid. Changed work is rerun through the normal
operation engine. Fork admission is scoped and idempotent; it is not a direct copy
of provider state into authoritative completion.

Checkpoint lineage is evidence. Unknown or foreign checkpoints cannot silently
become trustworthy results. The existing incident path classifies uncertain units
as in doubt and requires an authorized reconciliation decision. Actions retain
generation fences and settlement receipts.

The public runtime adapter delegates to real snapshot, fork and reconciliation
services. It does not synthesize a successful retry or expose an unimplemented
generic retry command. Immediate intervention semantics are limited to mapped,
supported controls.

## Forks seeded with an instruction and a lane snapshot

A fork never copies the source run's mailbox, children or in-flight commands
(REQ-CP-EXEC-012). After the fork saga admits the derived run, `application/recovery/fork_seed.py`
writes into the derived run's own [command mailbox](events-and-commands.md) two ordinary,
reducer-admitted commands with ids derived from the fork request, so a retried fork re-reads
them: the optional operator instruction as a `queue_instruction` (boundary `next_turn`,
attributed to the forking actor), and the Snapshot restore as a kernel `add_context` with
`expand: workspace`, which the first boundary seals into a `fork`-purpose Context Packet as its
single `workspace` item ([context and continuation](context-and-continuation.md)). Nothing is
launched: `run start` stays separate. Surfaces: `POST /runs/{id}/forks` (the latest safe Snapshot
by default, optional instruction), `missionctl run fork --instruction-file`, MCP
`mission_run_fork`; sponsorship and approvals are the principal's own claims, never the
request's. A fork is findable with `run list --query "forked_from='<run>'"` through the
`ForkedFromRunId` search attribute.

Restore is lane specific. `cursor_local` freezes a lease as `mc.cursor_snapshot.v1` and
`end_session` records the `cursor-snapshot:<ref>` on the released lease; the Snapshot service
puts that ref first in `sandbox_snapshot_refs` (else `run-snapshot://<id>`, which the lane
refuses with `CHECKPOINT_INVALID`), and the derived run's first lease re-applies the patch and
files with every digest verified ([cursor lane](cursor-lane.md)). Deep Agents runs have no
leases and their snapshots are unchanged. `cursor_cloud` fork restore is not built: the lane
expects a `branch:<branch>@<sha>` ref that nothing records yet.

A failed operation may retry according to its exact execution contract. That is
different from inventing a new mission-level command or replaying uncertain side
effects. Review effect/journal state before changing retry or recovery behavior.

# Citations

- [Public runtime service](../../src/mission_control/application/missions/runtime.py).
- [Fork saga](../../src/mission_control/application/recovery/run_forks.py).
- [Fork persistence](../../src/mission_control/adapters/postgres/runtime/run_forks.py).
- [Fork seed](../../src/mission_control/application/recovery/fork_seed.py).
- [Cursor snapshot and restore](../../src/mission_control/adapters/cursor/snapshot.py).
- Spec: [SPEC-06](../specs/fast-track-2026-10/SPEC-06-interventions-inspection-subscriptions.md) (Fork),
  [ADR-0032](../adr/0032-interventions-real-queue-inject-cancel-fork-and-subscriptions.md).
- [Real fork and reconciliation tests](../../tests/unit/run_control/test_mission_control_runtime.py).
- [Fork with instruction](../../tests/unit/run_control/test_ft_f4_fork_instruction.py),
  [fork in PostgreSQL](../../tests/integration/postgres/test_ft_f4_fork_postgres.py),
  [lane controls in PostgreSQL](../../tests/integration/postgres/test_ft_g4_lane_controls_postgres.py).
