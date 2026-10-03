---
type: Workflow
title: Snapshots forks and reconciliation
description: How recovery preserves immutable lineage and distinguishes uncertain work.
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

A failed operation may retry according to its exact execution contract. That is
different from inventing a new mission-level command or replaying uncertain side
effects. Review effect/journal state before changing retry or recovery behavior.

# Citations

- [Public runtime service](../../src/mission_control/application/missions/runtime.py).
- [Fork saga](../../src/mission_control/application/recovery/run_forks.py).
- [Fork persistence](../../src/mission_control/adapters/postgres/runtime/run_forks.py).
- [Real fork and reconciliation tests](../../tests/unit/run_control/test_mission_control_runtime.py).
