---
type: Workflow
title: StageGraph and GoalDirected execution
description: How both program families share Temporal and bounded operation settlement.
tags: [mission-control, implementation]
---

# StageGraph and GoalDirected execution

StageGraph interprets a dependency graph and reconciles runnable stages as
operations settle. Unrelated work need not wait for a slow stage. GoalDirected
prepares executor/verifier iterations and reconciles durable results until its
existing convergence or stopping rules decide the next action.

Both families launch governed operations. The operation shell owns admission,
binding, journal/effect coordination, bounded cognition and immutable settlement.
Deep Agents receives an exact materialized binding; it cannot become a separate
mission scheduler or bypass the budget ledger.

New scoped roots use `mc.mission_run.v1`; scoped operation children use
`mc.operation.v1`. IDs include installation, application, tenant and run identity.
Family inputs must match their root's scope, ERC, workflow type and epoch.
Cross-parent operation requests fail before child launch.

Linked independent roots preserve admitted parent/child lineage. Hosted subordinate
runs have their own submission and usage receipts. A delegated response does not
itself terminalize the parent: the parent reconciles according to its contract.

# Citations

- [Mission root](../../src/mission_control/adapters/temporal/workflows/mission_run.py).
- [StageGraph workflow](../../src/mission_control/adapters/temporal/workflows/stagegraph.py).
- [GoalDirected workflow](../../src/mission_control/adapters/temporal/workflows/goal_directed.py).
- [Operation workflow](../../src/mission_control/adapters/temporal/workflows/operation.py).
- [Scoped identity tests](../../tests/unit/mission_control/test_temporal_identities.py).
- [Signed API runtime proof](../../tests/acceptance/mission_control/test_authenticated_scoped_runtime.py).
