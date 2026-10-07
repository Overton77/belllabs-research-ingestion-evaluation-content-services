---
type: Concept
title: Budgets and usage
description: How a budget envelope is reserved before work and settled after, which rules the reducer enforces today, and where the spec's integer-micros and unknown-usage rules are still only specified.
tags: [mission-control, budgets, usage, settlement, implementation]
---

# Budgets and usage

A [Budget](../../GLOSSARY.md) is admitted as a ceiling, reserved before work and settled
after. The reducer owns every ledger transition; nothing else changes a balance. This
concept reads the rules from `domain/policies` and compares them with SPECIFICATION.md,
EXPERIENCE-AND-STREAMS and DEPLOYMENT-AND-RELEASE. Governors that stop work on depth,
rounds or iterations live in the program interpreters and are not covered here.

## Specified rules

Budget admission and child reservation are serialized in the application database.
Currency is integer micros; token, duration, tool and sandbox dimensions are recorded
separately. Reservations cover the maximum admitted exposure of calls, workspaces and
outstanding children; the remaining ceiling includes unsettled commitments. Unknown usage
cannot count as zero. Cancellation retains reservations until effects and costs settle.
Runs stop at a [Governor](../../GLOSSARY.md) with a governed outcome rather than retrying
for more funds (SPECIFICATION.md, Temporal and transactional execution). The expansion
adds a usage ledger keyed by `(provider, native_request_id, usage_component)` with a
settled, estimated or unknown disposition per dimension, exact rational unit rates in
micros, and `ProofBudget@1` for authorized live experiments
([release and qualification](release-and-qualification.md)).

## Envelope and state as implemented

`BudgetEnvelope` (`domain/policies/contracts.py`) declares at least one
`BudgetDimensionLimit`: a free-form dimension name matching `^[a-z][a-z0-9_.:-]*$`,
`applicability` of `bounded`, `unbounded` or `not_applicable`, an optional `soft_limit`
and a `hard_cap` that bounded dimensions must carry. Baseline reservations must name
declared, applicable dimensions. `BudgetState` holds `reserved`, `consumed` and
`pending_settlement` per dimension, reservations by id, usage and settlement records and
`outstanding_usage_ids`. Ledger kinds are `reservation`, `consumption`, `release`,
`pending_settlement`, `settlement` and `adjustment`. Amounts are plain integers; the code
does not fix micros as the money unit nor enumerate the spec's four dimensions, so a
binding chooses its own dimension names.

## Reserve, record, settle, terminalize

The reducer (`domain/policies/reducer.py`) grants `reserve_budget`, `record_usage` and
`settle_pending_usage` only to actors holding `workflow_run.reserve_budget`,
`workflow_run.report_usage` and `workflow_run.settle_usage`. A reservation is checked by
`_enforce_hard_caps` over reserved + consumed + pending and rejected as
`budget_hard_cap_exceeded`. `_record_usage` requires an existing reservation
(`reservation_required`), draws actual, pending-external and release amounts from it,
refuses a release larger than the unused reservation and moves external amounts to
`pending_settlement`. Settlement resolves pending amounts; finalization is rejected with
`budget_not_settled` while any reservation or pending charge remains. A resume must prove
it can re-reserve its next unit (`_probe_reservation`, `insufficient_budget`).

`domain/policies/budget.py::roll_up_child_budget` applies one child account's deltas to
its locked parent: consumption can never decrease, totals can never go negative, parent
hard caps are enforced on new reservations, and parent ledger entries are written per
kind with a child-scoped idempotency id. This is the serialized child reservation the
spec asks for, applied through the parent's own account.

## Unknown usage

The spec's per-dimension unknown disposition is not modelled. What exists is
`pending_external_amounts` on a usage record: an amount the provider has not confirmed
stays reserved and pending, which satisfies "never zero" by holding the reservation, but
there is no `unknown` marker, no native usage identity and no price-sheet reference.
Usage attribution for Agent Server children is computed in
`adapters/deep_agents/async_subagents.py::attribute_usage` ([lanes](lanes-and-harness.md)).

## Reads

`GET /run-control/v1/runs/{run_id}/budget` returns `BudgetState` on the technical facade;
the application-scoped prefix exposes budget only through inspection
([interfaces](interfaces.md)). The spec's `budget.consumed`, `threshold_crossed` and
`exhausted` events were not located in the code opened here.

# Citations

- Spec: `../mission-control-general/general-mission-control/SPECIFICATION.md` (budget
  admission paragraph);
  `../mission-control-general/general-mission-control/expansion/EXPERIENCE-AND-STREAMS.md`
  (usage ledger);
  `../mission-control-general/general-mission-control/expansion/DEPLOYMENT-AND-RELEASE.md`
  (compute allocation model, ProofBudget).
- ADR: [0011](../adr/0011-distinct-retry-counters-and-retry-classifier.md).
- Code: [budget policy](../../src/mission_control/domain/policies/budget.py),
  [policy contracts](../../src/mission_control/domain/policies/contracts.py),
  [reducer](../../src/mission_control/domain/policies/reducer.py),
  [technical run control](../../src/mission_control/interfaces/http/run_control.py),
  [usage attribution](../../src/mission_control/adapters/deep_agents/async_subagents.py).
- Tests: [boundary commands, resume re-reserve](../../tests/unit/run_control/test_boundary_commands.py),
  [async child parent budget](../../tests/unit/operations/test_async_child_parent_budget.py),
  [RRM-008 cancellation settlement](../../tests/unit/operations/test_rrm_008_cancellation.py),
  [atomic family admission](../../tests/unit/run_control/test_atomic_family_admission.py).
