# WP-CP-001 accepted evidence

Disposition: `accepted`  
Recorded: 2026-08-10  
Runtime or schema changes: none

## Delivered authority and ownership evidence

- The canonical authority hierarchy and all contract owners are recorded in
  [`IMPLEMENTATION_READINESS.md`](../../implementation_work_packages_v2/IMPLEMENTATION_READINESS.md#31-canonical-contract-ownership-matrix).
- Exact target paths are frozen in
  [`IMPLEMENTATION_READINESS.md`](../../implementation_work_packages_v2/IMPLEMENTATION_READINESS.md#3-frozen-implementation-paths).
- Replacement/deletion gates are recorded in
  [`IMPLEMENTATION_READINESS.md`](../../implementation_work_packages_v2/IMPLEMENTATION_READINESS.md#9-replacement-and-deletion-rules).
- The active package sequence and replacement posture are recorded in the
  [v2 index](../../implementation_work_packages_v2/README.md).
- Every atomic canonical requirement is expanded in
  [`TRACEABILITY.md`](../../implementation_work_packages_v2/TRACEABILITY.md).

## Replacement decision

The repository will not implement dual reads/writes, compatibility workers, old-execution drain,
prototype-data backfill, OpenAI Agents SDK fallback, or Agent Server macro-runtime fallback.
Historical documentation and experiments may remain inert and non-normative. Active imports,
dependencies, launch paths, worker registrations, settings, live scripts, and obsolete tests are
deleted by the package that lands their accepted replacement.

## Validation performed

```text
Markdown link validation: all local targets resolved across the 14 edited planning/support/evidence files.
Atomic traceability validation: 67 canonical requirement IDs; 67 traceability IDs; no difference.
Patch validation: git diff --check reported no whitespace errors.
Deep Agents installed version: 0.7.5.
Async mechanism inspection: AsyncSubAgentMiddleware exposes Agent Protocol-backed
start/check/update/cancel/list operations and provider thread/run bindings.
Focused prior-art tests: 23 passed (StageGraph, GoalDirected, semantic routing).
```

## First WP-CP-010 code move

Implement strict canonical `CON-CP-DEFINITION-REF-V1` and `CON-CP-ERC-V1` models and canonical
serialization/digest tests under `app/domain/control_plane/`, then replace the pure compiler and
publication/compilation application service. No admission, Temporal, provider, or compatibility
work belongs in that first slice.

## Remaining risks

- Numeric workflow budgets, cycle limits, verifier rubrics, and retention values remain authored
  Workflow Type/blueprint/deployment configuration, not architecture gaps.
- Exact physical PostgreSQL table and MongoDB collection names are chosen in their owning package
  migrations/models and must remain behind the frozen domain contracts.
