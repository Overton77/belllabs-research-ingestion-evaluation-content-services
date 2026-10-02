# RRM-017 — Declare patchable fork fields on Workflow Types and qualify the terminal `cognitive_seed`

**What to build:** (a) Workflow Types (or blueprints) declare which fields a `RunForkPatch` may change and what each change invalidates, replacing RRM-006's composition-registered `ForkPatchPolicy`; (b) an explicit terminal-checkpoint `cognitive_seed` is written into the derived run's own namespace and consumed by the matching re-executed unit, with `seeded_from` lineage.

**Blocked by:** RRM-006.
**Blocks:** Nothing in the mission (RRM-001 §8 #5 allows the seed to be deferred). Useful before company fork sessions that edit stage objectives.
**Status:** ready-for-agent after RRM-006 is accepted (found by RRM-006)
**Branch:** `wp/rrm-017-fork-patch-declarations-and-seed`
**Authority:** REQ-CP-EXEC-012 (patchable fields, `cognitive_seed`), REQ-CP-DA-016 (namespaces, stamps), REQ-CP-CS-007 (schema gate), `CON-CP-CONTINUATION-V1`

## Diagnosis (RRM-006, 2026-10-02)

- No definition declares patchable fields. RRM-006 validates patches against a `ForkPatchPolicy` registered per Workflow Type digest at composition (`app/application/runtime/run_forks.py` `ForkPatchPolicyRegistry`), defaulting to whole-run fields (`input_manifest`, `effective_configuration_digest`, GoalDirected `goal.objective`) that invalidate every unit. The StageGraph blueprint already carries `invalidation_reuse_declarations`, a natural home for the declaration; adding a field changes blueprint digests, so it needs a versioned definition.
- RRM-006 rejects any `cognitive_seed` with `cognitive_seed_not_supported`. Seeding needs: copying the source namespace's root chain up to the terminal result checkpoint *with its ancestor writes* (LangGraph 1.2 stores `messages` and `files` as `DeltaChannel` ancestor writes; `langgraph-checkpoint` 4.1.1 `acopy_thread` is not implemented by the Postgres or memory savers); creating the derived unit's namespace with the seed as its head and owner; and confirming the adapter's `not_submitted` admission accepts a source stamped by another unit. The terminal, settled, recorded-lineage and schema checks are the RRM-005 `_unit_lineage` checks.

## Acceptance

- [ ] A versioned Workflow Type/blueprint declaration of patchable fields and their invalidation closure; the composition registry is retired.
- [ ] A seeded StageGraph fork: the re-executed unit continues the source's terminal state in its own namespace, the saver shows the copied chain, `seeded_from` lineage is recorded, and a schema-digest mismatch fails `incompatible_restore`.
- [ ] Non-terminal or intermediate checkpoints stay rejected.
