---
type: Concept
title: Mission revisions and the scheduling head
description: How a mission definition becomes an immutable revision with a compiled program, how proposals activate against an expected head, and what the compiler and schema implement today.
tags: [mission-control, revisions, authoring, compiler, implementation]
---

# Mission revisions and the scheduling head

A [Mission Definition](../../GLOSSARY.md) is the typed document a mission is authored
as. Editing it never overwrites committed intent: every submitted validation snapshot,
committed [Revision](../../GLOSSARY.md), [Compiled Program](../../GLOSSARY.md),
activation, attempt and artifact is immutable, and an edit produces a successor
revision (`../mission-control-general/workflow-types/07-MISSION_REVISION_AND_RUNTIME_EVOLUTION.md`,
section 1). The code calls the compiler output `EffectiveRunConfiguration`; prose
says Compiled Program.

## From draft to scheduling head

Authored edits and runtime graph changes share one pipeline: edit a private draft,
submit a [Revision Proposal](../../GLOSSARY.md), validate, compile, review when
required, commit, activate the [Scheduling Head](../../GLOSSARY.md) (section 3). A
proposal names its `base_revision_id` and records author, rationale, the semantic and
program diff, affected nodes and policies, and the expected treatment of in-flight
work. A mission has exactly one head; only that revision releases new work (section 4).

Activation uses optimistic concurrency: it locks and compares the expected mission
version and prior head in one transaction. If another proposal advanced the head
first the later one becomes `stale` and must be rebased and revalidated; nothing is
merged silently (section 10; `SPECIFICATION.md`, "Definition and compiler contracts").
Committing a revision and activating the head are distinct recorded transitions, and
neither starts a run (section 10a). Revert is a new proposal derived from old content,
never a silent reactivation (section 8). A material change to goals, authority, tenant
or the meaning of success is not a revision; it creates a successor or forked mission
(section 2 and 6.3).

## Transition impact and carry-forward

When a proposal is validated against the head, the compiler emits a
[Transition Impact](../../GLOSSARY.md) per node: `unchanged`, `policy_changed`,
`definition_changed`, `removed` or `added`, each with a default treatment of running
activations (continue, apply at the next child boundary, `pause_and_checkpoint`,
`supersede_after_completion`, release from the head). Transition policies apply at
activation granularity (a stage activation, swarm member, iteration, round or wait),
and a composite's choice cascades unless a child declares its own (sections 4.1-4.2).
For a run, an applied `run_revision_transition` is what changes the future scheduling
revision; in-flight work stays pinned to its starting revision otherwise.

[Carry-Forward](../../GLOSSARY.md) eligibility is computed from digests, never by
inspection: node definition digest, input digests, policy digest and rubric version
must all be equal. Stale evidence under a proof policy yields `re_verify_required`
(outputs carry forward, acceptance re-runs), so the vocabulary is
`eligible | re_verify_required | ineligible` (section 5.1).

## Implemented today

The compiler exists for catalog definitions rather than for a `MissionDefinition@1`
document. `compile_effective_run_configuration` in
`src/mission_control/domain/authoring/compiler.py` resolves exact definition refs
(workflow type, blueprint, control, runtime, workspace and evaluation profiles),
verifies each ref's digest against the resolved definition, intersects caller and
parent authority, flattens agent profiles into bindings and produces a
`capability_attachment_plan`, `overlay_decisions` and a content digest over the
whole result (`EffectiveRunConfiguration` in `src/mission_control/domain/authoring/contracts.py`).
`ControlPlaneService.compile` in `src/mission_control/application/authoring/service.py`
resolves selectors and aliases, persists the result and `retrieve_for_admission`
re-verifies the digest and that every source ref is still admissible. Determinism is
tested in `tests/unit/control_plane/test_control_plane.py`.

Expected-head concurrency is implemented for published catalog definitions: a
`PublishRequest` carries `expected_head_revision`, and the repository raises
`DefinitionConflict` when the current published revision differs
(`src/mission_control/application/authoring/control_plane_repository.py`,
`src/mission_control/adapters/postgres/control_plane/definition_repository.py`). This
guards a definition's publication head, not a mission's scheduling head.

The common schema has the revision tables, confirmed in
`packages/mission-control-db-contract/component/migrations/0002_authoring.sql`:
`mission_draft`, `definition_snapshot`, `mission_revision` (with `revision_no`,
`parent_revision_id`, `policy_digest`, `binding_digest`, immutable trigger),
`revision_proposal` (lifecycle `proposed | validating | awaiting_review | resolved`,
resolution `accepted | rejected | stale | withdrawn`, `impact_ref`, `review_task_ref`),
`compiled_program` (one per revision, `program_digest`, immutable) and `program_node`;
`0003_execution.sql` adds `run_revision_transition` with an impact manifest and
per-activation policies, and `mission_run.scheduling_revision_id`. Today the only
writer is `insert_admitted_run` in
`src/mission_control/adapters/postgres/run_control/canonical.py`: admitting a run
creates a mission, one definition snapshot, revision 1 and its compiled program
derived from the admitted Compiled Program digest, input manifest and obligations.
No proposal, validation, activation, transition-impact or carry-forward service
exists in `src/`.

## Specified only

Proposal lifecycle and resolution, head activation with transition impact, in-flight
transition policies, carry-forward eligibility, revert and rebase are specified in
`07` and the spec's "Definition and compiler contracts", and have schema rows but no
application logic. The compiler validation list in the spec (objective coverage,
evaluator independence, cycle and fan-out governors, human and proof gates) is also
beyond today's definition compiler. Authoring entry points are in
[authoring](authoring.md).

# Citations

- `../mission-control-general/workflow-types/07-MISSION_REVISION_AND_RUNTIME_EVOLUTION.md` (sections 1-11).
- `../mission-control-general/general-mission-control/SPECIFICATION.md` ("Definition and compiler contracts").
- [Compiler](../../src/mission_control/domain/authoring/compiler.py),
  [authoring contracts](../../src/mission_control/domain/authoring/contracts.py).
- [Control-plane service](../../src/mission_control/application/authoring/service.py),
  [repository port](../../src/mission_control/application/authoring/control_plane_repository.py),
  [PostgreSQL definition repository](../../src/mission_control/adapters/postgres/control_plane/definition_repository.py).
- [Canonical run writer](../../src/mission_control/adapters/postgres/run_control/canonical.py).
- [Control-plane tests](../../tests/unit/control_plane/test_control_plane.py).
- [`0002_authoring.sql`](../../packages/mission-control-db-contract/component/migrations/0002_authoring.sql),
  [`0003_execution.sql`](../../packages/mission-control-db-contract/component/migrations/0003_execution.sql).
