# Agent implementation-session prompt template

Status: reusable operator template for Stage 3–8 implementation sessions  
Use: copy the prompt below, replace every `<PLACEHOLDER>`, and attach only the active package and
directly relevant evidence where the agent interface supports attachments.

---

## Copyable prompt

```text
You are implementing one evidence-gated BellLabs migration work unit in:

Repository:
  biotech-research-ingestion-evaluation-system

Active package:
  <STAGE_AND_PACKAGE_PATH>

Work unit:
  <UNIT_ID_AND_TITLE>

Objective:
  <ONE_CONCRETE_VERIFIABLE_OUTCOME>

Required acceptance evidence:
  <TESTS_ARTIFACTS_AND_GATE_ROWS>

Explicitly in scope:
  - <ITEM>
  - <ITEM>

Explicitly out of scope:
  - <ITEM>
  - <ITEM>

Known decisions or constraints for this unit:
  - <DECISION_OR_NONE>

Read before editing, in this order:

1. docs/migrations_instructions/implementation_work_packages/00_MAIN_GOAL_AND_INDEX.md
2. docs/migrations_instructions/implementation_work_packages/00A_REFERENCE_RESEARCH_BLUEPRINTS_AND_INCREMENTAL_PROOFS.md
3. docs/migrations_instructions/implementation_work_packages/01_GLOBAL_HANDOFF_AND_STAGE_GATE_RULES.md
4. docs/migrations_instructions/implementation_work_packages/02A_OWNER_AMENDMENTS_FOR_STAGES_3_TO_6.md
   when applicable.
5. docs/migrations_instructions/implementation_work_packages/SUPPLEMENT_CODEBASE_ORGANIZATION.md
6. docs/migrations_instructions/implementation_work_packages/SUPPLEMENT_APPLICATION_CONTRACT_ENHANCEMENTS.md
7. docs/interview_and_research_result_documentation/CANONICAL_APPLICATION_CODEBASE_ORGANIZATION.md
8. The complete active package, every dependency it declares, and their accepted handoffs/evidence.
9. The accepted architecture/contract documents referenced by the active package.
10. docs/interview_and_research_result_documentation/CODEBASE_DOMAIN_WORKFLOW_GUIDE.md,
   current code, and current tests as as-built evidence.

Do not begin substantive edits until you can state:

- the active package's exact authority and exit gate;
- the smallest end-to-end slice for this work unit;
- the current implementation path and accepted target owner;
- the contracts and compatibility surfaces that must remain stable;
- the exact evidence that will demonstrate completion.
- the exact immutable Q/D blueprint/implementation increment owned by this unit, its deterministic
  run, bounded live canary or skip reason, and comparison against the preceding accepted increment.

ARCHITECTURE THAT MUST NOT DRIFT

- Temporal is the sole production macro-workflow executor.
- The production hierarchy is:

  BellLabs API/control service
    -> BellLabsRunWorkflow
         -> StageGraphWorkflow | GoalDirectedWorkflow | another accepted family workflow
              -> OperationWorkflow
                   -> native | Deep Agents/LangGraph | MCP | sandbox | remote provider adapter

- Deep Agents/LangGraph provides bounded operation cognition. It does not own macro scheduling,
  admission, authoritative lifecycle, readiness, convergence, effects, settlement, or terminality.
- Pure BellLabs interpreters own StageGraph scheduling/readiness and GoalDirected convergence.
- BellLabs application services and PostgreSQL own admission, budgets, commands, approvals,
  claims, effects, accepted evidence, settlement, product events, and terminality.
- The BellLabs API is the sole governed public facade. Do not introduce a direct public Temporal,
  Agent Server, LangSmith, sandbox, MCP-provider, or worker API.
- An agent child that needs independent durability, messaging, cancellation, capacity, lineage,
  settlement, or a reusable result must become a Temporal child through governed BellLabs
  delegation. Built-in Deep Agents subagents remain operation-local and non-addressable.
- Temporal histories and agent/checkpoint state are not the BellLabs product query or authority
  model.

AUTHORITY AND DEPENDENCY RULES

Preserve this dependency direction:

  app/domain <- app/application <- app/api | app/temporal | app/integrations

- Domain owns meaning, invariants, reducers, interpreters, and authoritative contract semantics.
- Application owns use-case coordination and ports; it persists and enforces domain decisions.
- API, Temporal, persistence, agent, sandbox, MCP, and provider code are adapters.
- Provider SDK objects, Temporal types, API DTOs, and database documents must not leak into domain
  authority.
- Search for the current owning contract before creating a new noun, V2 type, provider-specific
  duplicate, or generic configuration object.
- Prefer explicit translators at boundaries over reusing an adapter/storage type as a domain type.

CURRENT CODE VERSUS ACCEPTED TARGET

Treat the repository as a migration state, not as proof that every current path is desirable.
Classify every touched path as exactly one of:

1. KEEP — accepted owner and implementation;
2. ENHANCE IN PLACE — current owner remains valid while its contract/behavior is extended;
3. MOVE WITH COMPATIBILITY — active package authorizes the target path and old imports need a
   narrow, temporary bridge;
4. REPURPOSE — code remains but loses obsolete macro/runtime authority;
5. REPLACE — the new path becomes authoritative after parity and recovery evidence passes;
6. RETIRE AFTER GATE — preserve until its named replacement and decommission gates pass;
7. DELETE NOW — allowed only when the active package explicitly authorizes deletion and evidence
   proves there is no live caller, durable execution, compatibility obligation, or retained value.

Do not preserve obsolete architecture merely because tests currently exercise it. Determine whether
those tests are authoritative acceptance tests, temporary compatibility tests, historical evidence,
or tests that must be replaced. Conversely, do not delete old code merely because a target tree
exists. A projected path is not authorization to move files.

REPLACEMENT AND DECOMMISSION PROTOCOL

For every replacement, repurpose, move, or decommission:

1. Inventory callers, imports, routes, workers, configuration, persisted records, running/durable
   workflow implications, tests, scripts, documentation, and operational entry points.
2. Name the old responsibility and the new authoritative owner.
3. Define semantic parity plus recovery, replay, cancellation, idempotency, lineage, redaction, and
   settlement evidence appropriate to the boundary.
4. Introduce the replacement behind an exact binding, feature gate, compatibility adapter, or
   staged migration when required.
5. Prevent new callers from using the old surface.
6. Migrate/backfill or dual-read authoritative data only through an explicit compatibility plan.
7. Remove the old surface only after the named gate passes and no admitted/durable work relies on it.
8. Record a disposition for every old path: retained, repurposed, compatibility-only, quarantined,
   or removed.
9. Preserve historical evidence even when executable paths are removed.

Never use deletion as a substitute for understanding. Do not create parallel "new", "v2", or
provider-specific architectures that leave both paths authoritative.

IMPLEMENTATION METHOD

1. Inspect repository instructions and git status. Preserve unrelated and pre-existing changes.
2. Trace the current end-to-end call path and the owning contracts before editing.
3. Publish or update the work unit's requirements-to-evidence rows before substantive implementation.
4. Write a short implementation note listing:
   - current state;
   - target state authorized by the active package;
   - exact files expected to change;
   - replacement/decommission dispositions;
   - compatibility and migration strategy;
   - verification commands.
5. Implement the smallest production-shaped vertical slice. Do not perform an unrelated bulk
   reorganization.
6. Keep Temporal workflows deterministic. Perform I/O, database, provider, filesystem, model, and
   network work through activities/application ports/adapters.
7. Carry compact stable identities, references, digests, bounded decisions, and summaries through
   Temporal. Do not place secrets, PHI, raw corpora, unrestricted transcripts, or large artifacts
   in histories, checkpoints, traces, logs, heartbeats, or handoffs.
8. Do not claim exactly-once provider execution. Use stable claims and idempotency identities,
   reconcile ambiguity, and guarantee exactly-once BellLabs settlement.
9. Add or update tests with the implementation. Test negative authority and failure paths, not only
   successful execution.
10. Update as-built navigation when paths change and normative documents only when an accepted
    decision/package requires it.

VERIFICATION EXPECTATIONS

Run the narrowest checks first, then the package-required suite. Unless the active package says
otherwise, consider:

- unit tests for domain invariants and pure interpreter decisions;
- serialization and backward/forward compatibility tests;
- repository and migration/backfill tests;
- Temporal workflow, activity, replay, Continue-As-New, cancellation, and worker-loss tests;
- inbox/outbox dedupe, stale generation, effect ambiguity, reconciliation, and settlement tests;
- adapter conformance tests across local/remote/provider variants;
- API authorization, tenant isolation, redaction, and bypass-negative tests;
- the active package's acceptance vertical and requirements-to-evidence matrix;
- ruff, mypy, and the relevant pytest suite.

Do not mark the work unit complete because code compiles or a happy-path test passes. If an external
service or credential blocks a live test, complete every safe local/fixture check, record the exact
blocked command and missing prerequisite, and do not represent the live gate as passed.

WHEN TO ASK ME BEFORE CONTINUING

Ask only when the answer would materially change architecture, authority, data migration, public
compatibility, destructive scope, deployment topology, or the active package's gate. In particular,
pause for:

- a conflict between two normative sources that cannot be resolved by precedence;
- an unapproved new service/repository/package boundary;
- deletion before the named parity/decommission gate;
- a breaking public or persisted contract without an accepted migration path;
- use of real credentials, consequential external effects, or sensitive/regulated data;
- a scope expansion outside this work unit.

Otherwise, make the smallest documented assumption and proceed.

REQUIRED HANDOFF

At completion, update or create the active unit/package handoff and report:

1. Outcome and gate recommendation: PASS, CONDITIONAL, or BLOCKED.
2. Exact changed paths.
3. Contracts added/changed and compatibility classification.
4. Current-to-target path dispositions, including all replacements and decommissions.
5. Migrations/backfills and rollback status.
6. Tests and verification commands with exact results.
7. Replay, recovery, idempotency, lineage, settlement, security, and redaction evidence.
8. Failures, skipped/live checks, residual risks, and temporary compatibility surfaces.
9. Explicitly not completed.
10. The next work unit's first safe action.
11. Q/D versions, runtime path exercised, deterministic/live evidence, capability promotions or
    unsupported surfaces, and drift-comparison disposition.

Begin by reading and inspecting. Then give me a concise readiness statement and proceed with the
implementation unless one of the mandatory clarification conditions applies. Continue until this
work unit is genuinely complete or concretely blocked; do not stop after planning alone.
```

## Operator notes

Keep each assigned work unit narrow enough to finish and verify in one agent session. Good units
name one vertical behavior, one replacement seam, or one gate subsection. Avoid prompts such as
“implement Stage 3” unless the agent is explicitly expected to coordinate multiple sequential
sessions and maintain the aggregate handoff.

The most important placeholders are:

- `<STAGE_AND_PACKAGE_PATH>` — the single active authority;
- `<UNIT_ID_AND_TITLE>` — a stable identifier used in evidence and handoff records;
- `<ONE_CONCRETE_VERIFIABLE_OUTCOME>` — observable behavior, not “refactor” or “improve”;
- `<TESTS_ARTIFACTS_AND_GATE_ROWS>` — the evidence that prevents a false completion;
- in-scope/out-of-scope lists — the boundary that prevents opportunistic redesign.

For a destructive retirement unit, add the exact old paths and named prerequisite gate explicitly.
For a contract-freeze unit, add the compatibility window, persisted versions, and N/N+1 replay
expectation explicitly.
