# Research runtime: capability summary

Recorded: 2026-10-03. This summarizes what the research-runtime mission (RRM-001 to RRM-021, CR-1 to CR-5) delivered, for reuse in a larger mission control. The details are in each ticket's evidence under `docs/migrations_instructions/evidence_v2/research-runtime-mission/`. The reusable seams are listed in `CLEANUP.md`, under "CR-5".

## The shape

The system uses three layers:

- **Temporal** is the only macro scheduler. It runs durable workflows: the root, the two families and the operation.
- **Run-control PostgreSQL** is the only lifecycle, budget, effect and receipt authority.
- **Deep Agents** do bounded cognition inside one `operation.execute` Activity.

Async subagents run on a self-hosted LangGraph Agent Server. That server only hosts graphs and never schedules BellLabs work.

```text
API facade (/run-control/v1)
  -> admission (reducer: budgets, ERC, idempotency)
  -> governed launch: TemporalWorkflowSubmitter.for_production (Search Attributes required)
  -> BellLabsRunWorkflow (root: message receipts, cancel space)
      -> StageGraphWorkflow | GoalDirectedWorkflow (family semantics, boundaries)
          -> OperationWorkflow -> operation.execute / operation.cancel Activities
              -> Deep Agent (create_deep_agent through the single adapter)
                  - sync subagents (in-process)
                  - async subagents (Agent Server, governed spawn)
```

## The workflow types

### StageGraph (fixed graph of stages)

- **Execution.** Stages run in dependency order, with joins and cycles. Each stage is one runtime unit, identified by mapped instance, cycle and slot.
- **Declared waits.** A wait holds the run in phase `waiting`. It can be released through the facade (`satisfy_wait`) with durable `accepted → delivered → applied` receipts. Satisfied waits survive Continue-As-New.
- **Pause and resume.** These can be scoped to stages. Unrelated admissible work keeps running, and the aggregate phase stays accurate.
- **Cancellation.** Cancelling a running run cancels its siblings, reconciles their usage and effects, and terminalizes `cancelled`. Cancellation is applied only when an operator requested it; a worker restart never counts as a cancel.
- **Budgets.** The admitted baseline reservation is released before terminalization on completion, cancellation and the blocked path (RRM-021).
- **Forks.** A run can fork at `stage_settled` (no open liability or active stage). The derived run reuses settled, compatible results by reference, without re-running the model.

### GoalDirected (iterate until a verifier accepts)

- **Executor and verifier.** They are isolated, with disjoint writable workspace roots (`/goal/{n}/{role}`). Each iteration is a pair of runtime units.
- **Settlement.** Every executor and verifier operation is journaled, fenced and settled exactly once through run control (RRM-016). The family consumes that settlement, so usage is counted once.
- **Shared sessions.** A shared cognitive session carries across iterations through pinned checkpoints (GD-012). Rollover starts a new session generation.
- **Workspace.** The `shared` mode keeps one workspace per role across iterations, adding one manifest revision per iteration (RRM-020). The `fresh` mode also works.
- **Durable pause.** Pause leaves the run durably `paused` at the iteration boundary, and survives worker restart and Continue-As-New. Resume continues from the correct frontier. If the run's budget can't cover the next iteration, the resume is rejected with `insufficient_budget`.
- **Completion.** A completed run promotes only the verified final executor's outputs (RRM-019). Mongo documents persist idempotently across iterations (RRM-018).
- **Cancellation.** Cancellation runs to the end of the saga instead of failing. An `in_doubt` unit is kept as a liability until it is reconciled.
- **Forks.** A fork can be taken at `goal_verifier_settled`. This has been proven only on terminal runs. GoalDirected units are never reused, because their identity is bound to the run.

## Deep Agents configuration (the operation's cognition)

- **One composition root.** `app/integrations/agents/deep_agents/adapter.py` and `materializer.py` turn an exact, digest-bound binding into `create_deep_agent` arguments: model policy, tools, MCP servers, skills, subagents, `state_schema`/`context_schema` packs, the checkpointer and the store. Nothing outside these files builds an agent.
- **Checkpoint lineage.** Each invocation runs in a deterministic thread with root namespace `""`. It is pinned to the expected source `checkpoint_id`, uses `durability="sync"`, and stamps unit, generation, attempt and schema digests into checkpoint metadata. The result checkpoint is captured and linked to the immutable result manifest (RRM-003).
- **Crash recovery.** Before each attempt, the unit is classified as one of: `not_submitted`, `interrupted`, `terminal_unobserved`, `observed_unsettled`, `settled` or `in_doubt`.
  - An interrupted unit resumes with `None` input, so the prompt is never appended again.
  - A finished-but-unrecorded unit is rebuilt from its checkpoint without calling the model.
  - Anything ambiguous opens an `in_doubt` incident and waits for an operator's `reconcile_unit` decision.
  - Leases are fenced, and a lease takeover happens at fence N+1 (RRM-004).
- **Sync subagents** run in-process. Their usage is charged to the parent.
- **Async subagents** run on the Agent Server (RRM-013).
  - BellLabs reserves and links the child before the provider run. The spawn is fenced, so each child gets exactly one provider run, even across a worker kill or a server restart.
  - Duplicate or ambiguous runs move the child to `in_doubt`, which is resolved by `adopt_provider_run` or `orphan_child`.
  - Child usage settles to the parent's budget. Usage that can't be attributed stays pending until it is reconciled.
  - Scope claims are signed, last 30 minutes, and carry a `jti`.
- **Capabilities** (RRM-009).
  - Skills and MCP servers are pinned by digest and verified on the worker before launch.
  - Browser egress is denied by default, and only the operation's granted hosts are reachable.
  - Search goes through worker-mediated MCP.
  - Outputs are promoted into governed artifacts, either S3-compatible or on the filesystem.
  - Sanitized capability lineage is recorded with each settlement.
- **Heartbeats.** Each operation class sets its own heartbeat timeout. A cancel reaches cognition within about 0.8 × the heartbeat timeout.

## Operating it

| Concern | Capability | Where |
|---|---|---|
| Launch | Governed admission, then a production start with a baseline-reservation check | `app/api/run_control.py`, the run-launch service, `app/integrations/temporal_workflow_submission.py` |
| Inspect | Scoped list, detail, unit and history reads; signed cursors; redacted checkpoint summaries; freshness and `in_doubt` | `/run-control/v1/inspection/*` (RRM-005) |
| Intervene | Wait release, pause, resume and cancel, with durable receipts and ordered sequence spaces; a delivery relay | `/run-control/v1/runs/{id}/commands`, boundary interventions (RRM-007) |
| Fork | Immutable `RunSnapshotManifest` and a validated `RunForkPatch`, admitted independently at epoch 1 | `/run-control/v1/…` fork routes (RRM-006) |
| Recover | Lease and fence takeover, terminal reconstruction, `in_doubt` incidents, `reconcile_unit` | RRM-004 |
| Reconcile | `reconcile_unit` and `reconcile-usage` (privileged roles) | RRM-004, RRM-009 |
| Deploy | Worker composition factory, persistent saver and store, Search Attribute registration, Agent Server recipe | the README runbook; RRM-009 evidence |

## Not yet built (next phase)

1. **Context engineering is underdeveloped.** The cognitive-schema packs are accepted in narrowed form: spec 05 is canonical, with CS-006 and CS-008 deferred. Three gaps remain:
   - Sync subagents don't get a seeded, BellLabs-governed state slice (CS-006).
   - Workflow Types don't declare their context packs (CS-008).
   - There is no typed context or steering derivation contract, so mid-run steering and source clarification are deferred.

   Context between GoalDirected iterations moves only through the typed handoff. Iteration N+1 can't read iteration N's workspace.
2. **The coordinator agent skill doesn't exist.** Nothing yet lets an agent use mission control to plan, launch, inspect, intervene in and fork workloads. All the facade routes it would call exist and are governed. What's missing is the skill and tool layer on top, its authorization role, and its spend policy.
3. **Open tickets.**
   - RRM-012: retire or repair the reference-research harness.
   - RRM-014: re-admit a unit at a new generation after `start_new_generation`.
   - RRM-017: Workflow Types declare patchable fork fields, plus the terminal `cognitive_seed`.
4. **Residuals listed in the evidence.**
   - Session-generation admission after a sealed head.
   - Two superseded-generation windows.
   - `GenericArtifactWorkflow` sits outside the cancellation saga.
   - The Agent Server uses a symmetric signing key; asymmetric or per-scope keys are the follow-up.
   - S3 is qualified against MinIO, not AWS.
   - The local dev Temporal server stood in for a production namespace.
5. **Company fixtures (RRM-011)** are held for a separate, user-started session. See `RRM-010/READY_FOR_SEPARATE_FIXTURE_SESSION.md` and `docs/RESEARCH_RUNTIME_FIXTURE_SESSION_PROMPT.md`.

## What to reuse when generalizing

Reusable and free of company or provider specifics:

- run-control domain (reducer, receipts, budgets, effects);
- runtime-unit identity, lease, journal and settlement;
- the checkpoint-lineage ledger and classifier;
- the snapshot and fork saga;
- the boundary-command ledger and relay;
- inspection read composition;
- the deterministic workflow shells (park-in-doubt loop, cancellation saga);
- async-child adoption and reconciliation;
- composition from capability pins;
- the production-stack test harness.

Keep these behind ports:

- Deep Agents and LangGraph checkpoint reads;
- the Agent Server and Temporal adapters;
- the web-research and schema-grounding modules.

The concrete module list is in `CLEANUP.md`, under "CR-5".
