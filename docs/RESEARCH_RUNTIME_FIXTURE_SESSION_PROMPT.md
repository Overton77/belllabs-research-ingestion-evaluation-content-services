# Research runtime: company fixture session prompt (RRM-011)

Paste this into a **new session that the user starts**. Prerequisite sessions must not run it.

---

You are running **RRM-011**, the company fixtures, in a session the user started for this purpose. Read first:

1. `docs/migrations_instructions/evidence_v2/research-runtime-mission/RRM-010/READY_FOR_SEPARATE_FIXTURE_SESSION.md`, the readiness manifest. It is your starting state: the revision, accepted meta, capability pins, deployment, budgets, proven boundaries and open residuals.
2. `docs/migrations_instructions/implementation_work_packages_v2/research-runtime-mission/issues/11-company-fixtures-separate-session.md`, whose checklist is your acceptance.
3. `docs/migrations_instructions/implementation_work_packages_v2/WP-CP-050-foundation-capability-materialization-vertical.md` and `docs/RESEARCH_RUNTIME_MISSION_SPECIAL_HANDOFF_2026-10-01.md`, which hold the bounded objectives and the CP-050 tracer requirements.
4. The RRM-009 evidence runbook (`evidence_v2/research-runtime-mission/RRM-009/README.md`, "Deployment runbook") and `README.md` "Production-shaped composition".

## Goal

Two useful, cited company reports, produced through the governed BellLabs API on the production composition:

- **Qualia Life on StageGraph:** early provisional synthesis, then final accepted evidence with liability closure.
- **GenerationLab on GoalDirected:** independent verification, bounded repair and convergence, and a qualified handoff.

Both use Deep Agents with an **in-process sync subagent** in each family, and at least one fixture runs a **governed async subagent on the Agent Server**. For each family, also:

- inspect active and terminal lifecycle and an earlier checkpoint;
- replay the captured histories;
- run one compatible semantic-boundary fork with an immutable parent and a derived report;
- demonstrate the already-qualified boundary intervention.

Then complete the CP-050 tracer evidence and submit it for acceptance.

## Start-up checks (stop and ask if any fails)

1. Confirm with the user that this is the separately initiated fixture session. Record the prerequisite revision: `main` after the RRM-010 merge, or the integration merge commit named in the manifest.
2. Create a branch `wp/research-company-fixtures` from that revision, in a sibling worktree. Never touch the main checkout's uncommitted work.
3. Re-check the environment against the manifest, not from memory:
   - the services are up: application Postgres with migrations through 0026, Mongo, object storage, the persistent Temporal namespace with the Search Attributes registered, and the `rrm009-agent-server` with signed-claim auth (probe it: a valid claim returns 200, a claim longer than one hour or without a `jti` returns 401);
   - the capability pins verify;
   - the credentials resolve by reference;
   - the provider prices are current.
4. Resolve the current official company and offer identity at execution time. Freeze bounded objectives, source, visit, iteration, time and spend ceilings, and exact capabilities **before** compiling definitions.

## Rules

- **Spend.** Tiny smoke first, then one fixture at a time. Default ceiling is about USD 10 per fixture (manifest §9). Report the observed spend per run. Stop and ask before exceeding it.
- **Governed path only.** Admission (`/run-control/v1/run-requests`), then launch (`/runs/{id}/launch`), then `/commands`, `/snapshots`, `/forks` and `/inspection/...`. Temporal is the only macro scheduler; the Agent Server only hosts async subagent graphs.
- **Stay inside the proven boundaries** (manifest §6):
  - Forks only at StageGraph `stage_settled`, or at a terminal GoalDirected `goal_verifier_settled`, with declared patchable paths.
  - No `cognitive_seed`, no intermediate-checkpoint fork, no mid-invocation steering.
  - Interventions are wait release, GoalDirected pause and resume, and cancellation. Never imply unrestricted mid-invocation edits.
- **Known limits to respect:**
  - The checkpoint history of an in-flight unit is `unavailable` until a transition is recorded.
  - A child's usage overage is recorded, not refused.
  - Session-generation admission after a sealed GoalDirected head is not composed (RRM-014).
  - The symmetric Agent Server key, DNS rebinding and MCP transitive dependencies are not pinned.
- **Evidence.** Durable cited reports, claim and source manifests, verifier findings, usage and effect settlement, exact lineage, inspection reads, captured-history replay, run IDs, and sanitized commands. Store them in the CP-050 aggregate directory and the assigned company-report references, following `docs/migrations_instructions/evidence_v2/README.md`.
- **Safety.**
  - Never print or commit `.env` or secrets. Remove only containers you created, by exact name; never prune. Serialize service runs. Do not push.
  - Research outputs are not medical advice.
  - Report limitations and follow-on tickets plainly. Fixture evidence is reviewed and merged on branches.

## Finish

Report the following, then stop:

- the run IDs and outcomes;
- the report artifacts with their digests;
- the fork lineage and parent invariance;
- the interventions with their receipts;
- the spend;
- the CP-050 checklist status;
- unresolved gates and new tickets.

CP-050 is accepted only after its complete tracer evidence has been reviewed.
