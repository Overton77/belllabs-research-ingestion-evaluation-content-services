# WP-BP-020 parallel-worktree handoff

Disposition: `accepted`

## Kickoff record

- Repository: `biotech-research-ingestion-evaluation-system` on `main`
- Work package: `WP-BP-020` — Canonical GoalDirected runtime
- Integrated base: `9453eec` then sequential finish commits on `main`
- Accepted dependency evidence: WP-CP-030/040/045

## Ownership

- GoalDirected contracts, interpreter, Temporal workflow/activities
- GoalDirected application services/repositories/models
- Focused `tests/test_wp_bp_020_*.py`

## Progress after integrator atomic switch

- Coordinator/worker composition uses `app/temporal/activities/goal_directed.py`
- Beanie GoalDirected models registered in Mongo init
- Worker registers GoalDirected family admissions by default
- Compaction failure actions `retry`/`fresh_from_handoff` recover deterministically;
  `pause`/`escalate` apply frozen interpreter stopping proposals
- Cancellation Temporal test proves cancel enters reconciliation saga and forbids
  `terminalize` while reservations remain unsettled
- Deletion gate completed:
  - deleted `app/temporal/goal_directed_workflow.py`
  - deleted `app/temporal/goal_directed_activities.py`
  - deleted `app/agent_server/goal_directed/` and `app/agent_server/stagegraph/`
  - emptied Agent Server `GRAPH_REGISTRY` / `langgraph.json` graphs
  - relocated shared run-state validation to `app/agent_server/common.py`

## Acceptance closure

- The credential-gated API-to-Temporal vertical passed with `gpt-5.6-luna`.
- A token threshold forced one fresh agent session and a typed, host-mediated handoff.
- The durable executor workspace survived replacement and was exposed to the independent verifier
  as a read-only mount; the verifier accepted the Moderna/Spikevax artifact.
- Four real provider calls completed and run control terminalized the run as `completed`.

## 2026-08-11 qualification addendum

- Published `evidence_v2/WP-BP-020/README.md`.
- Corrected the production operation-result envelope seam and host-owned token accounting.
- Bound LangGraph checkpoint continuity to governed session identity.
- Added and ran the real Deep Agents 0.7.5 Docker filesystem sandbox vertical with forced token
  rollover, persisted mediated handoff, empty replacement session, persistent workspace artifact,
  separate executor/verifier operations, and reducer-authorized completion.
- Added model-observation-only executor/verifier/handoff schemas; canonical identities, authority,
  usage, reservations, remaining budgets, and remaining iterations are now host-bound.
- Added the explicit credential-gated API-to-Temporal real-OpenAI qualification using the Docker
  sandbox. It collects and skips safely until the operator flag and credentials are supplied.
- Focused GoalDirected/Deep Agents gate passed; full mypy passed. Repository-wide baseline failures
  are recorded in evidence and remain outside this package's changed paths.

## Focused verification

```text
uv run pytest -q tests/test_wp_bp_020_goal_directed.py tests/test_wp_bp_020_temporal.py --tb=short
```

FINAL_DISPOSITION: ACCEPTED
