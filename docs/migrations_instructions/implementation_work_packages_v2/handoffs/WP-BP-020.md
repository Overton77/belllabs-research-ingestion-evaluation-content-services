# WP-BP-020 parallel-worktree handoff

Disposition: `ready_for_review`

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

## Remaining before acceptance

- Credential-gated live API-to-Temporal vertical remains pending
- Integrator combined gate with WP-BP-010 and evidence publication remain required
- Do not claim `accepted`

## Focused verification

```text
uv run pytest -q tests/test_wp_bp_020_goal_directed.py tests/test_wp_bp_020_temporal.py --tb=short
```

FINAL_DISPOSITION: READY_FOR_REVIEW
