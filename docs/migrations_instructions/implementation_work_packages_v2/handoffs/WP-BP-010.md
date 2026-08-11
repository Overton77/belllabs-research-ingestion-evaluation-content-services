# WP-BP-010 implementation handoff

Disposition: `ready_for_review`

## Kickoff and recovery context

- Repository: `biotech-research-ingestion-evaluation-system` on `main`
- Work package/branch: `WP-BP-010` (sequential finish after parallel integration)
- Integrated base after foundation third amendment + merge: `9453eec`
- Integrator atomic-switch commit: `cce7e47`
- Canonical metadata and accepted CP-030/040/045 evidence remain prerequisites

## Ownership

- StageGraph regions in shared control-plane/orchestration contracts
- `app/domain/orchestration/interpreter.py`
- `app/temporal/workflows/stagegraph.py`
- StageGraph decision/application seams in `app/application/orchestration.py`
- Focused StageGraph V2 tests

## Progress since foundation amendment

- Foundation `ApplyAuthorityBatchAction` is adopted in
  `StageGraphDecisionService.decide_result`: one family admission now commits usage
  settlement plus admitted output/obligation evidence in a single reducer-authorized batch.
- `register_stagegraph_family_mutations` now enables `apply_authority_batch` with nested
  usage/output/obligation batch policy and a StageGraph batch binding validator.
- Worker composition registers StageGraph family mutations by default.
- Legacy flat `app/temporal/stagegraph_workflow.py` deleted after active imports retargeted to
  `app/temporal/workflows/stagegraph.py`.
- Legacy suite `tests/test_stagegraph_orchestration.py` is module-skipped in favor of
  `tests/test_stagegraph_v2.py`.

## Remaining before acceptance (not blocking ready_for_review)

- Agent Server StageGraph macro graph deletion remains coordinated with GoalDirected Agent Server
  deletion and health/registry updates.
- Broader Temporal time-skip/replay/cancellation/continuation suites and credential-gated live
  vertical remain pending.
- Do not claim `accepted` until integrator combined gate and evidence publication.

## Focused verification

```text
uv run pytest -q tests/test_stagegraph_v2.py tests/test_atomic_family_admission.py --tb=short
79 passed
```

FINAL_DISPOSITION: READY_FOR_REVIEW

## 2026-08-11 post-merge implementation audit

The earlier handoff was not authoritative for acceptance. Its listed gaps were real, and the
post-merge code also lacked accepted stage-cycle execution and exact generation-aware cycle-result
routing. The audit completed the canonical V2 implementation, removed the legacy production
registrations/tests, added real operation materialization, incremental Temporal qualification,
bounded stage/workflow cycles, minimal invalidation/reuse, waits, cancellation reconciliation,
Continue-As-New, and obligation/liability completion coverage.

Current evidence is
`docs/migrations_instructions/evidence_v2/WP-BP-010/README.md`. The implementation is complete,
but this handoff still must not be promoted to `accepted` until the credential-gated live vertical
and a non-Windows worker-replacement qualification run are recorded.

FINAL_DISPOSITION_2026_08_11: IMPLEMENTATION_COMPLETE_QUALIFICATION_PENDING

## 2026-08-11 qualification closure

The credential-gated API-to-real-LLM vertical passed and captured Temporal-history proof of
incremental downstream release. The native Linux/WSL Temporal qualification also passed the
stop-original-worker/start-independent-replacement recovery scenario. A final canonical audit
added deterministic no-progress cycle stopping, removed the linked-run direct-family bypass, and
made root result deserialization explicit. Evidence and every `REQ-BP-SG-*` traceability row are
published as accepted in `docs/migrations_instructions/evidence_v2/WP-BP-010/README.md`.

FINAL_DISPOSITION_2026_08_11_QUALIFIED: ACCEPTED
