# RRM-012 — Reconcile or retire the Stage 0–2 reference-research journal harness

**What to build:** make `execute_reference_fixture` settle operation-journal effects through accepted run-control authority, or retire the harness and its executable test with an accepted deletion rationale.

**Blocked by:** None. Discovered by RRM-002 and split out because the repair is outside baseline scope.
**Blocks:** No mission ticket. The harness is not in active composition: Agent Server `graphs` is empty, and the company fixtures run through the BellLabs API and Temporal families.
**Status:** ready-for-agent (repair or retirement decision)
**Branch:** `wp/rrm-012-reference-journal-authority`
**Authority:** accepted CP-020 operation-journal invariants as amended by `69b699b` (apply-authority-batch family admission seam); readiness contract §1 replacement/deletion rules

## Diagnosis (RRM-002, 2026-10-01)

`tests/unit/reference_research/test_reference_research_stage0_2.py::test_real_loader_compiler_executor_journal_and_reconstruction` (both parameters) had been hidden behind a stale fixture path. Once that path was repaired, the test reaches `app/application/reference_research/service.py` `execute_reference_fixture`. That function commits `OperationJournalMutation(settlement=..., expected_run_version=1)` with no `command_result` or `authority_result`. `OperationJournalMutation` validation (`app/application/operations/operation_journal.py`) correctly rejects it with `ValueError: journal settlement must bind its exact accepted authority result`. The invariant is accepted. The harness predates it and was never updated.

Fabricating an accepted `CommandResult` inside the harness would bypass run-control authority and is not an acceptable repair.

RRM-002 marked exactly those two parametrized cases `xfail(strict=True, raises=ValueError)` with a reason that references this ticket. Any other exception type still fails the test. Once the harness is repaired, the strict xfail turns into a failing XPASS and must be removed.

## Acceptance

- [ ] Choose a disposition. **Repair:** admit the reference run through the run-control service and settle each operation through the accepted journaled-execution or apply-authority path, without a synthetic authority. **Retire:** delete the harness, its Agent Server operation module and test under readiness §1, with a recorded deletion rationale and replacement inventory.
- [ ] Remove the strict xfail. The test passes, or it is deleted along with the retired code.
- [ ] `ruff`, `mypy app` and the full offline suite remain green. Record the evidence under `evidence_v2/research-runtime-mission/RRM-012/`.

Out of scope: changing journal authority invariants and company fixture execution.
