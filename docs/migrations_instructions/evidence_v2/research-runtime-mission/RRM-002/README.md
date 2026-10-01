# RRM-002 implementation evidence

Disposition: accepted (see Final disposition for scope and unresolved external gates)
Recorded date: 2026-10-01 (America/New_York)
Qualification identity: RRM-002 verification-baseline repair; no new lifecycle semantics
Base revision and head revision: base `687c1b7debbd427b3cfed4a982d82bf5d4b8d441` (`main` = `integration/research-runtime-mission` at kickoff). Tested code head: `ea0f529816bd36c9daba2fe1bde979eeb5743d94` (repairs) on top of `858c721a1f9b478b2a3dcc75e1d5a6810824ce44` (Ruff style only). The evidence/ticket commit follows on `wp/rrm-002-baseline`, which is merged `--no-ff` into `integration/research-runtime-mission`.
Framework/package baseline: `uv sync --frozen` from the committed `uv.lock`; CPython 3.12.14 (the Codex runtime interpreter that the main checkout's `.venv` uses), pytest 8.4.2, pydantic 2.13.4, ruff 0.15.22, mypy 1.20.2

## Worktree provenance

- Repository: `biotech-research-ingestion-evaluation-system`
- Worktree: `C:\Users\Pinda\Proyectos\Biotech\biotech-research-ingestion-evaluation-system-rrm-002`, branch `wp/rrm-002-baseline`
- Integration branch: `integration/research-runtime-mission`, created from `main` at `687c1b7`
- The worktree is a sibling of the main checkout on purpose. `app.config.PROJECT_ROOT.parent` and several tests resolve workspace siblings: `../biotech-kg`, the workspace `.agents/skills/agent-browser` and `.tools`. A worktree nested under `.scratch/` cannot see them. A first nested attempt was discarded before any edit.
- Dirty paths in the main checkout at kickoff, all left untouched and unstaged: modified `app/api/control_plane.py`, `app/application/control_plane/service.py` and `app/application/runtime/postgres_runtime_execution_repository.py`. Untracked: `app/{application,domain,integrations}/agentic_components/`, `tests/unit/agentic_components/`, `scripts/query_agentic_components.py`, `docs/AGENTIC_COMPONENTS_HARNESS.md`, `docs/DOMAIN_CONTRACTS_WALKTHROUGH.md`, `docs/RUNTIME_LIFECYCLE_INSPECTION_AND_CONTROL_IMPLEMENTATION_BRIEF.md`, `docs/WORKFLOW_RUNTIME_CODE_WALKTHROUGH.md`, `docs/runtime_lifecycle_doc.md`. No agentic-component work was consumed.
- Shared-path note: Ruff's import sort of `app/api/control_plane.py` is byte-identical to the import reorder already in the user's uncommitted diff of that file.

## Implemented contracts and seams

None. RRM-002 restores verification under existing accepted authority. One production regression was repaired (below). No contract, migration, registry or lifecycle semantics changed.

## Requirement-to-evidence map

| RRM-002 requirement | Evidence (test → observed assertion) |
|---|---|
| Reproduce the audit baseline | The clean committed base with `.env` loaded in-process gave 44 failed, 638 passed, 44 skipped. The failing node IDs are identical to `BASELINE_FINDINGS_2026-10-01.txt`. The 7-test pass delta is the uncommitted agentic-component suite. |
| Repair moved authority guard without removing it | `test_wp_cp_020.py::test_domain_owner_has_no_runtime_or_persistence_authority_imports` reads `app/application/operations/journaled_operation_execution.py`. It still asserts no `reduce_lifecycle` and the presence of the Claim, Observe and Settle effect actions. |
| Repair repository-root lookups, keep DML/provenance checks | `test_atomic_family_admission.py::test_migration_has_private_repository_dml_and_no_attachment_function` reads `app/migrations/0017…` and the moved `app/application/run_control/postgres_run_control_repository.py`, with all DML and role assertions unchanged. `test_reviewed_capability_promotion.py` (7 tests) reads the real `app/domain/coordinator/reviewed_payloads` and the workspace `agent-browser` skill; no payload was invented. `test_quarantine_static_inspection.py` (4 tests) runs the real `scripts/quarantine_static_scan.py` with its non-execution and fail-closed assertions intact. |
| Deterministic snapshot time; production expiry unchanged | `test_sandbox_snapshots.py` restore tests (4) pass the service's existing `clock=` parameter fixed at `NOW + 1h`. `test_restore_uses_trusted_time_and_rejects_cross_scope` still proves expiry at `NOW + 31d`. `sandbox_snapshots.py` is unchanged. |
| Diagnose the 422 before changing anything | The API correctly rejected a pre-CON-BP-GOAL-DIRECTED-V1 blueprint missing 12 required policies. `test_control_plane_publish_route_uses_strict_contracts` now publishes the canonical `GENERIC_GOAL_DIRECTED` fixture (201, revision 1). It also asserts that the legacy shape still returns 422. Validation is unchanged. |
| Classify every failure | See the classification table below; all 44 are covered. |
| Ruff green; mypy green | `ruff check app tests scripts`: All checks passed. `mypy app`: no issues in 332 source files. |
| Service gates explicit and honest | Two experiment tests that silently used the developer `.env` database now require `TEST_APPLICATION_POSTGRES_DSN`. Opted-in Postgres/Mongo suites ran against disposable containers (see Live runtime qualification). |
| Dependent ticket for out-of-scope defects | RRM-012 covers the reference-research journal authority defect. |

### Failure classification (all 44 audit failures)

| Failing tests | Cause | Owner | Disposition |
|---|---|---|---|
| CP-020 domain-authority guard (1) | Source moved to `operations/` by `e83d2ef` | CP-020 acceptance | Path repaired; guard retained |
| Agent Server Block C unit (4), reviewed capability promotion (7), coordinator surface promotion (1), operation-journal backfill unit (1), atomic family admission unit (2), quarantine static inspection (4), live-runner unit (2), schema authority issuance (1), schema catalog core (3), schema workspace (2), coordinator evaluation dataset (1), reference-research fixtures (6) | `e83d2ef` moved tests one or two directories deeper without updating `Path(__file__).parents[N]` or `parent / "fixtures"` | Test infrastructure | Indices corrected to the repository root or workspace root; fixtures resolved under `tests/fixtures` |
| Schema catalog core (2), schema workspace (1), after their path repair | **Production regression:** `app/application/schema/schema_catalog.py` `DEFAULT_SEMANTIC_OVERLAY` used `parents[2]` after moving into `schema/`, so it resolved to the nonexistent `app/schema-catalog/…` | Schema catalog application owner | Fixed to `PROJECT_ROOT / "schema-catalog" / …` |
| Schema workspace tier-0 (1) | Read an ignored local run input, `.scratch/schema-context-selection-runs/live-windows-bind-9/inputs/report.md` | Test fixture | Byte-identical copy promoted to `tests/fixtures/schema_context/` (sha256 `2a67cfa5…893b`) with a provenance README |
| Sandbox snapshot restores (4) | Retention `NOW + 30d` expired against the wall clock | Snapshot tests | Deterministic injected clock |
| Control-plane publish (1) | Stale pre-BP-020 GoalDirected request shape | API test | Canonical fixture; legacy shape asserted 422 |
| Web capability catalog seed (1) | Asserted removed v1 `StageNode.depends_on` / `max_parallel_stages` | Test | The same topology is now asserted from v2 `dependencies` and the workflow concurrency `CapacityCeiling` (=2) |
| Coordinator GoalDirected binding (1) | BP-020 routes the verifier OEB through the dedicated `goal_verifier` slot; the test only read `goal_operation_handlers` | Test | Collects the verifier slot; asserts executor ≠ verifier binding |
| Experiment Postgres (2) | Connected to the developer database from `.env` (127.0.0.1:55432) without an opt-in | Test gating | Explicit `TEST_APPLICATION_POSTGRES_DSN` opt-in (skip, or fail under `BELL_LABS_REQUIRE_STAGE3_ENTRY_SERVICES=1`) |
| Reference-research real loader/journal (2) | **Genuine defect beyond scope:** the inactive Stage 0–2 harness settles without the run-control authority required since `69b699b` | RRM-012 | `xfail(strict=True, raises=ValueError)` on exactly these two parameters, citing RRM-012 |

### Additional reproducibility repairs (outside the 44)

- Fresh worktrees have no `.env`, and `app.server` builds `Settings()` at import. `tests/conftest.py` now supplies non-routable `*.invalid` placeholders for required settings, but only when `PROJECT_ROOT/.env` is absent. Explicit environment variables always win. Without this, two modules failed collection and further `Settings()` tests failed.
- Never-run opted-in suites were repaired after diagnosis against disposable services:
  - Mongo control-plane: stale GoalDirected fixture; same cause as the 422.
  - Mongo sandbox snapshots: wall-clock retention; same cause as the four unit restores.
  - Postgres backfill integration: worker and cwd paths moved by `e83d2ef`.
  - Postgres atomic family admission: the ledger filter assumed `command:` reservation identities, and `is_version_final` was read as a nonexistent column. It is now read from the outbox `envelope`, still asserting `[False, True]`.
  - Postgres journal crash: the settlement command lacked the third-amendment evidence and binding authority that production supplies. It now mirrors the production actor and asserts `ACCEPTED`. The stale post-crash `claim is None` and `acquired` expectations were updated, because `69b699b` commits the authority-bound claim first. The crash still proves that the settlement rolls back and that recovery and replay are idempotent.

## Changed paths and migrations

- Production: `app/application/schema/schema_catalog.py` (overlay root). Ruff import order only (`858c721`): 20 `app/` modules, 9 `scripts/` and 16 `tests/` files. `scripts/reorganize_tests.py` has wrapped dict entries; `MOVE_MAP` was verified identical with `ast.literal_eval`.
- Tests: the repairs above plus import-order-only edits. New fixture `tests/fixtures/schema_context/` (report and README).
- Docs: this README, ticket RRM-012, and the RRM-002 ticket/index status.
- Migrations: none. Registrations: none. Deleted owners: none.

## Deterministic verification

All runs used the worktree `.venv`. `run_pytest_with_env.py` loads the main checkout `.env` into the process without copying it and sets the `BELLABS_RUN_WP_*_LIVE` flags to 0 and LangSmith tracing off. Raw logs remain local under ignored `.scratch/rrm-002/`.

| Command | Result |
|---|---|
| Baseline at `687c1b7`, `.env` loaded: `pytest -q` | 44 failed, 638 passed, 44 skipped (IDs = audit) |
| Baseline at `687c1b7`, no `.env` | Collection interrupted: 2 errors (`Settings` required fields) |
| `ruff check app tests scripts` | All checks passed (baseline 104: 56 I001, 48 E501) |
| `mypy app` | Success: no issues found in 332 source files |
| Full `pytest -q`, `.env` loaded | 678 passed, 46 skipped, 2 xfailed, 0 failed |
| Full `pytest -q`, no `.env` (hermetic) | 678 passed, 46 skipped, 2 xfailed, 0 failed |
| Audit focused command (CP-020/030/040/045, runtime recovery/intervention/bootstrap, both pure family suites) | 142 passed (baseline: 1 failed, 141 passed) |
| `git diff --check` | clean |

Skips in the shared offline gate (46): 19 `AGENT_SERVER_ENDPOINT` not configured; 18 `TEST_APPLICATION_POSTGRES_DSN` (including the 2 newly gated experiments); 6 `TEST_MONGODB_URI`; 3 live-provider flags (BP-010, BP-020, CP-040); 1 WSL-only BP-010 recovery; 1 pre-existing retirement skip in `test_schema_grounding_services.py:484`. That skip cites the accepted WP-BP-020 atomic switch; readiness §1 prefers deleting the superseded test, and that cleanup is left to its owner.

## Live runtime qualification

No real-LLM or provider qualification was run. RRM-002 does not require one, and historical BP/CP live evidence is **not** re-qualified today.

Opted-in service suites ran against disposable local containers. `rrm002-app-postgres` is `pgvector/pgvector:pg16` with the repository init SQL on `127.0.0.1:55432/belllabs`; that address is required by the tests' disposable-target guards. `rrm002-app-mongodb` is `mongo:8.0`, a single-node replica set on `127.0.0.1:27017`. Neither uses named volumes, and both were removed after the run. No `.env`, Atlas or developer database was used.

| Command | Result |
|---|---|
| `TEST_APPLICATION_POSTGRES_DSN=<disposable> TEST_MONGODB_URI=<disposable> pytest tests/experiments/test_langgraph_temporal_stagegraph.py tests/integration/mongodb tests/integration/postgres` | First live run ever recorded for these suites: 6 failed, 28 passed before diagnosis; **38 passed** after repair |

The foundation-amendment handoff recorded that these Postgres suites had never run live. This is their first live result. The Agent Server endpoint suite (19) is still not run.

## Replay and recovery artifacts

None new. Existing Temporal replay and recovery suites ran inside the offline gate and passed.

## Replacement and deletion checks

No owner was replaced or deleted. RRM-012 tracks the possible retirement of the inactive reference-research harness.

## Unresolved risks and drift checks

- **RRM-012:** the reference-research harness violates journal authority. It is strict-xfailed (2 cases) and is not mission-blocking.
- Unrun external gates: the Agent Server endpoint (19), live providers (3) and WSL (1). Atlas-backed Mongo was not used.
- The live-runner settings test requires an interpreter with a sibling bundled `node` (the Codex runtime). A uv-managed CPython 3.13 venv fails it for environmental reasons.
- The user's uncommitted change to `postgres_runtime_execution_repository.py` adds a line over 100 characters. It will trip E501 when committed.

## Final disposition

accepted
