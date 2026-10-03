# RRM-010 test-stability items

Branch `wp/rrm-010-stability`, based on integration `5d38f19`. Test-only changes; no `app/` change and no product bug found.

## 1. Wall-clock timeouts in `tests/integration/temporal/test_rrm_007_boundary_interventions.py`

- Root cause (confirmed by reading the code): the waits were fixed wall-clock ceilings (`wait_for` 30/60/120 s, `until` default 30 s, 8 s inside the holding `complete` activity) on a time-skipping server whose real work takes about 10 s alone. `until` was also bounded by an iteration count (`seconds * 10` polls of `sleep(0.1)` plus the predicate's own latency), so a slow predicate shortened the wait below the stated seconds. Host load stretches both.
- Fix: one `WAIT_SECONDS = 180` and `RESULT_SECONDS = 300` ceiling for every wait. `until` now uses a monotonic deadline. Each wait still returns as soon as its condition holds, so a passing run is no slower and a genuine hang still fails, only later. No assertion was changed and no retry was added. `tests/acceptance/control_plane/test_rrm_007_interventions.py` imports `until`, so it inherits the deadline fix.

## 2. Race in `tests/acceptance/control_plane/test_rrm_007_interventions.py` ("paused is None on worker 2")

- Root cause (inferred from the code, not reproduced): the boundary activity records the `applied` receipt and the PAUSED projection atomically, before the family's workflow task has consumed the activity result. The test stopped worker 1 as soon as run control showed PAUSED. If worker 1 stopped before the activity result reached Temporal history, worker 2 started with the family's in-memory `paused` still `None` while the activity waited to be retried, and the immediate `boundary_state` query saw `None`. This is a test-only race. The pause is not lost: the activity is re-run and the receipt is idempotent, so no product race was found. I did not confirm that idempotence by running a forced-retry case.
- Fix: before leaving worker 1, wait until the family's own `boundary_state` query reports `paused is not None` (`_family_recorded_pause`). Every later assertion is unchanged.

## 3. `tests/unit/integrations/test_langsmith_tracing.py::test_settings_expose_langsmith_contract`

- Root cause (confirmed): the test read `get_settings()`, which is `lru_cache`d and reads the developer `.env` and the process environment. Two ambient sources broke it: a developer `.env` with LangSmith values, and `test_configure_langsmith_tracing_exports_env`, which exports `LANGSMITH_*` into `os.environ`. The `monkeypatch.delenv(..., raising=False)` it used does not record a variable that was absent, so the exports leaked into later tests.
- Fix: new `tests/fixtures/isolated_settings.py` builds `Settings(_env_file=None, ...)` from explicit required values. The langsmith tests use it, and the contract test now also asserts `langsmith_tracing is False` and `langsmith_api_key is None`, which strengthens it. An autouse fixture in that module clears and restores every `LANGSMITH_*` variable through `monkeypatch`.
- Other tests that read the ambient `Settings`, fixed the same way:
  - `tests/unit/config_api/test_config.py` asserted defaults from `get_settings()`. It now uses `isolated_settings()`. While there I found `test_coordinator_settings_reject_floating_skill_package_and_wrong_embedding` passed vacuously, because the re-validated payload lacked the `NEO4J_URI` alias and so failed on a missing field. The payload now supplies the alias and first asserts that the unmodified payload validates.
  - `tests/unit/operations/test_rrm_009_cancellation_composition.py` (two tests) asserted default heartbeat and drain values from `get_settings()` plus env mutation. They now pass explicit overrides to `isolated_settings`.
- Reviewed and left alone: `Settings()` in `test_control_plane_payloads.py` (the S3 client is faked and the settings are never read); `test_capability_pins_and_runtime_ports.py` (sets its own env explicitly and clears the cache); `test_coordinator_mcp_http_deployment.py` and `test_coordinator_security_audit.py` (`model_copy` of values the assertions do not depend on).

## 4. Sweep for other flakes

No failure occurred in any run below, so there is nothing further to diagnose. The hash-seed lesson from RRM-004 was applied by varying `PYTHONHASHSEED` across runs (unset/random, `12345`, `777`).

Gates on the final commit: `ruff check app tests` clean, `mypy app` clean, `git diff --check` clean. `mypy` on the two RRM-007 test files reports the same 11 pre-existing errors as before the change (`list[object]` activities and an optional `.action`); none are new.

| Run | Scope | Environment | Result |
|-----|-------|-------------|--------|
| H1 | full hermetic | DSNs unset | 1091 passed, 93 skipped, 2 xfailed (228 s) |
| H2 | full hermetic | DSNs unset | 1091 passed, 93 skipped, 2 xfailed (203 s) |
| H3 | full hermetic | DSNs unset, `PYTHONHASHSEED=12345` | 1091 passed, 93 skipped, 2 xfailed (200 s) |
| D1 chunk A | `--ignore=tests/acceptance` | DSNs, developer `.env` via `uv run --env-file`, LIVE=0, no `LANGSMITH_TRACING` forced | 1085 passed, 27 skipped, 2 xfailed (323 s) |
| D1 chunk B | `tests/acceptance` | same | 63 passed, 9 skipped (394 s) |
| D1 total | | | 1148 passed, 36 skipped, 2 xfailed |
| D2 chunk A | `--ignore=tests/acceptance` | same, `LANGSMITH_TRACING=false`, `PYTHONHASHSEED=777` | 1085 passed, 27 skipped, 2 xfailed (308 s) |
| D2 chunk B | `tests/acceptance` | same | 63 passed, 9 skipped (391 s) |
| D2 total | | | 1148 passed, 36 skipped, 2 xfailed |

Baselines matched exactly: hermetic 1091/93/2, DSN 1148/36/2. The two RRM-007 files alone: 7 passed (31 s). The run held the stack lock as `RRM-010S`; no container was stopped or pruned.
