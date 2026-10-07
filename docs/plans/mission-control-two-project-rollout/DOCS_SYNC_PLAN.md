# Documentation sync plan (draft, phase 1)

Status: **applied in phase 2 (2026-10-03)** to README.md, the operator guide,
implementation status, docs/knowledge (persistence, qualification, operations, index,
log), root/postgres/bootstrap/tests AGENTS.md, three active Cursor rules and the removal
guide. Rows below remain the checklist; final test counts are left to the lead in the
implementation status "Final qualification results" section only. The original draft
follows. Owner: qualification/docs teammate. Inputs: [implementation plan](IMPLEMENTATION_PLAN.md)
sections 8, 9, 12 and 13, the [G1 contract freeze](G1_CONTRACT_FREEZE.md) and the
current tree (2026-10-03). Each edit below names its precondition; an edit whose
precondition is unmet stays unapplied and the old text keeps its blocked-gate wording.
No edit may claim a live installation, seed receipt or production readiness that a
recorded receipt does not prove.

Already applied in phase 1 (link hygiene only): `docs/README.md` (deleted-tree
paragraph replaced, rollout plan linked), `docs/REMOVAL_GUIDE.md` (inventory,
Mongo table, pending deletions), the two `.cursor/rules/_briefings` files and three
MCP Markdown notes (links to the 85 deleted documents repointed or marked historical
with `git show f6521c1:<path>`), plus `docs/tools/check_links.py`.

## 1. README.md

| Location | Current text | Replace with | Precondition |
| --- | --- | --- | --- |
| Lines 45-47 | "Local execution uses explicit `transitional_local` storage and reports `production_ready: false`. Production common-schema startup remains blocked ..." | "Startup accepts only `storage_mode = \"production_common\"`: it verifies the persisted installation identity, the complete attested common release (`mission_control` + `mission_control_search`), writer compatibility and restricted pool roles, and fails closed otherwise. There is no transitional fallback." Keep one sentence that live targets are not installed until their receipts exist. | Repository conversion complete (static scan passes) and bootstrap readiness tests green |
| Lines 13-15 (handoff paragraph) | Links the plan as the "next implementation handoff" | Link the plan, G1 freeze and the per-target evidence index `docs/qualification/two-project/`; keep "does not report a live installation" until G4 receipts exist | Phase 2 |
| "Checks" section | Existing commands | Add `uv run --no-sync pytest tests/qualification/two_project` (requires `MISSION_CONTROL_TEST_ADMIN_DSN`; fails, never skips) and `python docs/tools/check_links.py` | Suite passing |
| New short paragraph | none | Name `packages/mission-control-db-contract/` (`mission-db`) as the sole common SQL owner and `deployments/<app>/` as the app manifests | G2 release exists |

## 2. docs/MISSION_CONTROL_LOCAL_API.md (operator guide)

| Location | Edit | Precondition |
| --- | --- | --- |
| Lines 7-10 | Replace the `transitional_local` / `belllabs_control` paragraph with the `production_common` readiness contract (identity, attestation, fingerprint `mc-pg-catalog-v2`, writer `mission-control-runtime/1`, role checks from `bootstrap/common_installation.py`) | Bootstrap lane final |
| Lines 30-58 example | `required_component_version="transitional-local-v1"` -> `"1.0.0"`; `storage_mode="transitional_local"` -> `"production_common"`; `installation_id` must equal `deployments/<app>/target.toml` | Same |
| Lines 74-95 "Prepare a disposable local database" | Replace the legacy runner + `belllabs_control_runtime` / `belllabs_family_repository_writer` text with: start a loopback PostgreSQL 17 + pgvector; provision `extensions.vector`; `mission-db release-build` / `lock`; `inspect` -> `plan --out plan.json` -> `apply --expected-plan-digest ... --confirm-target <project_ref>:<installation_uuid>` -> `verify`; login roles get exactly one capability membership (`mission_control_runtime`, `_family_writer`, `_catalog_writer`, `_outbox_worker`, `_readonly`); family writer login never in runtime | `mission-db` `--help` tested, G2 |
| New section "Target manifests" | `deployments/<app>/target.toml`: format_version 1, `[target]` fields (app, application_id, project_label, project_ref, installation_id UUIDv7 allocated once, environment, database host/port/name/user, `database_url_env` NAME only) and `[approval]` (`approved_by`, `identity_evidence`); placeholders (`REPLACE`) are refused; credentials never in files/CLI args | Lead confirms manifest schema final |
| New section "Runtime phase" | `mission-db runtime-plan` / `runtime-apply --descriptor ... --confirm-target ... --receipt-out ...` provisions `mission_control_runtime` (LangGraph saver/store, concurrent indexes, separate session lock); startup never calls `setup()`; invalid-index recovery is inspection, not blind retry. State the Agent Server private-schema topology blocker | Runtime persistence lane final |
| New section "Seeds" | `mission-db seed-plan` / `seed-apply`; seed keys `mc.installation`, `mc.catalog.workflow-parity`, `mc.catalog.runtime-profiles`, `mc.catalog.approved-assets`, `mc.app.bindings`, optional `mc.qualification.parity`; replay/conflict semantics; seeds run after verify | Seed bundles exist |
| New section "Storage reconciliation" | Buckets `capability-bundles`, `mission-artifacts` (private, app-local); exact `storage.objects` policy additions under an allowed-differences manifest; `knowledge-artifacts` stays domain-owned; no global policy replacement | DB lane storage reconciliation implemented |
| New section "Protected-data evidence" | `mission-db snapshot` / `compare --allowed` / `qualify --phase before|after --out-dir docs/qualification/two-project/<target>/<run-id>/`; quiescence/attribution rule for live writers | Same |
| Readiness section (lines 107-108) | `/health/ready` reports `storage_mode`, `component_version`, `schema_fingerprint`, `production_ready` from real checks | Bootstrap final |
| Lines 207-210 Agent Server | Keep `agent_server/langgraph.json`; add that local profile proof is not production native persistence | none |
| Lines 260-261 | Replace "qualifies the explicit transitional local adapters" with the qualification-suite statement and its environment gate | Suite passing |

## 3. docs/MISSION_CONTROL_IMPLEMENTATION_STATUS.md

| Location | Edit | Precondition |
| --- | --- | --- |
| Lines 3-10 header | Keep the historical 1,343 / 23 / 2 counts labelled "pre-common-component baseline (f6521c1)"; add a separate, non-additive line for the two-project qualification and full regression on the common release with exact counts | Phase 2 runs |
| Line 21 | "Installation tooling: sibling `../biotech-postgres-db-contract`, CLI `biotech-db`" -> "`packages/mission-control-db-contract` (`mission-db`); `biotech-db` is a thin consumer" (or "replaced", per DB-lane decision) | Wrapper decision |
| Line 46 row "Installation/migration tooling" | Cite the new package tests and `tests/qualification/two_project/test_release_parity.py`; keep the sibling's 22 tests as history | G2 |
| Line 47 row "Common production schema" | Replace "runtime deliberately rejects unavailable `production_common`" with the actual status: release version, fingerprint, disposable proofs passed/failed, live targets not installed (or receipts) | G2/G3 |
| Lines 93-95 gate 1 | Remove the `ai-engineer-db-contract` ownership statement (superseded by D09 amendment); new gate text: "release `mission_control` 1.0.0 from `packages/mission-control-db-contract`; two disposable proofs; per-target G4 approval" | Now true per G1 (can apply in phase 2 regardless) |
| New gate | Agent Server private schema `mission_control_agent_server` blocked: the pinned server cannot use a private schema (owner topology decision required) | Record as blocker |
| New gate | Legacy `capability_search` (Biotech, about 542 rows) and `belllabs_*` data: separate persistent-data authorization; not part of source cleanup | none |

## 4. docs/knowledge (OKF bundle)

| File | Edit | Precondition |
| --- | --- | --- |
| `persistence.md` lines 21-27 | Replace transitional paragraph: common `mission_control` (business authority) + `mission_control_search` (rebuildable projection) installed by `mission-db`; `mission_control_runtime` for LangGraph saver/store via the runtime phase; scope via `adapters/postgres/scope.py` (`mc.installation_id`, `mc.application_id`, `mc.tenant_id`, `mc.actor_ref`), forced RLS with `ctx_*()` functions; `belllabs.request_scope` retired | Conversion complete |
| `persistence.md` citations | Replace "Migration source: `src/mission_control/adapters/postgres/migrations/`" with the package migrations; keep the legacy chain as "historical, pinned by `docs/organization/legacy-belllabs-control-chain.json`"; replace `../biotech-postgres-db-contract/` with the package path | Same |
| `qualification.md` line 21 | Replace "Remaining gates include the released common schema" with the actual gate list and link `tests/qualification/two_project/` | Suite passing |
| `operations.md` | Add `mission-db` phases and `deployments/<app>/target.toml` pointer (operator guide remains the detail) | Operator guide updated |
| `architecture.md` | Add the `packages/` and `deployments/` roots to the ownership table | none |
| `index.md` | No new concept needed unless a "Common component" concept is added; if added, one row "Install or verify the common release" | Optional |
| `log.md` | New dated entry: common component authored, transitional storage exited (only once true), removal inventory, link checker | Phase 2 |

Validate with the neighbouring OKF validator (`../biotech-knowledge-catalog/tools/validate_okf.py`) and `python docs/tools/check_links.py`.

## 5. Scoped AGENTS.md files (keep each under 8 KiB)

| File | Edit | Precondition |
| --- | --- | --- |
| `AGENTS.md` (root) | "Persistent records or migrations" row: add `packages/mission-control-db-contract` (common SQL, `mission-db`) and `deployments/<app>`; Verify: add the qualification selection | G2 |
| `src/mission_control/adapters/postgres/AGENTS.md` lines 7, 12-13 | "migrations/ contains the ordered transitional application migration history" -> "historical transitional chain, pinned by the legacy-chain archive, test-only, never applied to live"; replace "Local belllabs_control proof does not certify production_common" with "Every statement is schema-qualified to `mission_control`/`mission_control_search` and runs after `scope.apply_scope` inside an explicit transaction" | Conversion complete |
| `src/mission_control/bootstrap/AGENTS.md` line 16 | "Production common-schema mode fails closed until its released component ..." -> describe `common_installation.py` readiness checks; no transitional mode exists | Bootstrap final |
| `tests/AGENTS.md` | Add: `tests/qualification/two_project` is the reviewer-owned independent proof; `common_db` tests fail without `MISSION_CONTROL_TEST_ADMIN_DSN` | none |
| `packages/mission-control-db-contract/` | Add a scoped AGENTS.md only if the DB lane wants one (its owner writes it) | DB lane |
| `docs/AGENTS.md` | Mention `docs/tools/check_links.py` and `docs/organization/*inventory*.json` | none |

## 6. Active Cursor rules (.cursor/rules)

| File | Edit | Precondition |
| --- | --- | --- |
| `api-runbook.mdc` line 24 | "Local transitional readiness is not production common-schema readiness" -> "Readiness is `production_common` only; local disposable proof is not live evidence" | Bootstrap final |
| `tech-stack-authority.mdc` lines 18-19 | Replace transitional-schema sentence with common component ownership (`packages/mission-control-db-contract`) and the live-target gate | G2 |
| `codebase-organization.mdc`, `project-organization.mdc` | Add `packages/` and `deployments/` roots | none |
| `_briefings/02-codebase-organization-briefing.md` | Body still describes an `app/` package map (old organization); rewrite against `src/mission_control` or mark the briefing historical | Owner preference |
| `wp-bp-010-stagegraph.mdc`, `wp-bp-020-goal-directed.mdc`, `parallel-blueprint-worktrees.mdc` | A 2026-10-03 grep found no references into the deleted trees; re-grep in phase 2 and decide whether these WP-era rules stay active | Owner decision (rules are user tooling) |

## 7. Machine-readable and evidence consistency (phase 2 checks)

- `docs/organization/removal-inventory.json` regenerated; every "keep-until-parity" row
  re-evaluated against its precondition; nothing deleted without its receipt.
- Per-target evidence directories under `docs/qualification/two-project/<target>/<run-id>/`
  contain exactly the plan section 12 files; no DSNs, rows or secrets.
- G1 freeze link `persistence-map.json` resolved (file merged by the lead).
- Instruction budgets: root/scoped AGENTS.md below 8 KiB; Cursor rules under ~120 lines.
