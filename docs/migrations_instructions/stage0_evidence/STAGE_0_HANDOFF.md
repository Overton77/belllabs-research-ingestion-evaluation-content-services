# Stage 0 handoff — architecture baseline and qualification

Status: BLOCKED_ON_EXTERNAL_STATE  
Prepared by: Cursor coding agent  
Prepared at: 2026-08-04  
Repository/worktree: `biotech-research-ingestion-evaluation-system`; dirty before stage  
Base revision: `6e49ef1e49670c626956bfe0a9b1e65699dd279b`  
Result revision or diff ref: uncommitted worktree

## Outcome

D-01–D-16 are owner-accepted, the current backend baseline is reconciled, an exact
independent migration lock and disposable local Agent Server spike exist, and several
architecture/API defects were found before production contracts were written. The
stage cannot pass its mandatory gate until database, Cloud entitlement, persistence,
restart, sandbox, MCP, async-subagent, middleware, and production-like build proofs
are completed.

## Scope completed

- S0-R01: owner decision package for D-01–D-16.
- S0-R02–R03: reproducible worktree/toolchain/static/test baseline.
- S0-R04–R05: exact independent dependency lock without changing/removing legacy
  dependencies.
- S0-R06–R09: disposable-only changes, explicit flags/fallbacks, no schema/deployment/
  destructive action.
- Local portions of Q01–Q16 documented in the requirements/evidence matrix.

## Explicitly not completed

- Cloud entitlement, region, Serverless/Dedicated, quota, cold-start, maximum wait,
  revision, managed persistence, and Sandbox proofs.
- Live PostgreSQL transaction/crash/RLS/grant and digest-verified Mongo
  claim/settlement backfill.
- N/N+1 persisted checkpoint and blue/green endpoint drill.
- End-to-end DB/model/MCP/Sandbox/Store/stream cancellation proof.
- Real process-restart interrupt recovery and server concurrent-run strategies.
- Remote Streamable HTTP MCP auth/schema/elicitation/session proof.
- Async-subagent remote lifecycle/crash/orphan/capacity/tenant proof.
- Actual Store and Sandbox isolation/expiry/deletion/egress proof.
- Full middleware-order/duplicate/conflict/failure proof.
- Live trace-shape/tenant/redaction inspection and evaluation dataset thresholds.
- `langgraph build`/`langgraph up`.
- QuickJS, PTC, and dynamic delegation, which are owner-disabled optional tracks.

## Owner decisions and assumptions

| ID | Decision/assumption | Source/actor | Scope | Revisit trigger |
|---|---|---|---|---|
| D-01–D-16 | Accept recommended bundle with D-11 exact-API amendment. | owner, Stage 0 interview | migration | accepted spec conflict |
| O-DEPLOY | CLI-managed Standard/Serverless staging; Dedicated evidence-driven. | owner | Stages 0/8 | entitlement or SLO evidence |
| O-META | Keep `biotech-meta` read-only. | owner | Stage 0 | explicit authorization |
| O-CLOUD | Local qualification only; no deployment or purchase. | owner | Stage 0 | explicit Cloud authorization |
| O-OPTIONAL | QuickJS/PTC/dynamic disabled; async subagents required. | owner | migration | owner amendment or failed Stage 6 gate |
| A-DATA | Existing non-production research/test run classes may use the new interfaces; PHI/raw private corpora remain excluded pending formal policy. | owner statement + conservative interpretation | pre-staging | data-classification decision |

## Changes

| Area | Files/migrations/config | Behavioral effect |
|---|---|---|
| Decisions | `stage0_evidence/01_DECISION_PACKAGE.md` | accepted architecture contract |
| Baseline | `stage0_evidence/02_RECONCILED_BASELINE.md` | reconciled current-state evidence |
| Compatibility | `stage0_evidence/03_COMPATIBILITY_AND_CAPABILITY_MATRIX.md` | exact pin/API/maturity proposal |
| Gate evidence | `stage0_evidence/04_REQUIREMENTS_AND_QUALIFICATION_EVIDENCE.md` | atomic status and command record |
| Disposable spike | `spikes/stage0/**` | local-only exact-package proofs |
| Trace test | `tests/test_langsmith_tracing.py` | adds synthetic-PHI sentinel assertion |
| Artifact hygiene | `.gitignore` | ignores local Agent Server state and Windows `NUL` artifacts |
| Database | none | no schema or data change |
| Production runtime | none | no production graph/runtime abstraction |

## Contract and compatibility impact

- No production schema or digest changed.
- D-05 is amended so async subagents and linked runs use explicitly bound child
  threads rather than sharing the BellLabs run's parent thread.
- D-08 is amended to include one-to-many runtime attempts/tasks and exact graph
  assembly/state-schema compatibility digests.
- D-11 is amended to the exact `langgraph-sdk==0.4.2` contract:
  `execution_runtime` returns the execution variant for `threads.create_run` and
  `None` otherwise; only that execution variant carries `context`.
- D-13 is amended under source precedence: PostgreSQL receives effect
  claim/attempt/usage/settlement authority, while the immutable semantic Operation
  Execution Binding remains MongoDB/Beanie-authoritative and is referenced by stable
  identity/digest.
- `mcp==1.29.0` must be exact with `langchain-mcp-adapters==0.3.1`; unconstrained
  resolution to MCP 2.0 is import-incompatible.
- Deep Agents 0.7.4 default tools differ from local skill prose; the pin's inspected
  graph/tool surface is authoritative.
- Checkpoint compatibility is a declared manifest/blue-green contract, not yet an
  operational proof.
- Provider/runtime identities remain distinct from BellLabs identities.

## Data and migration status

- Applied/not applied: no migration applied.
- Backfill/rollback status: amended design direction accepted; implementation absent.
  The backfill targets Mongo claim/settlement records, not semantic binding authority.
- RLS/grant verification: current root suite passes; new operation journal not tested.
- Destructive actions: none.

## Feature maturity and flags

| Capability | Version | stable/beta/preview | Flag/default | Fallback | Evidence |
|---|---|---|---|---|---|
| Standard Agent Server | API 0.12.0 local pin | stable target | `LANGGRAPH_RUNTIME_ENABLED=false` | Temporal | local dev/run |
| Sync subagents | Deep Agents 0.7.4 | stable | disabled until Stage 5 | linked run | surface |
| Async subagents | Deep Agents 0.7.4 | preview | disabled until Stage 6 | linked/sync | surface only |
| QuickJS call/PTC | 0.3.5 | beta | disabled | native tool | deferred |
| Dynamic subagents | 0.3.5/0.7.4 | beta | disabled | compiled child | deferred |
| LangSmith Sandbox | LangSmith 0.10.15 | entitlement-dependent | disabled | unsupported/legacy sandbox | surface only |
| Procedural Store | Agent Server-managed | non-authoritative | default deny | context refs | pure model only |

## Verification evidence

| Command/drill/experiment | Environment | Result | Evidence ref |
|---|---|---|---|
| root Ruff/mypy/pytest | local Windows | pass | `02_RECONCILED_BASELINE.md` |
| exact spike lock/imports/tests | local Python 3.12.7 | pass | `04_REQUIREMENTS_AND_QUALIFICATION_EVIDENCE.md` |
| local Agent Server auth/custom route/native API/run | local in-memory server | pass | Q02/Q16 log summary |
| Docker Compose/build/up | local Windows | blocked, daemon absent | BL-02 |
| Cloud platform/Sandbox | not authorized | blocked | BL-03 |

## Failures, skips, and residual risks

| Item | Reason | Gate effect | Owner/follow-up |
|---|---|---|---|
| Mandatory external spikes | Docker/Cloud unavailable or unauthorized | Stage 0 cannot be READY_FOR_REVIEW | rerun in qualified environment |
| Data retention/deletion/encryption thresholds | no accepted policy | staging blocked | owner/security |
| MCP 2.0 resolution | adapter incompatibility | exact 1.29.0 pin required | Stage 2 lock |
| Custom route native-auth assumption | unauthenticated local custom route initially returned 200 | custom routes must explicitly share principal enforcement | Stage 2 |
| Async subagent preview lifecycle | surface only | required track remains disabled | Stage 6 |
| Eight root integration skips | services absent | live authority proof absent | environment |
| Baseline deprecations | pre-existing | no immediate gate failure; upgrade risk | baseline issue register |

## Security and data handling

- No secret value was read, logged, or committed.
- No PHI was used; synthetic sentinel only.
- Native Agent Server resources and custom spike route reject missing auth.
- Store model denies cross-tenant reads and scientific authority.
- Sandbox/network/mount/ambient-credential guarantees are not proven.
- Checkpoint encryption/retention/deletion and trace retention remain policy blockers.

## Operations and rollback

Stage 0 adds no enabled production behavior. Rollback is deletion of the disposable
spike/evidence diff. The root dependency lock, databases, Temporal workers, and current
admission/runtime paths are unchanged. Do not delete migrated evidence or change
runtime routing as part of rollback.

## Configuration keys still requiring pinned CLI/platform verification

- `langgraph.json`: `auth.path`, `auth.openapi`, `http.app`,
  `http.middleware_order`, optional `checkpointer.path`, optional `store.path`, and
  the production base-image/API compatibility pin.
- LangSmith: `LANGSMITH_API_KEY`, `LANGSMITH_ENDPOINT`, `LANGSMITH_PROJECT`,
  `LANGSMITH_WORKSPACE_ID`, and `LANGSMITH_TRACING`. The owner reports the applicable
  values are present locally; Stage 0 did not read or publish them.
- Provider secrets: `OPENAI_API_KEY` and `ANTHROPIC_API_KEY` remain deployment-secret
  inputs, never contract/state fields.
- Disposable local spike only: `STAGE0_LOCAL_AUTH_TOKEN`,
  `STAGE0_LOCAL_REQUEST_SCOPE`, and `STAGE0_LOCAL_ENVIRONMENT`. They have no
  production meaning.
- Feature flags listed in `spikes/stage0/capability_manifest.json` are contract inputs
  for later stages, not active root settings yet.
- Exact CLI deployment-type, base-image, secret-forwarding, revision, and ownership
  flags must be re-read from the accepted Stage 2/8 CLI pin before implementation.

## Next-stage entry assessment

| Entry criterion | Met? | Evidence/blocker |
|---|---|---|
| D-01–D-16 accepted | yes | decision package |
| Mandatory Q01–Q09/Q14–Q16 pass | no | external/integration rows blocked |
| Q03 transaction/migration direction accepted | direction yes, proof no | D-13/Q03 |
| Context/async thresholds accepted | context yes; full async no | Q05/Q06 |
| Preview flags and fallbacks explicit | yes | capability manifest |
| No unqualified production abstraction | yes | diff |
| Outgoing handoff accepted | no | owner/gate reviewer pending |

Stage 1 is not authorized by the written gate yet.

## Recommended first actions for next agent

1. Start Docker Desktop and rerun Compose health plus PostgreSQL/Mongo integration
   prerequisites without resetting volumes.
2. Execute Q03 against a disposable PostgreSQL schema and a disposable Mongo source
   collection; capture crash/RLS/grant/backfill evidence.
3. Run persisted checkpoint restart, N/N+1 endpoint, interrupt, and concurrent-run
   drills.
4. Obtain explicit local use of existing LangSmith workspace entitlements (still no
   deployment/purchase unless separately authorized) and inspect Serverless/Dedicated/
   region/quota/Sandbox/revision facts.
5. Complete remote Streamable HTTP MCP and async-subagent lifecycle proofs.
6. Accept data classification, encryption, retention, deletion, and staging thresholds.
7. Rerun full root and spike checks, then request independent gate review.

## Gate recommendation

BLOCK

Reason: architecture direction and local qualification are strong enough to preserve,
but mandatory Stage 0 operational/database/platform evidence is absent. Advancing to
Stage 1 would weaken the explicit gate.

## Stage acceptance record

Candidate requirement matrix digest:
`sha256:b473650eafe2a113670349c6b719331703ad17f0eebd2f337e3b1d802bc4cfc3`

```text
gate_disposition:
accepted_by:
accepted_at:
accepted_requirement_matrix_digest:
deferred_tracks: quickjs_call, quickjs_ptc, dynamic_subagents
required_follow_ups:
next_stage_authorized: no
```
