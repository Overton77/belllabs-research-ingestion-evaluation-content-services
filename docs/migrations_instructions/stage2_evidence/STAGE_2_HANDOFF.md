# Stage 2 handoff — standard Agent Server foundation

Status: BLOCKED_ON_EXTERNAL_STATE — the local Agent Server foundation is ready for review;
external trace-arrival, production-image, Studio UI, and Stage 1 service-backed acceptance
remain blocked by unavailable disposable infrastructure/deployment identity.

Prepared by: Cursor implementing agent
Prepared at: 2026-08-06
Repository/worktree: `biotech-research-ingestion-evaluation-system`, dirty worktree preserved
Base revision: `main` tracking `origin/main`
Result revision or diff ref: uncommitted worktree

## Delivered

- Reproducible Stage 0-qualified LangGraph, Deep Agents, Agent Server API/runtime, MCP,
  QuickJS, checkpoint, and LangSmith pins in the root lock.
- Stable `belllabs_stagegraph` and `belllabs_goal_directed` graph IDs in `langgraph.json`.
- Import-safe, side-effect-free compiled graphs with stable state, reducer, and node names.
- Exact `GraphAssemblyDefinition`/`RunPlan` selection boundary.
- JWT authentication and default-deny tenant authorization for threads, runs, assistants,
  Store, and crons.
- Shared custom HTTP composition with protected readiness and graph-runtime schema routes.
  Native Agent Server `/ok` remains the sole liveness route, avoiding a custom-route collision.
- Non-mutating readiness reporting plus tested pseudonymization/redaction utilities. Native
  tracing may be enabled only when the official `LANGSMITH_HIDE_INPUTS=true` and
  `LANGSMITH_HIDE_OUTPUTS=true` fail-closed posture is configured; trace arrival still requires
  an owner-approved workspace and synthetic identity.
- Explicit managed-persistence boundary: exported graphs compile without a checkpointer or Store.
- Agent Server environment names and deployment artifact exclusions.
- Authenticated discovery and schema inspection of the two immutable deployment assistants;
  mutable per-tenant assistants are denied in Stage 2.
- Unscoped Store namespace enumeration fails closed with 403 instead of leaking namespaces or
  producing an internal server error.
- Native tracing selects `AGENT_SERVER_LANGSMITH_PROJECT` before handling Agent Server runs.
- JSON round-trip-safe event reducers, proven across an Agent Server interrupt/resume boundary.
- The legacy FastAPI/Temporal/OpenAI Agents paths remain present and passing.

## Stable graph compatibility surface

StageGraph nodes:

1. `admit_runtime_binding`
2. `interpret_next_stage`

GoalDirected nodes:

1. `admit_goal_binding`
2. `bounded_agent`
3. `independent_verifier`

Both state schemas preserve:

- `request_scope`
- `belllabs_run_id`
- `execution_epoch`
- `graph_assembly_digest`
- `run_plan_digest`
- compact `event_refs`

These names must be preserved or versioned in later stages.

## Dependency position

Qualified core pins include:

- `langgraph==1.2.10`
- `langgraph-sdk==0.4.2`
- `langgraph-cli[inmem]==0.4.31`
- `langgraph-api==0.12.0`
- `langgraph-runtime-inmem==0.32.0`
- `deepagents==0.7.4`
- `langchain-mcp-adapters==0.3.1`
- `mcp==1.29.0`
- `langsmith[openai-agents,pytest,sandbox]==0.10.15`

`langchain-openai==1.4.1` is intentionally not promoted into the root lock. It requires
OpenAI >=2.45, which breaks the retained `openai-agents==0.17.8` usage model. The root keeps
OpenAI `<2.45`, preserving the coexistence gate. A future provider-adapter promotion must
qualify an OpenAI Agents upgrade first.

## Verification

Executed locally on 2026-08-05 and re-verified on 2026-08-06:

- `uv sync --frozen`: pass; 201 packages audited.
- `uv run pytest -q`: 434 passed, 9 skipped, 11 warnings.
- `uv run mypy app`: pass; 258 source files.
- `uv run ruff check app tests`: pass.
- focused integrity/security/contract tests: 25 passed.
- post-audit Stage 2 tests: 18 passed, including immutable-assistant authorization,
  trace fail-closed posture, and JSON round-trip reducer coverage.
- `uv run langgraph dev --config langgraph.json --no-browser --no-reload --port 2025`:
  custom auth and HTTP app loaded; both graphs imported; API 0.12.0 and runtime-inmem 0.32.0
  reached healthy startup.
- Native `/ok` returned success and an unauthenticated native assistant request returned 401.
- Synthetic signed-JWT native API drill:
  - both graph assistants were discoverable and both schema APIs returned successfully;
  - StageGraph executed and history returned four checkpoints;
  - GoalDirected streamed five events through the independent verifier;
  - a cross-scope thread create and mutable assistant create both returned 403;
  - the protected readiness route returned `ready`.
- Synthetic interrupt/resume drill suspended before `interpret_next_stage`, exposed that pending
  node through thread state, then resumed to the expected terminal event without replay failure.

No network, database, secret, tracing, sandbox, MCP, or worker resource was created during graph
import or inspection.

## Remaining acceptance evidence

The following cannot be claimed in the current environment:

- migration `0013` clean-apply/upgrade and non-superuser RLS/grant proofs;
- isolated Mongo-to-PostgreSQL backfill count/digest, quarantine, crash/restart, and rollback
  drills;
- trace arrival in an owner-approved LangSmith workspace;
- interactive Studio UI inspection under an owner-approved Studio identity;
- production image/deployment proof.

The native API drill used an ephemeral synthetic RSA issuer and in-memory Agent Server only; it
does not substitute for staging identity evidence. Docker Desktop is not running, and no isolated
`TEST_APPLICATION_POSTGRES_DSN` or `TEST_MONGODB_URI` was supplied. Production deployment,
secret upload, and production backfill remain unauthorized.

## Next safe commands

With isolated targets and an approved synthetic identity configured:

```powershell
uv sync --frozen
uv run pytest -q
uv run mypy app
uv run ruff check app tests
uv run langgraph dev --config langgraph.json --no-browser --no-reload
```

Then run the Stage 1 service-backed migration/backfill suite and the Stage 2 authenticated native
API/stream/interrupt/tracing drills. Do not point those drills at primary Supabase or production
Atlas.

## Owner decisions and assumptions

| ID | Decision/assumption | Source/actor | Scope | Revisit trigger |
|---|---|---|---|---|
| S2-D01 | Standard Agent Server is primary and legacy execution remains available | accepted Stage 0 D-01/coexistence rules | Stage 2 | cutover gate |
| S2-D02 | The two registered assistants are immutable deployment topology; authenticated tenants may inspect them, but cannot create or mutate assistants | safe Stage 2 assumption | Agent Server auth | owner accepts tenant-authored assistant configuration |
| S2-D03 | Managed deployment injects persistence; exported graphs compile without saver/Store | accepted D-07 | graph exports | self-hosted persistence qualification |
| S2-D04 | Trace inputs and outputs are fully hidden before native tracing can be enabled | official LangSmith privacy guidance plus BellLabs data boundary | Agent Server tracing | reviewed field-level anonymizer replaces full hiding |
| S2-D05 | No production deployment, secret upload, or production data migration is authorized | stage mission | external operations | explicit owner authorization |

## Requirements-to-evidence matrix

| Requirement ID | Requirement | Implementation location | Verification/evidence | Status |
|---|---|---|---|---|
| S2-R01 | Reproducible qualified dependency lock | `pyproject.toml`, `uv.lock` | `uv sync --frozen`; dependency version tests | pass |
| S2-R02 | Stable config and graph IDs | `langgraph.json` | pinned CLI startup; config test | pass |
| S2-R03 | Side-effect-free graph imports and managed persistence boundary | `app/agent_server/graphs.py`, graph packages | clean import tests; local server import | pass |
| S2-R04 | Exact assembly/RunPlan selection | `app/agent_server/graph_factory.py` | contract tests | pass |
| S2-R05 | Compact typed schemas and stable nodes/reducer | graph state/node packages, `reducers.py` | schema APIs, node tests, interrupt/resume drill | pass |
| S2-R06 | JWT auth and resource authorization | `auth.py`, `context.py` | signed JWT, 401, cross-scope 403, immutable-assistant 403 tests/drill | pass |
| S2-R07 | Protected custom routes without native collision | `http_app.py`, `app/api/graph_runtime_schemas.py` | route inventory and protected readiness tests | pass |
| S2-R08 | Truthful non-mutating readiness | `health.py` | unit and native API drill | pass |
| S2-R09 | Native tracing cannot export raw I/O | `tracing.py`, `.env.example` | fail-closed configuration tests | pass |
| S2-R10 | Trace arrival, correlation, and workspace inspection | owner-approved LangSmith workspace | not available | blocked_external |
| S2-R11 | Local authenticated native run/stream/history/interrupt | Agent Server in-memory runtime | synthetic signed-JWT E2E drills | pass |
| S2-R12 | Production-like image/deployment | Docker/LangSmith deployment | Docker unavailable; deployment unauthorized | blocked_external |
| S2-R13 | Legacy API coexistence | `app/server.py` and retained adapters | full 434-pass suite | pass |
| S2-R14 | Stage 1 migration/RLS/backfill prerequisites | migrations `0012`/`0013` and Stage 1 repositories | isolated stores unavailable | blocked_external |

## Contract and compatibility impact

- Stable graph IDs: `belllabs_stagegraph`, `belllabs_goal_directed`.
- Stable node and state names are listed above and must be versioned if changed.
- Event reducers now accept JSON-deserialized lists as well as tuples while preserving a
  deterministic tuple result; this fixes checkpoint resume without changing channel semantics.
- No checkpointer or Store is embedded in either exported graph.
- Provider/runtime identity remains qualified through the Stage 1 contracts; no SDK object enters
  the domain layer.

## Data and migration status

- No migration or backfill was applied in this stage.
- No destructive action occurred.
- Migration `0013`, non-owner grants/RLS, and backfill recovery evidence remain a Stage 1
  infrastructure blocker and therefore block final gate acceptance.

## Feature maturity and flags

| Capability | Version | Maturity | Flag/default | Fallback |
|---|---|---|---|---|
| Agent Server/LangGraph | pins above | qualified foundation | `LANGGRAPH_RUNTIME_ENABLED=false` | legacy runtime |
| Native tracing | LangSmith 0.10.15 | guarded | `LANGSMITH_TRACING=false`; hidden I/O required | disabled tracing |
| QuickJS | 0.3.5 | optional/unqualified | no runtime enablement | unsupported |
| Async subagents | pinned transitive surface | preview/deferred | disabled | synchronous/linked-run later |
| Managed persistence | Agent Server managed | deployment boundary | injected only by server | in-memory local test |

## Security and data handling

- Raw bearer tokens are verified but never copied into graph state, resource metadata, traces, or
  prompts.
- Threads/runs use default-deny request-scope metadata filters; Store uses
  `(request_scope, environment, purpose)` namespaces and strict value schemas; unscoped namespace
  listing and crons are denied.
- Deployment assistants are read-only. Tenant-authored assistant configuration is denied until a
  separately governed schema and isolation policy exists.
- Native tracing fails startup when enabled without both input and output hiding. Pseudonymous
  correlation helpers and payload masking remain covered by deterministic tests.
- No secrets, PHI, raw corpora, sandbox, MCP session, or external worker were created by graph
  import or local drills.

## Operations and rollback

- Keep `LANGGRAPH_RUNTIME_ENABLED=false` to route admissions through the legacy path.
- Disable native tracing with `LANGSMITH_TRACING=false`; this does not alter lifecycle authority.
- Remove Agent Server endpoint selection from new exact runtime bindings to stop new launches;
  do not delete run-control records, operation evidence, checkpoints, or historical traces.
- The Stage 2 graph modules and config can be removed without schema rollback because no
  authoritative data migration is owned by this stage.

## Next-stage entry assessment

| Entry criterion | Met? | Evidence/blocker |
|---|---|---|
| Both graph families import, inspect, execute, stream, and resume locally | yes | tests and signed-JWT drills |
| Native resources and custom routes are authenticated | yes locally | signed-JWT drills; staging issuer still required |
| Import/introspection is side-effect free | yes | tests and observed startup |
| Stage 1 database authority proof is accepted | no | disposable PostgreSQL/MongoDB unavailable |
| Trace arrival/masking is observed in approved workspace | no | workspace/key not supplied |
| Production-like build is proven | no | Docker unavailable |

## Failures, skips, and residual risks

| Item | Reason | Gate effect | Owner/follow-up |
|---|---|---|---|
| LangSmith trace arrival not observed | no approved workspace/key | blocks final Stage 2 acceptance | owner/platform operator |
| Studio UI not inspected interactively | no approved Studio identity | blocks Studio acceptance evidence | owner/platform operator |
| Docker image/up proof absent | Docker unavailable | blocks production-like evidence | infrastructure owner |
| Stage 1 service-backed proof absent | no disposable data stores | blocks Stage 2 dependency acceptance | data/infrastructure owner |
| Starlette `TestClient` deprecation warning | upstream transition to `httpx2` | no current functional gate effect | dependency maintenance |

## Gate recommendation

`BLOCK` only on external acceptance evidence; the local implementation is ready for gate review.
Do not authorize Stage 3 production binding or cutover until the blocked rows above are completed
or explicitly accepted by the owner under the global gate rules.
