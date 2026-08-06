# Stage 0 requirements and qualification evidence

Status values follow the global gate: `pass`, `fail`, `blocked`, or
`deferred_optional_feature_disabled`. A local model/surface test is not promoted to a
Cloud, database, restart, or entitlement proof.

## Requirements-to-evidence matrix

| ID | Atomic requirement | Implementation/evidence | Verification | Status |
|---|---|---|---|---|
| S0-R01 | D-01–D-16 have owner dispositions. | `01_DECISION_PACKAGE.md` | owner interview record | pass |
| S0-R02 | Baseline revision and pre-existing dirty state are recorded. | `02_RECONCILED_BASELINE.md` | Git inspection | pass |
| S0-R03 | Full root lint/type/test baseline is reproducible. | verification log below | Ruff/mypy/pytest | pass |
| S0-R04 | Exact migration dependency proposal is independently locked. | `spikes/stage0/{pyproject.toml,uv.lock}` | `uv lock`; import/tests | pass |
| S0-R05 | Legacy dependencies remain in place. | root `pyproject.toml`/`uv.lock` | diff inspection | pass |
| S0-R06 | No production graph package or schema migration is introduced. | Stage 0 diff | diff inspection | pass |
| S0-R07 | QuickJS/PTC/dynamic delegation are contract-visible and disabled. | capability manifest | JSON inspection | deferred_optional_feature_disabled |
| S0-R08 | Async subagents are required but cannot run before their preview gate. | decision/capability manifests | inspection | pass |
| S0-R09 | No destructive database or deployment action occurs. | command log | inspection | pass |
| S0-R10 | Platform entitlements, regions, quotas, revisions, and Sandbox are measured. | no authorized Cloud run | external workspace proof | blocked |
| S0-R11 | Checkpoint/trace/Store/sandbox retention and deletion thresholds are accepted. | decision package lists conservative interim policy | owner policy decision | blocked |
| S0-R12 | Stage 1 transaction/migration direction is accepted without overriding `biotech-meta`. | amended D-08/D-13 | owner interview plus source-precedence reconciliation | pass |
| S0-R13 | Mandatory architecture-invalidating spikes pass. | Q01–Q16 below | exact spike evidence | blocked |

## Spike-by-spike evidence

| Spike | Evidence produced | Result and architecture impact | Status |
|---|---|---|---|
| S0-Q01 graph factory | Exact four access contexts; exact execution/read field sets; `execution_runtime` returns the execution variant only for `threads.create_run`; import/build creates no resource; success/failure/cancellation close resources; local server invokes execution graph. | The 0.4.2 contract is a type alias with an inherited `execution_runtime` discriminator and execution-only `context`. Real Agent Server interrupt cleanup remains a later server integration proof. | pass for architecture API; server failure-path proof pending |
| S0-Q02 Agent Server/auth/platform | `langgraph.json` loads; custom auth and app load; `disable_studio_auth=true`; unauthenticated native/custom requests return 401; scoped requests succeed; graph runs through `/threads/.../runs/wait`; custom route does not collide. | The explicit FastAPI dependency is defense in depth for custom routes; native resources remain resource-filtered. Cloud persistence, entitlements, quotas, regions, revisions, and managed Store remain unmeasured. | blocked |
| S0-Q03 Postgres operation transaction/Mongo migration | Pure crash injection before effect, after effect, and after settlement proves stable claim/settlement/outbox identities and one observed effect. Baseline confirms current Mongo/Postgres split. | Direction is accepted with source precedence: PostgreSQL receives claim/attempt/settlement authority and references the immutable Mongo-authoritative semantic binding by stable identity/digest. No live transaction, RLS/grant test, backfill, or crash-at-commit proof ran because services were unavailable. | blocked |
| S0-Q04 checkpoint compatibility/blue-green | Compatibility digest excludes implementation revision and changes on reducer surface change. Identity/endpoint policy accepted. | No N/N+1 deployment or persisted checkpoint was available. Revision metadata is explicitly rejected as the router. | blocked |
| S0-Q05 context reconstruction | 100 reversed/repeated reconstruction cycles preserve protected goals, instructions, citations, contradictions, approvals, attempts, digests, and tombstones with identical assembly digest. | Stage 5 threshold: zero missing protected atoms and zero assembly-digest drift; model summaries remain derived. | pass |
| S0-Q06 async policy | Bounded fan-out max=3 across 20 operations; timeout scope; cancellation and resource closure; graph resource closes on failure/cancel. | Representative pure proof passes. DB/model/MCP/sandbox/artifact/Store/stream integration proof is still required. | blocked |
| S0-Q07 frontier/Send/reducers | Two roots dispatch with `Send`; deterministic join; 100 randomized merge orders prove associative/commutative/idempotent unique-key behavior; conflicting duplicate fails closed. | Core frontier/reducer architecture is valid. Stateful subgraph namespace combinations still require exact integration tests. | pass for core; namespace proof pending |
| S0-Q08 interrupts/state/forks/concurrency | Interrupt/resume restarts behind a stable idempotent claim; `update_state` applies reducer; `Overwrite` replaces only in privileged test; epoch identity accepted. | Process-restart persistence, parallel interrupt map, durable decision repository, fork endpoint, and server concurrent-run strategies were not exercised. | blocked |
| S0-Q09 MCP | Fresh lock exposed adapter/MCP 2.0 incompatibility; exact `mcp==1.29.0` restores client/session/interceptor imports. Existing coordinator Streamable HTTP tests pass. | Exact MCP pin is mandatory. Remote auth/schema drift/elicitation/session cleanup/cancellation were not exercised. | blocked |
| S0-Q10 QuickJS/dynamic | Exact middleware defaults inspected. | Owner disabled QuickJS, PTC, and dynamic subagents. Defaults (`subagents=True`, PTC limit 256) prove that explicit override/feature denial is mandatory. | deferred_optional_feature_disabled |
| S0-Q11 async subagents | Exact preview `AsyncSubAgent`, middleware, and `async_tasks` state surface imported. | Async is required for the migration but remains default-off until launch/check/update/cancel/list, crash/orphan, capacity, wait/resume, settlement, stale-status, full-ID, and tenant proofs pass. | blocked |
| S0-Q12 Store memory | Tenant/environment/purpose isolation, deletion, cross-tenant denial, and scientific-authority denial pass in a pure model. | Actual Agent Server Store expiry/retraction/contamination proof was not run. | blocked |
| S0-Q13 Sandbox/snapshots | Exact Sandbox client create limits, mount/proxy, TTL, snapshot, and Deep Agents backend constructor surfaces imported. | No entitlement, create/execute/reconnect/egress/secret/snapshot/orphan/usage/cross-tenant proof was authorized. | blocked |
| S0-Q14 middleware | Exact `create_deep_agent` and QuickJS signatures inspected. Default graph nodes/channels/tool names captured from 0.7.4. | Checked-in skill prose is stale for this pin. Wrapper order, async after-hooks, duplicate detection, summarization collision, and failure propagation need deeper executable proof. | blocked |
| S0-Q15 tracing/evaluation | Root trace tests prove secret/prompt/output redaction and now include a synthetic-PHI sentinel. Existing trace metadata shape is documented. | No live LangSmith root/child trace or tenant-negative query was produced; evaluators are therefore not authored yet. | blocked |
| S0-Q16 build/deployment ownership | `langgraph dev` starts Agent Server 0.12.0/runtime-inmem 0.32.0 with custom auth and `disable_studio_auth=true`, and serves a scoped graph. CLI-managed Serverless staging selected. | `langgraph build/up`, image exclusions, and deployment revision behavior are blocked by the unavailable Docker daemon and local-only authorization. | blocked |

## Accepted thresholds

| Area | Stage 0 threshold |
|---|---|
| Protected context | 100% atom/ref/digest/tombstone preservation |
| Context reconstruction drift | exact assembly digest equality |
| Reducers | associative, commutative, idempotent; conflict fails closed |
| Resource cleanup | opened resources equal closed resources on success/failure/cancel |
| Async fan-out | explicit configured bound; no unbounded gather in production paths |
| Effect execution | at-least-once runtime; one stable effect claim and settlement identity |
| Store authority | zero authorization, approval, budget, scientific acceptance, or terminality decisions |
| Trace redaction | zero secret, PHI sentinel, or full private prompt/output leakage |
| Preview capability | default off until its named gate passes |

Latency, cost, concurrency, cold-start, maximum wait, and quality thresholds cannot be
accepted from this local spike; they need representative staging measurements and
baseline datasets.

## Verification command log

| Command | Environment | Result |
|---|---|---|
| `git status --short --branch && git rev-parse HEAD ...` | root | base/dirty state captured |
| `uv run ruff check app tests` | root | pass |
| `uv run mypy app` | root | pass, 218 files |
| `uv run pytest` | root | pass, 395 passed/8 skipped |
| `docker compose ps --all` | root | fail: Docker Desktop Linux engine pipe absent |
| `uv pip install --dry-run --no-deps ...` | root | package candidates resolved |
| `uv lock && uv sync` | `spikes/stage0` | pass, exact lock created |
| `uv run pytest -q` | `spikes/stage0` | pass, 29 tests, one baseline Starlette/httpx2 deprecation warning |
| `uv run ruff check app tests spikes/stage0` | root | pass |
| `uv run langgraph dev --config langgraph.json --no-browser --no-reload` with local identity and `disable_studio_auth=true` | spike | pass; hardened config loaded and server reached startup |
| unauthenticated native/custom route probes | local Agent Server | 401/401 |
| authenticated native/custom route probes | local Agent Server | 200/200 |
| authenticated thread + run/wait | local Agent Server | output `tenant-a:execution-resource` |
| async Postgres saver/Store import probe | spike | pass |
| LangChain MCP adapter import probe | spike | initially failed with MCP 2.0; pass with MCP 1.29.0 |

## Traceability rows assigned to Stage 0

| Architecture row | Disposition |
|---|---|
| BellLabs definitions/compiler authority | contracted_for_later_stage; current authority confirmed |
| PostgreSQL effect claims/attempts/settlements | owner accepted; live transaction proof blocked |
| Stable node/channel/reducer/interrupt names | contracted for Stages 1–3; compatibility policy accepted |
| Blue/green for incompatible checkpoints | owner accepted; deployment proof blocked |
| All I/O native async | local representative proof; full integration proof blocked |
| Async preview disablement | owner amended: required migration track, default-off until Stage 6 gate |
| Streamable HTTP production default | accepted; exact outbound transport proof blocked |
| Standard Agent Server primary | implemented in decision contract; local server proof passes |
| Managed persistence boundary | accepted; Cloud injection proof blocked |
| Runtime graph factory/introspection safety | exact API amended and local proof passes |
| Per-run resource cleanup | local proof passes; server failure-path proof pending |
| Deployment ownership | CLI accepted |
| Serverless/Dedicated evidence | blocked |
| No broad database reset | implemented_and_proven by command/diff inspection |
