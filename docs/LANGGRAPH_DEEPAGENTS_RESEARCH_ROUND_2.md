# LangGraph / Deep Agents control-plane research round 2

Status: **research conclusion and migration-plan amendment evidence**  
Access date: 2026-08-01  
Scope: BellLabs research, ingestion, evaluation, and content-production workflow control plane

## Verdict

**READY WITH LISTED PRECONDITIONS** for Phase 0 and disposable Phase 1 qualification. The stack direction remains sound: BellLabs domain/application services retain authority, LangGraph supplies durable execution, Deep Agents supplies selected operation harness capabilities, and LangSmith supplies deployment/observability/evaluation mechanics.

Production implementation is not yet authorized by evidence. The blocking preconditions are:

1. accept the graph-assembly/rebuild contract and prove introspection does not create resources or side effects;
2. accept the PostgreSQL migration of authoritative operation claims/settlements;
3. publish the context/delegation/middleware contract shapes and naming ADR;
4. qualify exact package versions and feature maturity, especially `ServerRuntime`, QuickJS dynamic delegation, async subagents, and Sandboxes;
5. prove Agent Server auth, persistence, deployment compatibility, limits, and recovery in the intended organization/environment.

## Highest-impact findings

1. **VERIFIED FACT — graph factories have a wider call surface than execution.** Agent Server calls a graph factory for new runs and also for state update, state read/history, and assistant schema/graph inspection. A factory that provisions sandboxes, MCP sessions, secrets, or BellLabs mutations on every call is unsafe. `ServerRuntime` exposes the access context but is beta. The factory must have an introspection-safe branch and an adapter boundary. [Rebuild graph at runtime](https://docs.langchain.com/langsmith/graph-rebuild)
2. **GAP — operation-effect authority is split across databases.** Current `OperationExecutionBindingDocument`, `OperationExecutionClaimDocument`, and `OperationSettlementDocument` live in MongoDB while lifecycle, budgets, and outbox live in PostgreSQL. Atomic reserve/claim/settle/outbox behavior cannot be guaranteed across that boundary. Move authoritative claims and settlements to PostgreSQL before advertising exactly-once effects.
3. **VERIFIED FACT — Deep Agents exposes several distinct delegation mechanisms.** Ordinary subagents block the supervisor; dynamic subagents depend on the beta interpreter runtime; async subagents are preview, use Agent Protocol, return immediately, retain state on their own thread, and support check/update/cancel/list. These cannot share one boolean or lifecycle contract. [Subagents](https://docs.langchain.com/oss/python/deepagents/subagents), [async subagents](https://docs.langchain.com/oss/python/deepagents/async-subagents), [interpreters](https://docs.langchain.com/oss/python/deepagents/interpreters)
4. **VERIFIED FACT — production construction should be natively async.** Current Deep Agents production guidance recommends async tools, async middleware hooks, and async graph factories/resource lifecycle for sandboxes and MCP. The BellLabs pure domain core should remain synchronous, while every I/O boundary becomes async. [Going to production](https://docs.langchain.com/oss/python/deepagents/going-to-production)
5. **GAP — context policy was too shallow.** Native compression/offloading and subagent isolation are useful mechanics, but scientific provenance, contradiction retention, reconstruction, deletion, trust, and evaluation remain BellLabs contracts. Long-horizon systems need externalized context plus explicit retrieval/compression policy; multi-agent isolation can improve focus but adds large cost and coordination risk. [Deep Agents context engineering](https://docs.langchain.com/oss/python/deepagents/context-engineering), [Anthropic context engineering](https://www.anthropic.com/engineering/effective-context-engineering-for-ai-agents), [Anthropic multi-agent research system](https://www.anthropic.com/engineering/multi-agent-research-system)
6. **VERIFIED FACT — middleware is a capability inventory, not one universal stack.** Current prebuilt middleware includes summarization, HITL, model/tool retry, model-call limits, context editing, provider tool search, shell, file search, filesystem, subagents, and beta rubric grading. Deep Agents already composes core harness middleware. BellLabs must compile an ordered stack per operation class and reject duplication/conflicts. [Prebuilt middleware](https://docs.langchain.com/oss/python/langchain/middleware/built-in)
7. **PROPOSED DECISION — runtime rebuilding assembles mechanics, never authority.** A per-run factory may bind an exact sandbox/backend/tool/middleware/subagent set from a frozen `GraphAssemblySpec`; it may not resolve aliases, choose a Workflow Type, widen capability grants, or change the admitted goal.

## Research synthesis

### Long-running execution

LangGraph checkpoints and Agent Server threads provide restart/resume mechanics, not transactional scientific or business authority. BellLabs still needs an effect journal, idempotency keys, lifecycle CAS, budget ledger, outbox, reconciliation, cancellation propagation, and deployment compatibility routing. The robust pattern is at-least-once execution with exactly-once *settlement/claim identities*, not a claim that arbitrary provider side effects execute exactly once.

The generic StageGraph frontier scheduler remains the safest first port because BellLabs semantics include cycles, joins, fairness, waits, reuse, and invalidation. `Send` is appropriate only after authoritative admission/reservation, with associative/commutative/idempotent result reducers and one deterministic settlement boundary. A linked Workflow Run, not a subagent, remains the unit for independent admission, authority, durable waits, substantial budgets, and reusable governed outputs.

### Context and memory

Treat context as a materialized view assembled from immutable sources, current authoritative state, bounded working state, and purpose-compatible non-authoritative memory. Filesystem offloading and summarization solve token pressure but do not prove fidelity. Preserve exact instructions, goals, citations, evidence edges, contradictions, approvals, attempt identities, and digests outside model-written summaries. Rebuild context from a manifest after compaction or epoch rollover.

Primary and first-party work supports evaluating memory rather than presuming more memory is better. Long-horizon memory benchmarks explicitly test retention across extended agent interactions, while production multi-agent reports emphasize context isolation and external memory alongside higher token cost and coordination complexity. [AMA-Bench](https://arxiv.org/abs/2602.22769), [Google context-aware multi-agent architecture](https://developers.googleblog.com/architecting-efficient-context-aware-multi-agent-framework-for-production)

Cross-thread Store should therefore be off by default for scientific claims. Enable it for reviewed procedural knowledge or low-risk preferences only, with tenant/subject/purpose namespaces, provenance, expiry, contradiction handling, and deletion. Store never authorizes, proves a claim, satisfies approval, or terminalizes a run.

### Deep Agents and coordinator composition

Deep Agents should remain an inner harness. Its todo, filesystem, skills, subagent, summarization/offloading, and optional interpreter capabilities should be exposed to the coordinator as granular capability facts with exact bindings and maturity—not copied into BellLabs-specific implementations.

The coordinator compiles capabilities in two steps:

1. domain selection: exact Workflow Type, implementation, operation class, authority, budgets, workspace, linked-run slots;
2. runtime assembly: exact agent harness, middleware stack, context policy, tools/MCP, skills, delegation mode, interpreter, sandbox, model, verifier, and fallbacks.

The launch ticket freezes all exact refs/digests and the resulting graph-assembly digest. Search rank, installed packages, native tool availability, or assistant configuration never grant authority.

## Gap and contradiction matrix

| Area | Existing claim | New evidence | Gap/risk | Severity | Correction | Confidence | Required spike |
|---|---|---|---|---|---|---|---|
| Graph rebuilding | Static compiled graphs are normally sufficient | Factory is invoked for execution, update, read, and schema inspection; `ServerRuntime` is beta | Resource creation or mutations during introspection; mutable per-run assembly | Pre-implementation blocker | D-11 and introspection-safe `GraphAssemblyFactory` | High | Factory access-context/resource-lifecycle spike |
| Persistence | PostgreSQL owns lifecycle; Mongo owns bindings/claims today | Atomic effect claim must coordinate with budget/outbox | Cross-database exactly-once illusion | Architectural blocker | D-13; migrate authoritative operation journal to PostgreSQL | High | Transaction/crash/backfill spike |
| Async | Plan mentioned async saver/store | Production guidance recommends async tools, middleware, factories, external lifecycle | Blocking event loop, cancellation/resource leaks | Pre-implementation blocker | D-12 plus async policy/test gate | High | Cancellation/backpressure/resource-close spike |
| Context | Compact/offload and Store placement table | Native context compression is mechanical; long-horizon evaluation remains necessary | Summary drift, lost citations/contradictions, memory contamination | Pre-implementation blocker | D-14; `ContextPolicyDefinition` and reconstruction protocol | High | Repeated compaction/reconstruction benchmark |
| Sync subagents | Custom subagents treated as bounded delegates | Ordinary custom calls are fresh/stateless and skills are not inherited automatically | Missing skill/context/output bindings | Hardening requirement | Exact `ContextSlice`, explicit skills, result manifest | High | Skill inheritance/context leak spike |
| Dynamic subagents | Listed with async subagents | Dynamic mode relies on beta QuickJS interpreter | Approval/PTC bypass and unstable API | Pre-implementation blocker for feature | Separate beta mode and independent capability wrappers | High | QuickJS bypass and source/snapshot spike |
| Async subagents | Durable job binding proposed | Preview; stateful child thread over Agent Protocol with check/update/cancel/list | Stale status, ID coupling, orphan/capacity failure | Pre-implementation blocker for feature | Separate preview mode, typed task/thread IDs, reconciler | High | Crash/orphan/deadlock/cancel spike |
| Middleware | One recommended logical order | Large prebuilt inventory and Deep Agents core middleware | Duplicate summarizers, ambiguous hook nesting, hidden call limits | Hardening requirement | Per-operation ordered manifest and conflict validator | High | Hook-order and duplicate-middleware spike |
| Naming | Several provider/domain IDs coexist | Node/state names affect checkpoint compatibility | Overloaded `run_id`/`agent_id`, accidental breaking rename | Pre-implementation blocker | D-16 vocabulary and suffix grammar | High | Schema/compatibility snapshot test |
| Agent Server revisions | Revision recorded in binding | Revision metadata does not itself route old threads to old code | Incompatible resume | Architectural blocker already recognized | Blue/green endpoint binding plus assembly/schema digest | High | N-on-N after N+1 drill |
| Serverless limits | Async/waits assumed workable | Entitlement/capacity/duration/recovery remain environment-specific | Architecture may exceed platform limits | Pre-implementation blocker | Verify organization and measured limits before Phase 2 | Medium | Longest-wait/cold-start/concurrency spike |
| Scientific memory | Store can hold cross-thread memory | More memory can contaminate or outlive purpose | Unsafe claim reuse/retraction handling | Hardening requirement | Default-deny scientific Store memory; purpose/expiry/provenance | High | Contamination/deletion/retraction test |

## Decision disposition

| Decision | Disposition | Amendment |
|---|---|---|
| D-01 | Accept | Standard Agent Server remains necessary for custom outer graphs and APIs. |
| D-02 | Accept | Keep generic StageGraph first; measure settlement throughput. |
| D-03 | Accept | Generated native graphs remain an optimization after parity. |
| D-04 | Accept | Independent verifier and BellLabs terminality remain mandatory. |
| D-05 | Amend | One thread per run/epoch remains default; async subagents and linked runs have separate child threads recorded explicitly. |
| D-06 | Accept | Shared router/service composition is still the safest coexistence path. |
| D-07 | Accept | Managed persistence injection remains the production default; standalone async saver/store only where owned. |
| D-08 | Amend | Add assembly/state-schema digests and one-to-many execution attempts/async tasks. |
| D-09 | Accept | Typed interventions only. |
| D-10 | Accept | Messages remain local to bounded agent subgraphs. |
| D-11 | Add | Governed async graph factory and introspection-safe rebuild protocol. |
| D-12 | Add | Async I/O, synchronous pure domain. |
| D-13 | Add | PostgreSQL authoritative operation journal. |
| D-14 | Add | First-class context policy/assembly manifest. |
| D-15 | Add | Four delegation modes. |
| D-16 | Add | Canonical vocabulary and identifier grammar. |

## Revised qualification and phase order

Before production contract implementation, Phase 1 must run these architecture-invalidating spikes first:

1. graph factory access contexts, introspection behavior, async context-manager cleanup, and immutable assembly cache;
2. Agent Server auth/resource coverage and exact organization entitlement/limits;
3. PostgreSQL operation claim/reserve/settle/outbox transaction plus Mongo backfill/rollback design;
4. checkpoint/state-schema compatibility and blue/green N-on-N resume;
5. context compaction/reconstruction/provenance retention;
6. async end-to-end cancellation, deadlines, backpressure, and resource closure;
7. QuickJS PTC/dynamic-subagent authorization bypass;
8. async-subagent crash/orphan/update/cancel/capacity behavior;
9. Store contamination, expiry, deletion, and retraction;
10. remaining reducer, MCP, sandbox, tracing/redaction, and throughput spikes.

Phase 2 may start only after items 1–6 produce accepted contracts. QuickJS and async subagents may remain disabled while the stable StageGraph and bounded synchronous Deep Agent path proceeds. This makes preview features optional rather than architectural dependencies.

## Implementation handoff

The primary migration plan now includes D-11 through D-16, a complete context policy, runtime graph assembly/rebuild protocol, four delegation modes, Mongo/PostgreSQL target schemas, field governance, naming conventions, an async policy, and coordinator capability compilation rules. No runtime dependencies, database schemas, accepted `biotech-meta` specifications, or production code were changed in this research round.

