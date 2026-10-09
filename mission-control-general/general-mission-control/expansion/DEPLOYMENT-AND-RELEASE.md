---
type: Specification Annex
title: "Deployment, compute and release contracts"
description: "Design baseline, not a deployment instruction. Release is gated by evidence and issue dependencies, not dates. No purchases or metered work are authorized by this packet."
tags: [mission-control, spec, expansion]
---
# Deployment, compute and release contracts

Design baseline, not a deployment instruction. Release is gated by evidence and issue dependencies, not dates. No purchases or metered work are authorized by this packet.

## Topology

AWS containers host shared FastAPI HTTP/MCP/stream gateway, scoped relay/reconciler/notification workers, app-bound Temporal workers and required Agent Server processes. The proposed AWS baseline is ECS/Fargate + ALB + private networking and Secrets Manager/KMS; verify image/runtime/region eligibility in the topology proof before production. Agent Server supported packaging/licensing/storage requirements must be verified against its lock; a preferred diagram cannot override vendor-supported persistence.

Temporal Cloud owns durable workflow histories, app/environment namespaces and authenticated worker identities. Each app's Supabase owns Auth/membership, common Mission Control schema, private runtime/checkpoint schemas where qualified and private buckets. LangSmith owns qualified Deep Agents sandboxes/traces. Python Cursor Cloud owns its repository execution workspace. Domain services connect only to their own PostgreSQL or Neo4j entities. Redis, if used for transient fan-out/rate control, is disposable and never the authoritative replay/mission ledger. No third authoritative mission database is introduced.

Load balancer/WebSocket affinity cannot replace durable replay. API/stream nodes can restart; clients recover from app-local event cursors. Gateway read pools/worker write pools/runtime pools are independently bounded. Database outage causes per-app admission unavailable and bounded backoff; no pool falls back to another project's DSN. Keep user-facing read snapshots explicitly stale during reconnection.

## Storage and event record additions

Add the following common records to the self-contained SQL component, with all base scoped columns/FKs/RLS/indexes/immutable contracts from DATABASE. Both apps install the same record shapes, including optional-feature tables.

| Record | Required fields / constraint |
| --- | --- |
| `interview_session` | interview_id, authoring_session/draft refs, app/tenant/actor scope, configuration/context/profile digests, native workspace/thread refs, generation, lifecycle, version, next_event_seq, lease/usage refs; independently scoped authoring execution, not an attempt or fictitious Mission Run |
| `interview_turn` | turn_id, interview_id, turn ordinal, admitted message/attachment refs/digests, generation, native run ref, execution disposition, candidate draft refs and usage/receipt frontier; unique interview/turn ordinal |
| `interview_event` | event_id, interview_id, seq, transaction/causation refs, registered event schema, bounded payload ref; unique interview/seq, no fake mission_id; same durable replay/retention/auth laws |
| `context_selection` | selection_id, target run/node/attempt, policy/tokenizer/candidate capture refs/digests, manifest ref/digest, total token bound, omitted count; immutable |
| `state_handoff` | handoff_id, producer/consumer refs, base/new state version/digest, typed delta/artifact refs, frontier and admission decision; unique semantic handoff identity |
| `message_delivery` | message_id, target/generation/admission seq, sender/recipient, kind/boundary/content digest, deadline, disposition/current version; payload immutable; append delivery reports |
| `process_lease` | process_id, workspace/attempt/generation/fence, admitted profile/native handle, desired/observed state, exit/cleanup/usage refs; scoped writer owner |
| `view_snapshot` | mission/version/event frontier, view schema version, state ref/digest; derived rebuildable projection |
| `notification_delivery` | human task/event ref, channel/profile, recipient grant, payload digest, idempotency key, attempts/state/external receipt; no approval authority |
| `review_feedback` | resolution/task/artifact version/digest, feedback ref/digest, desired remediation, policy; immutable child of accepted Human Task resolution |
| `memory_admission` | namespace/source/consent/grant/policy, proposed artifact version/digest, admission/revocation decision, expiry; no cross-tenant namespace joins |
| `knowledge_operation_binding` | immutable operation/endpoint/schema/auth/target/domain version and effect/receipt lookup contract; generic envelope only |
| `evaluation_run` | dataset/split/profile/baseline/treatment/metric/rubric refs/digests, resource allocation, outputs/results/admission, seed/version; immutable inputs |

Catalog search uses existing asset tables plus a scoped disposable index, not duplicate asset truth. Artifact versions/relations and Human Tasks handle deliverables/review; no second deliverable approval engine. A specialized knowledge service retains its own receipt/evidence records. MC records external refs/verification digests and operation intents; no FK into entity schemas. Contract events and immutable reports permit rebuilding views; SQL trigger/projection writer cannot invent business transitions.

Authoring-budget/effect admission must not require a nonexistent run. Extend `budget_account` and `operation_intent` with a strict owner discriminator `mission_run` or `interview_session` and scoped owner FK. A run-owned intent has run/activation/attempt refs as required by its action; an interview-owned intent has interview/turn refs and no run/activation/attempt. SQL CHECKs enforce exactly one owner variant; request/effect keys include that owner identity. Authoring effects can acquire bounded search/model/sandbox work under their own admitted ceiling, never domain apply or ungranted mission start. Attempt-owned `agent_session` remains unchanged for mission execution; interview native thread/run handles belong to interview_session/turn. No fabricated attempt IDs or unbudgeted pre-mission agent calls. Draft-to-mission link is explicit, and spent interview costs do not reset or silently become run expenses.

## Release manifest and preflight

Pin build/source/lock/schema digests, ordered migration/component checksum set, operation/public schema/catalog, worker workflows/Agent Server graph and artifact versions, environment images, middleware/tool/provider profiles, skill bundle, UI resources, web/mobile artifacts, and per-app registry version. No credential values. A build must run in isolated checkout with no sibling imports, Mongo/Beanie dependency or unmanaged scheduler.

Preflight validates installation identity, actual project/schema fingerprint, least-privilege roles/RLS and pool context, private bucket policies, runtime schema compatibility/load/backups, domain endpoint contracts, namespace/TLS/access, task profile/quotas, notification channels and UI host capability. Migration execution/deployment remains a separately authorized operation with concrete targets. Existing unrelated Auth/domain data cannot be reset. Subsequent releases of the new engine use expand/contract and compatible worker history handling.

Restore proof includes database metadata + referenced artifact bytes + authorized runtime checkpoint state + retained workflow histories. A DB backup alone does not restore PDFs/snapshots. Test under disposable isolated targets; validate input/artifact hashes, uncertain effects and command/event frontiers before resume. Lost domain write response uses domain receipt lookup, never a blind rerun of a Neo4j transaction.

## Compute allocation model

Compute profiles are role-based, not unverifiable dollar promises. `C0` deterministic local checks: one owner process, no provider budget. `C1` foundational architecture/proof coding: one strongest available authorized reasoning model plus one independent reviewer, at most two overlapping agents per issue. `C2` bounded implementation: one capable coding model per disjoint ownership region plus targeted reviewer, maximum one writer per file region. `C3` dataset/evaluation: deterministic captured/offline replay first, independently reserved treatment profiles and finite calls/rows/repetitions. `C4` live qualification: zero permitted calls until a signed budget allocation identifies actual prices/ceilings/targets. Frontier reasoning concentrates on control/state, isolation, compiler and review; routine generation/fixtures use smaller models when qualified.

Global default team cap is four implementation agents plus one integrator/reviewer, bounded lower by available slots and resource profiles. Parallelize only DAG-ready issues with disjoint file regions; schema/contracts and shared registries have a single integrator. Provider concurrency is the minimum of allocation, app/harness quota, run/parent ceiling and global account slice. Increased local compute cannot bypass a rate/spend/authority gate.

`ProofBudget@1`: budget_id, actor/scope, target environments, provider/model/tool/profile versions, price snapshot refs/time/currency, maximum calls/input-output tokens/sandbox seconds/search units/storage/egress/concurrency, reserved micros, hard stop conditions and expiry/revocation. Cost upper bound is the sum over dimensions of maximum units * verified applicable unit rates + fixed fees + declared tax/FX/rounding/safety assumptions. Cached/batch discounts apply only when eligibility is proved. Unknown billing retains reservation and reconciles; unknown price cannot be treated as zero. Current prices are fetched from official provider pages at live-budget preparation; this packet quotes no dollar amounts and authorizes no metered experiment.

Represent unit rates as exact rational micros: `rate_micros_numerator` decimal-digit string / positive `rate_units_denominator`, with explicit billing_unit (token, million tokens, second, search unit, etc.) and dimension mapping. Never force fractional-micro per-token rates into an integer per-token price. Compute each dimension's aggregate exposure with exact rational arithmetic, round upward to whole micros at reservation settlement boundaries, and retain numerator/denominator/price snapshot for audit. Summed charges/fixed fees/FX/tax assumptions must be explicitly attributed. MC-P006/P007 fake contract proofs can proceed independently; actual persistence/server/hydration qualification requires accepted MC-P009 topology, enforced again at MC-F009/MC-I001. A fake pass cannot enable runtime storage.

## Integration and release gates

G0: contract/schema baseline and preserved source evidence. G1: foundational state/context/control/subordinate/runtime topology proofs. G2: neutral deterministic kernel/schema + authoring/catalog; G3: Deep Agents/Agent Server parity for both apps; G4: Cursor/direct providers + four systems/revisions; G5: knowledge data-readiness and reproducible research/PDF/coding scenarios; G6: public skill/CLI/MCP + web/mobile + generative UI/MCP Apps; G7: isolation/load/recovery/security + independently approved production qualification. Stages are partial-order gates; no calendar is attached. Full release requires all, even when early usable gates ship behind availability flags.

Release verification distinguishes offline fixtures, real isolated infrastructure, finite authorized live provider proofs and production deployment. Disabled/unqualified capabilities are honest describe-system entries. A browser prototype or mock screenshot is not mobile/runtime qualification. PDF render/citation completeness and app navigation are acceptance artifacts, not mere model text. Failure conditions stop the affected gate, preserve receipts/history and allow independent work.

## Operations and measurable limits

Expose per-app readiness/capabilities, outbox/notification lag, stuck lease/child/effect counts, stream floor/gaps/backpressure, context overrun/handoff conflicts, checkpoint failures, actual vs reserved/unknown spend, sandbox/process cleanup and dataset drift. Profiles declare numeric fixture limits/latency SLOs at qualification from measurements; do not label unmeasured instant interruption as an SLO. Safe operator actions are inspect/reconcile/retry/fork under current grants, not raw database editing. Every manual resolution records actor/evidence/reason and cannot fabricate irreversible effect absence.

Official references checked for technology facilities, not account readiness: [AWS Fargate](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/AWS_Fargate.html), [ALB WebSockets](https://docs.aws.amazon.com/elasticloadbalancing/latest/application/load-balancer-listeners.html), [Temporal Python messages](https://docs.temporal.io/develop/python/message-passing), [LangSmith sandboxes](https://docs.langchain.com/langsmith/sandboxes), [Supabase RLS](https://supabase.com/docs/guides/database/postgres/row-level-security). Pinned-source/topology proofs remain mandatory.
