---
type: Specification Annex
title: Knowledge Services integration contract
description: "Expansion specification, 2026-10-03. This annex defines the current requested integration scope; it does not certify implementation or deployment. Read the canonical specification, runtime contracts, and workflow…"
tags: [mission-control, spec, expansion]
---
# Knowledge Services integration contract

Expansion specification, 2026-10-03. This annex defines the current requested integration scope; it does not certify implementation or deployment. Read [the canonical specification](../SPECIFICATION.md), [runtime contracts](../RUNTIME-CONTRACTS.md), and [workflow semantics](../../workflow-types/index.md). Knowledge integration is in scope. The clean-break engine policy applies: no old-engine history compatibility is required. Agent Server/LangGraph recovery remains in restricted app-local PostgreSQL schemas, separate in authority from mission state and domain knowledge.

## Owners and execution boundary

Mission Control owns mission authoring, recursive scheduling, grants, reservations, operation intents, external-effect reconciliation, review tasks and completion. Knowledge Services owns capture/preparation, verification algorithms, knowledge admission, query/retrieval and domain-operation execution. An agent proposes intent; a deterministic admitted executor validates and applies it. Agent completion, verified evidence, canonical ingestion and mission acceptance are distinct decisions.

The common integration envelope and capability interfaces are reusable. AI Engineer entity storage remains PostgreSQL; Biotech entity storage remains Neo4j Aura. Each application has its own Supabase project with identical Mission Control tables. Do not add domain foreign keys, raw SQL/Cypher or domain conditionals to the common kernel. Domain schemas, named queries, graph constraints, vocabularies, judgments and canonical writers remain app-owned. This annex does not mandate one physical Knowledge Services deployment or a Python rewrite of the existing TypeScript service.

Cross-repository callers use admitted HTTP/MCP/CLI operations. The existing Knowledge Services API, MCP and workers call their application handlers in process; Mission Control must not import their algorithm packages. Secret references resolve server-side. A catalog capability pins endpoint/server identity, input/output contract versions, executor digest, domain schema manifest, policy/rubric versions, side-effect class, scopes, limits and receipt lookup semantics.

## Versioned intent artifacts

An intent file is an immutable registered artifact, not an executable script or implicit permission. The common wrapper `mc.knowledge_request.v1` requires:

| Field | Contract |
| --- | --- |
| Identity | request ID, installation/application/tenant, mission/run/activation/attempt, generation and stable semantic effect key |
| Target | exact capability binding, endpoint audience, operation name, executor and target-schema versions/digests |
| Input | intent artifact reference/digest, native domain contract name/version, upstream artifact/evidence references/digests |
| Authority | attributable actor, requested domain scope, originating admission reference; effective grants resolved again at invocation |
| Preconditions | typed schema pins, read snapshot, domain concurrency token, expected plan digest and approval references where required |
| Governors | reservation, timeout/deadline, bounded item count and partial-result policy |

The domain intent payload retains its published wire schema. Existing AI Engineer `knowledge-ingestion-intent.v1` uses camelCase; wrap it unchanged and validate with its native schema. Do not translate it by handwritten renaming into Mission Control lower_snake or silently change its digest. Record the exact native canonicalization algorithm and native payload digest alongside the wrapper digest. Approved input bytes are frozen; a corrected proposal creates a new version/digest and a new logical action after prior uncertainty is settled.

Draft intents can be agent-written within owned scratch space. Only registered immutable bytes enter validation, planning, approval or apply. Compilation checks capability availability and wrapper types; the domain executor performs authoritative payload/schema/evidence validation. Validation/plan may acquire reads and produce artifacts but must not perform canonical domain writes. Their declared compute/storage costs still require admission.

## Required capability interfaces

These are logical interface names to bind in the catalog, not claims about existing endpoint names. Python adapters use strict Pydantic request/results and a typed protocol; SDK and native service objects stay inside adapters.

| Interface | Input | Output and invariants |
| --- | --- | --- |
| `knowledge.describe` | scope and exact deployment/schema binding | available contract versions, operation catalog, supported consistency/read/recovery guarantees; unavailable is explicit |
| `knowledge.capture_prepare` | source/upload refs, media/profile and custody policy | immutable capture/representation/locator manifests, transformations and quality findings; original bytes retained |
| `knowledge.verify` | registered evidence bundle, assertions, selector refs, pinned rubric/policy | attributable deterministic/semantic findings and sealed verification/admission refs; semantic approval cannot override deterministic failure |
| `knowledge.classify_propose` | immutable input/evidence refs and taxonomy/rubric version | typed label proposal, uncertainty, supporting locators and omitted/unsupported facets; no canonical mutation |
| `knowledge.classify_admit` | exact proposal digest, policy and required review refs | admitted/held/rejected decision; classification as domain metadata uses the same governed intent/apply mechanism |
| `knowledge.query` | named-query read intent, parameters, role/limits and schema/consistency pins | per-operation status, truncation, scoped snapshot/token and digest; missing data differs from failed/skipped read |
| `knowledge.retrieve` | bounded retrieval plan, hard filters, publication/clock pins and required/optional features | evidence packet, support/citations, omission/abstention and query scope; score is not evidence validity |
| `knowledge.ingestion_plan` | immutable domain intent and snapshot | deterministic plan digest, per-proposal dispositions, resolved identities, rewrites, order, preconditions and required approvals |
| `knowledge.ingestion_apply` | unchanged intent, approved plan digest, current grants and stable effect identity | immutable domain receipt, affected refs, applied/noop/partial/rejected outcomes and pre/post concurrency token |
| `knowledge.receipt_get` | original scoped effect/native intent identity | existing canonical receipt, explicit unresolved/not-found disposition; authenticated lookup before uncertain repeat |
| `knowledge.verify_result` | domain receipt and affected refs | new bounded read/verification report; a pre-write snapshot cannot prove application |

Content access classification and semantic taxonomy classification are different contracts. Existing content enum is `public/restricted/confidential/sensitive`; verification enum is `public/internal/confidential/restricted`. There is no proved common semantic classification endpoint in the inspected catalog. An adapter must declare a versioned security-classification mapping, reject unmapped values and never downgrade confidentiality. Semantic classifier implementation and admission are proposed work, not an alias for these enums or a confidence score.

## Plan, approval, apply and recovery protocol

1. Register source/evidence/read and intent artifacts. Validate scope, native schema and deployed schema/executor pins. Unavailable operations reject before execution.
2. Record a Mission Control operation intent/reservation. Invoke plan under a read-only domain capability. Register the plan and explicit per-item dispositions.
3. Resolve required Human Gates against exact intent/plan/output digests and policy. Approval of report content does not authorize domain apply. The domain service validates externally issued approval attestations against its own trust/policy or obtains its own native approval; a caller-supplied `approved: true` is invalid.
4. Recheck live grants, plan preconditions, evidence eligibility and current domain concurrency token. Rebase is allowed only by a declared, proved native policy, with a new plan and renewed approval if semantics change.
5. Persist stable effect intent before apply. Submit identical native identity and bytes on retry. Store returned domain receipt as an external authoritative reference and settle Mission Control receipt/accounting transactionally.
6. If response is lost, mark uncertain and query receipt using the original identity. Do not manufacture a new intent ID or repeat a non-idempotent action. A provider without reliable receipt/idempotency support requires visible manual recovery and cannot advertise automatic uncertain retry.
7. Read the affected domain state anew and compare with receipt/plan. Completion evaluates declared item coverage, acceptable partial outcomes, unresolved effects, costs and review evidence independently.

Technical retries retain semantic effect identity and payload; schema/evidence rejection, quality failure and stale preconditions are typed domain outcomes requiring re-plan/authored remediation, not blind infrastructure retry. Cancellation stops new work and requests native cancellation where supported, but retains reservations and reconciliation until effect/usage settles. Compensation is an explicit new admitted domain action, never presumed rollback.

Partial is not synonymous with transaction corruption. The AI Engineer planner supports per-proposal `admitted/no_op_duplicate/superseded/review_required/held/quarantined/rejected` and batch `applied/noop/partial/rejected`. A batch can atomically apply admitted items while holding others. The mission must choose all-required or an explicit accepted subset and preserve every item disposition. Infrastructure uncertainty is a separate unresolved state. Repair only failed/held items with linked new intents after previous receipt settlement; do not replay accepted items as new effects.

## Domain consistency obligations

AI Engineer adapter consumes its current temporal knowledge head/read snapshot and native rules. Keep world time, capture time and knowledge sequence distinct. Read-intent execution uses repeatable-read snapshots and verifies deployed migration head. `rebase_if_disjoint` is admitted only when the native planner proves touched slots unchanged. The PostgreSQL domain receipt and canonical write must commit within the owning domain transaction.

Biotech adapter must declare its actual graph concurrency/snapshot guarantee. A schema/SDL digest is not a graph data revision; do not invent a knowledge-sequence equivalence. Each graph apply transaction checks a unique scoped intent marker with payload/plan digest, validates preconditions, writes graph effects and the immutable marker/receipt projection atomically. A repeated identity/digest returns its recorded result; changed digest conflicts. After Neo4j commit but lost control receipt, the reconciler reads that marker and settles once. PostgreSQL mission ledger and Neo4j never share one transaction. Graph schema/write-port qualification is a required new proof; this document does not claim that writer already exists.

Runtime checkpoints, operational memory, sealed evidence and admitted domain knowledge have separate identities/retention. App-local checkpoint tables are private harness recovery state; they are never ingested automatically or used as evidence of a domain write. Knowledge-owned captures stay external references in the mission artifact catalog with issuer/digest/access checks; do not create competing source authority.

## Inspected implementation anchors and limitations

Paths below are relative to `C:/Users/Pinda/Proyectos/aiengineer/ai-engineer-knowledge-services/`. Source was inspected during this specification task; no tests or service were run.

| Source | Established fact / integration implication |
| --- | --- |
| `AGENTS.md`, `packages/AGENTS.md` | Published external protocols; app-owned algorithms/admission; no internal cross-repo imports |
| `packages/knowledge-db/src/ingestion/intent.ts`, `plan.ts`, `executor.ts`, `receipt.ts` | Versioned strict intent, deterministic planning, authenticated evidence oracle required, duplicate lookup, head checks and attributable partial receipts |
| `packages/knowledge-db/src/ingestion/evidence-admission.ts` | Trusted oracle hydrates sealed bytes/provenance; agent-inline claim metadata is insufficient |
| `packages/knowledge-db/src/ingestion/tests/executor.integration.test.ts` | Existing disposable-database test anchor; test presence is not a rerun result |
| `packages/knowledge-db/src/db-read/read-executor.ts` | Bounded named reads; embedded retrieval currently skipped as unavailable |
| `packages/application/src/operations/catalog.ts` | Schema/read/ingestion remain executor-only; published platform migration/parity must be qualified, not assumed |
| `packages/application/src/knowledge/retrieval/canonical-retrieval-executor.ts` | Separate canonical retrieval route rejects unsupported required features before provider work |
| `packages/contracts/src/content.ts`, `verification/primitives.ts` | Distinct access-classification enums; explicit mapping required |
| `knowledge/schema-read-and-ingestion.md`, `knowledge/retrieval-and-evidence.md`, `knowledge/verification-and-admission.md` | Context/read routing and limits; implementation/test sources remain evidence authority |

## Suggested implementation issue contracts

These are contract work-package aliases. The owner explicitly requested a repo-local issue packet; [REQUIREMENTS.md](REQUIREMENTS.md) and `issues.json` are the sole planning backlog. KS-01 through KS-06 map to MC-P010, MC-F021, MC-F022, MC-D003 and MC-I003; they are not additional tickets and nothing is published externally.

| Package | Dependencies | Deliverable and acceptance |
| --- | --- | --- |
| KS-01 Envelope/catalog binding | MC contracts, authority, artifacts | Native-schema wrapper and digest fixtures; reject foreign app/scope, drift and unknown fields before effect |
| KS-02 Read/verify/classification adapters | KS-01 | Published transport fixtures, confidentiality mapping, deterministic-failure precedence; unsupported classifier/features are explicit |
| KS-03 PostgreSQL ingestion | KS-01, scoped effect ledger | Plan/apply/receipt/post-read adapter; duplicate/changed payload/stale head/partial approval tests |
| KS-04 Neo4j writer contract | KS-01, app graph schema owner | Unique intent marker and preconditions; crash before/after graph commit and lost-response recovery; no duplicate graph write |
| KS-05 Review/recovery integration | KS-02 through KS-04, Human Gate and budgets | Exact-plan approvals, uncertainty/cancel reconciliation, immutable receipt provenance and partial completion policies |
| KS-06 Scenario qualification | KS-05, requisite workflow/harness availability | Paired app fixtures in [worked scenarios](WORKED-SCENARIOS.md); separate offline, disposable and authorized live evidence |

Release acceptance requires public-operation parity for the advertised surfaces, two-app isolation, no raw agent database credentials, no self-admission, receipt recovery at every crash boundary, preserved provenance and explicitly measured capability availability. Clean break removes old-engine compatibility work, not these correctness obligations.
