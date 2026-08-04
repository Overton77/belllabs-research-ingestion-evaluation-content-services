# BellLabs biotech research, ingestion, and evaluation backend

> **Status: active research and development.** This service is a work in progress. Its
> contracts, runtime, deployment topology, and product surfaces will continue to change as the
> architecture is proven. It supports research and decision intelligence; it is not a medical
> device, a diagnostic system, or a substitute for a qualified clinician.

This repository contains the backend and agentic workflow control plane for **BellLabs**, an
evidence-aware system for researching the biotech, longevity, biohacking, consumer-health, and
environmental-health ecosystems. Its job is larger than generating reports. BellLabs is intended
to turn research into durable, inspectable knowledge: registered sources, claims, evidence
bundles, entities, relationships, ingestion candidates, graph changes, evaluation cases, and
reasoning-backed decisions that can improve later runs.

The long-term ambition is a full-stack biotech intelligence ecosystem spanning public knowledge,
private user context, research agents, ingestion and evaluation, product decisions, protocols,
commerce, APIs, MCP servers, agent skills, and consumer web and mobile applications.

The accepted product roadmap and domain language live in the adjacent
[`biotech-meta`](../biotech-meta/) repository. Start with the
[`BellLabs roadmap`](../biotech-meta/docs/BellLabs/roadmap.md),
[`vision and product system`](../biotech-meta/docs/BellLabs/vision-and-product-system.md), and
[`agentic commerce and provenance`](../biotech-meta/docs/BellLabs/agentic-commerce-and-provenance.md).

## Why BellLabs exists

Consumers already compare supplements, order their own lab tests, use wellness and measurement
devices, follow public longevity protocols, investigate environmental exposures, read case
studies, and ask AI systems to help them interpret a fragmented evidence landscape. Today that
work is scattered across search results, papers, podcasts, vendor pages, forums, reports,
spreadsheets, shopping carts, and chats. Most of the reasoning disappears after a decision is
made.

BellLabs is building the missing evidence and workflow layer. It should help answer:

- What exactly is this product, compound, device, test, company, or protocol?
- Which claims are marketing statements, scientific findings, regulatory facts, practitioner
  opinions, or personal experiences?
- What evidence supports or contradicts those claims, and how well does it apply to the exact
  formulation, device, population, dose, or use case?
- Which risks, interactions, adverse effects, harmful exposures, uncertainties, conflicts of
  interest, and missing measurements remain?
- What alternatives were considered, why was one selected, and what should happen next?
- What did the system learn that should be evaluated and safely added to the knowledge graph?

The backend is designed to make those questions reproducible rather than hiding them inside an
opaque chat transcript.

## What this service owns

The service is the governed path from an intent or source to an evaluated BellLabs artifact. Its
major responsibilities include:

- versioned workflow definitions, aliases, drafts, publication, and compilation into immutable
  Effective Run Configurations;
- run admission, lifecycle transitions, budgets, pauses, approvals, commands, parent/child run
  composition, and durable result settlement;
- deterministic **StageGraph** workflows for known dependency graphs;
- bounded **GoalDirected** workflows for adaptive discovery, repair, and convergence-based work;
- exact prompt, model, tool, MCP server, skill, workspace, secret, and sandbox bindings;
- source procurement, registration, extraction, trust assessment, and evidence preservation;
- compact-schema selection and retrieval of only the relevant graph schema;
- candidate extraction, identity resolution, claim adjudication, graph matching, ingestion
  planning, review, approved writes, and post-write validation;
- coordinator APIs and MCP surfaces for discovering capabilities and launching admitted work;
- traces, datasets, evaluators, safety checks, citation-fidelity checks, and learning loops;
- durable artifacts and provenance for downstream APIs, skills, applications, and commerce
  surfaces.

BellLabs application code remains the authority for what is allowed, budgeted, accepted,
promoted, written, and considered complete. Agent frameworks execute work; they do not define the
scientific, security, or governance truth of the platform.

## Runtime direction: LangChain, LangGraph, Deep Agents, and LangSmith

BellLabs is switching its agent-execution direction from the **OpenAI Agents SDK plus Temporal**
architecture to the **LangChain/LangGraph, Deep Agents, and LangSmith** ecosystem.

This is an execution-platform migration, not a rewrite of the BellLabs domain model. The existing
control-plane compiler, immutable definitions and bindings, admission rules, run-control reducer,
budget ledger, schema-grounding boundary, ingestion semantics, provenance model, and database
authority boundaries are intended to survive the migration.

### Why the switch

The OpenAI Agents SDK and Temporal combination proved valuable concepts: durable workflows,
bounded agent execution, exact operation bindings, retries, intervention, and strong separation
between execution mechanics and domain state. It also required BellLabs to build and reconcile a
large amount of agent-runtime infrastructure across two abstractions.

The LangChain ecosystem is now the preferred direction because it offers a more integrated set of
agentic-engineering capabilities and prebuilt solutions:

- **LangGraph** provides graph-native, stateful agent orchestration, checkpointing, interrupts,
  streaming, replay, forks, and explicit control over deterministic and agentic nodes.
- **Deep Agents** provides a prebuilt harness for long-horizon work, planning, subagents, context
  management, filesystem-oriented artifacts, and tool use without requiring every agent pattern
  to be constructed from scratch.
- **LangChain** provides the model, tool, middleware, structured-output, retrieval, and provider
  integration layer needed to route work across different model and infrastructure choices.
- **LangSmith** unifies tracing, datasets, evaluation, prompt and context iteration, Studio-based
  debugging, operational visibility, and a deployment platform for Agent Server workloads.

Together, these components better match the kind of agentic engineering BellLabs needs: observable
graphs, resumable state, human intervention, reusable subgraphs, prebuilt agent patterns, systematic
evaluation, and a managed route from local development to deployment.

### Migration posture

The repository still contains production-oriented Temporal workflows and OpenAI Agents SDK
adapters. The current `pyproject.toml`, worker entry points, Docker Compose stack, probes, and tests
reflect that baseline. They are not yet evidence that the new runtime has fully replaced the old
one.

Migration will proceed through compatibility, parity, and cutover gates:

1. Freeze representative StageGraph and GoalDirected behavior and evaluation fixtures.
2. Introduce runtime-neutral contracts and an anti-corruption layer around the BellLabs domain.
3. Prove a minimal LangGraph Agent Server vertical slice with LangSmith tracing.
4. Re-express StageGraph execution while preserving deterministic scheduling, identities,
   idempotency, budgets, joins, cycles, and settlement.
5. Re-express GoalDirected execution with Deep Agents while preserving protected scope, bounded
   iteration, independent verification, no-progress detection, and typed terminal results.
6. Add durable interrupts, steering, cancellation, recovery, subagents, sandboxes, and API/MCP
   convergence.
7. Run both paths only long enough for shadow, replay, parity, safety, and cost evaluations.
8. Cut over execution authority and drain the legacy runtime rather than maintaining two permanent
   workflow authorities.

See the current engineering material in
[`docs/LANGGRAPH_DEEPAGENTS_CONTROL_PLANE_MIGRATION_PLAN.md`](docs/LANGGRAPH_DEEPAGENTS_CONTROL_PLANE_MIGRATION_PLAN.md)
and
[`docs/LANGGRAPH_LANGSMITH_MIGRATION_RECOMMENDATIONS.md`](docs/LANGGRAPH_LANGSMITH_MIGRATION_RECOMMENDATIONS.md).
Those documents are active design material; the code and accepted domain specifications remain the
arbiter of implemented behavior.

## Workflow model

BellLabs separates a domain **Workflow Type** from its execution blueprint. StageGraph and
GoalDirected are general blueprint families, not descriptions of a biotech mission by themselves.
A research, ingestion, reconciliation, evaluation, monitoring, or product-comparison Workflow Type
can bind to either family when appropriate.

### StageGraph

StageGraph is the default for work whose dependencies and completion mechanics can be declared in
advance. It supports reproducible stage inputs and outputs, parallel branches, joins, invalidation,
bounded cycles, human gates, and typed settlement.

Typical uses include:

- source procurement and evidence triangulation;
- product, company, or technology landscape reports;
- schema-context selection;
- extraction, entity resolution, and ingestion pipelines;
- safety and contradiction review;
- report-to-graph reconciliation;
- evaluation and release gates.

### GoalDirected

GoalDirected is for work whose next useful action cannot be fully known before execution. It uses a
frozen initial goal and protected scope, bounded iterations, explicit budgets, durable handoffs,
no-progress detection, and independent verification.

Typical uses include:

- filling sparse or unexpected evidence gaps;
- open-ended ecosystem mapping;
- repairing a failed or incomplete artifact;
- long-tail case-study and experience-report discovery;
- resolving contradictions that require adaptive follow-up;
- deciding which specialized workflow or capability should run next.

GoalDirected does not mean unbounded autonomy. It must converge, stop, request review, or fail with
a typed reason.

## Research-to-ingestion learning loop

Reports are useful human artifacts, but prose is not ingestion-safe. A sentence may combine several
claims, erase source qualifications, merge different dates, confuse vendor claims with accepted
knowledge, or apply evidence from an ingredient or modality to a specific commercial product.

The intended high-level flow is:

```text
Question, entity, source, or monitoring event
  -> knowledge-graph preflight and mission binding
  -> source discovery, registration, and research
  -> cited report and evidence bundle
  -> exact schema-context selection
  -> source-grounded entity and assertion seeds
  -> graph retrieval and match candidates
  -> identity resolution, temporal reconciliation, and claim adjudication
  -> graph candidate and ingestion plan
  -> human/agent approval for consequential changes
  -> graph commit and post-write validation
  -> evaluation cases, improved retrieval, and future workflow seeds
```

Every serious run should have an afterlife. Its outputs may become new sources, claims, entities,
relationships, evidence bundles, product-decision examples, safety tests, evaluation cases, prompt
examples, protocol templates, or monitoring targets. This compounding loop is the core of the
platform's defensibility.

## First product surfaces

BellLabs will not expose the entire biotech ecosystem at once. The most likely early surfaces are
bounded consumer domains where evidence, identity, safety, and product comparison already create
clear value.

### Consumer supplements

Supplement intelligence should distinguish the product, variant, formulation version, ingredient
material, active component, serving basis, dosage form, lot or quality evidence, listing, offer,
and claimed goal. Evidence for a molecule or ingredient must not be silently treated as evidence
for every branded formulation.

Early capabilities may include formulation comparison, claim and study mapping, contaminant and
certification research, interaction and adverse-effect retrieval, price and availability tracking,
and inspectable evidence bundles.

### Consumer devices

Device intelligence should distinguish measurement, intervention, and hybrid devices; wellness
products and regulated medical devices; modality and delivered parameters; sensors and measured
metrics; exact models and software versions; protocols; subscriptions; privacy behavior; and
evidence for a general modality versus evidence for the exact device.

Likely examples include wearables, red-light devices, neurostimulation or sound devices,
environmental sensors, sleep products, recovery devices, and at-home measurement systems.

### Harmful compounds and exposures

BellLabs should make harmful compounds and exposure pathways first-class rather than treating
safety as a footnote. This surface may connect compounds, mixtures, contaminants, consumer
products, environmental media, exposure routes, biomarkers, symptoms, mechanisms, regulatory
thresholds, testing methods, remediation options, and uncertainty.

Initial use cases may include product contaminant research, endocrine-disrupting compounds, heavy
metals, mold and mycotoxin investigations, air and water hazards, occupational or household
exposures, and the difference between hazard, measured exposure, and demonstrated personal risk.

### Case studies and experience reports

Case studies, testimonials, adverse-event discussions, practitioner reports, and user experiences
contain valuable signals but require strict labeling. BellLabs should preserve source type,
selection bias, similarity to the user's situation, corroboration, conflicts, temporal context, and
the distinction between anecdote and clinical evidence.

This surface can help users discover real-world questions and safety signals without presenting
individual stories as proof of efficacy.

### How these surfaces may ship

The same governed knowledge and workflows are intended to support several delivery forms:

- **APIs** for search, evidence bundles, comparisons, entity pages, claims, safety context, research
  missions, and ingestion/evaluation status;
- **MCP servers** that let external agents retrieve BellLabs context and launch narrowly authorized
  workflows;
- **agent skills** that package repeatable tasks such as supplement comparison, device evidence
  review, exposure investigation, or case-study triangulation;
- **web applications** for exploration, review, workflow operations, evidence inspection, and
  reasoning-backed product decisions;
- **mobile applications** for saved decisions, scanning and discovery, reminders, observations,
  protocol state, device or report intake, and post-purchase follow-up.

These are different interfaces over the same provenance and policy model, not separate truth
systems.

## BellLabs roadmap

The roadmap is intentionally phased so each layer improves the next.

### 1. Foundation and graph legibility

Stabilize the Neo4j schema and GraphQL boundary, canonical entity pages, compact schema maps,
Graph RAG, research-run and artifact registries, evidence bundles, claims, decision-ledger schemas,
and safety categories.

### 2. Research mission MVP

Let a user begin a structured research campaign from a question or knowledge-graph entity. Add KG
preflight, source procurement, cited reports, ingestion candidates, evaluation cases, human
checkpoints, and trace inspection.

### 3. Context-aware ingestion

Transform evidence into reviewable graph improvements. Select relevant schema, retrieve current
graph context, extract candidates, resolve identity, adjudicate claims, generate create/update/
merge/replace/disconnect/ignore/defer decisions, execute approved writes, validate changes, and
retain rollback provenance.

### 4. Consumer intelligence surfaces

Expose supplements, devices, harmful compounds and exposures, and case studies through APIs, MCP,
skills, and initial web/mobile experiences. Expand into adjacent longevity diagnostics,
environmental testing, lab tests, wearables, educational guides, and product/vendor profiles.

### 5. Agentic cart and decision ledger

Make product consideration inspectable and durable. Link items to goals, protocols, alternatives,
evidence, risks, commercial relationships, budgets, and lifecycle state. Start with external links
and assisted checkout before deeper vendor integrations.

### 6. Personal health graph and dashboard

Keep private user data separate from the public knowledge graph. Add consent-aware report parsing,
biomarkers, specimens, reference ranges, wearable streams, observations, goals, constraints,
protocol states, timelines, and clinician-review prompts for high-risk findings.

### 7. Protocol studio

Support protocol templates, forks, versions, measurements, schedules, adherence, retrospectives,
agent checkpoints, stop conditions, safety gates, and appropriately redacted community sharing.

### 8. Workflow platform, evaluation, and learning

Complete the LangGraph/Deep Agents/LangSmith control plane, composable workflows, intervention,
scheduling, monitoring, model routing, sandboxing, evaluation suites, citation checks, safety tests,
and trace-derived learning. Fine-tuning or distillation comes only after high-quality labeled traces
and governance exist.

### 9. Broader biotech ecosystem and marketplace

Expand coverage from consumer categories to organizations, diagnostics, assays, biomarkers,
multi-omics, therapeutics, clinical trials, patents, researchers, manufacturers, supply chains,
regulation, care and laboratory services, financing, partnerships, protocols, and validated data
products. Add structured vendor profiles, APIs, community publishing, transparent commercial
relationships, and native marketplace flows where appropriate.

### 10. Cautious personal modeling

Only after the knowledge, provenance, evaluation, privacy, and safety foundations are mature,
develop longitudinal personal models and limited scenario analysis across labs, wearables,
observations, protocols, and decisions. Uncertainty and clinical boundaries remain explicit; the
system must not turn sparse personal data into false diagnostic certainty.

See the canonical [`roadmap.md`](../biotech-meta/docs/BellLabs/roadmap.md) for the current phase
definitions. This README describes the product trajectory and may lead or lag individual
implementation issues.

## Agentic commerce and reasoning-backed product decisions

BellLabs treats purchasing as the visible tip of a research process. The primary artifact is not
the cart row or affiliate link; it is the **reasoning chain and evidence package attached to the
decision**.

For every considered supplement, device, lab test, service, guide, or protocol component, the
system should be able to show:

- the user's goal or question that introduced it;
- the exact product, formulation, model, vendor, listing, price, and time context;
- claims and sources that supported, weakened, or contradicted the choice;
- relevant studies, mechanisms, case studies, safety signals, and unresolved questions;
- alternatives considered and why they were rejected or deferred;
- personalization inputs used with permission, plus what was not known;
- contraindications, interactions, uncertainty, clinician-review triggers, and purchase friction;
- the approving user or workflow and any commercial relationship affecting presentation;
- what happened after purchase and whether the outcome changed the original reasoning.

The product-decision ledger should survive even when checkout happens on another site. Commerce can
progress in levels: external link, assisted checkout, vendor API checkout, agent-mediated commerce,
and finally a BellLabs-native marketplace. At every level the user should be able to inspect why an
item appeared, separate evidence from promotion, and see affiliate, referral, sponsorship, or vendor
incentives.

Reasoning provenance does not mean publishing private hidden model reasoning. BellLabs should store
the auditable decision artifacts that matter—goals, evidence, claims, alternatives, evaluations,
policy checks, uncertainties, approvals, and outcomes—rather than exposing raw chain-of-thought.

## Cost-effectiveness and model strategy

Cost-effective agentic research at scale is one of the project's hardest unresolved engineering
problems. Deep research can combine long contexts, repeated retrieval, browser or MCP calls,
parallel branches, verification, structured extraction, and multiple model passes. A workflow that
looks affordable as a single demonstration may become untenable across thousands of entities,
products, sources, users, or monitoring runs.

BellLabs therefore cannot route every step to a frontier model in real time. The emerging strategy
is a measured, evaluation-driven model portfolio:

- use deterministic code, retrieval, rules, and cached immutable artifacts whenever reasoning is
  unnecessary;
- use smaller or cheaper models for classification, normalization, routing, candidate generation,
  and other tasks where evaluations show adequate reliability;
- reserve frontier models for the points where their reasoning quality changes the result, such as
  difficult planning, contradiction resolution, complex entity identity, safety adjudication,
  high-impact synthesis, and independent verification;
- submit targeted frontier-model work through provider **Batch APIs** at selected StageGraph stages
  or GoalDirected boundaries when latency is flexible and batching improves unit economics;
- cap iterations, tokens, tool calls, concurrency, and wall-clock time with run-level and stage-level
  budgets;
- reuse source snapshots, extraction products, embeddings, graph context, and verified artifacts by
  content digest rather than paying to rediscover the same facts;
- measure cost per accepted claim, resolved entity, validated graph change, useful evidence bundle,
  and completed user decision—not only cost per token or per run.

At larger volumes, BellLabs may also need open-weight models deployed on cloud-provider GPU
clusters for stable, high-throughput workloads. That path is not automatically cheaper: GPU
utilization, autoscaling, queueing, inference optimization, model serving, observability, security,
upgrades, and operations all carry real cost. Open-weight models should be adopted only where a
repeatable workload, quality evaluation, privacy requirement, or sustained utilization makes the
total cost of ownership favorable.

The intended routing hierarchy is therefore heterogeneous: local deterministic work, retrieval and
cached artifacts, economical hosted models, targeted frontier calls, asynchronous batch work, and
qualified open-weight inference. LangChain's provider abstractions, LangGraph's explicit nodes, and
LangSmith's traces and evaluations should make those choices measurable rather than ideological.

## Architecture and authority boundaries

The current and target runtimes share the following intended ownership model:

| Concern | Authority |
|---|---|
| Workflow definitions, exact revisions, aliases, and Effective Run Configurations | MongoDB and the BellLabs control plane |
| Run admission, lifecycle, budgets, commands, approvals, links, outbox, and terminal results | Application PostgreSQL |
| Agent execution checkpoints and resumable runtime state | Currently Temporal persistence; target LangGraph/LangSmith checkpointer storage |
| Canonical public biotech knowledge and relationships | Neo4j through bounded application-owned query/write ports |
| Immutable payloads, reports, source snapshots, and large artifacts | MongoDB metadata plus S3-compatible object storage |
| Realtime fan-out, notifications, cache, and ephemeral coordination | Redis; never the sole durable authority |
| Prompts, tools, MCP servers, skills, models, sandboxes, and workspaces | Exact BellLabs bindings; LangChain/Deep Agents execute only the granted set |
| Tracing, datasets, experiments, and evaluations | LangSmith plus BellLabs domain evaluation records |

Important invariants:

- Public knowledge and private personal-health data remain separate.
- Agents do not receive arbitrary database write access.
- Schema-context selection is content-addressed and default-deny at the graph boundary.
- Mutable aliases are resolved before admission; runs carry exact revisions and digests.
- Technical retries are distinct from semantic attempts and workflow iterations.
- Expensive or consequential side effects require idempotency, budget reservation, and durable
  settlement.
- Human approval is required at configured safety, privacy, commerce, and graph-mutation gates.

## Current implementation map

```text
app/
├── api/              FastAPI control-plane, run-control, and schema-grounding routes
├── application/      use cases, composition roots, coordinators, promotion, and execution services
├── domain/           framework-neutral contracts, reducers, schedulers, policies, and value objects
├── integrations/     PostgreSQL, MongoDB, Neo4j, S3, Redis, model, MCP, and runtime adapters
├── mcp/              governed BellLabs coordinator MCP server and resources
├── migrations/       ordered application PostgreSQL migrations
├── models/           persistence and transport models
├── temporal/         current durable workflow baseline and migration-era workers/probes
├── preflight.py      external-system readiness checks
└── server.py         FastAPI, Socket.IO, health, lifecycle, and optional MCP composition
```

Useful implementation guides:

- [`docs/CODEBASE_DOMAIN_WORKFLOW_GUIDE.md`](docs/CODEBASE_DOMAIN_WORKFLOW_GUIDE.md)
- [`docs/SCHEMA_GROUNDING_PIPELINE.md`](docs/SCHEMA_GROUNDING_PIPELINE.md)
- [`docs/workflow-control-plane-current-state-and-next-slices.md`](docs/workflow-control-plane-current-state-and-next-slices.md)
- [`docs/WORKFLOW_IMPLEMENTATION_BINDINGS_PROTOTYPE.md`](docs/WORKFLOW_IMPLEMENTATION_BINDINGS_PROTOTYPE.md)

## Current API and MCP surfaces

The FastAPI application currently exposes:

- `/health/live` and `/health/ready` health checks;
- `/control-plane/v1` for definitions, drafts, publication, aliases, compilation, retirement, and
  control-plane schemas;
- `/run-control/v1` for run requests, commands, lifecycle state, budgets, composition links, and
  the durable outbox;
- `/schema-grounding/v1` for schema catalog, context, workspace, projection, and graph-boundary
  resources;
- OpenAPI documentation at `/docs`;
- an optional governed coordinator MCP deployment, mounted at `/mcp/coordinator` by default when
  enabled.

These are control and engineering surfaces. The consumer supplement, device, harmful-compound,
case-study, commerce, web, and mobile APIs described above are roadmap surfaces, not a claim of
current production availability.

## Local development

### Prerequisites

- Python 3.12+
- [`uv`](https://docs.astral.sh/uv/)
- Docker Desktop with Docker Compose v2
- credentials for any external integrations exercised by the chosen run

Never commit credentials, personal health information, or a populated `.env` file. Use the
variable names documented in `.env.example` and provide real values through a local secret manager
or environment-scoped development secrets.

### Install and start the current baseline

```powershell
uv sync
docker compose up -d
uv run python -m app.preflight
uv run uvicorn app.server:asgi_app --host 127.0.0.1 --port 8000
```

In a second terminal, start the current Temporal worker:

```powershell
uv run python -m app.temporal.worker
```

Current local endpoints:

- FastAPI: <http://127.0.0.1:8000>
- OpenAPI: <http://127.0.0.1:8000/docs>
- Temporal UI: <http://127.0.0.1:8080>

The Compose stack includes separate application and Temporal PostgreSQL services, Redis, Temporal,
and Temporal UI. The application PostgreSQL service is exposed on port `55432` by default to avoid
confusion with Temporal persistence or another local PostgreSQL installation.

Do not treat this Compose topology as a production deployment. The LangGraph/Deep Agents Agent
Server and LangSmith deployment runbook will replace or supplement these commands as the migration
vertical slices land.

### Optional current probes

The legacy bootstrap diagnostic is:

```powershell
uv run python -m app.temporal.run_probe
```

The governed operation probe starts an operation worker, verifies an immutable skill fixture, and
executes a bounded command in an isolated Docker sandbox:

```powershell
uv run python -m app.temporal.run_operation_probe
```

These probes exercise the current baseline and are migration fixtures, not the final product
experience.

## Verification

Run the standard checks from this repository:

```powershell
uv run ruff check app tests
uv run mypy app
uv run pytest
```

External-system and database integration tests require their documented environment variables and
should not be made to pass with fake credentials. A disposable application-PostgreSQL acceptance
path can be run with:

```powershell
$env:TEST_APPLICATION_POSTGRES_DSN="postgresql://belllabs:belllabs-local@127.0.0.1:55432/belllabs"
uv run pytest tests/test_run_control_postgres_integration.py -q
```

The test drops its application schema after verification. Do not run destructive database or
volume-reset commands against shared or non-disposable environments.

## Development principles

- **Evidence before authority.** Preserve what a source says separately from what BellLabs accepts.
- **Claims before summaries.** Atomize consequential assertions and retain source location,
  context, time, and epistemic status.
- **Evaluation before scale.** A cheaper workflow is not cost-effective if it creates unusable or
  unsafe knowledge.
- **Exact bindings before execution.** Reproducibility requires frozen versions, digests, grants,
  and budgets.
- **Human review at consequential boundaries.** Privacy, high-risk health interpretation, commerce,
  and important graph changes need visible gates.
- **Research is not medical advice.** BellLabs organizes evidence and decisions; it does not
  diagnose, prescribe, or manufacture certainty.
- **Commercial transparency.** Affiliate, referral, sponsorship, marketplace, and vendor incentives
  must be visible beside affected decisions.
- **Work in public artifacts.** Reports, traces, evaluations, decisions, and migrations should leave
  inspectable outputs that improve the system.

## Near-term focus

The immediate engineering program is to preserve the existing controlled-run behavior while
qualifying and implementing the LangGraph, Deep Agents, and LangSmith runtime; prove StageGraph and
GoalDirected parity; connect tracing and evaluation to cost and quality; then use that platform to
produce the first bounded consumer intelligence surfaces.

BellLabs is deliberately unfinished. The goal of the current phase is not to pretend the entire
biotech ecosystem is solved. It is to build a trustworthy system that can expand its coverage one
evaluated research mission, one reviewed graph change, and one inspectable decision at a time.
