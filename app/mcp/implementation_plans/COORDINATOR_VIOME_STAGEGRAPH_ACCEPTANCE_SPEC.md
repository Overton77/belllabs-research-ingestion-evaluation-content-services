# Official Viome StageGraph acceptance specification

Date: 2026-07-27  
Status: Official coordinator tracer plan  
Depends on: Phase 1 for the core tracer; Phase 6 for the full product gate

## Mission

Launch the following objective as a governed StageGraph mission through the Coordinator MCP:

> Find the flagship product or service sold by the biotechnology company Viome.

This is a bounded current-web research task. It validates coordinator discovery, exact asset
selection, proposal construction, preparation, admission, Temporal execution, result persistence,
and evidence-backed inspection. It is not a rigorous biomedical Research Mission and must not
produce medical advice or treat commercial claims as scientific validation.

The expected product name must not be hard-coded. Viome's offering can change, so acceptance is
based on live evidence, contract conformance, and visible uncertainty.

## Two acceptance gates

### Gate A: Phase 1 core tracer

Required as part of Phase 1:

- MCP bootstrap;
- internal discovery through the currently available catalog surface;
- exact retrieval of the published web-research Workflow Type and StageGraph implementation;
- exact promoted Tavily capability selection;
- server validation where currently implemented;
- prepare, inspect ticket, and launch through MCP;
- actual run admission and Temporal StageGraph execution;
- durable typed result retrieval through the canonical run result URI;
- readable launch/result/binding URIs that are advertised.

Gate A may inspect evidence, artifact, and output references through the existing
`WorkflowResultRecord`. It does not require official Stage/full-Workflow reports, general
artifact S3 retrieval, Phase 4 composition, or a Phase 6 sandbox bundle.

### Gate B: full product acceptance

Run after Phase 6:

- begins in a fresh clone of the reviewed coordinator base snapshot;
- uses decision-oriented discovery and exact schema-bearing launch contracts;
- validates asset/workspace/snapshot fit;
- exercises progressive status/result/binding/artifact/evidence/report resources only when live
  bootstrap says they are available;
- verifies content-addressed workspace materialization when supported;
- records explicit unavailability for optional report/artifact providers that remain unshipped.

Gate B cannot fail merely because an optional Phase 5 official report API was truthfully omitted.
It fails if the server advertises a resource that is broken or if the final evidence-backed result
cannot be obtained through available authoritative returns.

## Initial conditions

The test harness supplies only:

- the mission text;
- authenticated coordinator principal;
- tenant and request scope;
- permission to read the internal catalog and launch this accepted test run;
- policy, environment, budget, and approval fixtures required by the governed path;
- configured Tavily credential through the normal provider-managed secret boundary;
- runtime dependencies such as databases, Temporal, worker, and configured artifact adapters.

The harness must not supply:

- an answer about Viome;
- preselected exact Workflow Type, implementation, Tavily, model, prompt, or workspace refs;
- a final launch proposal;
- a direct Temporal workflow call;
- a candidate-only external asset as authorized;
- prior Viome evidence or report files;
- credentials inside the workspace.

## Required coordinator flow

1. Call `coordinator_bootstrap` and record effective tools, resources, prompts, workflow families,
   and provider readiness.
2. Search the internal catalog for a published general current-web research Workflow Type.
3. Select a StageGraph implementation suitable for bounded product/service identification.
4. Retrieve the exact Workflow Type, implementation, launch contract, input schema, workspace
   requirements, and output contracts through currently available exact resources.
5. Search internal authority for the reviewed, promoted Tavily server/tool/skill and any required
   Agent Profile, prompt, model/runtime profile, and workspace template.
6. Reject candidate-only, unpromoted, forbidden, unavailable, incompatible, retired, revoked, or
   digest-mismatched assets.
7. Resolve or create a fresh governed workspace from an exact Workspace Template.
8. Construct the launch proposal from the mission, admitted inputs, exact selected definitions,
   bounded controls, and workspace reference.
9. Submit authoritative workflow-design/launch validation. Repair only permitted proposal fields;
   do not modify catalog authority locally.
10. Prepare the launch and inspect the frozen ticket for exact refs, digests, policy/environment
    snapshots, semantic binding plan, launchability, warnings, and idempotency identity.
11. Stop if Tavily or another required dependency is unavailable or if the ticket is not
    launchable. Record typed blockers; do not silently substitute providers.
12. Launch through `launch_workflow` using the prepared ticket.
13. Confirm admission occurred through run control and submission targeted the StageGraph
    Temporal family.
14. Allow real StageGraph activities to execute with the bound Tavily operation.
15. Poll status through MCP until a valid terminal outcome or bounded timeout.
16. Retrieve the immutable typed result and follow every returned, advertised, authorized
    resource link.
17. Produce a concise answer naming the best-supported current flagship offering, source
    locators, retrieval observations, candidate comparison, and uncertainty.

## Minimum StageGraph semantics

The selected approved graph may vary, but it must realize:

```text
frame objective and bounded search plan
  -> retrieve current Viome product/service evidence with Tavily
  -> extract and compare flagship-offering candidates
  -> verify the conclusion against retrieved sources
  -> emit concise output and evidence references
```

The workflow is bounded to:

- no broad company dossier;
- no biomedical efficacy or diagnostic-validity assessment;
- no medical recommendation;
- no knowledge-graph ingestion;
- no external mutation;
- no hidden linked Research Mission;
- no inference that marketing prominence establishes scientific validity.

## Workspace requirements

The run uses a fresh governed workspace containing, where supported:

- read-only admitted task brief;
- operation-specific working directories;
- allowed Tavily skill/helper instructions;
- retrieved source observations or references;
- candidate-comparison artifact or typed output;
- final concise result or report;
- Workspace Materialization Manifest linking governed files to durable records.

For Gate A, existing typed output and durable references are sufficient if the official report
and full materialization contracts do not yet exist. For Gate B, use Phase 6 bundle/materialization
capabilities that bootstrap reports as available.

Credentials remain provider-managed and never enter the workspace, result, artifact, evidence,
snapshot, log assertion, or test fixture.

## Tavily capability requirements

The selected Tavily capability must be:

- an exact promoted catalog asset;
- permitted by the Workflow Type and implementation;
- compatible with the selected operation class;
- available in the execution environment;
- included in the frozen semantic binding plan;
- recorded in the actual Operation Execution Binding;
- granted only to operations requiring web retrieval.

The existing reviewed web capability definitions and semantic binding implementation are useful
fixtures/evidence:

- `app/domain/coordinator/web_capability_fixtures.py`
- `app/application/web_research_semantic_binding.py`
- `app/application/web_research_semantic_handlers.py`
- `app/integrations/web_research_runtime.py`

The acceptance harness must still make the coordinator discover and select exact current records.

## Result and report behavior

The mandatory result is the existing immutable typed `WorkflowResultRecord`, including:

- terminal outcome;
- output contract results;
- evidence and artifact refs that were actually persisted;
- operation binding refs;
- warnings/degradations;
- usage and StageGraph execution details.

Apply the Phase 5 provisional policy:

- do not fabricate S3 locators for opaque refs;
- do not call an output an official Stage or Workflow report unless that contract/provider has
  shipped;
- use bounded coordinator summaries over the typed result when official reports are unavailable;
- record unavailable optional retrieval providers explicitly;
- require every returned URI to be registered and readable.

The final human-facing answer can be generated from available typed outputs and evidence
references. An official report API is not a prerequisite for Gate A.

## Test modes

### Recorded CI mode

Use recorded Tavily adapter responses with fixed source payloads and timestamps. Exercise the same
MCP, facade, preparation, admission, semantic binding, StageGraph, workspace, result, and resource
contracts as live mode. Only the external retrieval adapter differs.

Assertions target deterministic contract behavior, not a permanent product-name string.

### Live product mode

Use current Tavily retrieval and the deployed MCP/backend/Temporal composition. Mark and schedule
it separately from hermetic CI. Preserve source locators and retrieval observations while
respecting content and secret policy.

This is the official product demonstration.

## Acceptance assertions

The applicable gate passes only when:

- bootstrap capability state matches composed providers;
- the coordinator selected a published StageGraph path through catalog queries;
- exact selected refs/digests are recorded for Workflow Type, implementation, blueprint, Tavily
  assets, workspace, prompts/models, and evaluation bindings that apply;
- server validation and preparation occurred before launch;
- the launchable ticket was consumed exactly once;
- run admission occurred through `RunControlService`;
- Temporal executed `StageGraphWorkflow`, not a direct test stub;
- at least one actual operation binding used the exact required Tavily capability;
- the workspace was fresh and contained no credential;
- the answer was derived from retrieved evidence rather than a fixture-provided conclusion;
- source locators and retrieval observations were preserved;
- candidate comparison supports the flagship conclusion and exposes uncertainty;
- commercial prominence is distinguished from scientific validity;
- the run reached a valid terminal outcome;
- exactly one immutable typed result was persisted across retries;
- all returned resource URIs were readable by the same principal;
- unavailable optional Phase 5 resources were represented truthfully.

Gate B additionally requires a fresh reviewed sandbox clone, manifest verification, stale-resource
checks, and all effectively advertised progressive result capabilities.

## Failure classes

Fail closed and record evidence for:

- no suitable published StageGraph Workflow Type;
- required contract still opaque when exact schema is required by the active gate;
- Tavily missing, candidate-only, unpromoted, forbidden, unavailable, or incompatible;
- no StageGraph worker poller;
- preparation non-launchable or stale;
- direct harness-to-Temporal bypass;
- missing/incorrect semantic or operation binding;
- terminal run without durable typed result;
- conflicting duplicate result;
- broken advertised URI;
- evidence-free or hard-coded conclusion;
- credential or prior mission data found in the workspace.

Optional unadvertised report/general-artifact retrieval is a documented limitation, not a false
failure.

## Implementation and test references

- `app/mcp/coordinator_server.py`
- `app/application/coordinator_composition.py`
- `app/application/coordinator_launch.py`
- `app/application/coordinator_results.py`
- `app/application/web_research_coordinator_live.py`
- `app/temporal/coordinator_runtime.py`
- `app/temporal/stagegraph_workflow.py`
- `app/temporal/web_research_smoke.py`
- `tests/test_run_web_research_coordinator_live.py`
- `tests/test_coordinator_launch_preparation.py`
- `tests/test_coordinator_semantic_binding_integration.py`
- `tests/test_coordinator_temporal_runtime.py`

The existing live script/test path is a building block. Official acceptance must enter through
the Coordinator MCP and must not call the live application facade as a substitute. The closest
current live scenario is a general web-research/browser-verification mission with a different
objective; it is evidence that components work, not an existing Viome acceptance test. Reuse its
published StageGraph, binding, worker, and adapter seams where they satisfy this specification,
but replace scenario-specific objectives, evaluation fixtures, and hard-coded search planning
with the coordinator-driven Viome flow.

## Acceptance evidence handoff

Produce a final record using the shared handoff contract plus:

- gate (`core-tracer` or `full-product`);
- mission and run identity;
- exact selected asset manifest;
- prepared ticket and consumed state;
- workflow/Temporal identities;
- StageGraph execution trace summary;
- typed result identity/digest;
- URI-read verification;
- evidence/source manifest;
- workspace/snapshot manifest where applicable;
- current flagship conclusion and uncertainty;
- unavailable optional capabilities;
- redacted logs and test command/deployment revision.
