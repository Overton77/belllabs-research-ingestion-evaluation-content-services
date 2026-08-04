# Coordinator MCP Phase 5 specification

Date: 2026-07-27  
Status: Implementation plan with explicit provisional result policy  
Depends on: accepted Phase 4 handoff

## Objective

Let the coordinator inspect large and completed runs without turning
`get_workflow_result` into an unbounded payload. Split status, typed result summary, bindings,
artifacts, evidence, events, and—when an official report contract exists—reports into
progressively retrievable resources.

## Current infrastructure truth

Phase 5 must not assume infrastructure that has not been designed or deployed.

What exists now:

- `WorkflowResultRecord` carries terminal outcome, `output_contract_results`, `artifact_refs`,
  `evidence_refs`, warnings, degradations, operation-binding refs, usage summary, and
  family-specific execution details.
- `CoordinatorResultService` joins the run projection to that immutable typed result.
- PostgreSQL typed-result persistence exists through
  `PostgresWorkflowResultRepository`.
- MCP currently registers run `result`, `launch`, and `bindings` resources.
- `app/integrations/artifact_payloads.py` and current web-research support can store certain
  payloads in S3 when `Settings.s3_bucket` is configured.
- The web-research live path has specific verified S3 screenshot behavior.

What does not yet exist as canonical authority:

- a canonical bucket/key/retention configuration for every artifact class;
- a general artifact locator and authorization policy covering every result reference;
- an official Stage report contract;
- an official full-Workflow report contract;
- durable report repository/index/search models;
- complete report, artifact, evidence, event collection resources.

Therefore, Phase 5 must use available typed result returns first and advertise richer navigation
only where an actual provider and durable record exist.

## Provisional result-return policy

Until canonical artifact storage and report contracts are accepted:

1. `get_workflow_result` returns a bounded coordinator summary built only from the authoritative
   run projection and existing immutable `WorkflowResultRecord`.
2. Preserve current fields rather than inventing report authority:
   - run/request identity;
   - phase and terminal outcome;
   - output-contract result summary;
   - warnings and degradations;
   - artifact/evidence/binding references already persisted;
   - usage and family execution summary;
   - stable result digest/revision added without mutating the immutable payload.
3. Return the canonical existing result URI and only additional links that are both registered
   and readable by the same principal.
4. An `artifact_ref` or `evidence_ref` may be returned as an opaque durable reference. Do not
   rewrite it into an `s3://` URI unless the authoritative record already contains that URI.
5. Do not upload arbitrary inline outputs to the single configured S3 bucket merely to satisfy
   this phase.
6. If referenced content has no authorized retrieval provider, return metadata plus
   `retrieval_state: unavailable` and a stable reason. Do not claim the object is navigable.
7. Do not label `output_contract_results` as a Stage report or Workflow report.
8. Report tools/resources remain omitted or explicitly unavailable until official report
   contracts and providers are composed.

This policy is a compatibility bridge, not the final report architecture.

## Scope A: status and typed result summary

Separate:

- `get_workflow_status` — current lifecycle, outcome when terminal, readiness, blockers, and
  observation identity;
- `get_workflow_result` — immutable typed terminal summary and progressive links.

Output readiness is distinct from execution outcome. A run may be terminal while an optional
artifact is unavailable, quarantined, pending promotion, or not retrievable through MCP.

Define response size limits and deterministic truncation/summary metadata. Never silently drop
references.

## Scope B: binding navigation

Mature `belllabs://runs/{run_id}/bindings` into a scope-authorized, paginated view over actual
semantic and operation execution bindings. Include exact assets, operation class, authority
grant, capability binding, and immutable identities without exposing credentials.

Binding resources are mandatory for Phase 5 because current application authority exists.

## Scope C: artifact and evidence navigation

Before exposing collection tools, define a minimal reference metadata view:

- reference identity and kind;
- owning run and output/operation relation;
- digest when known;
- media type and size when known;
- storage/retrieval state;
- sensitivity/authorization summary;
- immutable locator only when authorized;
- creation/observation time;
- unavailability reason.

Then implement adapters only for artifact/evidence record types already backed by durable
authority. Collections use opaque cursors and stable ordering.

Canonical S3 policy is a prerequisite for general object retrieval, not for returning existing
opaque refs in the typed result.

## Scope D: events

Expose a compact, paginated application-owned event projection if one is accepted. Temporal
history may inform the projection but must not become the public domain contract or expose raw
workflow internals. If no application event projection is available, defer this resource and
report it in the handoff.

## Scope E: reports

Report work is gated:

### Gate 1: contract acceptance

Accept separate exact contracts for:

- Stage report;
- full-Workflow report;
- report identity, revision/digest, provenance, and relation to typed outputs/evidence.

### Gate 2: durable materialization

Add idempotent application-owned report materialization and repositories.

### Gate 3: coordinator navigation

Only then add:

- `search_run_reports`
- `get_run_report`
- `belllabs://runs/{run_id}/reports`
- `belllabs://runs/{run_id}/reports/{report_id}`

Until all gates pass, these operations must not be advertised as available. A summarization prompt
may summarize the existing bounded typed result, but it must say that it is a generated
coordinator summary, not an official Stage or Workflow report.

## Target resource progression

Mandatory with current authority:

```text
belllabs://runs/{run_id}/status
belllabs://runs/{run_id}/launch
belllabs://runs/{run_id}/bindings
belllabs://runs/{run_id}/result
```

Conditional on durable provider readiness:

```text
belllabs://runs/{run_id}/result-summary
belllabs://runs/{run_id}/artifacts
belllabs://runs/{run_id}/evidence
belllabs://runs/{run_id}/events
belllabs://runs/{run_id}/reports
belllabs://runs/{run_id}/reports/{report_id}
```

Bootstrap must report each conditional family independently.

## Canonical storage decision required

Phase 5 may document requirements and implement ports, but must not silently establish the
system-wide S3 policy. An accepted storage decision must define at least:

- artifact classes and owning lifecycle;
- bucket/account/region separation by environment and sensitivity;
- object key namespace and collision/idempotency rules;
- encryption and key management;
- retention, legal hold, deletion, and promotion;
- tenant isolation and authorization;
- integrity digests and metadata;
- upload/download size and media-type controls;
- presigned/direct retrieval policy;
- provenance and database record of authority;
- migration from existing screenshot and artifact refs.

The existing single `s3_bucket` setting is an adapter configuration, not this policy.

## Application ownership

Primary areas:

- `app/domain/coordinator/launch.py`
- `app/application/coordinator_results.py`
- `app/application/postgres_workflow_result_repository.py`
- `app/application/orchestration_binding_repository.py`
- `app/application/operation_execution.py`
- `app/application/artifact_promotion.py`
- `app/integrations/artifact_payloads.py`
- `app/mcp/coordinator_server.py`
- `app/mcp/coordinator_resources.py`
- `app/mcp/coordinator_prompts.py`
- `app/config.py`

Add report domain/application packages only after the report lifecycle and authority are accepted.

## Verification

- Nonterminal status does not fabricate a result.
- Terminal result is immutable, bounded, digest-stable, and scope-authorized.
- Output readiness can differ from terminal outcome.
- Every returned link has a registered provider and is readable by the caller.
- Missing providers yield explicit unavailable metadata, not broken links.
- Opaque existing refs survive unchanged.
- Collections paginate stably and enforce tenant/request scope.
- Bindings expose no secret values.
- Large inline outputs trigger bounded summary/reference behavior.
- Report operations remain absent until official contracts and repositories exist.
- Generated summaries are never labeled official reports.
- Existing verified S3 screenshot paths continue to work without becoming a general bucket
  policy.

## Exit criteria

Phase 5 is accepted when:

1. the coordinator can always obtain bounded status and, for terminal runs, the existing durable
   typed result summary;
2. it can navigate bindings and every other advertised result collection;
3. unavailable artifact/evidence content is represented truthfully;
4. no resource link is broken or unauthorized;
5. missing canonical S3/report infrastructure is visible in capability state and handoff;
6. official report APIs are either fully backed by accepted contracts and durable providers or
   are not advertised.

## Incoming Phase 4 checks

Confirm exact binding, composition result-admission, workspace, snapshot, and artifact mapping
identities. Preserve separate child-run result records and parent admission state.

## Outgoing handoff to Phase 6

Include:

- status/result summary schemas and size limits;
- result digest/revision rule;
- mandatory and conditional URI inventory;
- collection pagination semantics;
- binding/artifact/evidence retrieval-state model;
- exact list of artifact classes with working durable providers;
- explicit list of classes lacking canonical storage;
- report contract/provider state;
- any generated-summary prompt and its non-authoritative labeling;
- test evidence for large, partial, unavailable, and composed-run results.

Phase 6 may materialize only resources that have authorized, digest-bearing retrieval. It must not
copy unavailable or opaque content into a workspace by guessing its location.
