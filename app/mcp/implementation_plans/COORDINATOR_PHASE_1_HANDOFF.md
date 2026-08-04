# Coordinator Phase 1 handoff

Status: accepted; implementation and deployment Gate A complete.

## Effective surface and defaults

The MCP surface is derived from feature flags and composed providers. Startup fails when an
enabled required provider is absent. Optional prompts and run resources are omitted when their
bindings/providers are absent.

- Always available: `coordinator_bootstrap`, `get_capability`,
  `validate_workflow_design`.
- `CAPABILITY_SEARCH_ENABLED`: adds `search_capabilities`.
- `EXTERNAL_CAPABILITY_DISCOVERY_ENABLED`: adds both discovery tools. They are annotated as
  open-world, non-read-only operations because they persist quarantined candidates/evidence.
- `COORDINATOR_LAUNCH_ENABLED`: requires preparation, launch, result, and launch-context
  providers and adds prepare/launch/result tools.
- Run projection and semantic-binding readers add canonical run launch/binding resources.
- Exact published prompt bindings determine prompt advertisement.

All feature flags default to false. The mounted endpoint defaults to `/mcp/coordinator`.
Standalone mode defaults to explicitly read-only and does not advertise launch.

## Composition

Mounted:

```text
FastAPI lifespan
  -> Mongo definition state + capability PostgreSQL pool
  -> production CoordinatorFacade (application services)
  -> verified JWT principal resolver
  -> FastMCP Streamable HTTP ASGI app at /mcp/coordinator
```

Standalone:

```text
stdio FastMCP
  -> production read providers
  -> read-only runtime readiness
  -> static development principal from explicit CLI arguments
  -> no prepare/launch/result advertisement
```

Temporal:

```text
admitted exact blueprint family
  -> StageGraph or GoalDirected worker
  -> lifecycle terminalization accepted by Run Control
  -> application-owned TerminalWorkflowCompletionService activity
  -> immutable PostgreSQL WorkflowResultRecord
```

Coordinator workers fail composition when the durable completion provider is absent.

## Contracts and URIs

- Launch/result contracts use schema version `1`.
- Typed results use `WorkflowResultRecord` with family-specific
  `StageGraphResultDetails` or `GoalDirectedResultDetails`.
- Canonical returned result URI: `belllabs://runs/{run_id}/result`.
- Run resources:
  - `belllabs://runs/{run_id}/launch`
  - `belllabs://runs/{run_id}/bindings`
  - `belllabs://runs/{run_id}/result`
- Catalog and Workflow Type resources remain exact-revision reads.
- Every read derives tenant/request scope from the authenticated principal; caller payloads
  cannot widen scope.

## Ordering and retries

The workflow reports authoritative terminal facts to an application service. The service verifies
the terminal Run projection and terminal outcome, validates the frozen semantic binding when
present, and writes one content-addressed result. Identical retries return the existing record.
A different digest for the same Run is rejected. Ambiguous transport/storage failures remain
retryable; deterministic completion conflicts are non-retryable Temporal activity failures.

## Deployment order

1. Apply application migrations through:
   - `0006_coordinator_launch_tickets.sql`
   - `0008_orchestration_input_bindings.sql`
   - `0010_coordinator_audit_events.sql`
   - `0011_coordinator_workflow_results.sql`
2. Publish/reindex coordinator skill, prompt, Workflow Type, implementation, blueprint, profile,
   MCP tool, and browser-skill definitions in MongoDB/capability search.
3. Start StageGraph/GoalDirected workers with lifecycle, semantic-routing, and terminal-completion
   providers.
4. Start FastAPI with coordinator dependencies injected before lifespan startup.
5. Enable search/discovery/launch flags only after their providers and worker readiness are live.

## Verification evidence

- Focused Phase 1 suite: 49 passed.
- Full repository suite: 389 passed, 8 skipped.
- Ruff over `app`, `tests`, and `scripts`: passed.
- Mypy over all changed Phase 1 modules: passed.
- Mounted Viome Gate A completed over FastAPI Streamable HTTP and selected exact
  `web-research-browser-verification` revision 2.
- The authorized MCP path completed prepare → launch → Temporal → immutable typed result.
- Both provider operations succeeded; two independent browser pages were verified.
- Two content-addressed screenshots were persisted, read back, and digest-verified in the private
  test S3 bucket.
- Canonical launch, binding, and result URIs were read successfully through the mounted MCP
  endpoint.
- The terminal outcome was `completed`; durable PostgreSQL audit evidence includes every MCP
  operation exercised by the configured path.
- Secret-free acceptance evidence is at `.artifacts/phase-1-viome/result.json`.

The disposable test bucket blocks all public access, uses SSE-S3 encryption, and expires objects
after seven days. It is acceptance infrastructure only, not a decision about the eventual
multi-tenant artifact-storage architecture.
