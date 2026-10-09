---
type: Specification Annex
title: "Capability catalog, workspace and environment contracts"
description: "Normative design extends the admitted asset model in SPECIFICATION. Discovery does not imply installation or authority. Search results carry availability, compatibility, grants and evidence status; agents select exact…"
tags: [mission-control, spec, expansion]
---
# Capability catalog, workspace and environment contracts

Normative design extends the admitted asset model in [SPECIFICATION](../SPECIFICATION.md). Discovery does not imply installation or authority. Search results carry availability, compatibility, grants and evidence status; agents select exact versions before compilation/materialization.

## Catalog interface

Asset kinds: skill_bundle, mcp_server, mcp_tool, plugin_bundle, workflow_template, spawn_contract, agent_profile, model_profile, environment_profile, middleware_profile, context_bundle, memory_profile, knowledge_operation, notification_profile and ui_resource. The new canonical base specification's asset kinds use explicit versioned mappings; this does not require historical Biotech literal/data compatibility under the clean break. Do not silently change new-engine persisted literals. A plugin bundle is a pinned collection of admitted assets; installation cannot execute arbitrary package hooks as an agent action.

`CatalogAsset@1`: asset_id, immutable version, manifest_digest, kind, title/description/tags, issuer/provenance/license, content/object refs, input/output/state/context schema refs, compatible runtime/service versions, dependencies with exact versions, required grants, tool/side-effect scopes, environment constraints, optional billing model/ref, evidence refs/status, revocation state. Deployment configuration contains secret refs, never values.

`EnvironmentProfile@1`: exact image/snapshot and tool lock digests, harness placement, CPU/memory/disk/process/file/network ceilings, permitted repository/mounts/paths, token broker binding, process specs, MCP connection specs, memory namespaces/retention, checkpoint format/backend, sandbox lifetime/cleanup, model routes, output/state schemas and middleware order. Extra processes are deployment-admitted argv arrays, never mission text executed on the host. Reject unknown executable fields.

Public operations below the app prefix: `GET /catalog/assets`, `POST /catalog/search`, `GET /catalog/assets/{id}/versions/{version}`, `POST /context:select`, `POST /configurations:validate`, `POST /configurations:materialize` (worker/operator scoped), and existing mission spawn/start operations. CLI `catalog search/get`, `configuration validate`, `context select`; MCP `mission_catalog_search`, `mission_catalog_asset_get`, `mission_configuration_validate`, `mission_context_select`. Configuration validation returns missing/revoked dependencies, runtime incompatibility, scope/budget conflicts, intended binding digest and evidence gaps; it launches nothing.

Search request: schema_version, request_id, query, kind/tag/placement filters, intended task, allowed scope, max_results and token ceiling. Server determines actual scope from credentials. Search projection may combine lexical/full-text and a qualified vector reranker. Initial proof uses PostgreSQL full-text/lexical filtering; vector retrieval is a replaceable ranking adapter and requires reproducible evaluation. No Supabase vector extension or model deployment is assumed installed. Filter authority/compatibility before returning snippets or reranking content. Cursor is opaque signed query/scope/version state, not offset pagination over hidden results.

Search result includes exact asset refs, safe excerpts, score/rank provenance, availability `available/unavailable/gated`, evidence status and missing prerequisites. Authoring aliases resolve before commit; materializer rechecks immutable versions, grant revocation and actual tool/image/middleware versions. Dependency cycles, digest mismatches, unsupported plugin hooks and egress bypass reject. No product-specific fallback to the other app's catalog.

Workflow spawn contract declares template/version, strict inputs/goals, permitted overrides, authority intersection, depth/fanout ceilings, budget allocation, output projections, completion/child ownership and failure policies. Calling a skill or MCP tool that spawns work must use that contract and durable admission; arbitrary shell subprocesses cannot create untracked missions.

## Workspace manifest and materialization

Manifest fields: workspace_id, owner attempt/session/generation, environment profile ref/digest, image/snapshot digest, read-only context/skill/input file entries `(path, artifact_ref, digest, bytes, trust, mode)`, writable output/scratch prefixes, processes, network allowlist, quotas, lease/fence, expiry, cleanup/retention policy, optional repository/base commit and snapshot lineage. Profile is deployment-admitted; manifest is execution-admitted. Immutable durable artifacts live in `mission-artifacts`, evidence in `knowledge-artifacts`, assets in `capability-bundles`; sandbox storage is scratch until registered.

Materialization algorithm: admit/reserve -> persist workspace creation intent -> create/recover same native resource -> verify image/profile -> install exact bundles read-only -> hydrate selected context files and references -> validate path/digest/quota -> start admitted extra processes -> health-check MCP endpoints -> persist actual manifest/lease -> allow execution. A partial attempt records each resource handle/process receipt; retry reconciles before recreating. Unknown create outcome must be resolved; no duplicate paid sandbox.

Path rules: normalize POSIX sandbox paths, reject traversal, symlink escape, duplicate normalized paths and output writes into read-only seeds. `.mission/` contains Operating Contract, selected context manifest, state summary, pending commitments and public reporting procedure. `/skills/` has complete pinned bundles. `/inputs/` holds declared inputs; `/outputs/` holds candidates; `/scratch/retrieval/` holds search captures. Permission enforcement is host/backend-specific and must be proved; the installed executable Deep Agents backend's framework permissions limitation cannot be ignored.

## Retrieval CLI offload

Admitted Tavily/Firecrawl or other CLI profiles write retrieved bytes to scratch plus `retrieval_capture.json`: source URL/query, service/tool/version, retrieval time, authorization class, capture digest/bytes/media type, immutable source locator, truncation status, request/usage identity and allowed consumption. Tools return bounded index/excerpt manifests, not unbounded content. Agent selects a source/excerpt and calls range/schema-limited read. Preserve full capture only when custody/retention policy requires it; register cited bytes durably before accepting a report.

No automatic web browsing grants, credential discovery or paid search arises from CLI presence. Network access/tool grants are narrower than installation. Search results are untrusted data; source content cannot alter Operating Contract. Distinguish partial scrape, timeout, forbidden source and exact source failure from empty search. Reproduce using captured datasets/receipts, not an expectation that future web responses match.

## Safe process lifecycle

Each process spec declares process_key, argv, cwd within workspace, env secret references/scoped token requests, ports/protocol, readiness check, resource limit, restart bound, stdout/stderr redaction/offload, graceful termination policy and ownership. No shell string interpolation, global env secret injection or arbitrary host service startup. Start with an allowlisted launcher inside the sandbox; record native PID/process group or provider handle plus generation. PID alone cannot identify a process after restart.

Processes use workspace-bound ports. Sidecars/MCP servers cannot access host mounts/database credentials or unrelated tenants. Cleanup signals owned process groups, observes exit, retains required output bytes, revokes task tokens, terminates sandbox and settles usage. Failed cleanup creates reconciliation case and retains exposure; lease expiry does not imply deletion. Lost/late processes cannot publish results after fence. Fork restores immutable files in a new resource and never copies running processes.

## Persistence, security and reproducibility

Sandbox snapshot includes source image, declared files, tool/config versions and custody digest; omit secrets, tokens and provider login caches. Native checkpoints may contain sensitive tool arguments; encrypt/access-control/retain them separately. Credentials remain server-side; necessary in-sandbox CLI tokens are short-lived app/tenant/attempt/action bound and revocable. Remote destinations/signed object URLs are allowlisted; no arbitrary URI fetch through the control service.

Artifacts register after digest/schema/size/malware policy checks and authorization; signed preview links are short-lived. Partial uploads are unreferenced objects quarantined/collected by audited cleanup. Snapshot retention cannot delete bytes needed by active continuations/evidence. Disk, file count, process count, bandwidth/tool/provider costs and lifetime ceilings reserve exposure; unknown usage blocks acceptance. Checkpoints use qualified private app-local runtime schemas and separate backups from scratch. Resource availability failures affect the bound app/profile, never cause cross-app rerouting.

Mandatory proofs: same configuration digest yields same installed bundle/image/file manifest from captured inputs; malicious symlink/path and plugin postinstall denied; unauthorized MCP/process egress blocked; lost sandbox-create response recovered; cleanup failure visible; CLI retrieval exceeds context limit yet bounded excerpts succeed; cited capture survives scratch teardown; fork gets new writer/process ownership; secrets absent from prompt/files/outputs/traces. Live provider/network qualification is a separately budgeted gate.
