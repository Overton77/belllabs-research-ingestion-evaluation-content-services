---
type: Operator Guide
title: Configured Mission Control API
description: "The configured entrypoint is mission_control.bootstrap.api:create_app. It serves the scoped API and uses real asymmetric JWT verification, restricted PostgreSQL services, and the Temporal submitter when configured.…"
tags: [mission-control, operations]
---
# Configured Mission Control API

The configured entrypoint is `mission_control.bootstrap.api:create_app`. It serves the scoped
API and uses real asymmetric JWT verification, restricted PostgreSQL services, and
the Temporal submitter when configured. Startup performs no migration or seed writes.

The only storage mode is `production_common`: the common `mission_control` component
(business authority) and `mission_control_search` (rebuildable projection), installed
by `mission-db` from `packages/mission-control-db-contract/`. Startup verifies the
persisted installation identity, the complete attested release and its schema
fingerprint, this build's writer version (`mission-control-runtime/1`) and the
restricted pool roles (`bootstrap/common_installation.py`), and fails closed otherwise.
There is no transitional fallback. Release 1.0.0 is qualified on two local disposable
databases and **installed in both Supabase projects** (owner-approved 2026-10-03; see
[implementation status](MISSION_CONTROL_IMPLEMENTATION_STATUS.md)). Release 1.1.0 (the fast-track
migrations 0025 to 0030, PostgreSQL 17 required) is built and proven on scratch databases only; a
binding must pin `required_component_version="1.1.0"` to run against it, and it is not applied to
either live project. The three fast-track owner missions have not run; for them follow the
[owner fixture runbook](specs/fast-track-2026-10/OWNER-FIXTURE-RUNBOOK.md).

Operator-relative assets resolve from `MISSION_CONTROL_HOME`, defaulting to the
current working directory, rather than the installed package directory.

## Operator configuration

Create an operator JSON file outside version control using the typed models below.
All database settings are environment **variable names**, never DSNs in this file.
The public JWKS file contains only trusted RSA/EC/OKP public keys with unique `kid`s.
Supported configured algorithms are RS256, ES256, and EdDSA. Replace every example
identity with the installation's actual operator-owned identity before use.

```python
from pathlib import Path
from uuid import UUID
from mission_control.application.installations.registry import ApplicationBinding
from mission_control.adapters.auth.jwt import ActorGrant, ApplicationAuthentication
from mission_control.bootstrap.api import ApplicationDeployment, MissionDeployment

binding = ApplicationBinding.seal(
    application_id="biotech",
    # Must equal deployments/<app>/target.toml installation_id (allocated once, UUIDv7).
    installation_id=UUID("11111111-1111-4111-8111-111111111111"),
    binding_version="1",
    supabase_project_ref="VERIFIED_PROJECT_REF",  # equals target.toml project_ref
    database_secret_ref="MC_BIOTECH_RUNTIME_DSN",
    accepted_issuers={"https://YOUR_PROJECT.supabase.co/auth/v1"},
    accepted_audiences={"authenticated"},
    required_component_version="1.0.0",
)
auth = ApplicationAuthentication(
    binding=binding,
    issuer="https://YOUR_PROJECT.supabase.co/auth/v1",
    audience="authenticated",
    public_jwks_file=Path("public-jwks.json"),
    algorithms={"RS256"},
    grants=(ActorGrant(
        subject="VERIFIED_ISSUER_SUBJECT",
        actor_id="operator",
        tenant_ids={UUID("22222222-2222-4222-8222-222222222222")},
        permissions={"workflow_run.read"},
    ),),
)
deployment = MissionDeployment(
    storage_mode="production_common",
    max_request_bytes=1_000_000,
    applications=(ApplicationDeployment(authentication=auth),),
)
Path("deployment.json").write_text(deployment.model_dump_json(indent=2))
Path("binding.json").write_text(binding.model_dump_json(indent=2))
```

`binding_digest` is computed over the exact binding by `seal` and verified when loaded.
Claims must include `iss`, `aud`, `sub`, `exp`, and signed
`app_metadata.application_id` / `app_metadata.tenant_id`. Claim paths are configurable;
user-editable `user_metadata` is rejected as a scope source. The operator grants map
verified subjects to exact tenants, actor permissions, authority, sponsorship, and
approval references. Token `permissions` fields are ignored. Changing a path cannot
change an application's issuer or obtain another tenant's configured grant.

Pin trusted public keys from the issuer through the operator's provisioning process.
Key rotation and grant changes require reloading the deployment; restarting invalidates
the old in-process grant map. Token-supplied keys and JWKS URLs are never fetched.

## Install the common component (`mission-db`)

`mission-db` (distribution `mission-control-db-contract`, `uv sync --group dbcontract`)
is the only installer. Exit code 0 means passed, 2 means a blocked gate. It never runs
`supabase db reset`, unscoped pushes or domain SQL, and it never reads a DSN from a
file or argument: targets name an environment variable.

### Target manifest: `deployments/<app>/target.toml`

One authoritative file per app (`deployments/biotech/`, `deployments/ai-engineer/`):

```toml
format_version = 1

[target]
app = "biotech"                       # biotech | ai-engineer; equals application_id
application_id = "biotech"
project_label = "biotech-research-ingestion"
project_ref = "<verified project ref>"
installation_id = "<UUIDv7 allocated once; never regenerate>"
environment = "disposable"            # disposable | development | staging | production
database_host = "<direct or session host>"
database_port = 5432
database_name = "postgres"
database_user = "<approved migration login>"
database_url_env = "MC_BIOTECH_MIGRATION_DATABASE_URL"   # NAME only

[approval]
approved_by = "<approver reference>"
identity_evidence = "<identity evidence reference>"
```

Every field is required; unknown fields and any `REPLACE`/placeholder value are
refused. The DSN in `database_url_env` must match host, port, user and database
exactly. Optional `database_session_user` covers Supabase session-pooler logins
(`postgres.<project_ref>` connects as session user `postgres`); it is accepted only when
the login is exactly `<session_user>.<project_ref>` of the same target. Both tracked
live manifests were approved and filled on 2026-10-03 (Biotech direct route, Blue Ocean
session pooler); credentials stay in the existing `.env` sources and are referenced by
variable name only.

### Release, lock, plan, apply, verify

```powershell
$pkg = 'packages\mission-control-db-contract'
# Build manifest + generated contract on a loopback PostgreSQL 17 scratch database
uv run --no-sync mission-db release-build --component-root $pkg\component --admin-dsn-env MC_RELEASE_ADMIN_DSN
uv run --no-sync mission-db lock --app biotech --component-root $pkg\component --deployments-root deployments
uv run --no-sync mission-db inspect --deployment-dir deployments\biotech
uv run --no-sync mission-db plan --deployment-dir deployments\biotech --out plan.json
uv run --no-sync mission-db apply --deployment-dir deployments\biotech `
  --expected-plan-digest <plan_digest from plan.json> --confirm-target <project_ref>:<installation_id>
uv run --no-sync mission-db verify --deployment-dir deployments\biotech
```

`plan` is read-only and bound to the before-fingerprint, identity, receipts and
release digests. `apply` re-observes under the migration advisory lock, rejects a
stale plan, and commits migrations, `component_release` receipts, the
`application_installation` identity row and the `release_attestation` atomically; a
repeat is a verified no-op. Unknown history, checksum drift, an owned schema without
receipts or a `mission_control_*` role with LOGIN/SUPERUSER/BYPASSRLS/CREATEROLE/CREATEDB
holds the install. `vector` must already exist in schema `extensions`.

### Runtime phase (LangGraph saver/store)

```powershell
uv run --no-sync mission-db runtime-plan --deployment-dir deployments\biotech `
  --descriptor $pkg\runtime\descriptor.json
uv run --no-sync mission-db runtime-apply --deployment-dir deployments\biotech `
  --descriptor $pkg\runtime\descriptor.json --confirm-target <project_ref>:<installation_id> `
  --receipt-out runtime-receipts.json
```

This separate phase provisions the pinned `langgraph-checkpoint-postgres` saver/store
in the private schema `mission_control_runtime` (role `mission_control_checkpointer`)
on a dedicated session, including `CREATE INDEX CONCURRENTLY`; it records per-step
progress. After an interruption inspect with `runtime-plan` (invalid indexes are
reported) instead of blindly retrying. Workers never call vendor `setup()`.

### Seeds

```powershell
uv run --no-sync mission-db seed-plan --deployment-dir deployments\biotech `
  --bundle $pkg\seeds\common --bundle $pkg\seeds\biotech
uv run --no-sync mission-db seed-apply --deployment-dir deployments\biotech `
  --bundle $pkg\seeds\common --bundle $pkg\seeds\biotech --confirm-target <project_ref>:<installation_id>
```

Seeds run after `verify`, one transaction per dependency-closed bundle, recorded in
`installation_seed_receipt`. The same digest replays; changed bytes under an existing
key/version conflict. Bundles and exclusions are listed in
`packages/mission-control-db-contract/seeds/README.md`; `seeds/qualification/` is
opt-in for disposable or approved qualification tenants only. No tenant, actor or
grant mapping for a real user is seeded until the owner approves it.

### Storage reconciliation and protected-data evidence

Buckets `capability-bundles` and `mission-artifacts` are private, app-local stores;
`knowledge-artifacts` stays with its domain owner. Bucket and `storage.objects`
policy differences are reconciled separately from the SQL transaction and must be
listed in an operator-approved allowed-differences manifest.

```powershell
uv run --no-sync mission-db snapshot --deployment-dir deployments\biotech --out before.json
uv run --no-sync mission-db compare --before before.json --after after.json --allowed allowed-differences.json
uv run --no-sync mission-db qualify --deployment-dir deployments\biotech --phase before `
  --out-dir docs\qualification\two-project\<target>\<run-id>
```

Snapshots publish hashes only (catalog inventory, per-relation row digests, sequence
states). On a live, concurrently written project equality is meaningful only inside a
quiescent or attributable window.

### Two-disposable driver

`scripts/qualify_two_disposables.py` runs the whole CLI sequence (qualify before,
plan, apply, replay, verify, runtime-plan/apply + replay, seed-plan/apply + replay,
qualify after) against two loopback PostgreSQL 17 + pgvector clusters with different
protected domain fixtures, and writes `docs/qualification/two-project/<target>/<run-id>/`
plus a cross-target `comparison-<run-id>.json`:

```powershell
uv run --no-sync python scripts/qualify_two_disposables.py `
  --biotech-admin-dsn-env MC_PROOF_BIO_ADMIN --ai-engineer-admin-dsn-env MC_PROOF_AIE_ADMIN `
  --run-id <run-id>
```

The independent reviewer suite is `tests/qualification/two_project/` (requires
`MISSION_CONTROL_TEST_ADMIN_DSN` naming a loopback disposable server; it fails, never
skips, without it).

### Restricted pool roles

Login identities are granted membership in exactly one NOLOGIN capability role, only in
disposable tests or after the access-expansion approval:

| Capability role | Use | Must not |
| --- | --- | --- |
| `mission_control_runtime` | API/worker business reads/writes (`binding.database_secret_ref`) | own objects, BYPASSRLS, inherit family/catalog roles |
| `mission_control_family_writer` | atomic family admission (`family_writer_secret_ref`) | be held by the runtime login |
| `mission_control_catalog_writer` | catalog admission and projection writes | tenant business writes |
| `mission_control_outbox_worker` | outbox/projection claims and acknowledgement | mission state mutation |
| `mission_control_readonly` | inspection | any write |
| `mission_control_checkpointer` | `mission_control_runtime` schema only (`LANGGRAPH_CHECKPOINT_DATABASE_DIRECT`) | any `mission_control` access |

Startup asserts NOSUPERUSER, NOBYPASSRLS, the expected single membership and no
ownership of component objects. The installation identity row is written by
`mission-db apply`; the transitional `python -m mission_control.bootstrap.installation`
registration command was removed with the legacy schema runner.

## Start and use

Populate the configured DSN environment reference through the normal secret mechanism.
Then:

```powershell
$env:MISSION_CONTROL_DEPLOYMENT_FILE = 'C:\operator\deployment.json'
uv run uvicorn mission_control.bootstrap.api:create_app --factory --host 127.0.0.1 --port 8000
```

`/health/live` checks process availability. `/health/ready` reports checked local
readiness and whether launch is configured. OpenAPI is at `/docs`.

Set `MISSION_CONTROL_APPLICATION_ID`, `MISSION_CONTROL_URL`, and an issuer-signed
`MISSION_CONTROL_TOKEN` in the CLI process. Then use `missionctl run inspect RUN_ID`.
No placeholder token or development authentication bypass is provided.

For execution, configure `ApplicationDeployment.temporal` with the exact namespace,
address, and root/StageGraph/GoalDirected queues; required Search Attributes are
verified before use. Start the corresponding worker separately. Without Temporal,
inspection and durable command admission work, but launch fails explicitly and
boundary delivery stays pending.

Supply accepted admission policies, extension validators, payload storage, and fork
patch policies through the `create_application(..., runtime_options={...})` Python
composition hook. Default empty policy/extension registries fail closed for unknown
executable contracts; the API does not invent catalog definitions or policy grants.
The standard factory therefore provides a configured inspection/control deployment;
an execution deployment must install its accepted runtime options and definitions.

The standard ASGI factory can load those options from one explicitly installed,
operator-reviewed module using `MISSION_CONTROL_RUNTIME_OPTIONS_FACTORY=package.module:build`.
The callable receives the validated `MissionDeployment` and must return exactly one
typed `RuntimeOptions` for every configured application. Unknown, missing, or untyped
entries fail startup. Requests, JWT claims, catalog content and skill bundles cannot
select this module. For example, the installed module can expose:

```python
def build(deployment):
    return {
        "biotech": RuntimeOptions(
            admission_policies=accepted_admission_policies(),
            extensions=accepted_extension_registry(),
            payload_store=configured_payload_store(),
            family_admissions=accepted_family_admission_registry(),
            fork_policies=accepted_fork_patch_registry(),
        )
    }
```

These factory functions belong to the reviewed deployment package and must register
the exact admitted contracts. The deterministic acceptance proof demonstrates this
with the existing technical admission policies and real StageGraph/GoalDirected
family mutation registries, without substituting model prompts for policy.

## Worker startup

Use the same deployment file and reviewed runtime-options factory for the API and
worker. Set `ApplicationDeployment.family_writer_secret_ref` and configure its
Temporal namespace and queues. The worker selects exactly one application; queues
shared by different configured applications are rejected.

Provide these environment settings through the operator configuration/secret manager:

- `MISSION_CONTROL_DEPLOYMENT_FILE`, `MISSION_CONTROL_WORKER_APPLICATION_ID`, and
  `MISSION_CONTROL_WORKER_BINDING_DIGEST` (the exact sealed binding digest).
- `COORDINATOR_LAUNCH_ENABLED=true`.
- `LANGGRAPH_CHECKPOINT_DATABASE_DIRECT`: a dedicated login in
  `mission_control_checkpointer` to the same selected installation database, distinct
  from runtime/family authority.
- `LANGGRAPH_CHECKPOINT_SCHEMA=mission_control_runtime` (the default; `public`, the
  business schemas and the retired legacy schemas are refused).
- `LANGGRAPH_CHECKPOINT_SETUP=false`: schema setup is never a worker startup action.
- `MISSION_CONTROL_RUNTIME_OPTIONS_FACTORY`, when using an accepted operator
  registry package; both processes resolve the same typed per-application options.

Provision the pinned official LangGraph PostgresSaver and PostgresStore migrations
with `mission-db runtime-apply` before starting the worker. Readiness checks
their actual migration versions and rejects owner/superuser/BYPASSRLS credentials,
business authority privileges, missing schemas and mismatched installation data.
Select model/backend credentials only for the providers this deployment uses.

```powershell
uv run python -m mission_control.bootstrap.worker
```

The worker uses a Psycopg-compatible selector event loop on Windows. The configured
entrypoint has been exercised with real PostgreSQL and Temporal, including root,
both family and linked-run pollers, without creating schema during startup.

## Fast-track entrypoints and configuration

This section lists names only. Values come from the process environment, `mission-control/.env` or
the operator's secret manager; never write a value into a manifest, seed, deployment file under
version control, Linear comment or document. Settings names are case-insensitive environment
variables (`bootstrap/settings.py`).

**API process** (`make server`):

| Name | Effect |
| --- | --- |
| `MISSION_CONTROL_SUBSCRIPTION_RELAY` | `1` starts the subscription relay (webhook and MCP delivery) inside the API; SSE `events watch` needs no relay |
| `CURSOR_API_KEY`, `MISSION_CONTROL_ALLOW_UNQUALIFIED_LANES` | only so `lane list` and `lane describe` show the Cursor profiles and mirror the worker's admission policy |
| `CAPABILITY_EMBEDDING_PROFILE` | unset: capability search is lexical-only; `embedding.openai.text-embedding-3-small` embeds each query (needs `OPENAI_API_KEY`, a small paid effect) |
| `MISSION_CONTROL_CATALOG_SCOPE` | installation catalog scope used by the projection scripts (`mc/<installation>/<app>/catalog`) |
| `CAPABILITY_BUNDLE_BACKEND`, `CAPABILITY_BUNDLE_NAMESPACE`, `CAPABILITY_BUNDLE_LOCAL_ROOT`, `CAPABILITY_BUNDLE_PUBLISHER_TOKEN`, `CAPABILITY_BUNDLE_READER_TOKEN` | bundle custody backend (`local` or `supabase`) and its publisher or reader credentials, never the service key; the configured API does not yet expose publish routes |

**Worker process** (`make worker`; Mission 3 needs WSL or Linux because `cursor_local` cannot launch
on the Windows selector event loop):

| Name | Effect |
| --- | --- |
| `CURSOR_API_KEY` | registers the `cursor_local` and `cursor_cloud` lane profiles |
| `MISSION_CONTROL_ALLOW_UNQUALIFIED_LANES` | `true` admits unqualified lanes for a local proof; both Cursor profiles are unqualified, and the refusal names this flag |
| `MISSION_CONTROL_LANE_SEGMENT_LOOP` | default `false`; `true` moves Deep Agents units onto `lane.turn` (Cursor units always use it) |
| `MISSION_CONTROL_FRAMES_EXPIRE_SCHEDULE` | default `true`: serve `mc.frames_expire.v1` on `<base>-maintenance` and keep the daily retention Schedule |
| `CURSOR_LEASE_ROOT`, `CURSOR_LOCAL_REPOSITORY` | where `cursor_local` leases git worktrees, and the worker-local checkout used when a binding names no repository |
| `MISSION_CONTROL_HOOK_CALLBACK_PORT` | loopback port (default 47555) of the Kernel Hook callback listener |
| `CAPABILITY_PINS_PATH`, `WEB_RESEARCH_AGENT_BROWSER_NODE` | the worker's pin file of launchable components and the node binary for the pinned `agent-browser` tool |
| `TEMPORAL_ADDRESS`, `TEMPORAL_NAMESPACE`, `TEMPORAL_TARGET`, `TEMPORAL_CLOUD_API_KEY` | `TEMPORAL_TARGET` is `local` by default (the `make temporal-up` server); `cloud` only on purpose |
| `TEMPORAL_WORKER_VERSIONING`, `TEMPORAL_DEPLOYMENT_NAME`, `TEMPORAL_BUILD_ID`, `TEMPORAL_PROMOTE_ON_START` | Worker Deployment versioning (`temporalio` 1.34): default on, deployment `mission-control`, build id defaults to the package version, promote on start; production operators may set promotion off and promote deliberately |
| `OPENAI_API_KEY`, `TAVILY_API_KEY`, `FIRECRAWL_API_KEY` | Mission 1 model and retrieval secrets (paid) |
| `NCBI_API_KEY`, `EDGAR_IDENTITY` | secret references for the PubMed and EDGAR MCP seeds; not set today (keyless PubMed works at a low rate; EDGAR is unused by the three missions) |

Capability bundle tokens, webhook secrets and MCP secrets are referenced by name
(`environment:<NAME>` for a subscription webhook) and resolved at the point of use.

**Client** (`missionctl`): `MISSION_CONTROL_URL`, `MISSION_CONTROL_APPLICATION_ID`,
`MISSION_CONTROL_TOKEN` (an issuer-signed token; load it from a file without echoing it). New
groups: `mission compile|submit|start|schema`, `chain inspect`, `subscribe`, `events watch`,
`lane list|describe`, `run transcript|frames|list|search|checkpoint|fork`,
`command queue|inject|cancel`, `catalog pin|render|publish`
([interfaces](knowledge/interfaces.md)). `mission start` answers `409 start_unavailable` in the
configured deployment until a production launch input author is composed (blocker B1).

**Make targets and scripts:**

| Command | Purpose |
| --- | --- |
| `make lane-qualify PROFILE=cursor_local\|cursor_cloud` | offline fixture, describe-honesty and replay suites; `LIVE=1` adds the paid drill (`CURSOR_API_KEY`, finite `MC_PAID_BUDGET_USD`, for cloud `MC_CURSOR_CLOUD_REPO`; optional `MC_LANE_DRILL_APPROVAL_URL`); see [lane qualification](qualification/lanes/README.md) |
| `make skills-check` / `make skills-manifest` | fail on, or rewrite, drifted `skills/*/manifest.json` digests (LF-canonical) |
| `make seeds-validate` | fail if a seed Capability Pin does not parse or a tools/list digest drifted |
| `make temporal-up`, `make temporal-search-attributes` | start the local Temporal stack and register the Search Attributes (`mc_mission_id`, `mc_run_id`, `mc_lane`, `mc_phase`, `ForkedFromRunId` among them) |
| `scripts/rebuild_capability_search_projection.py --tenant <scope> --lexical-only` | build the capability search projection without a paid embedding batch |
| `scripts/fast_track_dry_run.py [--with-fixture-rows]` | unpaid dry run on scratch installs (`MISSION_CONTROL_TEST_ADMIN_DSN` names the loopback admin DSN; refuses while `CAPABILITY_EMBEDDING_PROFILE` is set) |

Installing release 1.1.0 uses the `mission-db` commands above against a local target outside
version control for fixtures (never the live `deployments/<app>` targets), after `vector` and
`pg_trgm` exist in schema `extensions`. The exact local-stack procedure, the mission commands, the
paid-unit budget policy, how to stop safely and the open owner decisions are in the
[owner fixture runbook](specs/fast-track-2026-10/OWNER-FIXTURE-RUNBOOK.md).

## Operational capability catalog

The configured API includes the application catalog at
`/v1/applications/{application_id}/catalog`: GET `definitions` and POST `resolve`,
`search`, `discover`, `inspect` and `components/search`; the fast-track surface adds `pin`,
`render`, `publish:prepare`/`publish:complete` and `GET pins/{pin}`. `missionctl catalog`
provides corresponding `list`, `resolve`, `search`, `discover`, `inspect`, `components`, `pin`,
`render` and `publish` commands. Requests use the same trusted installation/tenant identity;
permissions remain explicit (`catalog:read`, `catalog:discover`, `catalog:inspect`).

Default composition supports list/resolve over the admitted application catalog
without additional provider credentials. Search requires a trusted
`RuntimeOptions.catalog_factory` supplying an embedding port. External discovery,
inspection and plugin/component search require their explicitly configured adapters
in the same factory. Missing adapters fail explicitly; runtime does not fabricate
credentials or choose a provider fallback.

Discovery candidates remain quarantined and inspection does not promote or execute
them. Plugin filtering preserves existing trust, host, OS, architecture and required
capability constraints. Marketplace installation is not provided by these commands.

## Agent Server

Use the sole registration file `agent_server/langgraph.json`:

```powershell
uv run langgraph dev --config agent_server/langgraph.json --host 127.0.0.1 --port 2024 --no-browser
```

`MISSION_CONTROL_AGENT_SERVER_PROFILE` selects `runtime` (default, bounded child),
`qualification` (three diagnostic graphs), or `qualification_n1` (N1 only).
Profile-denied run creation returns 403; a registered diagnostic graph is not
implicitly available to the runtime profile. These graphs never schedule missions.

Supply secrets through the operator environment; tracked `agent_server/runtime.env`
contains references only. Runtime subordinate authentication uses the exact
`BELLABS_ASYNC_SUBAGENT_SERVER_TOKEN` signing-secret reference and scoped claims,
or the configured existing JWT verifier. Qualification uses
`BELL_LABS_AGENT_AUTH_ISSUER`, `BELL_LABS_AGENT_AUTH_PUBLIC_KEY_B64` (or PEM
`BELL_LABS_AGENT_AUTH_PUBLIC_KEY`) and its audience/algorithm settings.

An explicit `MISSION_CONTROL_AGENT_HOSTING_FACTORY=installed.module:callable`
returns the typed `HostedGraph` and exact bounded child definition. This is trusted
operator code, never selected by a request or downloaded bundle. The default
production graph resolves its configured model only when invoked; local
qualification uses deterministic factories and does not exercise a paid model.

`MISSION_CONTROL_AGENT_AUTHORITY_ENABLED=1` opts into the separately configured
PostgreSQL authority bootstrap. It defaults to disabled: native bounded graph
hosting does not acquire ambient application database authority.

The development command is a local service check, not proof of production hosted
persistence or restart durability. See implementation status for exact evidence.

**Topology blocker.** The pinned Agent Server (`langgraph-api==0.12.0`) has no schema
setting, applies its own unqualified migrations at startup (including `checkpoints` and
`store` tables that collide with the standalone saver/store) and needs extension and
`CREATE` privileges. It therefore cannot use a private schema of the business database,
and `mission_control_agent_server` is not created by any release. Production native
persistence needs an owner topology decision (preferred: a separate database per
installation with its own owner/login and no access to the business database) and a
license decision. See
`packages/mission-control-db-contract/runtime/AGENT_SERVER_TOPOLOGY.md`.

## Evidence

`tests/integration/postgres/test_mission_control_bootstrap_postgres.py` creates its
own disposable database and restricted login, runs actual migrations, registers and
replays the identity seed, starts the real factory, and reads through the API with
a signed JWT. It verifies missing authentication, cross-application rejection,
actual database access, and shutdown readiness. Authentication tests additionally
cover wrong issuer/audience, expiry, unsigned/tampered tokens, tenant denial, and
operator-only grants. These local proofs do not imply live Supabase deployment.

`tests/acceptance/mission_control/test_authenticated_scoped_runtime.py` also passes
with the real configured JWT verifier, restricted PostgreSQL repositories, cached
local Temporal server, and production worker composition. The scoped API admits
and launches StageGraph and GoalDirected runs through `mc.mission_run.v1` and
`mc.operation.v1`. The proof completes two StageGraph operations and four
GoalDirected operations, applies pause/resume/wait satisfaction with durable command
replay, and replays the recorded mission and operation histories. Its 30 model
events use deterministic local models; it does not call a paid provider. It requires
`MISSION_CONTROL_E2E_POSTGRES_DSN` pointing to the authorized disposable local
PostgreSQL owner connection and `MISSION_CONTROL_TEMPORAL_CLI` pointing to the
installed Temporal CLI. Run it with `pytest
tests/acceptance/mission_control/test_authenticated_scoped_runtime.py -q -s`.
These acceptance proofs are being re-run on the common component; their current
results are recorded only in the implementation status "Final qualification results"
section. Local disposable proofs never imply a live Supabase installation.
