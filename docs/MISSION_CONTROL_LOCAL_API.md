# Configured Mission Control API

The configured entrypoint is `mission_control.bootstrap.api:create_app`. It serves the scoped
API and uses real asymmetric JWT verification, restricted PostgreSQL services, and
the Temporal submitter when configured. Startup performs no migration or seed writes.

This release supports explicit `transitional_local` qualification using the existing
`belllabs_control` schema. Its readiness response always says `production_ready: false`.
`production_common` refuses startup because the separately released common component
and its adapters are not available. Local PostgreSQL must use a loopback endpoint.

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
    installation_id=UUID("11111111-1111-4111-8111-111111111111"),
    binding_version="1",
    supabase_project_ref="biotech-research-ingestion",
    database_secret_ref="MC_BIOTECH_RUNTIME_DSN",
    accepted_issuers={"https://YOUR_PROJECT.supabase.co/auth/v1"},
    accepted_audiences={"authenticated"},
    required_component_version="transitional-local-v1",
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
    storage_mode="transitional_local",
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

## Prepare a disposable local database

Apply the repository's versioned migrations using the existing explicit migration
runner and a schema-owner connection. The API runtime connection must be a separate,
non-superuser/non-BYPASSRLS login with `belllabs_control_runtime` membership and no
family writer membership. Family writes require a separately configured login in
`belllabs_family_repository_writer` and no control-runtime membership.

After migration 0034, explicitly register the installation identity once:

```powershell
uv run python -m mission_control.bootstrap.installation `
  --binding-file C:\operator\binding.json `
  --expected-database mission_control_parity `
  --owner-dsn-env MC_SCHEMA_OWNER_DSN
```

Registration verifies the exact database name, accepts only loopback connections,
and refuses to replace a different installation identity. Matching repeats are safe.
This command is separate from API startup. Startup reads the persisted identity,
checks migration receipts and restricted roles, and enables each binding only after
those checks pass. No configuration value is substituted for a missing database row.

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
- `LANGGRAPH_CHECKPOINT_DATABASE_DIRECT`: a dedicated recovery-role connection to
  the same selected installation database, distinct from runtime/family authority.
- `LANGGRAPH_CHECKPOINT_SCHEMA`: the separately provisioned saver/store schema.
- `LANGGRAPH_CHECKPOINT_SETUP=false`: schema setup is never a worker startup action.
- `MISSION_CONTROL_RUNTIME_OPTIONS_FACTORY`, when using an accepted operator
  registry package; both processes resolve the same typed per-application options.

Provision the pinned official LangGraph PostgresSaver and PostgresStore migrations
as an explicit local operator step before starting the worker. Readiness checks
their actual migration versions and rejects owner/superuser/BYPASSRLS credentials,
business authority privileges, missing schemas and mismatched installation data.
Select model/backend credentials only for the providers this deployment uses.

```powershell
uv run python -m mission_control.bootstrap.worker
```

The worker uses a Psycopg-compatible selector event loop on Windows. The configured
entrypoint has been exercised with real PostgreSQL and Temporal, including root,
both family and linked-run pollers, without creating schema during startup.

## Operational capability catalog

The configured API includes the application catalog at
`/v1/applications/{application_id}/catalog`: GET `definitions` and POST `resolve`,
`search`, `discover`, `inspect` and `components/search`. `missionctl catalog`
provides corresponding `list`, `resolve`, `search`, `discover`, `inspect` and
`components` commands. Requests use the same trusted installation/tenant identity;
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
This qualifies the explicit transitional local adapters, not the unavailable
production common-schema adapters.
