---
type: Operator Guide
title: "Owner fixture runbook: running Missions 1 to 3 on the local real stack"
description: "What the owner needs to run the three fast-track mission fixtures (I1, I2, I3): what is ready and what still blocks a live run, the local real stack (PostgreSQL 17, Temporal, release 1.1.0 and seeds through mission-db, API and worker), the exact missionctl commands per mission, the acceptance criteria to check, the paid units and budget policy, how to stop safely, and the open owner decisions. Written by the readiness pass of 2026-10-08 on branch ft/readiness."
tags: [mission-control, fast-track, runbook, operations, owner]
---

# Owner fixture runbook (Missions 1, 2 and 3)

This runbook is for the owner's acceptance runs of tickets I1 (OVE-59), I2 (OVE-60) and I3
(OVE-61), using the manifests under [missions/](missions/). It uses environment variable
names only. Values come from `mission-control/.env` and your shell. Never paste a value into
a manifest, a seed, a deployment file under version control, a Linear comment or this
document.

Read section 1 before you spend anything.

## 0. Readiness at a glance (2026-10-08, branch `ft/readiness`)

| Step | Status | Evidence |
| --- | --- | --- |
| Component release 1.1.0 (migrations 0025 to 0030) built, locks for both apps | ready | `component/manifest.json`, `deployments/*/release.lock.json` |
| Upgrade of an installed 1.0.0 to 1.1.0 (plan, apply, replay, verify) | proven on a scratch database | readiness commit `95aea03`; section 9 |
| Fresh 1.1.0 install with all seeds (`common`, app, `qualification`) | proven on scratch databases | `scripts/fast_track_dry_run.py` |
| API startup on 1.1.0 (`/health/ready`: `production_ready: true`, `component_versions: ["1.1.0"]`) | proven | dry run |
| `missionctl mission compile` for all three manifests through the real API | proven. It blocks on the capabilities the production seeds do not carry (B2). | dry run, production seeds |
| `mission compile` and `mission submit` with the FT-E2 stand-in rows published | proven: all three compile with no blockers, submit admits the runs (Mission 2 creates the chain with two `armed` links) | dry run `--with-fixture-rows` |
| `mission start` of any of the three missions | **blocked** (B1) | `start_unavailable` |
| Cursor lane qualification drill (`make lane-qualify PROFILE=... LIVE=1`) | ready, owner-run, paid | [docs/qualification/lanes/README.md](../../qualification/lanes/README.md) |

**Bottom line:** today you can install, seed, compile, submit, inspect, subscribe and watch
on the local real stack. You cannot yet start a live run of any of the three missions.
B1 (no launch-input author) blocks every start. Missions 2 and 3 also need B5 and B6.
Running the Cursor lane drill now is useful and is the only paid step that is ready.

## 1. What blocks a live run today

| # | Blocker | Affects | What closes it |
| --- | --- | --- | --- |
| B1 | **No production author of a manifest run's semantic input binding.** `mission start RUN` needs the lane execution templates for every lowered stage or Goal Loop role: model component, prompt segments, MCP servers, skills, capability grant, workspace contract, output schema, plus the Cursor binding for Cursor lanes. Only the test author `StagedLaunchInputs` (`tests/fixtures/manifest_runtime.py`, deterministic models) exists. The API composes `launch_inputs=None`, so `mission start` answers `409 start_unavailable`. The chain relay that starts Mission 2's `ingestion` needs the same author through `ChainLaunchInputPort`, so it is not composed either. No interface persists templates, so `mission start --request-file` is not a workaround. | I1, I2, I3 | A follow-up ticket: a production `LaunchInputPort` and `ChainLaunchInputPort` that author templates from the compiled manifest. It needs owner decisions first. Which pinned model does `frontier.default` (and `frontier.long_context`, `cursor.default`) map to? Which sandbox do `research.standard` and `ingestion.standard` map to? Which secret refs may each lane use? |
| B2 | **Production seeds lack many capabilities the manifests search for.** With the production seeds only, compile blocks on these searches. Mission 1: `skill.biotech-literature-review`, context bundle `biotech schema context muscle aging`, the Biotech `kg_ingest` tool, hook `citation presence check`, assessment `evidence coverage`. Mission 2 adds a cloud-qualified PubMed (`mcp.pubmed` is unqualified on `cursor_cloud`), `literature verifier subagent` and `claim schema validation`. Mission 3: executors `test_run` and `git_snapshot`, subagent `code verifier`, hook `shell command policy`, assessment `code change verification`. | I1, I2, I3 | Publish real definitions for them (`missionctl catalog publish` for skills, hooks and subagent profiles, or new seed versions). The FT-E2 stand-ins in `tests/fixtures/catalog/fast_track_seeds.json` are test rows, not reviewed capabilities. The Biotech `kg_ingest` capability must be the real app-owned one, otherwise Mission 1 ends with a visible blocker, as the spec requires. |
| B3 | **The worker can launch only components in its pin file.** `infra/capability-pins/research-capabilities.json` pins one OpenAI model (`model.wp-cp-040`), `mcp.tavily`, `mcp.firecrawl` and the `agent-browser` skill. PubMed, the Biotech KG MCP server and the mission skills are not pinned. The worker refuses any other launch. | I1, I2 | Extend the pin file, or `DeploymentCapabilityComponents` in a reviewed runtime-options factory, together with B1. |
| B4 | **Worker startup fails on the drifted `agent-browser` skill.** The pin above reads `../.agents/skills/agent-browser` (workspace `.agents/`). Its bytes changed on 2026-10-08, so `CapabilityPinError: Skill bundle agent-browser differs from its pinned bundle digest` stops `make worker`. The same cause produces the known `test_capability_pins_and_runtime_ports` failure. | all | Restore that directory to the pinned bytes (bundle digest `sha256:30722859...`), or re-pin it with `scripts/pin_research_capabilities.py` in a reviewed change. |
| B5 | **Cursor lanes are unqualified.** Admission refuses `cursor_local` and `cursor_cloud`. The refusal names the remedy. | I2, I3 | Run the qualification drill (section 2.7). For a local proof only, set `MISSION_CONTROL_ALLOW_UNQUALIFIED_LANES=true` on the worker (and on the API, for `lane describe`). |
| B6 | **`cursor_local` needs a Proactor or Unix event loop.** The worker runs a SelectorEventLoop on Windows (psycopg), so the SDK bridge cannot launch there. | I3 | Run the worker under WSL or Linux. Mission 3's `repo.path` is a Windows path (`C:\Users\Pinda\Proyectos\Biotech\mission-control`), so a WSL worker needs a `/mnt/c/...` path or a clone (owner decision, section 8). |
| B7 | **Subscription event names in the manifests are not emitted.** The manifests subscribe to `activation.completed`, `human_task.opened` and `run.completed`. Mission 3 also uses `command.completed`, which the kernel does emit. The kernel stream names the first three `activation.lifecycle_changed`, `workflow_run.set_wait` and `workflow_run.terminalize`. Filters match exact names, so those subscriptions register but never deliver. | I1, I2, I3 | Owner decision: emit the SPEC-06 names from the kernel, or add an alias table to subscription filters. Until then, use `events watch` (all events) or subscribe with kernel names (section 2.6). |

Not blocking, but limited (known gaps):

- `request_continuation` (in Mission 1's `commands_allowed`) is recorded as
  `session.continuation_requested`, but no worker seals or transfers. The B4 continuation
  activities and the Cursor hydrators exist, but no family workflow calls them yet. Do not
  rely on it.
- `run transcript --full` answers `501` because no production `ArtifactBodyReader` is
  wired. The default transcript (digests and excerpts) works.
- Fork restore works for `cursor_local` (readiness commit `37d7deb` records the frozen
  `cursor-snapshot:` ref). It does not work for `cursor_cloud`: the lane expects a
  `branch:<branch>@<sha>` ref that nothing records yet.
- Webhook and MCP subscriptions need the relay: set `MISSION_CONTROL_SUBSCRIPTION_RELAY=1`
  on the API. SSE (`events watch`) needs no relay.
- `nested_goal_loop_lowered_as_stage`: Mission 1's `collect` runs as one Stage Graph stage,
  not a nested Goal Loop.
- `MISSION_CONTROL_LANE_SEGMENT_LOOP` can stay `false`. Cursor units always run through the
  `lane.turn` segment loop whatever its value (`OperationWorkflowRequest.segment_driven`).
  The flag only moves Deep Agents units off `operation.execute`.

## 2. The local real stack (all missions)

### 2.1 Servers

| Need | How | Note |
| --- | --- | --- |
| PostgreSQL **17** + pgvector, loopback | the disposable container `mc-ft-disposable-pg17` on `127.0.0.1:55433` (`docker start mc-ft-disposable-pg17` after a reboot), or any `pgvector/pgvector:pg17` | `make infra-up`'s `application-postgres` is PostgreSQL 16; release 1.1.0 refuses a non-17 server. Never `docker volume prune`. |
| Temporal 1.31 + UI + Search Attributes | `make temporal-up` (`127.0.0.1:7233`, UI `:8080`) | local by default; `TEMPORAL_TARGET=cloud` only on purpose |
| API | `make server` (`127.0.0.1:8000`) | section 2.5 |
| Worker | `make worker` | Mission 3: under WSL or Linux (B6) |
| Agent Server | not needed for these missions | |

### 2.2 Install release 1.1.0 and the seeds on a local target

`deployments/biotech` and `deployments/ai-engineer` describe the **live Supabase** targets.
Never run `apply`, `seed-apply` or `runtime-apply` with them for a fixture run. Create a
local target directory outside version control, for example
`.scratch/local-stack/deployments/<app>/target.toml` (`.scratch/` is ignored):

```toml
format_version = 1

[target]
app = "biotech"                      # or "ai-engineer" (Mission 3)
application_id = "biotech"
project_label = "local-real-stack"
project_ref = "mcdisposablebiotech"  # a local identity, never a live project ref
installation_id = "0192a4f0-0000-7000-8000-00000000b10e"
environment = "disposable"
database_host = "127.0.0.1"
database_port = 55433
database_name = "mc_local_biotech"   # create it first (below)
database_user = "postgres"
database_url_env = "MC_LOCAL_BIOTECH_MIGRATION_URL"   # NAME only; your shell holds the DSN

[approval]
approved_by = "owner"
identity_evidence = "local loopback fixture database"
```

For `ai-engineer`, use `project_ref = "mcdisposableaieng"`,
`installation_id = "0192a4f0-0000-7000-8000-0000000a1e00"` and its own database.

```powershell
# 1. Database and extensions (as the server admin; extensions must precede the release).
#    CREATE DATABASE mc_local_biotech;  then in it:
#    CREATE SCHEMA IF NOT EXISTS extensions;
#    CREATE EXTENSION IF NOT EXISTS vector SCHEMA extensions;
#    CREATE EXTENSION IF NOT EXISTS pg_trgm SCHEMA extensions;
#    GRANT USAGE ON SCHEMA extensions TO PUBLIC;
$pkg = 'packages\mission-control-db-contract'
$dep = '.scratch\local-stack\deployments'
$v = '--reader-version','mission-control-runtime/1','--writer-version','mission-control-runtime/1'
uv run --no-sync mission-db lock --app biotech --component-root $pkg\component --deployments-root $dep
uv run --no-sync mission-db plan --deployment-dir $dep\biotech @v --out $dep\biotech-plan.json
uv run --no-sync mission-db apply --deployment-dir $dep\biotech @v `
  --confirm-target mcdisposablebiotech:0192a4f0-0000-7000-8000-00000000b10e `
  --expected-plan-digest <plan_digest from the plan file>
uv run --no-sync mission-db verify --deployment-dir $dep\biotech @v
# 2. Seeds: common + app + the OPT-IN qualification bundle (its synthetic tenant is the
#    tenant a local run acts in; no real tenant or grant is seeded).
uv run --no-sync mission-db seed-apply --deployment-dir $dep\biotech @v `
  --bundle $pkg\seeds\common --bundle $pkg\seeds\biotech --bundle $pkg\seeds\qualification `
  --confirm-target mcdisposablebiotech:0192a4f0-0000-7000-8000-00000000b10e
# 3. Runtime phase (LangGraph saver and store in schema mission_control_runtime).
uv run --no-sync mission-db runtime-apply --deployment-dir $dep\biotech @v `
  --descriptor $pkg\runtime\descriptor.json `
  --confirm-target mcdisposablebiotech:0192a4f0-0000-7000-8000-00000000b10e `
  --receipt-out $dep\biotech-runtime-receipts.json
```

`mc.storage.capability-bundles@1.0.0` reports `blocked` on plain PostgreSQL because there is
no Supabase Storage. That is expected locally. Re-running `apply` and `seed-apply` is a
no-op (`noop` and `replay`).

### 2.3 Logins, tenant and catalog projection

Create one LOGIN per capability role (cluster-global names; pick your own passwords; keep
them in your shell, not in files under version control):

```sql
CREATE ROLE mc_local_runtime LOGIN NOSUPERUSER NOBYPASSRLS INHERIT PASSWORD '<choose>' IN ROLE mission_control_runtime;
CREATE ROLE mc_local_family  LOGIN NOSUPERUSER NOBYPASSRLS INHERIT PASSWORD '<choose>' IN ROLE mission_control_family_writer;
CREATE ROLE mc_local_ckpt    LOGIN NOSUPERUSER NOBYPASSRLS INHERIT PASSWORD '<choose>' IN ROLE mission_control_checkpointer;
GRANT CONNECT ON DATABASE mc_local_biotech TO mc_local_runtime, mc_local_family, mc_local_ckpt;
SELECT tenant_id FROM mission_control.tenant;   -- the qualification tenant: your grant's tenant
```

Build the searchable catalog without a paid embedding batch. The projection tooling runs on
the runtime role. Set `DATABASE_DIRECT` in this shell so the script never falls back to the
checkout's `.env` (which points at Supabase):

```powershell
$env:DATABASE_DIRECT = '<runtime login DSN of the local database>'
$env:MISSION_CONTROL_CATALOG_SCOPE = 'mc/0192a4f0-0000-7000-8000-00000000b10e/biotech/catalog'
uv run --no-sync python scripts/rebuild_capability_search_projection.py `
  --tenant $env:MISSION_CONTROL_CATALOG_SCOPE --lexical-only
Remove-Item Env:DATABASE_DIRECT
```

A later run without `--lexical-only` embeds every catalog row (OpenAI
`text-embedding-3-small`; one batch of about 100 rows, a small paid effect under the budget
policy).

### 2.4 Deployment, key and token

Follow [the operator guide](../../MISSION_CONTROL_LOCAL_API.md) (Operator configuration),
with these local choices:

- `ApplicationBinding.seal(... required_component_version="1.1.0",
  supabase_project_ref="mcdisposablebiotech", installation_id=<as in target.toml>,
  database_secret_ref="MC_LOCAL_BIOTECH_RUNTIME_DSN")`.
- `ApplicationDeployment(family_writer_secret_ref="MC_LOCAL_BIOTECH_FAMILY_DSN",
  temporal=TemporalDeployment(address="127.0.0.1:7233", namespace="default",
  root_task_queue="<base>-root", stagegraph_task_queue="<base>-coordinator-family-stagegraph",
  goal_directed_task_queue="<base>-coordinator-family-goal-directed"))`, for example with
  base `mc-local-biotech`. The worker derives every other queue from `<base>` (operations,
  artifacts, `<base>-linked-runs`, `<base>-maintenance`) and refuses family queues that do
  not follow this convention.
- One `ActorGrant` for your subject, with `tenant_ids={<qualification tenant>}` and the
  permissions `workflow_run.read`, `catalog:read`, `mission.author`, `mission.start`,
  `workflow_run.control`, `workflow_run.cancel`, `workflow_run.pause`,
  `workflow_run.resume`, `workflow_run.observe_wait` and
  `workflow_run.request_continuation`, plus the `sponsorship_refs` and `approval_refs`
  that submit and fork require.
- A local RSA key: keep the private key outside the repository and put only the public JWKS
  next to `deployment.json`. Mint a short-lived RS256 token with `iss`, `aud`, `sub`, `exp`
  and `app_metadata.{application_id, tenant_id}`. `scripts/fast_track_dry_run.py` shows the
  exact calls (`joserfc`). Write the token to a file and load it into
  `MISSION_CONTROL_TOKEN` without echoing it.

### 2.5 Start the processes

API (`make server`), environment names:

| Name | Purpose |
| --- | --- |
| `MISSION_CONTROL_DEPLOYMENT_FILE` | the operator deployment JSON |
| `MC_LOCAL_<APP>_RUNTIME_DSN`, `MC_LOCAL_<APP>_FAMILY_DSN` | the names your binding uses |
| `MISSION_CONTROL_SUBSCRIPTION_RELAY=1` | webhook and MCP subscription delivery (Mission 2 webhook) |
| `CURSOR_API_KEY` | only so `lane list`/`lane describe` show the Cursor profiles |
| `MISSION_CONTROL_ALLOW_UNQUALIFIED_LANES` | describe-only mirror of the worker policy |
| `CAPABILITY_EMBEDDING_PROFILE` | unset for lexical search; `embedding.openai.text-embedding-3-small` embeds every search query (paid, tiny) |

Worker (`make worker`):

| Name | Purpose |
| --- | --- |
| `MISSION_CONTROL_DEPLOYMENT_FILE`, `MISSION_CONTROL_WORKER_APPLICATION_ID`, `MISSION_CONTROL_WORKER_BINDING_DIGEST` | select one application by its sealed binding |
| `COORDINATOR_LAUNCH_ENABLED=true` | execution is opt-in |
| `LANGGRAPH_CHECKPOINT_DATABASE_DIRECT` (checkpointer login, **same** database), `LANGGRAPH_CHECKPOINT_SETUP=false` | runtime persistence |
| `OPENAI_API_KEY`, `TAVILY_API_KEY`, `FIRECRAWL_API_KEY` | Mission 1 model and retrieval (paid) |
| `NCBI_API_KEY` | **not set**; PubMed works keyless at 3 requests per second |
| `EDGAR_IDENTITY` | **not set**; not used by these three missions |
| `CURSOR_API_KEY`, `MISSION_CONTROL_ALLOW_UNQUALIFIED_LANES=true` (local proof only), `CURSOR_LEASE_ROOT`, `CURSOR_LOCAL_REPOSITORY`, `MISSION_CONTROL_HOOK_CALLBACK_PORT` | Cursor lanes (Missions 2 and 3) |
| `MISSION_CONTROL_LANE_SEGMENT_LOOP` | leave `false` (section 1) |
| `MISSION_CONTROL_FRAMES_EXPIRE_SCHEDULE` | default `true`: the worker serves `mc.frames_expire.v1` on `<base>-maintenance` and keeps the daily retention Schedule (readiness commit `f163f1a`) |
| `CAPABILITY_PINS_PATH`, `WEB_RESEARCH_AGENT_BROWSER_NODE` | the pin file (B3, B4) and the node binary for the pinned agent-browser tool |
| `TEMPORAL_PROMOTE_ON_START`, `TEMPORAL_WORKER_VERSIONING`, `TEMPORAL_BUILD_ID` | worker deployment versioning (defaults are fine locally) |
| `LANGSMITH_*` | optional tracing; traces are evidence, not the ledger |

### 2.6 Commands common to every mission

```powershell
$env:MISSION_CONTROL_URL = 'http://127.0.0.1:8000'
$env:MISSION_CONTROL_APPLICATION_ID = 'biotech'      # 'ai-engineer' for Mission 3
missionctl lane list --json
missionctl catalog search --query "pubmed literature retrieval" --kind mcp_server --json
missionctl mission compile <manifest> --json          # Validation Report + resolution per search
missionctl mission submit  <manifest> --json          # revision, run id(s), chain id (Mission 2)
missionctl mission start   <RUN_ID> --json            # B1: start_unavailable today
missionctl run inspect <RUN_ID> --json [--wait 60]
missionctl run transcript <RUN_ID> --format md [--follow]
missionctl run list --query "mission_id='<MISSION_ID>' AND lane='deep_agents'" --json
missionctl run search <RUN_ID> --query "creatine" --json
missionctl events watch <MISSION_ID> [--after-seq N] [--types workflow_run.*,chain_link.*]
missionctl subscribe create --mission <MISSION_ID> --webhook <URL> --secret-ref environment:<NAME> `
  --events chain_link.released,workflow_run.terminalize --json
missionctl command list <RUN_ID> --json
missionctl command cancel <RUN_ID> --urgency immediate --reason "<why>" --json
```

`command cancel` is new in readiness commit `1ba4e4b`. 00-ARCHITECTURE section 8 and the
intervene skill already documented it, but the CLI did not have it before. Kernel event
names to subscribe to instead of the manifest names (B7): `workflow_run.start`,
`workflow_run.set_wait` (a human gate opened), `workflow_run.satisfy_wait`,
`workflow_run.terminalize` (run completed), `activation.lifecycle_changed`,
`activation.phase_changed`, `session.*`, `command.*`, `chain_link.released`,
`chain_link.blocked` and `chain.completed`.

### 2.7 Unpaid checks you can run now

```powershell
$env:MISSION_CONTROL_TEST_ADMIN_DSN = '<loopback PG17 admin DSN>'
uv run --no-sync python scripts/fast_track_dry_run.py                     # production seeds
uv run --no-sync python scripts/fast_track_dry_run.py --with-fixture-rows # + FT-E2 stand-ins
make lane-qualify PROFILE=cursor_local    # offline fixture, describe-honesty and replay suites
make lane-qualify PROFILE=cursor_cloud
```

The dry run installs, seeds, projects, starts the API without Temporal, compiles and submits
the three manifests, and drops everything. It refuses to run while the checkout's `.env`
sets `CAPABILITY_EMBEDDING_PROFILE`. The paid Cursor drill is
`make lane-qualify PROFILE=cursor_local LIVE=1` (then `cursor_cloud`) with `CURSOR_API_KEY`,
a finite `MC_PAID_BUDGET_USD` and, for cloud, `MC_CURSOR_CLOUD_REPO`. Follow the
[qualification README](../../qualification/lanes/README.md).

## 3. Mission 1: research and ingestion on Deep Agents (I1, OVE-59)

Manifest: [missions/01-research-ingestion-deep-agents.yml](missions/01-research-ingestion-deep-agents.yml),
application `biotech`, lane `deep_agents`, Stage Graph `collect -> synthesize -> review
(human gate) -> ingest`.

**Prerequisites:** B1, B2 (the Biotech `kg_ingest` capability is app-owned and must be
real), B3 (PubMed, Biotech KG and the mission skills pinned on the worker) and B4. Also the
local stack of section 2, `OPENAI_API_KEY`, `TAVILY_API_KEY` and `FIRECRAWL_API_KEY`.

```powershell
missionctl mission compile docs\specs\fast-track-2026-10\missions\01-research-ingestion-deep-agents.yml --json
missionctl mission submit  docs\specs\fast-track-2026-10\missions\01-research-ingestion-deep-agents.yml --json
missionctl mission start <RUN_ID> --json
missionctl events watch <MISSION_ID> --types "workflow_run.*,activation.*,session.*"
missionctl run inspect <RUN_ID> --wait 300 --json        # active_waits names the review wait
# Resolve the human gate `review` (the lowered stage wait) after reading the claim table:
missionctl command send <RUN_ID> --request-file satisfy-review.json --json
#   {"schema_version":"mc.command.v1","request_id":"<uuid>","expected_version":<version>,
#    "expected_generation":<generation>,"target":{"kind":"run","id":"<RUN_ID>"},
#    "kind":"satisfy_wait","payload":{"condition_id":"<from active_waits>",
#    "verification_evidence_ref":"review:owner-accepted:<date>"},"reason":"owner accepted the claim table"}
missionctl run transcript <RUN_ID> --format md
```

**Success (I1):** compile reports zero blockers and one resolution per `search`. `run
inspect` shows lane `deep_agents`. `synthesize` sees `/inputs/sources/...` with digests
matching `collect`'s artifact, and `.mission/context.md` lists the packet items. Resolving
`review` releases `ingest`, the domain receipt is recorded, and the run ends `accepted`.
The transcript renders stages, tool calls (digests) and interventions in order. Save the
evidence under `.scratch/fast-track-2026-10-07/I1/`. The subscription criterion needs B7.

**Paid units:** manifest budget USD 40, 3M tokens, 6 h, 600 tool calls (`collect` USD 12,
`ingest` USD 6). One end-to-end run is the primary evidence (I1). A second live run needs
an approved cap in Linear.

## 4. Mission 2: research and ingestion as a Cursor Cloud chain (I2, OVE-60)

Manifest: [missions/02-research-ingestion-cursor-cloud-chain.yml](missions/02-research-ingestion-cursor-cloud-chain.yml),
application `biotech`. `research` (Goal Loop, `cursor_cloud`) supplies `ingestion` (Goal
Loop, `deep_agents`).

**Prerequisites:** B1 (including the chain relay), B2 (a PubMed endpoint qualified for
`cursor_cloud`, for example `mcp.pubmed-remote`, the literature verifier subagent and claim
validation), B3, B4 and B5. Also `CURSOR_API_KEY`, a GitHub-connected throwaway or test
repository (the manifest names `Overton77/biotech-research-notes`) that the worker's git
can push to, and `MISSION_CONTROL_SUBSCRIPTION_RELAY=1` with a webhook receiver and an
`environment:<NAME>` secret ref for the webhook HMAC.

```powershell
missionctl mission compile docs\specs\fast-track-2026-10\missions\02-research-ingestion-cursor-cloud-chain.yml --json
missionctl mission submit  docs\specs\fast-track-2026-10\missions\02-research-ingestion-cursor-cloud-chain.yml --json
missionctl chain inspect <CHAIN_ID> --json        # chain.links: supplies + depends_on `armed`
missionctl subscribe create --mission <INGESTION_MISSION_ID> --webhook <URL> --secret-ref environment:<NAME> `
  --events chain_link.released,workflow_run.terminalize,chain.completed --json
missionctl mission start <RESEARCH_RUN_ID> --json # only `research` starts; the reducer admits `ingestion`
missionctl chain inspect <CHAIN_ID> --wait 600 --json
missionctl run transcript <RESEARCH_RUN_ID> --format md
```

**Success (I2):** submit returns one chain id and two missions, and only `research` is
admitted. `ingestion` is admitted by the chain reducer when `evidence_map` is accepted, and
its first packet carries the supplied outputs. Cloud frames resume from `Last-Event-ID`
with no duplicates. `409 agent_busy` is reported as `wait_then_send`. Usage moves from
`estimated` to `settled`. The webhook receives `chain_link.released` and both run
completions, with valid signatures (B7: use the kernel names).

**Paid units:** `research` USD 30 / 2.5M tokens / 5 h, and `ingestion` USD 15 / 1.5M
tokens / 3 h. A full Cursor Cloud mission run needs an owner-approved cap in a Linear
comment on OVE-60 before it starts (TEAM-WORKSPACE budget policy). Archive or delete every
Cloud agent the run creates.

## 5. Mission 3: codebase feature on Cursor Local with interventions (I3, OVE-61)

Manifest: [missions/03-codebase-feature-cursor-local.yml](missions/03-codebase-feature-cursor-local.yml),
application `ai-engineer`, lane `cursor_local`, Goal Loop with a `deep_agents` verifier.

**Prerequisites:** B1, B2 (the `test_run` and `git_snapshot` executors, the code verifier
subagent, the shell policy hook and the code-change assessment), B4, B5 and B6. Also
`CURSOR_API_KEY` on a WSL or Linux worker, and the ai-engineer local installation (section
2.2 with the ai-engineer identity). The lane leases `git worktree`s from `repo.path`: with
the manifest as written, that is your primary `mission-control` checkout (its `.git`
gains worktree entries; your uncommitted work is not touched). A dedicated clone is safer
(section 8).

```powershell
$env:MISSION_CONTROL_APPLICATION_ID = 'ai-engineer'
missionctl mission compile docs\specs\fast-track-2026-10\missions\03-codebase-feature-cursor-local.yml --json
missionctl mission submit  docs\specs\fast-track-2026-10\missions\03-codebase-feature-cursor-local.yml --json
missionctl mission start <RUN_ID> --json
# Scenario 1: queued instruction, consumed at the next iteration (wait_then_send)
missionctl command queue <RUN_ID> --file queue.json --boundary next_iteration --json
# Scenario 2: interrupt and inject (cancel_and_replace)
missionctl command inject <RUN_ID> --file inject.json --json
# Scenario 4 (before 3): fork from the latest snapshot with an instruction
missionctl run fork <RUN_ID> --instruction-file fork.md --sponsorship-ref <ref> --approval-ref <ref> --json
missionctl run list --query "forked_from='<RUN_ID>'" --json
# Scenario 3: immediate cancel; the Stop Fence denies a beforeShellExecution callback
missionctl command cancel <RUN_ID> --urgency immediate --reason "scenario 3" --json
missionctl run inspect <RUN_ID> --json        # stop fence: four timestamps
missionctl run transcript <RUN_ID> --format md
```

**Success (I3):** compile reports Cursor Local `host_support` for the skill, hook and
subagent pins. The lease contains `AGENTS.md`, `.cursor/rules/mc-mission.mdc`,
`.cursor/skills/agent-browser/`, `.cursor/agents/verifier.md`, `.cursor/mcp.json`,
`.cursor/hooks.json` (kernel hooks first) and `.mission/context.md`. Each of the four
scenarios shows its Delivery Report as SPEC-06 describes. The fork restores the frozen
workspace; this is fixed in readiness commit `37d7deb` and was previously refused with
`CHECKPOINT_INVALID`. On the uncancelled path the run ends `accepted` with `git_snapshot`
and `test_run` artifacts.

**Paid units:** manifest budget USD 20, 2M tokens, 3 h, 500 tool calls, plus the
`deep_agents` verifier's model calls. One real run is the primary evidence. A repeat needs
an approved cap in Linear. Close the Cursor agents the run created.

## 6. Budget policy

Small real fixture runs are permitted by default (TEAM-WORKSPACE, Environment): one agent
run, one search query, one embedding batch of a few hundred rows, or one mission iteration
per recording. A full Cursor Cloud mission run, a bulk catalog embedding, or any repeated
live drill needs an owner-approved cap in a Linear comment on that ticket. Record spent,
reserved and unknown units in the handoff. Stop and record on any ambiguous paid effect,
for example a Cursor agent that may exist without a recorded id: check the dashboard and
archive it.

## 7. Stopping safely

1. `missionctl command cancel <RUN_ID> --urgency immediate`. The Stop Fence is written
   first, so no new effects start; tools already dispatched are not promised to halt.
   Then `run inspect --wait` until the run is `cancelled`.
2. Mission 2: cancelling `research` cancels downstream (`on_upstream_cancel:
   cancel_downstream`). Archive the Cloud agents.
3. Stop the worker with Ctrl+C. The drain (`WORKER_GRACEFUL_SHUTDOWN_SECONDS`) is shorter
   than every heartbeat timeout. Then stop the API.
4. Do not terminate workflows from the Temporal UI unless a run is unrecoverable, and
   record it if you do. `make infra-down` keeps volumes. Never `docker volume prune`.
5. To discard a local installation, drop its database and the LOGIN roles you created.
   Never drop the `mission_control_*` capability roles on a shared server.

## 8. Open owner decisions

| Decision | Context |
| --- | --- |
| Author of lane execution templates (B1) | Map `frontier.default`, `frontier.long_context` and `cursor.default`, the sandbox profiles and the secret refs to pinned components. Then compose a production `LaunchInputPort` and `ChainLaunchInputPort`. |
| Real definitions for the B2 capabilities | Which of the FT-E2 stand-ins become reviewed catalog entries or seeds. The Biotech `kg_ingest` tool is app-owned. |
| Subscription vocabulary (B7) | Emit the SPEC-06 names (`run.completed`, `activation.completed`, `human_task.opened`), or alias them in subscription filters. |
| `pg_trgm` on both Supabase projects before 0026 | `create extension if not exists pg_trgm with schema extensions` (like `vector`), then plan/apply 1.1.0 with an approval comment. |
| Applying release 1.1.0 to Supabase | The plan admits the live 1.0.0 fingerprint `sha256:ef5a9e71...` (reproduced locally). The 0002 and 0004 bytes are restored to the applied CRLF and mixed bytes, so receipts match. Readiness accepts the upgraded receipts (readiness commit `51bb8c7`). Bindings must then pin `required_component_version="1.1.0"`. |
| A2 storage claim name | Bucket policies key on the JWT claim `mc_capability_role` (`publisher` or `reader`). Confirm the name; applying `mc.storage.capability-bundles` to Supabase needs approval on OVE-23. |
| Approved-assets seed drift | `mc.catalog.approved-assets@1.0.0` (applied) is frozen. `@1.0.1` adds only the 0.3.0 router manifest. The coordinator skill definition revision 1 differs from today's repository bytes (kind `skill` relabelled `skill_bundle`, `.agents/skills` checkout bytes). A revision 2 is your decision. |
| D2 family-writer INSERT widening | 0028 lets `mission_control_family_writer` insert `mission_run`, `budget_account` and `effect_ledger` rows (chain-release admission, ADR-0029). Security review pending. |
| Lane qualification | `make lane-qualify PROFILE=cursor_local LIVE=1`, then `cursor_cloud`, with a finite `MC_PAID_BUDGET_USD`. Flip `qualified` only through a reviewed release that cites the record. |
| `NCBI_API_KEY`, `EDGAR_IDENTITY` | Not set. Keyless PubMed is enough for one run. EDGAR is not used by these missions. Provide both as secret refs before EDGAR or higher-rate PubMed runs (OVE-27). |
| Mission 3 repository and worker host | Point `repo.path` at a dedicated clone (and a WSL path for a WSL worker) instead of the primary checkout. This is a fixture adjustment in I3's region. |
| Local PostgreSQL 17 | `make infra-up` runs PostgreSQL 16. Either keep using the disposable PG17 container for fixture runs, or move compose to `pgvector/pgvector:pg17`. |
| Operator profile names | `frontier.default` and the other profiles have no registry yet (part of B1). |

## 9. Evidence of this readiness pass

- Release 1.1.0: the scratch upgrade path (1.0.0 applied, then 1.1.0 plan and apply of
  0025 to 0030, replay `noop`, verify) and a fresh 1.1.0 install with seeds
  `plan/apply/replay` both passed on the disposable server; the scratch databases were
  dropped. The schema fingerprint of 1.1.0 is `sha256:7da7567a...`.
- `pytest -m common_db tests/integration/postgres tests/qualification` on 1.1.0: 175
  passed. One failure, `test_mission_worker_startup`, is B4.
- Dry runs (`scripts/fast_track_dry_run.py`, both modes): results in section 0. Local
  evidence is under `.scratch/fast-track-2026-10-07/readiness/` (not versioned).
- Time-bomb scan: the unit suite was run with the production clock shifted by 60 and by
  400 days. The only new failures compare the shifted production clock with the test's
  real clock or Temporal's: one token-expiry assertion and two Temporal lease checks. No
  test pins a fixed timestamp against the wall clock. SQL has no wall-clock comparisons.
- Secret scan of the fast-track code: hook task tokens live in `.mission/bin/.token` and are
  stored only as digests. They are excluded from patches and from snapshot staging, along
  with `.mission/state` and `.mission/hooks`. MCP projections carry only `${env:NAME}`
  references. Hook script environments are scrubbed. Frame bodies are redacted before they
  are digested. Webhook secrets resolve at send time. No literal credential appears in the
  fast-track diff; the only secret-like strings are synthetic test values in
  `tests/unit/frames/test_frame_contracts_and_body.py`.
