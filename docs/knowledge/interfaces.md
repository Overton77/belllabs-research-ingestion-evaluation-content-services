---
type: Concept
title: Public interfaces
description: The HTTP routers, the /missions Socket.IO entrypoint, missionctl command groups and coordinator MCP tools that exist today (including the fast-track manifest, chain, transcript, subscription, lane and catalog-pin surfaces and the multi-provider Human Task routes), set against the specified operation catalog, scope vocabulary, error envelope and OAuth rule.
tags: [mission-control, interfaces, http, cli, mcp, implementation]
---

# Public interfaces

The specification wants one operation catalog served identically by HTTP, CLI, MCP and
the skill under the fixed prefix `/v1/applications/{application_id}`. This concept lists
what is mounted today and marks every spec surface as implemented or not. Authentication
and startup are in [operations](operations.md); catalog routes in
[capabilities](capabilities.md).

## HTTP routers that exist

Mounted by the configured API (`bootstrap/api.py`, `create_app`):

| Prefix | Module | Routes |
| --- | --- | --- |
| `/v1/applications/{application_id}` | `interfaces/http/mission_control.py` | `GET runs/{id}/inspection`, `GET runs/{id}/commands`, `POST runs/{id}/commands`, `POST runs/{id}/snapshots`, `GET runs/{id}/snapshots/{snapshot_id}`, `POST runs/{id}/forks`, `POST runs/{id}/reconcile-unit`, `POST run-requests`, `POST runs/{id}/launch` |
| `/v1/applications/{application_id}/catalog` | `interfaces/http/catalog.py` | `GET definitions`; `POST resolve`, `search`, `discover`, `inspect`, `components/search`; `POST publish:prepare`, `publish:complete`, `pin`, `render`; `GET pins/{pin}` |
| `/v1/applications/{application_id}` | `interfaces/http/missions.py` | `POST missions:compile`, `missions:submit`, `missions:start`; `POST missions/{id}/runs` |
| `/v1/applications/{application_id}` | `interfaces/http/chains.py` | `GET chains/{chain_id}`, `GET chains?mission_id=` |
| `/v1/applications/{application_id}` | `interfaces/http/transcript.py` | `GET runs/{id}/transcript`, `runs/{id}/frames/tail`, `runs`, `runs/{id}/transcript/search` |
| `/v1/applications/{application_id}` | `interfaces/http/subscriptions.py` | `POST` and `GET /subscriptions`, `DELETE /subscriptions/{id}`, `POST /subscriptions/{id}/resume`, `GET missions/{id}/events` (SSE) |
| `/v1/applications/{application_id}` | `interfaces/http/continuation.py`, `stop_fence.py` | `GET runs/{id}/checkpoints[/{checkpoint_id}]`, `GET runs/{id}/stop-fence` |
| `/v1/applications/{application_id}/lanes` | `interfaces/http/lanes.py` | `GET` (list), `GET {lane_profile}` |
| `/v1/applications/{application_id}` | `interfaces/http/human_tasks.py` | `GET human-tasks`, `GET human-tasks/{id}`, `POST human-tasks/{id}/resolutions` ([Human Gates](human-gates.md)) |
| (root) | `bootstrap/api.py` | `GET /health/live`, `GET /health/ready` (503 `installation_unavailable`) |

Mounted only by the lower-level proof facade `bootstrap/technical_api.py` (FastAPI title
"Biotech Research Ingestion Evaluation System"; the removal guide calls it "not primary
public startup"): `/control-plane/v1` (definitions, drafts, aliases, compile,
effective-run-configurations, schemas), `/run-control/v1` (run-requests, launch, commands,
reconcile-unit, run, budget, effects, outbox, schemas), `/run-control/v1/inspection`
(runs, units), `/run-control/v1` snapshots and forks (`interfaces/http/run_forks.py`), and
`/v2/graph-runtime/schemas`. The Agent Server app (`adapters/agent_server/http_app.py`)
also mounts the graph-runtime schema router.

The worker-only Kernel Hook callback `POST /v1/applications/{app}/internal/hook-callback`
(`interfaces/http/hook_callback.py`) is served on the worker's loopback listener, never mounted
on the public API. `bootstrap.realtime:create_asgi_app` serves the same API plus the `/missions`
Socket.IO namespace (subscribe, ack, command, `resolve_human_task`; typed `stream_error` codes
including `UNAVAILABLE` and `UNSUPPORTED_OPERATION`), described in
[mission stream](mission-stream.md).

Not implemented from the spec's catalog: `GET /system`, `/schemas`, `/capabilities`,
`POST /context:select`, mission drafting (`POST /missions`, draft patch, `:validate`),
proposals and `revisions/{id}:activate`, mission and artifact reads, `/recoveries`,
`/requests/{request_id}`, `/attempts/{id}/completion-candidates`, interviews and stream tickets.
Manifest compile, submit and start replace drafting ([authoring](authoring.md)); `missions:start`
answers `409 start_unavailable` with a manifest pointer until the operator's launch bindings file
binds every node ([mission manifest](mission-manifest.md)).

## missionctl

`interfaces/cli/main.py` is an argparse client over the scoped HTTP prefix. Groups:
`run` (`inspect`, `admit`, `snapshot`, `fork` with `--instruction-file`, `reconcile`, `start`,
`transcript`, `frames --tail`, `list --query`, `search`, `checkpoint --list|--get`), `command`
(`send`, `list`, `queue`, `inject`, `cancel --urgency`), `mission` (`schema`, `compile` with
`--offline`, `submit`, `start`), `chain` (`inspect`), `subscribe` (`create`, `list`, `close`),
`events` (`watch`), `lane` (`list`, `describe`) and `catalog` (`list`, `resolve`, `search`,
`discover`, `inspect`, `components`, `pin`, `render`, `publish`).
Flags `--application`, `--url`, `--json`, `--wait` (on inspection reads) and
`--request-file` (strict JSON object) may precede or follow subcommands;
`MISSION_CONTROL_APPLICATION_ID`, `MISSION_CONTROL_URL` and `MISSION_CONTROL_TOKEN` are
the environment inputs; HTTP is allowed only to loopback. Exit codes follow the spec:
0 success, 2 invalid, 3 denied, 4 conflict, 5 unavailable, 6 wait timeout or blocked
terminal result. Spec commands absent: `auth login`, `system describe`, `context select`,
`proposal *`, `revision activate`, `human-task *`, `run retry`, `run replay`,
`recovery get`, `request get`, `--tenant`. (`events watch` takes `--after-seq`.) `preflight`
subcommands are a separate module ([operations](operations.md)).

## Coordinator MCP server

`interfaces/mcp/coordinator_server.py` builds a FastMCP server (Streamable HTTP) whose
production tools are `coordinator_bootstrap`, `search_capabilities`, `get_capability`,
`discover_mcp_servers`, `discover_agent_skills`, `inspect_external_candidate`,
`validate_workflow_design`, `prepare_workflow_launch`, `launch_workflow`,
`get_workflow_result`, plus the fast-track tools registered on the same server:
`pin_capability` (catalog pins), `mission_manifest_compile`, `mission_manifest_submit`,
`mission_run_start`, `mission_chain_inspect`, `mission_command_send`, `mission_run_inspect`,
`mission_run_fork`, `mission_subscribe` and, when a transcript service is composed,
`mission_run_transcript` (and `mission_run_search` and `mission_run_list` when search and
Temporal Visibility are composed). Every tool calls the same application service as HTTP and
CLI for the principal's verified tenant scope and refuses a principal from another application.
Resources use the `belllabs://` scheme (`workflow-types/...`,
`catalog/...`, `runs/{run_id}/result|launch|bindings`) plus `mc://applications/{application_id}/...`
resources for chains and run transcripts; prompts are registered from
`coordinator_prompts.py`. `coordinator_auth.py` derives the principal only from FastMCP's
verified access token (claims `tenant_scope`, `request_scopes`, `roles`, `permissions`);
the local runner in `interfaces/mcp/__main__.py` uses a static principal with grants
`catalog.read`, `capability.discover`, `workflow.design.validate`, `workflow.prepare`,
`workflow.launch`, `workflow.result.read`. `interfaces/mcp/human_task_tools.py` defines
`mission_human_task_list|get|resolve` over the one `HumanTaskService`, but no served MCP server
registers them yet (reported, not resolved). The spec's draft and proposal tools
(`mission_create`, `mission_validate`, `mission_proposal_create`, `mission_revision_activate`) are
not implemented.

## Scopes, errors and OAuth

Specified scope vocabulary: `mission.read`, `mission.author`, `mission.start`,
`mission.command`, `mission.invoke`, `mission.review`, `mission.admin`, `catalog.read`,
`catalog.manage`, `execution.report` (SPECIFICATION.md). Implemented grants are
`workflow_run.*` names (`admit`, `start`, `read`, `pause`, `resume`, `cancel`, `admin`,
`reconcile_unit`, `relay`, budget and effect actions; see `ROLE_PERMISSIONS` in
`interfaces/http/run_control.py` and checks in `application/missions/*.py`) plus
`catalog.read`. Manifest submit accepts `workflow_run.admit` or `mission.author`, manifest
start accepts `workflow_run.start` or `mission.start`, and subscribing needs
`workflow_run.read`; `mission.author` and `mission.start` are the only spec scopes the
manifest path honours, so the two vocabularies still differ.

Specified error envelope: `{request_id, code, message, details, retryable,
recovery_ref?}` with codes such as `APPLICATION_FORBIDDEN`, `TENANT_FORBIDDEN`,
`INSTALLATION_MISMATCH`, `VERSION_CONFLICT`, `IDEMPOTENCY_CONFLICT`,
`CAPABILITY_UNAVAILABLE`, `AUTHORITY_DENIED`, `UNSUPPORTED_BEHAVIOR`, `BUDGET_EXHAUSTED`,
`STALE_GENERATION`, `EFFECT_UNCERTAIN`, `CHECKPOINT_INVALID`, `CURSOR_EXPIRED`.
Implemented: FastAPI `detail: {code, message}` with lower-case codes (`unauthorized`,
`stale_version`, `stale_generation`, `resource_not_found`, `request_conflict`,
`idempotency_key_mismatch`, `unsupported_control`, `installation_unavailable`) mapped to
403, 409, 404, 422 and 503 in `interfaces/http/mission_control.py`; Human Task refusals use
their own lower-case codes (`not_reviewer`, `stale_version`, `packet_digest_mismatch`, ...). The
upper-case codes are `IDEMPOTENCY_CONFLICT` (`domain/coordinator/errors.py`) and the socket's
`mc.stream_subscription.v1` codes, which overlap the spec only in `CURSOR_EXPIRED` and
`STALE_GENERATION`.

ADR-0015 requires resource-bound OAuth with PKCE for remote MCP onboarding, resource
tokens scoped to application, tenant and scopes, and forbids forwarding application JWTs.
No authorization-server discovery or OAuth adapter exists in `interfaces/mcp`; the
server trusts whatever verified token FastMCP presents. The CLI sends a bearer token it
never minted (no `auth login`).

# Citations

- Spec: [SPEC-05](../specs/fast-track-2026-10/SPEC-05-mission-manifest.md),
  [SPEC-06](../specs/fast-track-2026-10/SPEC-06-interventions-inspection-subscriptions.md);
  `../mission-control-general/general-mission-control/SPECIFICATION.md` (Public
  skill CLI MCP and dashboard contract);
  `../mission-control-general/general-mission-control/RUNTIME-CONTRACTS.md` (public
  operation additions).
- ADR: [0015](../adr/0015-remote-mcp-oauth-no-jwt-forwarding.md).
- Code: [configured API](../../src/mission_control/bootstrap/api.py), [technical facade](../../src/mission_control/bootstrap/technical_api.py),
  [scoped router](../../src/mission_control/interfaces/http/mission_control.py), [catalog router](../../src/mission_control/interfaces/http/catalog.py),
  [manifest router](../../src/mission_control/interfaces/http/missions.py), [chain router](../../src/mission_control/interfaces/http/chains.py),
  [transcript router](../../src/mission_control/interfaces/http/transcript.py), [subscription router](../../src/mission_control/interfaces/http/subscriptions.py),
  [lane router](../../src/mission_control/interfaces/http/lanes.py), [Human Task router](../../src/mission_control/interfaces/http/human_tasks.py),
  [realtime entrypoint](../../src/mission_control/bootstrap/realtime.py), [manifest MCP tools](../../src/mission_control/interfaces/mcp/mission_tools.py),
  [run control MCP tools](../../src/mission_control/interfaces/mcp/run_control_tools.py),
  [transcript MCP tools](../../src/mission_control/interfaces/mcp/transcript_tools.py),
  [subscription MCP tools](../../src/mission_control/interfaces/mcp/subscriptions.py), [run control](../../src/mission_control/interfaces/http/run_control.py),
  [CLI](../../src/mission_control/interfaces/cli/main.py), [MCP server](../../src/mission_control/interfaces/mcp/coordinator_server.py),
  [MCP auth](../../src/mission_control/interfaces/mcp/coordinator_auth.py).
- Tests: [manifest interfaces](../../tests/unit/authoring/test_manifest_interfaces.py), [chain interfaces](../../tests/unit/chains/test_chain_interfaces.py),
  [transcript interfaces](../../tests/unit/frames/test_transcript_interfaces.py), [catalog CLI](../../tests/unit/capability/test_catalog_cli.py),
  [command cancel CLI](../../tests/unit/run_control/test_cli_command_cancel.py), [subscribe MCP](../../tests/unit/coordinator/test_mission_subscribe_mcp.py),
  [Human Task interfaces](../../tests/unit/human_tasks/test_human_task_interfaces.py), [public interfaces](../../tests/unit/mission_control/test_public_interfaces.py),
  [MCP HTTP deployment](../../tests/unit/coordinator/test_coordinator_mcp_http_deployment.py),
  [authenticated scoped runtime](../../tests/acceptance/mission_control/test_authenticated_scoped_runtime.py).
