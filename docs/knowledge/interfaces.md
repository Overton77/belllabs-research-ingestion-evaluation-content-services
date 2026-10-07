---
type: Concept
title: Public interfaces
description: The HTTP routers, missionctl command groups and coordinator MCP server that exist today, set against the specified operation catalog, scope vocabulary, error envelope and OAuth rule.
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
| `/v1/applications/{application_id}/catalog` | `interfaces/http/catalog.py` | `GET definitions`; `POST resolve`, `search`, `discover`, `inspect`, `components/search` |
| (root) | `bootstrap/api.py` | `GET /health/live`, `GET /health/ready` (503 `installation_unavailable`) |

Mounted only by the lower-level proof facade `bootstrap/technical_api.py` (FastAPI title
"Biotech Research Ingestion Evaluation System"; the removal guide calls it "not primary
public startup"): `/control-plane/v1` (definitions, drafts, aliases, compile,
effective-run-configurations, schemas), `/run-control/v1` (run-requests, launch, commands,
reconcile-unit, run, budget, effects, outbox, schemas), `/run-control/v1/inspection`
(runs, units), `/run-control/v1` snapshots and forks (`interfaces/http/run_forks.py`), and
`/v2/graph-runtime/schemas`. The Agent Server app (`adapters/agent_server/http_app.py`)
also mounts the graph-runtime schema router.

Not implemented from the spec's catalog: `GET /system`, `/schemas`, `/capabilities`,
`POST /context:select`, mission drafting (`POST /missions`, draft patch, `:validate`),
proposals and `revisions/{id}:activate`, `POST /missions/{id}/runs`, mission and artifact
reads, `GET /missions/{id}/events`, human tasks, `/recoveries`, `/requests/{request_id}`,
`/attempts/{id}/completion-candidates`, interviews and stream tickets.

## missionctl

`interfaces/cli/main.py` is an argparse client over the scoped HTTP prefix. Groups:
`run` (`inspect`, `admit`, `snapshot`, `fork`, `reconcile`, `start`), `command` (`send`,
`list`) and `catalog` (`list`, `resolve`, `search`, `discover`, `inspect`, `components`).
Flags `--application`, `--url`, `--json`, `--wait` (only on `run inspect`) and
`--request-file` (strict JSON object) may precede or follow subcommands;
`MISSION_CONTROL_APPLICATION_ID`, `MISSION_CONTROL_URL` and `MISSION_CONTROL_TOKEN` are
the environment inputs; HTTP is allowed only to loopback. Exit codes follow the spec:
0 success, 2 invalid, 3 denied, 4 conflict, 5 unavailable, 6 wait timeout or blocked
terminal result. Spec commands absent: `auth login`, `system describe`, `context select`,
`mission *`, `proposal *`, `revision activate`, `events watch`, `human-task *`,
`run retry`, `run replay`, `recovery get`, `request get`, `--tenant`, `--after-seq`.

## Coordinator MCP server

`interfaces/mcp/coordinator_server.py` builds a FastMCP server (Streamable HTTP) whose
production tools are `coordinator_bootstrap`, `search_capabilities`, `get_capability`,
`discover_mcp_servers`, `discover_agent_skills`, `inspect_external_candidate`,
`validate_workflow_design`, `prepare_workflow_launch`, `launch_workflow` and
`get_workflow_result`. Resources use the `belllabs://` scheme (`workflow-types/...`,
`catalog/...`, `runs/{run_id}/result|launch|bindings`); prompts are registered from
`coordinator_prompts.py`. `coordinator_auth.py` derives the principal only from FastMCP's
verified access token (claims `tenant_scope`, `request_scopes`, `roles`, `permissions`);
the local runner in `interfaces/mcp/__main__.py` uses a static principal with grants
`catalog.read`, `capability.discover`, `workflow.design.validate`, `workflow.prepare`,
`workflow.launch`, `workflow.result.read`. The spec's `mission_*` tool names, `mc://`
resource scheme and in-process application-service calls are not implemented.

## Scopes, errors and OAuth

Specified scope vocabulary: `mission.read`, `mission.author`, `mission.start`,
`mission.command`, `mission.invoke`, `mission.review`, `mission.admin`, `catalog.read`,
`catalog.manage`, `execution.report` (SPECIFICATION.md). Implemented grants are
`workflow_run.*` names (`admit`, `start`, `read`, `pause`, `resume`, `cancel`,
`reconcile_unit`, `relay`, budget and effect actions; see `ROLE_PERMISSIONS` in
`interfaces/http/run_control.py` and checks in `application/missions/*.py`) plus
`catalog.read`; only `catalog.read` overlaps.

Specified error envelope: `{request_id, code, message, details, retryable,
recovery_ref?}` with codes such as `APPLICATION_FORBIDDEN`, `TENANT_FORBIDDEN`,
`INSTALLATION_MISMATCH`, `VERSION_CONFLICT`, `IDEMPOTENCY_CONFLICT`,
`CAPABILITY_UNAVAILABLE`, `AUTHORITY_DENIED`, `UNSUPPORTED_BEHAVIOR`, `BUDGET_EXHAUSTED`,
`STALE_GENERATION`, `EFFECT_UNCERTAIN`, `CHECKPOINT_INVALID`, `CURSOR_EXPIRED`.
Implemented: FastAPI `detail: {code, message}` with lower-case codes (`unauthorized`,
`stale_version`, `stale_generation`, `resource_not_found`, `request_conflict`,
`idempotency_key_mismatch`, `unsupported_control`, `installation_unavailable`) mapped to
403, 409, 404, 422 and 503 in `interfaces/http/mission_control.py`; the only upper-case
spec code present is `IDEMPOTENCY_CONFLICT` in `domain/coordinator/errors.py`.

ADR-0015 requires resource-bound OAuth with PKCE for remote MCP onboarding, resource
tokens scoped to application, tenant and scopes, and forbids forwarding application JWTs.
No authorization-server discovery or OAuth adapter exists in `interfaces/mcp`; the
server trusts whatever verified token FastMCP presents. The CLI sends a bearer token it
never minted (no `auth login`).

# Citations

- Spec: `../mission-control-general/general-mission-control/SPECIFICATION.md` (Public
  skill CLI MCP and dashboard contract);
  `../mission-control-general/general-mission-control/RUNTIME-CONTRACTS.md` (public
  operation additions).
- ADR: [0015](../adr/0015-remote-mcp-oauth-no-jwt-forwarding.md).
- Code: [configured API](../../src/mission_control/bootstrap/api.py),
  [technical facade](../../src/mission_control/bootstrap/technical_api.py),
  [scoped router](../../src/mission_control/interfaces/http/mission_control.py),
  [catalog router](../../src/mission_control/interfaces/http/catalog.py),
  [run control](../../src/mission_control/interfaces/http/run_control.py),
  [run forks](../../src/mission_control/interfaces/http/run_forks.py),
  [runtime inspection](../../src/mission_control/interfaces/http/runtime_inspection.py),
  [control plane](../../src/mission_control/interfaces/http/control_plane.py),
  [graph runtime schemas](../../src/mission_control/interfaces/http/graph_runtime_schemas.py),
  [CLI](../../src/mission_control/interfaces/cli/main.py),
  [MCP server](../../src/mission_control/interfaces/mcp/coordinator_server.py),
  [MCP resources](../../src/mission_control/interfaces/mcp/coordinator_resources.py),
  [MCP auth](../../src/mission_control/interfaces/mcp/coordinator_auth.py),
  [MCP runner](../../src/mission_control/interfaces/mcp/__main__.py).
- Tests: [public interfaces](../../tests/unit/mission_control/test_public_interfaces.py),
  [MCP HTTP deployment](../../tests/unit/coordinator/test_coordinator_mcp_http_deployment.py),
  [authenticated scoped runtime](../../tests/acceptance/mission_control/test_authenticated_scoped_runtime.py).
