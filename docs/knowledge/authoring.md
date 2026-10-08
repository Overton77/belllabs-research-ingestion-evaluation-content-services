---
type: Concept
title: Coordinator authoring and launch
description: How a coordinator or a human takes a mission from description to an explicit start - the coordinator MCP launch path and the Mission Manifest v1 compile, submit and start path - and which steps run today; mission start is blocked in the configured deployment by blocker B1 (no production launch input author).
tags: [mission-control, authoring, coordinator, mcp, implementation]
---

# Coordinator authoring and launch

The [Coordinator](../../GLOSSARY.md) is an API client with authoring capabilities. It
may run in Codex, Claude Code, a dashboard chat or an admitted Deep Agent session; its
conversation with the human is not the execution program and not Temporal history
(`../mission-control-general/general-mission-control/SPECIFICATION.md`,
"Coordinator interaction and authoring").

## The required interaction

1. Establish application, tenant and actor from authenticated context, then select a
   bounded context pack: schemas, available behaviors, templates, capabilities,
   grants and policies ([Context Selection](../../GLOSSARY.md)).
2. Discuss the outcome with the human: goals, success criteria, evidence,
   deliverables, side effects, review needs and ceilings. This is the
   [Interview](../../GLOSSARY.md), the question-asking mode of a bounded, budgeted
   [Authoring Session](../../GLOSSARY.md), used for research, ingestion, content and
   coding missions alike.
3. Choose exact templates, profiles, skills, MCP tools, sandbox profiles and
   executors from the admitted catalog; missing capabilities are visible blockers.
4. Create an editable [Draft](../../GLOSSARY.md) with program, objectives, bindings,
   completion contracts and governors.
5. Validate without launching agents or performing domain writes. The
   [Validation Report](../../GLOSSARY.md) is typed: errors with JSON pointers, missing
   capabilities, policy conflicts, cost and capacity warnings, transition impact.
6. Submit a revision proposal, compile deterministically, resolve any required
   authoring review, commit the revision and activate its head with expected-head
   concurrency ([revisions](revisions.md)).
7. Start a run with immutable input and binding manifests. Start is a separate,
   explicit, authorized call; commit does not imply start. Application policy may let
   the coordinator make that call within the human-authorized task.
8. Observe events and artifacts, resolve human tasks through attributed actions,
   issue commands (structural edits create a new proposal), and submit outputs as a
   completion candidate; Mission Control computes acceptance ([completion](completion.md)).

`AuthoringSession@1` records draft ids, participants and attributed decision refs;
raw chat stays in the client's product store. The spec's MCP surface for this flow is
the `mission_*` family (`mission_create`, `mission_validate`, `mission_proposal_create`,
`mission_revision_activate`, `mission_run_start`, `mission_human_task_resolve`, ...).

## Implemented today: the coordinator MCP server

`create_coordinator_server` in `src/mission_control/interfaces/mcp/coordinator_server.py`
builds a FastMCP server ("BellLabs Coordinator"). The coordinator launch path uses these
tools (the manifest, run-control, transcript, chain and subscription tools registered on the
same server are listed in [interfaces](interfaces.md)):

`coordinator_bootstrap`, `search_capabilities`, `get_capability`,
`discover_mcp_servers`, `discover_agent_skills`, `inspect_external_candidate`,
`validate_workflow_design`, `prepare_workflow_launch`, `launch_workflow`,
`get_workflow_result`.

Read-only tools are annotated as such; the two discovery tools are open-world and
return candidate-only results that must be inspected and published before use;
`launch_workflow` is tagged `consequential`, requires the `workflow.launch` grant on
the authenticated principal and an idempotency issuer equal to the actor. Prompts are
`propose_workflow`, `review_workflow_design`, `explain_launch_blocker` and
`summarize_workflow_result` (`coordinator_prompts.py`); resources cover workflow-type
contracts and schemas, catalog assets and manifests, and
`belllabs://runs/{run_id}/result|launch|bindings` (`coordinator_resources.py`).
Tools, prompts and resources absent from the effective surface are disabled, not faked.

Each tool calls `ProductionCoordinatorFacade` in
`src/mission_control/application/coordinator/coordinator_facade.py`, which enforces a
grant per operation, rate limits, audits and never touches the database directly:

- `validate_workflow_design` parses a `WorkflowDesignDraft`
  (`src/mission_control/domain/coordinator/contracts.py`), resolves each requested
  exact asset, lists missing refs and candidates requiring promotion, and always
  returns `launchable=False` and `requires_publication=True`. It is a structural
  validation report; it launches nothing and carries no transition impact.
- `prepare_workflow_launch` (`CoordinatorLaunchPreparationService` in
  `src/mission_control/application/coordinator/coordinator_launch.py`) compiles the
  proposal through `ControlPlaneService.compile`
  (`src/mission_control/application/authoring/service.py`), runs an admission
  preview, freezes the run request and its digest, and stores a
  `PreparedLaunchTicket` with a TTL, warnings and a `launchable` flag. This is the
  code's [Launch Ticket](../../GLOSSARY.md); its durable row is
  `mission_control.coordinator_launch_ticket` (migration `0016_coordinator_support.sql`,
  states `prepared | consumed | expired | invalidated`).
- `launch_workflow` (`CoordinatorWorkflowLaunchService.launch`) is the separate
  explicit start: it re-validates the launch context, rejects expired, invalidated,
  unlaunchable or already-consumed tickets, admits the frozen run request through the
  reducer, authors the exact semantic binding, submits the Temporal root and consumes
  the ticket.

## Mission Manifest v1 (compile, submit, start)

ADR-0022 and ADR-0034, SPEC-05. A `mission.yml` (`mc.mission_manifest.v1`) is compiled
deterministically into a typed `MissionDefinition@1`, a Compiled Program and a Validation Report
(`compile`, nothing persisted), committed as a revision with the admitted run or chain
(`submit`, nothing started), and launched by a separate authorized `start`. The verbs are on
`missionctl mission`, `/missions:compile|submit|start` and MCP `mission_manifest_*` /
`mission_run_start`. Detail is in [mission manifest](mission-manifest.md).

**Mission start is blocked (B1).** The API composes the manifest service with
`launch_inputs=None`, so `start` answers `409 start_unavailable`; only a test author of the lane
execution templates exists. Closing it needs owner decisions (model, sandbox and secret-ref
mappings) and a production launch input author for the run and the chain relay
([owner runbook](../specs/fast-track-2026-10/OWNER-FIXTURE-RUNBOOK.md)).

## Specified only

No `AuthoringSession@1` record, interview budget, `mission_*` draft or proposal tools,
or context-pack selection operation exists in `src/`; the coordinator path is validate
design, prepare ticket, launch, and the manifest path is compile, submit, start (the
manifest Validation Report does carry JSON pointers). The only drafts are catalog definition drafts
(`ControlPlaneService.save_draft`, `publish_draft` and `AuthoringHead` with
`draft_revision` and `published_revision`), not mission drafts. Remote MCP onboarding
with resource-bound OAuth (ADR-0015) lives in `coordinator_auth.py`, outside this concept.

# Citations

- [SPEC-05](../specs/fast-track-2026-10/SPEC-05-mission-manifest.md), [ADR-0022](../adr/0022-mission-manifest-yaml-authoring-surface.md), [ADR-0034](../adr/0034-mission-manifest-v1-settled-environment-inheritance-chains-agents-hooks.md).
- `../mission-control-general/general-mission-control/SPECIFICATION.md` ("Coordinator interaction and authoring", "Public skill CLI MCP and dashboard contract").
- `../mission-control-general/workflow-types/07-MISSION_REVISION_AND_RUNTIME_EVOLUTION.md` (section 3).
- [ADR-0015](../adr/0015-remote-mcp-oauth-no-jwt-forwarding.md).
- [Coordinator MCP server](../../src/mission_control/interfaces/mcp/coordinator_server.py),
  [prompts](../../src/mission_control/interfaces/mcp/coordinator_prompts.py),
  [resources](../../src/mission_control/interfaces/mcp/coordinator_resources.py).
- [Coordinator facade](../../src/mission_control/application/coordinator/coordinator_facade.py),
  [launch preparation and launch](../../src/mission_control/application/coordinator/coordinator_launch.py),
  [coordinator domain contracts](../../src/mission_control/domain/coordinator/contracts.py),
  [launch ticket contracts](../../src/mission_control/domain/coordinator/launch.py),
  [control-plane authoring service](../../src/mission_control/application/authoring/service.py).
- [Launch preparation tests](../../tests/unit/coordinator/test_coordinator_launch_preparation.py),
  [idempotency tests](../../tests/unit/coordinator/test_coordinator_launch_idempotency.py),
  [facade tests](../../tests/unit/coordinator/test_coordinator_facade.py),
  [MCP surface tests](../../tests/unit/coordinator/test_coordinator_mcp_read_surface.py),
  [control-plane tests](../../tests/unit/control_plane/test_control_plane.py).
- Manifest: [mission manifest](mission-manifest.md).
- [`0016_coordinator_support.sql`](../../packages/mission-control-db-contract/component/migrations/0016_coordinator_support.sql).
