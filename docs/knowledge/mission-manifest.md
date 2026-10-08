---
type: Concept
title: Mission Manifest
description: The Mission Manifest v1 YAML authoring surface - schema, environment inheritance, lowering to MissionDefinition, the compile, submit and start verbs and their interfaces and grants, and why start is blocked in the configured deployment (B1).
tags: [mission-control, authoring, manifest, compile, implementation]
---

# Mission Manifest

The [Mission Manifest](../../GLOSSARY.md) is the human and coordinator authoring surface; it
compiles into a [Mission Definition](../../GLOSSARY.md) and is never the committed revision. The
coordinator launch path and the overall interview flow are in [authoring](authoring.md); chains
are in [mission chains](mission-chains.md); capability search and pins in
[capabilities](capabilities.md).

ADR-0022 and ADR-0034, SPEC-05. A `mission.yml` (`manifest: mission/v1`, schema
`mc.mission_manifest.v1`, exported by `missionctl mission schema`) is the human and coordinator
authoring surface for one mission or for `missions` plus `links` (a chain, see
[mission chains](mission-chains.md)). It is parsed with YAML 1.2 core scalars, duplicate keys
and floats rejected, unknown keys rejected everywhere, and every failure is a
`(code, JSON pointer, message)` triple (`domain/authoring/manifest.py`). A mission-level
[Environment](../../GLOSSARY.md) is inherited by every node: mappings deep-merge, lists and
scalars replace, narrowing only, and the provenance of every field is recorded. The committed
Revision is the typed `MissionDefinition@1` (`domain/authoring/mission_definition.py`), never
the YAML; `manifest_lowering.py` lowers it deterministically onto the existing compiler (a
`goal_loop` root to a GoalDirected blueprint, a `stage_graph` root to a StageGraph with a human
gate as a stage carrying a declared wait). A nested Goal Loop under a Stage Graph is lowered as a
stage with the warning `nested_goal_loop_lowered_as_stage`, because the kernel has no
nested-loop family. Lane execution bindings are never named by the manifest.

- **compile** (`application/authoring/manifest_service.py`, `POST /missions:compile`,
  `missionctl mission compile`, MCP `mission_manifest_compile`) is deterministic and persists
  nothing. It checks the file's `application` against the authenticated scope, expands plugins
  to member pins, resolves every `search` through [Hybrid Search](capabilities.md) in the
  installation catalog partition to the top admitted hit (`CAPABILITY_UNAVAILABLE` when none,
  warning `ambiguous_search` when the top two are within 0.05) and every `pin` to its exact
  row (`CAPABILITY_DRIFT` on another digest), computes effective environments, validates lane
  host support and hook events per lane vocabulary, coverage, budgets and depth, and compiles
  through the control plane against a dry-run catalog overlay. The output is the Validation
  Report plus `mc.manifest_resolution.v1`. `--offline` runs only the catalog-free structure
  compile. A static table still stands in for lane behavior support.
- **submit** (`application/authoring/manifest_submit.py`, `POST /missions:submit`, MCP
  `mission_manifest_submit`, grants `workflow_run.admit` or `mission.author`) refuses on
  blockers, publishes the lowered definitions and the Effective Run Configuration
  (content-addressed, idempotent), builds the Run Request exactly as ordinary admission does and
  commits the revision, typed definition rows, authoring provenance (the manifest bytes and
  resolution) and the admitted run, or the chain with its first run and the frozen admissions of
  later members, in one transaction (`adapters/postgres/control_plane/manifest_submission.py`).
  It is idempotent on `request_id` and never starts anything.
- **start** (`POST /missions:start`, `POST /missions/{id}/runs`, `missionctl mission start`,
  MCP `mission_run_start`, grants `workflow_run.start` or `mission.start`) registers the
  manifest's `controls.subscriptions` and then launches the admitted run through the governed
  `RunLaunchService`, with the mission id written to the root's `mc_mission_id` search attribute.

**Mission start is blocked (B1).** `start` needs the lane execution templates for every lowered
stage or Goal Loop role (model component, prompt segments, MCP servers, skills, capability
grant, workspace contract, output schema, plus the Cursor binding). The API composes
`compose_manifest_service` with `launch_inputs=None` (`bootstrap/api.py`, `bootstrap/manifests.py`),
so `ManifestSubmitService.start` raises `ManifestStartUnavailable` and the route answers
`409 start_unavailable` unless the caller passes `--request-file` family input. Only the test
author `StagedLaunchInputs` (`tests/fixtures/manifest_runtime.py`, deterministic models)
exists. The chain relay needs the same author (`ChainLaunchInputPort`). Closing B1 needs
owner decisions first: which pinned model backs `frontier.default`, `frontier.long_context` and
`cursor.default`, which sandbox backs `research.standard` and `ingestion.standard`, and which
secret refs each lane may use ([owner runbook](../specs/fast-track-2026-10/OWNER-FIXTURE-RUNBOOK.md)).
Compile and submit of the three fixture manifests were proven on a scratch 1.1.0 installation
(`scripts/fast_track_dry_run.py`): with the production seeds only they block on missing
capabilities (B2); with the test stand-in rows they compile cleanly and submit admits the runs.

# Citations

- Spec: [SPEC-05](../specs/fast-track-2026-10/SPEC-05-mission-manifest.md),
  [owner runbook](../specs/fast-track-2026-10/OWNER-FIXTURE-RUNBOOK.md) (B1, B2).
- ADRs: [0022](../adr/0022-mission-manifest-yaml-authoring-surface.md),
  [0034](../adr/0034-mission-manifest-v1-settled-environment-inheritance-chains-agents-hooks.md).
- Code: [manifest schema and inheritance](../../src/mission_control/domain/authoring/manifest.py),
  [definition](../../src/mission_control/domain/authoring/mission_definition.py),
  [lowering](../../src/mission_control/domain/authoring/manifest_lowering.py),
  [compile](../../src/mission_control/application/authoring/manifest_service.py),
  [submit and start](../../src/mission_control/application/authoring/manifest_submit.py),
  [composition](../../src/mission_control/bootstrap/manifests.py),
  [PostgreSQL submit](../../src/mission_control/adapters/postgres/control_plane/manifest_submission.py),
  [revision writers](../../src/mission_control/adapters/postgres/control_plane/mission_revisions.py),
  [manifest routes](../../src/mission_control/interfaces/http/missions.py),
  [manifest MCP tools](../../src/mission_control/interfaces/mcp/mission_tools.py),
  [dry run script](../../scripts/fast_track_dry_run.py).
- Tests: [schema](../../tests/unit/authoring/test_manifest_schema.py),
  [compile](../../tests/unit/authoring/test_manifest_compile.py),
  [interfaces](../../tests/unit/authoring/test_manifest_interfaces.py),
  [submit in PostgreSQL](../../tests/integration/postgres/test_manifest_submit.py),
  [lifecycle acceptance](../../tests/acceptance/mission_control/test_manifest_lifecycle.py).
- Fixtures: `docs/specs/fast-track-2026-10/missions/` (the three owner manifests),
  `tests/golden/manifests/` (compile resolutions), `tests/fixtures/manifests/`.
