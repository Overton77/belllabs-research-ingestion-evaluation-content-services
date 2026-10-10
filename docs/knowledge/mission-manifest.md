---
type: Concept
title: Mission Manifest
description: The Mission Manifest YAML authoring surface (mission/v1 and the multi-provider mission/v2) - schema, environment inheritance, lowering to MissionDefinition, V01 lane admission, the compile, submit and start verbs and their interfaces and grants, and the deployment launch bindings start needs, including sealed claude/codex provider bindings and sealed Cursor bindings (B1, MP-02).
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

**Mission start needs a deployment bindings file (B1, MP-02).** `start` needs the lane
execution templates for every lowered stage or Goal Loop role. The production author is
`ManifestLaunchInputAuthor` (`application/authoring/manifest_launch_inputs.py`), composed by
`compose_manifest_launch_inputs` (`bootstrap/manifests.py`) for the API (HTTP, and the CLI over
HTTP) and for the worker's chain relay. It resolves each node's effective environment against
the operator-reviewed `mc.manifest_launch_bindings.v1` file at `MANIFEST_LAUNCH_BINDINGS_PATH`
(Deep Agent profile scaffold and placement, `model_profiles`, `sandbox_profiles`, catalog
`capabilities` → exact MCP/Skill/tool components) and the components the workers serve;
credentials are `SecretRef`s named by `MANIFEST_PROVIDER_SECRET_ENV`. It persists the
templates once under `semantic-input:manifest:{run_id}` and reuses them on any later call.
Anything without an exact binding (a provider or Cursor lane without a `providers` entry,
an unbound or unserved model, sandbox or capability, a missing provider credential name, an
unmapped model setting, hook scripts, subagent profiles, plugins, context bundles) fails with
`409 start_unavailable` before dispatch; the message starts with the selecting pointer.
With the variable unset, start still needs `--request-file` family input. The worker runs the
chain relay when `CHAIN_RELAY_ENABLED=1` (`CHAIN_RELAY_INTERVAL_SECONDS`,
`CHAIN_RELAY_BATCH_LIMIT`, `CHAIN_RELAY_LEASE_OWNER`); it requires the bindings file. Which
pinned model backs `frontier.default`, `frontier.long_context` and `cursor.default`, which
sandbox backs `research.standard` and `ingestion.standard`, and which secret refs each lane may
use stay owner decisions recorded in that file
([owner runbook](../specs/fast-track-2026-10/OWNER-FIXTURE-RUNBOOK.md)).

**mission/v2 (multi-provider, MP-01/MP-02).** Compile and the structural compile dispatch on
the document's `manifest:` literal (`manifest_v2.parse_manifest_yaml_versioned`): `mission/v1`
takes the exact v1 parser and lowering (every v1 digest and golden resolution is unchanged);
`mission/v2` parses `MissionManifestV2` (v1 plus `auth`, `execution_environment`,
`workspace.policy`, `continuation`, `requires`) and lowers through the same
`MissionDefinition@1`. Each `DefinitionNode` then carries additive `selections` /
`verifier_selections` (`mc.environment_selections.v1`), left out of v1 dumps and digests. v2
inheritance adds: a child cannot drop required features or clear `requires.all_writes_gated`,
widen `sandbox.egress`, remove an inherited fail-closed hook (an explicit empty list clears
optional hooks only; Kernel Hooks are never declared), or raise
`continuation.max_transfers`/`max_session_turns`; a node switching to a provider lane declares
its `execution_environment`. **V01**: compile admits every node role's `requires` against its
lane's declared matrix (`harness/describe.declared_matrix`) with `admit_requirements` and
`admit_gate_coverage`; each refusal is an `UNSUPPORTED_BEHAVIOR` blocker whose pointer names the
manifest entry that required it (reason `LANE_REQUIREMENT_UNSUPPORTED`/`_UNQUALIFIED`,
`GATE_COVERAGE_UNENFORCEABLE`, `ELICITATION_NOT_FORWARDED`), before anything is submitted.
`claude_cloud`/`codex_cloud` compile structurally with the warning `hosted_launch_refused`;
their launch is refused (MP-18/19 evidence-blocked). Code:
`application/authoring/manifest_v2_admission.py`.

**Provider launch bindings.** `mc.manifest_launch_bindings.v2` is v1 plus `providers`, one
entry per local provider lane (`claude_agent_sdk`, `codex`; `provider_launch.ProviderLanes`):
the lane activity `task_queue`, `model_profiles` (profile -> `ModelPin`), `auth_profiles`
(-> `AuthPin`), `host_profiles` (-> local `mc.environment_binding.v1`), the exact adapter `pins`
(`claude-agent-sdk 0.2.165` / bundled CLI `2.1.294`; `codex-cli 0.162.0` + app-server schema
sha256), typed `provider_options` (`ClaudeSdkOptions`/`CodexAppServerOptions`, no free-form
keys), session `budgets`, and the template's agent profile, policy refs and workspace
provision. A v1 file reads unchanged. For each claude/codex stage or Goal Loop role,
`ManifestLaunchInputAuthor` authors a sealed `ProviderExecutionBinding`
(`mc.execution_binding.v2`): auth from the node's auth profile (route admission stays at lane
start, MP-05), workspace policy admitted per profile, `requirements` re-admitted against the
declared matrix with its `describe_digest`, a `policy_digest`, budgets narrowed by the node,
the lane `task_queue`, and a `materialization_digest` equal to the MP-03 Host Projection
digest the lane recomputes at `prepare` (`render_host_files(rows, profile,
operating_contract, None, DEFAULT_KERNEL_HOOKS)`; projected capability pins travel as
`capability.<role>.<id>` binding pins, rows via `ProjectionRowsPort` / `ProviderBindingRows`).
The template sets `execution_runtime`, `lane_profile` and `provider_binding`, so
`OperationWorkflowRequest.activity_task_queue` routes it to the lane. A lane, model, auth or
host profile without an exact binding fails at start with `ManifestLaunchBindingError` naming
its manifest pointer. Checked examples (Stage Graph, Goal Loop, two-member chain per lane):
`docs/specs/multi-provider-2026-10/examples/*.mission.yml` with
`deployments/examples/manifest-launch-bindings.multi-provider.example.json`.
**Cursor launch bindings.** `providers.cursor_local`/`cursor_cloud` bind queue, model, auth,
repositories (manifest `repo.path`/`url` -> lane path or URL, `base_refs`), `cursor-sdk 1.0.37`
pins, budgets, hook callback or `environments`; each Cursor role (v1 or v2) gets a sealed
`mc.cursor_binding.v1` pinning the digests the harness re-renders at `prepare`; hooks, plugins,
executors and model settings are refused at their pointer ([cursor_launch](../../src/mission_control/application/authoring/cursor_launch.py)).

Compile and submit of the three fixture manifests were proven on a scratch 1.1.0 installation
(`scripts/fast_track_dry_run.py`): with the production seeds only they block on missing
capabilities (B2); with the test stand-in rows they compile cleanly and submit admits the runs.

# Citations

- Spec: [SPEC-05](../specs/fast-track-2026-10/SPEC-05-mission-manifest.md),
  [owner runbook](../specs/fast-track-2026-10/OWNER-FIXTURE-RUNBOOK.md) (B1, B2).
- ADRs: [0022](../adr/0022-mission-manifest-yaml-authoring-surface.md),
  [0034](../adr/0034-mission-manifest-v1-settled-environment-inheritance-chains-agents-hooks.md).
- Code: [manifest schema and inheritance](../../src/mission_control/domain/authoring/manifest.py),
  [mission/v2](../../src/mission_control/domain/authoring/manifest_v2.py),
  [v2 admission](../../src/mission_control/application/authoring/manifest_v2_admission.py),
  [provider launch bindings](../../src/mission_control/application/authoring/provider_launch.py),
  [launch author](../../src/mission_control/application/authoring/manifest_launch_inputs.py),
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
  [mission/v2](../../tests/unit/authoring/test_manifest_v2.py),
  [v2 compile and provider launch](../../tests/unit/authoring/test_manifest_v2_launch.py),
  [v2 provider launch on real services](../../tests/integration/temporal/test_manifest_v2_provider_launch.py),
  [Cursor launch](../../tests/unit/authoring/test_manifest_cursor_launch.py),
  [compile](../../tests/unit/authoring/test_manifest_compile.py),
  [interfaces](../../tests/unit/authoring/test_manifest_interfaces.py),
  [submit in PostgreSQL](../../tests/integration/postgres/test_manifest_submit.py),
  [lifecycle acceptance](../../tests/acceptance/mission_control/test_manifest_lifecycle.py).
- Fixtures: `docs/specs/fast-track-2026-10/missions/` (the three owner manifests),
  `tests/golden/manifests/` (compile resolutions), `tests/fixtures/manifests/`.
