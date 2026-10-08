---
type: Decision Record
title: "Mission Manifest v1 is settled: environment inheritance per node, missions and links for chains, agents and hooks and plugins blocks, capability search-or-pin entries, and three CLI verbs compile, submit and start"
description: "Closes the open questions of ADR-0022: the file is mission/v1 with either one mission or a missions list plus links; environment is the bundle name and nodes overlay it by deep merge with list replacement; capabilities accept pin or search entries resolved at compile; agents, hooks and plugins are declared at mission or node level and resolve to catalog rows; templates and extends are deferred; the schema lives in domain/authoring/manifest.py with a JSON Schema export."
tags: [mission-control, adr, decision, authoring]
status: accepted
source: fast-track interview 2026-10-07 (requirement 2d); ADR-0022; .scratch/mission-manifest/spec.md; ADR-0023; ADR-0029
---

# Mission Manifest v1 is settled: environment inheritance per node, missions and links for chains, agents and hooks and plugins blocks, capability search-or-pin entries, and three CLI verbs compile, submit and start

ADR-0022 left names and details to a grilling session; the owner accepted the recommendations below. The manifest keeps `environment` as the name for lane, model, sandbox, workspace, capabilities, agents, hooks, plugins, budget and governors; a node's `environment` overlays the mission's by deep merge where lists replace rather than append, so a node can narrow but the compiler rejects a node that widens budget, governors or side-effect allowances. A file holds `mission:` or `missions:` with `links:`; the latter compiles to a Mission Chain (ADR-0029). Capability entries are `{pin: id@version#digest}` or `{search: "...", kind: ..., as: alias, require: [host]}`; every search resolves to an exact pin at compile and the resolution is written into the Validation Report. `agents:` names subagent profiles, `hooks:` names hook scripts with their events, `plugins:` names plugin rows whose members are expanded; all are catalog capabilities and inherit like everything else in `environment`. `templates:` and `extends:` are deferred to v2. `missionctl mission compile` validates and prints the report, `submit` commits the revision and admits the run or chain without starting, and `start` is the separate authorized call; an MCP tool mirrors each.

## Consequences

- The committed revision is still `MissionDefinition@1`; the manifest and its resolution report are stored beside it as authoring provenance.
- Three example manifests ship with the repository, one per owner mission, and are the acceptance fixtures.
- Lane support per behavior is validated at compile: a Goal Loop on `cursor_cloud` is allowed; a Parallel Swarm on any lane but Deep Agents is `UNSUPPORTED_BEHAVIOR` until qualified.
