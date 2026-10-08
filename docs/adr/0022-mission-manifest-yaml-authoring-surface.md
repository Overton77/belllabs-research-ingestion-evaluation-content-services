---
type: Decision Record
title: The Mission Manifest is a YAML authoring surface that compiles into the typed Mission Definition
description: A human- or coordinator-written mission.yml with mission-level environment and per-node overlays, searches resolved to exact pins at submit; the committed revision never carries aliases or searches.
tags: [mission-control, adr, decision, authoring]
status: accepted
source: interview 2026-10-07 (Q8 remark, Q18); .scratch/mission-manifest/spec.md; SPECIFICATION.md (Definition and compiler contracts); ADR-0010; ADR-0020
supplement: accepted; details settled by ADR-0034 (2026-10-07)
---

# The Mission Manifest is a YAML authoring surface that compiles into the typed Mission Definition

Missions are authored as a **Mission Manifest** (`mission.yml`): goals and criteria, a mission-level `environment` (lane, model profile, sandbox profile, workspace context and skills, capabilities, budget, governors) that every program node inherits and may overlay, and the program tree. The manifest may name capabilities by catalog **search** or alias; the existing deterministic compiler resolves them to exact pins, validates coverage, lane support, budgets and governors, and emits the typed `MissionDefinition@1`, the Compiled Program and a Validation Report. The committed revision is the typed definition, never the YAML, so the specification's rule that aliases cannot survive a committed binding still holds. We chose a separate authoring surface over authoring the typed definition directly because the owner needs to describe computational and agent environments at the granularity people think in (per node, with inheritance) while keeping the kernel's contract strict. The name avoids "contract", which already names Completion Contract and Operating Contract.

## Consequences

- Schema lives beside the other authoring contracts (`src/mission_control/domain/authoring/`) with a JSON Schema export; `missionctl mission compile` and `mission submit`, and an MCP tool, take the manifest.
- A search with zero admitted hits is a typed blocker in the Validation Report, never a fallback.
- Details (templates, `extends`, naming of `environment`) are settled in a dedicated grilling session before implementation.
