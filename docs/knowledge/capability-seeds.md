---
type: Concept
title: Capability seeds and skill bundles
description: Which MCP servers, skill bundles and hooks the seed bundles admit, the router skill and its five bundles with their LF-canonical digests, and the capabilities the three fixture manifests need that no reviewed seed provides yet (blockers B2 to B4).
tags: [mission-control, capabilities, seeds, skills, implementation]
---

# Capability seeds and skill bundles

How a capability is stored, pinned, searched and projected is in
[capabilities](capabilities.md). This concept lists what is actually admitted and what is not.

## Seeds

`mc.catalog.agent-capabilities@1.0.0` (common) holds `mcp.tavily`, `mcp.firecrawl`
and `mcp.agent-browser`, `skill.agent-browser`, the router `skill.mission-control` and its five
bundles (`-author`, `-catalog`, `-compose`, `-intervene`, `-observe`) and `hook.mc-policy-template`;
the `biotech` bundle adds `mcp.pubmed` and `mcp.biomcp` (+ `skill.biomcp`), `ai-engineer` adds
`mcp.edgartools` (+ `skill.edgartools`) (`packages/mission-control-db-contract/seeds/`, notes
in `AGENT_CAPABILITY_SEED_NOTES.md`). Tool lists are transcribed, not captured live. The
operational catalog surfaces are unchanged: `missionctl catalog list|resolve|search|discover|inspect|components`
plus the new `pin`, `render` and `publish` ([interfaces](interfaces.md)).

## Not wired or not published

- The production seeds lack many capabilities the three fixture manifests search for (the
  Biotech `kg_ingest` tool, a literature-review skill, a cloud-qualified PubMed endpoint,
  verifier subagents, shell-policy hook, `test_run` and `git_snapshot` executors). The
  stand-in rows in `tests/fixtures/catalog/fast_track_seeds.json` are test data, not reviewed
  capabilities (blocker B2 in the [owner runbook](../specs/fast-track-2026-10/OWNER-FIXTURE-RUNBOOK.md)).
- The worker launches only components in its pin file `infra/capability-pins/research-capabilities.json`
  (one OpenAI model, `mcp.tavily`, `mcp.firecrawl`, the `agent-browser` skill) (B3), and that
  pin currently drifts from the workspace `.agents/skills/agent-browser` bytes (B4).
- Live bucket policies, the storage claim name `mc_capability_role` and executable read-only
  mounts are unqualified. Secure pathname validation does not prove an operating-system
  read-only mount.
- Text-only backends reject binary content explicitly. Bundle text cannot choose privileged
  bootstrap modules, credentials, application scope or additional authority.

## Skill bundles of this repository

`skills/mission-control/` (manifest 0.3.0) is the router over `mission-control-author`,
`-catalog`, `-compose`, `-intervene` and `-observe` (SPEC-08, ADR-0033). Digests in each
`skills/*/manifest.json` are LF-canonical; `make skills-check` fails on drift and
`make skills-manifest` rewrites them (`scripts/skills_manifest.py`,
`tests/unit/skills/test_skills_manifest.py`). The coordinator design helper
`.agents/skills/mission-control-coordinator/` is a distinct authoring workflow, not a second
runtime authority.

# Citations

- Spec: [SPEC-01](../specs/fast-track-2026-10/SPEC-01-capabilities-catalog.md),
  [SPEC-08](../specs/fast-track-2026-10/SPEC-08-agent-skills.md),
  [seed capabilities and formats](../specs/fast-track-2026-10/research/seed-capabilities-and-formats.md),
  [owner runbook](../specs/fast-track-2026-10/OWNER-FIXTURE-RUNBOOK.md) (B2 to B4).
- ADRs: [0023](../adr/0023-capability-kinds-provider-neutral-core-host-projection.md),
  [0033](../adr/0033-mission-control-skill-is-a-router-over-specific-skill-bundles.md).
- Code: [seed generator](../../packages/mission-control-db-contract/seeds/generate_catalog_seeds.py),
  [capability definitions](../../packages/mission-control-db-contract/seeds/agent_capabilities.py),
  [seed notes](../../packages/mission-control-db-contract/seeds/AGENT_CAPABILITY_SEED_NOTES.md),
  [skills manifest tool](../../scripts/skills_manifest.py),
  [seed validation](../../scripts/seeds_validate.py),
  [capability pins adapter](../../src/mission_control/adapters/capabilities/capability_pins.py).
- Tests: [capability seeds](../../tests/unit/capability/test_agent_capability_seeds.py),
  [skill seeds](../../tests/unit/capability/test_agent_skill_seeds.py),
  [skills manifest](../../tests/unit/skills/test_skills_manifest.py).
- Test stand-ins (not reviewed capabilities): `tests/fixtures/catalog/fast_track_seeds.json`.
- Pin file: `infra/capability-pins/research-capabilities.json`; skills: `skills/`.
