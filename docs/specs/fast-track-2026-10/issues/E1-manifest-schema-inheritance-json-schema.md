# [FT-E1] Manifest schema, inheritance rules and JSON Schema export

Linear: OVE-41

**Epic:** Manifest (SPEC-05)
**Team:** T3
**Blocked by:** None (can start immediately)
**Status:** ready-for-agent

**What to build:** `mission.yml` files parse into a strict `MissionManifest` model (`mc.mission_manifest.v1`): goals and criteria with the closed acceptance AST, the Environment bundle, capability pin-or-search entries, agents, hooks, plugins, the program tree with every v1 behavior body, input bindings with expansion tiers, controls, and the `missions` plus `links` form. Per-node effective environments are computed by the inheritance rules (deep merge, lists replace, narrowing only) with field provenance, and violations and shape errors carry JSON pointers. `missionctl mission schema` writes the JSON Schema, and the three owner manifests under `missions/` validate against it. No catalog access and no compile yet; that is E2.

**Spec sections:** SPEC-05 §Contracts (file shape, Environment, Inheritance, Program Node, Input binding, Controls, Chain form), §Field reference, §Insertion points

**Writable regions:** `src/mission_control/domain/authoring/manifest.py`, `src/mission_control/domain/authoring/mission_definition.py`, `src/mission_control/contracts/schemas/mc.mission_manifest.v1.json` (generated), `tests/unit/authoring/test_manifest_schema.py`, `tests/fixtures/manifests/`; integrator-owned: the `mission schema` CLI verb in `interfaces/cli/main.py`

**Acceptance criteria:**
- [ ] `MissionManifest` rejects unknown keys everywhere (`extra="forbid"`) and reports errors as `(pointer, message)` pairs; a malformed `/mission/program/nodes/1/environment/budget/usd` is reported at that pointer.
- [ ] `MissionDefinition@1` (`goals[]`, `objectives[]`, `criteria[]`, `inputs[]`, `program`, `policies`, `budget`, `completion_contract`) exists as a strict model with a stable canonical digest.
- [ ] `manifest_to_definition()` is pure and deterministic (same input → same digest), and the acceptance AST rejects strings that look like code or natural-language predicates.
- [ ] Inheritance: mappings deep-merge, lists replace, scalars replace; a node that raises `budget.*`, `governors.*` or adds a `side_effects` value not in the parent fails with `widens_authority` and a pointer; provenance per field is `inherited | overlay | plugin:<alias>` (plugin provenance is a placeholder until E2 expands plugins).
- [ ] Every behavior body in SPEC-05's table validates; `parallel_swarm` and `evaluator_optimizer` parse (lane rejection is E2).
- [ ] The `missions` form requires `links`; the single `mission` form rejects `links`; `links` shapes validate (semantic link checks belong to D1).
- [ ] `missionctl mission schema --out PATH` writes the JSON Schema and the generated file is committed; the three files under `docs/specs/fast-track-2026-10/missions/` validate against it in a test.
- [ ] `make check` passes; mypy strict on the new modules.

**Verification:** `uv run pytest tests/unit/authoring/test_manifest_schema.py -q`; `uv run missionctl mission schema --out src/mission_control/contracts/schemas/mc.mission_manifest.v1.json && git diff --exit-code -- src/mission_control/contracts/schemas/`; `make check`

**Notes:** `pyyaml` is already a dependency; use `yaml.safe_load` only. Coordinate with D1, which lands in the same wave and owns the `links` block semantics inside `manifest.py`: define the `links` Pydantic shape here and leave graph validation to D1. Decimal money and token fields must not pass through floats (parse as `Decimal`/`int`). Durations (`4h`) parse to seconds with a tiny grammar; document it in the field reference if it changes.
