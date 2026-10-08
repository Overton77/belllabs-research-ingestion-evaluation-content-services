# [FT-A1] Capability kinds migration, definitions and host support

Linear: OVE-22

**Epic:** Capabilities (SPEC-01)
**Team:** T1
**Blocked by:** None (can start immediately)
**Status:** ready-for-agent

**What to build:** A catalog that can hold the five agent-composition kinds. After this ticket an operator can insert a `hook_script`, `subagent_profile` or `plugin` row (and `skill_bundle`, `mcp_server`, `mcp_tool` under their new literals) through the existing control-plane definition path, each with a `host_support` matrix and `secret_refs`, and `missionctl catalog list` shows them with their kind and supported lane profiles. A plugin row lists its exact member pins and cannot be admitted while a member is not admitted. The Python `DefinitionKind` and the SQL `asset_version.kind` agree through one mapping, and `middleware` no longer maps to `hook`.

**Spec sections:** SPEC-01 "The five kinds and the provider-neutral core", "Host support matrix", "Plugins", "Subagent profiles", "Vocabulary reconciliation", "Contracts" (`mc.capability_host_support.v1`, `mc.subagent_profile.v1`, `mc.plugin_manifest.v1`), "Persistence" (0025).

**Writable regions:** `src/mission_control/domain/capabilities/` (new), `src/mission_control/adapters/postgres/control_plane/catalog_assets.py`, `packages/mission-control-db-contract/component/migrations/0025_capability_kinds_and_host_support.sql`; shared (integrator-reviewed): `src/mission_control/domain/authoring/contracts.py` (`DefinitionKind`, `CapabilityKind`, `CapabilityRequirement.validate_policy`, `Definition` union).

**Acceptance criteria:**
- [ ] Migration `0025_capability_kinds_and_host_support.sql` extends the `asset_version.kind` CHECK with `skill_bundle`, `mcp_server`, `mcp_tool`, `hook_script`, `subagent_profile`, `plugin`, `middleware`, adds `host_support jsonb` (schema-version CHECK) and `secret_refs text[]`, creates `capability_plugin_member` with forced RLS and `mc.*` scope policies, and the plugin admission trigger; applies and replays as a no-op on a disposable cluster.
- [ ] `DefinitionKind` gains `hook_script`, `subagent_profile`, `plugin` (`plugin_package` removed); `ASSET_KIND` maps every `DefinitionKind` to exactly one SQL literal; `MIDDLEWARE → middleware`; a unit test asserts the mapping is total.
- [ ] New Pydantic definitions `HookScriptDefinition`, `SubagentProfileDefinition`, `PluginDefinition` with JSON Schema export; `SkillDefinition` and `MCPServerDefinition` gain `host_support`, `secret_refs`, and (`MCPServerDefinition`) `transport`, `env_refs`, `header_refs`, `oauth`, `package_pin`, `tools_list_digest`, `tool_allowlist`.
- [ ] Capability Pin parser and renderer (`<id>@<version>#sha256:<digest>`) with round-trip and rejection tests.
- [ ] `host_support` validation rejects unknown profile ids and unknown overlay keys per kind; plugin `host_support` is computed as the member intersection honouring `optional`.
- [ ] `CapabilityKind` and `CapabilityRequirement.validate_policy` accept the new kinds; existing seed bundles still validate (`tests/unit/control_plane/test_catalog_seed_bundles.py` passes).
- [ ] `missionctl catalog list --json` includes `kind` and `host_support` for the new rows (read path only; search is A3).
- [ ] `common_db` test: insert one row per new kind, admit a plugin only after its members are admitted (trigger refuses otherwise).

**Verification:** `make check`; `uv run --group biotech pytest tests/unit/control_plane tests/unit/capability -q`; `uv run --group biotech pytest -m common_db tests/integration/postgres/test_catalog_kinds_0025.py -q` (new) with `MISSION_CONTROL_TEST_ADMIN_DSN` set; `uv run mission-db plan && uv run mission-db apply` against the disposable target then `mission-db verify`.

**Notes:** ADR-0020 forbids editing an applied migration; 0025 is a new file. Keep the old literals `skill`, `tool`, `hook` in the CHECK so existing rows stay valid; retiring them is a later migration. Do not touch `search_document` here (A3 owns 0026). The subagent `name` must be rejected when it collides with a lane built-in (`explore`, `shell`, `bash`, `browser`, `debug`, `computerUse`, `cursorGuide`, `general-purpose`).
