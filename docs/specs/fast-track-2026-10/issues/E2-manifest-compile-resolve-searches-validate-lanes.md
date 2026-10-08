# [FT-E2] Compile resolves searches to pins and validates lanes

Linear: OVE-42

**Epic:** Manifest (SPEC-05)
**Team:** T3
**Blocked by:** FT-E1, FT-A3
**Status:** ready-for-agent

**What to build:** `missionctl mission compile mission.yml` and `POST /missions:compile` run the full compile algorithm: scope check, plugin expansion, every `search` entry resolved through Hybrid Search with a `host_support` filter into an exact Capability Pin (zero admitted hits is a `CAPABILITY_UNAVAILABLE` blocker, near ties a warning), every `pin` verified for existence, admission and digest, effective environments flattened with provenance, lane support checked per behavior and per hook event, coverage and governors validated, and the definition lowered through the existing compiler (`CompileInvocation` → `ControlPlaneService.compile`) into a Compiled Program. The command returns the Validation Report, the `mc.manifest_resolution.v1` document and the definition digest, and persists nothing. The three owner manifests compile with zero blockers against the seeded catalog fixture.

**Spec sections:** SPEC-05 §Compile algorithm, §Mapping onto the existing compiler, §Error codes, §Interfaces (compile only); SPEC-01 §Hybrid search (host_support filter); SPEC-07 §Lane describe (behavior support)

**Writable regions:** `src/mission_control/application/authoring/manifest_service.py`, `src/mission_control/interfaces/http/missions.py` (compile route), `src/mission_control/interfaces/mcp/coordinator_server.py` (`mission_manifest_compile` only), `tests/unit/authoring/test_manifest_compile.py`, `tests/fixtures/catalog/fast_track_seeds.json`, `tests/golden/manifests/`; integrator-owned: `mission compile` CLI verb, `application/authoring/service.py` compile entry if a signature change is needed

**Acceptance criteria:**
- [ ] `ManifestCompileService.compile(manifest_yaml, scope)` returns `(definition, compiled_program, validation_report, resolution)` and writes no rows.
- [ ] A file whose `application` differs from the scope fails `APPLICATION_FORBIDDEN`.
- [ ] Each `search` entry calls the Hybrid Search service with `kind`, `host_support ⊇ require | node lane`, `max_results 5`; the top admitted hit becomes the pin; zero hits → `CAPABILITY_UNAVAILABLE{search_no_admitted_hits}`; top two within 0.05 → warning `ambiguous_search` naming both.
- [ ] `pin` entries: missing or revoked → `CAPABILITY_UNAVAILABLE`; digest mismatch → `CAPABILITY_DRIFT`; `@latest` resolves and is recorded.
- [ ] Plugins expand to member pins with provenance `plugin:<alias>`; conflicting versions of one capability id fail `INVALID_DEFINITION`.
- [ ] Lane support: `deep_agents` accepts all behaviors; `cursor_local`/`cursor_cloud` reject `parallel_swarm` and `evaluator_optimizer` with `UNSUPPORTED_BEHAVIOR`; `claude_agent_sdk`/`codex` lanes are rejected in v1; hook events absent on the lane are `unsupported_on_lane` warnings, or blockers when `fail_closed: true`.
- [ ] Coverage and governors: goals without criteria, criteria evidence naming no output, unresolved `from`, unresolved `action_space` aliases, Goal Loops without `iterations`/`patience` are `INVALID_DEFINITION` with pointers; sibling budget oversubscription is warning `oversubscribed_budget`.
- [ ] Lowering produces a Compiled Program through `ControlPlaneService.compile` with recorded `lowering` digests; `cursor_*` nodes lower to `CursorExecutionBinding` placeholders when SPEC-07's binding type is not yet merged (feature-flagged, noted in handoff).
- [ ] Compiling twice yields identical `manifest_digest`, definition digest and resolution; golden resolutions for the three owner manifests are committed.
- [ ] `POST /v1/applications/{app}/missions:compile` returns 200 with report and resolution, 422 with the report on blockers; MCP `mission_manifest_compile` is read-only annotated and requires `mission.read` and `catalog.read`.

**Verification:** `uv run pytest tests/unit/authoring/test_manifest_compile.py -q`; `uv run missionctl mission compile docs/specs/fast-track-2026-10/missions/01-research-ingestion-deep-agents.yml --json` against the API with the fixture catalog (exit 0); `make check`

**Notes:** The Hybrid Search API with `host_support` filtering comes from FT-A3; if A3's lexical-only mode is active in the test environment, searches still resolve (lexical hits are admitted hits). The seeded catalog fixture must contain rows matching every search in the three manifests (PubMed, Tavily, Firecrawl, agent-browser plugin, verifier and literature-verifier subagent profiles, citation-check and shell-policy hook scripts, `kg_ingest`, `coverage_check`, `verify_change`, `test_run`, `git_snapshot`, context bundles, skills); coordinate names with FT-A6 and FT-A7 so production seeds match the fixture. Lane describe (behavior support table) is read from the lane registry when FT-G1 has landed, otherwise from a static table in `manifest_service.py` marked for replacement.
