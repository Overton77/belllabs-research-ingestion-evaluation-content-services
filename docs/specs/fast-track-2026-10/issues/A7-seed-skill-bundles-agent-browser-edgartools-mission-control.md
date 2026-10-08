# [FT-A7] Seed skill bundles: agent-browser, edgartools, mission-control bundles

Linear: OVE-28

**Epic:** Capabilities (SPEC-01)
**Team:** T1
**Blocked by:** FT-A2, FT-H1
**Status:** ready-for-agent

**What to build:** After seeding, the catalog holds admitted `skill_bundle` rows for `skill.agent-browser` (the Vercel stub), `skill.biomcp`, `skill.edgartools`, the six `skill.mission-control*` bundles from SPEC-08, the `hook.mc-policy-template` hook script and the `plugin.web-research` plugin, with their bytes in `capability-bundles` under digest paths and their `computed_hash` matching the `skills` CLI algorithm. A Deep Agents or Cursor mission that pins `skill.mission-control-observe` gets the directory materialized read-only, and `missionctl catalog inspect --pin plugin.web-research@...` lists the four members.

**Spec sections:** SPEC-01 "Seeds" (skill and plugin rows), "Custody in Supabase Storage", "Plugins"; SPEC-08 (bundle contents and manifests).

**Writable regions:** `packages/mission-control-db-contract/seeds/common/mc.catalog.agent-skills-1.0.0.json`, per-application admission seeds, `scripts/seeds_publish_bundles.py` (uploads seed bundle bytes through A2's publish path), `tests/fixtures/skills/` (captured upstream skills at pinned commits), `scripts/hooks/policy_template/` (bytes from A5).

**Acceptance criteria:**
- [ ] `skill.agent-browser` captured from `vercel-labs/agent-browser` at the commit matching 0.38.2 (recorded sha), `computed_hash` recorded; `skill.biomcp` from `biomcp skill install` at 0.9.1; `skill.edgartools` from `edgar.ai.install_skill()` at 5.61.1; each `SKILL.md` `name` equals its directory and passes the Agent Skills frontmatter rules.
- [ ] The six `skill.mission-control*` bundles (router plus `author`, `catalog`, `observe`, `intervene`, `compose`) are seeded from `skills/` with manifests whose digests equal `make skills-manifest` output.
- [ ] `hook.mc-policy-template` seeded as a `hook_script` row (events `before_shell`, `before_tool`, `fail_closed: true`) with its script directory in the bucket.
- [ ] `plugin.web-research` seeded with members `mcp.tavily`, `mcp.firecrawl`, `skill.agent-browser`, `mcp.agent-browser` (optional); `capability_plugin_member` rows exist; `host_support` is the computed intersection.
- [ ] `scripts/seeds_publish_bundles.py` uploads every seed bundle through the A2 publish path, is idempotent (second run uploads nothing), and the seed JSON references the resulting object prefixes and digests.
- [ ] Materialization test (`common_db` plus storage fake): pinning `skill.mission-control-observe` in a Deep Agents binding mounts the directory read-only and a tampered object is refused.
- [ ] Per-application admission: all skills and the plugin in both applications; `skill.biomcp` Biotech only; `skill.edgartools` AI Engineer only.

**Verification:** `make check`; `make skills-manifest && git diff --exit-code skills/`; `uv run python scripts/seeds_publish_bundles.py --target <disposable> --storage <local-or-fake>`; `uv run mission-db seed-apply` then `qualify`; `uv run --group biotech pytest -m common_db tests/integration/postgres/test_skill_bundle_materialization.py -q`.

**Notes:** The agent-browser `SKILL.md` is a thin stub that tells the agent to run `agent-browser skills get core`; do not inline the full skill. Keep upstream skills verbatim; Mission Control adds nothing inside a third-party bundle. The `skills` CLI hash skips `.git` and `node_modules` and sorts by forward-slash relative path. H1 must be `Done` so the mission-control bundle digests are final before seeding.
