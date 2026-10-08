# [FT-H1] Router skill and five bundles with manifests and digest check

Linear: OVE-57

**Epic:** Skills (SPEC-08)
**Team:** T1
**Blocked by:** None (can start immediately)
**Status:** ready-for-agent

**What to build:** A coordinator agent installs `skills/mission-control` and is routed to one of five bundles (`mission-control-author`, `-catalog`, `-observe`, `-intervene`, `-compose`) for its branch; every bundle has a digest manifest that `make check` verifies; commands that ship later carry `Availability: FT-xx` lines; the retired coordinator skill points at the router. The first draft of every file was written with the packet on 2026-10-07; this ticket reviews it against SPEC-08, adds the unit test for the manifest script, and leaves the bundles ready for FT-A7 to seed.

**Spec sections:** SPEC-08 §Implementation Decisions (Topology, Manifests and digests, Availability lines, Writing rules), §Testing Decisions (manifest unit test)

**Writable regions:** `skills/`, `scripts/skills_manifest.py`, `Makefile` (skills targets only), `tests/unit/skills/`

**Acceptance criteria:**
- [ ] `python scripts/skills_manifest.py --check` passes on a clean tree and `make check` runs it.
- [ ] `tests/unit/skills/test_skills_manifest.py` covers: write produces sorted digests; a changed file fails check naming the path; missing manifest fails; directory and manifest name mismatch fails; `operation_catalog_digest` tracks `references/operations.json`.
- [ ] Every `SKILL.md` frontmatter `name` equals its directory and the description lists the triggers; bodies are numbered steps with a stated completion criterion per step; every reference is one level deep and named by a step.
- [ ] Every command in the bundles appears in `00-ARCHITECTURE.md` §8; those not yet shipped carry `Availability: FT-xx` with the correct ticket (A2 publish, A8 catalog flags, C3 transcript, C4 run list and search, D2 chain inspect, E2 compile, E3 submit and start, F1 queue, F2 inject, F3 immediate cancel, F4 fork flags, F5 subscribe and events watch, F6 inspection enrichment).
- [ ] Bundle vocabulary matches `GLOSSARY.md` including Avoid lists.
- [ ] Router manifest version is `0.2.0`; new bundles are `0.1.0`; the frozen seed `mc.catalog.approved-assets-1.0.0.json` is unchanged.
- [ ] `.agents/skills/mission-control-coordinator/SKILL.md` begins with the retirement pointer and is otherwise unchanged.

**Verification:** `make check`; `python scripts/skills_manifest.py --check`; `uv run pytest tests/unit/skills`; `python docs/tools/check_links.py`.

**Notes:** Do not edit seeds here; FT-A7 registers the bundles. When your review changes any bundle file, bump that bundle's `version` and rerun `make skills-manifest`. The Agent Skills validator (`skills-ref validate`) is optional; record whether it ran.
