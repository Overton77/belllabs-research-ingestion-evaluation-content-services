---
type: Specification
title: "SPEC-08: Mission Control agent skills as a router over five specific bundles"
description: "Replaces the single runtime skill and the retired coordinator skill with a router skill plus five versioned bundles (author, catalog, observe, intervene, compose), each with a digest manifest checked by make check, seeded into the catalog as skill_bundle capabilities and projected to every lane's skill path; includes the Availability-line policy for commands that ship later and the headless walkthrough that proves an agent can drive Mission 1 with the skills alone."
tags: [mission-control, spec, fast-track, skills]
---

# SPEC-08: Agent skills

Decision: [ADR-0033](../../adr/0033-mission-control-skill-is-a-router-over-specific-skill-bundles.md). Vocabulary: `GLOSSARY.md` (Skill Bundle, Capability Pin, Host Projection, Coordinator, Command, Delivery Report, Mission Manifest, Mission Chain, Transcript, Subscription). Tickets: FT-H1, FT-H3. Related: SPEC-01 (seeding and projection), SPEC-05 (manifest the author bundle teaches), SPEC-06 (commands the intervene bundle teaches).

## Problem Statement

A coordinator agent (in Claude Code, Codex, Cursor or a Deep Agents session) must drive Mission Control through `missionctl` without inventing commands, without treating prose as authority and without re-reading the whole specification. Today one runtime skill (`skills/mission-control`, version 0.1.1) teaches admission, inspection, a few commands and catalog reads, and a second bundle (`.agents/skills/mission-control-coordinator`) teaches an older Mongo-era design flow with its own tool order. Neither covers authoring a manifest, chaining missions, reading a transcript, queueing or injecting an instruction, forking with an instruction, or subscribing to events. A single skill that covered all of it would exceed what an agent attends to on activation (the Agent Skills guidance is under 500 lines and roughly 5,000 tokens per `SKILL.md`), and two competing authorities already confuse which procedure is current.

## Solution

One router skill, `skills/mission-control`, establishes scope and credentials and routes five branches to five specific bundles. Each bundle is a versioned directory with a `manifest.json` of file digests (`mc.skill_bundle.v1`), a `SKILL.md` whose description front-loads its triggers, numbered steps with checkable completion criteria, and one-level-deep `references/` for the material only some runs need. Commands a bundle describes that ship with a later ticket carry an `Availability: FT-xx` line, so an agent never learns a command that does not exist yet and the line disappears when the ticket closes. The bundles are seeded into the catalog as `skill_bundle` capabilities (SPEC-01, FT-A7), which is how a mission can pin the coordinator's own skills and have them projected into `.claude/skills`, `.cursor/skills`, `.agents/skills` or a Deep Agents skills path like any other capability. `make check` fails when any manifest digest drifts from disk. The coordinator bundle is retired with a pointer, not deleted.

## User Stories

1. As a coordinator agent, I want one skill description to tell me which bundle handles my branch, so that I load only the procedure I need.
2. As a coordinator agent, I want each bundle's steps to end in a condition I can check, so that I know when a step is done instead of guessing.
3. As a coordinator agent, I want to write a Mission Manifest from an interview with a field cheatsheet, so that the compiler, not I, resolves searches to pins.
4. As a coordinator agent, I want to read a Validation Report field by field, so that I fix the manifest at the pointer rather than retrying blindly.
5. As a coordinator agent, I want to search capabilities by kind and lane, inspect one, and pin it exactly, so that a mission never binds a mutable version.
6. As a coordinator agent, I want to publish a new skill bundle with custody then registration, so that the bundle is immutable and verifiable.
7. As a coordinator agent, I want to know that discovery results are quarantined candidates, so that I never attach an external MCP server to a running mission.
8. As a coordinator agent, I want to report a run's lifecycle, phase and terminal outcome separately, so that a finished turn is never mistaken for acceptance.
9. As a coordinator agent, I want to read a transcript as JSONL with a cursor, so that I can resume reading where I stopped.
10. As a coordinator agent, I want to register a subscription when I start a run, so that human tasks and completion reach the consumer without polling.
11. As a coordinator agent, I want command body templates for every command kind, so that `expected_version` and `expected_generation` come from inspection and the request id survives retries.
12. As a coordinator agent, I want to know which delivery semantics a lane used for my command, so that I report requested and delivered separately.
13. As a coordinator agent, I want to fork a run from a snapshot with an instruction, so that an alternative branch starts from known state without touching the source.
14. As a coordinator agent, I want to decide between chaining missions, nesting a loop and invoking a child mission, so that governance boundaries match the work.
15. As a coordinator agent, I want to know what crosses a chain link and what does not, so that I never expect a shared workspace.
16. As an operator, I want `make check` to fail on a stale skill manifest, so that a pinned bundle always matches its bytes.
17. As an operator, I want the retired coordinator skill to point at the router, so that an agent that still finds it is redirected rather than misled.
18. As a mission author, I want to pin `skill.mission-control-observe` into a Deep Agents or Cursor mission, so that the agent inside the mission can inspect sibling runs through the same procedure.
19. As an implementation agent, I want each bundle to name the ticket that delivers a command it describes, so that I know which skill text to update when my ticket lands.
20. As a reviewer, I want a headless walkthrough that drives Mission 1 with the skills alone, so that the skills are proven against the real API rather than read for plausibility.

## Implementation Decisions

### Topology

- `skills/mission-control/SKILL.md` is the router: model-invoked (its description is the one pointer most hosts load), under 60 lines of body, with a scope-and-credentials step, a routing table (branch, bundle, entry command) and the invariants every bundle obeys. `references/protocol.md` stays with the router as the full command surface with availability markers; `references/operations.json` stays as the operation catalog the service contract range refers to.
- Five bundles, each model-invoked so the router can name them and an agent can fire them directly: `mission-control-author` (manifest cheatsheet, validation report), `mission-control-catalog` (kinds and projections), `mission-control-observe` (transcript format), `mission-control-intervene` (command templates), `mission-control-compose` (chain patterns). Bundle names equal directory names and match the Agent Skills name rule (lowercase, hyphens, at most 64 characters).
- `.agents/skills/mission-control-coordinator/SKILL.md` gains a first-line retirement pointer to ADR-0033 and is otherwise untouched; the seed row `definition:skill:skill.mission-control-coordinator` stays in the frozen 1.0.0 seed and is retired through a later seed version in FT-A7, never edited in place.

### Manifests and digests

- Every bundle carries `manifest.json` with `schema_version: mc.skill_bundle.v1`, `name`, `version`, `service_contract_range`, `files` (relative POSIX path to `sha256:<hex>` for every file except the manifest itself) and, when the bundle contains `references/operations.json`, `operation_catalog_digest` equal to that file's digest.
- `scripts/skills_manifest.py --check` verifies every bundle under `skills/` (a bundle is a directory with `SKILL.md`): missing manifest, name mismatch and any file digest drift are failures; `--write` rewrites `files` and the operation digest while preserving `name`, `version` and `service_contract_range`. `make skills-check` runs it and is part of `make check`; `make skills-manifest` rewrites.
- A content change to a bundle is a version bump: the router moves from 0.1.1 to 0.2.0 because its procedure changed; new bundles start at 0.1.0. The frozen seed `mc.catalog.approved-assets-1.0.0.json` keeps describing router 0.1.1 as applied live; FT-A7 registers 0.2.0 and the five new bundles in a new seed version.
- At publish (FT-A2) the catalog additionally records the Vercel `skills` CLI `computedHash` (sorted relative paths, path then bytes) so pins interoperate with `skills-lock.json`.

### Availability lines

- A command, flag or endpoint that a bundle describes but that is not yet shipped carries `Availability: FT-xx` naming the delivering ticket from `00-ARCHITECTURE.md` section 9; where a present-day equivalent exists the line says so (`today use …`).
- The implementing ticket's acceptance criteria include removing its Availability line and bumping the bundle version; FT-I4 verifies no `Availability:` line remains for a `Done` ticket.
- Until the ticket is `Done`, the service answers the described command with a typed error, never a partial result; the skill text says so in the router.

### Seeding and projection

- FT-A7 registers each bundle as a `skill_bundle` capability with its manifest digest and `host_support` of every lane profile; bytes live in `capability-bundles` under digest paths (ADR-0024).
- Host projection (SPEC-01) writes the bundle to `.claude/skills/<name>/` for Claude Code (which does not read `.agents/skills`), to `.agents/skills/<name>/` for Codex and Cursor (Cursor also reads `.cursor/skills`), and to a skills path passed to `create_deep_agent(skills=[…])` for Deep Agents. The bundle bytes never change per host; only the path does.
- A manifest that pins `skill.mission-control-observe` inside a mission therefore gives the executing agent the same observe procedure the coordinator uses, scoped by that mission's grants.

### Writing rules the bundles follow

- Description first-loads the triggers (the branches that should fire it) and nothing the body repeats.
- Body is numbered steps; each ends in a checkable completion criterion ("Done when `blockers` is empty").
- Reference files are one level deep and named in the step that needs them; nothing in a reference is required by every run.
- Glossary terms only; the Avoid lists are honoured (Goal Loop not GoalDirected in prose, Command not signal, Lane not provider, Transcript not trace).
- No secret values, no invented commands beyond `00-ARCHITECTURE.md` section 8, no restatement of OpenAPI schemas beyond shapes.

## Testing Decisions

- A good test here checks external behaviour: the manifest check fails on drift and passes on a clean tree; a skill-driven agent reaches the expected API calls and outcomes. Tests never assert prose wording.
- `tests/unit/skills/test_skills_manifest.py`: for a temporary bundle, `--write` produces the expected digests and sorted keys; editing a file makes `--check` exit 1 and name the path; a missing manifest and a name mismatch fail; `operation_catalog_digest` tracks `references/operations.json`. Prior art: `tests/unit/control_plane/test_catalog_seed_bundles.py` (digest-of-manifest assertions).
- `make check` includes `skills-check`; CI runs it on every change.
- FT-H3 headless walkthrough (`tests/acceptance/skills/test_skills_walkthrough.py`, opt-in through an environment flag like the other live drills): start the local real stack, run `claude -p` with the router and bundles installed under `.claude/skills` and a prompt that asks it to compile, submit and start the Mission 1 manifest, queue one instruction, read the transcript and register a stream subscription; assert from the API, not from the agent's text, that a run was admitted and started, one `queue_instruction` command reached `applied` with a Delivery Report, the transcript endpoint returned entries, and a subscription row exists. Prior art: `experiments/docs_retrieval/run.py` (`claude -p` runner) and `tests/acceptance/mission_control/test_authenticated_scoped_runtime.py` (API-driven acceptance).
- Each bundle's `SKILL.md` is validated with the Agent Skills reference validator (`skills-ref validate`) when available in the environment; otherwise the manifest check's frontmatter name rule stands in.

## Out of Scope

- Rewriting `docs/knowledge/capabilities.md` and the OKF index (FT-I4).
- Seeding the bundles and uploading bytes (FT-A7, FT-A2) and the `catalog publish` command (FT-A2).
- Host-specific extra frontmatter (`allowed-tools`, Claude `context: fork`, Cursor `icon`); the bundles use only spec fields so every host accepts them.
- Translating the bundles into MCP prompts; the coordinator MCP server keeps its own prompts.
- Any command not listed in `00-ARCHITECTURE.md` section 8.

## Further Notes

- The router is model-invoked rather than user-invoked because other skills and the catalog projection must be able to reach it; the cost is one always-loaded description, which is why its wording carries the branches and nothing else.
- Deep Agents reads a skill body through `read_file` after the `SkillsMiddleware` injects name and description; the bundles' one-level references suit that pattern because the agent reads exactly one more file per branch.
- The old coordinator skill's `scripts/validate_workflow_design.py` and schemas are not carried forward; manifest compile (FT-E2) replaces their role.

## Tickets

| Id | Title | Blocked by |
| --- | --- | --- |
| FT-H1 | Router skill and five bundles with manifests and digest check | None |
| FT-H3 | Skill acceptance walkthrough with a headless coding agent | FT-I1, FT-F1, FT-F4, FT-C3 |

# Citations

- [ADR-0033](../../adr/0033-mission-control-skill-is-a-router-over-specific-skill-bundles.md); [ADR-0023](../../adr/0023-capability-kinds-provider-neutral-core-host-projection.md); [ADR-0024](../../adr/0024-skill-bundle-custody-supabase-storage-digest-paths.md).
- `GLOSSARY.md`; [00-ARCHITECTURE.md](00-ARCHITECTURE.md) sections 8 and 9.
- [research/seed-capabilities-and-formats.md](research/seed-capabilities-and-formats.md) section 6 (Agent Skills frontmatter, host paths, `computedHash`).
- `.claude/skills/writing-for-agents/SKILL.md` and `SKILL-MECHANICS.md` (router skills, pointers, completion criteria).
- Code: `skills/mission-control/`, `skills/mission-control-*/`, `scripts/skills_manifest.py`, `Makefile` (`skills-check`), `packages/mission-control-db-contract/seeds/common/mc.catalog.approved-assets-1.0.0.json` (frozen seed describing router 0.1.1).
- Tests: `tests/unit/control_plane/test_catalog_seed_bundles.py`; `experiments/docs_retrieval/run.py`.
