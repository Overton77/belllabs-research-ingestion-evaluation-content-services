# [FT-H3] Skill acceptance walkthrough with a headless coding agent

Linear: OVE-58

**Epic:** Skills (SPEC-08)
**Team:** Integrator
**Blocked by:** FT-I1, FT-F1, FT-F4, FT-C3
**Status:** ready-for-agent

**What to build:** A headless coding agent (`claude -p`) with only the router and the five bundles installed drives Mission 1 end to end against the local real stack: compiles, submits and starts the manifest, queues one instruction, reads the transcript and registers a stream subscription; the test asserts the outcomes from the API, not from the agent's words.

**Spec sections:** SPEC-08 §Testing Decisions (FT-H3 walkthrough); SPEC-05 (compile, submit, start); SPEC-06 (queue_instruction, subscriptions); SPEC-03 (transcript)

**Writable regions:** `tests/acceptance/skills/`, `experiments/docs_retrieval/` (runner reuse only), `skills/*/SKILL.md` (Availability-line removal for `Done` tickets only)

**Acceptance criteria:**
- [ ] `tests/acceptance/skills/test_skills_walkthrough.py` is opt-in by environment flag like the other live drills and documents the local stack it needs (`make infra-up`, `make temporal-up`, API, worker, bundles under `.claude/skills`).
- [ ] After the agent run, the API shows: one run admitted and started for `missions/01-research-ingestion-deepagents.yml`; one `queue_instruction` command with receipt `applied` and a Delivery Report; `GET /runs/{id}/transcript` returns at least one entry per activation that ran; one `mission_subscription` row for the mission.
- [ ] The agent made no call outside the documented surface (assert on the API audit log or the CLI invocation log the runner captures).
- [ ] Every `Availability: FT-xx` line whose ticket is `Done` at the time of this walkthrough is removed and the bundle version bumped; `make skills-check` passes.
- [ ] The run's recorded cost is reported in the handoff; one real walkthrough is permitted by default, repeats need an approved cap on this issue.

**Verification:** `uv run --group biotech pytest tests/acceptance/skills -m common_db` with the flag set; `make check`; handoff lists passed, failed, blocked and unrun separately.

**Notes:** Reuse the `claude -p` runner pattern from `experiments/docs_retrieval/run.py`. The walkthrough prompt names the mission file and the outcomes; it never pastes the skill text. If the agent fails a step because a bundle is unclear, fix the bundle (bump version) and record the change in the handoff rather than loosening the assertions.
