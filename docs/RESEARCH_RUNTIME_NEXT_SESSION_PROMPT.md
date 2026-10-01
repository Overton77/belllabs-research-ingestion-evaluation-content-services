# Next agent session: prerequisite implementation only

Recorded: 2026-10-01
Paste the prompt below into Claude Code from the application repository. For an agent team, use [`RESEARCH_RUNTIME_AGENT_TEAM_COORDINATOR_PROMPT.md`](RESEARCH_RUNTIME_AGENT_TEAM_COORDINATOR_PROMPT.md) instead. The audit and ticket creation are already complete; do not restart M0 as an unbounded planning task.

```text
Work in C:\Users\Pinda\Proyectos\Biotech\biotech-research-ingestion-evaluation-system.

Complete the next prerequisite implementation issue for our research-runtime mission. The user will create and run the Qualia Life and GenerationLab company fixtures in a SEPARATE LATER SESSION. Do not create or launch those company fixtures, reports, forks, automations or an agent session to run them, even if every prerequisite passes. Stop with an implementation handoff.

Read in order:
1. AGENTS.md and applicable .cursor/rules, especially engineering-sequence, project-organization and workflows-domain-contracts.
2. docs/RESEARCH_RUNTIME_IMPLEMENTATION_AUDIT_2026-10-01.md
3. docs/migrations_instructions/implementation_work_packages_v2/RESEARCH_RUNTIME_MISSION_TICKETS.md
4. docs/migrations_instructions/implementation_work_packages_v2/research-runtime-mission/issues/02-repair-verification-baseline.md
5. docs/RESEARCH_RUNTIME_MISSION_SPECIAL_HANDOFF_2026-10-01.md
6. docs/migrations_instructions/implementation_work_packages_v2/IMPLEMENTATION_READINESS.md and evidence_v2/README.md.
7. Canonical governing specs and accepted CP/BP evidence named in the ticket index.

The audit found mypy green (340 files), Ruff 104 errors, full pytest 44 failed/645 passed/44 skipped, and focused owning suites 1 failed/141 passed. The focused failure reads a moved source path. Representative other causes are wrong repository-root paths, snapshot retention tied to an expired test date, and a publish test receiving 422 instead of 201. These are findings to verify and repair, not permission to weaken assertions or skip tests.

Start with RRM-002: restore the verification baseline under existing accepted authority. Recheck the current checkout/revision and dirty-work inventory. Do not repeat the whole architecture audit or implement new checkpoint semantics in this issue. Diagnose every remaining failure before classifying it as stale or environmental.

Preserve pre-existing user changes and untracked agentic-component work. Use a dedicated issue branch/worktree based on a reviewed committed base and integrate into integration/research-runtime-mission. Do not silently stage unrelated files. Only use uncommitted component work or the local lifecycle brief after ownership and a reviewed dependency commit are established. The tracked audit/ticket bodies are available in fresh worktrees.

Follow the repository implement skill and existing spec/ticket authority. Repair moved references while preserving architecture/security/provenance checks. Make tests use deterministic time without changing production expiry. Diagnose strict API validation before changing either contract or expectation. Resolve mission-blocking baseline failures; create explicit dependent repair tickets for genuine defects beyond the selected issue. Do not blanket skip/xfail, broadly lower assertions, or use fake provider success as qualification.

Run the narrow owning checks, then the required shared gate. Keep technical service/provider tests explicitly opted in and record their results honestly. Small technical fixtures, captured-history replay and failure-injection inputs are allowed. Live company missions are not.

Complete RRM-002 through code changes, tests, review, sanitized evidence, ticket/index update and integration merge. Keep main merge gated on RRM-010's combined acceptance. Do not publish remote issues/PRs or push without separate authorization. Do not mark historical CP acceptance as today's green baseline.

The next frontier after baseline repair is RRM-001 canonical contract/spec reconciliation, then RRM-003 exact checkpoint lineage and RRM-004 crash recovery. RRM-001 is a specification issue; its accepted outputs gate new contracts. SPEC-CP-COGNITIVE-SCHEMAS is draft and ADR-0004 proposed: do not treat them as accepted merely because code already carries digests. Follow recorded dependencies and clear context between implementation issues.

End by reporting the actual ticket disposition, changed/tested commits, commands/results, evidence paths, unresolved gates and exact next ticket. STOP. Never proceed to company fixture execution in this prerequisite session. After RRM-010 eventually passes, return ready_for_separate_fixture_session and let the user start the later fixture session.
```

Update 2026-10-01: RRM-002 is accepted and merged into `integration/research-runtime-mission` (`ebd1ca1`), so the next frontier is RRM-001. The user added RRM-013: real async subagents on a persistent Agent Server. It follows RRM-004, and RRM-008, RRM-009 and RRM-010 depend on it. A fake Agent Protocol client no longer satisfies any gate. Select the next ready prerequisite ticket from the index and read its full body. Keep the separate-session boundary. Do not repeat accepted work or jump over RRM-001's spec gate. Read the index's mission-horizon note: keep mechanisms reusable for the later generalization, but do not extract a framework during this mission.
