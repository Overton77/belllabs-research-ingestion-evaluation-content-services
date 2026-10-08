# [FT-B3] Goal Loop iteration packet replaces the stringified handoff

Linear: OVE-32

**Epic:** Context (SPEC-02)
**Team:** T2
**Blocked by:** FT-B1
**Status:** ready-for-agent

**What to build:** Each GoalDirected iteration (and each role inside it) starts from a Context Packet instead of `str(dict)` of the prior handoff. `pack_for_iteration` captures bounded Loop State (inline, mandatory), the sealed journal head as a reference, the prior Progress Review (inline), unresolved blockers and human answers (inline, mandatory), accepted artifacts from `GoalHandoff.artifact_refs` (`auto`), the `/goal/HANDOFF.md` and `/goal/checkpoint.json` snapshot files as `materialize`, and, for the verifier role, only the executor's registered outputs. `_prompt_segments` emits the packet's `admitted_input` segment; `_workspace_for` adds the packet's read-only entries per role; `_bind_handoff` fills the schema-only `GoalHandoffReference` with the packet reference so it has a runtime writer. Demo: two iterations on the real stack where iteration 2's packet contains iteration 1's Progress Review inline and the journal head as a reference, and the verifier's packet contains no executor prompt text.

**Spec sections:** SPEC-02 §Implementation Decisions (Goal Loop iteration handoff), §Contracts, §Testing Decisions

**Writable regions:** `src/mission_control/application/programs/goal_directed.py::_prompt_segments`, `::_workspace_for`, `::_bind_handoff`, `src/mission_control/application/context/pack_service.py` (`pack_for_iteration`), `tests/unit/operations/test_rrm_016_goal_directed_recovery.py` (extend), `tests/integration/postgres/` (goal loop packet test)

**Acceptance criteria:**
- [ ] `_prompt_segments` no longer serializes `asdict(handoff)`; the only context-bearing segment is the packet's `admitted_input` segment, and model-authored `GoalHandoffDraft` content appears inside it as an `untrusted_content` data block.
- [ ] Loop State, unresolved blockers and `human_answer` entries are `mandatory` inline items; the journal head is a `mandatory` `reference` with a `missionctl journal read` retrieval instruction; overflow of these fails with `CONTEXT_BUDGET_EXCEEDED` rather than truncating.
- [ ] `/goal/HANDOFF.md` and `/goal/checkpoint.json` keep their paths and are `materialize` items whose digests match the `GoalWorkspaceService` snapshot.
- [ ] The verifier role receives an independent packet and workspace: its items are only registered artifacts plus `verifier_input_refs`; a test asserts no executor prompt text or reasoning is present.
- [ ] `GoalHandoffReference.{checkpoint, artifact_ref, content_digest}` is populated from the sealed packet and persisted with the handoff; `GoalContinuationState` carries the packet reference across iterations.
- [ ] A `context_selection` row exists per iteration and role with the packet digest.
- [ ] Integration test: two iterations, four operations (prior art: GoalDirected parity acceptance), iteration 2 packet content asserted as above; recovery test still passes with the packet reference in `prior_handoff_ref`.
- [ ] `make check` passes.

**Verification:** `make check`; `uv run pytest tests/unit/operations -k goal_directed -q`; `MISSION_CONTROL_TEST_ADMIN_DSN=... uv run --group biotech pytest -m common_db tests/integration/postgres -k "goal and packet" -q`

**Notes:** Keep the iteration identity and `GoalExecutionClaim` untouched; this ticket changes what the agent reads, not how iterations are scheduled. Nested Goal Loops (02 §14) expose only projected outputs, terminal outcome and a journal reference to the outer loop; make the inner loop's packet candidates respect that subset. Coordinate with F1 (SPEC-06): a queued `add_context` item is simply another candidate of `source_kind: queued_instruction` for the next iteration's packet; leave a hook point (`extra_candidates`) in `pack_for_iteration` for it.
