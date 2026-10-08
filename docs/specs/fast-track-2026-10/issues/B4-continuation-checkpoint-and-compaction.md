# [FT-B4] Continuation checkpoint sealed from the packet; compaction frames; request_continuation

Linear: OVE-33

**Epic:** Context (SPEC-02)
**Team:** T2
**Blocked by:** FT-B1, FT-C1
**Status:** ready-for-agent

**What to build:** Continuation becomes real on the Deep Agents lane. The `mc.continuation_checkpoint.v1` contract and validator (every field of workflow-types/08 §6; invalid checkpoints never seed a session). Compaction is observed: `ObservedSummarizationMiddleware` replaces the stock deepagents `SummarizationMiddleware` in place (same `.name`), detects the private `_summarization_event` (or the `compact_conversation` tool) and emits `before_compaction` and `after_compaction` Provider Frames through the FrameSink; the offloaded `/conversation_history/<session>.md` is registered as a `conversation_history` artifact. A `continuation.seal` activity runs at the next safe boundary after a trigger (context health threshold, provider compaction frame, turn count, boundary, `request_continuation` command): freeze actions, snapshot state and workspace, deterministic reduction, optional admitted compactor, pack a `purpose = continuation` packet with one `workspace` item, validate, seal, write `session.checkpoint_sealed`. A fresh thread hydrates from the checkpoint packet (restored files, checkpoint fields inline), `session.transferred` is written, and held mailbox commands are released after the first `session_init` frame. Demo: force summarization with `trigger=("messages", 3)`, see both compaction frames and a sealed checkpoint; send `request_continuation` and see a new thread continue with the restored `/inputs` and `/outputs` candidates.

**Spec sections:** SPEC-02 §Contracts (`mc.continuation_checkpoint.v1`), §Implementation Decisions (Continuation checkpoint and compaction, Hydration from a checkpoint packet), §Persistence (`continuation_checkpoint`), §Testing Decisions

**Writable regions:** `src/mission_control/domain/context/checkpoint.py`, `src/mission_control/adapters/deep_agents/compaction.py`, `adapters/deep_agents/materializer.py` (middleware list replacement, hydration from checkpoint), `src/mission_control/adapters/temporal/activities/continuation.py`, `src/mission_control/application/context/pack_service.py` (`pack_for_continuation`), `adapters/postgres/context/` (checkpoint repository), family workflow boundary call sites in `adapters/temporal/workflows/{stagegraph,goal_directed}.py` (seal call only; coordinate with T4 who owns `operation.py`), `interfaces/cli/main.py` (`run checkpoint`), `interfaces/http/mission_control.py` (checkpoint reads; coordinate with T5), `tests/unit/operations/`, `tests/integration/deep_agents/`

**Acceptance criteria:**
- [ ] `ContinuationCheckpoint` model has every 08 §6 field; the validator rejects missing fields, digest mismatches, dangling references, authority or capability widening, unresolved gates and workspace inconsistency, each with a typed reason; a corrupted summary cannot drop `queued_commands`, `budgets_remaining` or `unresolved.human_task_refs` (test).
- [ ] `ObservedSummarizationMiddleware` keeps the stock middleware's `.name`, replaces it in `create_deep_agent`'s list, and emits `before_compaction` (cutoff index, pre-estimate) and `after_compaction` (summary digest, `file_path`, post-estimate) frames; `/conversation_history/<session>.md` is downloaded and registered as an artifact of kind `conversation_history`.
- [ ] `continuation.seal` activity implements the 08 §9 sequence and failure policy (§8): park, retry compactor within cap, fallback compactor, human review when required, explicit failure; a fresh session never starts from `validator.result = invalid`.
- [ ] The continuation packet has exactly one `workspace` item restoring `/inputs/**`, `/outputs/**` candidates and `.mission/**`; a digest mismatch on restore fails with `CHECKPOINT_INVALID`.
- [ ] `session.checkpoint_sealed` and `session.transferred` mission events are written with source and target session refs; mailbox commands held during transfer are released only after the target's first `session_init` frame.
- [ ] `request_continuation` is accepted by the command path for Deep Agents with delivery `turn_boundary_guaranteed` (coordinate the reducer action with T5/F1; this ticket owns the trigger and seal).
- [ ] Governors `max_transfers`, cumulative cost and wall-clock, `max_failed_compactions`, `no_progress_transfers` are enforced and exhaustion is a governed terminal outcome.
- [ ] `missionctl run checkpoint RUN_ID --list|--get ID` and `GET .../runs/{id}/checkpoints[/{id}]` return checkpoints with bodies redacted to digests above 4 KiB.
- [ ] Integration test on the local stack: forced summarization yields both frames and a sealed, valid checkpoint; `request_continuation` yields a new thread whose `ls /inputs` matches the snapshot manifest; `make check` passes.

**Verification:** `make check`; `uv run pytest tests/unit/operations -k "checkpoint or continuation" -q`; `MISSION_CONTROL_TEST_ADMIN_DSN=... uv run --group biotech pytest -m common_db tests/integration/deep_agents -k continuation -q`

**Notes:** Deep Agents has no compaction callback (research/deepagents-middleware.md §2.3, implication 2): detect `_summarization_event` changes in `wrap_model_call` or catch the `compact_conversation` tool in `wrap_tool_call`. Use `durability="sync"` for threads whose checkpoints Mission Control must observe immediately (implication 9). Cross-thread hydration copies checkpoint values onto a new `thread_id`, which is UNVERIFIED as a practice; record what works. The Cursor side of continuation (new agent + git patch) is G4, which consumes the same checkpoint packet.
