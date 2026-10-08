# [FT-B2] Stage handoff delivers the packet and materializes inputs

Linear: OVE-31

**Epic:** Context (SPEC-02)
**Team:** T2
**Blocked by:** FT-B1
**Status:** ready-for-agent

**What to build:** A Stage Graph consumer stage actually receives what its producers made. The interpreter keeps the `producer_output_slot → consumer_input_slot` mapping (new `frozen_input_bindings` beside the unchanged `frozen_input_refs`), the stage preparation service captures those accepted outputs plus the node's selected catalog context and the operating contract, calls the Context Packer, adds the rendered read-only mounts to the workspace contract, replaces the ad hoc objective segment with the packet's `admitted_input` segment, seeds `.mission/context.md` and `.mission/inputs.json`, and persists the packet with its `context_selection` record in the admission transaction. On the producer side the Deep Agents adapter fills `RuntimeResult.output_refs` from the artifacts it promoted so dependency projections carry registered refs. Demo: a two-stage graph on the real local stack where stage B's sandbox contains stage A's accepted artifact under `/inputs/<binding>/` with a verified digest and B's prompt lists it.

**Spec sections:** SPEC-02 §Implementation Decisions (Stage handoff, Renderers, lane seeding for Deep Agents), §Persistence (`context_selection`), §Worked example

**Writable regions:** `src/mission_control/domain/programs/interpreter.py::_input_refs` and the `frozen_input_bindings` fields in `domain/programs/contracts.py`, `src/mission_control/application/programs/service.py::StageGraphOperationPreparationService.materialize`, `src/mission_control/application/context/pack_service.py` (`pack_for_stage`, capture through ports, persistence), `src/mission_control/adapters/postgres/context/` (selection repository), `src/mission_control/adapters/deep_agents/adapter.py` (`RuntimeResult.output_refs`), `adapters/deep_agents/materializer.py::prepare` (`upload_files` / `files` seeding), `tests/unit/operations/`, `tests/integration/postgres/`, `tests/integration/deep_agents/`

**Acceptance criteria:**
- [ ] `_input_refs` returns `StageInputBinding(consumer_input_slot_id, producer_stage_key, producer_output_slot_id, artifact_ref, accepted_decision_ref, provisional)` tuples; `frozen_input_refs` (sorted refs) is byte-for-byte unchanged so existing snapshot and fork digests still match (existing parity tests pass unmodified).
- [ ] `StageGraphOperationPreparationService.materialize` calls `pack_for_stage`; the request's prompt segments contain exactly one `admitted_input` packet segment and no `objective_override` segment (the objective is a `goals_and_criteria` item).
- [ ] `WorkspaceContract.read_mounts` / slot bindings include every `materialize` item; the existing `WorkspaceMaterializationService._load_and_verify_inputs` fetches and digest-verifies them without changes to its contract.
- [ ] `.mission/context.md` and `.mission/inputs.json` are present in the materialized workspace before the first model call (Docker sandbox via `upload_files`; `StateBackend` via the `files` input; binary items on the text-only backend are downgraded to `reference` with `unsupported_media`).
- [ ] A `context_selection` row is written in the same transaction as the admission proposal with `packet_digest`, `prompt_plan_digest`, `file_plan_digest`; a `PackFailure` rejects admission with `CONTEXT_BUDGET_EXCEEDED` before any provider call.
- [ ] An unaccepted producer output is omitted with `not_accepted`; a provisional binding yields a non-mandatory item flagged `provisional`.
- [ ] `DeepAgentRuntimeAdapter.execute` sets `RuntimeResult.output_refs` to the durable refs of this attempt's promotions; a model-emitted `output_refs` entry that is not a promotion of this attempt is dropped with a `provenance` warning in the settlement.
- [ ] Integration test (`common_db`): stage A → stage B, B's packet has A's artifact as `materialize`, B's workspace has the file, the selection row exists; prior art `tests/acceptance/mission_control/test_postgres_runtime_parity.py`.
- [ ] `make check` passes; `uv run --group biotech pytest -m common_db tests/integration/postgres -k handoff` passes on the disposable stack.

**Verification:** `make check`; `make infra-up`; `MISSION_CONTROL_TEST_ADMIN_DSN=... uv run --group biotech pytest -m common_db tests/integration/postgres -k "handoff or packet" -q`; `uv run pytest tests/integration/deep_agents -k packet -q`

**Notes:** Shared-file touch: `domain/programs/contracts.py` gains additive fields only; coordinate with the integrator if a non-additive change is needed. Record in the handoff whether `graph.update_state` can seed `StateBackend` files on an existing thread and whether `CompositeBackend.upload_files` routes to every child (both UNVERIFIED in research/deepagents-middleware.md §3.3). The Biotech `schema_context` catalog item is a `reference` with a `tool_call` retrieval; do not inline it.
