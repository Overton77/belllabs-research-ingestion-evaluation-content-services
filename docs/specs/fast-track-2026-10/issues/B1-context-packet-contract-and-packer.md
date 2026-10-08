# [FT-B1] Context Packet contract, packer and renderers

Linear: OVE-30

**Epic:** Context (SPEC-02)
**Team:** T2
**Blocked by:** None (can start immediately)
**Status:** ready-for-agent

**What to build:** The `mc.context_packet.v1` contract and the pure Context Packer in the domain layer, plus the renderers that turn a sealed packet into `.mission/context.md`, `.mission/inputs.json`, one `admitted_input` prompt segment and read-only `WorkspaceSlotBinding` entries. Given captured candidates (accepted outputs, Loop State, journal digest, catalog context, operating contract, goals, commitments, budget remaining) and a model profile budget, the packer deterministically assigns each item an Expansion Tier (`inline`, `reference`, `materialize`, `workspace`), allocates mandatory items first, downgrades optional overflow to `reference`, fails mandatory overflow with `CONTEXT_BUDGET_EXCEEDED`, records omissions with reason codes and seals a digest that is stable across shuffled inputs. A developer can run the packer on fixture candidates and get byte-identical golden outputs; JSON Schema for the packet is exported beside the other contracts.

**Spec sections:** SPEC-02 §Contracts (`mc.context_packet.v1`, `mc.context_selection.v1` linkage), §Implementation Decisions (Budget arithmetic, Packer algorithm, Renderers), §Testing Decisions (unit goldens)

**Writable regions:** `src/mission_control/domain/context/packet.py`, `src/mission_control/domain/context/render.py`, `src/mission_control/application/context/pack_service.py` (ports only: `ArtifactBytesPort`, `TokenCounterPort`, `ContextSelectionRepository`; no persistence wiring yet), `src/mission_control/contracts/` (JSON Schema export for the packet), `tests/unit/context/`

**Acceptance criteria:**
- [ ] `ContextPacket`, `ContextItem`, `RetrievalInstruction`, `OmittedItem` Pydantic models exist with the fields in SPEC-02 and reject unknown fields; JSON Schema is exported and round-trips.
- [ ] `pack(request)` is a pure function (no I/O, no clock, no randomness); unit tests prove identical `packet_digest` for shuffled candidate order and different digests when `packer_version` changes.
- [ ] Budget arithmetic implements `available_input = context_window - reserved_output - fixed_overhead - control_reserve - safety_margin`; a negative term raises a typed profile error; unknown tokenizer uses the conservative bound and records `counting: conservative_bound`.
- [ ] Mandatory items are never `reference`; mandatory overflow returns `PackFailure(CONTEXT_BUDGET_EXCEEDED)` naming the item; optional overflow downgrades to `reference` with `budget_exhausted` and `downgraded_to: reference`.
- [ ] Tier assignment covers `expand: inline|reference|materialize|auto` with the `auto_inline_cap` and `auto_materialize_floor` policy defaults and boundary tests at exactly the cap.
- [ ] Every omission reason code (`budget_exhausted`, `trust_filtered`, `expired`, `duplicate`, `not_accepted`, `unsupported_media`, `policy_denied`) has a test producing it.
- [ ] `render_context_index` and `render_inputs_manifest` produce golden files; untrusted items render inside fenced data blocks labelled with source and trust, never as instruction text.
- [ ] `render_prompt_segment` returns one segment of trust class `admitted_input` whose digest equals the selection record's `prompt_plan` digest; `render_workspace_entries` yields `WorkspaceSlotBinding(access="read_only", durable_ref, content_digest)` per `materialize` item.
- [ ] `make check` passes (ruff, mypy, deptry, architecture tests: `domain/context` imports no adapters, no Temporal, no provider SDK).

**Verification:** `uv run pytest tests/unit/context -q`; `make check`; `uv run python -c "from mission_control.domain.context.packet import pack"`

**Notes:** Keep `frozen_input_refs` semantics untouched; this ticket adds nothing to interpreter or preparation services (B2 does). The packer version string is part of the digest input. Reuse the existing `WorkspaceSlotBinding` and `PromptSegment` types rather than inventing new ones; if their fields do not fit, record the gap in the handoff for the integrator instead of editing shared contracts. Policy defaults (`auto_inline_cap=4000` tokens, `auto_materialize_floor=65536` bytes, `frame_excerpt_cap` is SPEC-03's) live in a `ContextPackPolicy` dataclass so SPEC-05 can override them per node later.
