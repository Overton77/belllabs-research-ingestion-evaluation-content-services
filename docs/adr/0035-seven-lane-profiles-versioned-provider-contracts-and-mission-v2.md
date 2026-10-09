---
type: Decision Record
title: "Seven Lane Profiles, versioned provider contracts (describe v2, execution binding v2, approval binding, stream subscription) and mission/v2 beside an untouched mission/v1"
description: "Proposed delta for the multi-provider packet (MP-01/OVE-64): one LaneProfile vocabulary across host_support, frames, lanes and the manifest; four unqualified v2 stub describes for claude_agent_sdk, codex, claude_cloud and codex_cloud; mc.lane_describe.v2 feature evidence; mc.execution_binding.v2 with a discriminated provider binding; mc.approval_binding.v1 and mc.stream_subscription.v1; mission/v2 as a Pydantic subclass tree with v1 parsing and lowering unchanged; migration 0031 as the common SQL slot."
tags: [mission-control, adr, proposed, execution, lanes, authoring]
status: proposed
source: docs/specs/multi-provider-2026-10/ARCHITECTURE.md; MP-01 (OVE-64); docs/qualification/lanes/claude_cloud/FEASIBILITY.md; docs/qualification/lanes/codex_cloud/FEASIBILITY.md; ADR-0018; ADR-0019; ADR-0030; ADR-0034
---

# Seven Lane Profiles, versioned provider contracts and mission/v2 beside an untouched mission/v1

**Status: proposed.** This record describes the contract delta that MP-01 froze so that dependent
tickets can build against one vocabulary. It becomes *accepted* only when the owner authorizes
it; until then ADR-0018/0019/0030/0034 remain the accepted text and nothing here claims that a
new profile is qualified, implemented end to end or account-enabled.

## Context

ADR-0018 made coding agents execution lanes and ADR-0019 placed them worker-hosted, local first.
ADR-0030 split the Cursor lane into `cursor_local` and `cursor_cloud`. The repository then carried
three runtime profiles (`deep_agents`, `cursor_local`, `cursor_cloud`) next to five projection
profiles (adding `claude` and `codex` as host-file targets) with separately maintained enums in
`host_support.py`, `frames/contracts.py`, `lanes.py` and the manifest. The owner's multi-provider
target adds provider-hosted Claude Code and Codex. "Cloud" means the provider-hosted product; an
Anthropic model running through Cursor is `cursor_*`, and our own remote SDK worker is
`claude_agent_sdk`/`codex` with `worker_hosted` placement, never a cloud profile.

## Decision

1. **One vocabulary.** `host_support.LaneProfile` is the single source for the seven profiles
   `deep_agents | cursor_local | cursor_cloud | claude_agent_sdk | codex | claude_cloud | codex_cloud`.
   `frames.contracts.LaneProfile` and the manifest `Lane` alias it; `lanes.py` checks at import
   that `LANE_PROFILES`, `RUNTIME_OF_LANE`, `LANE_OF_PROFILE` and `HOOK_EVENT_MAPPING` agree.
   Lanes are `deep_agents | cursor | claude | codex`; placements are `worker_hosted | cloud`.

2. **Implemented, qualified and account-enabled are three different facts.** A declared describe
   (`DECLARED_LANE_MATRICES`) is the lane's own statement of what it implements; `qualified` flips
   only through a recorded live drill (never from fakes); account enablement is observed at launch
   time and is never baked into a describe. The four new profiles ship as **unqualified v2 stubs**:
   every control and feature is `unqualified`, except cells that the hosted feasibility studies show
   have no vendor surface, which are `unsupported` (`claude_cloud`: `cancel`, `status`,
   `approval_suspension`, `subordinate_lineage`; `codex_cloud`: all delivery semantics,
   `observe`, `follow_up`, `cancel`, `usage`, `approval_suspension`, `subordinate_lineage`,
   `continuation`). `LaneDescribe.unqualified()` preserves `unsupported` cells.

3. **`mc.lane_describe.v2`** extends v1 (whose digest is unchanged) with `features`
   (`FeatureEvidence` per feature: `qualified | unqualified | unsupported` with evidence refs),
   `approval_modes`, `compaction_control`, `subordinate_visibility` and `enforcement_coverage`.
   Requirement admission (`lane_requirements.admit_requirements`) refuses a required feature that
   is `unsupported` (`LANE_REQUIREMENT_UNSUPPORTED`) or merely `unqualified`
   (`LANE_REQUIREMENT_UNQUALIFIED`) with a JSON pointer into the requirement; `workflow_gate` is
   always admitted because the kernel enforces it; a v1 describe proves the basic observations
   through its qualified controls.

4. **`mc.execution_binding.v2`** (`ProviderExecutionBinding`, sealed digest) binds a claude or
   codex profile with model/auth/environment pins, effective permissions, repo commit,
   materialization digest, workflow requirements and policy digest. The existing
   `mc.cursor_binding.v1` stays the Cursor binding. `OperationExecutionRequest` and
   `OperationExecutionBinding` carry at most one of the two and the pairing check requires the
   runtime, profile and binding lane to agree. `mc.environment_binding.v1`
   (`local_workspace | provider_hosted`) and `mc.workspace_snapshot.v1` are the environment and
   snapshot receipts referenced from the binding.

5. **`mc.approval_binding.v1`** extends the Human Task with `origin`
   (`workflow_gate | permission | governed_effect | elicitation`), native correlation, generation,
   tool/input digests, policy digest and deadline; `ApprovalResolutionIntent` is validated against
   its binding (idempotency, edited-input digest rules). Reducers still own acceptance.

6. **`mc.stream_subscription.v1` / `mc.stream_envelope.v1`** describe a browser subscription
   (scope, targets, filters, per-stream cursors, visibility) distinct from durable callback
   registration; the error vocabulary is `STREAM_ERROR_CODES` in the existing upper-snake form.

7. **Hooks vocabulary is derived, not duplicated.** `HOOK_EVENT_MAPPING` covers all seven profiles:
   Claude settings-file hooks (`_CLAUDE`, reused by `claude_cloud`) are distinct from the Python
   SDK callback union (`CLAUDE_SDK_CALLBACK_EVENTS`); Codex maps its twelve documented events
   (`Bash`, `apply_patch`, no post-tool-failure event); `codex_cloud` has no hook surface.

8. **`mission/v2` beside `mission/v1`.** `manifest_v2.py` is a Pydantic subclass tree
   (`EnvironmentV2`, node V2 classes, `MissionManifestV2`) adding hosted provider, workspace mode and
   reuse, dirty-input and cleanup policy, native compaction policy, continuation fallback and
   `requirements`. `parse_manifest_any` dispatches on `manifest:`; unknown versions are
   `UNSUPPORTED_BEHAVIOR` at `/manifest`. The v1 module, its lowering and all previously compiled
   digests are unchanged; the regenerated v1 JSON Schema differs only by the `Lane -> LaneProfile`
   definition name and the two added enum values.

9. **Migration 0031 is the common slot** (`0031_multi_provider_lanes.sql`): it widens the
   lane-profile CHECKs on `lane_profile`, `harness_execution`, `provider_frame`,
   `continuation_transfer` and `search_document`, admits `mc.execution_binding.v2` on claude/codex
   lanes in `execution_binding`, and seeds the four stub rows with `qualified = false`. Rows stay
   immutable; qualification is a later release.

10. **Command semantics are per profile.** `LANE_COMMAND_SEMANTICS` (cancel, interrupt_and_inject)
    joins `LANE_PAUSE_SEMANTICS`/`LANE_RESUME_SEMANTICS` as per-profile tables so the workflow's
    receipts and every declared describe say the same thing; hosted profiles without a vendor
    cancel operation report `unsupported` while the kernel still fences the unit on its side.

## Consequences

- Dependent tickets (MP-02..MP-15, MP-18/19) import these symbols; they do not edit shared enums.
- No new profile is dispatchable in production: `describe_only_registry` admits the stubs only
  with `allow_unqualified`, and admission refuses workflows that require unsupported or
  unqualified features. Partial local parity is never reported as all-provider parity.
- Hosted Claude Code and Codex stay at Outcome 3 (unqualified) until the vendor operations listed
  in their feasibility studies exist; MP-18/19 are evidence gates, not implementation tickets.
- Reconciliation with the sibling runtime annex: nothing here changes ADR-0017 persistence policy,
  ADR-0018 lane semantics or ADR-0019 placement; it adds profiles and versioned contracts beside
  them. Any conflict found later is reported, not patched silently.
