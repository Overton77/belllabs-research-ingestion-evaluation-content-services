---
type: Verification Reference
title: Local and Cursor workflow parity evidence (MP-20, 2026-10-09)
description: Per-profile evidence matrix for Stage Graph, GoalDirected and two-member Mission Chain parity through the production compile/submit/start path on real PostgreSQL and Temporal, with fixture provider clients only, the unsupported features and their typed refusals, usage dispositions, the gaps found and what remains blocked. Nothing here qualifies a profile.
tags: [mission-control, qualification, parity, providers, mp-20]
---

# Local and Cursor workflow parity — MP-20 evidence (2026-10-09)

MP-20 / OVE-83. **No profile is qualified by this record.** Every result below is offline (G1),
real-PostgreSQL (G2) or real-Temporal (G3) evidence with deterministic FIXTURE provider clients;
`qualified` stays `false` on every lane profile. The live level (G4) is **BLOCKED** for every
profile: it needs the owner-run, budgeted drills (`make lane-qualify PROFILE=<profile> LIVE=1`,
see [lane qualification](../lanes/README.md)). Hosted Claude/Codex parity is MP-21 and is not
claimed here.

## Environment and versions

| Item | Value |
| --- | --- |
| Checkout | `mp/integration-recovery-2026-10-09` (base `efa55f9`, uncommitted integrator state) |
| Python | 3.12.14 |
| PostgreSQL | disposable PostgreSQL 17 (`127.0.0.1:55433`, `with-test-db.sh`), common component release 1.2.0 (`sha256:0113df03…`) installed fresh per test |
| Temporal | the production-stack fixture's dev server (Temporal CLI 1.9.1, Server 1.32.0) for the MP-20 suite; `127.0.0.1:7233` for the MP-06/07/08/10/12 drills |
| Claude lane | `claude_agent_sdk` 0.2.165 / bundled Claude Code 2.1.294 (FIXTURE SDK client; no process spawned) |
| Codex lane | `codex-cli` 0.162.0, app-server protocol v2, schema `sha256:0bf5254b…` (FIXTURE app-server; no process spawned) |
| Cursor lanes | `cursor-sdk` 1.0.37 pins, Cloud Agents API v1 (FIXTURE responder bridge for `cursor_local`, FIXTURE responder Cloud API behind `httpx.MockTransport` for `cursor_cloud`; real `CursorLocalHarness`/`CursorCloudHarness`) |
| Deep Agents | `deepagents` 0.7.5 (FIXTURE parent chat model; real runtime, tools, workspace) |

Paid calls: none. Provider logins: none. Usage of every fixture turn is synthetic.

## The production path under test

`tests/integration/temporal/test_mp20_workflow_parity.py` (fixtures:
`tests/fixtures/mp20_parity.py`) compiles, submits and starts mission/v2 manifests through the
public `missions:submit` / `missions:start` router and the production `ManifestLaunchInputAuthor`
over a `mc.manifest_launch_bindings.v2` file, runs them through the production StageGraph /
GoalDirected families, `OperationWorkflow`, the production operation boundary and the
**deployment's own `OperationExecutionActivities` and `LaneTurnService`** (mailbox, injections,
Stop Fence, MP-12 continuation coordinator, frame facts), the production workspace-candidate
capture service, chain release hook and relay pump. Only the provider seam is a fixture
(Cursor: `ResponderBridgeLauncher` and `ResponderCloudApi`, with the production
`RenderedProjectionSource(CatalogRows(...))` re-rendering and verifying each sealed
`mc.cursor_binding.v1` at `prepare`; the Cursor target repository is a tmp git repository and a
tmp bare remote that the deployment file's `repositories` map the manifest repository to):
`ParityModel` (Deep Agents chat model), `ResponderClient` (Claude SDK client), `ResponderLauncher`
(Codex app-server). A scripted provider writes its declared output into the lease's `outputs/`
and answers with one JSON object (the lane Completion Candidate). Receipts and records are
compared (settlements, accepted obligation/output evidence, workspace-candidate descriptors and
digests, lane-turn task queues, mailbox receipts, Human Task resolutions), not final text.

The first pass used two test-local stand-ins (the context-packer binding-name normalization and
the duplicate Human Gate wait). Both deltas are now in production (gaps 2 and 3), and the
2026-10-10 rerun uses no stand-ins.

## Evidence matrix

Levels: **O** offline/unit (G1), **D** real PostgreSQL (G2), **T** real Temporal (G3; the MP-20
rows also run the full production path on real PostgreSQL), **L** live drill (G4, BLOCKED for all).
"xfail" is a strict expected failure documenting a found defect.

| Scenario | `deep_agents` | `claude_agent_sdk` | `codex` | `cursor_local` | `cursor_cloud` |
| --- | --- | --- | --- | --- | --- |
| V01 unsupported control refused at submit (MP-20) | T pass (`hard_pause`) | T pass (`hard_pause`) | T pass (`hard_pause`) | T pass (`pause`, `hard_pause`) | T pass (`pause`, `hard_pause`) |
| V02 compile/submit/start via the production author | T pass | T pass | T pass | **T pass** (sealed `cursor_binding`, lane queue) | **T pass** (sealed `cursor_binding`, lane queue) |
| V04 queued instruction, applied once at the declared boundary | T (FT-F1, `test_command_mailbox`) | **T pass** (MP-20, production path; receipts accepted→queued→delivered→observed→applied, `turn_boundary_guaranteed`) | **T pass** (MP-20; `wait_then_send`) | O (FT-F1/G4 unit suites) | O |
| V05 interrupt/steer racing turn completion | T (`test_ft_f2_inject`) | O (`tests/unit/claude/test_claude_controls.py`: cancel-and-replace drains the interrupted turn, replacement reads only its own response) | O (`tests/unit/codex/test_harness.py`: `turn/steer` and stale-target handling) | O (FT-F2/FT-G unit) | O |
| V06 cancel reaches the provider turn | T (`test_ft_f3_immediate_cancel`) | **T pass** (MP-20: interrupt, settled `cancelled`, dependent stage never ran, no reservation open) | **T pass** (MP-20, same) | O (FT-G unit) | O (`tests/unit/cursor`) |
| V07 process lost mid-turn, reattached without a duplicate turn | — | O (`tests/unit/claude/test_claude_harness.py` process-dies script) | T (`test_mp08_codex_lane_turns`: app-server dies mid-turn, reattached by the next segment) | O (MP-09 lease takeover, `tests/unit/cursor`) | O |
| V08 observation resumes from durable cursor | T (`test_lane_turn`) | T (`test_mp07_claude_lane_turns`) | T (`test_mp08_codex_lane_turns`) | D/T (`test_cursor_local_fixtures`, `test_lane_turn`) | T (`test_mp09_cloud_segment_loop_real_services`) |
| V09 Human Gate approve between provider stages | T (MP-10 family drill) | **T pass** (MP-20: produce on Claude, gate, consume on Codex; one attributed resolution, replay = `duplicate`, second decision refused) | (consumer in the same run) | — | — |
| V09 deny | T (MP-10 family drill) | **T pass**: the denied gate fails the run (gap 4, fixed) | — | — | — |
| V09 request_changes on a manifest Stage Graph gate | **typed refusal** (`HumanTaskRejected`; no remediation route is lowered) | typed refusal (MP-20) | — | — | — |
| V10 governed MCP fallback | refused at compile (`governed_effect` unsupported) | D (`test_mp11_governed_postgres`, lane-neutral service) | D (same) | refused at compile (`governed_effect` unsupported) | refused at compile (`governed_effect` unsupported; cloud `mcp` enforcement unsupported) |
| V11 native approval through the broker | not applicable (refused `provider_permission`) | D (`test_claude_broker_postgres`) | D (`test_codex_approvals_postgres`) | **typed refusal** (`provider_permission` unsupported) | **typed refusal** |
| V12 Goal Loop, two iterations, counters intact (MP-20) | **T pass** | **T pass** | **T pass** | **T pass** | **T pass** (production `GitBranchPublisher`, gap 13 fixed) |
| V12 mixed: executor Claude, independent verifier Codex | — | **T pass** | **T pass** | — | — |
| V13 continuation, worker lost at every phase | n/a (checkpoint rollover, not a session transfer) | **T pass** (`test_claude_continuation_temporal`, after the double-send fix, gap 5); O pass (`tests/unit/claude/test_claude_continuation.py`) | **T pass** (`test_codex_continuation_temporal`) | O (hydrated handover) | O |
| V14 accepted stage output feeds the next stage on another provider (MP-20) | **T pass** (producer → Claude; consumer ← Codex) | **T pass** (producer → Codex; consumer ← Deep Agents; consumer ← `cursor_cloud`) | **T pass** (producer → Deep Agents; consumer ← Claude) and (producer → `cursor_local`) | **T pass** (producer → `cursor_cloud`; consumer ← Codex) | **T pass** (consumer ← `cursor_local`; producer → Claude, under a Deep Agents mission environment, gap 14) |
| V15 two linked Goal Loops, replayed release starts the consumer once (MP-20) | **T pass** (consumer of Claude) | **T pass** (supplier → Deep Agents; consumer of Codex) | **T pass** (supplier → Claude) | **T pass** (supplier → `cursor_cloud`) | **T pass** (consumer of `cursor_local`) |
| Lane-neutral drills (MP-06/MP-12 fixture lanes, real Temporal + PG) | V06 cancel with Stop Fence during `cancel_and_replace`, V07 worker lost after the native send (one native turn), V12 counters intact across a continuation and Continue-As-New, V13 one activated target after a loss at every phase (`test_mp06_dispatch_recovery`, `test_mp12_continuation`) | | | | |
| V16–V19 lineage, socket replay, auth, callbacks | not exercised by MP-20 (realtime suites, MP-13/14/15) | | | | |
| Live drill (G4) | BLOCKED (owner) | BLOCKED (owner) | BLOCKED (owner) | BLOCKED (owner, OVE-55) | BLOCKED (owner, OVE-55) |

What the V14 rows assert: the run completes on the consumer's typed obligation evidence; every
accepted output is a registered `workspace-candidate://` ref whose descriptor digest equals the
bytes the provider wrote; each Session Lane unit ran `lane.turn` on its own binding's task queue;
the consumer received the producer's accepted bytes as a materialized input whose digest matches
`inputs.json` (read-only by `chmod 0444` on the POSIX worker hosts; this Windows host skips the
chmod by design); neither the producer's lease path nor any credential marker appears in the
consumer's context index or input manifest; every unit's settlement carries its Completion
Candidate (see "Usage dispositions" for what the settlement charges).

What the V12 rows assert: executor/verifier units in order (iteration 1 rejected, iteration 2
accepted), the accepted output is the second executor's registered candidate, all reservations
released, at least four usage settlements. V15: the supplier completes, the chain releases the
consumer through the production relay, a redelivered start intent leaves exactly one root
execution, the consumer completes with its own budget account and saw the supplier's accepted
output (digest) in its first packet.

## Unsupported features and their typed refusal (from each declared matrix)

| Profile | Controls refused at compile (`UNSUPPORTED_BEHAVIOR`/`LANE_REQUIREMENT_UNSUPPORTED`) | Approvals refused | Observation refused | Launch |
| --- | --- | --- | --- | --- |
| `deep_agents` | `hard_pause` | `provider_permission`, `provider_question`, `mcp_elicitation`, `governed_effect` | `subordinate_lifecycle`, `compaction` | production author |
| `claude_agent_sdk` | `hard_pause` | `provider_question`, `mcp_elicitation` | — | production author (`provider_binding`) |
| `codex` | `hard_pause` | — | — | production author (`provider_binding`) |
| `cursor_local` | `pause`, `hard_pause` | `provider_permission`, `provider_question`, `mcp_elicitation`, `governed_effect` | `subordinate_lifecycle` | production author (`cursor_binding`); hook scripts, plugins, executors and model settings refused at their pointer |
| `cursor_cloud` | `pause`, `hard_pause` | `provider_permission`, `provider_question`, `mcp_elicitation`, `governed_effect` | `subordinate_lifecycle` | production author (`cursor_binding`; v1 nodes need the reviewed `v1_environment`); same refusals |

The control refusals are asserted per profile through `missions:submit` (V01 rows); the approval
and observation refusals are computed from `declared_matrix` with `admit_requirements` and are
covered offline by `tests/unit/authoring/test_manifest_v2_launch.py`.

## Usage dispositions

| Profile | Tokens (declared) | Cost (declared) | Evidence |
| --- | --- | --- | --- |
| `deep_agents` | per model call (callback usage) | not priced | unit/acceptance suites |
| `claude_agent_sdk` | `settled_per_turn` | `estimated` (upgraded only when the provider reports cost) | `tests/unit/claude` usage tests; MP-20 settlements |
| `codex` | `settled_per_turn` | `estimated` | `tests/unit/codex` usage tests; MP-20 settlements |
| `cursor_local` | `settled_per_turn` | `estimated_then_settled` (`get_usage` account-gated) | unit suites (`feature_unavailable` keeps `estimated`) |
| `cursor_cloud` | `settled_per_turn` | `estimated_then_settled` | unit suites (`test_cloud_usage_unknown`: unknown is never zero) |

Observed in the MP-20 runs (parity, but a finding): every manifest Stage Graph unit, on every
lane, settles **empty** public usage amounts. The family admits a stage unit with
`budget_limits` equal to its reservation (`operation.attempts`, `concurrency.slots`), so the
run's bounded `tokens.total` is never charged by a stage unit; a Session Lane keeps the observed
usage only in its closing facts (settlement event payload) and lane state; nothing is recorded
against the run's token budget for Stage Graph work on any lane. A
subscription-backed turn still consumes the run's resource budget; no usage was paid.

## Gaps found by MP-20

1. **Fixed (this ticket): a Session Lane's outputs never reached the families.** Lane settlement
   carried no structured output, so a Stage Graph provider stage could never accept a typed
   obligation and a GoalDirected executor/verifier on any Session Lane failed
   (`structured_output=None`); lanes staged `outputs/` as opaque payload locators that neither the
   Context Packer nor the chain release resolve (Claude registered none at all). Now the final
   answer, when it is one JSON object, is the Completion Candidate (the Deep Agents rule), with
   `output_refs` reconciled to what the attempt registered, and the lanes register declared outputs
   as workspace candidates of the exact operation binding through `WorkspaceCandidateLaneOutputs`.
   The integrator wired the port into all four Session Lane compositions (2026-10-10).
2. **Fixed (integrator, 2026-10-10; `domain/context/packet.packet_binding_name`):** every manifest Stage Graph dependency's consumer
   slot is `from:<producer>` (`domain/authoring/stagegraph_builder.py`), which the packer uses as
   a `ContextBinding.binding_name` (no `:` allowed): the first dependent stage of any manifest
   Stage Graph fails admission on every lane, Deep Agents included.
3. **Fixed (integrator; `StageGraphLaunchService.prepare` marks a gate stage's same-id wait satisfied):** the manifest lowering still emits a pre-MP-10
   `StageGraphWait` per `human_gate` node; the family holds the gate stage on that run-level wait
   forever and the MP-10 gate child never starts.
4. **Fixed:** a denied manifest Human Gate used to fail the Stage Graph family ("no admissible
   work and no terminal completion proposal"); the run now concludes `failed` (interpreter
   `failure_completion`, patch `mp20-stagegraph-concluded-failure`; linked runs record the child
   as failed behind `mp20-linked-child-concluded-failed`). V09 deny passes.
5. **Fixed:** the Claude continuation turn was sent twice to the fresh target (the harness kept the
   adopted target as a pending handover); `test_claude_continuation_temporal.py` passes both cases.
6. **Fixed (Cursor launch author):** `ManifestLaunchInputAuthor` seals an exact
   `mc.cursor_binding.v1` for `cursor_local`/`cursor_cloud` stages and Goal Loop roles (mission/v1
   and mission/v2) from `providers.cursor_*` of `mc.manifest_launch_bindings.v2`
   (`application/authoring/cursor_launch.py`), pinning the rule/agents/hooks digests the harness
   re-renders at `prepare` (`tests/unit/authoring/test_manifest_cursor_launch.py`, 22 passed).
   The former typed-refusal test is replaced by the Cursor V14/V12/V15 rows.
7. **Fixed** (authored names and expand modes travel as `StageAuthoredInput`, no digest change). Before:
   authored `inputs[].expand` was not honoured: the packer binding is always `auto` (a small
   accepted output is inlined, a large one materialized). The V14 handoff uses a 70 KB draft to
   exercise materialization.
8. **Fixed** (slots stay under the role root; packet text and `packet_files` map the unit root to the
   lease root on every Session Lane). Before: Goal Loop packets mount under the role root (`goal/<n>/<role>/.mission/`), while the lane
   operating contract points at `.mission/context.md` at the lease root (the index is also inline
   in the prompt, which the fixture follows).
9. **Fixed for the text bound:** the Completion Candidate is read from the lane's full final text
   (`FinalTextLane.final_text` on Claude, Codex and Cursor; ≤262,144 characters; an excerpt only
   when provably whole). Still open: The lanes do not
   yet project the GoalDirected output schema to the agent or use native structured output
   (Claude `output_format`, Codex `outputSchema`).
10. Stage Graph obligation evidence remains the operation's claim (`obligation_refs`), as on Deep
    Agents: no closed completion-contract AST validates `report_note@1` today
    ([completion](../../knowledge/completion.md)).
11. **Fixed:** Session Lane settlements charge their settled/estimated tokens once on reserved or
    run-declared token dimensions (unknown charges nothing). Before: Stage Graph units settled no token usage against the run budget on any lane (their bound
    dimensions are the reservation's attempts and slots); see "Usage dispositions".
12. Under concurrent load two existing drills failed once and passed on an isolated rerun:
    `test_mp06_dispatch_recovery::test_a_stop_fence_during_cancel_and_replace_blocks_the_replacement`
    (Temporal RPC timeout) and
    `test_codex_approvals_postgres::test_a_relaunch_marks_the_dead_connections_correlation_lost`.

13. **Fixed in production `GitBranchPublisher.publish` (2026-10-10; stand-in removed):**
    the run branch is per run (`mc/<run>`) and `GitBranchPublisher.publish` returns an existing
    branch untouched, so every later `cursor_cloud` unit of the same run (Goal Loop verifier and
    later iterations, a second cloud stage) reads the first unit's packet. Strict xfail
    `test_a_later_cloud_unit_of_the_run_publishes_its_own_packet[production]`; the suite uses
    `RunBranchPublisher` (commit the unit's files on top of the run branch; idempotent retry).
    V12 also asserts each provider unit saw its own activation in its packet.
14. **Fixed (`merge_environment_documents` replaces an `execution_environment` of another kind):** a node `execution_environment` overlay of
    another kind (`provider_hosted` over `local_workspace` or the reverse) is deep-merged with the
    parent's selection and raises an unhandled `ValidationError` (HTTP 500 at submit) instead of
    replacing it or returning a pointed blocker. A `cursor_cloud` stage beside a worker-hosted
    stage therefore inherits from a Deep Agents mission environment in V14.
15. Missions 2 and 3 (mission/v1) still cannot launch as authored: their hook scripts, plugin and
    deterministic executors have no slot in `mc.cursor_binding.v1` and are refused at their
    pointers (`/…/hooks/0`, `/mission/environment/capabilities/0`); `CatalogRows` resolves only
    skills, MCP servers and subagents. Mission 2's research member without its hook launches on
    `cursor_cloud` from v1 (unit test). Neither composed rows resolver
    (`CatalogProjectionRows`, `CatalogRows`) is given bundle custody, so a selected Skill fails
    to resolve on both sides (pre-existing, all provider lanes).
16. Test isolation: the production-stack fixture uses a fixed dev-server port (7341) and fixed
    coordinator queues; concurrent runs on the shared host took each other's workflow tasks
    (`ERC not found`, `workflow run not found`, `human_task_missing`). `MP20_TEMPORAL_PORT`
    (opt-in, `tests/fixtures/mp20_parity.py`) gives a run its own dev server. One isolated run
    failed the three Cursor Goal Loop cases once at the iteration-2 executor
    (`workspace identity was reused with different materialization`); two later runs of the same
    selection passed and the failure did not reproduce (a debug trace of the passing runs shows
    only executor units registering outputs). Not explained; recorded, not hidden.

## Status statement

Re-run on 2026-10-10 on production behaviour only (every test-local stand-in removed; isolated
Temporal dev server `MP20_TEMPORAL_PORT=7397`, disposable PostgreSQL 17): **32 passed, 0 xfail**.
Deep Agents, Claude Agent SDK, Codex, Cursor local and Cursor cloud run Stage Graph (cross-provider
handoffs), GoalDirected (two iterations, independent and cross-provider verifier) and a two-member
Mission Chain through the production path at the G2/G3 fixture level, plus V01/V04/V06/V09 (approve
and deny). Open: gaps 9 (structured-output projection), 10, 12, 15, 16. Live qualification is
BLOCKED for all five profiles pending owner drills; the hosted profiles are evidence-blocked. This
is not an "all providers complete" statement.
