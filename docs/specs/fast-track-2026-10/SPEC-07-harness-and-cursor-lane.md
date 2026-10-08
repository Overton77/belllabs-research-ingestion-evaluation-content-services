---
type: Specification
title: "SPEC-07: AgentHarness protocol, Temporal lifecycle synthesis, and the Cursor lane (local and cloud)"
description: "The provider-neutral AgentHarness protocol every lane implements, the lane registry and dispatch, the Temporal activities and workflow loop that synthesize a provider's run into mission lifecycle (segmented observe activity, heartbeat cursors, Update commands, cancellation, seeded forks), and the two Cursor lane profiles: cursor_local on the worker through the Python SDK bridge and cursor_cloud through the Cloud Agents API. Implements ADR-0018, ADR-0019, ADR-0030 and ADR-0031."
tags: [mission-control, spec, fast-track, lanes, cursor, temporal]
---

# SPEC-07: AgentHarness protocol, Temporal lifecycle synthesis, and the Cursor lane

Decisions: [ADR-0018](../../adr/0018-coding-agents-are-execution-lanes.md), [ADR-0019](../../adr/0019-coding-lanes-worker-hosted-local-first.md), [ADR-0030](../../adr/0030-cursor-lane-two-profiles-reducer-from-frames-hydrated-fork.md), [ADR-0031](../../adr/0031-temporal-lifecycle-synthesis-observe-activity-updates-seeded-forks.md). Evidence: [research/cursor-platform.md](research/cursor-platform.md), [research/temporal-lifecycle.md](research/temporal-lifecycle.md), [research/codebase-map.md](research/codebase-map.md) sections 4 and 5, and [docs/research/2026-10-07-coding-lane-surfaces.md](../../research/2026-10-07-coding-lane-surfaces.md). Companion specifications: [SPEC-01](SPEC-01-capabilities-catalog.md) (host projection, hook scripts), [SPEC-02](SPEC-02-context-packet.md) (Context Packet), [SPEC-03](SPEC-03-mission-state-and-transcript.md) (Provider Frame store), [SPEC-06](SPEC-06-interventions-inspection-subscriptions.md) (commands and Delivery Reports).

Pins this specification is written against: `cursor-sdk==1.0.37` (bridge `sdk.v1`, bundled `sdk-bridge` 1.0.37), Cloud Agents API v1 (public beta, OpenAPI `cloud-agents-openapi.yaml` 1.0.0), `temporalio==1.34.0` (repo lock is 1.30.0; the upgrade is ticket FT-G7). Claims the research marked UNVERIFIED are repeated as UNVERIFIED here and each has a qualification test.

## Problem Statement

Mission Control runs exactly one lane today, Deep Agents, through a `RuntimePort` whose only method is `execute`. There is no `AgentHarness` protocol with the specified operations, no lane registry, and `OperationExecutionService` is constructed with a single runtime. The operation activity holds the whole cognition in one `operation.execute` call, heartbeating only for liveness. Commands other than pause, resume, satisfy-wait and normal cancel are rejected. Nothing persists what the provider said during a run.

The owner's second and third missions need Cursor as a lane, on the worker (a repository feature mission) and in the cloud (a research and ingestion chain). Cursor's Python SDK offers no system prompt, no steer, no pause and no fork, hooks are files, headless local runs auto-approve every tool call, and the cloud API has one active run per agent and no webhooks. The kernel must therefore synthesize a mission lifecycle from provider frames, inject everything through the workspace, and be honest about what each control actually does.

## Solution

1. A provider-neutral `AgentHarness` Protocol with ten operations and typed handles, a `LaneRegistry` keyed by lane profile, and dispatch in `OperationExecutionService` by `execution_runtime` (`native | deep_agent | cursor`). The existing `DeepAgentRuntimeAdapter` is wrapped to conform without changing its behavior.
2. One lifecycle synthesis in Temporal for every lane: a segmented `lane.turn` activity that streams and persists Provider Frames, heartbeats the provider cursor, and returns closing facts; a `lane.status` reconciler; an idempotent `lane.cancel`; Updates for commands with per-run dedupe carried across continue-as-new; forks as new runs seeded from snapshots; search attributes for listing.
3. Two Cursor Lane Profiles sharing one adapter package: `cursor_local` (bridge subprocess on the worker, leased git worktree, file projections, fail-closed Kernel Hooks calling the worker back) and `cursor_cloud` (REST v1 plus SDK cloud runtime, repository branch `mc/<run_id>` carrying the projections, SSE stream with `Last-Event-ID`, artifacts, usage settlement).
4. An honest `describe` per profile; emulated fork and continuation by hydrating a fresh agent from a Context Packet plus a git patch or branch; qualification fixtures recorded from real runs and replayed in CI with zero paid budget by default.

## User Stories

1. As a coordinator, I want to pick `lane: cursor_local` or `lane: cursor_cloud` in a Mission Manifest, so that a Goal Loop runs on Cursor with the same mission contracts as Deep Agents.
2. As a coordinator, I want `missionctl lane describe cursor_local` to tell me which controls are native, emulated or unsupported, so that I never assume a pause or steer that the lane cannot deliver.
3. As an operator, I want a worker crash during a Cursor run to resume observation from the last persisted frame without re-sending the turn, so that no paid agent work is duplicated.
4. As an operator, I want `cancel` to reach a running Cursor agent through the provider's own cancel and to settle usage and effects, so that cancellation is a fact, not a hope.
5. As an operator, I want an immediate cancel to stop new shell and MCP side effects even when the provider has not yet acknowledged, so that the Stop Fence holds on Cursor as it does on Deep Agents.
6. As a mission author, I want my repository's `.cursor/rules`, `.cursor/skills`, `.cursor/agents` and `.cursor/hooks.json` written from catalog capabilities, so that the agent's environment is pinned and reproducible.
7. As a mission author, I want a `verifier` Subagent Profile projected to `.cursor/agents/verifier.md` with `readonly: true`, so that the main agent can delegate verification without write access.
8. As a mission author, I want the Context Packet placed under `.mission/` and `/inputs/` before the first send, so that the agent reads its brief and inputs from disk on every lane.
9. As a reviewer, I want every Cursor SDK message and SSE event persisted as a Provider Frame, so that `missionctl run transcript` shows what the agent did.
10. As a reviewer, I want the reducer to derive Attempt phase, tool effects, usage and terminal outcome only from closing frames, so that partial deltas never move mission state.
11. As a mission author, I want `queue_instruction` to deliver at the next Cursor send with `wait_then_send` semantics, so that follow-up guidance reaches the agent at a safe boundary.
12. As a mission author, I want `interrupt_and_inject` on Cursor to cancel the run, settle uncertain effects, and start a replacement turn with the injected context, reported as `cancel_and_replace`.
13. As a mission author, I want `fork` on Cursor to create a new agent in a fresh workspace hydrated from the packet plus the source workspace's patch, so that I can branch a coding mission without cloning in-flight commands.
14. As an operator, I want cloud agents created idempotently with a client-supplied `agentId`, so that a lost HTTP response never creates a second paid agent.
15. As an operator, I want the cloud SSE stream resumed with `Last-Event-ID` and, after `410 stream_expired`, the run read from `GET .../runs/{runId}`, so that observation survives retention windows.
16. As an operator, I want cloud agents to work on a pre-created branch `mc/<run_id>` that already carries the projections, so that rules, hooks and subagents load in the cloud VM.
17. As a budget owner, I want Cursor usage recorded as `estimated` from stream frames and `settled` from `get_usage`, so that budgets never count unknown usage as zero.
18. As a budget owner, I want qualification tests to run on recorded fixtures by default and live calls to require a finite approved budget, so that CI never spends money.
19. As a platform engineer, I want `lane.turn`, `lane.status` and `lane.cancel` activities shared by every lane, so that a new lane adds an adapter, not a workflow.
20. As a platform engineer, I want Update handlers to reject duplicate command ids within and across continue-as-new, so that a retried relay never applies a command twice.
21. As a platform engineer, I want `temporalio` upgraded to 1.34 with the payload-limit option on `Client.connect`, so that the lane activities use current cancellation details and visibility APIs.
22. As an operator, I want `missionctl run list --query "mc_lane='cursor_local' AND mc_phase='executing'"` to work through Temporal search attributes, so that I can find live Cursor runs without a ledger scan.
23. As a security reviewer, I want hook callbacks authenticated with a task-scoped token bound to attempt and generation, so that a stale or foreign hook cannot claim an effect.
24. As a security reviewer, I want no provider credential, bucket credential or database credential to appear in the workspace, prompt, frames or transcript, so that materialization never leaks secrets.
25. As a Windows operator, I want the lane to state whether the Cursor sandbox is available on the worker host and refuse `sandbox_options.enabled=True` where it is not, so that unsupported configurations fail at prepare, not mid-run.
26. As a coordinator, I want a Cursor local run that ends without a Completion Candidate to get one follow-up turn asking only for declared outputs, so that `missing_output_policy` behaves identically to Deep Agents.
27. As a reviewer, I want cloud `git.branches[]` recorded per run with the branch and PR URL and the diff fetched from the SCM, so that code results are artifacts with provenance.

## Implementation Decisions

### 1. The AgentHarness protocol

A Harness is the protocol; a Lane is a qualified implementation; a Lane Profile is one placement of a lane. The protocol lives in `application/execution/harness/protocol.py` and carries no provider type.

```python
class AgentHarness(Protocol):
    def describe(self) -> LaneDescribe: ...                                   # mc.lane_describe.v1, pure
    async def prepare(self, req: PrepareRequest) -> PreparedSession: ...      # workspace + projections + packet
    async def start(self, req: StartRequest) -> SessionHandle: ...            # native session (agent, thread)
    async def reattach(self, req: ReattachRequest) -> SessionHandle: ...      # by native identity, no new turn
    async def send_turn(self, req: SendTurnRequest) -> TurnHandle: ...        # one Session Turn
    async def cancel_turn(self, req: CancelTurnRequest) -> CancelReceipt: ... # native cancel, idempotent
    def observe(self, req: ObserveRequest) -> AsyncIterator[ProviderFrame]: ... # resumable by cursor
    async def snapshot(self, req: SnapshotRequest) -> SnapshotManifest: ...   # emulated unless native
    async def usage(self, req: UsageRequest) -> UsageReport: ...              # settled | estimated | unknown
    async def end_session(self, req: EndSessionRequest) -> CleanupReceipt: ...
```

Every request carries `scope` (installation, application, tenant, actor), `binding_digest`, `idempotency_key`, `generation`, `lease` (fenced lease id and expiry) and `deadline`. Every handle carries native identity (`native_session_ref`, `native_turn_ref`), `generation`, `lane_profile`, and no secret. `observe` yields `ProviderFrame` values (SPEC-03 `mc.provider_frame.v1`) and accepts an opaque `cursor` string whose meaning is lane-specific (bridge offset, SSE id, LangGraph checkpoint id). Unsupported operations raise `HarnessUnsupported(operation, lane_profile, reason)`; a `describe` that says `unsupported` and an implementation that raises anything else is a conformance failure.

### 2. `mc.lane_describe.v1`

```json
{
  "schema_version": "mc.lane_describe.v1",
  "lane": "cursor", "lane_profile": "cursor_local",
  "versions": {"cursor_sdk": "1.0.37", "bridge": "1.0.37", "protocol": "sdk.v1"},
  "controls": {
    "prepare": "native", "start": "native", "reattach": "emulated", "send_turn": "native",
    "cancel_turn": "native", "observe": "native", "snapshot": "emulated", "usage": "native",
    "end_session": "native", "pause": "unsupported", "fork": "emulated"
  },
  "delivery_semantics": {
    "queue_instruction": "wait_then_send", "interrupt_and_inject": "cancel_and_replace",
    "pause": "unsupported", "hard_pause": "unsupported", "resume": "wait_then_send",
    "cancel": "turn_boundary_guaranteed", "fork": "emulated", "request_continuation": "emulated"
  },
  "identity": {"session_ref": "agent_id", "turn_ref": "run_id", "effect_ref": "call_id", "cursor": "bridge_offset"},
  "hooks": {"mechanism": "command_hooks", "events_supported": ["session_start", "before_tool", "after_tool", "after_tool_failure", "before_shell", "after_shell", "after_file_edit", "before_prompt", "before_compaction", "subagent_start", "subagent_stop", "stop", "session_end"], "fail_closed": true},
  "instruction_channel": ["AGENTS.md", ".cursor/rules/mc-mission.mdc", "prompt_prefix"],
  "subagents": {"file": ".cursor/agents/*.md", "inline": "AgentOptions.agents", "readonly_supported_inline": false},
  "usage": {"tokens": "settled_per_turn", "cost": "estimated_then_settled"},
  "placement": "worker_hosted",
  "qualified": false
}
```

Values of `controls` are `native | emulated | unsupported | unqualified`; `delivery_semantics` values are the glossary's `turn_boundary_guaranteed | cooperative_inject | cancel_and_replace | wait_then_send | pause_at_tool_gate | emulated | unsupported`. `qualified` flips to true only by a recorded qualification (section 12). `describe` is pure and cheap; the registry caches it and the Validation Report (SPEC-05) and admission read it. The three profiles' matrices are the lane matrix in [00-ARCHITECTURE.md](00-ARCHITECTURE.md); the `cursor_cloud` entry differs from `cursor_local` in `reattach: native`, `hooks.events_supported` (no `session_start`, `session_end`, MCP hooks), `identity.cursor: sse_event_id`, `placement: cloud`, and `instruction_channel` (no `prompt_prefix` dependence on `sessionStart`).

### 3. Lane registry and dispatch

`application/execution/harness/registry.py::LaneRegistry` maps `lane_profile -> AgentHarness` and is built once per worker in `deployment_composition.py` from the application binding: `deep_agents` always; `cursor_local` when `CURSOR_API_KEY` is bound and the bridge binary is present; `cursor_cloud` when `CURSOR_API_KEY` is bound. A profile that is configured but not qualified registers with `describe().qualified=False` and admission refuses it unless the application policy allows unqualified lanes (local proof only).

`OperationExecutionRequest.execution_runtime` becomes `Literal["native", "deep_agent", "cursor"]` and gains `lane_profile: str | None` and `cursor_binding: CursorExecutionBinding | None`; the existing validator rule (exactly one binding matches the runtime) extends to the new pair. `OperationExecutionService.execute` resolves `registry.for_profile(request.lane_profile or "deep_agents")` and routes to `lane.turn` instead of calling `runtime.execute` directly; the Deep Agents path keeps lineage classification (`checkpoint_plan`) because its `AgentHarness` wrapper (`DeepAgentsHarness`) performs classification in `prepare`. The `RuntimePort`, `CancellableRuntimePort`, `SandboxPort`, `CapabilityAssetPort` and `MCPRuntimePort` protocols stay; `DeepAgentsHarness` adapts them, and the Cursor harness implements `CapabilityAssetPort.verify` and `MCPRuntimePort.verify_servers` through the same `PinnedCapabilityAssetVerifier`.

### 4. Temporal lifecycle synthesis

#### 4.1 Activities

Three activities in `adapters/temporal/activities/lane_turn.py`, registered on the `-agent-cognitive` queue beside `operation.execute` (which remains for one release as the Deep Agents fallback and is removed by FT-G6):

| Activity | Input | Output | Timeouts |
| --- | --- | --- | --- |
| `lane.turn` | `LaneTurnRequest{scope, run_id, operation_id, activation_id, attempt_no, generation, lane_profile, binding_digest, harness_execution_id, phase: start|resume, cursor?, packet_ref, instruction_ref?, segment: {max_duration_s, max_frames}}` | `LaneTurnResult{done, cursor, frames_persisted, closing_facts?: ClosingFacts, native: {session_ref, turn_ref}, usage_estimate}` | `start_to_close` 30 to 60 min (segment bound), `heartbeat_timeout` 30 s, retry policy: infrastructure classes only, `cancellation_type=WAIT_CANCELLATION_COMPLETED` |
| `lane.status` | `{harness_execution_id, native refs}` | `{status, terminal, usage?}` | `start_to_close` 60 s, short retries |
| `lane.cancel` | `{harness_execution_id, native refs, reason, urgency}` | `CancelReceipt{acknowledged, already_terminal, native_status}` | `start_to_close` 120 s, idempotent, retries allowed |

`lane.turn` behavior, in order:

1. Load the harness execution row; if `phase == resume`, read the persisted frames' last `arrival_ordinal` and provider key for this generation and take the larger of that and the heartbeat `cursor` (heartbeats are throttled, so the persisted frames are the truth and the cursor is a hint).
2. `phase == start`: `prepare` (idempotent on `harness_execution_id`), `start`, `send_turn`. `phase == resume`: `reattach`, then `observe(after=cursor)`. A resume never calls `send_turn`; if the native turn was lost (local bridge gone), return `closing_facts.outcome = "in_doubt"` and let the workflow run `lane.status` and the reconciliation path.
3. For each frame from `observe`: persist it through the `FrameSink` (SPEC-03) first, dedupe by `(harness_execution_id, generation, provider_key)`, then `activity.heartbeat(cursor, frames_persisted)`. Deltas are persisted but never interpreted here.
4. Stop the segment when the provider reports a terminal state (return `done=True` with `closing_facts`), when `segment.max_duration_s` or `max_frames` is reached (return `done=False, cursor`), or when the activity is cancelled (section 4.3).
5. `closing_facts` is the only input the reducer reads: `{native_status, result_text_ref, output_refs, usage{tokens settled, cost disposition}, git?, error?, duration_ms, model}`. Text bodies go to artifacts or frame excerpts, never into the activity result.

Activity sketch (shape only):

```python
@activity.defn(name="lane.turn")
async def lane_turn(req: LaneTurnRequest) -> LaneTurnResult:
    harness = registry.for_profile(req.lane_profile)
    cursor = frames.last_cursor(req.harness_execution_id, req.generation) or req.cursor
    handle = await (harness.start_then_send(req) if req.phase == "start" else harness.reattach(req))
    persisted = 0
    try:
        async for frame in harness.observe(ObserveRequest(handle, after=cursor, deadline=req.segment_deadline())):
            if await frames.persist(frame):          # False on duplicate provider_key
                persisted += 1
            cursor = frame.cursor
            activity.heartbeat(cursor, persisted)
            if frame.terminal:
                return LaneTurnResult(done=True, cursor=cursor, closing_facts=harness.closing_facts(handle, frame))
        return LaneTurnResult(done=False, cursor=cursor, frames_persisted=persisted)
    except asyncio.CancelledError:
        details = activity.cancellation_details()
        if details and details.cancel_requested:     # a real cancel, not worker shutdown, pause or reset
            await harness.cancel_turn(CancelTurnRequest(handle, reason="command"))
        raise
```

#### 4.2 Operation workflow loop

`mc.operation.v1` keeps its identity and queries; its body becomes a segment loop. Each segment is one `lane.turn`; between segments the workflow applies queued boundary commands (SPEC-06 mailbox for `next_turn`) and checks history size.

```python
@workflow.run
async def run(self, inp: OperationInput) -> OperationOutput:
    seen = set(inp.seen_cmds); cursor = inp.cursor; phase = inp.phase
    while True:
        if self.cancel_requested:
            return await self._cancel_and_settle(inp, cursor)
        handle = workflow.start_activity(lane_turn, LaneTurnRequest(..., phase=phase, cursor=cursor),
            start_to_close_timeout=timedelta(minutes=45), heartbeat_timeout=timedelta(seconds=30),
            cancellation_type=ActivityCancellationType.WAIT_CANCELLATION_COMPLETED, retry_policy=INFRA_ONLY)
        self._turn = handle
        result = await handle
        cursor, phase = result.cursor, "resume"
        if result.done:
            return await self._settle(result.closing_facts)          # reducer action via activity
        await self._deliver_mailbox_at_boundary()                     # next_turn instructions, if any
        if workflow.info().is_continue_as_new_suggested():
            await workflow.wait_condition(workflow.all_handlers_finished)
            workflow.continue_as_new(OperationInput(..., cursor=cursor, phase=phase, seen_cmds=sorted(seen)))
```

#### 4.3 Cancellation path

1. A `cancel` or `interrupt_and_inject` command arrives as an Update whose validator rejects an id already in `seen_cmds` and rejects a terminal unit; the handler records the command and sets `cancel_requested` (and `replacement_ref` for inject). Handlers mutate state only; the loop does the work.
2. The loop calls `self._turn.cancel()`; because the activity started with `WAIT_CANCELLATION_COMPLETED`, the workflow waits for the activity's cleanup (provider cancel attempted when `cancel_requested` is true).
3. The workflow then runs `lane.cancel` (idempotent, its own retries) because the activity may have been between retries when the cancel arrived, then `lane.status` until the provider is terminal (bounded poll with `workflow.sleep` backoff; after the bound the unit is `in_doubt`).
4. Usage and effects settle through the existing settlement path; the Delivery Report for the command names what happened (`turn_boundary_guaranteed` for cancel, `cancel_and_replace` for inject with the replacement turn as the next segment).
5. For immediate urgency (SPEC-06, FT-F3) the Stop Fence row is written before step 2 by the command handler's activity, so Kernel Hooks begin denying side effects before the provider acknowledges.

#### 4.4 Commands, dedupe and continue-as-new

Update ids are `command_id`. Server-side Update dedupe is per run, so `seen_cmds` travels in the continue-as-new input and validators check it. `all_handlers_finished` is awaited before any `continue_as_new` or completion. A `queue_instruction` targeting `next_turn` is consumed between segments; the mailbox stays in PostgreSQL (SPEC-06) and the workflow only reads a boundary token, so a continue-as-new never loses an instruction.

#### 4.5 Visibility

The existing `BELLLABS_SEARCH_ATTRIBUTES` registry (`domain/programs/search_attributes.py`, `adapters/temporal/search_attributes.py`) gains four Keyword attributes: `mc_mission_id`, `mc_run_id`, `mc_lane` (`deep_agents | cursor_local | cursor_cloud`), `mc_phase`. `mc_phase` is upserted at segment boundaries only, never per frame. `ForkedFromRunId` is written at fork start. Registration uses the existing operator-service path; the dev server is started with `--search-attribute` flags in `make temporal-up`.

#### 4.6 temporalio 1.30 to 1.34 (FT-G7)

Breaking: payload limits move from the data converter to `Client.connect(payload_limits=PayloadLimitsConfig(...))` with renamed fields. Adopt `workflow.uuid7()`, `original_execution_run_id`, `WorkflowAlreadyStartedError.first_run_id`. Worker Deployment versioning (`deployment_config`) replaces build-id-only routing and is adopted together with replay tests (`Replayer`) over captured histories in `tests/integration/temporal`. The experimental `workflow.signal_with_start_workflow`, Workflow Streams and the Temporal Deep Agents plugin stay out of the critical path. Family workflow edits that change command structure are guarded with `workflow.patched`.

### 5. Cursor Local lane profile

Package `adapters/cursor/` with `local.py`, `cloud.py`, `frames.py`, `projection.py`, `hooks_callback.py`, `binding.py`.

#### 5.1 Workspace lease and projections (`prepare`)

1. Lease a Workspace: `git worktree add <lease_root>/<run_id>/<attempt>` at the binding's base ref of the target repository (Mission 3) or an empty initialized repository (research missions). The lease row records path, base commit, fenced lease id and expiry.
2. Write Host Projections (SPEC-01 renders them; this lane only places them): `AGENTS.md` (Operating Contract summary and the packet index pointer); `.cursor/rules/mc-mission.mdc` with `alwaysApply: true` (the instruction channel, since the Python SDK has no system prompt); `.cursor/skills/<name>/...` for each pinned Skill Bundle; `.cursor/agents/<name>.md` for each Subagent Profile (file form so `readonly` and `is_background` apply); `.cursor/mcp.json` for pinned MCP Servers (secret values are never written, the bridge receives them through its environment); `.cursor/hooks.json` with Kernel Hooks first and catalog Hook Scripts after.
3. Materialize the Context Packet (SPEC-02): `.mission/context.md`, `.mission/inputs.json`, `.mission/operating-contract.md`, files under `/inputs/<binding>/`, and an empty `/outputs/`.
4. Record the actual projection digests in the Materialization record; a digest mismatch against the binding is `CAPABILITY_DRIFT`.

Kernel Hooks in `.cursor/hooks.json` (all `type: command`, matcher `*`): permission events `preToolUse`, `beforeShellExecution`, `beforeMCPExecution`, `subagentStart` carry `failClosed: true`; observation events `postToolUse`, `postToolUseFailure`, `afterShellExecution`, `afterMCPExecution`, `afterFileEdit`, `sessionStart`, `sessionEnd`, `preCompact`, `stop`, `subagentStop` are fail-open. Each command is `python <lease>/.mission/bin/mc_hook.py <event>`; the script forwards stdin to the worker callback (section 5.5) and writes the response to stdout. Headless SDK local runs approve every tool call on their own, so the permission hooks are where the Stop Fence and Operation Intent are enforced. `sandbox_options.enabled=True` is requested by the Sandbox Profile when the host supports it; where the SDK raises `ConfigurationError` at create, `prepare` fails with `UNSUPPORTED_BEHAVIOR` instead of running without the sandbox. Windows sandbox support is UNVERIFIED and is a qualification gate (section 12).

#### 5.2 Bridge, agent and turn (`start`, `send_turn`)

```python
client = CursorClient.launch_bridge(workspace=lease.path, state_root=lease.path / ".mission/state")
agent = client.agents.create(AgentOptions(
    model=binding.model_id, mode=binding.mode, name=f"mc-{run_id}-{attempt}",
    mcp_servers=binding.inline_mcp or None, agents=binding.inline_subagents or None,
    disallowed_tools=binding.disallowed_tools or None,
    local=LocalAgentOptions(cwd=str(lease.path), setting_sources=["project"],
                            sandbox_options=SandboxOptions(enabled=binding.sandbox_enabled))))
run = agent.send(UserMessage(text=turn_text),
                 SendOptions(idempotency_key=f"{harness_execution_id}:{generation}:{turn_no}"))
```

- `state_root` is pinned per run so `Agent.resume(agent_id)` finds the sqlite store on a resume; the lane does not rely on `CURSOR_SDK_BRIDGE_STATE_ROOT`. The `custom` store with `store_handler` (events to mission-db) is an optional later profile.
- The turn text is the packet's instruction segment plus the pointer to `.mission/context.md`; it carries no secrets and no inline bodies above the packet's inline budget.
- `agent_id` and `run.id` are persisted on the harness execution row before `observe` begins (native identity first, then observation).
- `tools`, `disallowed_tools` and inline `mcp_servers` are not persisted across `Agent.resume`; the lane re-supplies them on every `reattach`.

#### 5.3 Observation and closing facts

`observe` wraps `run.observe(after_offset=cursor)` (durable `ObserveRun` offsets) and maps each `SDKMessage` or `InteractionUpdate` to a Provider Frame with `provider_key = f"{run.id}:{offset}"`. Closing frames the reducer may read: `tool_call{status: completed|error}`, `TurnEndedUpdate` (per-turn usage), `usage`, and the terminal `RunResult` (status `finished|error|cancelled|expired`, `result`, `error`, `usage`, `git`, `duration_ms`, `model`). `closing_facts.cost` is `estimated` from token counts and a pinned price table; `usage()` later calls `agent.get_usage(run_id=...)` and upgrades to `settled` when `cost` is non-null (account-gated; `feature_unavailable` leaves the disposition `estimated`). `end_session` closes the agent, captures `git diff` plus the untracked file list as the snapshot patch, registers declared outputs under `/outputs/` as artifacts, and releases the worktree lease only after the patch is stored.

#### 5.4 Cursor hook payload to `mc.hook_input.v1`

| Cursor event | `mc.hook_event` | Fields carried (the `provider` block keeps the raw payload) | Decision returned |
| --- | --- | --- | --- |
| `sessionStart` | `session_start` | `session_id`, `is_background_agent` | `additional_context` (the packet index) |
| `beforeSubmitPrompt` | `before_prompt` | `prompt` digest | `continue` |
| `preToolUse` | `before_tool` | `tool_name`, `tool_input`, `tool_use_id` as `effect_ref` | `permission allow|deny`, `updated_input` |
| `beforeShellExecution` | `before_shell` | `command`, `cwd`, `sandbox` | `permission allow|deny` (`ask` is treated as deny headless) |
| `beforeMCPExecution` | `before_tool` (kind `mcp`) | `tool_name`, `mcp_server_name`, `tool_input` | `permission allow|deny` |
| `postToolUse`, `afterShellExecution`, `afterMCPExecution` | `after_tool`, `after_shell` | outputs as digest plus excerpt, `duration` | `additional_context` |
| `postToolUseFailure` | `after_tool_failure` | `error_message`, `failure_type`, `is_interrupt` | none |
| `afterFileEdit` | `after_file_edit` | `file_path`, edit count | none |
| `subagentStart`, `subagentStop` | `subagent_start`, `subagent_stop` | `subagent_type`, `task`, `status`, `modified_files` | `permission` / `followup_message` (never used by Kernel Hooks) |
| `preCompact` | `before_compaction` | `context_usage_percent`, `message_count` | none (frame only) |
| `stop` | `stop` | `status`, `loop_count` | `followup_message` only for the `missing_output_policy` follow-up turn |
| `sessionEnd` | `session_end` | `reason`, `final_status` | none |

Common fields map `conversation_id` to `native_session_ref`, `generation_id` to `provider_turn_ref`, plus `hook_event_name`, `cursor_version`, `workspace_roots`. Every hook invocation is also persisted as a Provider Frame of kind `hook`. Cloud runs omit `sessionStart`, `sessionEnd` and the MCP hooks, and hooks do not run during the early read-only turns of a cloud agent; the packet index must therefore be complete on disk and in the branch before the first send.

#### 5.5 Hook callback endpoint

`POST /v1/internal/hook-callback` on the worker's loopback listener (`interfaces/http/hook_callback.py`, bound to `127.0.0.1`, started by the worker, never by the public API). Request: `mc.hook_input.v1` plus `Authorization: Bearer <task token>`. The task token is minted at `prepare`, bound to `(installation, application, tenant, run_id, attempt, generation, harness_execution_id)`, expires with the lease, and is written only to `.mission/bin/.token` with mode 0600 inside the lease (never into frames or the transcript). Processing order for permission events: verify token and generation; consult the Stop Fence (deny when fenced); classify the side effect (shell command, MCP tool, write) against the binding's allowances; write the Operation Intent keyed on `effect_ref` (`tool_use_id`, or a digest of the shell command and ordinal); persist the frame; return `mc.hook_result.v1`. A callback that cannot reach the worker fails the hook, and because the permission hooks are fail-closed the action is blocked.

### 6. Cursor Cloud lane profile

1. `prepare`: create branch `mc/<run_id>` from the binding's base ref in the target repository, commit the projections and the packet files (`.mission/`, `/inputs/`), push. Cloud agents read `.cursor/rules`, `.cursor/agents`, `.cursor/hooks.json` (command hooks only) and project MCP from the repository; `setting_sources` is ignored in the cloud.
2. `start`: `Agent.create(AgentOptions(model, mode, agent_id=f"bc-{uuid5(run_id, attempt)}", agents=inline or None, cloud=CloudAgentOptions(repos=[CloudRepository(url, starting_ref=f"mc/{run_id}")], work_on_current_branch=True, auto_create_pr=binding.auto_pr, metadata={"mc_run_id": ...})))`. A client-supplied `agent_id` is the idempotency contract: `409 agent_id_conflict` means already created, so `reattach`. `env_vars` cannot be combined with `agent_id`; a binding that needs `env_vars` uses `idempotency_key` instead and records that the dedupe window is UNVERIFIED. `metadata` returns `403 feature_unavailable` on accounts without it; the lane records the absence rather than failing.
3. `send_turn`: `agent.send(...)`; `409 agent_busy` (`AgentBusyError`, not retryable) means `wait_then_send`: the lane returns `TurnHandle(status="busy")` and the workflow runs `lane.status` until the agent is `IDLE` before sending.
4. `observe`: REST `GET /v1/agents/{id}/runs/{runId}/stream` with `Accept: text/event-stream` and `Last-Event-ID = cursor`; frames carry `provider_key = sse id` (the leading `status` event has no id and is deduped by content). Record `X-Cursor-Stream-Retention-Seconds`. On `410 stream_expired`, call `GET .../runs/{runId}` and synthesize the terminal frame from the run record. `heartbeat` SSE events are not persisted.
5. Closing facts from the `result` event or the run record: `status`, `durationMs`, `result` text (stored as an artifact), `git.branches[] {repoUrl, branch, prUrl}` (agent-scoped; attributed to the run through `latestRunId`). There is no diff or commit SHA in v1; `end_session` fetches the branch into a worker clone and registers the diff as the patch artifact.
6. `usage`: `GET /v1/agents/{id}/usage?runId=` for tokens (settled); `get_usage().cost` for cost (settled when non-null).
7. `end_session`: `archive` the agent after artifacts (`GET /v1/agents/{id}/artifacts`, download through presigned URLs) and the patch are registered; `delete` only by an explicit cleanup policy.
8. Subagents: file form in the branch for `readonly` and `is_background`; inline `customSubagents` (max 20) only for secret-free, per-run prompts.
9. Self-hosted pools (`CloudEnvironment(type="pool", name=...)`) are a later profile variant; the first qualification uses Cursor-hosted machines.

### 7. Controls and emulations

| Command | cursor_local | cursor_cloud | Mechanics |
| --- | --- | --- | --- |
| `send_turn`, `queue_instruction` | `wait_then_send` | `wait_then_send` | mailbox consumed between segments; the next `send` carries the instruction |
| `cancel` (normal) | `turn_boundary_guaranteed` | `turn_boundary_guaranteed` | activity cancel, then `run.cancel()` or `POST .../cancel`, then `lane.cancel`, then `lane.status` |
| `cancel` (immediate) | same, Stop Fence first | same | Kernel Hooks deny new effects at once; `409 run_not_cancellable` means already terminal |
| `interrupt_and_inject` | `cancel_and_replace` | `cancel_and_replace` | cancel, settle uncertain effects (a `tool_call{status: running}` frame without a completion is an Uncertain Effect), then a replacement turn whose packet includes the injected item |
| `pause` | unsupported mid-run; boundary only | same | the family stops releasing; no new segment starts; the Delivery Report says `unsupported` for mid-run pause |
| `fork` | emulated | emulated | snapshot (patch plus files, or branch plus artifacts), new run, `prepare` with the packet's `workspace` tier restoring the patch or checking out the branch, new agent |
| `request_continuation` | emulated | emulated | seal a Continuation Checkpoint from the packet and closing facts; fresh agent hydrated from it (`Agent.resume` is used only for the same Agent Session within one attempt) |
| `snapshot` | emulated | emulated | local: `git diff`, untracked files, `.mission/` digests; cloud: branch head and artifact list |

Describe honesty tests (Testing Decisions) assert that each cell above matches `describe()` and that the implementation raises `HarnessUnsupported` for every `unsupported` cell.

### 8. Lifecycle synthesis for Cursor

| Provider event | Frame kind | Reducer fact | Mission event |
| --- | --- | --- | --- |
| `system/init` (SDK) or first `status{CREATING}` (SSE) | `session_init` | Agent Session started, native refs recorded | `session.started` |
| `send` accepted, `status{RUNNING}` | `turn_start` | Session Turn started | `session.turn_started` |
| `assistant`, `thinking`, text deltas | `delta` | none | none |
| `tool_call{status: running}` | `tool_running` | effect candidate opened (`call_id`) | none |
| `tool_call{status: completed|error}` | `tool_closed` | tool effect settled | `tool_call.completed` |
| hook `before_*` with allow or deny | `hook` | Operation Intent written or denied | none (intent ledger) |
| `preCompact` | `compaction` | compaction observed | `session.compaction_observed` |
| `TurnEndedUpdate`, `usage` | `turn_usage` | tokens settled, cost estimated | `session.turn_completed` |
| `RunResult{finished}`, `result` plus `status{FINISHED}` | `terminal` | Attempt `succeeded` if a Completion Candidate exists, else follow-up turn or `not_accepted(outputs_missing)` | `attempt.completed` |
| `RunResult{error}`, `status{ERROR}` | `terminal` | Attempt `failed(provider_error)` | `attempt.completed` |
| `RunResult{cancelled}`, `status{CANCELLED}` | `terminal` | `cancelled(cancelled_by_command)` when a command caused it, else `failed(infrastructure)` | `attempt.completed` |
| `RunResult{expired}`, `status{EXPIRED}` | `terminal` | `failed(timeout)` | `attempt.completed` |
| `409 agent_busy`, `429` | none (adapter error) | `failed(capacity)` only after the wait bound | `attempt.completed` |
| stream lost, run state unknown after the bound | none | unit `in_doubt`, reconciliation incident | `activation.phase_changed` |

A native `FINISHED` is never acceptance; the Completion Contract decides.

## Contracts

### `mc.cursor_binding.v1` (`CursorExecutionBinding`)

```text
CursorExecutionBinding {
  schema_version: "mc.cursor_binding.v1"
  lane_profile: cursor_local | cursor_cloud
  pins: { cursor_sdk: "1.0.37", bridge: "1.0.37", protocol: "sdk.v1", cloud_api: "v1" }
  model_id, mode: agent | plan
  workspace: { repo_url?, base_ref, lease_root?, branch_prefix: "mc/" }
  projections: { rules_digest, agents_digest, skills[], mcp_servers[], hooks_digest }   # exact pins from the catalog
  inline_subagents[]                                                                   # Subagent Profile refs, optional
  disallowed_tools[]                                                                   # local only
  sandbox_enabled: bool
  hook_callback: { listen: "127.0.0.1:<port>", token_ttl_s }
  cloud?: { auto_create_pr, env_vars_ref?, metadata, environment: cloud | pool | machine }
  budgets: { max_turns, max_segments, wall_clock_s }
  binding_digest
}
```

### Hook callback

Request body `mc.hook_input.v1`: `{schema_version, hook_event, lane_profile, harness_execution_id, generation, effect_ref?, tool: {name, kind: shell|mcp|file|task|other, input_digest, input_excerpt}, provider: {event_name, raw}}`. Response `mc.hook_result.v1`: `{decision: allow|deny|defer, reason_code?, updated_input?, additional_context?, message?}`. `defer` maps to `deny` on Cursor and is recorded as such in the frame.

### Activity payloads

`LaneTurnRequest`, `LaneTurnResult`, `ClosingFacts`, `LaneStatusRequest`, `LaneStatusResult`, `LaneCancelRequest`, `CancelReceipt` as in section 4.1; all are strict Pydantic contracts in `domain/execution/contracts.py` and pass through the Temporal Pydantic data converter already in use.

## Persistence (migration `0030_lane_bindings.sql`, T4)

- `mission_control.lane_profile` (`lane_profile text primary key`, `lane`, `placement`, `describe jsonb`, `qualified boolean`, `qualified_at`, `qualification_ref`), seeded with the three profiles and `qualified=false` for Cursor.
- `mission_control.execution_binding` gains `lane_profile text not null default 'deep_agents'` and `lane_binding jsonb` (the typed binding by lane).
- `mission_control.harness_execution` gains `native_session_ref`, `native_turn_ref`, `provider_cursor`, `cursor_sdk_version`, `bridge_state_root`, `cloud_branch`, `cloud_agent_url`, `usage_disposition` (`estimated | settled | unknown`), `last_segment_at`.
- `mission_control.workspace_lease` (`lease_id`, `run_id`, `attempt_no`, `generation`, `path`, `base_commit`, `fence`, `expires_at`, `released_at`, `patch_artifact_ref`).
- `mission_control.hook_task_token` (`token_hash`, `harness_execution_id`, `generation`, `expires_at`, `revoked_at`); the token value is never stored.

## Interfaces

- CLI: `missionctl lane list`, `missionctl lane describe <profile> --json`.
- HTTP: `GET /v1/applications/{app}/lanes`, `GET /v1/applications/{app}/lanes/{profile}`; worker-only `POST /v1/internal/hook-callback`.
- MCP: `mission_lane_describe`; the Validation Report (SPEC-05) embeds the describe digest it validated against.
- Operator: `make temporal-up` registers the four search attributes; `make lane-qualify PROFILE=cursor_local` runs the recorded-fixture suite and, with `MC_PAID_BUDGET_USD` set and the owner's approval comment referenced, the live drill.

## Insertion points

- `application/execution/operations/operation_execution.py`: the `RuntimePort` family stays; `OperationExecutionService.__init__` takes `lanes: LaneRegistry`; `execute` routes by `request.lane_profile`; the `execution_runtime == "deep_agent"` lineage branch moves into `DeepAgentsHarness.prepare`.
- `domain/execution/contracts.py`: `execution_runtime` literal, `lane_profile`, `CursorExecutionBinding`, activity payload contracts, the validator pairing rule.
- `adapters/temporal/workflows/operation.py`: the segment loop replaces the single activity call in `_run` while keeping the signals and queries the family depends on.
- `adapters/temporal/operation_activities.py`: `operation.execute` stays as the Deep Agents path until `DeepAgentsHarness` passes the same acceptance tests, then is removed (FT-G6).
- `adapters/temporal/deployment_composition.py`: build the `LaneRegistry`; wire `adapters/cursor` when `CURSOR_API_KEY` is bound; pass the `FrameSink`.
- `adapters/temporal/search_attributes.py` and `domain/programs/search_attributes.py`: add the four keys.
- `bootstrap/worker.py`: start the loopback hook-callback listener with the worker; `pyproject.toml`: `cursor-sdk==1.0.37`, `temporalio[opentelemetry]>=1.34,<2`.

## Testing Decisions

Good tests observe behavior at the highest seam: the operation workflow driven in `WorkflowEnvironment.start_time_skipping()` against a fake harness, and the Cursor harness driven against recorded frame fixtures. Bridge RPC and SSE parsing internals are tested only through those seams.

- Unit (`tests/unit/harness/`): `describe` conformance for each registered profile (every `unsupported` control raises `HarnessUnsupported`); `LaneRegistry` composition rules; the hook payload mapping table (one case per Cursor event); `CursorExecutionBinding` validation; closing-facts derivation from fixture frames; usage disposition transitions.
- Integration Temporal (`tests/integration/temporal/test_lane_turn.py`, time-skipping): the segment loop resumes from a heartbeat cursor after a simulated worker crash without a second `send`; cancel Update, activity cancel, `lane.cancel`, `lane.status`, settled; duplicate command id rejected within a run and after continue-as-new; `all_handlers_finished` before continue-as-new; search attributes upserted at boundaries.
- Integration PostgreSQL (`common_db`): frames persisted before heartbeat (crash injection between persist and heartbeat leaves no gap); lease and token tables; Stop Fence consulted by the callback.
- Cursor local, recorded (`tests/integration/cursor/fixtures/local/*.jsonl`): a full run (init, tool calls, usage, finished), a cancelled run, an error run, a run with `preCompact`, a hook deny; replayed through the real `adapters/cursor/frames.py` and reducer. Recording happens once under an approved budget with a scrubber that removes secrets and user email.
- Cursor cloud, recorded (`fixtures/cloud/*.sse`): a stream with ids, reconnect with `Last-Event-ID`, `410` fallback to the run record, `409 agent_busy`, `409 agent_id_conflict`, `git.branches[]` on result.
- Live drills (skipped unless `MC_PAID_BUDGET_USD` and `CURSOR_API_KEY` are set): one local run of Mission 3's first iteration; one cloud run on a throwaway repository; each asserts the describe matrix empirically (cancel latency, busy behavior) and writes the qualification record that flips `lane_profile.qualified`.
- Prior art: `tests/acceptance/control_plane/test_wp_cp_040.py` (Deep Agents operation proof), `tests/integration/temporal/test_linked_runs.py` (time-skipping environment), `tests/unit/operations/test_checkpoint_recovery_classification.py` (in-doubt classification).

## Qualification fixtures and budget

Paid budget is zero until the owner approves a finite amount in the ticket's Linear comment. Recorded fixtures are the default evidence for every ticket; FT-G6 is the only ticket that spends, and it records spent, reserved and unknown units in its handoff. UNVERIFIED items the drill must settle: Windows sandbox support; whether local rules and hooks load without `setting_sources=["project"]` (the lane always sets it); `Run.request_id` exposure in Python; concurrent local `send()`; the `Idempotency-Key` dedupe window on cloud create; whether `run.git` is populated for local runs; REST `envVars` and `metadata` acceptance on the owner's account; the cloud stream retention value.

## Out of Scope

Claude Agent SDK and Codex lanes (next in ADR-0018 order); Cursor self-hosted pools and `/v1/sub-tokens` worker identities; the headless `agent` CLI as a lane (superseded by the SDK); v1 webhooks (not shipped); the bridge `custom` store writing to mission-db (optional later profile); Nexus; Temporal Workflow Streams and the Deep Agents Temporal plugin; mid-run pause on Cursor (no provider primitive).

## Further Notes

- `Agent.resume` continues the same Agent Session within one attempt (reattach); it never crosses attempts or activations, which keeps the specification's rule that a session serves one activation lineage.
- `wait_then_send` on cloud is bounded by the budgets' `wall_clock_s`; an agent that never returns to `IDLE` within the bound classifies `failed(capacity)`.
- Cloud `git.branches[]` is agent-scoped; the lane attributes it to the run whose `latestRunId` matches at the time of the terminal frame.
- The `prompt_prefix` instruction channel is a fallback for the first turn only; rules and `AGENTS.md` are the durable channel.
- Catalog Hook Scripts (SPEC-01) run after Kernel Hooks; a catalog hook can only narrow, never widen, because the merge rule is deny beats ask beats allow.

## Tickets

FT-G1 harness protocol, registry and dispatch; FT-G2 lane activities and segment loop; FT-G3 Cursor Local prepare, projections, callback, turn and frames; FT-G4 Cursor controls and hydrated fork and continuation; FT-G5 Cursor Cloud; FT-G6 qualification fixtures and describe honesty; FT-G7 temporalio 1.34 upgrade and search attributes; FT-F2 interrupt_and_inject per lane; FT-F3 immediate cancel with Stop Fence. Bodies are under [issues/](issues/).

# Citations

- ADR-0018, ADR-0019, ADR-0030, ADR-0031; `GLOSSARY.md`.
- [research/cursor-platform.md](research/cursor-platform.md) sections 1, 2.3, 2.6, 3, 4, 5, 6 and the implications; [research/temporal-lifecycle.md](research/temporal-lifecycle.md) sections 1, 2.1, 2.4, 4, 8, 9; [research/codebase-map.md](research/codebase-map.md) sections 4, 5, 9 and gaps (g), (h); `docs/research/2026-10-07-coding-lane-surfaces.md` (Cursor sections and the harness comparison table).
- Spec pack: `RUNTIME-CONTRACTS.md` (harness interface), `workflow-types/05-EXECUTORS_AND_DURABLE_CONTROLS.md` sections 2, 3.3, 3.5, `workflow-types/09-EVENTS_COMMANDS_AND_STREAMS.md` sections 4, 7, 8.
- Code: `src/mission_control/application/execution/operations/operation_execution.py`, `src/mission_control/domain/execution/contracts.py`, `src/mission_control/adapters/temporal/workflows/operation.py`, `src/mission_control/adapters/temporal/operation_activities.py`, `src/mission_control/adapters/temporal/search_attributes.py`, `src/mission_control/adapters/temporal/deployment_composition.py`.
