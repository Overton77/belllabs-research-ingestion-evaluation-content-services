---
type: Research Note
title: "Temporal lifecycle primitives for Mission Control (temporalio 1.34.0, read 2026-10-07)"
description: "Signals, queries, updates and limits; continue-as-new, child workflows, id policies, heartbeating activities and cancellation; reset, history export and replay; visibility; Nexus; worker versioning; timers; and recommendations for observing provider streams, delivering cancels, seeding forks and chaining missions."
tags: [mission-control, research, fast-track]
---

# Temporal lifecycle primitives for Mission Control: fact sheet

- **Date read:** 2026-10-07 (every citation below was read on this date)
- **SDK pinned:** `temporalio` **1.34.0**, published to PyPI 2026-09-30, `requires_python >=3.10`. Facts come from the tagged source tree `github.com/temporalio/sdk-python@1.34.0`.
- **CLI pinned:** Temporal CLI **v1.9.1** (2026-09-14). Facts come from `internal/temporalcli/commands.yaml@v1.9.1`.
- **Docs pinned:** `github.com/temporalio/documentation@5d9703a` (main, 2026-10-08 00:03 UTC). This repo is the source of docs.temporal.io.
- **Repo note:** `mission-control/uv.lock` locks `temporalio==1.30.0` and `pyproject.toml` allows `>=1.30,<2`. §9 lists the deltas from 1.30 to 1.34 that matter here.
- **How to read this:** a claim marked **UNVERIFIED** has no primary source behind it, or comes from inference or memory. Everything else is checked against the source or docs cited.

---

## 1. Workflow messages

### 1.1 API surface (verified in source, 1.34.0)

| Primitive | Python API |
|---|---|
| Signal handler | `@workflow.signal` (kwargs `name`, `dynamic`, `unfinished_policy`, `description`) |
| Query handler | `@workflow.query` (kwargs `name`, `dynamic`, `description`) |
| Update handler | `@workflow.update` (kwargs `name`, `dynamic`, `unfinished_policy`, `description`) |
| Update validator | `@<handler>.validator`. It must return `None`; raising rejects the update |
| Init before handlers | `@workflow.init` on `__init__`. State then exists before the first signal or update is handled |
| Send signal | `await handle.signal(Wf.sig, arg)` |
| Send query | `await handle.query(Wf.q)` |
| Execute update | `await handle.execute_update(Wf.upd, arg, id="cmd-123")` |
| Start update | `await handle.start_update(Wf.upd, arg, wait_for_stage=WorkflowUpdateStage.ACCEPTED, id=...)` returns `WorkflowUpdateHandle`. Later call `.result()` |
| Re-attach to an update | `handle.get_update_handle_for(Wf.upd, update_id)` / `get_update_handle(id)` |
| `WorkflowUpdateStage` | `ADMITTED`, `ACCEPTED`, `COMPLETED`. **`start_update` docstring: "ADMITTED is not currently supported."** |
| Update-with-start | `client.execute_update_with_start_workflow(upd, arg, start_workflow_operation=WithStartWorkflowOperation(Wf.run, id=..., task_queue=..., id_conflict_policy=...))` and `client.start_update_with_start_workflow(...)`. `id_conflict_policy` is **required** on `WithStartWorkflowOperation`. Get the workflow handle with `await start_op.workflow_handle()` |
| Signal-with-start (client) | `client.start_workflow(..., start_signal="name", start_signal_args=[...])` |
| Signal-with-start (from a workflow) | `workflow.signal_with_start_workflow(...)` was added in 1.29 and is **experimental**. It runs over a system Nexus endpoint `__temporal_system`. The server version it needs is **UNVERIFIED** |
| Wait | `await workflow.wait_condition(fn, timeout=..., timeout_summary=...)`. On timeout it raises `asyncio.TimeoutError`. Changing `timeout` between `None` and a value is a nondeterministic change |
| Drain handlers | `workflow.all_handlers_finished()` combined with `await workflow.wait_condition(workflow.all_handlers_finished)` |
| Unfinished policy | `workflow.HandlerUnfinishedPolicy.WARN_AND_ABANDON` (default) or `ABANDON`. Warnings are `UnfinishedUpdateHandlersWarning` and `UnfinishedSignalHandlersWarning` |
| Current update | `workflow.current_update_info()` returns `UpdateInfo(id, name)` |

```python
@workflow.defn
class OperationWf:
    @workflow.init
    def __init__(self, inp: OpInput) -> None:
        self.cancel_requested = False
        self.seen_cmds: set[str] = set(inp.seen_cmds)   # carried across continue-as-new
    @workflow.update
    async def cancel(self, cmd: CancelCmd) -> str:
        self.cancel_requested = True
        return "accepted"
    @cancel.validator
    def _v(self, cmd: CancelCmd) -> None:
        if cmd.id in self.seen_cmds: raise ValueError("duplicate")
```

```python
start_op = WithStartWorkflowOperation(
    MissionRunWf.run, inp, id=f"mission-run:{run_id}", task_queue="mc",
    id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING)
ack = await client.execute_update_with_start_workflow(
    MissionRunWf.command, cmd, start_workflow_operation=start_op, id=cmd.command_id)
wf_handle = await start_op.workflow_handle()
```

### 1.2 Semantics that matter

- **Ordering.** "Every time the Workflow wakes up ... it will process messages in the order they were received, followed by making progress in the Workflow's main method." Everything runs on one thread. Async handlers can still interleave with each other and with `run`, so they must be reentrant. The docs suggest `asyncio.Lock` for that. [handling-messages; py message-passing]
- **Update idempotency.** "For Updates, Temporal handles this for you on the server, by deduplicating according to the Update ID ... **if you are using Updates with Continue-As-New you should implement the deduplication in your Workflow code, since Update ID deduplication by the server is per Workflow run.**" Signals need an app-level idempotency key. Both signals and updates also dedupe retried client RPCs by request ID. [handling-messages]
- **Validators.** A rejection writes nothing to history. The caller gets `WorkflowUpdateFailedError`. `WorkflowExecutionUpdateAccepted` is written only after the update is accepted, and `WorkflowExecutionUpdateCompleted` when the handler finishes. Validators must not block. [py message-passing; handling-messages]
- **Update failure modes in Python** (all from [py message-passing#update-problems]):
  - No worker is polling: the SDK retries forever. Wrap the call in `asyncio.timeout` to bound it; that raises `WorkflowUpdateRPCTimeoutOrCancelledError`.
  - The handler raises `ApplicationError`, or an activity/child fails terminally: the update fails and the workflow keeps running.
  - The handler raises any other exception: the workflow task fails and the workflow is stuck until a fix is deployed. An update that was not yet accepted returns `FAILED_PRECONDITION`. An accepted one is durable; fetch its result with an `UpdateHandle` later.
  - The workflow finishes or continues-as-new while a handler is still running: the caller gets `RPCError` with status `NOT_FOUND`.
- **Updates cannot go workflow-to-workflow.** "You can't send Updates directly from one Workflow to another ... use an Activity." [py message-passing#send-update-from-client]
- **Update-with-start is not atomic.** "Unlike Signal-with-Start - Update-With-Start is _not_ atomic. If the Update can't be delivered ... a new Workflow Execution will still start." If the workflow exists and an update with the same Update ID exists in the latest run, the call attaches to that update whatever the conflict policy. Temporal Server 1.28+ is recommended. [sending-messages#update-with-start]
- **Updates need a live worker.** "Workers must be available and responsive. If you need a response as soon as the Server receives the request, use a Signal instead." [py message-passing]

### 1.3 Limits that constrain long-running goal loops (Temporal Cloud page; the docs give the same numbers for OSS defaults)

| Limit | Value |
|---|---|
| Event History | **Hard limit 51,200 events or 50 MB. Warning at 10,240 events or 10 MB.** (The brief said "50k warn". That is wrong: the warning is at 10,240.) |
| Single payload (arguments, results, CaN input) | 2 MB |
| Event History transaction / gRPC message | 4 MB / 4 MB |
| Signals per execution | 10,000. After that, signals are no longer processed |
| Updates per execution | **10 in flight**, **2,000 total in History** |
| Pending activities, signals, children, or external cancels | 2,000 each per execution |
| In-flight Nexus operations | 30 per execution |
| Custom search attributes (Cloud, per namespace) | Bool 20, Datetime 20, Double 20, Int 20, Keyword 40, KeywordList 5, Text 5 |

Sources: [cloud limits]; [best-practices/worker]. Hard limits on self-hosted are set by dynamic config. Whether self-hosted *defaults* match the Cloud update counts is **UNVERIFIED**; the docs link a separate `/self-hosted-guide/defaults` page that was not read.

---

## 2. Long-running loops

### 2.1 Continue-as-new

- Call: `workflow.continue_as_new(arg | args=[...], workflow=, task_queue=, run_timeout=, task_timeout=, backoff_start_interval=, retry_policy=, memo=, search_attributes=, versioning_intent=, initial_versioning_behavior=, event_groups=) -> NoReturn`. `backoff_start_interval` was added in 1.30.
- Info helpers, read from `workflow.info()`:
  - `is_continue_as_new_suggested()`
  - `get_current_history_length()`
  - `get_current_history_size()` (bytes)
  - `is_target_worker_deployment_version_changed()` (experimental, used for upgrade-on-CaN)
- Carrying state: design the input as `Input(state: State | None = None)` and pass `self.state` to `continue_as_new`. [py continue-as-new]
- Handlers: "don't call Continue-as-New from the handlers. Instead, wait for your handlers to finish in your main Workflow before you run `continue_as_new`." [py continue-as-new]
- Signals: drain buffered signals and carry the unprocessed ones forward, or they are lost. [design-patterns/continue-as-new]
- Children: "Child Workflows do not carry over when the Parent uses Continue-As-New". Their Parent Close Policy decides what happens to them. [child-workflows]
- Ids: the new run keeps the same Workflow ID and gets a new Run ID. Inside the workflow, `workflow.info().continued_run_id` points at the previous run.

```python
@workflow.run
async def run(self, inp: GoalLoopInput) -> GoalResult:
    while not self.done:
        await self.one_iteration()
        if workflow.info().is_continue_as_new_suggested():
            await workflow.wait_condition(workflow.all_handlers_finished)
            workflow.continue_as_new(GoalLoopInput(
                state=self.state, seen_cmds=sorted(self.seen_cmds)))
    return self.result
```

### 2.2 Child workflows

- `workflow.start_child_workflow(Wf.run, arg, id=, task_queue=, cancellation_type=ChildWorkflowCancellationType.WAIT_CANCELLATION_COMPLETED, parent_close_policy=ParentClosePolicy.TERMINATE, id_reuse_policy=ALLOW_DUPLICATE, execution_timeout=, run_timeout=, retry_policy=, memo=, search_attributes=, static_summary=, static_details=, priority=)` returns a `ChildWorkflowHandle`.
- `workflow.execute_child_workflow(...)` takes the same arguments and awaits the result.
- **Child starts have no `id_conflict_policy` parameter** (verified in the 1.34.0 signature). There is also **no namespace parameter**, so children run in the parent's namespace.
- `ParentClosePolicy`: `TERMINATE` (default), `ABANDON`, `REQUEST_CANCEL`, plus `UNSPECIFIED`.
- `ChildWorkflowCancellationType`: `ABANDON`, `TRY_CANCEL`, `WAIT_CANCELLATION_COMPLETED` (default), `WAIT_CANCELLATION_REQUESTED`.
- Guidance: "a single Parent should not spawn more than 1,000 Child Workflow Executions". There is a hard limit of 2,000 pending children. "When in doubt, use an Activity." [child-workflows]
- Visibility gives `parent_id`, `parent_run_id`, `root_id`, `root_run_id` on `WorkflowExecution`. Inside a workflow, `workflow.info().parent` and `.root` give the same.

### 2.3 Workflow ID policies (`temporalio.common`)

- `WorkflowIDReusePolicy` decides whether a new run may reuse the ID of a *closed* run:
  - `ALLOW_DUPLICATE` (default)
  - `ALLOW_DUPLICATE_FAILED_ONLY`
  - `REJECT_DUPLICATE`
  - `TERMINATE_IF_RUNNING`
- `WorkflowIDConflictPolicy` decides what happens when a run with that ID is *running*:
  - `UNSPECIFIED` (effectively `FAIL`)
  - `FAIL`
  - `USE_EXISTING`: the docstring says "Set to USE_EXISTING for idempotent deduplication on workflow ID. With Temporal Server 1.32.0 or later, the returned handle is scoped to the existing workflow's run chain."
  - `TERMINATE_EXISTING`
  - It cannot be combined with `TERMINATE_IF_RUNNING`.
- The reuse policy is checked only against closed runs still inside namespace retention. [workflowid-runid]
- 1.34.0 added `WorkflowAlreadyStartedError.first_run_id`.

### 2.4 Activities: heartbeats, resume, cancellation

- `activity.heartbeat(*details)`. After a retry, `activity.info().heartbeat_details` returns the details from the last heartbeat that reached the server. "If an Activity calls `heartbeat(123, 456)` and then fails and is retried, `heartbeat_details` returns an iterable containing `123` and `456`." [py activities/timeouts]
- **Throttling.** The worker sends heartbeats at most every `min(heartbeat_timeout*0.8 or default_heartbeat_throttle_interval, max_heartbeat_throttle_interval)`. The defaults are 30 s and 60 s, set as `Worker(default_heartbeat_throttle_interval=, max_heartbeat_throttle_interval=)`. Two consequences:
  - The persisted offset can **lag** the true position, so resume is at-least-once.
  - Cancellation is delivered only on a heartbeat, so it can arrive late.
  [detecting-activity-failures#throttling]
- **Cancellation** needs both `heartbeat_timeout` set and periodic heartbeats. An async activity sees `asyncio.CancelledError` raised. You can do cleanup in `except`/`finally` but must **re-raise**. Related helpers:
  - `activity.is_cancelled()` and `await activity.wait_for_cancelled()`
  - `activity.cancellation_details()` returns `ActivityCancellationDetails(not_found, cancel_requested, paused, reset, timed_out, worker_shutdown)`
  - `activity.is_worker_shutdown()` and `await activity.wait_for_worker_shutdown()`
  - `activity.client()` gives the worker's `Client`, async activities only
  [py cancellation; README]
- **Cancelling from the workflow.** `h = workflow.start_activity(...)` then `h.cancel()`. `ActivityCancellationType` is `TRY_CANCEL` (default), `WAIT_CANCELLATION_COMPLETED` or `ABANDON`.
- **Local activities.** Use `workflow.execute_local_activity` / `start_local_activity`. They do **not** heartbeat; heartbeating has no effect on them. They suit operations of a few seconds. They can be cancelled without heartbeats. [local-activity; README]
- **Workflow-side asyncio.** `asyncio.wait_for` and `asyncio.sleep` create durable timers. Use `workflow.wait()` and `workflow.as_completed()` in place of `asyncio.wait` and `asyncio.as_completed`, which are nondeterministic. Thread APIs and real I/O are forbidden. [README]

Resumable stream observer. The SSE resume mechanism is provider-specific. Whether Cursor's SSE honours `Last-Event-ID` or an offset parameter is **UNVERIFIED** here; check it against the Cursor SDK research.

```python
@activity.defn
async def observe_run(req: ObserveReq) -> ObserveResult:
    offset = activity.info().heartbeat_details[0] if activity.info().heartbeat_details else req.offset
    async for ev in provider.stream(req.run_id, after=offset):          # provider-specific resume
        await journal.append_idempotent(req.run_id, ev.id, ev)          # dedupe by event id (at-least-once)
        offset = ev.id
        activity.heartbeat(offset)
        if ev.terminal: return ObserveResult(done=True, offset=offset, status=ev.status)
        if segment_expired(): return ObserveResult(done=False, offset=offset)
    return ObserveResult(done=False, offset=offset)
```

Cancel-aware cleanup inside the observer:

```python
    except asyncio.CancelledError:
        d = activity.cancellation_details()
        if d and d.cancel_requested:          # real cancel, not worker_shutdown/pause
            await provider.cancel(req.run_id)  # idempotent provider cancel
        raise
```

---

## 3. Fork and rewind: reset, history export, replay

### 3.1 Reset semantics

- "A Reset terminates a Workflow Execution and creates a new Workflow Execution with the same Workflow Type and Workflow ID. The Event History is copied from the original execution up to and including the reset point." Valid reset points are `WorkflowTaskStarted`, `WorkflowTaskCompleted`, `WorkflowTaskTimedOut` and `WorkflowTaskFailed`. [event#reset]
- **A reset creates a new Run ID.** `ResetWorkflowExecutionResponse { string run_id = 1; }`. The proto comment says "In all cases the current run will be terminated and a new run started." [temporalio/api request_response.proto]
- **Lineage.** `first_execution_run_id` identifies the chain. `original_execution_run_id` is the Run ID recorded on `WorkflowExecutionStarted`. The docs say: "A Workflow `Reset` changes the first execution Run Id, but preserves the original execution Run Id." [workflowid-runid]
  - 1.34.0 adds `workflow.Info.original_execution_run_id`, documented as "Unlike `run_id`, this value is preserved across workflow resets."
  - `workflow.Info.first_execution_run_id` and `WorkflowHandle.first_execution_run_id` were already there.
- **Reapply.**
  - Signals in the original history "can be optionally copied to the new history".
  - Updates reapplied during a reset appear as a `WorkflowExecutionUpdateAdmitted` event. [references/events]
  - The deprecated `ResetReapplyType` has `SIGNAL`, `NONE` and `ALL_ELIGIBLE`; the proto default is `SIGNAL`.
  - The current mechanism is `reset_reapply_exclude_types: [ResetReapplyExcludeType]` with values `SIGNAL`, `UPDATE`, `NEXUS`, and `CANCEL_REQUEST` (deprecated, unimplemented).
  - The docs example uses `--reapply-exclude All` "to skip re-applying signals and Updates ... typically the right choice for a clean restart", and resetting to `LastWorkflowTask` "re-applies pending signals and Updates". [recover-pinned-workflows]
- **`post_reset_operations`** (`PostResetOperation.signal_workflow` / `.update_workflow_options`) "will be applied to the *new* run ... before the first new workflow task is generated". This lets you inject a signal into the reset run atomically.
- Intended use (Python docs): "Use reset when a Workflow is blocked due to a non-deterministic error or other issues ... Any progress made after the reset point will be discarded." [py cancellation#reset]

### 3.2 CLI (v1.9.1)

```bash
temporal workflow reset -w mission-run:R1 --event-id 42 --reason "fork test" --reapply-exclude Signal --reapply-exclude Update
temporal workflow reset -w mission-run:R1 --type LastContinuedAsNew --reason "..."
temporal workflow reset --query "WorkflowType='GoalLoopWf' AND ExecutionStatus='Running'" --type LastWorkflowTask --reason "..." --yes
```

- Flags:
  - `--workflow-id/-w`, `--run-id/-r`, `--event-id/-e`, `--reason` (required)
  - `--reapply-type All|Signal|None` (deprecated)
  - `--reapply-exclude All|Signal|Update` (repeatable)
  - `--type FirstWorkflowTask|LastWorkflowTask|LastContinuedAsNew|BuildId`
  - `--build-id`, `--query/-q`, `--yes/-y`
- Batch resets: "limit your resets to FirstWorkflowTask, LastWorkflowTask, or BuildId".
- Subcommand `temporal workflow reset with-workflow-update-options` resets and applies a versioning override in one atomic step.
- **`temporal workflow reset-batch` is not a command in CLI v1.9.1.** The name appears only as a docs keyword and is a tctl-era name. Batch reset is `temporal workflow reset --query ...`.
- Also present: `temporal workflow pause` / `unpause`. These are **pre-release**: Server v1.30.0+ with `frontend.WorkflowPauseEnabled`, and invite-only on Cloud. [workflow-pause]

### 3.3 Reset from Python

There is no high-level `client.reset_workflow`. Use the raw service call (`Client.workflow_service.reset_workflow_execution` exists in 1.34.0):

```python
from temporalio.api.workflowservice.v1 import ResetWorkflowExecutionRequest
from temporalio.api.common.v1 import WorkflowExecution
from temporalio.api.enums.v1 import ResetReapplyExcludeType
resp = await client.workflow_service.reset_workflow_execution(ResetWorkflowExecutionRequest(
    namespace=client.namespace, workflow_execution=WorkflowExecution(workflow_id=wid, run_id=rid),
    reason="operator rewind", workflow_task_finish_event_id=42, request_id=str(uuid4()),
    reset_reapply_exclude_types=[ResetReapplyExcludeType.RESET_REAPPLY_EXCLUDE_TYPE_SIGNAL,
                                 ResetReapplyExcludeType.RESET_REAPPLY_EXCLUDE_TYPE_UPDATE]))
new_run_id = resp.run_id
```

### 3.4 History export and replay

- `await handle.fetch_history()` returns `WorkflowHistory`. It has `.to_json()`, `.to_json_dict()`, `WorkflowHistory.from_json(workflow_id, str|dict)`, and `.run_id`.
- `handle.fetch_history_events(page_size=, next_page_token=, wait_new_event=False, event_filter_type=ALL_EVENT|CLOSE_EVENT, skip_archival=)` is an async iterator. `wait_new_event=True` long-polls, so you can tail a run's history.
- `client.list_workflows(q).map_histories()` gives histories in bulk.
- `Replayer(workflows=[...]).replay_workflow(history)`, `.replay_workflows(histories, fail_fast=...)` and `.workflow_replay_iterator(...)`. A nondeterminism error raises. [testing-suite#replay]

```python
h = client.get_workflow_handle(wid, run_id=rid)
Path("hist.json").write_text((await h.fetch_history()).to_json())
await Replayer(workflows=[GoalLoopWf]).replay_workflow(
    WorkflowHistory.from_json(wid, Path("hist.json").read_text()))
```

---

## 4. Visibility

- **Search attributes.** Create keys with `SearchAttributeKey.for_keyword/for_text/for_int/for_float/for_bool/for_datetime/for_keyword_list(name)`. At start, pass `search_attributes=TypedSearchAttributes([SearchAttributePair(key, value)])`. Inside a workflow, `workflow.upsert_search_attributes([key.value_set(v), key2.value_unset()])` and read them back from `workflow.info().typed_search_attributes`. The dict form is deprecated. [py observability#search-attributes]
- **Registering custom attributes.**
  - `temporal operator search-attribute create --name MissionId --type Keyword`; also `list` and `remove`.
  - Dev server: `temporal server start-dev --search-attribute MissionId=Keyword`. Allowed types: `Text, Keyword, Int, Double, Bool, Datetime, KeywordList`.
  [cli operator; commands.yaml]
- **Memo.** Set `memo={...}` at start. Update it with `workflow.upsert_memo(...)`. Read it from `workflow.info().raw_memo` inside a workflow or `await desc.memo()` / `await desc.memo_value(key, default)` from a client. Memos are not searchable.
- **Listing.** `client.list_workflows("WorkflowType='GoalLoopWf' AND ExecutionStatus='Running' AND MissionId='M1'", limit=, page_size=1000)` is an async iterator of `WorkflowExecution`. `await client.count_workflows(q)` counts them. The filter syntax is SQL-like. `ExecutionStatus = 'Paused'` is supported. [list-filter]
- **Default search attributes worth using:**
  - `WorkflowId`, `WorkflowType`, `RunId`, `ExecutionStatus`, `StartTime`, `CloseTime`, `TaskQueue`
  - `HistorySizeBytes`
  - `HistoryLength` (closed runs only)
  - `TemporalReportedProblems` (workflow task failures)
  - `TemporalWorkerDeploymentVersion`
  - `ParentWorkflowId` and `RootWorkflowId` are named in the docs but missing from the default-attributes table. Whether they can be queried on your server version is **UNVERIFIED**.
  [search-attributes]
- **`describe()`** returns `WorkflowExecutionDescription`, which extends `WorkflowExecution`. Fields:
  - `id`, `run_id`, `workflow_type`, `status`, `task_queue`, `namespace`
  - `start_time`, `execution_time`, `close_time`
  - `history_length`
  - `parent_id`, `parent_run_id`, `root_id`, `root_run_id`
  - `typed_search_attributes`, `raw_info`, `raw_description`
  - Async methods: `memo()`, `memo_value()`, `static_summary()`, `static_details()`
  - Live, updatable details come from `workflow.set_current_details(str)`.
- **`WorkflowExecutionStatus`:** `RUNNING, COMPLETED, FAILED, CANCELED, TERMINATED, CONTINUED_AS_NEW, TIMED_OUT`. The Python enum in 1.34.0 has **no `PAUSED`** member, even though the list filter supports `'Paused'`. How a paused run shows up in `describe().status` is **UNVERIFIED**.

---

## 5. Nexus

- **GA status for Python: GA.** Temporal's Replay 2026 post (2026-05-06) says Nexus is "now Generally Available for the Python SDK, and in Public Preview for the TypeScript SDK and .NET SDK". [replay-2026] Exceptions that are still experimental or pre-release:
  - the new "Temporal Operation Handler" developer experience
  - Nexus-backed Standalone Activities
  - Standalone Nexus Operations (1.28+, needs CLI 1.9.0+)
  - system Nexus `signal_with_start_workflow`
  - `NexusSerializationContext`
  [py nexus feature-guide; standalone-operations; release notes]
- **Contract.** Define the service with `@nexusrpc.service` and `nexusrpc.Operation[In, Out]` fields. The SDK pins `nexus-rpc==1.4.0`.
- **Handler.**
  - Decorate the class with `@nexusrpc.handler.service_handler(service=Svc)`.
  - Use `@nexusrpc.handler.sync_operation` for synchronous operations, which must respond in under 10 s.
  - Use `@temporalio.nexus.workflow_run_operation` for asynchronous operations; it returns `await ctx.start_workflow(...)`.
  - Register with `Worker(..., nexus_service_handlers=[Handler()])`.
- **Caller.** `workflow.create_nexus_client(service=Svc, endpoint="ep")` returns a `workflow.NexusClient`. Call it with `.execute_operation(...)` / `.start_operation(...)`.
- **Endpoint.** `temporal operator nexus endpoint create --name ep --target-namespace ns --target-task-queue tq`.
- **Limits.** Async operations have a maximum schedule-to-close of 60 days. A workflow can have 30 Nexus operations in flight. [nexus; cloud limits]
- **Usefulness for cross-mission linking:**
  - Nexus earns its cost only when missions live in **different namespaces** or are owned by different teams and need a typed contract. That case needs endpoints, a handler worker, and an open caller workflow waiting on the result.
  - Inside one namespace, it adds operational surface over a plain child workflow or a client start.
  - It does not solve "A has completed, now start B". The caller must still be running to receive the result.
  - The SDK README still says "There is no support currently for calling a Nexus operation from non-workflow code". Standalone Nexus Operations, which are experimental, partly supersede that.

```python
@nexusrpc.service
class MissionLinkSvc:
    start_mission: nexusrpc.Operation[StartMissionIn, MissionRef]

@nexusrpc.handler.service_handler(service=MissionLinkSvc)
class MissionLinkHandler:
    @temporalio.nexus.workflow_run_operation
    async def start_mission(self, ctx: WorkflowRunOperationContext, i: StartMissionIn) -> nexus.WorkflowHandle[MissionRef]:
        return await ctx.start_workflow(MissionRunWf.run, i, id=f"mission-run:{i.run_id}")
```

---

## 6. Worker

- **`Worker(...)` signature (1.34.0, abridged):**
  - Required: `client`, `task_queue`.
  - Registration: `activities=[]`, `nexus_service_handlers=[]`, `workflows=[]`.
  - Executors and runners: `activity_executor=None`, `workflow_task_executor`, `nexus_task_executor`, `workflow_runner=SandboxedWorkflowRunner()`, `unsandboxed_workflow_runner`.
  - Extension points: `plugins=[]`, `interceptors=[]`.
  - Concurrency: `max_cached_workflows=1000`, `max_concurrent_workflow_tasks`, `max_concurrent_activities`, `max_concurrent_local_activities`, `max_concurrent_nexus_tasks`, `tuner`.
  - Heartbeats: `max_heartbeat_throttle_interval=60s`, `default_heartbeat_throttle_interval=30s`.
  - Shutdown and failure handling: `graceful_shutdown_timeout=0`, `workflow_failure_exception_types=[]`, `debug_mode=False`.
  - Versioning: `use_worker_versioning=False` and `build_id`, both **deprecated** in favour of `deployment_config`.
  - Also: `deployment_config: WorkerDeploymentConfig | None`, `patch_activation_callback` (experimental, 1.31), `max_eager_activity_reservations_per_workflow_task=3`, `disable_eager_activity_execution`.
- **Worker Versioning.**
  - Configure with `deployment_config=WorkerDeploymentConfig(version=WorkerDeploymentVersion(deployment_name=, build_id=), use_worker_versioning=True, default_versioning_behavior=VersioningBehavior.PINNED|AUTO_UPGRADE|UNSPECIFIED)`.
  - Set behaviour per workflow with `@workflow.defn(versioning_behavior=VersioningBehavior.PINNED)`.
  - Temporal says Worker Versioning is "now Generally Available" (Replay 2026). The 1.34.0 `deployment_config` docstring still says "experimental". Treat the API shape as stable-but-check. Pre-2025 versioning "will be removed from Temporal Server in March 2026". [replay-2026; py versioning; configure-worker]
- **Patching.** `if workflow.patched("id"): new else: old`. Then `workflow.deprecate_patch("id")` once old histories can no longer replay. Both accept `event_groups=` (experimental). Use replay tests to validate. [py versioning]
- **Sandbox.** Use `with workflow.unsafe.imports_passed_through(): import pydantic, ...` or `SandboxedWorkflowRunner(restrictions=SandboxRestrictions.default.with_passthrough_modules("mymod"))`. `@workflow.defn(sandboxed=False)` opts a workflow out. [py sandbox]
- **Interceptors and tracing.** `temporalio.contrib.opentelemetry` offers two options:
  - `OpenTelemetryPlugin(add_temporal_spans=...)` with `create_tracer_provider()`. It is **experimental**, gives replay-safe spans with real durations, and is registered on the Client only.
  - The older `TracingInterceptor()`, whose workflow spans have no duration.
  - 1.32 added `ReplaySafeMeterProvider` / `ReplaySafeLoggerProvider`.
  [py observability#tracing]
- **Pydantic.** `from temporalio.contrib.pydantic import pydantic_data_converter`, then `Client.connect(..., data_converter=pydantic_data_converter)`. 1.32 caches type adapters (up to 1024 per converter, LRU). [py data-conversion; release 1.32]
- **Agent integrations relevant to the provider lifecycle** (in 1.34.0 contrib):
  - `temporalio.contrib.deepagents.DeepAgentsPlugin` / `create_temporal_deep_agent` (**pre-release**). LLM calls and I/O tools run as activities while the agent loop runs in the workflow. Human-in-the-loop uses a Query plus an **Update** carrying `Command(resume=...)`. `run_deep_agent(..., state_snapshot=)` handles continue-as-new. Requires deepagents `>=0.7,<0.8`. Durable checkpointers are "not replay-safe" inside a workflow.
  - `temporalio.contrib.langgraph` (**Public Preview**). Use `InMemorySaver`; "third-party checkpointers ... are not needed". Interrupts map to query + signal.
  - `temporalio.contrib.workflow_streams` (**experimental / Public Preview**). A durable stream addressed by offset, built on Signals (publish), Updates (subscribe long-poll) and a Query (offset). It is about 100 ms per round trip, dedupes publishes, and offers `WorkflowStreamClient.from_within_activity()` and `WorkflowStream.continue_as_new(...)`.
  [deepagents; langgraph; workflow-streams]

---

## 7. Timers and schedules

- `await workflow.sleep(duration, summary=...)` or `await asyncio.sleep(s)` both create durable server timers.
- `asyncio.wait_for(coro, timeout)` also creates a timer.
- `workflow.now()` returns UTC workflow time. `workflow.time()` returns loop time; use it for `call_at` / `timeout_at`.
- `workflow.uuid4()` and (since 1.32) `workflow.uuid7()` are deterministic.
- `wait_condition(timeout=)` is the idiomatic "wait for command or timeout".

```python
try:
    await workflow.wait_condition(lambda: self.cancel_requested or self.new_turn, timeout=timedelta(minutes=10))
except asyncio.TimeoutError:
    await workflow.execute_activity(poll_status, self.run_ref, start_to_close_timeout=timedelta(seconds=30))
```

Schedules:

```python
await client.create_schedule("nightly-eval", Schedule(
    action=ScheduleActionStartWorkflow(EvalWf.run, args, id="eval-wf", task_queue="mc"),
    spec=ScheduleSpec(intervals=[ScheduleIntervalSpec(every=timedelta(hours=24))]),
    state=ScheduleState(note="nightly eval")))
```

Other calls: `client.get_schedule_handle(id)` and `client.list_schedules()`. Schedule-started runs get the `TemporalScheduledById` and `TemporalScheduledStartTime` search attributes. [py schedules; search-attributes]

---

## 8. Design questions for Mission Control

Context taken from the repo: ADR-0004 makes Temporal the only scheduler, fed by a transactional outbox, with no distributed transaction; all I/O runs in activities. The glossary defines Fork as "A new run or mission created from a snapshot. It never clones in-flight commands or active children." The topology is a root workflow per mission run, a family workflow (StageGraph / GoalDirected), and an operation workflow per unit, with commands delivered as Updates.

### (a) vs (b): observing a provider run

| | (a) Long-lived observe activity + heartbeat offset | (b) Short poll activity + `workflow.sleep` |
|---|---|---|
| Latency | Real time (SSE) | Poll interval |
| History cost | About 3 events per activity no matter how long the stream runs. That heartbeats add no history events is **UNVERIFIED** but matches the docs' "persists on the server" | About 5 events per iteration (activity scheduled, started, completed, plus timer started, fired). At 30 s polling that reaches 10,240 events in about 17 h, so it needs continue-as-new |
| Resume after crash or deploy | `heartbeat_details` holds the last offset, which can lag because of throttling. Resume is at-least-once and the provider must support resume (**UNVERIFIED** for Cursor) | Stateless. Each poll reads authoritative status |
| Cancel delivery | Only on heartbeat, up to about 0.8 × `heartbeat_timeout` late | Immediate between polls; the workflow simply stops polling |
| Worker cost | Holds one activity slot for the whole run. Worker restarts interrupt it (use `is_worker_shutdown`) | No long-held slots |

**Recommendation: a hybrid "segmented observer".** Run (a), but bound each attempt.

- Set `start_to_close_timeout` to about 30–60 min and `heartbeat_timeout` to about 30 s. Heartbeat the provider event ID after each journaled event.
- Write every event to the application journal idempotently, keyed on the event ID. Do not return a stream through workflow history.
- At the segment deadline, return `{done: False, offset}`. The operation workflow then re-schedules the activity with that offset and checks `is_continue_as_new_suggested()` between segments.
- Use a short (b)-style `poll_status` activity as a **reconciler**:
  - after an activity fails non-retryably
  - after a cancel
  - for providers or bridges with no resumable stream, such as the local bridge if it restarts
- Use `temporalio.contrib.workflow_streams` (experimental) only if the UI should subscribe through Temporal rather than through the application database.

### (c) Delivering a cancel to a running activity

1. The operator command arrives as an Update with `id = command_id`.
   - The validator rejects duplicates and invalid states. Rejections cost no history.
   - The handler sets `self.cancel_requested = True` and returns an acknowledgement. Keep the handler synchronous and atomic.
2. The main loop wakes on `wait_condition`, then calls `observe_handle.cancel()`. Start the observer with `cancellation_type=ActivityCancellationType.WAIT_CANCELLATION_COMPLETED` so the workflow waits for its cleanup.
3. The observer catches `asyncio.CancelledError`.
   - Only if `activity.cancellation_details().cancel_requested` is true does it call the provider's cancel. **Not** on `worker_shutdown`, `paused` or `reset`. Worker shutdown should leave the provider run alive and resume on retry.
   - Then it re-raises.
4. Do not rely only on step 3: the observer may be dead or between retries when the cancel arrives. Always follow with an **idempotent `cancel_provider_run` activity**, which has its own retries, and then a `poll_status` reconcile until the provider reports a terminal state.
5. Before the workflow completes or continues-as-new, call `await workflow.wait_condition(workflow.all_handlers_finished)`.

Caveats:

- Cancel latency is bounded by heartbeat throttling. To shorten it, use a smaller `heartbeat_timeout`.
- The same command must not be sent more than 10 Updates in flight or 2,000 Updates per run, so continue-as-new before reaching that.
- Carry `seen_cmds` across continue-as-new, because server Update-ID dedupe is per run.

### (d) Fork from a snapshot: new workflow vs Temporal reset

| | New workflow seeded from persisted state | `temporal workflow reset` |
|---|---|---|
| Original run | Untouched and can keep running | **Terminated**: a reset terminates the current run |
| Parallel branches | Yes, each fork gets a new Workflow ID | No: one open run per Workflow ID, so a reset replaces rather than branches |
| Inputs or plan edits | Free: fork input is new data | Not possible; history is copied verbatim up to the reset point |
| Side effects | Only what the fork chooses to do | Activities after the reset point **re-execute** (provider runs, pushes) unless the code guards against them |
| In-flight commands and children | Not cloned, which matches the glossary | Signals and Updates after the point are **reapplied by default** unless `--reapply-exclude`. Children are not carried |
| Lineage | Explicit: memo or search attributes such as `ForkedFromRunId` / `ForkedFromCheckpointId` | Implicit: same Workflow ID, new Run ID, `original_execution_run_id` preserved |
| Determinism | Not involved | New code must replay the copied prefix |

**Recommendation.**

- Implement Fork as a **new workflow** with ID `mission-run:{new_run_id}`, started through the outbox from a sealed checkpoint in the application database. Record lineage in memo and search attributes.
- Keep `reset` as an **operator-only recovery tool** for nondeterminism, bad deploys or corrupted state. Use `reset with-workflow-update-options` together with `--reapply-exclude All` or `Update` as appropriate, and record the event in the application ledger.
- Never expose reset as the product's "fork".

### (e) Chaining linked missions (A completes, B starts with A's outputs)

| Option | Fit |
|---|---|
| Child workflow | A must stay open to parent B. With `ABANDON`, B outlives A, but A cannot complete "before" B starts in any meaningful sense. Same namespace only, and it adds to the parent's history. **Good for the hierarchy within one run** (root, then family, then operation), not for sequencing missions. |
| Signal-with-start | Atomic start-or-signal and idempotent through a deterministic Workflow ID. It can be issued from A's last activity (client) or, experimentally, from workflow code. There is no result or acknowledgement beyond delivery, and A's output must fit in 2 MB, so pass references. Viable, but it puts the business transition inside Temporal. |
| Update-with-start | Gives an acknowledgement and result, but is **not atomic**. It is good for "start B and get B's accepted plan back". |
| Nexus | GA in Python and cross-namespace with a typed contract, but the caller workflow must be open to await the result. **Only worth it if missions are split across namespaces or teams.** |
| Application outbox | A's completion and the "start B" intent are committed in **one database transaction**. The relay calls `client.start_workflow(MissionRunWf.run, ref_to_A_outputs, id=f"mission-run:{b_run_id}", id_conflict_policy=USE_EXISTING, id_reuse_policy=REJECT_DUPLICATE)`, which is idempotent. **This matches ADR-0004.** |

**Recommendation.**

- Use the **application outbox** for mission-to-mission links. The Workflow ID should come from the link or new run ID so relay retries are no-ops. Use `USE_EXISTING` (handles are run-chain-scoped on Server 1.32+) and pass artifact references, not payloads.
- Use **child workflows** only inside a mission run.
- Keep **Nexus** in reserve for a future multi-namespace split.
- Use signal-with-start or update-with-start only as the relay's transport into an already-running target, for example delivering "upstream completed" to a waiting family.

---

## 9. SDK delta: locked 1.30.0 vs current 1.34.0 (from release notes)

- **1.31**
  - Breaking: payload limits moved from `DataConverter` to `Client.connect(payload_limits=PayloadLimitsConfig(...))`, and the fields were renamed `payloads_warn_size` and `memo_warn_size`.
  - Added `max_eager_activity_reservations_per_workflow_task` and an experimental `patch_activation_callback`.
- **1.32**
  - `workflow.uuid7()`.
  - Replay-safe OpenTelemetry meter and logger providers.
  - The Pydantic type-adapter cache.
- **1.33**
  - Standalone Activities are GA.
  - `ActivityHandle` gets pause, unpause and options operations (experimental).
  - Breaking renames in `client.ActivityExecution*`.
  - `deepagents` result-cache fix, gated behind the patch `deepagents.retire-result-cache`.
- **1.34**
  - `workflow.Info.original_execution_run_id`.
  - `WorkflowAlreadyStartedError.first_run_id`.
  - Experimental Event Groups (`workflow.create_event_group`).
  - The `deepagents` extra moves to `>=0.7,<0.8`.
  - A fix so `USE_EXISTING` handles use the server's first-execution Run ID (Server 1.32.0+).

---

## Recommendations for Mission Control (summary)

1. **Commands are Updates with `id = command_id`.**
   - Validators reject duplicates and invalid states.
   - Handlers only mutate state; the main loop does the work.
   - Carry `seen_cmds` across continue-as-new.
   - Drain with `all_handlers_finished` before completing or continuing-as-new.
   - The outbox relay uses `start_update(wait_for_stage=ACCEPTED)` and re-attaches through `get_update_handle_for`.
2. **Budget for history:** continue-as-new on `is_continue_as_new_suggested()`, at the latest before about 10k events, 2,000 Updates or 10,000 Signals.
3. **Observe providers with segmented heartbeat activities** that journal events idempotently, plus a poll reconciler. Never stream tokens through workflow history.
4. **Cancel = Update → flag → activity cancel (`WAIT_CANCELLATION_COMPLETED`) → idempotent `cancel_provider_run` activity → poll to terminal.** Only cancel the provider when `cancellation_details().cancel_requested` is true.
5. **Fork = new workflow from a sealed checkpoint.** Reset is reserved for operator recovery, with explicit reapply-exclude choices.
6. **Mission chaining goes through the transactional outbox** with deterministic Workflow IDs and `USE_EXISTING`. Children stay inside one run. Nexus waits for a multi-namespace future.
7. **Visibility.** Register `MissionId`, `MissionRunId`, `FamilyKind`, `OperationId` and `ForkedFromRunId` as Keyword search attributes. Put large or non-indexed context in memo or the application database. Use `set_current_details` for live operator text.
8. **Upgrade** the lock from 1.30.0 to 1.34.x. Check the 1.31 `payload_limits` move and, if you adopt Deep Agents, the 1.33/1.34 `deepagents` changes. Adopt Worker Deployment versioning (`deployment_config`) and replay tests in CI.
9. **Leave the experimental and pre-release features out of the critical path:** Workflow Streams, OpenTelemetryPlugin, Workflow Pause, `workflow.signal_with_start_workflow`, and the DeepAgents plugin.

---

## Citations (all read 2026-10-07)

**PyPI and GitHub**

1. PyPI `temporalio` JSON (version 1.34.0, upload times): https://pypi.org/pypi/temporalio/json
2. sdk-python releases 1.28.0–1.34.0 (release notes quoted in §2, §5, §6, §9): https://github.com/temporalio/sdk-python/releases
3. sdk-python source at tag 1.34.0 (signatures and docstrings quoted): https://github.com/temporalio/sdk-python/tree/1.34.0. Files used:
   - `temporalio/workflow/_context.py`
   - `_handlers.py`
   - `_workflow_ops.py`
   - `_activities.py`
   - `_definition.py`
   - `temporalio/client/_client.py`
   - `temporalio/client/_workflow.py`
   - `temporalio/activity.py`
   - `temporalio/common.py`
   - `temporalio/worker/_worker.py`
   - `temporalio/worker/_replayer.py`
   - `temporalio/nexus/system/workflow_service/operations/signal_with_start_workflow.py`
   - `pyproject.toml`
   - `README.md` (Timers, Asyncio, Activity Context, Heartbeating and Cancellation, Nexus)
   - `temporalio/contrib/{deepagents,workflow_streams}/README.md`
4. Temporal CLI `commands.yaml` at v1.9.1 (reset flags; `server start-dev --search-attribute`): https://github.com/temporalio/cli/blob/v1.9.1/internal/temporalcli/commands.yaml
5. Temporal API protos (main): `ResetWorkflowExecutionRequest/Response` and `ResetReapplyExcludeType` / `ResetReapplyType`.
   - https://github.com/temporalio/api/blob/master/temporal/api/workflowservice/v1/request_response.proto
   - https://github.com/temporalio/api/blob/master/temporal/api/enums/v1/reset.proto
   - Field presence was cross-checked against the SDK-vendored `request_response_pb2.pyi` in 1.34.0.
6. Temporal docs source at commit 5d9703a: https://github.com/temporalio/documentation/tree/5d9703a237a9efc9a7da48ac09f79526823b8182/docs. The published pages are listed below.

**docs.temporal.io: Python developer guide**

7. Python message passing: https://docs.temporal.io/develop/python/workflows/message-passing
8. Python continue-as-new: https://docs.temporal.io/develop/python/workflows/continue-as-new
9. Python cancellation and reset: https://docs.temporal.io/develop/python/workflows/cancellation
10. Python activity timeouts and heartbeats: https://docs.temporal.io/develop/python/activities/timeouts
11. Python observability (search attributes, tracing): https://docs.temporal.io/develop/python/platform/observability
12. Python versioning: https://docs.temporal.io/develop/python/workflows/versioning
13. Python sandbox: https://docs.temporal.io/develop/python/best-practices/python-sdk-sandbox
14. Python data conversion (pydantic): https://docs.temporal.io/develop/python/best-practices/data-handling/data-conversion
15. Python testing and replay: https://docs.temporal.io/develop/python/best-practices/testing-suite
16. Python schedules: https://docs.temporal.io/develop/python/workflows/schedules
17. Python Workflow Streams: https://docs.temporal.io/develop/python/workflows/workflow-streams
18. Python Nexus feature guide and standalone operations: https://docs.temporal.io/develop/python/nexus/feature-guide and https://docs.temporal.io/develop/python/nexus/standalone-operations
19. Deep Agents integration: https://docs.temporal.io/develop/python/integrations/deepagents
20. LangGraph integration: https://docs.temporal.io/develop/python/integrations/langgraph

**docs.temporal.io: concepts and reference**

21. Handling messages: https://docs.temporal.io/handling-messages
22. Sending messages: https://docs.temporal.io/sending-messages
23. Event and Reset: https://docs.temporal.io/workflow-execution/event#reset
24. Workflow ID and Run ID (lineage, reuse and conflict policies): https://docs.temporal.io/workflow-execution/workflowid-runid
25. Child workflows: https://docs.temporal.io/child-workflows
26. Parent close policy: https://docs.temporal.io/parent-close-policy
27. Detecting activity failures (throttling): https://docs.temporal.io/encyclopedia/detecting-activity-failures
28. Local activity: https://docs.temporal.io/local-activity
29. Search attributes: https://docs.temporal.io/search-attribute
30. List filter: https://docs.temporal.io/list-filter
31. Temporal Cloud limits: https://docs.temporal.io/evaluate/cloud/limits
32. Worker best practices (2 MB / 50 MB): https://docs.temporal.io/best-practices/worker
33. CLI workflow reference: https://docs.temporal.io/cli/command-reference/workflow
34. CLI operator reference: https://docs.temporal.io/cli/command-reference/operator
35. Events reference (`WorkflowExecutionUpdateAdmitted`): https://docs.temporal.io/references/events
36. Recover pinned workflows (reset with `--reapply-exclude`): https://docs.temporal.io/production-deployment/worker-deployments/recover-pinned-workflows
37. Worker versioning, configure worker: https://docs.temporal.io/production-deployment/worker-deployments/worker-versioning/configure-worker
38. Workflow Pause: https://docs.temporal.io/encyclopedia/workflow/workflow-pause
39. Nexus overview: https://docs.temporal.io/nexus
40. Design pattern, continue-as-new (drain signals): https://docs.temporal.io/design-patterns/continue-as-new

**temporal.io blog**

41. Replay 2026 announcements (Nexus GA for Python; Worker Versioning GA; Workflow Streams Public Preview), published 2026-05-06: https://temporal.io/blog/replay-2026-product-announcements
42. Nexus GA, published 2025-03-06 (Go and Java at the time): https://temporal.io/blog/temporal-nexus-now-available

**Local repository context**

43. `mission-control/docs/adr/0004-temporal-sole-scheduler-transactional-outbox.md`, `mission-control/GLOSSARY.md` (Fork and Snapshot), and `mission-control/uv.lock` (temporalio 1.30.0).
