---
type: Specification
title: GENERAL Mission Control runtime and public contracts
description: "Current expansion contracts are in context/state/control, catalog/environments, knowledge services, experience/streams and JSON Schema seeds. The wire MissionEvent is normalized in workflow 09, including authorization…"
tags: [mission-control, spec, normative]
---
# GENERAL Mission Control runtime and public contracts

Current expansion contracts are in [context/state/control](expansion/CONTEXT-STATE-AND-CONTROL.md), [catalog/environments](expansion/CATALOG-AND-ENVIRONMENTS.md), [knowledge services](expansion/KNOWLEDGE-SERVICES.md), [experience/streams](expansion/EXPERIENCE-AND-STREAMS.md) and [JSON Schema seeds](expansion/schemas.json). The wire MissionEvent is normalized in workflow 09, including authorization scope, execution/source, UUIDv7 event identity and ledger_commit_id. MC-P001 generates all complete schemas before implementation. Safe queue/interrupt/respond/revision operations and bidirectional context projection cannot inherit unqualified stock async semantics.

Canonical consolidation 3, 2026-10-02. Normative companion to [SPECIFICATION.md](SPECIFICATION.md) and [DATABASE.md](DATABASE.md). These are target contracts, not claims that the new runtime exists. The current Biotech proofs supply invariants and implementation candidates; [EVIDENCE.md](EVIDENCE.md) records their limits.

## Python and naming contract

Python >=3.12 is the runtime, including compiler, application handlers, Temporal worker, harness adapters and CLI. Use Pydantic v2 `ConfigDict(extra="forbid", frozen=True)` for admitted value objects; mutable drafts are replaced under version control. Use `StrEnum`/`Literal`, discriminated unions and `typing.Protocol`; no SDK object crosses a public contract. TypeScript is a generated client and existing UI language. Use Python-compatible identifiers and lower_snake wire keys. Keep decimal costs as integer micros and reject nonfinite numbers. Python 3.12 needs a pinned UUIDv7 library; do not assume `uuid.uuid7` is in its standard library.

Canonical JSON for digests: normalize text identifiers to NFC, reject key collisions after normalization, UTF-8 encode, lexicographically sort object keys, no insignificant whitespace, no NaN/Infinity, integers within signed 64-bit bounds. Serialize UTC timestamps as ISO-8601 with `Z`; decimals as canonical strings. Preserve semantically ordered arrays; sort only fields explicitly declared set-like in their contract. The schema version, compiler version and resolved asset versions participate in digests. Generate cross-language golden byte/digest fixtures rather than relying on language-default serialization.

| Current Biotech name | GENERAL name | Preservation requirement |
| --- | --- | --- |
| BellLabs run / RunPlan | Mission Run / immutable execution-binding manifest | Clean-break naming; preserve behavioral invariants, no old-engine compatibility required |
| `BellLabsRunWorkflow` | `MissionRunWorkflow`, registered `mc.mission_run.v1` | Sole root for newly admitted GENERAL runs |
| StageGraph / GoalDirected family | Stage Graph / Goal Loop activation | Retain accepted scheduling/identity invariants; GoalDirected is a reuse candidate, not automatic complete Goal Loop conformance |
| `OperationWorkflow` | `OperationWorkflow`, registered `mc.operation.v1` | Durable bounded operation; generalize envelope, retain semantic effect identities |
| run epoch / generation / technical segment | execution epoch / execution generation / technical segment | Distinct from logical retries, cycles, turns and revisions |
| AgentThreadKey / provider task | agent session / harness execution | Scoped native identity in adapter mapping, not business authority |
| linked run | subordinate execution or Child Mission Invocation | Classify by independent goals/governance; never relabel automatically |

New root workflow ID is `mc/{installation_id}/{application_id}/run/{run_id}`; activation/operation child IDs include the corresponding scoped semantic activation/attempt ID. Store native Temporal run IDs in `runtime_segment`. The clean break starts fresh runs; subsequent releases must replay the new system's own histories with compatible workflow registrations.

Contract seed (implement as frozen Pydantic types; each referenced type has a generated versioned JSON Schema):

```python
from typing import Literal, Protocol
from uuid import UUID
from pydantic import BaseModel, ConfigDict, Field

class Contract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

class ResourceRef(Contract):
    installation_id: UUID
    application_id: str
    tenant_id: UUID
    resource_id: UUID

class RunStart(Contract):
    schema_version: Literal["mc.run_start.v1"]
    request_id: UUID
    expected_version: int = Field(ge=1)
    revision_id: UUID
    input_manifest_ref: str
    input_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")

class ForkRequest(Contract):
    schema_version: Literal["mc.fork.v1"]
    request_id: UUID
    expected_version: int = Field(ge=1)
    expected_generation: int = Field(ge=1)
    checkpoint_id: UUID
    checkpoint_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    target_kind: Literal["run", "mission"]
    reason: str = Field(min_length=1, max_length=4096)
    target_definition_ref: str | None = None
    budget_profile_ref: str
```

Resource references are parsed/authorized, not arbitrary fetch URLs. Domain references additionally include issuer, contract/version, immutable version/digest and required scopes. `target_kind=mission` requires a validated target definition; `run` requires the same mission intent and immutable revision/input checkpoint. Fork cannot silently change authority, inputs or mission success meaning. A changed program requires committed revision/new mission admission and independently validated carry-forward.

Before Step 1 exit, generate full schemas for every behavior, command payload and referenced contract in SPECIFICATION. Generic JSON dictionaries are allowed only in bounded data schemas, never as an unvalidated executable behavior or policy. Example schemas must validate using the actual lock. Contract defaults must be explicit and versioned.

## Lifecycle identity and state transitions

| Operation | Precondition and resulting semantic identity | Durable outcome |
| --- | --- | --- |
| inspect | Authorized source read; no identity changes | Safe state/checkpoint/receipt/lineage summaries; no execution |
| technical retry | Nonterminal run/activation/semantic attempt with recorded recoverable interruption; same revision, binding, inputs, run, activation, semantic attempt and effect keys | New recovery request/technical invocation; reconcile ambiguous effects first; counters and spend governors remain in force |
| semantic retry/remediation | Quality rejection or changed logical action; allowed authored revisit/optimization/loop policy | New semantic attempt/action ordinal or bounded round; recorded lineage and new effect key; not an automatic infrastructure retry |
| fork | Sealed authorized checkpoint; all selected facts/artifacts valid; fresh budget/grant admission | New run, native session/workspace, epoch/generation/segment one; immutable source and fork lineage; no live-child/message/effect ownership copying |
| diagnostic replay | Captured history/checkpoint with matching code/contract, isolated diagnostic mode | New diagnostic report/thread where needed; no business settlement or effect claims; model/tool reruns disabled unless explicitly admitted as isolated new diagnostic work |
| Temporal history replay | Worker reconstructs workflow from recorded history | Same semantic run and effects; activity results replayed, not re-executed merely by history reconstruction |
| pause | Authorized active scope; stop new launches and qualify quiescence | Command accepted -> reconciling -> applied; run becomes paused only after checkpoint/frontier/effects/children satisfy pause policy |
| resume | Paused scope; checkpoint/bindings/grants/usage valid now | Same run/activation/semantic attempt; fresh session/workspace if needed; generation advances when old execution is fenced; resumed counters do not reset |
| cancel | Authorized scope, monotonic desired stop | Stop releases, cancel owned resources, reconcile effects/children/usage; complete cancelled only on settlement; uncertain case remains visible |
| queue instruction | Committed policy allows bounded instruction; pinned target/generation | Delivery report names wait-then-send/turn boundary; does not rewrite structural program |
| interrupt and inject | Qualified cancel-and-replace or native control | Fenced old turn; reconcile interrupted effects, then next turn; receipt cannot claim delivery from mere enqueue |

Run lifecycle transitions: pending -> running -> paused -> running -> completed; pending may complete cancelled/execution_failed without launch. Activation: pending -> ready -> running -> waiting -> running -> completed; cancelled/skipped/superseded paths can complete from nonterminal states through an attributable decision. Phase describes behavior-specific progress (`executing`, `awaiting_human`, `awaiting_evidence`, `reconciling`, `hydrating`); lifecycle/phase/outcome are independent. Initial common Run outcomes are `accepted`, `not_accepted`, `stopped_by_policy`, `governor_exhausted`, `no_progress`, `revision_required`, `cancelled`, `execution_failed`, `stalemate`, `quorum_unreachable`, `threshold_not_reached`. Completed is irreversible; further work starts a new run/revision, not a terminal-row mutation. A semantic remediation can reactivate an enclosing authored loop, never overwrite a settled activation.

The application retry classifier below refines the workflow suite's broader Attempt Failure Class (for example, a provider error may be transient or terminal). Persist the workflow failure class and the retry disposition separately; a broad provider_error label alone cannot authorize repeating an uncertain effect. Retry policy classifies `transient_transport`, `worker_lost`, `provider_busy`, `invalid_input`, `authority_denied`, `quality_rejected`, `budget_exhausted`, `effect_uncertain`, `checkpoint_invalid`. Only the first three permit bounded technical retry, with provider_busy honoring Retry-After and stable launch identity. The others fail/hold/govern according to policy. Record maximum attempts, initial/max backoff, multiplier, start-to-close/schedule-to-close/heartbeat timeout and total deadline in a pinned execution profile. No unlimited retry: installed Temporal `RetryPolicy.maximum_attempts=0` means unlimited. Technical retry is not a blanket model rerun after an ambiguous paid request.

No public rollback rewrites authoritative history. Epoch rollover is unsupported until a separately accepted semantic policy exists. Diagnostic replay is never advertised as undo or fork. Replaying external native event streams is cursor recovery only; deduplication cannot itself prove an external side effect happened once.

A checkpoint manifest includes scoped source identity, revision/binding/input digests, sealed artifact manifests, loop state/journal frontier, event/command accepted frontier, governor counters, unresolved effect/child/usage/reservation references, native checkpoint format/version and workspace snapshot. Before sealing: freeze release, fence producers, reconcile the declared frontier, register bytes, then commit validation. A manifest is not a copied SQL snapshot. Fork carries immutable eligible facts, not outstanding obligations; external effects already performed remain historical facts and cannot be repeated unless the new mission explicitly authorizes a new action. Checkpoint-copy uncertainty uses a persisted saga intent and deterministic target identity.

## Harness interface

The following is the GENERAL interface, not a claim about an SDK's method names. Implement typed request/result models behind it, using the selected SDK calls internally.

```python
class AgentHarness(Protocol):
    async def describe(self, binding: "HarnessBinding") -> "HarnessCapabilities": ...
    async def prepare(self, request: "PrepareRequest") -> "PreparedExecution": ...
    async def start(self, request: "StartRequest") -> "ExecutionHandle": ...
    async def reattach(self, handle: "ExecutionHandle") -> "ObservedExecution": ...
    async def send_turn(self, request: "TurnRequest") -> "TurnHandle": ...
    async def cancel_turn(self, request: "CancelTurnRequest") -> "ControlReceipt": ...
    async def observe(self, request: "ObserveRequest") -> "ObservationBatch": ...
    async def snapshot(self, request: "SnapshotRequest") -> "SnapshotResult": ...
    async def usage(self, handle: "ExecutionHandle") -> "UsageReport": ...
    async def end_session(self, request: "EndSessionRequest") -> "CleanupReceipt": ...
```

All mutation requests carry scope, binding digest, semantic operation/idempotency key, generation, fenced lease and deadline. Handle = harness execution ID plus provider kinds/native refs, with no secret. Observation = native dedupe key/cursor, source generation, typed status/tool/usage/output fact and bounded safe payload. ObservationBatch includes next cursor, retention floor and gap/resync status. UsageReport has settled/estimated/unknown disposition per dimension, native usage identity and attributable source. CleanupReceipt includes actual resource state and outstanding obligations; a successful HTTP cancel call is insufficient.

`describe` returns each primitive and each composite control as `native`, `emulated`, `unsupported` or `unqualified`, the profile version, prerequisites and observed semantics. No adapter may lie to provide apparent parity. Common semantic fork can use fresh-session hydration; it must be reported as emulated. Native provider completion translates to an attempt execution result plus candidate, never accepted Mission Control output. Unknown native state blocks duplicate launch until reconciliation. Loss of replayable observations requires authoritative provider polling plus explicit stream-gap record.

Deep Agents uses the existing exact materializer: pinned graph, model, MCP allowlists, full skill bundle, middleware, state/context/output schemas and workspace profile. The inherited qualification record reports that version 0.7.5 exposed checkpointer/store; admission must configure durable scoped persistence rather than assume the live in-memory proof is durable. The accepted proof documents an executable sandbox/permissions limitation; host admission, tool filters, filesystem mount policy and network controls must enforce grants. Do not promise framework filesystem permissions on that executable backend.

Cursor SDK Cloud is a subsequent Python harness behind this protocol, with an exactly locked/qualified SDK and required native bridge binaries. Official [Python SDK](https://cursor.com/docs/sdk/python) describes local/cloud runtime and explicit async client. No exact Cursor version is selected by this spec; the earlier inspection did not identify a qualified Python Cursor installation, and this consolidation did not reinspect the runtime environment. The Cursor delivery stage locks and qualifies one before adapter admission. First qualify cloud placement for process-loss reattachment; local placement may be enabled after store/workspace durability qualification. Cursor is required for the complete general system, but does not block the first Deep Agents/Agent Server parity milestone. Local Cursor placement is not a required lane of this architecture.

The older [Cursor fact sheet](../runtime-facts/CURSOR_SDK_FACTS.md) records no native pause/fork and cloud steering fallback in its 1.0.31 baseline. It is historical evidence; qualify the chosen Python SDK against [current official docs](https://cursor.com/docs/cloud-agent/api/endpoints). Initially implement queue instruction at a turn boundary, pause as checkpoint/cancel/restore where qualified, and fork as new session with immutable context/patch hydration. Never equate native reattachment with restoring a cancelled run. Verify create/send idempotency, native recovery handles, MCP configuration persistence, cancellation terminality, event retention and usage under the actual locked client. A cloud workspace is not a LangSmith workspace.

Coding workspace binding pins repository issuer/URL, base commit, branch/worktree ownership, allowed paths, network policy, test commands, publication policy and immutable patch snapshot. Repository write access cannot imply production/database access. Patches, test logs and PR refs become artifacts only after custody/schema validation. Concurrent missions use separate worktrees; observed unrelated edits are preserved. Provider auto-PR, push, merge and deployment defaults must match granted capabilities, with auto-publication disabled otherwise.

## Public operation additions and wire protocol

All paths below follow `/v1/applications/{application_id}`. HTTP/CLI/MCP call the same authenticated handler. CLI flags precede/follow subcommands consistently; `--request-file` reads strict JSON, `--json` writes one JSON object or NDJSON for events, `--wait` polls the returned resource with a bounded deadline.

| HTTP operation | CLI | MCP tool | Scope / response |
| --- | --- | --- | --- |
| `GET /runs/{run_id}/inspection` | `run inspect <id>` | `mission_run_inspect` | mission.read; Inspection |
| `POST /runs/{run_id}/recoveries` kind technical_retry | `run retry <id> --request-file` | `mission_run_retry` | mission.command + capability grants; RecoveryReceipt |
| same path kind diagnostic_replay | `run replay <id> --request-file` | `mission_run_replay` | mission.read + authorized diagnostic history access; RecoveryReceipt |
| `POST /runs/{run_id}/forks` | `run fork <id> --request-file` | `mission_run_fork` | mission.author + mission.start + target capability grants; ForkReceipt |
| `GET /recoveries/{recovery_id}` | `recovery get <id>` | `mission_recovery_get` | mission.read; status/result/blocked reason |
| `POST /runs/{run_id}/commands` | `command send <id> --request-file` | `mission_command_send` | mission.command; CommandReceipt |
| `GET /commands/{command_id}` | `command get <id>` | `mission_command_get` | mission.read; DeliveryReport[] |
| `GET /requests/{request_id}` | `request get <id>` | `mission_request_get` | original action read grants; RequestReceipt |
| `POST /attempts/{attempt_id}/completion-candidates` | executor-only reporting | `mission_completion_candidate_submit` | attempt/generation-scoped execution.report; candidate receipt |

Recovery input contains schema_version, request_id, expected_version, expected_generation, kind, reason and source checkpoint/history ref/digest. Fork uses ForkRequest above. Command input contains schema_version `mc.command.v1`, request_id, expected_version, expected_generation, target `{kind,id}`, kind, typed payload and optional deadline. Kinds: `pause`, `resume`, `cancel`, `queue_instruction`, `interrupt_and_inject`; unsupported kinds reject. Structured edits use proposals, never command text interpreted as new authority.

HTTP receipt status: 201 for synchronously created resources; 202 for admitted asynchronous commands/recovery; 200 for matching request replay/read. Errors: 400 malformed, 401 unauthenticated, 403 scope denied, 404 absent/unreadable resource, 409 version/idempotency/generation conflict, 422 semantically invalid/unsupported, 429 capacity/rate limit with Retry-After, 503 app unavailable. RequestEnvelope has request_id, resource_ref, version, disposition, operation_ref and available result/error ref. A 202 only proves durable admission; command delivery/settlement is read separately. CLI exits 0 for successful read/admission, 2 invalid arguments/definition, 3 denied, 4 version/idempotency conflict, 5 infrastructure/unavailable, 6 wait timeout or blocked terminal result. Structured error bodies are preserved.

Idempotency scope = installation/app/tenant/actor/action/request_id; effect scope is separate and actor-independent after admission. DATABASE uses actor identity in the scoped request uniqueness key as well as action/key; this prevents receipt collision between distinct principals. Payload digest includes target/body and expected versions, excludes transport authorization headers. Replay checks present authorization, returns original identity/result and never executes again. HTTP Idempotency-Key, when supplied, must equal body request_id. POST payload size, artifact size, node/fanout and request rate bounds come from a pinned policy profile; absence is invalid configuration.

SSE `id` is the per-mission seq, with event body schema `mc.event.v1` carrying resource scope, event ID/type/version, seq, commit ID, actor, execution refs, causation and payload/ref. `after_seq` and Last-Event-ID must agree when both supplied. Client reconnect deduplicates by mission/seq; server returns CURSOR_EXPIRED with retained floor and resync snapshot version if the cursor is older. Native provider cursors never become Mission Control event seq.

Canonical skill manifest includes skill version, service contract range, full file digests and operation catalog digest. It teaches describe/context -> draft/validate -> proposal/commit/activate -> start -> observe/review/intervene -> verify completion; unknown write responses use request lookup. It distinguishes retry/fork/replay and names required receipts. The skill can request only caller-authorized operations, cannot manufacture human review, and uses app guides as data. API, CLI, MCP and skill examples are tested against one operation catalog. No separate skill-owned kernel exists.

## Failure recovery and observability

Workflow code performs deterministic state transitions only. Signals/Updates carry already persisted intent IDs and scoped generation, not unverified authority. The root must reconcile DB command frontiers after restart; no lost Update is recovered by changing command identity. Start with `WorkflowIDReusePolicy.REJECT_DUPLICATE` and `WorkflowIDConflictPolicy.USE_EXISTING`, then verify the existing workflow belongs to the admitted run/build. Continue-As-New uses `await workflow.wait_condition(workflow.all_handlers_finished)` after fencing release and persisting all relevant frontiers. Preserve active child identities; qualify child parent-close behavior and reattachment across rollover rather than accidentally cancelling them. Native Temporal run ID changes; semantic Mission Run does not. [Temporal Python guidance](https://docs.temporal.io/develop/python/workflows/continue-as-new).

Failure recovery must handle database-committed/dispatch-missing, launch-response-lost, checkpoint-copy-response-lost, artifact-upload-unregistered, output-from-stale-generation, command-delivered/ack-lost, event-gap, provider-cancel-uncertain, child-result-after-parent-terminal and unknown billing. Each creates an idempotent receipt or visible reconciliation case. Completion cannot bypass required unresolved effects, children, usage or proof. Compensation is an explicit admitted domain action, never assumed rollback.

Record request/command/run/activation/attempt/effect/checkpoint IDs, app/tenant/installation, generation/segment, binding/build/schema versions, native handles and causal event refs in structured logs/traces. Redact tokens and sensitive source content; keep PHI outside general logs/artifacts by policy. Metrics cover outbox/reconciliation lag, retries by failure class, active leases/children/workspaces, retained event floor/gaps, checkpoint restore failures, reservation vs settled/unknown spend and per-app admission capacity. Traces are linked diagnostics; the database ledger/receipts are business evidence. Readiness is per app and per advertised harness capability.

Release gates include two-app schema isolation, Deep Agents and Agent Server first with subsequent Cursor/frontier conformance, public operation parity, technical retry/fork/diagnostic replay distinction, pause/cancel/recovery, deterministic captured-history replay, stale-generation rejection and independent completion evidence. No source-presence claim can substitute for those tests.

## Required Agent Server and asynchronous subordinate protocol

Agent Server is required in both AI Engineer and Biotech. Register exact graph versions and app-bound endpoint/persistence configuration in the execution binding. The server is a bounded execution host, not a second mission scheduler. Privileged model/tool credentials stay server-side and graph tools remain filtered by the admitted grant intersection.

The common adapter exposes typed submit, inspect/observe, cancel and recover operations (contract names, not assumed vendor SDK methods). Submit contains scoped parent/subordinate identities, stable launch key, binding/graph digest, execution generation, input manifest, dependency/timeout/cancellation policy and reserved budget. The response contains native run/thread IDs and an observation cursor, never acceptance authority.

Before dispatch, transactionally record subordinate admission, reservation and outbox intent. The server adapter must map the stable launch key to recoverable native identity. A crash before acknowledgement enters submission reconciliation. Prove either provider idempotency or a durable wrapper that can resolve the lost launch without duplicate paid execution; otherwise the lane stays unqualified. Merely keeping a local request row is insufficient if the external submission can still be duplicated.

Observations deduplicate by native source identity and generation; result admission validates output custody/schema, parent policy and current authority. Required blocking children prevent completion. Parent pause/cancel preserves child/effect/usage reconciliation. Late or stale output records an observation/disposition without mutating a terminal parent. Settlement releases reservations only when usage/effects are known or governed by an explicit conservative policy.

Qualify both applications for concurrent launches, equal native IDs across isolated installations, lost submit response, Agent Server/worker restart, cancellation acknowledgement loss, stale-generation output, revoked grants, absent usage and runtime persistence outages. App-bound pools/endpoints are the initial isolation convention. No invocation routes to the other app on failure.

## Frontier provider protocol

Frontier model routes used by Deep Agents are part of the first runtime profile. Pin provider/model, route, parameter/schema support, credential reference, usage attribution and limits. Additional direct-provider profiles implement bounded Agent Executor execution under the same admission, result and accounting rules; a stateless profile may reject session continuation/fork rather than simulate unsupported native capabilities.

Provider SDK calls occur only in activities/server adapters. Persist effect/launch identity before billed work. Unknown response or usage enters reconciliation; provider substitution is not an automatic infrastructure retry. Each profile advertises qualified tools, structured outputs, cancellation and recovery semantics. This contract selects no unverified vendor version or method.
