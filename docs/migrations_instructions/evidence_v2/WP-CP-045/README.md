# WP-CP-045 implementation evidence

Disposition: `accepted`  
Recorded: 2026-08-10  
Qualification: `QUAL-CP-ASYNC-SUBAGENT-LIFECYCLE`  
Framework baseline: Deep Agents `0.7.5`, LangGraph SDK `0.3.8`, Temporal `1.30.0`

## Implemented contracts and authority

- `AsyncSubagentContract`, `AsyncSubagentExecution`, `ParentAsyncSubagentLink`, ordered
  `AsyncSubagentMessage`, and content-addressed `AsyncSubagentResultManifest` are strict canonical
  operation-execution contracts.
- `AsyncSubagentService` persists Mongo detail records, then PostgreSQL reservation/link admission,
  before the provider receives a start call. New spawning is feature-gated off by default;
  reconciliation, cancellation, and settlement remain callable after rollback.
- Migration `0016_async_subagent_parent_child_v1.sql` adds tenant-scoped parent authority,
  immutable command/fact ledgers, ordered messages, result decisions, cancellation, and settlement.
- Mongo models and the detail repository own immutable contract, execution, provider binding,
  result-manifest, and parent-link documents. Provider state never becomes lifecycle authority.
- `OperationWorkflow` preserves active subordinate identities in its durable input/result and exposes
  signal/query continuity without performing provider or database I/O.
- The remote-graph-shaped `AsyncSubagentDefinition`, `AsyncTaskKey`, runtime async-task projection,
  interventions, public schema exports, in-memory/PostgreSQL repositories, and governance registry
  entries were removed. Historical SQL tables remain inert migration provenance only.

## Deep Agents 0.7.5 mechanism

The adapter instantiates and drift-checks the exact `AsyncSubAgentMiddleware` surface:
`start_async_task`, `check_async_task`, `update_async_task`, `cancel_async_task`, and
`list_async_tasks`. Check, update, cancel, and list operate through those typed middleware tools.

The stock start tool invents a provider thread ID, leaving an unresolvable crash window after
submission. The BellLabs start wrapper therefore uses the same LangGraph SDK Agent Protocol calls
but supplies the already-persisted `child_execution_id` as `thread_id`, stores a BellLabs spawn key
in provider metadata, and reconnects an existing run before creating another. Qualification drives
this actual installed mechanism with a deterministic fake Agent Protocol client and proves that an
ambiguous retry creates one provider run. No hidden workflow type or provider-owned parent mutation
is introduced.

## Lifecycle and failure behavior

- Lifecycle follows `proposed -> admitted -> submitted/running/waiting -> completed/failed/
  cancelled/orphaned`; completion produces only a result proposal.
- `required_blocking`, `degradable_blocking`, `nonblocking`, and `advisory` have executable parent
  dependency behavior. Runtime/model output cannot weaken the frozen class.
- Parent and child messages receive independent monotonic target sequences and durable acceptance.
  Parent instructions persist before provider update; child thread content enters as an untrusted
  observation with a committed receipt. Provider application never replaces the ledger.
- Cancellation is authorized and journaled before best-effort provider cancellation. Start failure
  becomes an orphan fact; late or superseded-generation results cannot be admitted.
- Result admission is explicit `admit`, `conditionally_admit`, `reject`, or `defer`; a blocking
  child cannot settle without a result decision.
- Recovery loads the exact persisted contract into a fresh adapter and reconnects the deterministic
  provider thread/run without relying on in-memory middleware state.

## Deterministic governance classifier

`classify_async_delegation` routes operation-local bounded work to `subordinate`, independently
durable/messageable/budgeted/settled work to `operation`, and Workflow Type or independent product
authority to `linked_run`. It is pure and launches nothing.

## Requirement-to-evidence map

| Requirements | Executable evidence |
|---|---|
| REQ-CP-RUN-009 | reserve/admit before SDK submission; output cannot mutate parent before decision |
| REQ-CP-EXEC-005, 006 | stable generation/provider identities; ordered message and receipt tests |
| REQ-CP-EXEC-008 | cancellation-before-provider, orphan, late-result, recovery tests |
| REQ-CP-DA-008 | exact contract/link/execution/reservation before provider start |
| REQ-CP-DA-009 | truth table for all four frozen dependency classes |
| REQ-CP-DA-010 | monotonic parent-to-child messages and durable receipts |
| REQ-CP-DA-011 | typed result manifest, explicit admission, late/superseded rejection |
| REQ-CP-DA-012 | subordinate/operation/linked-run classifier fixtures |

Primary suite: `tests/acceptance/control_plane/test_wp_cp_045.py`.

## Verification

```text
uv run pytest tests/acceptance/control_plane/test_wp_cp_045.py -q
17 passed

uv run pytest tests/acceptance/control_plane/test_wp_cp_020.py \
  tests/acceptance/control_plane/test_wp_cp_030.py \
  tests/acceptance/control_plane/test_wp_cp_040.py \
  tests/acceptance/control_plane/test_wp_cp_045.py \
  tests/test_operation_execution.py -q
47 passed

TEST_APPLICATION_POSTGRES_DSN=postgresql://... \
  uv run pytest tests/test_run_control_postgres_integration.py -q
1 passed; migration 0016 applied through the application migration runner

uv run ruff check app tests scripts
All checks passed!

uv run mypy app
Success: no issues found in 321 source files

uv run pytest -q --tb=short
572 passed, 33 skipped
```

## Final disposition

The parent operation retains lifecycle, dependency, messaging, cancellation, result-admission, and
settlement authority. The Agent Protocol execution is deterministic and reconnectable, provider
completion remains evidence, and spawning remains independently feature-gated for rollout while
already-started children can always reconcile.
