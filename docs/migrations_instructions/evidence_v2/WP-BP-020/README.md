# WP-BP-020 GoalDirected runtime evidence

Disposition: `accepted`

## Revision and scope

- Integrated base: `e83d2ef2583924798bde86fd5a8827f8978ff464`
- Evidence date: 2026-08-11
- Worktree: main working tree; no commit was created by this qualification
- Unrelated pre-existing dirty documentation and rule files were preserved.

## Requirement evidence

| Requirement | Evidence |
|---|---|
| REQ-BP-GD-001/002 | Existing contract and revision-boundary suite in `tests/unit/orchestration/test_wp_bp_020_goal_directed.py` |
| REQ-BP-GD-003 | Temporal executor/verifier `OperationWorkflow` children and replay suite in `tests/integration/temporal/test_wp_bp_020_temporal.py` |
| REQ-BP-GD-004 | Separate verifier operation/binding/session/workspace assertions in the focused Temporal and sandbox-rollover suites |
| REQ-BP-GD-005/006 | Forced token rollover, immutable persisted handoff, empty-session resume, protected context, and persistent sandbox artifact proof in `tests/acceptance/control_plane/test_wp_bp_020_sandbox_rollover.py` |
| REQ-BP-GD-007 | Existing deterministic precedence/property coverage plus two-iteration convergence in the sandbox vertical |
| REQ-BP-GD-008/009/010 | Existing delegation routing, fork boundary, terminalization proposal, reducer, cancellation, and continuation tests |

## Defects closed during qualification

1. Deep Agents checkpoint threads used per-operation binding IDs, making authored `reuse` sessions
   fresh accidentally. The adapter now keys checkpoints by the governed operation `session_id`;
   rollover changes that ID and starts an empty session.
2. GoalDirected reconciliation parsed the complete `OperationExecutionResult` envelope as the
   provider's goal result. It now validates and unwraps the exact completed operation envelope,
   rejects binding/semantic-attempt drift, and reconciles strict structured output.
3. Token usage could be model-authored in goal structured output and was cumulative when a
   checkpoint was reused. Reconciliation now uses host-observed runtime usage, and the adapter
   counts only AI messages added by the current invocation.
4. No local executable Deep Agents sandbox placement existed. `DockerSandboxFactory` now provides
   a network-disabled, read-only-root, capability-dropped, resource-bounded container. Exact
   writable workspace paths are bind-mounted and survive container/session replacement.
5. Model-facing structured output exposed host-authoritative identities, workspace authority,
   verifier applicability, usage, and handoff budget balances. Dedicated executor, verifier, and
   handoff-draft observation schemas now exclude those fields; reconciliation derives them from
   the admitted operation, observed usage, frozen reservation, and blueprint iteration limit.
6. Temporal's `maximum_attempts=0` was incorrectly treated as no retries even though it means
   unlimited retries. Lifecycle and deterministic reconciliation boundaries now use one attempt,
   and invalid provider payloads report the exact mismatched field.
7. Independent verifier sandboxes could not consume executor artifacts. Governed prior workspaces
   are now mounted explicitly read-only while the verifier retains a separate writable subtree;
   Docker qualification proves reads succeed and mutation fails.
8. Partial/failure terminalization promoted diagnostic artifacts as valid output evidence while
   proposing an empty valid-output set. Only verified completion now promotes output evidence.

## Sandbox rollover qualification

The deterministic production-path acceptance test executes:

```text
BellLabsRunWorkflow
  -> GoalDirectedWorkflow
    -> executor OperationWorkflow iteration 1
      -> operation.execute -> DeepAgentRuntimeAdapter -> create_deep_agent
        -> isolated Docker sandbox writes artifact.txt
    -> verifier OperationWorkflow iteration 1 (independent sandbox/session)
    -> host-observed token threshold -> typed handoff persistence
    -> executor OperationWorkflow iteration 2 with a new empty session
      -> mediated handoff in admitted prompt + persistent sandbox artifact read
    -> verifier OperationWorkflow iteration 2
    -> GoalDirectedInterpreter completion proposal
    -> lifecycle reducer-authorized terminalization
```

The test asserts one rollover, two iterations, no prior model messages in the replacement session,
presence of the typed handoff, persistence of the filesystem artifact, separate verifier sessions,
and verified completion. Every executor and verifier operation receives a sandbox workspace.

Docker qualification image observed locally:
`python:3.12-slim@sha256:229a2c5bfa27522db7815ea81f9bed70af17ccb9de9fc7ad142b1877b5830d36`.

## Live API-to-Temporal qualification

The credential-gated vertical passed on 2026-08-11 with the exact requested model
`gpt-5.6-luna` and the bounded objective: create and independently verify a record linking Moderna
to Spikevax as an mRNA vaccine.

| Evidence | Value |
|---|---|
| API request | `wp-bp-020-live` |
| Run | `4428ce2d-d6df-5e76-9859-5e7ae413e625` |
| Root workflow | `belllabs-run/4428ce2d-d6df-5e76-9859-5e7ae413e625` |
| Family workflow | `family/4428ce2d-d6df-5e76-9859-5e7ae413e625/1` |
| Provider response IDs | `resp_072f1ab221009f29006a7ba276bb58819ca68a8b96aedfaa19`, `resp_0102157ae7c05b36006a7ba2829cb4819cbbbeab9084b8f841`, `resp_0c2a58d48d0510a5006a7ba28b4fe4819fb50b53498b41ab72`, `resp_0074ddc6dbebf476006a7ba291cbe481a0af74ff2cd4b55193` |
| Rollover | one token-threshold rollover into a new governed session |
| Context transfer | persisted typed handoff plus host-bound protected context and durable filesystem artifact |
| Verifier | independent session/workspace; executor workspace mounted read-only; accepted `fixture-obligation` |
| Result | convergence `complete`; run-control terminal outcome `completed` |

The accepted artifact was exactly
`company=Moderna, product=Spikevax, modality=mRNA vaccine.`

## Verification commands and sanitized outcomes

```powershell
.\.venv\Scripts\python.exe -m pytest -q `
  tests/unit/orchestration/test_wp_bp_020_goal_directed.py `
  tests/integration/temporal/test_wp_bp_020_temporal.py `
  tests/acceptance/control_plane/test_wp_cp_040.py `
  tests/integration/deep_agents/test_docker_sandbox.py `
  tests/acceptance/control_plane/test_wp_bp_020_sandbox_rollover.py `
  tests/acceptance/control_plane/test_wp_bp_020_live.py
# 53 passed, 1 credential-gated skip; the skipped live test passed separately with its flag enabled

.\.venv\Scripts\python.exe -m mypy app
# Success: no issues found in 332 source files
```

Changed-file Ruff checks pass. The repository-wide Ruff baseline has 58 unrelated import-order and
line-length failures. The repository-wide pytest baseline completed with 639 passed, 43 skipped,
and 40 unrelated failures (missing moved paths, unavailable PostgreSQL, and stale capability,
schema, reference-research, and Agent Server fixtures). These baseline failures do not occur in the
focused package gate and were not modified in this work package.

## Changed paths

- `app/application/orchestration/goal_directed.py`
- `app/domain/operation_execution/contracts.py`
- `app/domain/orchestration/goal_directed_runtime.py`
- `app/integrations/agents/deep_agents/{__init__,adapter,materializer,docker_sandbox}.py`
- `app/temporal/workflows/goal_directed.py`
- `tests/acceptance/control_plane/test_wp_cp_040.py`
- `tests/acceptance/control_plane/test_wp_bp_020_sandbox_rollover.py`
- `tests/acceptance/control_plane/test_wp_bp_020_live.py`
- `tests/integration/deep_agents/test_docker_sandbox.py`
- `tests/integration/temporal/test_wp_bp_020_temporal.py`
- this evidence and the WP-BP-020 handoff/traceability records

No database migration is required. No superseded runtime path was reintroduced. Test containers
are removed after each invocation; the generated local test artifact was deleted and is not
recoverable or required.

## Reproduction of external qualification

```powershell
$env:BELLABS_RUN_WP_BP_020_LIVE='1'
.\.venv\Scripts\python.exe -m pytest -q -s `
  tests/acceptance/control_plane/test_wp_bp_020_live.py
```

FINAL_DISPOSITION: ACCEPTED
