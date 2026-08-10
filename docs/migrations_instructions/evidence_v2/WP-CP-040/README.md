# WP-CP-040 implementation evidence

Disposition: `accepted`  
Recorded: 2026-08-10  
Qualification: `QUAL-CP-DEEP-AGENT-MATERIALIZATION`  
Framework baseline: Deep Agents `0.7.5`, LangGraph `1.2.10`, LangChain `1.3.14`,
LangChain OpenAI `1.3.4`, MCP adapters `0.3.1`, LangSmith `0.10.15`, Temporal `1.30.0`

## Implemented contracts and seams

- `DeepAgentProfile`, `DeepAgentExecutionPlacementProfile`, and the flattened
  `DeepAgentExecutionBinding` are strict, content-addressed domain contracts.
- Cognitive channel packs compose collision-checked `CognitiveStateSchema` and immutable,
  reference-only `CognitiveRuntimeContextSchema` contracts. The binding freezes both digests and
  exact runtime context values.
- The binding flattens the model, middleware order, tools, MCP filters/schema digests, Skills,
  sandbox, checkpointer/store, synchronous subagents, workspace/capability ceiling, placement
  behavior, package versions, authority references, and intended attachment plan.
- `app/integrations/agents/deep_agents/adapter.py` is the sole active production
  `create_deep_agent` call site. `materializer.py` performs digest-keyed resolution without aliases,
  mutable discovery, provider fallback, or framework types in public contracts.
- `OperationExecutionRequest` and `OperationExecutionBinding` select `native` or `deep_agent`
  through the provider-neutral `RuntimePort`. The canonical operation worker now registers
  `OperationWorkflow` (`belllabs.operation.v1`).

## Live Temporal + OpenAI qualification

The credential-gated test
`tests/acceptance/control_plane/test_wp_cp_040_live.py` ran a real Temporal time-skipping server,
canonical `OperationWorkflow`, and `operation.execute` activity. Inside the activity the exact
binding materialized:

- OpenAI model `gpt-5.6-luna` through `ChatOpenAI`;
- the reviewed `exact-binding-proof` Skill bundle;
- the stdio FastMCP server and sole tool `lookup_binding_marker`;
- a newly created LangSmith sandbox and its `execute` tool;
- exact in-memory LangGraph checkpointer/store revisions.

The successful run returned:

```text
workflow_id: qualification/wp-cp-040/live
workflow_disposition: completed
binding_id: 1cf750a4-012b-580d-a387-1a821cd93dcd
binding_digest: sha256:a570a700d63437b21c0ba8c44be6225cef3e87315f73e3ac018e7300eafabfca
state_schema_digest: sha256:23a84e8cd70ae342039ffd6ea0cb74f8b721e7434f8a57d9eb67ffb689cefceb
context_schema_digest: sha256:afb22a54847775c2f09e8ce31b3d71582bf84672ccacc1fd0e6afe22d638a686
called_tools: [ls, read_file, lookup_binding_marker, execute]
mcp_result: MCP-BOUND::LIVE040::EXACT
sandbox_output: SANDBOX-LIVE-040
```

The inspected checkpoint contained `artifact_index`, `context_manifest`, `child_result_index`,
`files`, `messages`, and the framework-owned Skill channel. The model-input observer proved the
startup metadata entry:

```text
name: exact-binding-proof
path: /skills/exact-binding-proof/SKILL.md
```

The persisted messages then contained the complete line-numbered `SKILL.md`, including
`SKILL-MD-IN-MESSAGES-040`. This proves progressive disclosure: metadata was available before the
model chose `read_file`; full instructions appeared only after that tool call.

## Integration findings resolved in-package

1. Deep Agents marks `skills_metadata` and `skills_load_errors` as `PrivateStateAttr`. Initially
   redeclaring those middleware-owned runtime annotations with an eager list reducer caused the
   empty default to suppress Skill discovery. The canonical schema still records the channel
   contract, while runtime type construction now leaves framework-owned annotations to the Skills
   middleware. A callback records only exact disclosed Skill identities/digests and does not retain
   the system prompt.
2. `langchain-mcp-adapters==0.3.1` deliberately rejects client-level async context management.
   Materialization uses `MultiServerMCPClient.get_tools()`; returned tools own scoped sessions per
   call. Names and JSON-schema digests are compared with the exact binding.
3. Deep Agents `0.7.5` rejects filesystem `permissions` with executable sandbox backends because
   execute-tool permission enforcement is not implemented. For `StateBackend`, framework
   permissions enforce the exact read/write paths. For an executable LangSmith sandbox, the
   adapter omits that unsupported argument and records
   `authority_enforcement=immutable_host_binding_executable_sandbox`; BellLabs still admits only
   the exact sandbox, capability grant, workspace, tools, and MCP surface. No backend fallback is
   attempted.
4. The default LangSmith sandbox image did not provide `python`; qualification therefore uses
   `printf SANDBOX-LIVE-040`, testing sandbox execution without assuming an undeclared runtime.
5. Nested `frozenset` projections must be hashed before JSON serialization. All new digest
   contracts use Python-mode values with the repository canonicalizer, which sorts sets. A stress
   qualification compiled the child-slice profile/binding 250 times and observed one digest tuple.

## Requirement-to-evidence map

| Requirements | Executable evidence |
|---|---|
| REQ-CP-DA-001..004 | Deterministic profile/placement/binding tests and canonical Temporal live run. |
| REQ-CP-DA-005..006 | Exact attachment plan, digest registries, MCP/schema checks, package-drift-before-effect test, and no runtime aliases. |
| REQ-CP-DA-007 | Synchronous subagent tool, Skill, schema-slice, budget, and private workspace ceilings fail closed. |
| REQ-CP-DA-013 | Binding/request identity and workspace ownership must match exactly; child writable slots cannot overlap parent-exclusive slots. |
| REQ-CP-DA-014 | `tests/test_artifact_promotion.py` proves gated, visible-only-after-authority, idempotent promotion. |
| REQ-CP-DA-015 | `tests/test_sandbox_snapshots.py` proves immutable snapshots, compatibility, clone lineage, present-authority revalidation, and live-resource reacquisition. |
| REQ-CP-CS-001..007 | Strict pack/schema digests, collision checks, reducer registry digest, reference-only context, base channels, middleware ownership, and subagent projections. |

## OpenAI Agents SDK removal

The active OpenAI Agents runtime, factory, session, tracing processor, Docker sandbox/snapshot
bridge, Temporal plugin/probes, live scripts, SDK-specific experiments, dependencies, and default
tests were removed. `uv lock` and `uv sync --offline` removed `openai-agents==0.17.8`. The server
retains its provider-neutral realtime approval gateway, and the worker connects to Temporal without
an Agents SDK plugin. There is no registered prior-provider fallback.

## Verification

```text
uv run pytest tests/acceptance/control_plane/test_wp_cp_040.py -q
8 passed

BELLABS_RUN_WP_CP_040_LIVE=1 \
  uv run pytest tests/acceptance/control_plane/test_wp_cp_040_live.py -q -s
1 passed; real Temporal activity, gpt-5.6-luna, MCP, Skill, LangSmith sandbox

uv run pytest tests/acceptance/control_plane/test_wp_cp_040.py \
  tests/test_artifact_promotion.py tests/test_sandbox_snapshots.py -q
21 passed

uv run ruff check app tests scripts
All checks passed!

uv run mypy app
Success: no issues found in 317 source files

uv run pytest -q --tb=short
555 passed, 33 skipped
```

## Final disposition

Every non-async WP-CP-040 requirement has executable evidence. Exact materialization fails closed,
the actual Deep Agent state and progressive Skill messages were inspected, the MCP and sandbox
surfaces executed through Temporal, and the superseded OpenAI Agents SDK path is absent. WP-CP-040
is accepted; asynchronous child lifecycle remains isolated to WP-CP-045.

## Foundation amendment addendum — cognitive worker composition boundary

Recorded: 2026-08-10
Amendment base: `cfe9db22580678d1dc563e93087283f823579442`
Canonical metadata revision: `c48867a240d09a98db9cdfb4937f55176f30adf1`

This addendum preserves the historical acceptance evidence above. The accepted
`OperationExecutionService` and `DeepAgentRuntimeAdapter`/`ExactDeepAgentMaterializer` semantics are
unchanged. The amendment makes their existing `operation.execute` activity seam explicit in the
deployment composition boundary:

- the `agent_cognitive` registry surface contains only `operation.execute`;
- `OperationWorkflow` remains registered only on coordinator/family workflow workers;
- coordinator launch requires a deployment `WorkerActivityCompositionFactory` that supplies a
  genuinely wired `OperationExecutionActivities`; no production activation is claimed when that
  factory is absent; and
- the activity queue is derived from the exact Deep Agent binding, preventing caller-, provider-,
  or family-selected fallback routing. Binding execution generation must also match the wrapper.

Contract and worker evidence is in `tests/test_operation_execution.py`,
`tests/test_coordinator_temporal_runtime.py`, and the credential-gated
`tests/acceptance/control_plane/test_wp_cp_040_live.py`, whose historical provider/materializer
assertions remain intact after adapting its wrapper to V2. This addendum does not reopen or rewrite
the original WP-CP-040 disposition.

Final focused and full applicable offline qualification results are recorded in the WP-CP-030
addendum. The credential-gated V2 live vertical passed:

```text
BELLABS_RUN_WP_CP_040_LIVE=1 \
  uv run pytest tests/acceptance/control_plane/test_wp_cp_040_live.py -q -s
1 passed in 112.38s

workflow_id: qualification/wp-cp-040/live
workflow_disposition: completed
model: gpt-5.6-luna
binding_id: 1cf750a4-012b-580d-a387-1a821cd93dcd
binding_digest: sha256:a570a700d63437b21c0ba8c44be6225cef3e87315f73e3ac018e7300eafabfca
```

This exercised real Temporal with separate workflow and binding-derived cognitive activity queues,
a real `gpt-5.6-luna` call, exact MCP and Skill materialization, and the LangSmith sandbox. The
binding identity/digest above are sanitized identifiers already present in the historical live
output; no credentials or secret values are recorded.

Production requires TLS, encrypted Temporal history persistence, namespace authorization, and
queue-scoped worker identities; these remain deployment prerequisites and are not claimed as
configured by this amendment.
