# Stage 0 exact compatibility and capability matrix

Source of pins: `spikes/stage0/pyproject.toml` and `spikes/stage0/uv.lock`  
Qualification environment: Windows, CPython 3.12.7  
Production dependency action: proposal only; root lock unchanged

## Exact package matrix

| Package/version | Exercised API | Maturity used by BellLabs | Support qualified | Flag/fallback | Evidence |
|---|---|---|---|---|---|
| CPython `3.12.*` | async runtime, typing, cancellation | stable | local pass | legacy runtime | spike lock/tests |
| `langchain==1.3.14` | agent middleware dependency | stable | local import pass | legacy runtime | exact lock |
| `langchain-core==1.5.3` | `RunnableConfig`, fake chat model | stable | local pass | legacy runtime | Q01/Q14 |
| `langgraph==1.2.10` | `StateGraph`, `Send`, reducers, interrupts, `Overwrite`, persistence | stable | local pass | `LANGGRAPH_RUNTIME_ENABLED=false`; Temporal | Q01/Q04/Q07/Q08 |
| `deepagents==0.7.4` | `create_deep_agent`, default graph/tools, async subagent types | core stable; async preview | local surface pass | sync/async flags; linked run | Q11/Q14 |
| `langgraph-sdk==0.4.2` | `ServerRuntime`, `AccessContext`, Auth, client protocol | stable SDK surface for this pin | local pass | Agent Server disabled | Q01/Q02 |
| `langgraph-cli[inmem]==0.4.31` | config load and `langgraph dev` | stable local CLI | local pass | legacy runtime | Q02/Q16 |
| transitive `langgraph-api==0.12.0` | custom auth/app, native resources, graph run | server API for this CLI pin | local pass | no root server package yet | dev logs/API probes |
| transitive `langgraph-runtime-inmem==0.32.0` | local queue/persistence | development only | local pass | never production | dev logs |
| `langchain-mcp-adapters==0.3.1` | client, session, tool loading/interceptors | stable adapter | local import/surface pass | MCP feature flags; unsupported response | Q09 |
| `mcp==1.29.0` | adapter protocol dependency | stable 1.x | local import pass | exact pin required | Q09 |
| `langchain-quickjs==0.3.5` | middleware signature/default limits | beta | local surface only; disabled | native reviewed tool | Q10/Q14 |
| `langsmith==0.10.15` with `pytest,sandbox` | existing trace processors/redaction; Sandbox client surface | trace/eval stable; sandbox entitlement-dependent | local trace tests; no Cloud proof | tracing/sandbox flags | root tests/Q13/Q15 |
| `langgraph-checkpoint-postgres==3.1.1` | `AsyncPostgresSaver`, `AsyncPostgresStore` imports | stable standalone integration | import pass; live DB blocked | managed Cloud persistence | import probe |
| `psycopg[binary,pool]==3.3.4` | async Postgres package dependency | stable | import/lock only | managed Cloud persistence | exact lock |
| `langchain-openai==1.4.1` | provider integration surface | stable | import/lock only | existing OpenAI Agents adapter | exact lock |
| `langchain-anthropic==1.5.3` | provider integration surface | stable | import/lock only | configured model-provider policy | exact lock |
| `fastapi==0.141.1` | custom Agent Server route | stable | local pass | standalone FastAPI | Q02 |
| `pytest==8.4.2`, `pytest-asyncio==1.4.0`, `hypothesis==6.165.1` | deterministic spike verification | test-only | local pass | none | 29 local spike tests |

## Resolution defect found and qualified

`langchain-mcp-adapters==0.3.1` declares an unconstrained `mcp` dependency. On
2026-08-04, a fresh lock selected `mcp==2.0.0`; importing the adapter then failed:

```text
ImportError: cannot import name 'RequestContext' from 'mcp.shared.context'
```

Pinning `mcp==1.29.0` restores `MultiServerMCPClient`, explicit session, and
`load_mcp_tools` imports. Stage 2 must retain the exact 1.29.0 constraint unless a
new adapter release is separately proven against MCP 2.x.

## Current API findings that amend planning prose

1. `ServerRuntime` is a `typing.TypeAliasType`, not a constructible class.
2. Its access contexts are exactly:
   `threads.create_run`, `threads.update`, `threads.read`, `assistants.read`.
3. The execution runtime fields are `access_context`, `user`, `store`, `context`.
4. The read runtime fields are `access_context`, `user`, `store`.
5. The inherited `execution_runtime` property returns the execution variant for
   `threads.create_run` and `None` for read/update/assistant contexts.
6. `CodeInterpreterMiddleware` defaults include `subagents=True`, `mode=None`,
   64 MiB memory, 5-second timeout, and 256 PTC calls. BellLabs must override or
   disable these defaults; defaults are not authority.
7. A default Deep Agents 0.7.4 graph exposed tools:
   `delete`, `edit_file`, `execute`, `glob`, `grep`, `ls`, `read_file`, `task`,
   and `write_file`. This differs from the checked-in skill prose and proves that
   the compiled tool surface must be inspected from the pin.
8. `AsyncSubAgent` currently identifies a remote `graph_id` with optional URL and
   headers; `AsyncSubAgentState` owns a dedicated `async_tasks` reducer channel.

## Platform qualification

| Capability | Local | Serverless | Dedicated/self-hosted |
|---|---|---|---|
| Agent Server graph import/run | proven | not inspected | not inspected |
| Custom auth/native resources | proven locally | not inspected | not inspected |
| Custom route auth | proven with explicit route dependency and `disable_studio_auth=true`; Agent Server native custom-route auth was not relied on | not inspected | not inspected |
| Managed checkpointer/Store injection | not applicable to `dev` | docs only | custom async imports only |
| Background runs/reconnect/restart | not fully proven | not inspected | not inspected |
| Revision/checkpoint routing | local contract model only | not inspected | not inspected |
| LangSmith Sandbox entitlement | API surface only | not inspected | not inspected |
| Concurrency/cold start/max wait/quota | not representative | not inspected | not inspected |
| `langgraph build/up` | blocked by Docker daemon | not applicable | blocked |

No Cloud support claim is inferred from documentation. The owner authorized local
qualification only, so platform rows remain external gate blockers.

## Capability posture

| Capability | Required? | Maturity | Default | Fallback |
|---|---:|---|---|---|
| Standard Agent Server | yes | stable target | disabled | Temporal/OpenAI Agents |
| Deep Agents synchronous subagents | yes | stable | disabled until Stage 5 | linked run |
| Deep Agents async subagents | yes | preview | disabled until Stage 6 gate | linked run or sync child |
| QuickJS call mode | no | beta | disabled | reviewed native async tool |
| QuickJS PTC | no | beta | disabled | reviewed native async tool |
| Dynamic subagents | no | beta | disabled | compiled sync/async child |
| Cross-thread procedural Store | yes, bounded | non-authoritative | default deny | immutable context refs |
| Cross-thread scientific claim memory | no | policy-disabled | disabled | evidence/claim services |
| LangSmith Sandbox | yes for sandboxed operations | entitlement-dependent | disabled | unsupported capability or legacy local sandbox |

The machine-readable source is
[`spikes/stage0/capability_manifest.json`](../../../spikes/stage0/capability_manifest.json).
