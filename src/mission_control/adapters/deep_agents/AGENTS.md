# Deep Agents lane agent guide

This adapter is the first execution lane (ADR-0005): bounded operation cognition on Deep Agents
0.7.5 over LangGraph, extended through public interfaces, never forked. Read the root
`GLOSSARY.md` (Lane, Harness, Execution Binding, Materialization, Subordinate) and
`docs/knowledge/lanes-and-harness.md` first.

## Where things are

| Concern | Start |
| --- | --- |
| Run an operation; observe; cancel | adapter.py (`DeepAgentRuntimeAdapter`) |
| Turn an execution binding into a real agent, model client and sandbox | materializer.py (`ExactDeepAgentMaterializer`, model factory, LangSmith/State/Docker sandboxes) |
| Async subordinates and parent effects | async_subagents.py; application/subordinates |
| Checkpoints, lineage and verification | checkpoint_history.py, checkpoint_reads.py, checkpoint_verifier.py; domain/execution/checkpoint_lineage.py |
| LangGraph saver/store in the runtime schema | persistence.py, runtime_persistence_verifier.py |
| Capability lineage recorded on the attempt | capability_lineage.py |
| Browser and Docker sandbox tools | browser_tool.py, docker_sandbox.py |

## Invariants

The adapter executes what the binding pins and refuses mismatches; it never chooses a model,
tool or skill the binding did not name. Model clients and capability credentials stay
server-side; nothing secret enters prompts, files, checkpoints or artifacts. Deep Agents
schedules nothing: Temporal owns scheduling, the reducer owns lifecycle, and native messages
or checkpoints are recovery state, not mission truth (ADR-0004, ADR-0007). An uncertain
provider state is reconciled, never re-launched. Fork only through the ADR-0005 dossier.

## Verify

`uv run pytest tests/integration/deep_agents` (needs the real local stack) plus the unit
selections under tests/unit that name deep agents, materialization and subordinates;
`uv run mypy src/mission_control` and `ruff check`. Paid provider calls need authorization.
