---
okf_version: "0.1"
---

# Mission Control knowledge

An Open Knowledge Format explanation bundle. These concepts describe what the code
and evidence do today and point at the normative specification where behavior is
specified but not yet built. Shared language lives in [GLOSSARY.md](../../GLOSSARY.md);
decisions live in [docs/adr](../adr/0001-one-python-distribution-clean-transformation.md).
On conflict the precedence is spec pack, then ADRs, then these concepts, then the
[implementation status](../MISSION_CONTROL_IMPLEMENTATION_STATUS.md); report the
conflict rather than choosing silently.

## Code and operations

| Task | Concept |
| --- | --- |
| Find ownership or change a module | [Architecture](architecture.md) |
| Admit or control a run | [Lifecycle](lifecycle.md) |
| Understand the Stage Graph and GoalDirected code families | [Execution](execution.md) |
| Snapshot, fork or repair a run | [Recovery](recovery.md) |
| Install the common schema or diagnose roles, scope and replayed records | [Persistence](persistence.md) |
| Pin and materialize a skill directory | [Capabilities](capabilities.md) |
| Configure identity and start processes | [Operations](operations.md) |
| Assess what tests prove | [Qualification](qualification.md) |

## Workflow systems and authoring

| Task | Concept |
| --- | --- |
| Know which program behaviors exist and which are built | [Workflow systems](workflow-systems.md) |
| Design or review a Parallel Swarm | [Parallel Swarm](parallel-swarm.md) |
| Design or review an Evaluator Optimizer | [Evaluator Optimizer](evaluator-optimizer.md) |
| Change a definition, propose or activate a revision | [Revisions](revisions.md) |
| Add a wait, timer, human gate or proof gate; resolve a Human Task | [Durable controls](durable-controls.md) |
| Decide whether work is accepted | [Completion](completion.md) |
| Drive a mission from interview to start | [Authoring](authoring.md) |

## Platform and interfaces

| Task | Concept |
| --- | --- |
| Publish or consume events; send a command | [Events and commands](events-and-commands.md) |
| Call the HTTP API, CLI or coordinator MCP tools | [Interfaces](interfaces.md) |
| Reserve, record or settle budget and usage | [Budgets and usage](budgets-and-usage.md) |
| Run work on a lane; generate agent-host config | [Lanes and harness](lanes-and-harness.md) |
| Select context, hand off state, checkpoint or continue | [Context and continuation](context-and-continuation.md) |
| Touch evidence, domain writes or the Biotech integration | [Knowledge Services](knowledge-services.md) |
| Check release gates, proof statuses and what is live | [Release and qualification](release-and-qualification.md) |

[Update log](log.md). Search the whole corpus with
`python docs/tools/okf_search.py "<terms>"`; validate this bundle with
`python docs/tools/validate_okf.py`.

# Citations

- [Open Knowledge Format specification](https://github.com/GoogleCloudPlatform/knowledge-catalog/blob/main/okf/SPEC.md).
- Local convention: `../biotech-knowledge-catalog/README.md`; one concept per file with
  YAML type, title and description, reserved index/log, and a Citations section.
