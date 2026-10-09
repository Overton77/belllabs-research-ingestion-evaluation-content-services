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
| Snapshot, fork (with an instruction or a lane snapshot) or repair a run | [Recovery](recovery.md) |
| Install the common schema (release 1.1.0), upgrade an installation or diagnose roles, scope and replayed records | [Persistence](persistence.md) |
| Pin, publish, search, project or materialize a capability (skill, MCP server, hook, subagent, plugin) | [Capabilities](capabilities.md) |
| Find which MCP servers, skills and hooks are seeded, and what is missing | [Capability seeds](capability-seeds.md) |
| Configure identity and start processes; run the readiness gate or the cluster-outage procedure | [Operations](operations.md) |
| Assess what tests prove | [Qualification](qualification.md) |

## Workflow systems and authoring

| Task | Concept |
| --- | --- |
| Know which program behaviors exist and which are built | [Workflow systems](workflow-systems.md) |
| Design or review a Parallel Swarm | [Parallel Swarm](parallel-swarm.md) |
| Design or review an Evaluator Optimizer | [Evaluator Optimizer](evaluator-optimizer.md) |
| Change a definition, propose or activate a revision | [Revisions](revisions.md) |
| Add a wait, timer, human gate or proof gate | [Durable controls](durable-controls.md) |
| Run a Human Gate, resolve a Human Task over HTTP, MCP or the socket | [Human Gates](human-gates.md) |
| Decide whether work is accepted | [Completion](completion.md) |
| Drive a mission from interview or manifest to start; see why start is blocked | [Authoring](authoring.md) |
| Write, compile, submit or start a Mission Manifest | [Mission Manifest](mission-manifest.md) |
| Chain missions, release the next one and transfer state | [Mission chains](mission-chains.md) |

## Platform and interfaces

| Task | Concept |
| --- | --- |
| Publish or consume mission events and subscriptions; know the command receipt states | [Events and commands](events-and-commands.md) |
| Subscribe, replay, acknowledge or send commands over the `/missions` Socket.IO namespace | [Mission stream](mission-stream.md) |
| Read provider frames, a run transcript, run list or search | [Provider frames and transcript](provider-frames-and-transcript.md) |
| Queue an instruction, inject, or cancel immediately with a Stop Fence | [Interventions](interventions.md) |
| Call the HTTP API, CLI or coordinator MCP tools (manifest, chain, transcript, subscription, lane, catalog pin) | [Interfaces](interfaces.md) |
| Reserve, record or settle budget and usage | [Budgets and usage](budgets-and-usage.md) |
| Run work on a lane, read a lane describe or add a lane; generate agent-host config | [Lanes and harness](lanes-and-harness.md) |
| Understand session ownership, the native dispatch journal, capacity waits or auth routes | [Session ownership and dispatch](session-ownership-and-dispatch.md) |
| Run, control or qualify Cursor Local or Cursor Cloud work | [Cursor lane](cursor-lane.md) |
| Change the Deep Agents lane, its materializer, middleware or hosted subordinates | [Deep Agents lane](deep-agents-lane.md) |
| Select context, build a Context Packet, hand off state or classify a checkpoint lineage | [Context and continuation](context-and-continuation.md) |
| Seal, validate or transfer a Continuation Checkpoint; see why it does not run yet | [Continuation checkpoint](continuation-checkpoint.md) |
| Touch evidence, domain writes or the Biotech integration | [Knowledge Services](knowledge-services.md) |
| Check release gates, proof statuses, lane qualification and what is live; read the per-profile release statement | [Release and qualification](release-and-qualification.md) |

[Update log](log.md). Search the whole corpus with
`python docs/tools/okf_search.py "<terms>"`; validate this bundle with
`python docs/tools/validate_okf.py`.

# Citations

- [Open Knowledge Format specification](https://github.com/GoogleCloudPlatform/knowledge-catalog/blob/main/okf/SPEC.md).
- Local convention: `../biotech-knowledge-catalog/README.md`; one concept per file with
  YAML type, title and description, reserved index/log, and a Citations section.
