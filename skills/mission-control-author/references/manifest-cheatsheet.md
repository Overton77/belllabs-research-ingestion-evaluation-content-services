# Mission Manifest cheatsheet (`manifest: mission/v1`)

Condensed from ADR-0022, ADR-0034 and `docs/specs/fast-track-2026-10/SPEC-05-mission-manifest.md`.
The JSON Schema exported beside the contracts is authoritative; this page is the shape to start from.

## Top level

```yaml
manifest: mission/v1
mission: { ... }            # exactly one of `mission` or `missions`
# missions: [ { key: research, ... }, { key: ingestion, ... } ]
# links:    [ { from: research.sources, to: ingestion.inputs.sources, kind: supplies, release_on: goal_accepted } ]
```

## Mission block

```yaml
mission:
  key: literature-sweep            # stable, lower-kebab
  title: Literature sweep on NAD+ precursors and skeletal muscle
  domain_pack: biotech.research    # app-owned templates, rubrics, prompts (optional)
  goals:
    - key: evidence_map
      description: Map the human evidence for NMN/NR effects on muscle function
      importance: primary          # primary | secondary | optional
      criteria:
        - key: coverage
          evidence: [source_manifest, claim_table]
          acceptance: { assessment: biotech.evidence_coverage, threshold: 0.8 }
        - key: reviewed
          acceptance: { human: review_accept }
  inputs:
    - { name: brief, artifact: "artifact://…", expand: inline }
  environment: { ... }             # mission-level defaults (below)
  program: { ... }                 # one program node, usually stage_graph or goal_loop
  controls:
    commands_allowed: [pause, cancel, queue_instruction, add_context, interrupt_and_inject, fork]
    notifications: { on: [human_task, completion], channel: inbox }
```

## Environment (mission level; nodes overlay by deep merge, lists replace)

```yaml
environment:
  lane: deep_agents                # deep_agents | cursor_local | cursor_cloud
  model: { profile: frontier.default }
  sandbox: { profile: research.standard, egress: [pubmed, biorxiv] }
  workspace:
    repo: { url: "https://…", ref: main }        # cursor lanes; optional for deep_agents
    context: [ .mission/brief.md, "biotech.schema_context@search:muscle aging" ]
    skills: [ "skill.mission-control-observe@0.1.0#sha256:…" ]
  capabilities:
    - { search: "pubmed literature retrieval", kind: mcp_server, as: pubmed, require: [deep_agents] }
    - { pin: "mcp.tavily@0.2.22#sha256:…", as: web }
  agents:                          # subagent profiles
    - { pin: "agent.verifier@0.1.0#sha256:…" }
  hooks:                           # hook scripts with the events they run at
    - { pin: "hook.pre-tool-use-policy@0.1.0#sha256:…", events: [before_tool, before_shell] }
  plugins:
    - { pin: "plugin.research-web@0.1.0#sha256:…" }
  side_effects: [read_only, workspace_write]      # closed vocabulary; nodes may only narrow
  budget: { usd: 25, tokens: 2_000_000, wall_clock: 4h }
  governors: { depth: 2, fan_out: 6, iterations: 8, transfers: 3 }
```

Inheritance: a node without `environment` runs with the mission's. A node with
`environment: { budget: { usd: 10 } }` keeps every other mission value and replaces `budget.usd`.
`capabilities`, `agents`, `hooks`, `plugins`, `skills` and `context` are lists: a node that
declares one replaces the mission list for that key. Widening budget, governors or
`side_effects` at node level is a compile blocker (`ENVIRONMENT_WIDENS_AUTHORITY`).

## Program nodes

```yaml
program:
  behavior: stage_graph
  nodes:
    - key: collect
      behavior: goal_loop
      objective: evidence_map
      action_space: [pubmed, web]                 # aliases from capabilities
      environment: { budget: { usd: 10 }, governors: { iterations: 5 } }
      outputs: [ { name: sources, schema: source_manifest@1 } ]
    - key: synthesize
      behavior: agent_executor
      depends_on: [collect]
      inputs: [ { from: collect.sources, expand: materialize } ]
      outputs: [ { name: claims, schema: claim_table@1 } ]
    - key: review
      behavior: human_gate
      depends_on: [synthesize]
      task: { kind: review, reviewers: [owner], packet: [synthesize.claims] }
    - key: ingest
      behavior: deterministic_executor
      depends_on: [review]
      executor: biotech.ingestion.apply
      inputs: [ { from: synthesize.claims } ]
```

Behaviors: `stage_graph`, `goal_loop`, `parallel_swarm`, `evaluator_optimizer`,
`agent_executor`, `deterministic_executor`, `event_wait`, `timer`, `human_gate`, `proof_gate`,
`child_mission_invocation`. Lane support per behavior is validated at compile (a Parallel Swarm
runs only on `deep_agents` until qualified).

## Links (chains)

```yaml
links:
  - { from: research.sources, to: ingestion.inputs.sources, kind: supplies,
      release_on: goal_accepted,        # goal_accepted | mission_accepted | execution_complete
      on_upstream_cancel: cancel_downstream }   # cancel_downstream | detach
  - { from: research, to: ingestion, kind: depends_on }
```

## Deferred to v2

`templates:` and `extends:`. Do not write them; the compiler rejects unknown keys.
