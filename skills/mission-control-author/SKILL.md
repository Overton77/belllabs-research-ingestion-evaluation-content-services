---
name: mission-control-author
description: Author a Mission Manifest (mission.yml, manifest mission/v1) from an interview, compile it with missionctl mission compile, read the Validation Report, submit it, and start the run as a separate authorized call. Use when a human describes a research, ingestion, content or coding outcome that should become a mission, or asks to edit, compile, validate, submit or start a mission.yml.
---

# Author a mission

A Mission Manifest is the YAML authoring surface; the compiler turns it into the typed
`MissionDefinition@1`. Aliases and `search:` entries are allowed in the file; the committed
revision carries only exact pins. You write the file, the compiler decides what it means.

## 1. Interview

Ask until each row has an answer the human confirmed. Done when you can fill every field of
the cheatsheet's mandatory set without guessing.

| Learn | Becomes |
| --- | --- |
| Outcome and why it matters | `goals[].description`, `importance` |
| How we will know it is done, and who judges | `goals[].criteria[]` (`evidence`, `acceptance`: an assessment capability or `human: review_accept`) |
| Inputs the mission may read | `inputs[]` as artifact refs or catalog context |
| Side effects allowed (write to a repo, ingest into a graph, publish) | `environment.side_effects`, human gates |
| Where it runs | `environment.lane`: `deep_agents`, `cursor_local`, `cursor_cloud` |
| Money, tokens, time, iterations | `environment.budget`, `environment.governors` |
| Known route or adaptive search | `stage_graph` nodes, or a `goal_loop` with an `action_space` |
| One mission or several linked ones | `mission:` or `missions:` plus `links:` (then read `mission-control-compose`) |

## 2. Write the manifest

Start from [the cheatsheet](references/manifest-cheatsheet.md). Rules the compiler enforces:

- `environment` at mission level is inherited by every node; a node's `environment` overlays it
  by deep merge where lists replace. A node may narrow budget, governors and side effects, never
  widen them.
- Capabilities are `{pin: id@version#sha256:…}` or `{search: "…", kind: …, as: alias}`. Use the
  `mission-control-catalog` bundle to search before you write a `search:` entry you cannot
  describe precisely; a search with zero admitted hits is a blocker, not a fallback.
- `agents:`, `hooks:` and `plugins:` name catalog capabilities (subagent profiles, hook scripts,
  plugins) and inherit like everything else in `environment`.
- Inputs name schemas and immutable artifacts or `{from: node.output}` bindings; add
  `expand: inline|reference|materialize|auto` when the default (`auto`) is wrong.
- Human gates and proof gates are explicit nodes; the compiler never hides a wait.

Done when the file parses as YAML and every `search:` has a `kind` and an `as` alias.

## 3. Compile

```text
missionctl mission compile mission.yml --json
```

Availability: FT-E2. Read the report with [validation-report.md](references/validation-report.md).
Done when `blockers` is empty; warnings are allowed and must be shown to the human. Fix the
file, never the report. The report's `resolutions` show the exact pin each `search:` became;
copy a resolution into the file as a `pin:` when the human wants it frozen.

## 4. Submit

```text
missionctl mission submit mission.yml --json
```

Availability: FT-E3. Submit commits the revision and admits the run (or the chain) without
starting anything. Record the returned `mission_id`, `revision_id`, `run_id` (or `chain_id` and
each `mission_id`). Submitting the same file twice returns the same identities; a changed file
is a new revision proposal.

## 5. Start

```text
missionctl mission start MISSION_ID --json
```

Availability: FT-E3. Start is a separate authorized action; only run it when the human asked
for the start and holds `mission.start`. Then hand over to `mission-control-observe` to watch
the run, and register a subscription if the human wants callbacks.

## Boundaries

- The manifest never carries secret values; it names secret references the deployment resolves.
- `application` is resolved from the authenticated scope, never trusted from the file.
- Structural changes after submit are a new revision proposal, not an edit of the running run.
