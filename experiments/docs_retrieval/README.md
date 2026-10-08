# Docs retrieval experiment

Measures whether an agent working in this repo finds and uses the right Mission Control documentation. It re-measures ADR-0021 (OKF bundle + compressed index in `AGENTS.md` + a search skill) instead of assuming Vercel's result transfers.

## What is compared

Four configurations, same tasks, same model:

| Config     | Compressed index in `AGENTS.md`/`CLAUDE.md` | `mission-control-docs` skill |
| ---------- | ------------------------------------------- | ---------------------------- |
| `baseline` | no                                          | no                           |
| `skill`    | no                                          | yes                          |
| `index`    | yes                                         | no                           |
| `both`     | yes                                         | yes                          |

Every configuration gets the same corpus copy (`GLOSSARY.md`, `docs/`, the sibling spec pack) and the same search tool on disk, so the only variable is how the agent is told the docs exist.

## Tasks

`tasks.jsonl`, one task per line:

```json
{"id": "erc-term", "category": "glossary", "prompt": "...", "expected_files": ["GLOSSARY.md"], "expected_terms": ["Compiled Program"], "terms_mode": "all"}
```

- `expected_files`: path suffixes; the task counts as **retrieved** if the agent read at least one of them (Read tool) or surfaced it through the search tool.
- `expected_terms`: strings that must appear in the final answer (`all` or `any`); the task counts as **answered** when they do.
- `pass` = retrieved and answered. An optional LLM judge (`--judge`) adds a `correct` verdict with a reason.

Tasks target facts that are project-specific and recent, so pretraining cannot answer them.

## Runner

```
python experiments/docs_retrieval/run.py --config all --run-id 2026-10-07a
python experiments/docs_retrieval/run.py --config index --task erc-term --model claude-sonnet-5-5
python experiments/docs_retrieval/run.py --config all --judge
```

Each task runs as `claude -p` (headless Claude Code) inside a sandbox copy of the corpus under `.scratch/docs-retrieval/<run-id>/<config>/`, with stream-json output so tool calls are observable. Allowed tools are Read, Grep, Glob and the search script only; MCP servers are disabled with an empty strict config; sessions are not persisted.

Outputs:

- `.scratch/docs-retrieval/<run-id>/<config>/<task>.jsonl`: raw transcript.
- `results/<run-id>/results.jsonl`: one scored row per (config, task).
- `results/<run-id>/summary.md`: pass, retrieved and answered rates, mean turns and cost per config.

Running costs API money. Keep runs small (the default task set is 14 tasks × 4 configs) and record the model in the run id or summary.

## Extending

- Add runners for Codex (`codex exec`) and Cursor (`agent -p`) as `--runner` options; the sandbox and scoring do not change.
- Add tasks whenever a session shows an agent asserting something the docs contradict; that is the signal this experiment exists to catch.

## Runs so far

- `results/20261007-sonnet/`: first run (Sonnet, judge on). Re-judged result: index and both 100% pass, baseline and skill 93%; the index configuration used the fewest turns. See `notes.md` there for the reading and the scoring fixes it caused. Re-score a finished run with `python experiments/docs_retrieval/run.py --rejudge <run-id> --model <judge-model>`.
