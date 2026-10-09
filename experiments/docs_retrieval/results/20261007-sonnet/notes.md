# Run 20261007-sonnet: notes

Model `claude-sonnet-5-5`, 15 tasks, four configurations, LLM judge on, 60 agent runs, about USD 9.8 including judging. `summary.md` is the first scoring; `summary-rejudged.md` is the same answers re-scored after fixing the judge rubric and broadening `expected_files` (no agent was re-run).

## Re-judged result

| config | pass | retrieved | judge correct | mean turns | mean cost USD |
| --- | --- | --- | --- | --- | --- |
| baseline | 93% | 93% | 87% | 3.7 | 0.090 |
| skill | 93% | 93% | 87% | 4.9 | 0.097 |
| index | 100% | 100% | 100% | 3.5 | 0.099 |
| both | 100% | 100% | 100% | 3.5 | 0.100 |

## What the run shows

1. The corpus answers project-specific questions that pretraining cannot, including the Stage Graph intervention task (accepted command kinds, declared-but-rejected kinds, mapping module) and the spec-versus-code splits (five-state command lifecycle vs four receipt states; `mission.*` scopes vs `workflow_run.*` grants). Every configuration surfaced those splits, which means the "implemented vs specified" discipline in the concepts is doing its job.
2. Because the corpus is on disk and Grep is allowed, the baseline already retrieves well. The compressed index was the only configuration with no misses and the fewest turns; the skill configuration took the most turns and used the search script in a third of its runs. The gap between index and baseline is one task and a few tenths of a turn: real but small at this corpus size.
3. The first judge rubric penalised correct answers for separating spec from code. The rubric now accepts a labelled split. The remaining judge disagreements are judge limitations, not agent errors: the judge has no access to the docs, so it cannot verify a rationale the agent quoted correctly from ADR-0014, and it over-reads "in order" on the lifecycle task.
4. `expected_files` must list every document that legitimately holds the answer; six tasks had a single file listed where three or four qualified.

## Changes made because of this run

- Judge rubric accepts spec-versus-code splits and ignores unverified-source caveats.
- `--rejudge <run-id>` re-scores saved answers under the current rubric and task file.
- Seven tasks gained additional expected files.

## Next runs

- Give the judge the expected source excerpt (or the paths) so it can verify rationale instead of guessing.
- Add tasks whose answer exists in exactly one file and tasks that require two documents to be combined; those are where a pointer should matter.
- Make the sandbox larger (include `src/` and the spec pack's generated issue views) so grepping is no longer nearly free; then compare turns and cost, not only pass rate.
- Repeat the `index` configuration on a stronger model (Opus or Fable) as the owner requested, and add Codex and Cursor runners.
