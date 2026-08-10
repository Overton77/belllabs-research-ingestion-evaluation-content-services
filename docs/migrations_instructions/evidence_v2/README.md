# v2 implementation evidence contract

Evidence directories are created by an active work package only after executable evidence exists.
The existence of a directory or draft is not acceptance. A package README must end with exactly
one disposition: `ready_for_review`, `accepted`, or `rework_required`.

Never include secrets, tokens, full prompts containing sensitive inputs, PHI, provider credentials,
or unsanitized connection strings.

## Required package README

Use this section order:

```text
# <WP-ID> implementation evidence

Disposition
Recorded date
Qualification identity
Base revision and head revision
Framework/package baseline

## Implemented contracts and seams
## Requirement-to-evidence map
## Changed paths and migrations
## Deterministic verification
## Live runtime qualification
## Replay and recovery artifacts
## Replacement and deletion checks
## Unresolved risks and drift checks
## Final disposition
```

Every requirement row names at least one exact test and the observed assertion. A suite name alone
is insufficient. Record exact changed paths, storage migrations, active composition/registration
changes, and deleted superseded owners.

## Worktree provenance

Parallel blueprint evidence must record:

- worktree branch and absolute worktree path;
- shared `BP_BASE_REVISION`;
- branch head revision tested;
- pre-existing dirty paths at kickoff;
- sibling WP branch and integration branch;
- shared-file regions edited;
- merge revision tested on `integration/bp-runtimes`.

If the two BP evidence files name different base revisions, neither package is integration-ready.

## Deterministic verification

Record commands and sanitized outputs for:

- unit/property and contract tests;
- API integration tests;
- Temporal time-skipping, replay, recovery, cancellation, late-result, and Continue-As-New tests
  applicable to the WP;
- accepted prerequisite regression suites;
- `uv run ruff check app tests scripts`;
- `uv run mypy app`;
- the full offline pytest suite;
- `git diff --check`.

Captured histories and fixtures must identify workflow type, schema/interpreter version, input
digest, and expected decision sequence. Do not replace replay evidence with current-worker success.

## Live runtime qualification

`WP-BP-010` and `WP-BP-020` require credential-gated, real-LLM acceptance runs. Record:

- exact command and opt-in environment flag name, with values/secrets redacted;
- API endpoint and sanitized request identity;
- admitted `run_id`, definition/ERC refs and digests;
- Temporal root, family, and operation Workflow IDs;
- exact Deep Agent binding digest, model/provider identity, and package versions;
- operation kinds and their semantic attempt identities;
- accepted outputs/evidence refs, interpreter proposals, and reducer decisions;
- terminal outcome and relevant timing/order observations;
- trace or checkpoint references only as subordinate evidence.

### StageGraph observations

Include a branching graph, the satisfying dependency result, slow unrelated sibling, downstream
launch time, sibling completion time, join decision, accepted obligations, and terminalization.
Hold the slow sibling behind a controlled synchronization gate while preserving the real LLM
operations. Use Temporal-history event ordering to demonstrate incremental release; provider
latency and wall-clock timestamps alone are not sufficient.

### GoalDirected observations

Include executor and independent-verifier operation identities/bindings, accepted iteration output,
verifier decision, revision or convergence proposal, handoff/context identity when exercised, and
reducer-authorized terminalization. Shared session or writable workspace between executor and
verifier is a failed qualification.

Real provider success cannot replace deterministic semantic assertions. A skipped live test cannot
support `accepted`; record it as an unresolved gate.

## Capability boundary

Blueprint live tests use the smallest exact Deep Agent binding needed to prove family runtime
logic. They need not combine Skills, MCP servers, executable sandboxes, sandbox snapshots, or all
subagent modes. `WP-CP-050` owns that cohesive advanced-capability proof.

## Sanitization

Preserve identities and digests needed for reproducibility while replacing secrets and sensitive
payloads with explicit markers such as `<redacted>`. Summarize LLM inputs/outputs to the minimum
needed to prove the assertion. Record costs and usage when available without exposing account data.
