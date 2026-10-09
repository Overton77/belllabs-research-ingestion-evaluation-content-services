---
type: Research Note
title: "codex_cloud feasibility: OpenAI-hosted Codex product control surface (MP-17)"
description: "Documentation and source evidence, checked 2026-10-08, for whether the OpenAI-hosted Codex Cloud product exposes an automatable control surface for Mission Control. No live drill ran; nothing here qualifies the profile."
tags: [mission-control, qualification, lanes, codex_cloud]
---

# codex_cloud feasibility (MP-17 / OVE-80)

Checked 2026-10-08. Evidence is documentation and open-source code only. No hosted task was started,
no login was performed, and the Codex CLI was not installed. Every feature below is `qualified: false`.
Machine-readable copy: [feasibility-matrix.json](feasibility-matrix.json).

## Scope

In scope: the **OpenAI-hosted Codex Cloud product**, meaning cloud tasks that run in an OpenAI VM from a
published cloud environment, and every public way to drive them: the `codex cloud` CLI subcommands,
the top-level `codex apply`, and any documented public API for hosted tasks, environments and follow-ups.

Explicitly **not** in scope, and non-qualifying if offered as a substitute:

| Surface | Why it does not count |
| --- | --- |
| Local `codex` / `codex exec` / `codex resume` | Runs on the worker, not in the hosted product. |
| `codex app-server` JSON-RPC (and `--remote`, `remote-control`) | Local embedding surface; the documented method set (`thread/*`, `turn/*`, `thread/compact/start`) has no cloud-task methods. That is MP-08. |
| Codex TypeScript SDK (`@openai/codex-sdk`) / Python SDK (`openai-codex`) | Documented as "Programmatically control **local** Codex agents"; Python SDK drives local app-server. |
| OpenAI Agents API (`POST /v1/agents/sessions`, `OpenAI-Beta: agents=v1`) / Responses API / Agents SDK | A different managed-harness product with API-key auth; it does not create or control Codex Cloud tasks or environments. |
| ChatGPT web/desktop/mobile UI, Slack/Teams `@ChatGPT` delegation | Human UI or chat integrations, not an automation API. A UI action proves the product can do something, not that Mission Control can. |
| This desktop app's internal task tools, browser automation, scraped cookies | Private/non-distributable. |
| Direct calls to `https://chatgpt.com/backend-api/wham/*` | Private ChatGPT backend endpoints the CLI happens to use (see below). Undocumented; not a public API. |

## Product identity

- **Product:** Codex Cloud (OpenAI-hosted; docs titled "Codex Cloud | ChatGPT Learn"). The older
  "Codex Cloud (Legacy)" experience is a separate, to-be-deprecated surface kept for Code Review and the
  Linear/GitHub integrations; its setup-script/maintenance-script/12-hour-cache guidance is **not**
  substituted for the current product.
- **Automation entry:** `codex cloud` (alias `codex cloud-tasks`), labeled **Experimental** in the
  official command overview. `codex apply` is labeled Stable.
- **CLI version the evidence corresponds to:** source tag `rust-v0.162.0` (workspace `version = "0.162.0"`,
  GitHub release published 2026-10-08T18:55:59Z). `main` HEAD at check time: `22ebb0fb178f`. The docs pages
  do not state a CLI version; they are mutable and were read on 2026-10-08.
- **Auth mode:** ChatGPT account sign-in only. `init_backend` exits with "Not signed in. Please run
  'codex login' to sign in with ChatGPT" when `auth.uses_codex_backend()` is false, so API-key auth does
  not drive cloud tasks. Requests carry the ChatGPT bearer token and a `ChatGPT-Account-Id` header.
- **Backend actually called by the CLI:** default base URL `https://chatgpt.com/backend-api`
  (override `CODEX_CLOUD_TASKS_BASE_URL`, restricted to trusted ChatGPT HTTPS origins). Routes:
  `POST /wham/tasks`, `GET /wham/tasks/list`, `GET /wham/tasks/{id}`,
  `GET /wham/tasks/{id}/turns/{turn_id}/sibling_turns`, `GET /wham/environments`,
  `GET /wham/environments/by-repo/{provider}/{owner}/{repo}`. None of these are documented as a public API.

### Sources (primary, checked 2026-10-08)

- [S1] CLI command reference: https://developers.openai.com/codex/cli/reference (renders as "Developer commands | ChatGPT Learn"; also cited in RESEARCH.md as https://learn.chatgpt.com/docs/cli/reference)
- [S2] Codex Cloud overview: https://developers.openai.com/codex/cloud (= https://learn.chatgpt.com/docs/cloud)
- [S3] Cloud environments: https://learn.chatgpt.com/docs/environments/cloud-environments
- [S4] Codex Cloud (Legacy): https://learn.chatgpt.com/docs/environments/cloud-environment
- [S5] App-server: https://developers.openai.com/codex/app-server
- [S6] Codex SDK: https://developers.openai.com/codex/sdk
- [S7] Hooks: https://developers.openai.com/codex/hooks
- [S8] Auto-review: https://learn.chatgpt.com/docs/sandboxing/auto-review
- [S9] MCP: https://learn.chatgpt.com/docs/extend/mcp (search excerpt: "Local MCP servers and transports may not be available in the cloud")
- [S10] CLI argument schema: https://github.com/openai/codex/blob/rust-v0.162.0/codex-rs/cloud-tasks/src/cli.rs
- [S11] CLI handlers: https://github.com/openai/codex/blob/rust-v0.162.0/codex-rs/cloud-tasks/src/lib.rs
- [S12] Backend trait and types: https://github.com/openai/codex/blob/rust-v0.162.0/codex-rs/cloud-tasks-client/src/api.rs
- [S13] HTTP mapping: https://github.com/openai/codex/blob/rust-v0.162.0/codex-rs/cloud-tasks-client/src/http.rs
- [S14] Backend route table: https://github.com/openai/codex/blob/rust-v0.162.0/codex-rs/backend-client/src/client.rs
- [S15] Environment discovery: https://github.com/openai/codex/blob/rust-v0.162.0/codex-rs/cloud-tasks/src/env_detect.rs
- [S16] Agents API (non-qualifying, distinct product): https://developers.openai.com/api/docs/guides/agents-api/overview

"Documented" below means stated on an official docs page. "Source-only" means present in the pinned
open-source CLI but absent from the docs reference. "Observed" is always `not observed` because no drill ran.

## Per-feature evidence

Common to every row: auth = ChatGPT sign-in via `codex login` (OAuth/device code; API key rejected for
cloud); observed = not observed (no live drill).

| Feature | Disposition | Source | Invocation / schema | Native IDs | Idempotency / dedup | Failure / retention |
| --- | --- | --- | --- | --- | --- | --- |
| Launch | documented_partial | S1, S10, S11, S13 | `codex cloud exec --env ENV_ID [--attempts 1-4] [QUERY]` (documented); `--branch BRANCH` and stdin query are source-only. Body sent: `{"new_task":{"environment_id","branch","run_environment_in_qa_mode":false},"input_items":[{"type":"message","role":"user",...}],"metadata":{"best_of_n"}?}`. Source-only env var `CODEX_STARTING_DIFF` adds a `pre_apply_patch` input item. Stdout is only the task URL `https://chatgpt.com/codex/tasks/{task_id}`; no JSON. | `task_id` (parsed from URL); `environment_id` | None. No client request ID or idempotency key in the request; a retry after an ambiguous failure can create a duplicate task. Readback is only by scanning `list` (title/updated_at). | "Codex exits non-zero if the task submission fails." Error body text only. Experimental command. |
| Inspect / status | documented_partial | S1, S11, S12, S13 | `codex cloud list [--env ENV_ID] [--limit 1-20] [--cursor C] --json` (documented): `{"tasks":[{id,url,title,status,updated_at,environment_id,environment_label,summary,is_review,attempt_total}],"cursor"?}`. `codex cloud status TASK_ID` is source-only, plain text only, exits 1 whenever status is not `ready`. | `task_id`, `environment_id`, list `cursor` | Read-only; safe to repeat. | Status enum is lossy: `pending`, `ready`, `applied`, `error`; backend `cancelled` and `failed` both map to `error`, `in_progress` maps to `pending`. Task record retention undocumented. |
| Observe / replay | undocumented | S1, S12, S13 | No event stream, log stream or cursor. Only coarse polling of `list`/`status`. Assistant messages and prompt are fetched by the interactive TUI from `GET /wham/tasks/{id}` (`current_assistant_turn.output_items`); no non-interactive subcommand prints them. No tool-call, command-output or setup-log retrieval. | `turn_id` (TUI/source only) | n/a | No replay offsets; nothing to resume from after disconnect. |
| Follow-up | undocumented | S2, S3 | UI only: "request follow-ups" on web/mobile/desktop; Slack/Teams follow-up "from the same connected account". No CLI flag, subcommand or backend client method sends a message to an existing task (backend client has `create_task` only). | none | n/a | Existing task "continues with its own saved files"; saved VM state recoverable up to 7 days after last turn or resume (S3). |
| Cancel / interrupt | undocumented | S1, S12, S14 | No CLI subcommand, docs mention, or backend client method. `AttemptStatus::Cancelled` exists as a read-only status value only. | none | n/a | Cannot stop a running task from automation; cancelled tasks surface as `error`. |
| Usage | undocumented | S1, S11, S14 | No per-task token/compute usage in `list`/`status` output. The backend client has account-level `get_rate_limits` / `profiles/me` on private routes, not exposed by `codex cloud`. | none | n/a | No per-task cost attribution. |
| Output / artifact custody | documented_partial | S1, S10, S11, S12 | `codex apply TASK_ID` (documented, Stable): fetches latest diff and runs `git apply`, non-zero on conflict. Source-only: `codex cloud diff TASK_ID [--attempt N]` prints the unified diff; `codex cloud apply TASK_ID [--attempt N]`. Commit / open-PR / branch push is UI-only (S2, S4). | `task_id`, attempt placement (1-4), `turn_id` | Diff fetch is read-only; apply is local and conflict-checked (preflight in source). | Diff is the only retrievable artifact; no PR URL, branch name or commit SHA is returned. Saved state "doesn't replace source control" (S3). |
| Environment selection | documented_partial | S1, S3, S11, S15 | `--env ENV_ID` on `exec`/`list` (documented). The source also accepts a unique environment label. Environment listing is only in the interactive `codex cloud` picker (docs: "Use `codex cloud` to list options"). Create/edit/publish/republish/share is UI-only (S3). | `environment_id`, `environment_label` | n/a | No environment revision, publish timestamp or setup digest is exposed; republish changes new tasks silently ("existing tasks retain their own state"). |
| Configuration materialization | documented_partial | S3, S4, S7, S9, S10 | Environment-level only, via UI: repositories, install script, start skill, env vars, network secrets, internet allowlist, VPN (Tailscale), OIDC. Repo skills and repo `AGENTS.md` are read from the checked-out repo. Personal skills do not sync. Local MCP servers "may not be available in the cloud". Hooks: only admin-managed MCP hooks on the cloud orchestrator (described for Work Cloud with managed policy); command/local/plugin hooks are unsupported with cloud orchestration. `exec` has no per-task model, approval, sandbox, MCP, hooks or config overrides (`config_overrides` is `#[clap(skip)]`). | none | n/a | Only per-task injection is the branch and the source-only `CODEX_STARTING_DIFF` patch, neither documented as a config mechanism. Ambiguity: whether the Work Cloud managed-MCP-hook rule applies to Codex Cloud tasks is not stated. |
| Approval suspension / resume | undocumented | S2, S3, S8, S10 | The UI shows a permission control ("Do anything", "Approve for me"). Setup and Slack flows say Codex "asks you for missing details or access" / "Complete any connection and approval prompts". Nothing documents a programmatic pending-approval read or resume for a hosted task. `exec` has no approval-policy flag. Auto-review docs cover the desktop app and local config only. | none | n/a | A hosted task that pauses cannot be resumed or answered by automation. |
| Subordinate / subagent lineage | undocumented | S12, S13 | Only best-of-N sibling attempts are visible: `attempt_total` (documented in list JSON); `sibling_turn_ids` and `GET .../sibling_turns` are TUI/source only. No subagent/child-task tree, events or IDs. | sibling `turn_id`s (source only) | n/a | Best-of-N siblings are not subagents. |
| Bounded continuation / compaction | undocumented | S3, S5 | No hosted compaction control, context-usage reading or task rollover. `thread/compact/start` is a local app-server method only. Continuation exists only as a UI follow-up on the same task, or by starting a new task. | none | n/a | Saved VM state up to 7 days after the last turn/resume; no documented context-window behavior. |

Disposition counts: documented_native 0, documented_partial 5, undocumented 7, explicitly_unsupported 0.
Launch is marked partial, not native, because the command is Experimental, `--branch` is undocumented,
output is an unstructured URL and the request has no idempotency key.

## Qualifying outcome

**Outcome 3: the `codex_cloud` profile stays unqualified.** The precise missing vendor operations are
listed below.

Justification:

1. The only public automation surface is an **Experimental** CLI command whose documented operations
   are submit (`exec`) and list (`list --json`), plus a local diff apply. The ticket acceptance says
   limited start/list does not certify parity.
2. Follow-up, cancel, observation, approval resume and continuation are absent from every public
   surface. That breaks Mission Control's Stop Fence (no cancel), Human Gate on hosted effects (no
   approval suspension/resume), Provider Frame observation (no events), and the `in_doubt`
   reconciliation rule in ARCHITECTURE.md (no idempotent create or reliable readback).
3. The CLI only works with ChatGPT account auth against private `chatgpt.com/backend-api/wham/*`
   routes. Calling those routes directly would be a private endpoint, which RESEARCH.md forbids as a
   production lane. Driving them through the CLI makes Mission Control depend on an Experimental
   wrapper with no schema stability promise.

Why not outcome 2 (limited integration): a submit, poll, collect-diff job is technically possible.
But it would have to reject every workflow that needs a Stop Fence, a Human Gate on hosted actions,
live observation, follow-up turns, rollover, subagent visibility or per-task config/hook enforcement.
It would also carry duplicate-launch risk. Shipping that as a lane would recreate the "start/list
proves parity" mistake. If the owner explicitly wants a submit-and-collect-diff job anyway, it should
be specified as a new, separate decision with those rejections written into admission, not slipped
in under this profile.

Why not outcome 1: there is no documented operation set to implement an adapter against.

## Required controls still missing

These are vendor operations Mission Control needs that no public surface provides as of 2026-10-08:

1. **Public, stable task API or non-experimental CLI contract** with a documented JSON schema for
   create/read, usable with a server-side credential (API key, service account or workspace token)
   rather than a personal ChatGPT session.
2. **Idempotent create:** a client-supplied request/idempotency key on task creation, or a lookup by
   client reference, so ambiguous launches can be reconciled without duplicates.
3. **Structured launch result:** a JSON response with task ID, environment ID/revision, resolved branch
   and commit.
4. **Follow-up:** send a message (new turn) to an existing hosted task, returning a turn ID.
5. **Cancel/interrupt:** stop the active turn of a hosted task, with a terminal `cancelled` state that
   is distinguishable from `failed`.
6. **Event stream or paged log:** turn/item/tool-call/command-output events with a resumable cursor
   and documented retention.
7. **Fine-grained status:** `queued | setting_up | in_progress | awaiting_input | completed | failed |
   cancelled` per turn, not the lossy `pending | ready | applied | error`.
8. **Approval read/resume:** list a task's pending approval/input requests and answer them
   programmatically, plus a per-task approval-policy selector.
9. **Per-task usage:** tokens/compute attributed to the task and turn.
10. **Artifact custody:** retrieve the PR URL, pushed branch and commit SHA, and programmatically
    request PR creation or branch push.
11. **Environment introspection:** non-interactive environment listing with a revision/publish ID or
    setup digest, so a binding can pin the exact published setup.
12. **Per-task configuration:** per-task overrides for model, instructions/`AGENTS.md`, MCP servers,
    hooks and network policy, or a documented attestation of what the task actually loaded.
13. **Subordinate lineage:** subagent/child-task IDs and events.
14. **Continuation/compaction:** read context usage, trigger compaction, or roll over to a new task
    with an explicit handoff.

## What a finite authorized drill would need

Do not run this without owner approval. It would only convert documented/source claims into observed
fixtures; it cannot make the profile qualify, because the missing operations above don't exist.

- **Inputs:**
  - an owner-approved budget comment on OVE-80;
  - a ChatGPT plan with Codex Cloud enabled (Plus or higher; VM size depends on plan, per S3), signed in
    with `codex login` on a disposable machine profile (`CODEX_HOME` pointed at a scratch dir);
  - a pinned CLI `0.162.0`, recorded with `codex --version` and OS;
  - a throwaway GitHub repository connected to ChatGPT.
- **Environment IDs:** one published private environment over that repository. Record its
  `environment_id` and label from the `codex cloud` picker or the `list --json` `environment_id`.
  Install script and start skill should be trivial, internet off, no secrets.
- **Probes (one task each, `--attempts 1`):**
  1. `exec` with a no-op prompt. Capture the URL/task ID and exit code.
  2. Poll `list --json --env` and `status` until terminal. Capture each status transition.
  3. Run `codex cloud diff` and `codex apply` in a scratch clone.
  4. Run `exec` twice with an identical prompt to record the duplicate-task behavior.
  5. Try a negative probe for any undocumented cancel or follow-up path. Expect none.
  6. Optionally run `--attempts 2` to record `attempt_total` and siblings.
- **Budget:** about 3–5 short tasks. Codex Cloud usage counts against the ChatGPT plan's limits, not
  metered API dollars, so record plan-limit consumption before and after. Cost is unknown until the
  drill measures it.
- **Outputs:** scrubbed recordings (no tokens, account IDs or e-mails) under
  `src/mission_control/adapters/codex_cloud/` protocol fixtures and a dated record
  `docs/qualification/lanes/codex_cloud-<date>.md` that mirrors the Cursor convention in
  [../README.md](../README.md), with `outcome: unqualified`.
