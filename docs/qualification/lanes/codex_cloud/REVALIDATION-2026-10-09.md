---
type: Research Note
title: "codex_cloud revalidation 2026-10-09: OpenAI-hosted Codex Cloud control surface (MP-17)"
description: "Fresh check, on 2026-10-09, of current OpenAI documentation and pinned open-source CLI code against the Oct 8 Outcome 3 decision for the codex_cloud profile. No login, no task, no paid call. Outcome unchanged."
tags: [mission-control, qualification, lanes, codex_cloud, revalidation]
---

# codex_cloud revalidation (MP-17 / OVE-80), 2026-10-09

Checked 2026-10-09 against current OpenAI documentation and the open-source Codex CLI at tag
`rust-v0.162.1` (released 2026-10-09T19:44:51Z). **No task was started, no login was performed, the Codex
CLI was not installed, and no paid or account-bound call was made.** This note revalidates
[FEASIBILITY.md](FEASIBILITY.md) and its [matrix](feasibility-matrix.json) (both dated 2026-10-08) and
does not replace them. Every feature stays `qualified: false`.

Owner rule applied: **cloud means the provider's own hosted coding product only**, here Codex Cloud tasks
running from a published cloud environment. The following do not count:

- local `codex exec` or app-server, including on our own remote worker;
- the Codex SDKs;
- the OpenAI Agents API and its hosted sandboxes;
- ChatGPT UI, Slack and Teams controls;
- the desktop app's internal task tools;
- private `chatgpt.com/backend-api/wham/*` routes.

## Decision

**Outcome 3 unchanged: `codex_cloud` stays unqualified.** The only public automation surface is still
`codex cloud exec` and `codex cloud list --json`, labelled **experimental**, plus the stable local
`codex apply`. Follow-up, cancel, event observation, approval resume, usage and lineage have no public
automatable operation. MP-19 remains evidence-blocked. Nothing is implementable as a qualified lane, so
this note contains no implementation sketch.

What changed since Oct 8 (none of it flips a required operation):

1. **The CLI release moved, but cloud code did not.** `rust-v0.162.1` and the `0.163.0-alpha.*` prereleases shipped. The last commit touching `codex-rs/cloud-tasks` is `3a16c0b70769` (2026-09-29, base-URL normalization), and the last touching `codex-rs/cloud-tasks-client` is `3ae4225b1761` (2026-08-28). At `rust-v0.162.1` the subcommands are still `exec | status | list | apply | diff`. The `CloudBackend` trait still has only `list_tasks`, `get_task_summary`, `get_task_diff`, `get_task_messages`, `get_task_text`, `list_sibling_attempts`, `apply_task_preflight`, `apply_task` and `create_task` (S7). It has no follow-up, cancel or approval method.
2. **A non-human workspace credential now exists for Codex automation, but it is not documented for cloud tasks.** ChatGPT workspace **service accounts** issue access tokens used via `CODEX_ACCESS_TOKEN` (CLI ≥ 0.142.0). They are "available only on pay-as-you-go plans", and the documented token scope is `chatgpt.workspace.feature.allow-codex-local-access.access`, with `codex exec` examples only (S4). In source, `AuthMode::PersonalAccessToken` and `AgentIdentity` satisfy `uses_codex_backend()`, the gate `codex cloud` checks (S8). So the CLI would not refuse such a token locally, but whether the backend accepts it for cloud tasks is **undocumented**. This narrows the Oct 8 credential gap but does not close it.
3. **The DevDay 2026 launch of "Codex in the cloud" covers UI and environments only.** Reusable environments are published and selected in ChatGPT web, desktop or mobile (S2, S3, S9). The "Agents API ... with the Codex harness" is a separate API-key product with its own `/v1/agents/environments/{environment_id}` (S10), and is non-qualifying.

## Sources (all retrieved 2026-10-09)

- S1 https://learn.chatgpt.com/docs/cli/reference (renders "Command line options"; `codex cloud` `type: "experimental"`)
- S2 https://learn.chatgpt.com/docs/cloud
- S3 https://learn.chatgpt.com/docs/environments/cloud-environments
- S4 https://learn.chatgpt.com/docs/enterprise/service-accounts
- S5 https://learn.chatgpt.com/docs/codex-sdk and https://learn.chatgpt.com/docs/app-server (no cloud-task methods)
- S6 https://learn.chatgpt.com/docs/third-party/slack ("Follow along" / "Cancel" controls)
- S7 https://github.com/openai/codex/blob/rust-v0.162.1/codex-rs/cloud-tasks/src/cli.rs, `.../cloud-tasks/src/lib.rs`, `.../cloud-tasks-client/src/api.rs`, `.../backend-client/src/client.rs`; commit history via `api.github.com/repos/openai/codex/commits?path=codex-rs/cloud-tasks`
- S8 https://github.com/openai/codex/blob/rust-v0.162.1/codex-rs/protocol/src/auth.rs (`uses_codex_backend`)
- S9 https://openai.com/index/devday-2026-recap/ and https://learn.chatgpt.com/docs/whats-new/devday-2026
- S10 https://openai.com/index/introducing-the-agents-api/ and https://developers.openai.com/api/docs/guides/agents-api/environments/openai-hosted (non-qualifying)
- Discovery index: https://learn.chatgpt.com/llms.txt (no "Codex Cloud API" or cloud-task reference page listed)

## Operation matrix

"Public surface" quotes are verbatim from the source on 2026-10-09. "Kind" says whether the surface is a
stable distributable API or a CLI/UI only. "Δ vs Oct 8" compares the disposition with
[FEASIBILITY.md](FEASIBILITY.md#per-feature-evidence).

| Operation | Public surface (source: quote) | Auth route | Kind | Δ vs Oct 8 | Still missing |
| --- | --- | --- | --- | --- | --- |
| Launch with repo/branch/environment | S1: "`codex cloud exec` submits a task directly". S1: `--env` "Target Codex cloud environment identifier (required)". S1: "Codex exits non-zero if the task submission fails." `--branch` is source-only (S7). Stdout is the task URL only. | S1: "Authentication follows the same credentials as the main CLI". In practice a ChatGPT sign-in; service-account token for cloud is undocumented (S4, S8) | Experimental CLI over private `POST /wham/tasks` | Unchanged (partial) | Stable API or non-experimental contract with JSON result (task ID, env revision, resolved branch/commit); idempotency key |
| Status / inspect | S1: "Use `--json` for automation. The JSON payload contains a `tasks` array plus an optional `cursor` value. Each task includes `id`, `url`, `title`, `status`, `updated_at`, `environment_id`, `environment_label`, `summary`, `is_review`, and `attempt_total`." | as launch | Experimental CLI | Unchanged (partial; lossy `pending | ready | applied | error`) | Fine-grained per-turn status with a distinct `cancelled` and `awaiting_input` |
| Observe / stream / replay | None. `get_task_messages` / `get_task_text` exist in the client trait but only the interactive TUI uses them (S7). | n/a | None | Unchanged (undocumented) | Event stream or paged log with cursor and retention |
| Follow-up | S2: "Review the changes and test results, request follow-ups, and commit or open a pull request when ready." S3: "Reopen the same task to continue its work across devices." | ChatGPT UI | UI (web, mobile, desktop) and Slack/Teams only | Unchanged (undocumented) | Send a turn to an existing task, returning a turn ID |
| Cancel / interrupt | S6: "When the **Follow along** and **Cancel** controls are available, they appear for the requester." No CLI subcommand or client method (S7). | Slack | UI/Slack only | Unchanged (undocumented) | Cancel the active turn with an acknowledged `cancelled` terminal state |
| Approval / tool-gate interception | S3: "Complete any connection and approval prompts, then review the result in the conversation." `exec` has no approval-policy flag (S1, S7). | ChatGPT UI / Slack | UI only | Unchanged (undocumented) | List and answer pending approvals; per-task approval policy |
| Usage | None per task. The client has only account-level `/wham/profiles/me` and rate-limit routes (S7, private). | n/a | None | Unchanged (undocumented) | Per-task and per-turn usage |
| Artifact / diff / branch collection | S1: `codex apply` (stable) "Apply the most recent diff from a Codex cloud chat to your local repository." `codex cloud diff` is source-only (S7). S3: "Commit important work or save the output you need. Saved state doesn't replace source control." | as launch | Stable local apply; diff fetch over private route | Unchanged (partial) | PR URL, branch, commit SHA; programmatic PR/branch push |
| Environment identity / revision attestation | S1: `--env` "Use `codex cloud` to list options." (interactive picker). S3: "Start a new task to use the update; existing tasks retain their own state." | ChatGPT UI | UI create/edit/publish/republish; ID passthrough on `exec` | Unchanged (partial) | Non-interactive environment list/get with publish revision or setup digest |
| Configuration materialization | S3: "Skills stored in your repository are available in cloud tasks. Personal skills from your local computer aren't synced to cloud environments." No per-task overrides on `exec` (S7: `config_overrides` is `#[clap(skip)]`). | ChatGPT UI | Environment-level UI plus repo content | Unchanged (partial) | Per-task model, instructions, MCP, hooks and network overrides, or a loaded-config attestation |
| Subordinate visibility | Only best-of-N `attempt_total` in list JSON (S1). Sibling turns are source/TUI only (S7). | as launch | Experimental CLI field | Unchanged (undocumented) | Subagent/child-task IDs and events |
| Bounded continuation / compaction | S3: "a task's saved VM state is recoverable for up to seven days after you last start a turn or resume the task." | ChatGPT UI | UI only | Unchanged (undocumented) | Context-usage read, compaction trigger or explicit rollover |
| Automation credential | S4: "Service accounts let you run and scale headless Codex workflows across your organization without relying on an employee's account." S4: "Service accounts are available only on pay-as-you-go plans." Token scope example: `chatgpt.workspace.feature.allow-codex-local-access.access`. | Workspace service-account token via `CODEX_ACCESS_TOKEN` | Documented for `codex exec` (local) only | **Narrowed**: non-human credential exists; cloud-task applicability undocumented | Documented server-side credential and scope for cloud tasks |

## Non-qualifying surfaces re-checked

| Surface (2026-10-09) | Why it still does not count |
| --- | --- |
| OpenAI Agents API (`client.beta.agents.sessions.create`, `environment: {type: "openai_hosted"}`, `/v1/agents/environments/{id}`) (S10) | A separate managed-agent API, "the harness behind Codex", with API-key billing and its own sandboxes. It does not create or control Codex Cloud tasks or published Codex Cloud environments. |
| Codex SDK (TypeScript and Python) (S5) | "The TypeScript library lets your application start, continue, and resume local Codex threads." "The Python SDK controls the local Codex app-server over JSON-RPC." |
| app-server JSON-RPC (S5) | Local threads and turns. The current reference has no cloud-task methods. |
| Slack/Teams `@ChatGPT` delegation and its Cancel button (S6) | Chat integration UI, not a distributable control API. |
| `https://chatgpt.com/backend-api/wham/*` (S7) | Private backend that the experimental CLI calls. Direct use is prohibited by [RESEARCH.md](../../../specs/multi-provider-2026-10/RESEARCH.md). |

## Still-missing vendor operations (OpenAI-hosted Codex Cloud)

Same 14 as Oct 8. Item 1 is narrowed by service accounts but stays open:

1. A public, stable task API or a non-experimental CLI contract with a documented JSON schema, usable with a server-side credential **documented for cloud tasks**. Service-account tokens are documented for local `codex exec` only.
2. Idempotent create: a client request/idempotency key, or lookup by client reference.
3. Structured launch result: task ID, environment ID and revision, resolved branch and commit.
4. Follow-up: send a new turn to an existing hosted task, returning a turn ID.
5. Cancel/interrupt the active turn, with a terminal `cancelled` distinct from `failed`.
6. Resumable event stream or paged log (turn, item, tool call, command output) with documented retention.
7. Fine-grained per-turn status (`queued | setting_up | in_progress | awaiting_input | completed | failed | cancelled`).
8. List and answer pending approval/input requests; per-task approval-policy selector.
9. Per-task and per-turn usage.
10. Artifact custody: PR URL, branch, commit SHA; programmatic PR creation or branch push.
11. Non-interactive environment listing with a publish revision or setup digest.
12. Per-task configuration overrides (model, instructions/`AGENTS.md`, MCP, hooks, network), or an attestation of what loaded.
13. Subagent/child-task lineage IDs and events.
14. Context-usage read, compaction trigger or explicit rollover.

## Drill prerequisites (unchanged; not performed)

A drill would only record the experimental submit/list/diff behavior; it cannot create items 2–14 and so
cannot flip qualification. The prerequisites in [FEASIBILITY.md](FEASIBILITY.md#what-a-finite-authorized-drill-would-need)
stand, with these updates:

- **Pin:** CLI `0.162.1` (or whichever release is current at drill time). Record `codex --version` and the `codex-rs/cloud-tasks` commit.
- **Extra probe (credential):** on a pay-as-you-go workspace with an owner-created service account, set `CODEX_ACCESS_TOKEN` in a scratch `CODEX_HOME` and run `codex cloud list --json`. Record whether the backend accepts a service-account token for cloud tasks and with which scope. Run this before any `exec`, and only with owner approval and a budget.
- Everything else is unchanged: throwaway repo, one published private environment, `--attempts 1`, about 3–5 tasks, negative probes for follow-up and cancel, and scrubbed recordings.

## Revisit trigger

Re-open MP-17 when OpenAI documents follow-up (item 4) and cancel (item 5) for Codex Cloud tasks on a
non-experimental surface with a documented server-side credential (item 1). Without those, the Stop Fence,
Human Gate on hosted effects and `in_doubt` reconciliation cannot be met for any workflow.
