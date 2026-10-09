---
type: Research Note
title: "claude_cloud feasibility: Anthropic-hosted Claude Code control surface (MP-16)"
description: "Per-feature evidence, checked 2026-10-08, for whether the Anthropic-hosted Claude Code cloud-session product exposes a documented, automatable lifecycle that Mission Control can admit as the claude_cloud lane profile. Documentation and local --help only; no live drill ran."
tags: [mission-control, qualification, lanes, claude_cloud]
---

# claude_cloud feasibility (MP-16 / OVE-79)

Checked 2026-10-08. Evidence is primary documentation plus the locally installed CLI's `--help`
output. **No hosted session was created, no prompt was sent, no login or paid call was made.**
Every feature is therefore `qualified: false`; "documented" below never means "observed".
Machine-readable companion: [feasibility-matrix.json](feasibility-matrix.json).

## Scope

In scope: the **Anthropic-hosted Claude Code cloud session** product ("Claude Code on the web",
claude.ai/code), reached through its public surfaces: the `claude` CLI (`--cloud`, `-p --cloud`,
`--teleport`, `/remote-env`), the routine `/fire` HTTP endpoint, and the documented telemetry
and hook mechanisms that run inside the hosted VM.

Explicitly **non-qualifying** for this profile, even where encountered below:

| Surface | Why it does not count |
| --- | --- |
| Claude Agent SDK / `claude -p` on our worker (MP-07 `claude_agent_sdk`) | Our compute, local lane. Kept separate; nothing here certifies it or vice versa. |
| Claude Managed Agents (`/v1/beta/sessions`, `/v1/beta/sessions/{id}/events`, ...) | A different Anthropic managed-agent product on the Claude Platform, not Claude Code cloud sessions. |
| Self-hosted environments (`--environment ccpool_...`, `claude self-hosted-runner`) | Cloud-session product on *our* infrastructure; not Anthropic-hosted. Would be a separate placement/profile. |
| Remote Control (`--remote-control`), cross-session `SendMessage`/`ListAgents` | Steers a *local* session, or is a model-side tool requiring a Remote Control-connected local session; not a distributable control API. |
| Agent view / background agents (`claude agents --json`, `attach`, `logs`, `stop`, `rm`, `respawn`) | Local background sessions only ("Agent view runs sessions on your machine"). |
| Projects (claude.ai coordinator threads), mobile/desktop/web UI actions, archive/delete buttons | UI-only; no documented automation API. |
| `claude ultrareview` | Cloud-hosted code-review feature with its own fixed workflow, not a general coding lane. |
| Compliance API remote sessions (`/v1/compliance/apps/sessions/remote`) | Documented as returning **Cowork** sessions only; Claude Code cloud sessions are explicitly excluded. |
| Browser automation, scraped claude.ai cookies, private endpoints behind the web app | Prohibited by RESEARCH.md exit criteria. None were used. |

## Product identity

| Item | Value |
| --- | --- |
| Product | Claude Code cloud sessions ("Claude Code on the web"), Anthropic-hosted environments, plus Routines (research preview) |
| Local CLI | `2.1.295 (Claude Code)` (`claude --version`, this Windows host) |
| CLI version inside the hosted VM | Unknown; not observable without a live session. Docs gate features on in-VM versions (e.g. `/teleport` reply needs v2.1.223+, `/fast` v2.1.271+). |
| Routine fire API | `POST https://api.anthropic.com/v1/claude_code/routines/{trig_id}/fire`, `anthropic-version: 2023-06-01`; labelled experimental; optional legacy `anthropic-beta: experimental-cc-routine-2026-04-01` |
| Plans | Pro, Max, Team, Enterprise (premium or Chat + Claude Code seats). Org policy `allow_remote_sessions` must be on. ZDR or HIPAA organizations cannot use cloud sessions. |
| Account/plan of owner | Not checked (would require reading local credentials or logging in). |
| Hosted VM | Ubuntu 24.04 x86_64, fresh per session, repository cloned via GitHub proxy (documented) |

## Sources (all retrieved 2026-10-08)

- S1 https://code.claude.com/docs/en/claude-code-on-the-web
- S2 https://code.claude.com/docs/en/cli-reference
- S3 https://code.claude.com/docs/en/cloud-environments
- S4 https://code.claude.com/docs/en/routines
- S5 https://platform.claude.com/docs/en/api/claude-code/routines-fire
- S6 https://code.claude.com/docs/en/monitoring-usage (section "Telemetry from cloud sessions and Claude Tag")
- S7 https://code.claude.com/docs/en/hooks
- S8 https://code.claude.com/docs/en/settings (section "Settings in cloud sessions")
- S9 https://platform.claude.com/docs/en/manage-claude/compliance-sessions, `/api/compliance/apps/sessions/remote/list`, `/remote/messages/list`
- S10 https://platform.claude.com/docs/en/api/beta/organization/usage_report/retrieve_claude_code
- S11 https://code.claude.com/docs/en/agent-view, https://code.claude.com/docs/en/cross-session-messaging, https://code.claude.com/docs/en/claude-projects
- S12 Local `claude --help`, `claude agents|attach|logs|stop|rm|respawn|ultrareview|auth --help` (CLI 2.1.295)
- Index used for discovery: https://code.claude.com/docs/llms.txt, https://platform.claude.com/llms.txt

Historical context only, not evidence: [2026-10-07 coding-lane survey](../../../research/2026-10-07-coding-lane-surfaces.md).

## Per-feature evidence

Dispositions: `documented_native` (a public, automatable operation exists for the whole feature),
`documented_partial` (a public operation covers part of it; gaps listed), `undocumented` (no
public automatable operation found, or docs ambiguous), `explicitly_unsupported` (docs say no).
"Observed" is `not_run` everywhere.

### Summary

| Feature | Disposition | Automatable surface found |
| --- | --- | --- |
| launch | documented_partial | Routine `/fire` (returns session ID, no idempotency, pre-built routine only); `claude --cloud "<task>"` (no documented machine-readable output) |
| inspect_status | undocumented | None for Claude Code cloud sessions; Compliance API excludes them; `claude agents --json` is local only |
| observe_replay | documented_partial | Push-only OpenTelemetry export from the hosted VM; no server-side replay or cursor |
| follow_up | documented_partial | `claude -p "<msg>" --cloud <id> --output-format json` returns `{ok, session_id, url}`; queue-and-exit, no message ID, no delivery ack |
| cancel_interrupt | undocumented | None; archive/delete are UI-only; `claude stop` is local-only |
| usage | documented_partial | OTel `claude_code.cost.usage` / `token.usage` per session (client estimate); Admin daily aggregate with `is_remote` |
| output_custody | documented_partial | Session pushes `claude/`-prefixed branches through the GitHub proxy; retrieve via GitHub ourselves; no session-to-artifact API |
| environment_selection | documented_partial | `remote.defaultEnvironmentId` setting via interactive `/remote-env`; `--environment` explicitly rejects Anthropic-hosted `env_` IDs; routine binds env in UI |
| configuration_materialization | documented_partial | Committed repo `.claude/` + `.mcp.json` (single-repo sessions), environment variables/setup script (UI), server-managed settings; repo plugins explicitly not installed |
| approval_suspension_resume | documented_partial | Repo-committed `PreToolUse`/`PermissionRequest` hooks (incl. `type: "http"`) fire in cloud, synchronous within hook timeout; native prompts answerable only in UI; `defer` only under `-p` |
| subordinate_lineage | documented_partial | OTel `agent_id`/`parent_agent_id`, `SubagentStart`/`SubagentStop` hooks; no API |
| bounded_continuation_compaction | documented_partial | Auto-compaction in cloud (trigger set by product), `CLAUDE_CODE_AUTO_COMPACT_WINDOW`, `/compact` in session, `PreCompact`/`PostCompact` hooks; no fork/continue API; teleport is an interactive local copy |

### launch — documented_partial

- **Sources:** S1 ("From terminal to cloud"), S2 (`--cloud`), S4 ("Add an API trigger"), S5.
- **Invocation A (CLI):** `claude --cloud "<task description>"` from a checkout whose GitHub remote/branch is pushed; or with `CCR_FORCE_BUNDLE=1` to upload a git bundle (<100 MB, untracked files excluded; on native Windows uncommitted tracked-file changes upload unfiltered). Interactive provisioning checklist; **no documented `-p`/`--output-format json` contract for creation**, so the new session ID is not machine-readable by contract. `--remote` is a deprecated alias. `--restricted` refuses to create cloud sessions.
- **Invocation B (HTTP):** `POST /v1/claude_code/routines/{trig_...}/fire`, headers `Authorization: Bearer sk-ant-oat01-...`, `anthropic-version: 2023-06-01`, `Content-Type: application/json`; body `{"text": "<= 65,536 chars"}` (unknown fields ignored). Returns `200 {"type":"routine_fire","claude_code_session_id":"session_...","claude_code_session_url":"https://claude.ai/code/session_..."}`. Request returns once the session is created; does not stream or wait.
- **Constraints of B:** routine must be pre-created in the web UI (prompt, repos cloned from default branch, environment, connectors, model). API trigger and token can only be created in the web UI ("The CLI cannot currently create or revoke tokens"; "There is no public API for token management"). `text` arrives wrapped as untrusted data; the saved prompt must opt in to acting on it. Routines run with **no permission-mode picker**, fully autonomous, and every included connector's tools (including writes) without approval. Research preview.
- **Auth mode:** A: claude.ai subscription OAuth (`claude auth login`); API keys, Bedrock/Vertex/Foundry rejected. B: per-routine bearer token, scope "one routine only; no read access".
- **Native IDs:** `session_...` / `cse_...` session ID, claude.ai/code URL; routine trigger `trig_...`.
- **Idempotency:** B: "There is no idempotency key. If a webhook caller retries, the endpoint creates multiple sessions." A: none documented. Mission Control cannot dedupe an ambiguous create (timeout after send) because there is no list/lookup operation to reconcile against.
- **Failure/retention:** B errors in standard envelope: 400 (bad version, text too long, routine paused), 401, 403, 404, 429 with `Retry-After` (30 fires/hour/routine shared with Run now; 100 API fires/hour/account), 500, 503. A: `Session creation failed`, `Not uploading this working tree: ...`, `Unable to get organization UUID`, org-policy refusals.
- **Observed:** not_run.

### inspect_status — undocumented

- **Sources:** S9, S11, S12, S5.
- **Finding:** No documented operation returns the status of a Claude Code cloud session by ID.
  - The Compliance API remote-session endpoints have exactly the needed shape (`status: active|paused|archived|failed|pending`) but S9 states: "Claude Code cloud sessions (including Claude Code routines that run in the cloud) ... are not remote sessions ... the remote session endpoints return Cowork sessions only." Also Enterprise-only, compliance-scoped key. **Non-qualifying.**
  - `claude agents --json [--all]` is documented as "the supported way to read session state from outside Claude Code", but agent view covers sessions on this machine only.
  - Routine `/fire` token has "no read access". Routine run status ("green status ... does not mean the task succeeded") is UI or conversational `/schedule` only.
  - Indirect liveness only: OTel events stop/start, `Stop`/`SessionEnd` hooks POSTing to an endpoint we host (see observe).
- **Auth / IDs / idempotency:** n/a.
- **Failure/retention:** VMs pause after a few idle minutes and can be reclaimed; reclaim loses running background work (subagents, shell commands). No API reveals paused vs reclaimed.
- **Observed:** not_run.

### observe_replay — documented_partial

- **Sources:** S6, S3 ("Set environment variables", "Requests that never get the secret"), S7, S1.
- **Invocation:** set `CLAUDE_CODE_ENABLE_TELEMETRY=1` and `OTEL_*` (exporter endpoint, `OTEL_LOG_USER_PROMPTS`, `OTEL_LOG_TOOL_DETAILS`, `OTEL_LOG_TOOL_CONTENT`, optionally `OTEL_LOG_RAW_API_BODIES`) either on the cloud environment's variables or in the org's server-managed settings `env` block. The environment's network access must allow the collector host. Events: `claude_code.user_prompt`, `assistant_response`, `tool_result`, `tool_decision`, `api_request`, `api_error`, plus trace spans `claude_code.interaction`, `llm_request`, `tool`, `tool.blocked_on_user`, `hook`.
- **Alternative push channel:** repo-committed hooks (`type: "http"`) on `PostToolUse`, `Stop`, `SubagentStop`, `SessionEnd`, etc. POST JSON to our endpoint (S7: hooks fire "wherever it runs ... cloud sessions"; single-repo sessions only).
- **Auth mode:** none toward Anthropic; collector credentials must NOT go in environment variables (readable by every user of the environment, and network secrets are never attached to telemetry export). A credentialed collector requires server-managed settings (org Owner).
- **Native IDs:** `session.id`, `ccr.session.id` (= `CLAUDE_CODE_REMOTE_SESSION_ID`), `organization.id`, `prompt.id`, `message.uuid` (v2.1.214+), `tool_use_id`, `agent_id`, `parent_agent_id`.
- **Idempotency/dedup:** we would dedupe on `(ccr.session.id, message.uuid | tool_use_id, event name)`; OTLP export is best-effort and **not replayable**; no server cursor or offset.
- **Failure/retention:** missed exports are lost to Mission Control. **No replay surface**: the only documented transcript retrieval for cloud sessions is `claude --teleport <id>`, which is interactive, needs a clean checkout of the same repo, and creates a *local copy*. Compliance transcripts exclude Claude Code cloud sessions. Prompt/tool content export is opt-in and sensitive (PHI/secret handling must be decided before enabling).
- **Observed:** not_run.

### follow_up — documented_partial

- **Sources:** S1 ("Send follow-ups from the CLI", "Errors when sending to a cloud session"), S2.
- **Invocation:** `claude -p "<message>" --cloud <session_... | cse_... | claude.ai/code URL> --output-format json` or stdin `echo "<message>" | claude -p --cloud <id>`. "The CLI queues the message into the session and exits without waiting for a reply." `--output-format stream-json` explicitly unsupported with `--cloud <id>`. `--cloud <id>` without `-p` errors (`Attaching to an existing cloud session is not enabled for your account.`).
- **Auth mode:** claude.ai account (`claude auth login`) on any machine; sends no local session state. Not available with third-party providers. Whether a `claude setup-token` long-lived token (`CLAUDE_CODE_OAUTH_TOKEN`) is accepted here is **undocumented**.
- **Native IDs:** returns `{ok: true, session_id, url}` or `{ok: false, session_id, error}`; **no message ID**.
- **Idempotency:** none documented; a retry after an ambiguous exit may double-queue. Queued messages can be taken back only in the web UI ("click the ✕").
- **Failure/retention:** `Session not found: <id>`, `cloud session is archived and cannot accept new messages`, org-policy and provider errors print to stderr without JSON. A message to an idle session restores the VM; to a reclaimed VM, history is restored but background work is not. `ok: true` proves enqueue, not that Claude read or acted on it.
- **Observed:** not_run.

### cancel_interrupt — undocumented

- **Sources:** S1 ("Archive sessions", "Delete sessions"), S11, S12, S5.
- **Finding:** No CLI flag, subcommand or HTTP endpoint interrupts a running turn or stops/archives a Claude Code cloud session. Archive and delete are sidebar/menu actions in claude.ai/code. `claude stop|kill <id>` and `claude rm <id>` operate on local background sessions. Routines can be paused (schedule only) or deleted in the web UI; the `/fire` token cannot. Sending "stop" as a follow-up message is a model instruction, not a control, and cannot fence effects.
- **Failure/retention:** without cancel, budget overrun, Stop Fence and cancel-and-replace (SPEC-01) cannot be enforced; work continues until the model stops or the VM idles out.
- **Observed:** not_run.

### usage — documented_partial

- **Sources:** S6 (metrics), S10, S4 ("Usage and limits"), S2 (`--max-budget-usd`).
- **Invocation:** OTel metrics `claude_code.cost.usage` (USD, client-side estimate), `claude_code.token.usage`, `claude_code.active_time.total`, attributed by `session.id`/`ccr.session.id` (`OTEL_METRICS_INCLUDE_SESSION_ID` default true). Admin API `GET /v1/organizations/usage_report/claude_code?starting_at=YYYY-MM-DD` returns **daily per-actor aggregates** with `is_remote` and `num_sessions`, tokens and estimated cost by model; not per session.
- **Auth mode:** OTel: none (push). Usage report: Admin API key (`X-Api-Key`); applicability to Pro/Max individual accounts is unclear from the docs.
- **Native IDs:** session attribution on OTel only; usage report has actor + date only.
- **Idempotency:** OTel counters are cumulative per session; usage report is a daily read (idempotent).
- **Failure/retention:** cloud sessions draw down **subscription usage limits** (claude.ai/settings/usage, UI), shared with all other Claude usage; optional metered overage via usage credits. No per-session hard budget for cloud sessions is documented (`--max-budget-usd` is print-mode only). Lost OTel exports mean lost usage.
- **Observed:** not_run.

### output_custody — documented_partial

- **Sources:** S1 ("Review changes", "From cloud to terminal"), S3 ("GitHub proxy", "Link output back to the session"), S4 ("Repositories and branch permissions"), S2 (`--from-pr`, `--teleport`).
- **Invocation:** the session pushes to `claude/`-prefixed branches (routines) through the GitHub proxy, which rejects branch deletion and tag pushes but does not restrict which branch is updated; PR creation is a UI action or a model action. Mission Control would retrieve branch/commit/diff via the GitHub API under its own GitHub credentials. The session can read its own `CLAUDE_CODE_REMOTE_SESSION_ID`, so a committed hook or the prompt can stamp it into commit trailers/PR bodies for correlation. `claude --teleport <id>` fetches and checks out the session branch locally (interactive, clean tree required, same account, same repo). `--from-pr` resumes a *local* session linked to a PR.
- **Auth mode:** GitHub App or `/web-setup` token on the claude.ai account; our own GitHub token for retrieval.
- **Native IDs:** branch name, commit SHA, PR number (GitHub-native); no Anthropic artifact ID.
- **Idempotency:** GitHub refs are idempotent to read; mapping session to branch is by convention we impose, not by a provider contract.
- **Failure/retention:** unpushed work in a reclaimed VM is lost; the web diff view is UI-only; non-GitHub hosts cannot receive pushes (bundle-only). No API returns "the branch/commit this session produced".
- **Observed:** not_run.

### environment_selection — documented_partial

- **Sources:** S3 ("The Default environment", "Select an environment from the CLI", "Archive an environment"), S2 (`--environment`), S4.
- **Invocation:** `/remote-env` (interactive picker) writes `remote.defaultEnvironmentId` in user settings; CLI cloud sessions use it, else fall back to the Anthropic-hosted environment, else the first non-bridge environment. `--environment <id>` accepts only self-hosted `ccpool_...`: "Claude Code rejects Anthropic-hosted `env_` IDs passed to the flag" (**explicitly_unsupported per invocation**). Whether `--settings '{"remote":{"defaultEnvironmentId":"env_..."}}'` is honored for one invocation is undocumented. Routines bind an environment in the web form.
- **Environment lifecycle:** create/edit/archive only in the web selector or admin pages; "There's no settings page or direct URL"; environments cannot be deleted, only archived. No list/get API, no revision or digest.
- **Auth mode:** claude.ai account.
- **Native IDs:** `env_...` (Anthropic-hosted), `ccpool_...` (self-hosted, non-qualifying).
- **Idempotency:** n/a (settings write).
- **Failure/retention:** archived environment: new sessions fall back silently for CLI defaults; routines fail to start. Setup-script cache rebuilds on script/allowlist change or ~7-day expiry; a script over ~5 minutes is not cached. Variable edits reach an existing session only on its next VM restore/rebuild.
- **Observed:** not_run.

### configuration_materialization — documented_partial

- **Sources:** S3 ("What carries over from your setup", "Setup scripts vs. SessionStart hooks"), S8, S7, S4 ("Connectors").
- **What reaches the hosted VM:** committed `CLAUDE.md`, `.claude/rules/`, `.claude/skills|agents|commands/`; committed `.claude/settings.json` hooks/permissions and `.mcp.json` **only in single-repository sessions**; server-managed settings (org); environment variables and setup script (web UI only); claude.ai connectors and skills enabled on claude.ai. **Not reached:** user `~/.claude/*`, local `--settings`/`--mcp-config`/`--plugin-dir` (follow-up "sends no local session state"; for launch, pass-through is undocumented), device MDM/managed-settings files. **Explicitly unsupported:** plugins/marketplaces declared in repo `enabledPlugins`/`extraKnownMarketplaces` are not installed.
- **Invocation for Mission Control:** the only automatable per-run route is the SPEC-02 one: materialize config into an admitted integration branch/commit, then launch from it. CLI launch clones "your current branch", so this is possible with `claude --cloud`; routines clone the default branch unless the prompt says otherwise (not a control).
- **Auth / IDs:** claude.ai account; no effective-config attestation ID.
- **Idempotency:** commit SHA of the materialized branch is our digest; the product reports no effective configuration.
- **Failure/retention:** settings files that fail validation are silently ignored in print mode (S12); multi-repo sessions silently skip hooks/permissions/env; `localhost` MCP endpoints are unreachable; environment variables are readable by every environment user (no secrets).
- **Observed:** not_run.

### approval_suspension_resume — documented_partial

- **Sources:** S1 ("Permission modes in cloud sessions", "Environment expired"), S4, S7 (`PreToolUse` `permissionDecision`, `defer`, HTTP hook fields, `timeout`), S6 (`tool.blocked_on_user`).
- **Native:** permission mode is picked from the web dropdown at creation and while running; native permission prompts and `AskUserQuestion` are answered in claude.ai/web/mobile UI only. A session idle while waiting for an MCP connector approval "can expire during that wait". Routines have no permission picker and run unattended.
- **Automatable piece:** repo-committed `PreToolUse` / `PermissionRequest` hooks, including `type: "http"`, fire in cloud sessions and can return `allow` / `deny` / `ask`. The call blocks synchronously until the hook answers or its `timeout` (default 600 s for `http`) expires. That can enforce a Mission Control gateway decision, but it is not durable suspension: a human decision longer than the timeout, a VM pause, or a reclaim is unrecoverable. `ask` falls back to the UI prompt.
- **Explicitly unsupported in this product:** `permissionDecision: "defer"` "Claude Code honors this value only in non-interactive mode with the `-p` flag"; cloud sessions are not driven by `-p` on our side, so durable defer/resume is unavailable.
- **Auth:** our hook endpoint must be reachable through the environment allowlist. Credential handling is unresolved: environment variables are readable by every environment user; network secrets exist only on Pro/Max, and whether they attach to Claude Code's own HTTP-hook requests is undocumented (they are documented as *not* attaching to its telemetry export).
- **Native IDs:** `tool_use_id`, `session_id` in hook payload.
- **Idempotency:** we would key decisions on `(ccr.session.id, tool_use_id)`.
- **Observed:** not_run.

### subordinate_lineage — documented_partial

- **Sources:** S1 ("Subagents work the same way they do locally"; agent teams via `CLAUDE_CODE_EXPERIMENTAL_AGENT_TEAMS=1`), S6 (`agent_id`, `parent_agent_id`, `query_source`, `subagent_type`, subagent spans nest under parent `claude_code.tool`), S7 (`SubagentStart`, `SubagentStop`, `agent_type`, `last_assistant_message`).
- **Invocation:** OTel export and/or repo-committed HTTP hooks, as in observe.
- **Native IDs:** `agent_id`, `parent_agent_id`, `tool_use_id`.
- **Idempotency:** push-only, best-effort; dedupe as in observe.
- **Failure/retention:** subagents running when a VM is reclaimed are not restored. `--forward-subagent-text` is print/stream-json only and not available for `--cloud`. No API lists child agents.
- **Observed:** not_run.

### bounded_continuation_compaction — documented_partial

- **Sources:** S1 ("Manage context"), S3 (`CLAUDE_AUTOCOMPACT_PCT_OVERRIDE`), S7 (`PreCompact`, `PostCompact`), S6 (`query_source: "compact"`), S2 (`--teleport`, `--fork-session`).
- **Native:** auto-compaction runs in cloud sessions; the product sets `CLAUDE_AUTOCOMPACT_PCT_OVERRIDE` itself (user values are overridden); `CLAUDE_CODE_AUTO_COMPACT_WINDOW` on the environment, or `/autocompact <tokens>`, changes the window; `/compact [focus]` works in cloud sessions. Whether `claude -p "/compact ..." --cloud <id>` is interpreted as the command rather than as text is **undocumented**. `PreCompact` hooks can block compaction; `PostCompact` can report it.
- **Continuation:** no API to fork or continue a cloud session into a new bounded session; `--fork-session` applies to local resume; `--teleport` produces an interactive local copy (one-way). Mission Control's portable continuation would be "artifacts + new launch" (E), which depends on launch, custody and status, the operations that are missing or partial.
- **Observed:** not_run.

## Qualifying outcome

**Outcome 3: `claude_cloud` stays unqualified, with the precise missing vendor operations listed below.**

Justification:

1. **Cancel/interrupt has no public surface.** SPEC-01 makes cancel-and-replace, Stop Fence and budget enforcement mandatory for every lane turn; the example `claude-cloud.environment.yml` requires `cancel`. The only stop mechanisms are UI archive/delete or a model instruction.
2. **No status or replay.** Mission Control persists provider frames verbatim keyed by dedupe IDs and reconciles after worker restart (MP-18 acceptance: "Caller/worker restart does not start a duplicate cloud task"). With no status/list endpoint and no idempotency key on launch or follow-up, an ambiguous create or send cannot be reconciled. Push-only OTel cannot recover gaps.
3. **Outcome 2 was considered and rejected.** A "launch-and-forget" lane (routine `/fire` + `-p --cloud` follow-ups + OTel/hook observation + GitHub custody) is technically wireable. But every Mission Control workflow requires at least terminal settlement and cancel, and governed write approvals require an enforceable gate. A limited profile would admit zero workflows under ARCHITECTURE.md admission (a required feature missing anywhere is an admission error). So it is not worth exposing as a profile. This can be revisited if a workflow class is defined that genuinely needs only launch + artifact custody (e.g. unattended advisory PR drafts).
4. Outcome 1 needs launch, status, observe/replay, follow-up, cancel and usage as documented operations; three of those are undocumented and the rest are partial.

## Required controls still missing (vendor operations)

No public surface provides any of these for Anthropic-hosted Claude Code cloud sessions as of 2026-10-08:

1. **Create session with idempotency:** a documented API (or `claude --cloud ... -p --output-format json`) that creates a session from repo + ref + `env_` ID + model + permission mode + prompt, accepts a client idempotency key, and returns the session ID machine-readably. Routine `/fire` lacks idempotency, per-call repo/ref/env/permission selection and token API.
2. **Get/list session status:** `GET` session by ID and list/filter, with lifecycle state (pending / running / waiting-for-input / idle-paused / reclaimed / completed / failed / archived) and the in-VM Claude Code version.
3. **Event stream with replay:** a resumable stream or paged events endpoint with stable event IDs/cursors covering messages, tool calls/results, permission requests, subagent events, compaction and terminal result, with a documented retention window.
4. **Interrupt turn and stop/archive session:** an API/CLI operation with an acknowledgement and a settled terminal state.
5. **Follow-up with message identity:** send returning a message ID, accepting an idempotency key, and exposing read/applied state (plus the existing withdraw-queued-message as an API).
6. **Per-session authoritative usage:** tokens and subscription units per session, retrievable after the fact (not only push OTel or daily aggregates), and a per-session hard budget/limit.
7. **Session output manifest:** branch, base, head commit, PR URL and diff stats per session.
8. **Environment list/get with revision:** read environment ID, name, network policy and a setup revision/digest; select an Anthropic-hosted `env_` per invocation non-interactively.
9. **Per-session configuration injection and effective-config report:** settings/MCP/hooks/skills supplied at launch without a git commit, plus an attestation of what loaded (including in multi-repo sessions).
10. **Pending-approval API:** list pending permission prompts/questions with correlation IDs and answer allow/deny durably, surviving VM pause/reclaim; or `defer` semantics for hosted sessions.
11. **Subagent listing:** child agent IDs, parent links and status via API.
12. **Explicit compaction / continuation:** trigger compaction and fork/continue a session into a new bounded session via API.
13. **Automation credential:** a documented non-interactive, scoped credential for `--cloud` create/send and the above reads (today: claude.ai OAuth sign-in; `setup-token` applicability undocumented; routine tokens have "no read access").

## What a finite authorized drill would need (not performed)

A drill can only verify *documented_partial* behavior; it cannot create the missing operations, so it would not flip qualification. It is still useful to replace synthetic fixtures for launch/follow-up/OTel/hook frames.

- **Approval:** owner comment on OVE-79 approving a finite budget and naming the account (plan tier, org) and confirming `allow_remote_sessions` is on and the org is not ZDR/HIPAA.
- **Budget:** subscription usage only (no separate VM charge). Cap at 2 cloud sessions, ≤3 follow-ups each, ≤15 minutes wall clock each, model pinned to the cheapest admitted alias; usage credits/overage OFF so the drill cannot meter-bill. Record `claude.ai/settings/usage` before and after (manual, UI).
- **Inputs:** a throwaway GitHub repository with the Claude GitHub App installed and push access; a pushed branch containing a materialized `.claude/settings.json` with HTTP hooks (`SessionStart`, `PreToolUse`, `PostToolUse`, `SubagentStart`, `SubagentStop`, `PreCompact`, `PostCompact`, `Stop`, `SessionEnd`) pointing at a disposable public HTTPS receiver; a deterministic task prompt (e.g. "create file X, run tests, spawn one Explore subagent, commit and push").
- **Environment IDs:** one dedicated Anthropic-hosted environment `env_...` (Custom network: receiver + collector hosts only; `CLAUDE_CODE_ENABLE_TELEMETRY=1`, OTLP endpoint without credential headers, `OTEL_LOG_TOOL_DETAILS=1`, prompt/content logging OFF unless approved), selected via `/remote-env`; optionally one routine `trig_...` with an API trigger bound to the same environment and repo for the `/fire` path.
- **Checks:** create via CLI and via `/fire` (record raw response, retry behavior: confirm duplicate session on retry); follow-up JSON `{ok, session_id, url}` and archived-session error; OTel `ccr.session.id` vs returned ID; hook payload IDs; branch name/commit on GitHub; whether `-p "/compact" --cloud <id>` compacts; whether `--settings` with `remote.defaultEnvironmentId` overrides per invocation; in-VM `claude --version` via the prompt.
- **Cleanup:** archive then delete both sessions in the UI, revoke the routine token, delete the routine, archive the environment, delete the throwaway branches; record any session without a recorded ID as `unknown` and stop (mirroring the Cursor drill rule in [README.md](../README.md)).
- **Outputs:** scrubbed recordings (no tokens, e-mails or OTel credential) under `tests/integration/claude_cloud/recordings/<date>/` and a record `docs/qualification/lanes/claude_cloud-<date>.md` with `evidence: live_drill` but `outcome: unqualified` unless the vendor operations above have appeared.
