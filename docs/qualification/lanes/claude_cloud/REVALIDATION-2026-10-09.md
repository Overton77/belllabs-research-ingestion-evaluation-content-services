---
type: Research Note
title: "claude_cloud revalidation 2026-10-09: Anthropic-hosted Claude Code control surface (MP-16)"
description: "Fresh check, on 2026-10-09, of current Anthropic documentation and the local CLI against the Oct 8 Outcome 3 decision for the claude_cloud profile. Documentation and local --help only; no login, no session, no paid call. Outcome unchanged."
tags: [mission-control, qualification, lanes, claude_cloud, revalidation]
---

# claude_cloud revalidation (MP-16 / OVE-79), 2026-10-09

Checked 2026-10-09 against current primary documentation and the locally installed CLI's `--help`.
**No hosted session was created, no prompt was sent, no login and no paid or account-bound call was made.**
This note revalidates [FEASIBILITY.md](FEASIBILITY.md) and its [matrix](feasibility-matrix.json)
(both dated 2026-10-08); it does not replace them. Every feature stays `qualified: false`.

Owner rule applied: **cloud means the provider's own hosted coding product only.** The Agent SDK or CLI on
our own remote worker, self-hosted environments (`ccpool_...`), Claude Managed Agents, Remote Control,
desktop/web UI actions and browser automation do not count. See the non-qualifying table in
[FEASIBILITY.md](FEASIBILITY.md#scope).

## Decision

**Outcome 3 unchanged: `claude_cloud` stays unqualified.** No new public operation exists for status,
cancel/interrupt, event replay, idempotent create or pending-approval answer on Anthropic-hosted
Claude Code cloud sessions. MP-18 remains evidence-blocked. Nothing is implementable as a qualified
lane, so this note contains no implementation sketch.

What changed since Oct 8 (none of it flips a required operation):

1. **Self-hosted dispatch has a machine-readable create.** `claude -p "<prompt>" --environment ccpool_... [--ref <branch>] --output-format json` prints `session_id` and exits (S5). The docs say this flag explicitly rejects Anthropic-hosted `env_` IDs (S3), so it is non-qualifying for this profile.
2. **The automation credential gap is now explicit in the vendor docs.** The cloud-session scope `user:sessions:claude_code` is capped at 30 days, `setup-token` does not cover it, and a machine identity is account-team only (S5). This closes the Oct 8 open question "does `setup-token` work for `--cloud`?" with a **no**.
3. **Per-invocation `env_` selection is now implied, but not confirmed.** The settings reference states that an `env_` value for `remote.defaultEnvironmentId` "follows the standard settings precedence" (S6), which includes the `--settings` flag. A `claude --cloud ... --settings '{"remote":{"defaultEnvironmentId":"env_..."}}'` path is plausible, but no example documents it. It still needs a drill.
4. Two more Anthropic APIs were found that are **not** this product: the self-hosted pool API `POST/DELETE /v1/code/runners/self-hosted/pools` (beta `ccr-byoc-2025-07-29`), and the Claude Platform `/v1/environments/*/work/*` work-queue endpoints. Both are non-qualifying (see [Non-qualifying surfaces re-checked](#non-qualifying-surfaces-re-checked)).
5. Local CLI is now `2.1.296 (Claude Code)`. The 2.1.289 to 2.1.296 changelog has cloud-session UI and reliability fixes only, and adds no lifecycle API or command (S12).

## Sources (all retrieved 2026-10-09)

- S1 https://code.claude.com/docs/en/claude-code-on-the-web
- S2 https://code.claude.com/docs/en/cli-reference
- S3 https://code.claude.com/docs/en/cloud-environments
- S4 https://code.claude.com/docs/en/routines and https://platform.claude.com/docs/en/api/claude-code/routines-fire
- S5 https://code.claude.com/docs/en/self-hosted-environments-testing
- S6 https://code.claude.com/docs/en/settings-reference (`remote.defaultEnvironmentId`)
- S7 https://code.claude.com/docs/en/hooks
- S8 https://code.claude.com/docs/en/monitoring-usage (section "Telemetry from cloud sessions and Claude Tag")
- S9 https://platform.claude.com/docs/en/manage-claude/compliance-sessions
- S10 https://platform.claude.com/docs/en/api/beta/organization/usage_report/retrieve_claude_code
- S11 https://code.claude.com/docs/en/agent-view, https://code.claude.com/docs/en/cross-session-messaging
- S12 https://raw.githubusercontent.com/anthropics/claude-code/main/CHANGELOG.md (head `2.1.296`); local `claude --version` = `2.1.296 (Claude Code)`; local `claude --help`
- S13 https://platform.claude.com/docs/en/api/beta/environments/work/stop (non-qualifying reference)
- Discovery indexes: https://code.claude.com/docs/llms.txt, https://platform.claude.com/llms.txt. A web search for an official Claude Code cloud-session create/list/archive API found none.

## Operation matrix

"Public surface" quotes are verbatim from the source on 2026-10-09. "Kind" says whether the surface is
a stable distributable API (HTTP with a documented schema and a non-human credential) or a CLI/UI only.
"Δ vs Oct 8" compares the disposition with [FEASIBILITY.md](FEASIBILITY.md#summary).

| Operation | Public surface (source: quote) | Auth route | Kind | Δ vs Oct 8 | Still missing |
| --- | --- | --- | --- | --- | --- |
| Launch with repo/branch/environment | S1: "This creates a new cloud session on claude.ai. The cloud VM clones your current directory's GitHub remote at your current branch". S1: "the CLI shows a live checklist of setup steps". S4: "Each successful request creates a new session. There is no idempotency key." S3: "Claude Code rejects Anthropic-hosted `env_` IDs passed to the flag, so use `/remote-env` to target those." | claude.ai OAuth (`claude auth login`) for `--cloud`; per-routine bearer `sk-ant-oat01-...` for `/fire` | CLI, interactive create with no documented JSON output; routine `/fire` is experimental HTTP bound to one pre-built routine | Unchanged. Machine-readable create exists only for self-hosted `--environment ccpool_...` (non-qualifying) | Non-interactive create for an `env_` environment returning the session ID; per-call repo, ref, env, model and permission mode; idempotency key |
| Status / inspect | S9: "These cloud sessions are not remote sessions, even though both run in the cloud; the remote session endpoints return Cowork sessions only." S11: "Agent view runs sessions on your machine". S4: per-routine token has "no read access". | n/a | None | Unchanged (undocumented) | `GET` session and list sessions with lifecycle state |
| Observe / stream / replay | S8: cloud telemetry via environment variables or server-managed settings; events carry `ccr.session.id`. S7: "Claude Code fires the same hook events wherever it runs: ... and cloud sessions." S5 reads replies "through a Stop hook" (self-hosted recipe). | none toward Anthropic (push to our collector or hook endpoint) | Push-only OTel or hooks; no server cursor | Unchanged (partial) | Resumable event stream or paged events with stable IDs and documented retention |
| Follow-up | S1: "The CLI queues the message into the session and exits without waiting for a reply." S1: "Pass `--output-format json` for a machine-readable result: `{ok, session_id, url}`". | claude.ai OAuth; scope `user:sessions:claude_code`, 30-day cap (S5) | CLI only | Unchanged (partial) | Message ID, idempotency key, delivered/applied state, programmatic withdraw |
| Cancel / interrupt / archive | S1: "To archive a session, hover over the session in the sidebar and select the archive icon." `claude stop|kill <id>` acts on "a background session" (S12 `--help`, local only). | n/a | UI only | Unchanged (undocumented) | Interrupt turn and stop/archive session with acknowledgement and settled terminal state |
| Approval / tool-gate interception | S7: hooks fire in cloud sessions; PreToolUse `permissionDecision` is "allow/deny/ask/defer". S7: "Claude Code honors this value only in non-interactive mode with the `-p` flag." S4: "there is no permission-mode picker". | our hook endpoint, reached through the environment network allowlist | Repo-committed hooks (synchronous, bounded by hook timeout); native prompts UI only | Unchanged (partial) | Durable pending-approval list/answer that survives VM pause/reclaim, or `defer` for hosted sessions |
| Usage | S8: `OTEL_METRICS_INCLUDE_SESSION_ID` includes "on cloud sessions, ccr.session.id". S10: `GET /v1/organizations/usage_report/claude_code` with `is_remote` (daily per-actor). | OTel push; Admin API key for the report | Push metrics plus a daily aggregate API | Unchanged (partial) | Per-session authoritative usage read and a per-session hard budget |
| Artifact / diff / branch collection | S1: "When a session completes, you can create a PR from claude.ai/code or teleport the session to your terminal". | GitHub App or `/web-setup` for the session; our GitHub credential to read refs | UI, interactive `--teleport`, or GitHub reads by our own convention | Unchanged (partial) | Session output manifest: branch, base, head SHA, PR URL, diff stats |
| Environment identity / revision attestation | S3: "`/remote-env` only sets the default: it doesn't start a session, and it can't add or edit environments." S3: "There's no settings page or direct URL for the selector." S6: an `env_` ID "follows the standard settings precedence". | claude.ai account | UI create/edit/archive; settings key for selection | Slightly narrowed: per-invocation `env_` via `--settings` implied (unverified). No read API | Environment list/get with revision or setup digest; documented per-invocation `env_` selection |
| Configuration materialization | S1: "To change a setting for a cloud session, set an environment variable on the environment, or in a session with one repository, commit the key to that repository's `.claude/settings.json`." | claude.ai account; org Owner for server-managed settings | Git commit or environment UI | Unchanged (partial) | Per-session injection without a commit, plus an effective-config report |
| Subordinate visibility | S8: OTel `agent_id` / `parent_agent_id`; S7 `SubagentStart` / `SubagentStop`. | push | Push-only | Unchanged (partial) | Child-agent listing with status via API |
| Bounded continuation / compaction | S1 / S2: `--teleport` pulls a session into a local, interactive copy; no fork/continue for cloud sessions. | claude.ai account | CLI interactive | Unchanged (partial) | Compaction trigger and fork/continue into a new bounded session via API |
| Automation credential | S5: "There is no long-lived CI token for this today. The scope that grants cloud-session control, `user:sessions:claude_code`, is capped server-side at 30 days, so `claude setup-token` ... doesn't cover it." S5: "Contact your Anthropic account team if you need a machine-identity path that isn't bound to a human account." | human claude.ai account only | n/a | Clarified: negative | Documented scoped non-human credential for create, send and read |

## Non-qualifying surfaces re-checked

| Surface (2026-10-09) | Why it still does not count |
| --- | --- |
| `claude -p ... --environment ccpool_... --output-format json` (S2, S5) | Self-hosted environment. Sessions execute "on infrastructure your organization operates". `env_` IDs are rejected. |
| `POST/DELETE https://api.anthropic.com/v1/code/runners/self-hosted/pools` with `anthropic-beta: ccr-byoc-2025-07-29` (S5) | Self-hosted pool lifecycle only. Docs call these "the same endpoints that the **Cloud environments** admin page on claude.ai uses", with an Owner's rotating OAuth token. There are no session operations. |
| `/v1/environments`, `/v1/environments/{id}/work/{work_id}/stop`, etc. (S13) | Claude Platform beta for Managed Agents (`managed-agents-2026-04-01`). Docs say they are "for orchestrating sessions with self-hosted sandbox environments". Not Claude Code cloud sessions. |
| Compliance API remote sessions (S9) | Still Cowork-only. Claude Code cloud sessions are explicitly excluded. |
| `claude agents --json`, `stop`, `rm` (S11, S12) | Local background sessions only. |
| Cross-session `SendMessage` / listing (S11) | Model-side tool, and only from a session connected to Remote Control. Not a distributable control API. |

## Still-missing vendor operations (Anthropic-hosted Claude Code)

Same set as Oct 8, with item 13 now confirmed negative by the vendor:

1. Create session (repo, ref, `env_` ID, model, permission mode, prompt) non-interactively, returning the session ID, with a client idempotency key.
2. Get and list session status with lifecycle state and the in-VM CLI version.
3. Resumable event stream or paged events with stable IDs and documented retention.
4. Interrupt the active turn and stop/archive the session, with acknowledgement and a settled terminal state.
5. Follow-up returning a message ID, accepting an idempotency key, exposing delivered/applied state and withdraw.
6. Per-session authoritative usage and a per-session hard budget.
7. Session output manifest (branch, base, head SHA, PR URL, diff stats).
8. Environment list/get with a revision or setup digest; documented non-interactive per-invocation `env_` selection.
9. Per-session configuration injection without a git commit, plus an effective-configuration report.
10. Durable pending-approval list/answer (or hosted `defer`) surviving VM pause and reclaim.
11. Subagent listing with parent links and status.
12. Explicit compaction trigger and fork/continue into a new bounded session.
13. A documented scoped non-human credential for 1–12. Today only a human claude.ai login with a 30-day refresh cap exists (S5).

## Drill prerequisites (unchanged; not performed)

A drill would still only record documented-partial behavior; it cannot create items 1–13 and so cannot flip
qualification. The prerequisites in [FEASIBILITY.md](FEASIBILITY.md#what-a-finite-authorized-drill-would-need-not-performed)
stand, with these additions:

- **Credential:** a dedicated claude.ai automation *user* (not a personal login), signed in with `claude auth login` on a scratch config directory. Re-login is required within 30 days (S5). Record that the drill used a human-bound credential.
- **Extra probe:** `claude --cloud "<task>" --settings '{"remote":{"defaultEnvironmentId":"env_..."}}'`, to confirm per-invocation `env_` selection. Also record whether `-p ... --output-format json` is accepted together with a `--cloud` description for an Anthropic-hosted environment.
- **Pin:** local CLI `2.1.296`; record the in-VM version from a prompt.

## Revisit trigger

Re-open MP-16 when Anthropic documents an operation for items 2 and 4 (status and cancel) together with a
non-human credential (item 13). Those three gaps alone block every Mission Control workflow under the
SPEC-01 Stop Fence and the `in_doubt` reconciliation rule.
