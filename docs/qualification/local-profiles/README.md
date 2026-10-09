# Local profile readiness (MP-22 / OVE-85)

This file separates three states: what is **implemented**, what is **qualified** on real local
services with deterministic cognition, and what is **account-enabled**. No provider call,
login or paid unit was used.

## Readiness of the owner workspace, 2026-10-08

[`readiness-owner-workspace-2026-10-08.json`](readiness-owner-workspace-2026-10-08.json) is the
`mc.local_readiness.v1` report for `deployments/examples/local-run-profile.example.json`, which
selects `deep_agents` and `cursor_local`. It was produced with `--workspace-root` set to the
owner's workspace (`C:\Users\Pinda\Proyectos\Biotech`) on the Windows host. Result: not ready,
with 24 unresolved pointers.

| Finding | Pointer | Owner action |
| --- | --- | --- |
| `PIN_DRIFT` agent-browser Skill: expected `sha256:30722859…`, computed `sha256:f65791f5…` | `infra/capability-pins/research-capabilities.json#/skills/0/bundle_digest` | Move the nested `.agents/skills/agent-browser/agent-browser/` copy (11 files) out of the pinned directory. With only the top-level `SKILL.md` (whose `skill_md_digest` `sha256:328161bf…` still matches), the bundle digest is exactly the pin again, so no re-pin is needed. The alternative is a reviewed re-pin to `sha256:f65791f5…`. |
| `RELEASE_LOCK_DRIFT`: the lock pins `manifest.json` `0853a2c0…`, but the component manifest is now `47bf5f9e…` | `deployments/biotech/release.lock.json#/files/manifest.json` | After MP-01 (0031), re-lock with `mission-db lock --app biotech` once the release is accepted (a release decision). The manifest's fingerprint `mc-pg-catalog-v2:sha256:bdb4c93c…` is reproduced by a scratch build on PostgreSQL 17. |
| `LANE_UNSUPPORTED_OS` `cursor_local` on Windows | `local-run-profile.example.json#/lanes/1/lane_profile` | Run that worker under WSL 2 or Linux (runbook 2.8). |
| `AUTH_CREDENTIAL_ABSENT` `OPENAI_API_KEY` (presence only) | `provider-auth-profiles.example.json#/profiles/0/credential_ref` | Export the key in the worker's environment; never write it to a file. |
| 18 × `BINDING_UNRESOLVED` | `manifest-launch-bindings.deep-agents.example.json#…` | Choose the model name, prompt, context-assembly, backend and tracing refs, the policy refs, the agent profile ref and the workspace provision, then run `preflight compose-bindings`. |
| 2 × `PROFILE_UNRESOLVED` (Temporal Cloud address and namespace) | `local-run-profile.example.json#/temporal_clusters/1/…` | Fill them in, or remove the `cloud` cluster if no cloud cluster is used. |

The installed release fingerprint of the owner database (55432) was not read. It is the
owner's application database, and this ticket may not touch it.

## Status by acceptance box

| Box | Implemented | Qualified (real local services, deterministic cognition) | Account-enabled |
| --- | --- | --- | --- |
| Public start and chain from a real bindings file | `deep_agents` only | yes: `tests/integration/temporal/test_mp22_local_profile_start.py` on disposable PostgreSQL 17 plus Temporal, using the committed example plus FIXTURE selections | no: the owner's selections are not made |
| Preflight refuses a missing profile, an unsupported OS, a wrong DB release or a drifted environment digest | yes | yes: the unit tests, plus `tests/integration/postgres/test_mp22_db_release_preflight.py` (a real install, and a `fixture-unqualified` database) | n/a |
| An outage drill never launches a second copy | yes: the guard and ledger in `bootstrap/preflight.py`, not yet wired into `RunLaunchService` | yes: `tests/integration/temporal/test_mp22_outage_drill.py` with two real namespaces on `127.0.0.1:7233` | n/a |

`cursor_local` is checked here for its host and auth route only. Its launch qualification
belongs to MP-09 and `docs/qualification/lanes/README.md`.
