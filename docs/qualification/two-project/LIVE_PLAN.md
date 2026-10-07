# G4 concrete live installation plan (awaiting owner approval)

Status: **approved by the project owner and executed on 2026-10-03** (Biotech first,
then Blue Ocean; both passed every gate). Evidence:
[Biotech](biotech-research-ingestion/20261003-live-r1/),
[Blue Ocean](supabase-blue-ocean/20261003-live-r1/),
[comparison](comparison-20261003-live-r1.json). Biotech plan digest
`sha256:eb81eed0cd569781106fc98860ed8b7ed68175c8a3f1f5abd67fb747bbac3464`, Blue Ocean
`sha256:8bd2a634fa80fa558ae027908b55340b08563c20764096151704a835b40f8dfe`. Deviations
from the text below: the AI Engineer route is the Supabase session pooler (approved
existing `POSTGRES_URL_NON_POOLING` credential, with `database_session_user = "postgres"`);
the Biotech route is the existing `DATABASE_DIRECT` credential; no recovery point was
verified before apply (owner approved proceeding; the change is additive-only);
`mc.app.bindings@1.0.1` was applied afterwards to record the approved target facts.
Items in section 6.5 remain unapproved and undone. Prepared
2026-10-03 from release `mission_control` 1.0.0 after the two-disposable proof
([comparison](comparison-20261003-g3-r2.json)) and the local real-stack parity run
([persistence trace](local-real-stack/20261003-g3/persistence-trace.json)).

## 1. Exact release and pins

| Item | Value |
| --- | --- |
| Component / version / protocol | `mission_control` 1.0.0, `mission-control-sql/v2`, fingerprint `mc-pg-catalog-v2` |
| Manifest | `packages/mission-control-db-contract/component/manifest.json` sha256 `946bc371a7837ef5f52af8c8a7de4eb85aa8b801dd7cb61dab8512f709498a3e` |
| Schema fingerprint (both owned schemas) | `sha256:ef5a9e71c9c4a4fe23f343a5708c476a757bc1c5a61f9aacae3a76577b209c09` (`mission_control` `sha256:46160921…`, `mission_control_search` `sha256:1df2cc3c…`) |
| Generated contract | `sha256:dbce50116df01e9a8d814f78e9ff61d91943afdfcc5fcb1a1d2d2001e488ab7f` |
| Ordered migrations | 18 (0001-0005 canonical, 0010-0017 runtime support, 0020-0024 catalog/search/artifacts); digests in the locks |
| Biotech lock | `deployments/biotech/release.lock.json` sha256 `1a473ba8a0b73310481dd040da3659e43042365803b2f2d082802c97bcced9be` |
| AI Engineer lock | `deployments/ai-engineer/release.lock.json` sha256 `081895bf7589e41535d83a885e3f242893df5f4d79b2b984cca18eed5ca5b2f8` |
| Runtime descriptor | `packages/mission-control-db-contract/runtime/descriptor.json` sha256 `b0330c546284548791dffae3c19452952db008fe5b32d36e8cc1a63488b93cfd` (langgraph-checkpoint-postgres 3.1.1; saver v0-v9, store v0-v3) |
| Common seeds (both apps, equal bytes) | `mc.catalog.workflow-parity@1.0.0` `sha256:ca92dd4f…`, `mc.catalog.runtime-profiles@1.0.0` `sha256:64dbe58e…`, `mc.catalog.approved-assets@1.0.0` `sha256:f45e04f9…` |
| App seeds | Biotech `mc.app.bindings@1.0.0` `sha256:670a3e64…`; AI Engineer `mc.app.bindings@1.0.0` `sha256:e5d58936…` |
| Not proposed for live | `mc.qualification.parity` (opt-in qualification tenant), any tenant/actor grant bundle (no owner-approved mappings exist) |

## 2. Targets (order: Biotech first; AI Engineer only after Biotech passes)

| | Biotech | AI Engineer |
| --- | --- | --- |
| Label / app | `biotech-research-ingestion` / `biotech` | `supabase-blue-ocean` / `ai-engineer` |
| Project ref (management metadata) | `bxnetwiimwhtlrjlbtab` | `wkythqbofmckbuoothhn` |
| Direct route | `db.bxnetwiimwhtlrjlbtab.supabase.co:5432/postgres` | `db.wkythqbofmckbuoothhn.supabase.co:5432/postgres` |
| PostgreSQL | 17.6 (`server_version_num` 170006) | 17.6 (170006) |
| `extensions.vector` | 0.8.2 present | 0.8.0 present |
| Installation id (allocated once) | `01a103c0-9a9e-7a95-961b-29dda83c842b` | `01a103c0-9a9e-70f3-9398-eb1573a98324` |
| Credential reference (name only) | `MC_BIOTECH_MIGRATION_DATABASE_URL` (proposed; a direct `postgres` DSN for this host exists locally as `DATABASE_DIRECT` but is **not approved** for migration use) | `MC_AI_ENGINEER_MIGRATION_DATABASE_URL` (no local credential exists) |
| Identity evidence | [G0 inventory](biotech-research-ingestion/g0-readonly-20261003/identity.json) | [G0 inventory](supabase-blue-ocean/g0-readonly-20261003/identity.json) |

`deployments/<app>/target.toml` deliberately holds `REPLACE_*` values for environment,
migration login and approval/evidence references; `mission-db` refuses the target until
the owner fills them.

## 3. Exact intended mutations (per target)

- Create schemas `mission_control`, `mission_control_search` (REVOKE ALL FROM PUBLIC).
- Create NOLOGIN roles `mission_control_runtime`, `_family_writer`, `_catalog_writer`,
  `_outbox_worker`, `_readonly` (no LOGIN, SUPERUSER, BYPASSRLS, CREATEROLE, CREATEDB,
  no membership in any role). PostgreSQL 16+ gives the creating `postgres` login an
  ADMIN-only membership (no INHERIT/SET) — an expected, allowed protected difference.
- 18 migrations + `component_release` receipts + one `application_installation` row +
  one `release_attestation` row, in **one transaction** under the migration advisory lock.
- Runtime phase (separate, after verify): schema `mission_control_runtime`, NOLOGIN role
  `mission_control_checkpointer`, the 14 pinned vendor steps (four `CREATE INDEX
  CONCURRENTLY` steps run outside a transaction; a version row is recorded only after the
  index is valid).
- Seeds (separate, after verify): the five bundles in section 1 (three common + one app
  bindings per app), one transaction per bundle.
- **Not included:** any change to existing schemas, tables, functions, policies, roles,
  default privileges, Auth users or extensions; no grant to `anon`, `authenticated`,
  `service_role`, `authenticator`; no PostgREST schema exposure; no storage buckets or
  `storage.objects` policies (a separate storage plan is required); no LOGIN role or
  membership for the runtime (access expansion is a separate approval); Biotech's legacy
  `capability_search` schema is left untouched (not dropped, not migrated, not read by
  the runtime).

Read-only pre-checks already observed for both projects: no `mission_control*` schema or
`mission_*`/`belllabs_*` role exists; default privileges of `postgres` are scoped to
`public`/`storage`/`api` (none global), so the new schemas receive no automatic grants;
`postgres` keeps `search_path="$user", public, extensions` and all component SQL is
schema-qualified.

## 4. Procedure per target (each command exits 0 or holds)

```powershell
# 0. Owner fills deployments/<app>/target.toml REPLACE_* fields and sets the approved secret.
$app = "biotech"   # then "ai-engineer" only after biotech passes step 9
$out = "docs/qualification/two-project/<label>/<run-id>"
$v = "--reader-version mission-control-runtime/1 --writer-version mission-control-runtime/1"
# 1. Read-only: identity + plan + protected snapshot in a quiescent/attributable window
uv run --group dbcontract mission-db qualify --deployment-dir deployments/$app $v --phase before --out-dir $out
# 2. Present plan.json (plan_digest), protected-before.json and this file for approval.
# 3. Apply (exact confirmation + reviewed plan digest; stale plans reject)
uv run --group dbcontract mission-db apply --deployment-dir deployments/$app $v --confirm-target <ref>:<installation-uuid> --expected-plan-digest <plan_digest>
uv run --group dbcontract mission-db verify --deployment-dir deployments/$app $v
# 4. Runtime phase
uv run --group dbcontract mission-db runtime-plan --deployment-dir deployments/$app $v --descriptor packages/mission-control-db-contract/runtime/descriptor.json
uv run --group dbcontract mission-db runtime-apply --deployment-dir deployments/$app $v --descriptor packages/mission-control-db-contract/runtime/descriptor.json --confirm-target <ref>:<uuid> --receipt-out $out/runtime-receipts.json
# 5. Seeds (common + this app only)
uv run --group dbcontract mission-db seed-apply --deployment-dir deployments/$app $v --bundle packages/mission-control-db-contract/seeds/common --bundle packages/mission-control-db-contract/seeds/$app --confirm-target <ref>:<uuid>
# 6. After evidence + protected comparison against the approved allowed differences
uv run --group dbcontract mission-db qualify --deployment-dir deployments/$app $v --phase after --out-dir $out --allowed <allowed-differences.json>
```

Allowed differences = exactly the six capability/checkpointer roles and the six ADMIN-only
creator memberships (the same rules used by `scripts/qualify_two_disposables.py`).
Anything else in a protected object holds the target and the second promotion.

## 5. Recovery

Transactional phases roll back completely on failure (proven on disposables). A lost
acknowledgement is resolved by read-only `verify`/`runtime-plan`/`seed-plan`, never by a
blind retry or reset. No DROP-based rollback is offered; correction is a forward release.
**A tested recovery point is required before step 3** (Supabase PITR or an approved
export with an approved scope/destination); none has been confirmed for either project.
If Biotech passes and AI Engineer fails, Biotech stays installed and the AI Engineer
binding stays disabled.

## 6. Approvals required before any live step (none granted yet)

1. Operator-verified mapping: environment, migration login, secret reference names and
   approver/evidence references for each target (fills `target.toml`).
2. Recovery point and restore procedure per project.
3. A quiescent or attributable window for protected-data comparison (both apps have live writers).
4. This exact plan (digests above) and each target's `plan_digest` from step 1.
5. Separately, later: runtime login roles and memberships (access expansion), storage
   buckets/policies, Agent Server topology and production license.
