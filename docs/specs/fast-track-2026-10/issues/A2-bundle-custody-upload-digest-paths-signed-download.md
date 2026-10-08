# [FT-A2] Bundle custody: upload, digest paths, signed download, bucket seed

Linear: OVE-23

**Epic:** Capabilities (SPEC-01)
**Team:** T1
**Blocked by:** FT-A1
**Status:** ready-for-agent

**What to build:** An operator runs `missionctl catalog publish --dir ./my-skill --kind skill_bundle` and the directory is hashed (manifest digest and `computed_hash`), uploaded once to `capability-bundles/<app>/<kind>/<id>/<version>/<manifest_sha256>/...`, registered as a `proposed` catalog row with its object prefix, and the command prints the Capability Pin. Publishing the same bytes again succeeds idempotently; publishing different bytes under the same path is impossible because no writer can overwrite. A worker materializing that pin asks the service for signed URLs, downloads, verifies every file against the manifest and mounts the directory read-only, and a tampered object is refused with `CAPABILITY_DRIFT`. The bucket and its policies come from a `mission-db` seed.

**Spec sections:** SPEC-01 "Custody in Supabase Storage", "Interfaces" (`catalog publish`, `/publish:prepare`, `/publish:complete`), "Persistence" (bucket seed in 0026 bundle), "Testing Decisions" (custody).

**Writable regions:** `src/mission_control/adapters/supabase_storage/bundles.py`, `src/mission_control/adapters/capabilities/capability_bundles.py`, `src/mission_control/adapters/postgres/capability_bundles.py`, `src/mission_control/application/capabilities/` (publish service), `src/mission_control/interfaces/http/catalog.py` (publish routes), `packages/mission-control-db-contract/seeds/common/mc.storage.capability-bundles-1.0.0.json`; shared: `src/mission_control/interfaces/cli/main.py` (`catalog publish`).

**Acceptance criteria:**
- [ ] `SupabaseCapabilityBundleStore.upload_bundle(manifest, files)` writes each file to the digest path without `upsert`, uses resumable TUS (6 MiB chunks) above 6 MiB, treats `400 Asset Already Exists` as success only after digest comparison, and never sends a service key from the runtime.
- [ ] `SupabaseCapabilityBundleStore.signed_urls(manifest, ttl=300)` returns one short-lived URL per file; the worker download path verifies `sha256` and byte count per file, applies the existing traversal, symlink and duplicate-path rejections, and raises `CAPABILITY_DRIFT` on mismatch (test with a tampered fixture).
- [ ] `computed_hash` is computed with the Vercel `skills` CLI algorithm and equals the value in the repository's own `skills-lock.json` for an existing installed skill (fixture test).
- [ ] `missionctl catalog publish --dir --kind [--capability-id] [--version]` and HTTP `POST /catalog/publish:prepare` and `POST /catalog/publish:complete` exist, register a `proposed` row with object prefix and manifest digest, and print or return the pin.
- [ ] Seed `mc.storage.capability-bundles-1.0.0` creates the private bucket (50 MiB limit) and exactly four `storage.objects` policies (INSERT publisher prefix-scoped, SELECT reader prefix-scoped, no UPDATE, no DELETE); `mission-db seed-apply` records a receipt and `mission-db qualify` verifies; on a cluster without the Storage schema the seed records `blocked`, not failure.
- [ ] Custody, registration and materialization are three resumable steps; a test kills the publisher between upload and registration and the re-run completes without re-uploading.
- [ ] Hook script directories publish through the same path with `--kind hook_script`.

**Verification:** `make check`; `uv run --group biotech pytest tests/unit/capability/test_bundle_custody.py -q`; `uv run --group biotech pytest -m common_db tests/integration/postgres/test_capability_bundles_publish.py -q`; custody against a local Supabase (`supabase start`) or the recorded storage fake: `uv run --group biotech pytest tests/integration/storage -q`; `uv run mission-db seed-apply --bundle mc.storage.capability-bundles-1.0.0 --target <disposable>`.

**Notes:** Supabase Storage has no versioning and deleted objects are unrecoverable, so immutability is path plus policy; never add an UPDATE or DELETE policy. Signed upload URLs last 2 hours, resumable upload URLs 24 hours, download signed URLs are minted with a 300 s TTL. The live buckets are operator-provisioned inputs; do not create them from the runtime or from tests against the live projects.
