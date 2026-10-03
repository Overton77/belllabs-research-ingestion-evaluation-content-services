# Capability directory bundle implementation

Implementation authority: approved Mission Control specification and expansion
`CATALOG-AND-ENVIRONMENTS.md` (complete `/skills/` bundles; immutable assets in
`capability-bundles`; path/digest/quota validation). Existing deployment v1 text pins
remain readable. New byte bundles explicitly declare `digest_format=bytes_v1`.

## Implemented

- `app/integrations/capability_bundles.py`: complete directory ingestion (hidden files
  included), raw SHA-256 bytes, canonical file and asset/version manifests, per-file,
  total-byte and count quotas; rejects traversal, ambiguous separators, Windows ADS
  and device names, symlinks/junctions, hard links and case-fold/file-directory collisions.
- `DirectoryBundleStore`: atomic no-overwrite content objects, manifests and version
  bindings. Retry of identical content succeeds; changed content at the same version
  fails. A partial write cannot publish a version; unreferenced objects are retained.
  Root must be service-owned and inaccessible to sandbox writers.
- `SupabaseCapabilityBundleStore`: installed `storage3` SDK upload/download adapter,
  fixed `capability-bundles` bucket, server-selected application namespace, no upsert,
  delete, bucket creation or policy mutation. Successful upload and duplicate retries
  verify actual remote bytes. Exact manifest retrieval verifies every file.
- `PinnedSkill.bundle(store=...)`: exact manifest locator, asset/version check and
  runtime bundle verification; defaults to local `.runtime/capability-bundles`.
- Deep Agent materialization validates all file paths and duplicate mount destinations.
  Byte-capable sandbox backends receive original bytes. Text-only StateBackend rejects
  binary assets explicitly; no omission or lossy conversion occurs.
- `scripts/publish_skill_bundle.py`: reviewed directory to offline store and exact pin
  JSON output. Does not execute scripts, amend deployment pins, or contact providers.

Example (a reviewed definition digest must replace the placeholder):

```powershell
.venv/Scripts/python.exe scripts/publish_skill_bundle.py PATH_TO_DIRECTORY `
  --asset-id skill.example --version 1 --skill-name example `
  --definition-digest sha256:REVIEWED_DEFINITION_DIGEST
```

## Evidence and remaining gates

Offline tests exercise complete binary directory round trips, immutable-version
conflicts, corruption, unsafe paths, links and collisions; actual installed Supabase
SDK requests run through an HTTP transport fixture to verify multipart/no-upsert,
duplicate retry and authorization failure behavior. These are SDK contract tests,
not evidence of a live bucket or database. Existing WP-CP-040 and capability-pin tests
remain passing. Tests run with provider tracing disabled.

No live writes, migrations, bucket changes, paid experiments or catalog publications
were performed. The Supabase adapter stages custody bytes; unique catalog asset/version
admission still requires the approved PostgreSQL catalog release and transaction.
Remote reader composition and app-scoped bucket/RLS credential qualification remain
deployment gates. SDK download returns bytes in memory before the adapter can validate
length; deployment must enforce object/bucket response size limits. Local byte quotas
are checked while reading.

Read-only file mode is manifest intent, not proof of OS-level enforcement. Existing
executable sandbox backends require separately qualified host restrictions for shell
writes and symlink replacement. This change does not represent SDK filesystem rules
as an OS security boundary. Directory ingestion requires a stable service-controlled
source while being captured; concurrent hostile filesystem mutation is outside this
offline adapter's custody guarantee.
