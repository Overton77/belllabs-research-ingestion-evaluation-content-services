---
type: Decision Record
title: Skill bundle bytes live in the private Supabase bucket capability-bundles under digest-addressed paths; the catalog row is the authority
description: "Each admitted bundle is uploaded once to capability-bundles at <application>/<kind>/<capability_id>/<version>/<sha256>/..., paths are immutable, the catalog row carries the manifest digest, and workers download through short-lived signed URLs and verify every file before mounting."
tags: [mission-control, adr, decision, capabilities, storage]
status: accepted
source: fast-track interview 2026-10-07 (requirement 1, Supabase bucket); docs/knowledge/capabilities.md; SupabaseCapabilityBundleStore; expansion/CATALOG-AND-ENVIRONMENTS.md; docs/specs/fast-track-2026-10/research/seed-capabilities-and-formats.md
---

# Skill bundle bytes live in the private Supabase bucket capability-bundles under digest-addressed paths; the catalog row is the authority

Supabase Storage has no object versioning, so immutability is achieved by path: a bundle is written exactly once to `capability-bundles/<application>/<kind>/<capability_id>/<version>/<manifest_sha256>/<file path>` and never overwritten. The catalog row stores the manifest digest and the object prefix; custody (upload), registration (catalog row) and materialization (download, verify, mount read-only) stay three separate resumable steps because they cannot share one transaction. Workers fetch through signed URLs minted by the service with a short TTL; agents never receive bucket credentials. We chose a private bucket with `storage.objects` policies over a public bucket because bundles can carry proprietary prompts, and over database blobs because bundles include scripts and assets that lanes mount as directories.

## Consequences

- `mission-db` gains a seed for the bucket and its policies per application; the bucket is an operator-provisioned input, not created by the runtime.
- The existing read-only `SupabaseCapabilityBundleStore` gains an upload path used only by `missionctl catalog publish` and the seed loader.
- A digest mismatch at download is `CAPABILITY_DRIFT`, never a retry with the current object.
