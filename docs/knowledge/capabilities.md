---
type: Workflow
title: Immutable directory capabilities
description: How downloadable skills are pinned and safely materialized.
tags: [mission-control, implementation]
---

# Immutable directory capabilities

The operational catalog is available through the same authenticated application
binding as runs. `missionctl catalog list`, `resolve`, `search`, `discover`,
`inspect` and `components` call shared application services. Under
`/v1/applications/{application_id}/catalog`, definitions use GET; resolve, search,
discover, inspect and components/search use POST. Missing configured discovery or
inspection services fail explicitly; a catalog request cannot select another scope.

Search produces candidates, not authority. External discovery is quarantined;
inspection evidence precedes any admitted version. Operational capability search
is separate from application entity/schema catalogs.

A skill is a downloadable directory bundle, not one prompt file. Its canonical
manifest identifies all admitted files, immutable version and content digests.
The runtime resolves the exact admitted version; current registry state cannot
silently replace content already pinned to a run.

Custody, registration and materialization are separate steps. Downloaded bytes
must match the admitted manifest. Reject traversal, unsafe links, colliding paths
and inconsistent content before exposing a directory to the harness. Remote
object storage and database registration need resumable protocols because they
cannot commit in one shared transaction.

Capabilities, models and tools are authorized bindings rather than executable
instructions from an arbitrary document. Bundle text cannot choose privileged
bootstrap modules, credentials, application scope or additional authority.

Text-only backends explicitly reject unsupported binary content. Secure pathname
validation does not prove an operating-system read-only mount; executable mount
and live private bucket policy qualification remain separate gates.

# Citations

- [Catalog application service](../../src/mission_control/application/capabilities/catalog.py).
- [Catalog HTTP facade](../../src/mission_control/interfaces/http/catalog.py).
- [Bundle custody](../../src/mission_control/adapters/capabilities/capability_bundles.py).
- [Capability pins](../../src/mission_control/adapters/capabilities/capability_pins.py).
- [PostgreSQL bundle admission](../../src/mission_control/adapters/postgres/capability_bundles.py).
- Canonical runtime skill: `skills/mission-control/`, manifest version 0.1.1.
- Coordinator design/launch helper: `.agents/skills/mission-control-coordinator/`;
  distinct authoring workflow, not a second runtime authority.
