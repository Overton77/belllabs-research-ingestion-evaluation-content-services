# WP-CP-010 implementation evidence

Disposition: `accepted`  
Recorded: 2026-08-10  
Contracts: `CON-CP-DEFINITION-REF-V1`, `CON-CP-ERC-V1`  
Schema versions: definition ref `1`, ERC `1`, canonical JSON `canonical-json/1`  
Compiler: `control-plane-definitions/1`

## Requirement-to-test/evidence map

| Requirement | Executable evidence |
|---|---|
| REQ-CP-DEF-001 | `tests/test_control_plane.py::test_authoring_head_uses_optimistic_revisions_and_publishes_exact_draft`, `::test_alias_movement_preserves_snapshot_and_retirement_is_readable`, `tests/test_control_plane_mongodb_integration.py::test_real_mongodb_published_revision_is_immutable_and_readable` |
| REQ-CP-DEF-002 | Workflow configuration ownership tests in `tests/test_control_plane.py`; profile topology collision plus capability, budget-amount, and concurrency authority-expansion rejection in `tests/acceptance/control_plane/test_wp_cp_010.py` |
| REQ-CP-DEF-003 | strict `WorkflowBlueprint` discriminator and `CompileInvocation` selection validation in `app/domain/control_plane/contracts.py`; GoalDirected fixture in `tests/test_control_plane.py::test_publish_compile_retrieve_is_deterministic_and_intersects_ceilings`; StageGraph fixtures in `tests/test_schema_grounding_control_plane.py` |
| REQ-CP-DEF-004 | `tests/test_control_plane.py::test_publish_compile_retrieve_is_deterministic_and_intersects_ceilings`, canonical-byte tests, and compiler drift guards proving no database/network/clock/environment or mutable extension-registry dependency |
| REQ-CP-DEF-005 | `tests/acceptance/control_plane/test_wp_cp_010.py::test_exact_deep_agent_composition_and_all_capability_families_compile`; strict generated ERC schema in `tests/test_control_plane.py::test_generated_schema_rejects_the_same_unknown_definition_field` |
| REQ-CP-DEF-006 | `tests/acceptance/control_plane/test_wp_cp_010.py::test_exact_deep_agent_composition_and_all_capability_families_compile`, `::test_profile_composition_collisions_fail_deterministically` |
| REQ-CP-DEF-007 | `tests/test_control_plane.py::test_overlay_cannot_escalate_or_select_undeclared_variant`, `::test_capability_overlay_cannot_remove_a_required_capability`; accepted/omitted/degraded assertions in `tests/acceptance/control_plane/test_wp_cp_010.py` |
| REQ-CP-DEF-008 | compiler-level exact MCP, Skill, sandbox, model, middleware and tool selection; strict definition-kind/capability-family validation at construction, publication, and selection; required failure; published maturity/compiler-compatibility/target/conflict governance; omission/degradation; and similarly-named non-substitution in `tests/acceptance/control_plane/test_wp_cp_010.py` |
| REQ-CP-DEF-009 | `tests/test_linked_runs.py` independent-child and frozen-parent-ceiling cases; parent authority intersection in `tests/test_control_plane.py::test_publish_compile_retrieve_is_deterministic_and_intersects_ceilings` |
| REQ-CP-DEF-010 | `tests/acceptance/control_plane/test_wp_cp_010.py::test_definition_ref_contract_and_secret_value_rejection`, `tests/test_control_plane.py::test_extension_payload_cannot_embed_secret_values`, sanitized payload tests in `tests/test_control_plane_payloads.py` |

## Changed implementation and persistence paths

- `app/domain/control_plane/contracts.py` — strict reference/ERC fields, lifecycle and immutable payload identity, exact capability families, profile composition, placements, flattened bindings and attachment decisions.
- `app/domain/control_plane/canonical.py` — explicit UTF-8 encoding, string-only JSON object keys, sorted compact Unicode-preserving serialization and finite-number rejection.
- `app/domain/control_plane/compiler.py` — sole pure compiler; deterministic composition, collision detection, authority intersection and exact capability selection.
- `app/application/control_plane.py` — publication validation, exact dependency resolution before compilation and immutable ERC persistence.
- `app/application/control_plane_repository.py` — in-memory and Beanie repository handling for contract/schema/lifecycle/payload identity.
- `app/application/catalog_projection.py` — exact semantic-identity comparison tolerates the canonical published-to-retired lifecycle transition while retaining digest checks.
- `app/models/control_plane.py` — canonical Beanie document contract IDs and schema fields.
- `tests/acceptance/control_plane/test_wp_cp_010.py` — qualification suite.
- `tests/test_pre_stage3_temporal_contracts.py` — frozen schema digest updated for the canonical definition-ref shape.
- `langgraph.block_c.json`, `langgraph.block_c_n1.json`, `langgraph.block_c.env` — restored tracked qualification configuration/variable-reference assets required by the repository suite; the env file contains references only, no values.

No PostgreSQL migration is applicable: MongoDB owns these documents. BellLabs is pre-production and the readiness contract explicitly authorizes direct recreation of the canonical collections without a compatibility reader, dual write, backfill, or prototype-data migration. Existing collection names and unique indexes remain; the Beanie document schema is replaced in place.

## Representative canonical bytes and digest

Sanitized input: `{"z":[2,1],"á":"寿命"}`

```text
{"canonical_schema_version":"canonical-json/1","payload":{"z":[2,1],"á":"寿命"}}
sha256:6b59ac38bb17be7b5ff15dc9ef979f389e2da5e0bf13eb09f95844bce8e722dd
```

Arrays retain authored order. Object keys are lexicographically sorted. Unicode is not escaped.

## Repository, migration and generated-schema rehearsal

- In-memory publication/compile/retrieve/retire and immutable ERC rehearsal: passed.
- Beanie/Mongo integration test is present and included in the suite; it skips when `MONGODB_URL` is not supplied (no credential was invented).
- The real-Mongo test explicitly attempts a conflicting mutation/republish and requires `DefinitionConflict`, then retires and reloads the immutable published document.
- Generated Pydantic/JSON Schema rejects unknown fields and exposes definition-ref schema/lifecycle/payload identity and complete ERC binding/attachment fields.
- Externalized ERC retrieval and content-address tamper detection: passed.
- Direct-recreation migration posture: no migration/backfill artifact by design; recreate local pre-production collections before downstream admission is enabled.

## Commands and sanitized results

```text
uv run ruff check app tests
All checks passed!

uv run mypy app
Success: no issues found in 316 source files

uv run pytest -q --tb=short
559 passed, 32 skipped, 11 warnings in 38.88s

Focused control-plane/repository/payload/linked-run/schema qualification
25 passed, 1 skipped (latest control-plane acceptance/service/Mongo slice)

git diff --check
exit 0; line-ending conversion warnings only, no whitespace errors
```

The skips are credential/service-gated integration and persistent-runtime tests. No failure was hidden.

## Replacement and deletion inventory

| Responsibility | Disposition |
|---|---|
| String-only capability availability in ERC compilation | Replaced in the sole compiler by exact revision attachments; retained strings serve only existing coarse authority intersection. |
| Runtime profile inheritance | Replaced by deterministic flattened `FlattenedDeepAgentBinding` records; runtime inheritance is not emitted. |
| Mutable alias/default lookup inside pure compilation | Absent; application resolution produces exact refs before the pure compiler is called. |
| Separate legacy definition/ERC compiler | None existed; the authorized modules were replaced in place, so no duplicate path remains to delete. |
| OpenAI Agents SDK runtime paths | Not a WP-CP-010 deletion gate; drift is recorded below and removal remains owned by WP-CP-040 acceptance. |

## Drift searches

- `app/domain/control_plane/compiler.py` AST guard: no database, network, clock, environment or secret access imports/calls.
- Direct StageGraph/GoalDirected `start_workflow` search: no matches in the control-plane slice.
- OpenAI Agents SDK search: active matches remain in Temporal/integration/experiment paths. They are outside WP-CP-010 and have the explicit WP-CP-040 deletion gate in `IMPLEMENTATION_READINESS.md`.
- No `v2`, `new`, compatibility, dual-read, dual-write, fallback or backfill control-plane implementation was introduced.

## Rollback posture and risks

Pre-production rollback is repository revert plus recreation of local canonical Mongo collections before any downstream admission package is enabled. It does not re-enable an alternate compiler.

Remaining review risks:

- Real Mongo transaction/index rehearsal requires an independently supplied disposable `MONGODB_URL`; the checked-in integration test is ready and was not given fake credentials.
- OpenAI Agents SDK runtime removal is intentionally deferred to WP-CP-040's accepted replacement gate.

## Independent review

The independent reviewer returned `ACCEPT` after two review/remediation cycles. The final pass
verified fail-closed capability, budget-amount, and concurrency authority checks; strict capability
family identity at construction, publication, and compiler selection; compiler purity; evidence
accuracy; and the recorded service-gated Mongo skip. No acceptance blocker remains.

## Final disposition

All WP-CP-010-owned requirements and required repository checks pass. Independent review accepted
the package. `WP-CP-020` is now unblocked as the next implementation frontier.
