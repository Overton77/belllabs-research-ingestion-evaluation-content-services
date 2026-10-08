# [FT-D1] Chain contracts, tables and compile from a missions list

Linear: OVE-38

**Epic:** Chains (SPEC-04)
**Team:** T3
**Blocked by:** None (can start immediately)
**Status:** ready-for-agent

**What to build:** A manifest with `missions:` and `links:` compiles into N independent Mission Definitions plus a typed `mc.chain.v1` with `mc.chain_link.v1` entries, and `missionctl mission compile chain.yml` prints a Validation Report whose `chain` section lists the topological order and every link; link errors (cycle, unbound supplier output, schema mismatch, missing incoming link) are blockers with JSON pointers. Migration `0028_mission_chains.sql` adds `mission_chain`, `chain_link`, `authoring_provenance` and the new `mission_relationship.kind` values, RLS-forced and included in the release fingerprint. Nothing is released yet; this ticket delivers the shapes, the compile-time validation and the persistence that D2 fills. Because E1 lands in the same wave, coordinate on the manifest schema module: D1 owns `links` validation and the chain contracts, E1 owns everything else in `manifest.py`.

**Spec sections:** SPEC-04 §Contracts, §Implementation Decisions (Compile, Persistence), §Insertion points; SPEC-05 §Contracts (Chain form)

**Writable regions:** `src/mission_control/domain/composition/chain.py`, `src/mission_control/application/chains/service.py`, `src/mission_control/domain/authoring/manifest.py` (links block only), `packages/mission-control-db-contract/component/migrations/0028_mission_chains.sql`, `tests/unit/chains/`, `packages/mission-control-db-contract/tests/`

**Acceptance criteria:**
- [ ] `domain/composition/chain.py` defines `MissionChain`, `ChainLink`, release conditions, cancel policies and `ChainTransition` as strict Pydantic contracts with JSON Schema export under `src/mission_control/contracts/schemas/`.
- [ ] `links` validation produces blockers `chain_cycle`, `unbound_chain_output`, `schema_mismatch`, `missing_incoming_link`, `unknown_mission_key`, each with a pointer; the resolution gains a `chain` section with topological order.
- [ ] A consumer input `from: <mission>.<output>` is accepted only when a `supplies` link binds that output.
- [ ] Migration `0028` applies and replays as a no-op on a disposable PostgreSQL 17; `mission-db verify` passes; fingerprint and generated contract digests are updated in the release manifest.
- [ ] `mission_relationship.kind` accepts `supplies`, `depends_on`, `parent_of`, `adopted_from`, `successor_of`, `forked_from` and still accepts the three existing values.
- [ ] Unit tests cover every blocker, the acyclic rule, the first-mission rule and deterministic digests; `packages/mission-control-db-contract/tests` covers the migration.
- [ ] `make check` passes.

**Verification:** `uv run pytest tests/unit/chains -q`; `uv run pytest packages/mission-control-db-contract/tests -q -k "0028 or fingerprint"` with `MISSION_CONTROL_TEST_ADMIN_DSN` set; `make check`

**Notes:** Do not renumber migrations; `0028` is assigned to T3 (00-ARCHITECTURE §4). The `authoring_provenance` table is specified in SPEC-05 but lives in this migration. Reuse the RLS and grant pattern of `mission_relationship` in `mig/0005`. UNVERIFIED: whether the existing forward-only trigger pattern should apply to `chain_link.state`; follow `asset_version` precedent if a trigger is added.
