---
type: Specification Annex
title: Agent team workspace and execution protocol
description: This is the dependency-oriented repo-local issue packet requested by the owner. No external tracker publication or implementation launch is performed. build_packet.py is the structured authoring source; issues.json is…
tags: [mission-control, spec, expansion]
---
# Agent team workspace and execution protocol

This is the dependency-oriented repo-local issue packet requested by the owner. No external tracker publication or implementation launch is performed. [build_packet.py](build_packet.py) is the structured authoring source; [issues.json](issues.json) is the canonical dispatch snapshot, with stable IDs/contracts/dependencies; [issue views](REQUIREMENTS.md) are generated from the same source. This workspace overrides older instructions to publish specification issues externally for this request only.

## Ready frontier and ownership

Executable first offline issues: [MC-P001](issues/MC-P001.md) contract/source baseline; [MC-P008](issues/MC-P008.md) secure Apps host/fallback prototype; [MC-P009](issues/MC-P009.md) supported storage/topology configuration qualification; [MC-D002](issues/MC-D002.md) public/synthetic dataset manifests. They can progress independently with disjoint file ownership. `MC-P001` unlocks schema/context/control/knowledge foundational proofs. The topological waves printed by `validate_packet.py` show parallel readiness, not a requirement to wait for an entire wave if a dependent proof is accepted earlier.

Teams: kernel/state/control, capability/context/environment, harness/subordinates, knowledge/data/evaluation, client/UI/streams, and integration/release. These are work ownership groups, not permanently running agents. At most four disjoint implementation agents plus one integrator/reviewer by default, reduced to actual available resource slots and profiles. Shared contracts/schema/task registries are integrator-only. No two teams edit one path region concurrently; overlapping repositories alone are allowed when exact paths and imports are disjoint.

Physical runtime destination `Biotech/mission-control` is proposed until the owner authorizes transformation. Run MC-F001 with the current backend source baseline and worktree inventory. No issue assumes a nonexistent destination exists; no automatic directory move/delete is authorized by this planning packet. App frontend paths are explicitly provisional and must be bound by MC-P001/MC-F015 before code edits. Database roles/targets and app-domain endpoints are operator/service-owner inputs, not agent inventions.

## Claim and handoff records

`issue_claim@1`: issue_id, agent/team ID, source branch/worktree/base commit, exact writable path regions, input contract/profile hashes, dependency proof refs, start status, reservation allocation and reviewer ID. Integrator resolves overlaps before work. `issue_handoff@1`: changed files/hashes, output contracts, tests with results/skips, proof artifact refs, unresolved receipts/assumptions, next ready dependencies and review disposition. Save actual implementation evidence in the owning repository only when implementation is authorized.

Proof states: proposed -> claimed -> implementation_review -> proof_pending -> accepted, with blocked/rejected branches. Only an accepted dependency with verified evidence enables another issue. `existing_recorded` library/runtime evidence is input, not the new issue's accepted output. Generated issue default `blocked_by_dependencies` reflects the initial planning state; dynamic scheduling computes readiness from accepted proofs and ownership/resource availability.

Integrator checks highest relevant interface tests, contract/schema parity, dependency availability, app scope, failure/usage uncertainty, unchanged shared baselines and artifact completeness before acceptance. Reviewer is independent of the implementation role for foundational/high-impact issues. Do not merge/push/publish from a handoff unless separately requested. Preserve concurrent/uncommitted edits and report conflicts rather than resetting a checkout.

## Compute and budget discipline

Use C0-C4 profiles in [DEPLOYMENT-AND-RELEASE](DEPLOYMENT-AND-RELEASE.md). Frontier reasoning is assigned to hardest state/context/control/security designs and independent review; routine fixtures/client boilerplate can use smaller qualified models. A model name is selected from authorized installed/provider profiles at execution, not guessed from this packet. Paid runtime calls, external search CLIs and cloud/sandbox provisioning start with zero permitted units until a concrete finite ProofBudget is approved. Team resource reservations count parent/children and unknown usage; concurrency cannot expand authority.

No calendar deadlines or dollar promises. Report progress by accepted issues, unresolved contracts/proof gaps, frontier-ready work, spent/reserved/unknown units, and blockers. A large compute allocation permits more disjoint ready work; it cannot skip foundational proofs or independent review.

## Stop conditions and user decisions

Stop an affected lane on ambiguous external effects, inability to reconcile paid launch, unauthorized scope, schema drift, checkpoint corruption, unsupported storage topology, or missed approval. Freeze new dispatch, persist uncertainty and seek concrete resolution; continue independent offline issues. Retry only recoverable technical failures under the same identity. A completed run needs fork/new authored work, not history repair.

Unresolved owner/operator choices: final mobile framework/build distribution (proposed Expo/React Native default), actual notification providers/channels, selected live provider/model/region/rates and finite budgets, supported Agent Server app-local persistence topology, actual app endpoint/grant/project IDs, and production hosting/credential/backup bindings. First issues can prepare fixtures and recommendations without those inputs. A required live or production gate remains blocked until its explicit authorization/input arrives.

## Validation and integration

Run `Biotech/biotech-research-ingestion-evaluation-system/.venv/Scripts/python.exe general-mission-control/expansion/validate_packet.py` from this workspace. It validates local links, closed JSON Schema definitions and fixtures, issue uniqueness/dependency DAG, coverage completeness, generated views and schema-field consistency. It does not run provider/runtime/deployment tests. Run owner-repository tests named by each issue after implementation. Release gates cannot be checked off by this documentation validator.
