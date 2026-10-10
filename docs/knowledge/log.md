# Knowledge update log

## 2026-10-07 (fast track)

- Owner fast-track interview: capabilities storage, search and composition; context
  transfer; mission state from provider frames; mission chains; the YAML manifest;
  real interventions and subscriptions; the Cursor lane. Recommendations accepted.
- Added ADR-0023 to ADR-0034 and accepted ADR-0018 (for Cursor), ADR-0019 and
  ADR-0022. Added glossary terms: Mission Chain, Chain Link, Context Packet, Expansion
  Tier, Context Packer, Lane Profile, Provider Frame, Native Event Store, Transcript,
  Subscription, MCP Server, Plugin, Hook Script, Hook Event, Kernel Hook, Subagent
  Profile, Host Projection, Hybrid Search, Cursor.
- Added the implementation packet `docs/specs/fast-track-2026-10/` (architecture, eight
  specifications, three mission manifests, ticket drafts, team workspace) and five
  primary-source research notes under its `research/` directory. The concepts in this
  bundle still describe the pre-fast-track code; ticket FT-I4 reconciles them after
  implementation.

## 2026-10-07

- Expanded the bundle from 8 to 22 concepts. Workflow and authoring:
  [workflow-systems](workflow-systems.md), [parallel-swarm](parallel-swarm.md),
  [evaluator-optimizer](evaluator-optimizer.md), [revisions](revisions.md),
  [durable-controls](durable-controls.md), [completion](completion.md),
  [authoring](authoring.md). Platform: [events-and-commands](events-and-commands.md),
  [interfaces](interfaces.md), [budgets-and-usage](budgets-and-usage.md),
  [lanes-and-harness](lanes-and-harness.md),
  [context-and-continuation](context-and-continuation.md),
  [knowledge-services](knowledge-services.md),
  [release-and-qualification](release-and-qualification.md). Each separates
  implemented behavior (cited code) from specified-only behavior (cited spec).
- Aligned the original concepts with the new shared language: Compiled Program for
  the code name ERC, GoalDirected named as the code family implementing part of the
  Goal Loop, Knowledge Services spelling, and the Temporal placement wording.
- Added `docs/tools/validate_okf.py` (repo-aware links, required title and
  description, Citations heading, index coverage), `okf_search.py`,
  `agents_docs_index.py` and `okf_frontmatter.py`. The compressed index in
  `AGENTS.md` is generated from this bundle, the ADRs, the glossary and the spec pack.
- Recorded 24 spec-versus-code conflicts found while writing, as a local draft for
  the tracker (`.scratch/spec-code-conflicts/spec.md`).

## 2026-10-03

- Created [architecture](architecture.md) and application-logic navigation for the
  general src/mission_control package organization.
- Added [lifecycle](lifecycle.md), [execution](execution.md), [recovery](recovery.md),
  [persistence](persistence.md), [capabilities](capabilities.md),
  [operations](operations.md) and [qualification](qualification.md).
- Recorded the production common-schema release gate separately from local proof.
- Rewrote [persistence](persistence.md) for the common `mission_control` component:
  three namespaces, transaction-local `mc.*` scope with forced RLS, `mission-db`
  installation and the retired transitional schema. Updated
  [qualification](qualification.md) and [operations](operations.md) with the
  two-disposable qualification and the blocked live gates. No live project was
  installed.
- Owner-approved live installation of release `mission_control` 1.0.0 into both Supabase
  projects (Biotech first, then Blue Ocean); both verified with identical fingerprints and
  clean protected-object comparisons. See [qualification](qualification.md) and
  `docs/qualification/two-project/comparison-20261003-live-r1.json`.

## 2026-10-08

Reconciled the bundle with the fast-track packet merged at `f8d325a` (tickets A1 to H1, release
1.1.0, readiness pass). Documentation only; nothing in this entry is live proof, and no live
mission has run (I1 to I3 are owner-run).

- Added [cursor-lane](cursor-lane.md), [mission-chains](mission-chains.md),
  [mission-manifest](mission-manifest.md), [provider-frames-and-transcript](provider-frames-and-transcript.md),
  [interventions](interventions.md), [continuation-checkpoint](continuation-checkpoint.md),
  [deep-agents-lane](deep-agents-lane.md) and [capability-seeds](capability-seeds.md). The last six
  hold detail split out of the updated concepts so each stays under the 160-line validator cap
  (30 concepts now).
- Rewrote [lanes-and-harness](lanes-and-harness.md) (the AgentHarness protocol, registry, describe
  matrices, `lane.turn` segment loop and qualification flag now exist; it had said no harness
  exists) and [capabilities](capabilities.md) (kinds, host support, pins, bundle custody, hybrid
  search, host projection, hooks, subagent profiles, plugins, seeds, skill bundles).
- Rewrote [context-and-continuation](context-and-continuation.md) (Context Packet, tiers,
  stage and iteration handoff, continuation checkpoint and service; recorded the limitation that
  the service and its activities are neither composed nor registered) and
  [events-and-commands](events-and-commands.md) (provider frames, reducer derivation, transcript,
  run search, SSE and subscriptions, the five-state receipt vocabulary, mailbox,
  interrupt_and_inject, immediate cancel with a persisted Stop Fence).
- Extended [interfaces](interfaces.md), [authoring](authoring.md) (Mission Manifest v1; blocker
  B1), [recovery](recovery.md) (fork with instruction and Cursor Local snapshot),
  [persistence](persistence.md) (release 1.1.0, migrations 0025 to 0030, byte-exact 0002 and 0004,
  approved-assets 1.0.1), [release-and-qualification](release-and-qualification.md),
  [qualification](qualification.md) and [operations](operations.md).
- Corrected stale statements: "nothing serves SSE" (now `GET /missions/{id}/events`),
  "four receipt states" (now the five-state vocabulary), "`queue_instruction` and
  `interrupt_and_inject` are rejected" and "no persisted stop fence" (both built), and the
  catalog skill manifest version (0.3.0).
- Spec versus code, reported not resolved: SPEC-07 names the Cursor kernel hook script
  `.mission/bin/mc_hook.py`, the code writes `.mission/hooks/kernel.py`; the manifests subscribe to
  `activation.completed`, `human_task.opened` and `run.completed`, which no writer emits (owner
  runbook B7); ADR-0021 still says `status: proposed` although the OKF bundle and the generated
  index exist (left for the owner; ADR bodies are not rewritten).
- Not done here (belongs after the owner's mission runs): the three mission acceptance results for
  I1 to I3, flipping lane qualification, and applying release 1.1.0 to the live projects.

## 2026-10-09 (multi-provider packet, MP-23 pass 1)

Reconciled the bundle with the multi-provider packet's integrated waves 0 to 2 and the
integrator's 2026-10-09 wiring (uncommitted on `7c9b755`). Wave 3 (MP-07, MP-08, MP-09, MP-11,
MP-12, MP-15) is in flight and not integrated; pass 2 updates these concepts after it lands.
Documentation only: nothing here is live proof, and no `qualified` flag or ADR status changed.

- Added [human-gates](human-gates.md) (the `mc.human_gate.v1` control activation, the one
  `HumanTaskService`, HTTP, MCP and socket resolution, the `mp10-*` patches),
  [mission-stream](mission-stream.md) (the `/missions` Socket.IO namespace,
  `bootstrap.realtime:create_asgi_app`, hints, error codes) and
  [session-ownership-and-dispatch](session-ownership-and-dispatch.md) (fenced ownership, dispatch
  journal, Stop Fence admission of dispatches, optional lane protocols, capacity waits, auth
  routes). 33 concepts now; the existing concepts were at the 160-line cap.
- Updated [durable-controls](durable-controls.md), [interfaces](interfaces.md),
  [lanes-and-harness](lanes-and-harness.md) (seven profiles, v2 stubs),
  [operations](operations.md) (worker readiness gate, preflight CLI, cluster binding, Temporal
  fallback), [persistence](persistence.md) (0031 and 0032, fingerprint `sha256:672549cd...`,
  locks still on the committed 1.1.0 manifest), [events-and-commands](events-and-commands.md)
  (public aliases close B7 in code), [interventions](interventions.md),
  [cursor-lane](cursor-lane.md), [authoring](authoring.md), [mission-chains](mission-chains.md),
  [qualification](qualification.md) and [release-and-qualification](release-and-qualification.md)
  (per-profile status). Published the release statement
  `docs/qualification/release/multi-provider-2026-10.md`.
- Spec versus code, reported not resolved: the production launch (`prepare_bound`) does not lower
  manifest Human Gates into the run inputs; `register_human_task_tools` has no served MCP
  caller; the manifest `HumanTaskSpec` cannot express remediation or a default answer;
  `compose_auth_admission`, `provider_child_environment` and `capacity_policy` have no
  production caller, so `MISSION_CONTROL_CAPACITY_*` does not change a wait; the operator guide
  said the release locks pin 1.0.0 (they pin the committed 1.1.0 build; corrected);
  `docs/qualification/local-profiles/README.md` still says the cluster guard is not wired into
  `RunLaunchService` and cites the pre-0032 manifest digest (owned by MP-22, not edited).

## 2026-10-10 (multi-provider packet, MP-23 pass 2)

Reconciled the bundle with the 2026-10-09 recovery integration (MP-07, MP-08, MP-09, MP-11 and
MP-12 ported; MP-15 and MP-20 finished; uncommitted on the local branch
`mp/integration-recovery-2026-10-09`). Documentation only: no profile is live-qualified, no
`qualified` flag or ADR status changed, and the hosted profiles stay Outcome 3.

- Added [provider-lanes](provider-lanes.md) (the `claude_agent_sdk` and `codex` lanes: pinned
  transports, opt-in Linux/WSL composition, broker-bound approvals, continuation, Codex native
  compaction, owner-run drills). 34 concepts now.
- Updated [lanes-and-harness](lanes-and-harness.md) (implemented v2 describes and the cell
  convention, Session Lane routing on the binding's queue, lane task queues, attempt recording,
  output custody), [cursor-lane](cursor-lane.md) (MP-09: v2 describes, `reconcile_dispatch`,
  allocator-backed cloud lease and branch snapshots, run-branch publisher, host gate, Cursor launch
  author), [human-gates](human-gates.md) (production lowering with the default `owner` Goal Loop
  reviewer, denied gate fails the run, MP-11 approval-origin tasks, broker, governed effects,
  approval resolution transports), [durable-controls](durable-controls.md),
  [continuation-checkpoint](continuation-checkpoint.md) and
  [context-and-continuation](context-and-continuation.md) (MP-12 production composition,
  ADR-0041), [session-ownership-and-dispatch](session-ownership-and-dispatch.md) (attempt
  recording, lane queues, which lanes reconcile and steer, the deployment's capacity policy),
  [budgets-and-usage](budgets-and-usage.md) (Session Lane usage charging, capacity separate from
  budget), [authoring](authoring.md) and [mission-chains](mission-chains.md) (v2 and Cursor
  launch, Mission 2 status), [qualification](qualification.md) and
  [release-and-qualification](release-and-qualification.md). Published release statement pass 2
  and `docs/specs/multi-provider-2026-10/HANDOFF-2026-10-10.md`.
- Corrected stale statements: "no harness exists for the four v2 profiles", "no production lane
  implements `DispatchReconcilingLane` or `SteeringLane`", "`ContinuationService` is not composed",
  "the production launch does not lower manifest gates", "`register_human_task_tools` has no served
  caller", "`MISSION_CONTROL_CAPACITY_*` does not change a wait" and "the production launch author
  binds `deep_agents` only".
- Reported, not resolved here: the Cursor lane qualification READMEs still describe the v2
  describe as a proposal; the MP-20 parity record lists gaps closed after it was written; the
  remaining code follow-ups are listed in the handoff.
