"""Rebuild generated schema/issue views from this packet's structured planning source.
No runtime, network, database or provider operation is performed.
"""

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent

requirements = {
    "R01": "General Python runtime; Temporal; separate app Supabase; entity-store isolation; clean break",
    "R02": "Deep Agents plus required Agent Server and frontier route; later required Python Cursor Cloud/direct profiles",
    "R03": "Sandboxed dashboard interview discovers/refines/validates/spawns complete missions",
    "R04": "MCP, CLI and canonical skill discover/configure/spawn/track/intervene/revise",
    "R05": "Search catalog of skills, MCP servers/tools, plugins, spawn contracts and environment config",
    "R06": "Validated authorized materialization including extra processes and memory",
    "R07": "Sourced complete PDF deliverable with inspectable citations and artifact lineage",
    "R08": "Implemented feature patch/tests/review; app/deploy navigation only from authorized receipts",
    "R09": "Experiment reports and reproducible knowledge research/seed runs in both apps",
    "R10": "Durable HITL approval/rejection/feedback, notifications and enforced exact-target review",
    "R11": "Immediate stop request/fence and truthful remote interruption/irreversible effect semantics",
    "R12": "Context injection/queue/pause/resume/cancel/fork/retry with ordered/stale-safe delivery",
    "R13": "Active mission revision, impact/carry-forward and concurrency/ordering",
    "R14": "Stage and Goal Loop iteration state handoff and bounded loops/governors",
    "R15": "Context selection/provenance/budgeting/checkpoints/files/operational memory",
    "R16": "Parent/subagent messages/progress/state, cancellation/recovery and exact middleware",
    "R17": "Public progress without hidden chain of thought; extensions-versus-fork evidence",
    "R18": "Knowledge inspect/ingest/verify/classify/query specialized service/skill ports",
    "R19": "Versioned intent files and deterministic writes, auth/idempotency/partial receipts/approvals",
    "R20": "Sandbox scratch offload via Tavily/Firecrawl CLI; selective consumption and custody",
    "R21": "Workspace persistence/cleanup/isolation/secrets/quotas/processes/reproducibility",
    "R22": "WebSockets durable aggregation/reconnect/replay/backpressure/views/analytics",
    "R23": "Observability/provider cost accounting/bounded verified live budgets",
    "R24": "Retrieval and educational/recommendation experiments with baselines/datasets/evals",
    "R25": "Skill/CLI/MCP/web/mobile release; desktop later",
    "R26": "Required generative UI + MCP Apps/MCP-UI with secure host/fallback prototype",
}

rows = []


def issue(
    key, title, category, deps, repo, paths, inputs, outputs, tests, covers, spec, compute="C1"
):
    rows.append(
        {
            "id": key,
            "title": title,
            "category": category,
            "dependencies": deps.split() if deps else [],
            "repository": repo,
            "ownership_paths": paths.split("|"),
            "goal": title,
            "input_contracts": inputs.split("|"),
            "output_contracts": outputs.split("|"),
            "acceptance_tests": tests.split("|"),
            "requirements": covers.split(),
            "spec": spec,
            "compute_profile": compute,
            "status": "ready" if not deps else "blocked_by_dependencies",
            "non_goals": [
                "No live deployment, schema apply or metered provider work without separate concrete authorization.",
                "No domain logic in the general scheduler or private SDK APIs without a reviewed exception.",
            ],
            "proof_artifacts": [
                "Fixture inputs and expected outputs with digests",
                "Machine-readable test report, dependency/profile/build hashes and skipped-test reasons",
                "Failure-injection or semantic trace and reviewer disposition",
            ],
            "concurrency": {
                "writers_per_owned_region": 1,
                "max_agents": 2 if compute == "C1" else 1,
                "reviewer_independent": True,
            },
            "stop_conditions": [
                "Dependency proof rejected or missing: do not advertise dependent capability.",
                "Concurrent edit/ownership collision: preserve changes and hand off to integrator.",
                "Authority, irreversible-effect uncertainty, unknown spend or unqualified provider behavior: fail/hold visibly; do not retry blindly.",
                "Required external identity/budget unavailable: record blocker and continue offline independent work.",
            ],
        }
    )


issue(
    "MC-P001",
    "Freeze canonical contracts and pinned-source capability baseline",
    "foundation",
    "",
    "mission-control",
    "src/mission_control/contracts/|tests/contract/|docs/qualification/",
    "Current canonical spec/workflow annex|schemas.json seed definitions|Existing accepted proof and dependency metadata",
    "Generated strict Pydantic/JSON Schema/OpenAPI contract set|Golden canonical bytes/digests|Availability/evidence matrix and source digest manifest",
    "Unknown fields and invalid discriminators reject|Cross-language canonical digest fixtures agree|Pinned installed methods and runtime/host gaps recorded without invented results",
    "R01 R02 R04 R17 R23",
    "ARCHITECTURE-AND-ADRS.md",
)
issue(
    "MC-P002",
    "Prove two-project schema identity and role isolation",
    "foundation",
    "MC-P001",
    "mission-control (packages/mission-control-db-contract; ownership amendment 2026-10-03)",
    "packages/mission-control-db-contract/tests/|packages/mission-control-db-contract/component/generated/",
    "Common schema inventory and installation identities|Disposable PostgreSQL fixture pair with unrelated app schemas",
    "Normalized MC schema fingerprint/role test contract|Isolation qualification report",
    "Equal tenant/resource UUIDs cannot cross app|RLS role and pool context leakage denied|Same common shape without domain FKs/helpers",
    "R01 R19 R21",
    "DEPLOYMENT-AND-RELEASE.md",
)
issue(
    "MC-P003",
    "Prove Stage and Goal Loop typed state handoff",
    "foundation",
    "MC-P001",
    "mission-control",
    "src/mission_control/domain/programs/|tests/contract/handoff/",
    "StateHandoff and typed source/target states|Recorded BP StageGraph/GoalDirected invariants",
    "Pure reducer/merge and iteration fixtures|Sealed handoff/journal proof manifest",
    "Conflicting deltas reject under base-version check|Accepted producer gate and crash/duplicate handoff retain identity|Loop replay preserves counters and unresolved obligations",
    "R14 R15 R16",
    "CONTEXT-STATE-AND-CONTROL.md",
)
issue(
    "MC-P004",
    "Prove context budget selection and bidirectional subagent projection",
    "foundation",
    "MC-P001",
    "mission-control",
    "src/mission_control/application/artifacts/|tests/contract/context/",
    "ContextSelection policies/tokenizer and captured candidates|Parent/child channel and runtime-context schemas",
    "Selection/offload algorithm|Explicit outgoing and returning channel/context projection",
    "Mandatory context overflow blocks|Cross-tenant memory and prompt-injection grants denied|Child cannot overwrite parent authority/budget/journal; context hydration explicit",
    "R06 R15 R16 R17 R20",
    "CONTEXT-STATE-AND-CONTROL.md",
)
issue(
    "MC-P005",
    "Prove guarded effects and ordered urgent stop",
    "foundation",
    "MC-P001",
    "mission-control",
    "src/mission_control/application/execution/|tests/contract/control/",
    "Command/message/effect envelopes and generations|Fake irreversible tool and uncertain cancellation scenarios",
    "Stop-fence/admission ordering protocol|Safe-boundary queued versus interrupt receipts",
    "Stop races effect admission without new post-fence claims|Already applied effect retained after cancel|Duplicate/out-of-order/stale messages yield correct receipts",
    "R11 R12 R13 R16",
    "CONTEXT-STATE-AND-CONTROL.md",
)
issue(
    "MC-P006",
    "Prove Agent Server lost-launch and native lifecycle contract",
    "foundation",
    "MC-P001",
    "mission-control",
    "src/mission_control/adapters/agent_server/qualification/|tests/contract/agent_server/",
    "Pinned Agent Protocol/native launch identities|Fake/disposable app-bound servers and reservations",
    "Idempotent launch/reconciliation wrapper profile|Observed cancellation/result/usage qualification",
    "Concurrent launch cannot use non-atomic list-then-create as guarantee|Lost acknowledgement paginated/visibility recovery does not duplicate child|Cancel ack differs from settled terminal and stale output denied",
    "R02 R16 R21",
    "CONTEXT-STATE-AND-CONTROL.md",
)
issue(
    "MC-P007",
    "Prove checkpoint hydration retry fork and replay",
    "foundation",
    "MC-P003 MC-P004 MC-P006",
    "mission-control",
    "src/mission_control/application/recovery/|tests/contract/recovery/",
    "Sealed checkpoint/handoff/context frontiers|Current fork saga and captured-history evidence",
    "Durable app-scoped recovery protocol|Fork copy/hydration gating and diagnostic replay fixtures",
    "Fork gets new thread/run/budget and copies no live ownership|Start dispatch held until hydrate/continuity validation|Nonterminal retry preserves identity; diagnostic replay effects disabled",
    "R12 R14 R15 R16",
    "CONTEXT-STATE-AND-CONTROL.md",
)
issue(
    "MC-P008",
    "Prove secure MCP Apps host and native fallback",
    "foundation",
    "",
    "mission-control",
    "clients/ui-prototype/|tests/contract/mcp_apps/",
    "Official stable Apps protocol and host SDK sources|Mock authorized UiPresentation/action/resource bundles",
    "Host/build compatibility matrix|Sandbox/CSP/handshake prototype and fallback evidence",
    "Spoofed frame tool requests or stale actions denied|Unknown host renders meaningful native/text/link fallback|No credentials arbitrary iframe egress or hidden approvals",
    "R25 R26",
    "EXPERIENCE-AND-STREAMS.md",
)
issue(
    "MC-P009",
    "Qualify supported runtime storage and AWS topology assumptions",
    "foundation",
    "",
    "mission-control",
    "deploy/qualification/|docs/qualification/topology/",
    "Current pinned Agent Server/checkpointer packaging docs|App-local private schema target profiles and AWS baseline",
    "Supported topology decision record|Offline image/schema configuration validation and live preflight checklist",
    "Do not assume unsupported shared Supabase schema routing|Independent app pools/namespaces/secrets and backup inventory validated|No account purchase or metered probe is performed by offline proof",
    "R01 R02 R21 R23",
    "DEPLOYMENT-AND-RELEASE.md",
)
issue(
    "MC-P010",
    "Prove knowledge approval and uncertain write receipt semantics",
    "foundation",
    "MC-P001",
    "ai-engineer-knowledge-services",
    "packages/knowledge-db/src/ingestion/tests/|contracts/mission-control-integration/",
    "Native intent/plan/apply contracts|Domain effect identity and approval trust|Fake PG/Neo4j commit-response-loss scenarios",
    "DomainOperationEnvelope and receipt recovery fixtures|Classification mapping proposal and no-downgrade policy",
    "Changed payload/plan approvals reject|Partial admitted subset and uncertainty differ|Post-commit response loss recovered from same scoped marker without repeat",
    "R18 R19",
    "KNOWLEDGE-SERVICES.md",
)
issue(
    "MC-F001",
    "Transform backend into neutral Python distribution",
    "feature",
    "MC-P001",
    "mission-control",
    "pyproject.toml|uv.lock|src/mission_control/|bootstrap/",
    "Current backend reusable modules and source digest baseline|Clean-break organization contract",
    "Isolated mission-control Python build|No Mongo/domain imports and one composition root",
    "Build in isolated checkout without sibling imports|No legacy alias/history migration requirement|Current reusable invariants preserved under new names and no concurrent edits deleted",
    "R01 R02",
    "ARCHITECTURE-AND-ADRS.md",
    "C2",
)
issue(
    "MC-F002",
    "Implement common SQL component and new expansion records",
    "feature",
    "MC-P002",
    "mission-control (packages/mission-control-db-contract; ownership amendment 2026-10-03)",
    "packages/mission-control-db-contract/component/migrations/|packages/mission-control-db-contract/component/generated/|packages/mission-control-db-contract/component/manifest.json",
    "Base DATABASE inventory plus expansion records|Role/fingerprint proofs",
    "Checksummed self-contained release and generated DB contract|New context/message/process/interview/view/eval records",
    "Both disposable installs fingerprint equal|Scoped FKs lifecycle/actor keys/append-only integrity tested|No app entity schema or vendor-table hand edits",
    "R01 R10 R15 R16 R22",
    "DEPLOYMENT-AND-RELEASE.md",
    "C2",
)
issue(
    "MC-F003",
    "Implement app resolution and pinned installation tooling",
    "feature",
    "MC-F001 MC-F002 MC-P009",
    "mission-control; biotech-postgres-db-contract",
    "src/mission_control/application/installations/|src/biotech_postgres_db_contract/|installation/",
    "ApplicationBinding and component manifest|Secret references and disposable identity fixtures",
    "Auth-first app router/least-privilege pools|Plan verify apply tooling and idempotent seeds (no live apply)",
    "App spoofing/issuer mismatch denied|Schema drift/unavailable app cannot fall back|Repeat seed/version digest conflicts and nonmutating plan verified",
    "R01 R21",
    "DEPLOYMENT-AND-RELEASE.md",
    "C2",
)
issue(
    "MC-F004",
    "Implement immutable compiler and first recursive workflow vertical",
    "feature",
    "MC-F001 MC-P003",
    "mission-control",
    "src/mission_control/domain/authoring/|src/mission_control/domain/programs/|tests/contract/compiler/",
    "Workflow suite and strict program/revision schemas|State handoff reducer contracts",
    "Deterministic compiler/StageGraph/GoalLoop interpretations|Exact bindings and completion criteria",
    "Cycles/fanout unbounded or capability missing reject before side effects|Producer acceptance and typed projection enforced|Repeat compile gives same bytes/digests",
    "R13 R14",
    "CONTEXT-STATE-AND-CONTROL.md",
    "C1",
)
issue(
    "MC-F005",
    "Implement transactional intents budgets outbox and Temporal kernel",
    "feature",
    "MC-F003 MC-F004 MC-P005",
    "mission-control",
    "src/mission_control/application/execution/|src/mission_control/adapters/temporal/|src/mission_control/adapters/postgres/",
    "Scoped contracts/schema|Approved deterministic program|Command/effect/usage proof fixtures",
    "Root/activation/operation workflows|Atomic request/effect/reservation/event/outbox writer and reconciliation",
    "Worker death before/after commit/effect/receipt resumes same identity|Integer exposure includes unsettled children and unknown billing|No IO in workflow replay and no duplicate root dispatch",
    "R01 R11 R12 R23",
    "DEPLOYMENT-AND-RELEASE.md",
    "C1",
)
issue(
    "MC-F006",
    "Implement scoped capability search and exact configuration assembly",
    "feature",
    "MC-F003 MC-P004",
    "mission-control",
    "src/mission_control/application/catalog/|src/mission_control/adapters/postgres/catalog/|tests/contract/catalog/",
    "CatalogAsset/Environment/Spawn contracts|Seed assets and grants",
    "Authorized lexical search/index and config validator|Pinned selection/binding manifests",
    "Revoked/missing/transitive dependency drift rejects|Search cannot leak foreign snippets or resolve aliases at run time|Spawn depth/grants/budgets narrower than parent",
    "R05 R06",
    "CATALOG-AND-ENVIRONMENTS.md",
    "C2",
)
issue(
    "MC-F007",
    "Implement workspace custody offload and process leases",
    "feature",
    "MC-F005 MC-F006 MC-P004",
    "mission-control",
    "src/mission_control/adapters/langsmith/|src/mission_control/application/artifacts/|src/mission_control/application/workspaces/",
    "WorkspaceManifest and admitted CLI/process profiles|Selected context/skill/input artifacts",
    "Idempotent materializer/process lifecycle|Bounded retrieval captures and artifact registration/cleanup",
    "Traversal/symlink/secret/egress tests fail closed|Lost sandbox launch/partial upload/cleanup uncertainty visible|Large retrieval uses selective reads and cited bytes survive teardown",
    "R06 R20 R21",
    "CATALOG-AND-ENVIRONMENTS.md",
    "C2",
)
issue(
    "MC-F008",
    "Implement governed Deep Agents middleware and state channels",
    "feature",
    "MC-F007 MC-P003 MC-P004 MC-P005",
    "mission-control",
    "src/mission_control/adapters/deep_agents/|tests/contract/deep_agents/",
    "Pinned source hook matrix and extension profile|Tool wrappers and bidirectional context projections",
    "Exact effective middleware order/materializer|Typed public progress and guarded default tools",
    "Sync task cannot return parent authority fields|Runtime context explicitly reconstructed under grants|Guard covers native execute/dynamic launch and private reasoning never required",
    "R02 R14 R15 R16 R17",
    "CONTEXT-STATE-AND-CONTROL.md",
    "C1",
)
issue(
    "MC-F009",
    "Implement required Agent Server and subordinate mailbox",
    "feature",
    "MC-F008 MC-P006 MC-P009",
    "mission-control",
    "agent_server/|src/mission_control/adapters/agent_server/|src/mission_control/application/subordinates/",
    "Admitted graph/profile/endpoint and launch wrapper|Message mailboxes and dependency policies",
    "App-bound graphs/native persistence|Ordered parent-child delivery and settlement receipts",
    "Queue does not invoke unconditional native interrupt|Restart/lost update/concurrent launch recovers handles|Required unresolved child blocks completion; stale/late result preserved but not admitted",
    "R02 R12 R16",
    "CONTEXT-STATE-AND-CONTROL.md",
    "C1",
)
issue(
    "MC-F010",
    "Implement full control recovery and active revision transitions",
    "feature",
    "MC-F005 MC-F009 MC-P007",
    "mission-control",
    "src/mission_control/application/recovery/|src/mission_control/application/missions/revisions/|tests/acceptance/control/",
    "Fork/checkpoint/retry protocol and command mailbox|Revision impact/carry-forward contracts",
    "Inspect/queue/interrupt/pause/resume/cancel/fork/retry/replay endpoints|Recorded revision reconciliation",
    "Urgent stop observed versus settled states truthful|Competing revision/commands fail by expected version|In-flight pins unchanged and changed artifact invalidates approval",
    "R11 R12 R13",
    "CONTEXT-STATE-AND-CONTROL.md",
    "C1",
)
issue(
    "MC-F011",
    "Ship canonical public API MCP CLI and skill parity",
    "feature",
    "MC-F006 MC-F010",
    "mission-control",
    "src/mission_control/interfaces/|skills/mission-control/|clients/typescript/",
    "Single operation catalog and error/auth schemas|Authoring/run/control/catalog handlers",
    "FastAPI/OpenAPI HTTP client CLI remote MCP/stdio bridge|Digest-pinned skill/reference bundle",
    "Same mutation across transports returns same semantic receipt|Lost response same request identity|Host onboarding auth and complete skill bundle loaded; no direct table mutation",
    "R04 R25",
    "CATALOG-AND-ENVIRONMENTS.md",
    "C2",
)
issue(
    "MC-F012",
    "Implement sandbox interview and draft-to-workflow coordinator",
    "feature",
    "MC-F007 MC-F008 MC-F011",
    "mission-control",
    "src/mission_control/application/interviews/|src/mission_control/interfaces/http/interviews/|clients/web/interviews/",
    "InterviewCreate/Turn and scoped interview event schemas|Catalog/config/context operations and drafts",
    "Recoverable interview sessions and explicit validate/commit/start UI|Coordinator manifest and durable draft links",
    "Interview is not fictitious Mission Run|Unpermitted chat cannot launch effects|Start response loss recovered and warnings/effect grants visible",
    "R03 R04 R06",
    "EXPERIENCE-AND-STREAMS.md",
    "C2",
)
issue(
    "MC-F013",
    "Implement durable Human Tasks feedback and notification intents",
    "feature",
    "MC-F005 MC-F010",
    "mission-control",
    "src/mission_control/application/reviews/|src/mission_control/application/notifications/|tests/acceptance/review/",
    "ReviewPacket/Decision exact target digest|Notification/profile and artifact relations",
    "Attributable resolutions/remediation versions|Durable inbox/push adapter contract with duplicate-safe delivery",
    "Competing/stale/digest-changed approvals reject|Timeout cannot approve and notification cannot resolve|Response loss or duplicate notification does not duplicate decision",
    "R10 R13",
    "EXPERIENCE-AND-STREAMS.md",
    "C2",
)
issue(
    "MC-F014",
    "Implement durable event aggregation WebSocket SSE and views",
    "feature",
    "MC-F005 MC-P005",
    "mission-control",
    "src/mission_control/interfaces/http/streams/|src/mission_control/application/views/|src/mission_control/application/events/",
    "Existing canonical mc.event.v1 and snapshot frontiers|Stream profile/ticket/frame schemas",
    "App-scoped replayable WS/SSE adapters and idempotent projectors|Per-mission watermarks/backpressure/usage views",
    "Replay-to-tail race/duplicate/gap/retention paths correct|Slow consumer disconnect never drops durable events|Revocation/ticket expiry and projection rebuild tested; no cross-mission total order promised",
    "R17 R22 R23",
    "EXPERIENCE-AND-STREAMS.md",
    "C2",
)
issue(
    "MC-F015",
    "Integrate both web dashboards and navigable artifacts decisions",
    "feature",
    "MC-F012 MC-F013 MC-F014",
    "aiengineerapp; Biotech product dashboard",
    "shared mission-ui client/|app-owned dashboard routes/",
    "Generated scoped client and interviews/views|Artifact/review/publication receipts",
    "Shared mission debugger/interview/review experience in both apps|Authorized PDF/patch/result/big-decision/app navigation",
    "Only registered deploy URL displayed as finished app|Stale views/conflicting controls clearly refresh|Both app tokens cannot select other installation",
    "R03 R07 R08 R10 R22 R25",
    "EXPERIENCE-AND-STREAMS.md",
    "C2",
)
issue(
    "MC-F016",
    "Build mobile client and durable fallback review controls",
    "feature",
    "MC-F011 MC-F013 MC-F014 MC-P008",
    "mission-control mobile (proposed client)",
    "clients/mobile/|tests/mobile/",
    "Generated API and native component contracts|App auth and configured push profile",
    "Proposed Expo/React Native client with native fallback|iOS/Android proof artifacts and framework decision",
    "Background/resume reconnect and task refresh correct|No offline automatic approval|Push/deep links/document access/control and no token leakage qualified on both platforms",
    "R10 R25 R26",
    "EXPERIENCE-AND-STREAMS.md",
    "C2",
)
issue(
    "MC-F017",
    "Implement generative UI and standard MCP Apps resources",
    "feature",
    "MC-F011 MC-F013 MC-P008",
    "mission-control",
    "clients/ui-components/|src/mission_control/interfaces/mcp/ui/|tests/contract/ui/",
    "UiPresentation/UiActionIntent/component allowlist|Pinned MCP Apps resource and host matrix",
    "Schema-valid generated components and secure Apps bundle|MCP-UI dashboard prototype + native/text fallback",
    "Malformed props/arbitrary JS/unknown actions denied|Iframe handshake source/CSP/egress/spoofed mediation tests|Fallback retains review/stop functions; stale action uses existing receipt/version semantics",
    "R26 R25",
    "EXPERIENCE-AND-STREAMS.md",
    "C2",
)
issue(
    "MC-F018",
    "Qualify and implement Python Cursor Cloud harness",
    "feature",
    "MC-F007 MC-F010 MC-P001",
    "mission-control",
    "src/mission_control/adapters/cursor_cloud/|tests/contract/cursor/",
    "Locked Python SDK/bridge profile|Repository workspace and common harness contracts",
    "Qualified create/reattach/observe/cancel/usage adapter|Patch/test/artifact custody and emulated fork profile",
    "Lost launch/native handle recovery without blind paid repeat|Queue versus cancel/new-run semantics correctly reported|Fork native unsupported unless proved; no operator dirty-worktree writes or unauthorized auto-PR",
    "R02 R08 R12 R21",
    "CATALOG-AND-ENVIRONMENTS.md",
    "C1",
)
issue(
    "MC-F019",
    "Implement bounded frontier routes and cost accounting",
    "feature",
    "MC-F005 MC-F008",
    "mission-control",
    "src/mission_control/adapters/frontier/|src/mission_control/application/accounting/",
    "Provider profile and current price snapshots when authorized|Usage/reservation/correction schemas",
    "Pinned Deep Agents model route and subsequent direct executor profiles|Per-dimension usage settlement/reconciliation",
    "Unknown usage is not zero|Failover never changes uncertain billed identity|Stateless profile rejects unsupported continuation; double settlement impossible",
    "R02 R23",
    "DEPLOYMENT-AND-RELEASE.md",
    "C2",
)
issue(
    "MC-F020",
    "Complete Swarm Optimizer Child Missions and recursive semantics",
    "feature",
    "MC-F004 MC-F009 MC-F010",
    "mission-control",
    "src/mission_control/domain/programs/|src/mission_control/application/invocations/|tests/acceptance/workflow_systems/",
    "Workflow 00-09 suite|Admission/handoff/control/evaluator contracts",
    "Four-system recursive interpreter/conformance fixtures|Bounded revisit/fanout/convergence/child projections",
    "Quorum/patience/no-progress/governor outcomes explicit|Evaluator independent context cannot accept itself|Child mission independently admitted and active revision impacts proven",
    "R13 R14 R16",
    "CONTEXT-STATE-AND-CONTROL.md",
    "C1",
)
issue(
    "MC-F021",
    "Publish knowledge operation and deterministic PostgreSQL intent adapter",
    "feature",
    "MC-P010 MC-F011 MC-F013",
    "ai-engineer-knowledge-services",
    "packages/application/src/operations/|packages/knowledge-db/src/|skills/knowledge integration/|published clients/",
    "Native immutable intent/read/plan/receipt schemas|Generic external DomainOperationEnvelope|Trusted approval/oracle bindings",
    "Published inspect/query/retrieve/verify/classify proposal/plan/apply/receipt ports|No internal-import Python adapter and advertised availability",
    "Executor-only routes not falsely advertised as transport parity|Classifier mapping rejects downgrade/unknown labels|Plan digest/head drift/partial/uncertain response tested with public handlers",
    "R18 R19",
    "KNOWLEDGE-SERVICES.md",
    "C1",
)
issue(
    "MC-F022",
    "Implement governed Neo4j intent writer and receipt marker",
    "feature",
    "MC-P010 MC-F011 MC-F013",
    "biotech-kg / Biotech knowledge service",
    "domain-owned ingestion service/|graph constraints/|tests/intent_writer/",
    "Exact app graph schema and domain proposal families|Scoped intent/effect/approval contracts",
    "Atomic graph marker/precondition/write/receipt transaction|Authenticated receipt/read-back adapter",
    "Graph commit response loss recovers marker without duplicate write|Changed identity payload or stale domain preconditions reject|Partial policy preserved and no PG-Neo4j distributed transaction assumed",
    "R18 R19 R09",
    "KNOWLEDGE-SERVICES.md",
    "C1",
)
issue(
    "MC-F023",
    "Implement advisory memory admission and recall policy",
    "feature",
    "MC-P004 MC-F006 MC-F021",
    "mission-control / app knowledge services",
    "src/mission_control/application/memory/|domain memory adapter/|tests/contract/memory/",
    "MemoryRecall/admission/namespace policy|Consent/expiry/provenance and service bindings",
    "Scoped recall/write-proposal/revocation receipts|Bounded episodic/tenant/domain projections",
    "No automatic chat/checkpoint ingestion|Personal-public and cross-tenant filters applied before ranking|Revoked/expired memory not authority and retained audit policy explicit",
    "R06 R15 R18",
    "CONTEXT-STATE-AND-CONTROL.md",
    "C2",
)
issue(
    "MC-F024",
    "Implement sourced PDF production validation and revision workflow",
    "feature",
    "MC-F007 MC-F013 MC-F021",
    "mission-control / app artifact adapters",
    "artifact renderer capability/|tests/acceptance/pdf/",
    "Accepted report schema/captured source selectors|Render profile/font/tool versions|Human review contract",
    "Registered complete PDF plus citation/claim index/source manifest|Render/schema/citation assessment and revision lineage",
    "Every required report section/table/citation survives PDF rendering|Broken source selector/page overflow/missing font detected|Rejected revised bytes require fresh gate; no model text called finished PDF",
    "R07 R10",
    "WORKED-SCENARIOS.md",
    "C2",
)
issue(
    "MC-F025",
    "Implement reproducible retrieval and recommendation experiment runner",
    "feature",
    "MC-F020 MC-F021 MC-F019 MC-D002",
    "mission-control / app eval owners",
    "evaluation capability/|datasets manifests/|tests/acceptance/experiments/",
    "Dataset/split/license/consent and frozen baseline|Treatment/rubric/metric/seed/profile/budget manifests",
    "Reproducible paired evaluation/report artifacts|Independent admission and statistical uncertainty policy",
    "No train/test leakage or changing baseline mid-run|Captured replay reproduces metrics; treatment failures/abstention included|Recommendations cite evidence, disclose uncertainty and do not claim medical efficacy",
    "R09 R24 R23",
    "WORKED-SCENARIOS.md",
    "C3",
)
issue(
    "MC-D001",
    "Prepare separate app seed catalogs policies and endpoint bindings",
    "data",
    "MC-F003 MC-F006",
    "biotech-postgres-db-contract / AI Engineer installer",
    "seeds/|application binding manifests/",
    "Same common component version|Actual operator identity/endpoint references or offline fakes",
    "Versioned idempotent separate app seeds|Template capability/rubric/data readiness manifests",
    "Same-version changed seed digest conflicts|Missing endpoint remains unavailable|Seeds cannot manufacture users/operator authority or cross-project credentials",
    "R01 R05 R09 R18",
    "DEPLOYMENT-AND-RELEASE.md",
    "C2",
)
issue(
    "MC-D002",
    "Prepare licensed captured benchmark datasets and split manifest",
    "data",
    "",
    "app-owned evaluation packages",
    "datasets manifests/|rubrics/|offline fixtures/",
    "Owner-approved public/licensed captures or synthetic fixtures|Engineering/biotech educational task definitions",
    "Immutable dataset source/consent/provenance/split/digest manifest|Baseline metrics and safety/abstention rubric specification",
    "No PHI/consent leakage or duplicated train/test entities|Ambiguous labels/absent evidence flagged|Offline baselines include negative/conflicting-source cases; no unsupported clinical inference",
    "R09 R24 R17",
    "WORKED-SCENARIOS.md",
    "C3",
)
issue(
    "MC-D003",
    "Close knowledge-service availability and source readiness",
    "data",
    "MC-F021 MC-F022 MC-D001",
    "app knowledge services",
    "capability readiness manifests/|verification dataset fixtures/",
    "Domain endpoint/contracts and seeded grants|Source evidence/custody and schema mappings",
    "Both-app capability readiness/unsupported matrix|Verified source fixtures and receipt access",
    "Unavailable read-intent retrieval not relabeled success|Classification mapping and domains differ explicitly|All required seed items inspectable and every write has recovery receipt",
    "R18 R19 R09",
    "KNOWLEDGE-SERVICES.md",
    "C2",
)
issue(
    "MC-I001",
    "Qualify Deep Agents Agent Server parity in both installations",
    "integration",
    "MC-F009 MC-F010 MC-F019 MC-D001",
    "mission-control",
    "tests/acceptance/two_app_parity/|docs/qualification/",
    "Same release/lock/schema plus different app grants|Approved finite live allocation only for live variant",
    "Offline/disposable/live-separated parity report|Deep Agents stage/goal/child recovery artifacts",
    "Both apps same build isolated writes|Real required async path persists/restarts under chosen topology when authorized|Lost launch/pause/cancel/usage and host loaded skill truthfully qualified",
    "R01 R02 R14 R16 R23",
    "DEPLOYMENT-AND-RELEASE.md",
    "C4",
)
issue(
    "MC-I002",
    "Qualify all public surfaces and UI compatibility",
    "integration",
    "MC-F015 MC-F016 MC-F017 MC-F011",
    "mission-control / app clients",
    "tests/acceptance/public_surfaces/|host compatibility manifest/",
    "Operation catalog/skill/UI resource digests|Web/mobile/MCP/CLI auth fixtures",
    "Parity report plus secure host/fallback matrix|Mobile and web actual-shell results",
    "Same app grants and mutation receipts across clients|Host unsupported rich UI falls back safely|Offline mobile stale approvals and spoofed frame/tool context denied",
    "R03 R04 R10 R25 R26 R22",
    "EXPERIENCE-AND-STREAMS.md",
    "C2",
)
issue(
    "MC-I003",
    "Qualify PDF coding seed and research experiment scenarios",
    "integration",
    "MC-I001 MC-F018 MC-F024 MC-F025 MC-D003 MC-F023",
    "mission-control / app domain owners",
    "tests/acceptance/worked_scenarios/|docs/qualification/scenarios/",
    "All scenario input/output/dependency manifests|Finite allocation for any live variant",
    "Both-app artifacts and requirement criterion receipts|Patch/tests/PDF/ingestion/experiment provenance packets",
    "Real byte artifacts complete and navigable|Adversarial feedback/revision/partial failure cases settle|Feature output not deployed unless explicitly separate admitted stage; no unexplained skipped cases",
    "R07 R08 R09 R10 R18 R19 R24",
    "WORKED-SCENARIOS.md",
    "C4",
)
issue(
    "MC-R001",
    "Qualify security load failure recovery and accounting",
    "release",
    "MC-I001 MC-I002 MC-I003 MC-P009",
    "mission-control / infrastructure owner",
    "tests/release/|deploy/qualification/|runbooks/",
    "Release manifest and measured limits|Disposable load/failure/security/billing fixtures",
    "Capacity/latency/isolation/restore/reconciliation evidence|Revocation/redaction/budget enforcement runbooks",
    "Restore bytes plus DB/workflow/checkpoint frontiers|Slow sockets/DB outage/worker loss preserve event/receipt truth|Unknown spend and irreversible effects visible; no hidden reasoning/secret leakage",
    "R11 R12 R21 R22 R23",
    "DEPLOYMENT-AND-RELEASE.md",
    "C3",
)
issue(
    "MC-R002",
    "Package release artifacts skill CLI MCP web mobile and schema",
    "release",
    "MC-R001",
    "mission-control / app installers",
    "release manifest/|clients releases/|skills/|deploy/",
    "All passed gates/source hashes|Compatible common component and app registry refs",
    "Signed/checksummed reproducible artifact manifest|Installer plans and consumer compatibility report",
    "Isolated build installs exact dependencies with no domain/Mongo imports|Skill references and UI bundle hashes complete|Two projects same MC shape and unsupported capabilities remain disabled",
    "R01 R04 R25 R26",
    "DEPLOYMENT-AND-RELEASE.md",
    "C2",
)
issue(
    "MC-R003",
    "Prepare and execute separately authorized production qualification",
    "release",
    "MC-R002",
    "infrastructure operator / app owners",
    "deploy/production plans/|docs/qualification/production/",
    "Concrete target identities/roles/backups/regions and owner-approved finite budget|Reviewed nonmutating install/deploy/rollback plans",
    "Preflight/proposed production change packet first|Only after authorization: deployed proof and release disposition",
    "No purchases/provisioning/migrations until concrete approval|Per-app readiness/namespace/security/notification/network actual targets qualified|Deployment status distinct from local tests; stop on cost/authority/backup mismatch",
    "R01 R02 R18 R21 R23 R25 R26",
    "DEPLOYMENT-AND-RELEASE.md",
    "C4",
)

# Tighten cross-repository output ownership to concrete proposed path regions.
overrides = {
    "MC-F015": (
        "aiengineerapp; mission-control",
        [
            "aiengineerapp/app/mission-control/",
            "aiengineerapp/components/mission-control/",
            "mission-control/clients/web/",
        ],
    ),
    "MC-F016": ("mission-control", ["clients/mobile/", "tests/mobile/"]),
    "MC-F022": ("biotech-kg", ["src/ingestion/", "operations/ingestion/", "tests/intent_writer/"]),
    "MC-F023": (
        "mission-control; ai-engineer-knowledge-services; biotech-kg",
        [
            "mission-control/src/mission_control/application/memory/",
            "mission-control/tests/contract/memory/",
            "ai-engineer-knowledge-services/packages/application/src/memory/",
            "biotech-kg/src/memory/",
        ],
    ),
    "MC-F024": (
        "mission-control",
        [
            "src/mission_control/adapters/report_rendering/",
            "catalog/report_profiles/",
            "tests/acceptance/pdf/",
        ],
    ),
    "MC-F025": (
        "mission-control",
        [
            "src/mission_control/application/evaluations/",
            "catalog/evaluation/",
            "tests/acceptance/experiments/",
        ],
    ),
    "MC-D002": (
        "mission-control",
        [
            "catalog/evaluation/biotech/",
            "catalog/evaluation/ai_engineer/",
            "tests/fixtures/evaluation/",
        ],
    ),
    "MC-D003": (
        "mission-control; app knowledge services",
        ["catalog/readiness/", "tests/acceptance/knowledge/"],
    ),
}
for row in rows:
    if row["id"] in overrides:
        row["repository"], row["ownership_paths"] = overrides[row["id"]]
    if row["id"] == "MC-I001":
        row["dependencies"].append("MC-F011")

# Schema seeds are closed; richer domain schema refs remain pinned external assets.
S = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": "urn:mc:expansion:v1",
    "$defs": {},
}


def obj(props, required=None):
    return {
        "type": "object",
        "properties": props,
        "required": list(props) if required is None else required,
        "additionalProperties": False,
    }


string = {"type": "string", "minLength": 1}
uid = {"type": "string", "format": "uuid"}
digest = {"type": "string", "pattern": "^sha256:[0-9a-f]{64}$"}
integer = {"type": "integer", "minimum": 0, "maximum": 9223372036854775807}
pos = {"type": "integer", "minimum": 1, "maximum": 9223372036854775807}


def arr(item):
    return {"type": "array", "items": item}


def ref(name):
    return {"$ref": f"#/$defs/{name}"}


S["$defs"]["Scope"] = obj({"installation_id": uid, "application_id": string, "tenant_id": uid})
S["$defs"]["ArtifactRef"] = obj(
    {
        "resource_id": uid,
        "scope": ref("Scope"),
        "issuer": string,
        "schema_ref": string,
        "digest": digest,
    }
)


def contract(name, props):
    S["$defs"][name] = obj(
        {
            "schema_version": {
                "const": "mc."
                + {
                    "ContextSelection": "context_selection",
                    "StateHandoff": "state_handoff",
                    "Message": "message",
                    "Progress": "progress",
                    "ProofBudget": "proof_budget",
                    "KnowledgeEnvelope": "knowledge_request",
                    "UiPresentation": "ui_presentation",
                    "UiActionIntent": "ui_action_intent",
                    "WorkspaceManifest": "workspace_manifest",
                    "StreamControl": "stream_control",
                    "StreamFrame": "stream_frame",
                }[name]
                + ".v1"
            },
            **props,
        }
    )


S["$defs"]["ContextItem"] = obj(
    {
        "artifact": ref("ArtifactRef"),
        "locator": string,
        "token_bound": integer,
        "trust": {
            "enum": [
                "untrusted_source",
                "admitted_evidence",
                "operating_contract",
                "advisory_memory",
            ]
        },
    }
)
S["$defs"]["Omission"] = obj(
    {
        "candidate_ref": string,
        "reason": {
            "enum": [
                "budget",
                "not_authorized",
                "expired",
                "duplicate",
                "irrelevant",
                "unqualified",
            ]
        },
    }
)
contract(
    "ContextSelection",
    {
        "selection_id": uid,
        "request_id": uid,
        "scope": ref("Scope"),
        "target_ref": string,
        "purpose": string,
        "policy_ref": string,
        "tokenizer_ref": string,
        "candidate_capture_refs": arr(ref("ArtifactRef")),
        "selected": arr(ref("ContextItem")),
        "omitted": arr(ref("Omission")),
        "max_input_tokens": integer,
        "total_token_bound": integer,
        "file_manifest_ref": ref("ArtifactRef"),
        "grants_snapshot_ref": string,
    },
)
contract(
    "StateHandoff",
    {
        "handoff_id": uid,
        "scope": ref("Scope"),
        "producer_attempt_id": uid,
        "producer_generation": pos,
        "target_ref": string,
        "base_version": pos,
        "base_digest": digest,
        "revision_digest": digest,
        "binding_digest": digest,
        "input_digest": digest,
        "delta": ref("ArtifactRef"),
        "outputs": arr(ref("ArtifactRef")),
        "journal_through_seq": integer,
        "liability_manifest_ref": ref("ArtifactRef"),
        "state_schema_ref": string,
        "admission_decision_ref": string,
    },
)
contract(
    "Message",
    {
        "request_id": uid,
        "scope": ref("Scope"),
        "target_ref": string,
        "target_generation": pos,
        "sender_ref": string,
        "client_message_id": uid,
        "kind": {"enum": ["instruction", "context", "progress", "question", "answer", "result"]},
        "boundary": {"enum": ["next_turn", "next_iteration", "after_checkpoint"]},
        "content": ref("ArtifactRef"),
        "deadline": {"type": ["string", "null"], "format": "date-time"},
    },
)
contract(
    "Progress",
    {
        "scope": ref("Scope"),
        "attempt_id": uid,
        "generation": pos,
        "summary": {"type": "string", "maxLength": 4096},
        "completed": arr(string),
        "remaining": arr(string),
        "blocker_refs": arr(string),
        "outputs": arr(ref("ArtifactRef")),
        "usage_disposition": {"enum": ["settled", "estimated", "unknown"]},
        "journal_through_seq": integer,
    },
)
S["$defs"]["PriceDimension"] = obj(
    {
        "dimension": string,
        "billing_unit": string,
        "max_units": integer,
        "rate_micros_numerator": {"type": "string", "pattern": "^[0-9]+$"},
        "rate_units_denominator": pos,
        "price_snapshot_ref": string,
    }
)
contract(
    "ProofBudget",
    {
        "budget_id": uid,
        "scope": ref("Scope"),
        "actor_ref": string,
        "target_refs": arr(string),
        "profile_refs": arr(string),
        "currency": {"type": "string", "pattern": "^[A-Z]{3}$"},
        "dimensions": arr(ref("PriceDimension")),
        "reserved_micros": {"type": "string", "pattern": "^[0-9]+$"},
        "max_concurrency": pos,
        "stop_conditions": arr(string),
        "authorization_ref": string,
        "expiry": {"type": "string", "format": "date-time"},
    },
)
contract(
    "KnowledgeEnvelope",
    {
        "request_id": uid,
        "scope": ref("Scope"),
        "run_id": uid,
        "activation_id": uid,
        "attempt_id": uid,
        "generation": pos,
        "operation": {
            "enum": [
                "inspect",
                "capture",
                "verify",
                "classify_propose",
                "classify_admit",
                "query",
                "retrieve",
                "ingestion_plan",
                "ingestion_apply",
                "receipt_get",
                "verify_result",
            ]
        },
        "capability_ref": string,
        "executor_digest": digest,
        "input_manifest": ref("ArtifactRef"),
        "native_schema_ref": string,
        "native_payload_digest": digest,
        "effect_key": string,
        "precondition_manifest": ref("ArtifactRef"),
        "approval_refs": arr(string),
        "admission_ref": string,
        "governor_profile_ref": string,
    },
)
contract(
    "UiPresentation",
    {
        "presentation_id": uid,
        "scope": ref("Scope"),
        "component_ref": string,
        "component_version": string,
        "props_schema_ref": string,
        "props_artifact_ref": ref("ArtifactRef"),
        "source_resource_refs": arr(string),
        "as_of_seq": integer,
        "allowed_action_refs": arr(string),
        "fallback_text": string,
    },
)
S["$defs"]["UiPresentation"]["properties"]["expires_at"] = {
    "type": ["string", "null"],
    "format": "date-time",
}
contract(
    "UiActionIntent",
    {
        "request_id": uid,
        "scope": ref("Scope"),
        "presentation_id": uid,
        "action_ref": string,
        "target_ref": string,
        "expected_resource_version": pos,
        "payload_artifact_ref": ref("ArtifactRef"),
    },
)
S["$defs"]["FileEntry"] = obj(
    {
        "path": string,
        "artifact": ref("ArtifactRef"),
        "bytes": integer,
        "mode": {"enum": ["read_only", "candidate_output"]},
    }
)
contract(
    "WorkspaceManifest",
    {
        "workspace_id": uid,
        "scope": ref("Scope"),
        "attempt_id": uid,
        "generation": pos,
        "environment_ref": string,
        "image_digest": digest,
        "files": arr(ref("FileEntry")),
        "output_prefixes": arr(string),
        "scratch_prefixes": arr(string),
        "process_profile_refs": arr(string),
        "network_policy_ref": string,
        "quota_profile_ref": string,
        "lease_ref": string,
        "cleanup_policy_ref": string,
    },
)
S["$defs"]["Subscription"] = obj({"mission_id": uid, "after_seq": integer})
S["$defs"]["StreamControl"] = {
    "oneOf": [
        obj(
            {
                "schema": {"const": "mc.stream_control.v1"},
                "type": {"const": "subscribe"},
                "subscriptions": arr(ref("Subscription")),
            }
        ),
        obj(
            {
                "schema": {"const": "mc.stream_control.v1"},
                "type": {"const": "ack"},
                "mission_id": uid,
                "through_seq": integer,
            }
        ),
    ]
}
S["$defs"]["EventExecution"] = obj(
    {
        "node_key": string,
        "activation_id": uid,
        "attempt_no": pos,
        "harness_execution_id": uid,
        "native_session_ref": string,
        "native_turn_ref": string,
        "generation": pos,
    },
    [],
)
S["$defs"]["EventSource"] = obj(
    {
        "kind": {"enum": ["mission_control", "adapter", "human", "agent"]},
        "actor_ref": string,
        "native_event_ref": string,
    },
    ["kind"],
)
S["$defs"]["MissionEvent"] = obj(
    {
        "schema_version": {"const": "mc.event.v1"},
        "event_id": uid,
        "scope": ref("Scope"),
        "mission_id": uid,
        "run_id": uid,
        "revision_id": uid,
        "seq": pos,
        "ledger_commit_id": uid,
        "occurred_at": {"type": "string", "format": "date-time"},
        "recorded_at": {"type": "string", "format": "date-time"},
        "event_type": string,
        "event_version": pos,
        "execution": ref("EventExecution"),
        "source": ref("EventSource"),
        "causation_id": uid,
        "correlation_id": uid,
        "payload_ref": ref("ArtifactRef"),
    },
    [
        "schema_version",
        "event_id",
        "scope",
        "mission_id",
        "seq",
        "ledger_commit_id",
        "occurred_at",
        "recorded_at",
        "event_type",
        "event_version",
        "execution",
        "source",
        "payload_ref",
    ],
)
S["$defs"]["StreamFrame"] = {
    "oneOf": [
        obj(
            {
                "schema": {"const": "mc.stream_frame.v1"},
                "type": {"const": "event"},
                "event": ref("MissionEvent"),
            }
        ),
        obj(
            {
                "schema": {"const": "mc.stream_frame.v1"},
                "type": {"const": "heartbeat"},
                "recorded_at": {"type": "string", "format": "date-time"},
            }
        ),
        obj(
            {
                "schema": {"const": "mc.stream_frame.v1"},
                "type": {"const": "resync_required"},
                "mission_id": uid,
                "retained_floor": integer,
                "snapshot_ref": string,
                "snapshot_seq": integer,
                "code": {"const": "CURSOR_EXPIRED"},
            }
        ),
        obj(
            {
                "schema": {"const": "mc.stream_frame.v1"},
                "type": {"const": "auth_expiring"},
                "reconnect_required": {"const": True},
            }
        ),
    ]
}
S["$defs"]["InterviewCreate"] = obj(
    {
        "schema_version": {"const": "mc.interview_create.v1"},
        "request_id": uid,
        "tenant_id": uid,
        "draft_ref": string,
        "environment_profile_ref": string,
        "admitted_agent_config_ref": string,
        "initial_message_ref": string,
        "context_request_ref": string,
        "budget_profile_ref": string,
    },
    [
        "schema_version",
        "request_id",
        "tenant_id",
        "environment_profile_ref",
        "admitted_agent_config_ref",
        "initial_message_ref",
        "context_request_ref",
        "budget_profile_ref",
    ],
)
S["$defs"]["ReviewDecision"] = obj(
    {
        "schema_version": {"const": "mc.review_decision.v1"},
        "request_id": uid,
        "human_task_id": uid,
        "expected_task_version": pos,
        "target_digest": digest,
        "decision": {"enum": ["approve", "reject", "request_changes", "abstain"]},
        "feedback_ref": string,
        "selected_evidence_refs": arr(string),
    },
    [
        "schema_version",
        "request_id",
        "human_task_id",
        "expected_task_version",
        "target_digest",
        "decision",
        "selected_evidence_refs",
    ],
)
S["$defs"]["ReviewDecision"]["allOf"] = [
    {
        "if": {"properties": {"decision": {"enum": ["reject", "request_changes"]}}},
        "then": {"required": ["feedback_ref"]},
    }
]
(ROOT / "schemas.json").write_text(json.dumps(S, indent=2) + "\n", encoding="utf-8")
(ROOT / "issues.json").write_text(
    json.dumps(
        {"schema_version": "mc.issue_packet.v1", "requirements": requirements, "issues": rows},
        indent=2,
    )
    + "\n",
    encoding="utf-8",
)
issues_dir = ROOT / "issues"
issues_dir.mkdir(exist_ok=True)
for row in rows:

    def bullets(values):
        return "\n".join("- " + v for v in values)

    content = f"""# {row["id"]} — {row["title"]}

Category: {row["category"]}. Status: {row["status"]}. Owner repository: `{row["repository"]}`. These are proposed destination paths from CODEBASE-ORGANIZATION; repository existence/physical transformation is not implied.

## Goal and authority

{row["goal"]}. Implement the contracts in [{row["spec"]}](../{row["spec"]}) and the canonical pack; do not introduce another scheduler or schema source. Requirements: {", ".join(row["requirements"])}.

## Dependencies and file ownership

Dependencies: {", ".join(row["dependencies"]) or "none; executable offline first issue"}. Start only after dependent proof disposition is accepted. One writer owns each claimed region; the integrator owns shared contracts/schema/registries.

{bullets("`" + p + "`" for p in row["ownership_paths"])}

## Input contracts

{bullets(row["input_contracts"])}

## Output contracts

{bullets(row["output_contracts"])}

## Acceptance tests

{bullets(row["acceptance_tests"])}

Run the highest meaningful public/interface seam. Add negative, concurrency and crash fixtures as specified. Existing recorded tests are regression anchors, not new-engine proof results. New full schemas and numeric profile defaults must be generated/versioned, with compatibility tests before advertising support.

## Scope and non-goals

{bullets(row["non_goals"])}

Scope is the goal, owned regions, input/output contracts and tests above. No blanket refactor, unrelated app cleanup, calendar schedule or external tracker publication. Preserve concurrent/uncommitted work. User authorization for implementation would be separate from this planning packet.

## Proof artifacts

{bullets(row["proof_artifacts"])}

Save implementation evidence in owner-repository `docs/qualification/{row["id"]}/` (proposed) with `evidence.json`, fixture manifest, test results and reviewer disposition. Link external service artifacts by issuer/version/digest; no secrets/private reasoning. Include exact skipped/unavailable cases.

## Model, compute and concurrency

Profile `{row["compute_profile"]}` from [deployment/compute](../DEPLOYMENT-AND-RELEASE.md). Maximum {row["concurrency"]["max_agents"]} agents on this issue, one writer per region, independent review. Use strongest authorized reasoning for architectural/failure seams and smaller qualified profiles for routine fixture work. No metered call until a finite ProofBudget with current official rate/assumptions is approved; offline work can proceed. Maximum issue parallelism is bounded by ready DAG nodes, disjoint ownership and team resource allocations.

## Stop and escalation

{bullets(row["stop_conditions"])}
"""
    (issues_dir / (row["id"] + ".md")).write_text(content, encoding="utf-8")
matrix = [
    "# Requirement coverage and dependency backlog",
    "",
    "This is the repo-local implementation-ready issue packet explicitly requested by the owner. It is not published externally. `build_packet.py` is the structured authoring source; `issues.json` is its canonical machine-readable dispatch snapshot, and the issue views/coverage table are generated from the same source. Change the authoring source and regenerate to keep them consistent. No calendar schedule is defined.",
    "",
    "| Requirement | Scope | Implementing / proving issues |",
    "| --- | --- | --- |",
]
for key, meaning in requirements.items():
    owners = [r for r in rows if key in r["requirements"]]
    matrix.append(
        f"| {key} | {meaning} | "
        + ", ".join(f"[{r['id']}](issues/{r['id']}.md)" for r in owners)
        + " |"
    )
matrix += [
    "",
    "## Issue sequence",
    "",
    "| ID | Category | Dependencies | Goal |",
    "| --- | --- | --- | --- |",
]
for row in rows:
    matrix.append(
        f"| [{row['id']}](issues/{row['id']}.md) | {row['category']} | {', '.join(row['dependencies']) or 'none'} | {row['title']} |"
    )
(ROOT / "REQUIREMENTS.md").write_text("\n".join(matrix) + "\n", encoding="utf-8")
print(
    f"Generated {len(rows)} issues covering {len(requirements)} requirements and {len(S['$defs'])} schema definitions."
)
