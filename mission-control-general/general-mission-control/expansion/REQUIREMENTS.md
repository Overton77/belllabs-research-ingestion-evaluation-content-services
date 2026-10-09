---
type: Specification Annex
title: Requirement coverage and dependency backlog
description: This is the repo-local implementation-ready issue packet explicitly requested by the owner. It is not published externally. build_packet.py is the structured authoring source; issues.json is its canonical…
tags: [mission-control, spec, expansion]
---
# Requirement coverage and dependency backlog

This is the repo-local implementation-ready issue packet explicitly requested by the owner. It is not published externally. `build_packet.py` is the structured authoring source; `issues.json` is its canonical machine-readable dispatch snapshot, and the issue views/coverage table are generated from the same source. Change the authoring source and regenerate to keep them consistent. No calendar schedule is defined.

| Requirement | Scope | Implementing / proving issues |
| --- | --- | --- |
| R01 | General Python runtime; Temporal; separate app Supabase; entity-store isolation; clean break | [MC-P001](issues/MC-P001.md), [MC-P002](issues/MC-P002.md), [MC-P009](issues/MC-P009.md), [MC-F001](issues/MC-F001.md), [MC-F002](issues/MC-F002.md), [MC-F003](issues/MC-F003.md), [MC-F005](issues/MC-F005.md), [MC-D001](issues/MC-D001.md), [MC-I001](issues/MC-I001.md), [MC-R002](issues/MC-R002.md), [MC-R003](issues/MC-R003.md) |
| R02 | Deep Agents plus required Agent Server and frontier route; later required Python Cursor Cloud/direct profiles | [MC-P001](issues/MC-P001.md), [MC-P006](issues/MC-P006.md), [MC-P009](issues/MC-P009.md), [MC-F001](issues/MC-F001.md), [MC-F008](issues/MC-F008.md), [MC-F009](issues/MC-F009.md), [MC-F018](issues/MC-F018.md), [MC-F019](issues/MC-F019.md), [MC-I001](issues/MC-I001.md), [MC-R003](issues/MC-R003.md) |
| R03 | Sandboxed dashboard interview discovers/refines/validates/spawns complete missions | [MC-F012](issues/MC-F012.md), [MC-F015](issues/MC-F015.md), [MC-I002](issues/MC-I002.md) |
| R04 | MCP, CLI and canonical skill discover/configure/spawn/track/intervene/revise | [MC-P001](issues/MC-P001.md), [MC-F011](issues/MC-F011.md), [MC-F012](issues/MC-F012.md), [MC-I002](issues/MC-I002.md), [MC-R002](issues/MC-R002.md) |
| R05 | Search catalog of skills, MCP servers/tools, plugins, spawn contracts and environment config | [MC-F006](issues/MC-F006.md), [MC-D001](issues/MC-D001.md) |
| R06 | Validated authorized materialization including extra processes and memory | [MC-P004](issues/MC-P004.md), [MC-F006](issues/MC-F006.md), [MC-F007](issues/MC-F007.md), [MC-F012](issues/MC-F012.md), [MC-F023](issues/MC-F023.md) |
| R07 | Sourced complete PDF deliverable with inspectable citations and artifact lineage | [MC-F015](issues/MC-F015.md), [MC-F024](issues/MC-F024.md), [MC-I003](issues/MC-I003.md) |
| R08 | Implemented feature patch/tests/review; app/deploy navigation only from authorized receipts | [MC-F015](issues/MC-F015.md), [MC-F018](issues/MC-F018.md), [MC-I003](issues/MC-I003.md) |
| R09 | Experiment reports and reproducible knowledge research/seed runs in both apps | [MC-F022](issues/MC-F022.md), [MC-F025](issues/MC-F025.md), [MC-D001](issues/MC-D001.md), [MC-D002](issues/MC-D002.md), [MC-D003](issues/MC-D003.md), [MC-I003](issues/MC-I003.md) |
| R10 | Durable HITL approval/rejection/feedback, notifications and enforced exact-target review | [MC-F002](issues/MC-F002.md), [MC-F013](issues/MC-F013.md), [MC-F015](issues/MC-F015.md), [MC-F016](issues/MC-F016.md), [MC-F024](issues/MC-F024.md), [MC-I002](issues/MC-I002.md), [MC-I003](issues/MC-I003.md) |
| R11 | Immediate stop request/fence and truthful remote interruption/irreversible effect semantics | [MC-P005](issues/MC-P005.md), [MC-F005](issues/MC-F005.md), [MC-F010](issues/MC-F010.md), [MC-R001](issues/MC-R001.md) |
| R12 | Context injection/queue/pause/resume/cancel/fork/retry with ordered/stale-safe delivery | [MC-P005](issues/MC-P005.md), [MC-P007](issues/MC-P007.md), [MC-F005](issues/MC-F005.md), [MC-F009](issues/MC-F009.md), [MC-F010](issues/MC-F010.md), [MC-F018](issues/MC-F018.md), [MC-R001](issues/MC-R001.md) |
| R13 | Active mission revision, impact/carry-forward and concurrency/ordering | [MC-P005](issues/MC-P005.md), [MC-F004](issues/MC-F004.md), [MC-F010](issues/MC-F010.md), [MC-F013](issues/MC-F013.md), [MC-F020](issues/MC-F020.md) |
| R14 | Stage and Goal Loop iteration state handoff and bounded loops/governors | [MC-P003](issues/MC-P003.md), [MC-P007](issues/MC-P007.md), [MC-F004](issues/MC-F004.md), [MC-F008](issues/MC-F008.md), [MC-F020](issues/MC-F020.md), [MC-I001](issues/MC-I001.md) |
| R15 | Context selection/provenance/budgeting/checkpoints/files/operational memory | [MC-P003](issues/MC-P003.md), [MC-P004](issues/MC-P004.md), [MC-P007](issues/MC-P007.md), [MC-F002](issues/MC-F002.md), [MC-F008](issues/MC-F008.md), [MC-F023](issues/MC-F023.md) |
| R16 | Parent/subagent messages/progress/state, cancellation/recovery and exact middleware | [MC-P003](issues/MC-P003.md), [MC-P004](issues/MC-P004.md), [MC-P005](issues/MC-P005.md), [MC-P006](issues/MC-P006.md), [MC-P007](issues/MC-P007.md), [MC-F002](issues/MC-F002.md), [MC-F008](issues/MC-F008.md), [MC-F009](issues/MC-F009.md), [MC-F020](issues/MC-F020.md), [MC-I001](issues/MC-I001.md) |
| R17 | Public progress without hidden chain of thought; extensions-versus-fork evidence | [MC-P001](issues/MC-P001.md), [MC-P004](issues/MC-P004.md), [MC-F008](issues/MC-F008.md), [MC-F014](issues/MC-F014.md), [MC-D002](issues/MC-D002.md) |
| R18 | Knowledge inspect/ingest/verify/classify/query specialized service/skill ports | [MC-P010](issues/MC-P010.md), [MC-F021](issues/MC-F021.md), [MC-F022](issues/MC-F022.md), [MC-F023](issues/MC-F023.md), [MC-D001](issues/MC-D001.md), [MC-D003](issues/MC-D003.md), [MC-I003](issues/MC-I003.md), [MC-R003](issues/MC-R003.md) |
| R19 | Versioned intent files and deterministic writes, auth/idempotency/partial receipts/approvals | [MC-P002](issues/MC-P002.md), [MC-P010](issues/MC-P010.md), [MC-F021](issues/MC-F021.md), [MC-F022](issues/MC-F022.md), [MC-D003](issues/MC-D003.md), [MC-I003](issues/MC-I003.md) |
| R20 | Sandbox scratch offload via Tavily/Firecrawl CLI; selective consumption and custody | [MC-P004](issues/MC-P004.md), [MC-F007](issues/MC-F007.md) |
| R21 | Workspace persistence/cleanup/isolation/secrets/quotas/processes/reproducibility | [MC-P002](issues/MC-P002.md), [MC-P006](issues/MC-P006.md), [MC-P009](issues/MC-P009.md), [MC-F003](issues/MC-F003.md), [MC-F007](issues/MC-F007.md), [MC-F018](issues/MC-F018.md), [MC-R001](issues/MC-R001.md), [MC-R003](issues/MC-R003.md) |
| R22 | WebSockets durable aggregation/reconnect/replay/backpressure/views/analytics | [MC-F002](issues/MC-F002.md), [MC-F014](issues/MC-F014.md), [MC-F015](issues/MC-F015.md), [MC-I002](issues/MC-I002.md), [MC-R001](issues/MC-R001.md) |
| R23 | Observability/provider cost accounting/bounded verified live budgets | [MC-P001](issues/MC-P001.md), [MC-P009](issues/MC-P009.md), [MC-F005](issues/MC-F005.md), [MC-F014](issues/MC-F014.md), [MC-F019](issues/MC-F019.md), [MC-F025](issues/MC-F025.md), [MC-I001](issues/MC-I001.md), [MC-R001](issues/MC-R001.md), [MC-R003](issues/MC-R003.md) |
| R24 | Retrieval and educational/recommendation experiments with baselines/datasets/evals | [MC-F025](issues/MC-F025.md), [MC-D002](issues/MC-D002.md), [MC-I003](issues/MC-I003.md) |
| R25 | Skill/CLI/MCP/web/mobile release; desktop later | [MC-P008](issues/MC-P008.md), [MC-F011](issues/MC-F011.md), [MC-F015](issues/MC-F015.md), [MC-F016](issues/MC-F016.md), [MC-F017](issues/MC-F017.md), [MC-I002](issues/MC-I002.md), [MC-R002](issues/MC-R002.md), [MC-R003](issues/MC-R003.md) |
| R26 | Required generative UI + MCP Apps/MCP-UI with secure host/fallback prototype | [MC-P008](issues/MC-P008.md), [MC-F016](issues/MC-F016.md), [MC-F017](issues/MC-F017.md), [MC-I002](issues/MC-I002.md), [MC-R002](issues/MC-R002.md), [MC-R003](issues/MC-R003.md) |

## Issue sequence

| ID | Category | Dependencies | Goal |
| --- | --- | --- | --- |
| [MC-P001](issues/MC-P001.md) | foundation | none | Freeze canonical contracts and pinned-source capability baseline |
| [MC-P002](issues/MC-P002.md) | foundation | MC-P001 | Prove two-project schema identity and role isolation |
| [MC-P003](issues/MC-P003.md) | foundation | MC-P001 | Prove Stage and Goal Loop typed state handoff |
| [MC-P004](issues/MC-P004.md) | foundation | MC-P001 | Prove context budget selection and bidirectional subagent projection |
| [MC-P005](issues/MC-P005.md) | foundation | MC-P001 | Prove guarded effects and ordered urgent stop |
| [MC-P006](issues/MC-P006.md) | foundation | MC-P001 | Prove Agent Server lost-launch and native lifecycle contract |
| [MC-P007](issues/MC-P007.md) | foundation | MC-P003, MC-P004, MC-P006 | Prove checkpoint hydration retry fork and replay |
| [MC-P008](issues/MC-P008.md) | foundation | none | Prove secure MCP Apps host and native fallback |
| [MC-P009](issues/MC-P009.md) | foundation | none | Qualify supported runtime storage and AWS topology assumptions |
| [MC-P010](issues/MC-P010.md) | foundation | MC-P001 | Prove knowledge approval and uncertain write receipt semantics |
| [MC-F001](issues/MC-F001.md) | feature | MC-P001 | Transform backend into neutral Python distribution |
| [MC-F002](issues/MC-F002.md) | feature | MC-P002 | Implement common SQL component and new expansion records |
| [MC-F003](issues/MC-F003.md) | feature | MC-F001, MC-F002, MC-P009 | Implement app resolution and pinned installation tooling |
| [MC-F004](issues/MC-F004.md) | feature | MC-F001, MC-P003 | Implement immutable compiler and first recursive workflow vertical |
| [MC-F005](issues/MC-F005.md) | feature | MC-F003, MC-F004, MC-P005 | Implement transactional intents budgets outbox and Temporal kernel |
| [MC-F006](issues/MC-F006.md) | feature | MC-F003, MC-P004 | Implement scoped capability search and exact configuration assembly |
| [MC-F007](issues/MC-F007.md) | feature | MC-F005, MC-F006, MC-P004 | Implement workspace custody offload and process leases |
| [MC-F008](issues/MC-F008.md) | feature | MC-F007, MC-P003, MC-P004, MC-P005 | Implement governed Deep Agents middleware and state channels |
| [MC-F009](issues/MC-F009.md) | feature | MC-F008, MC-P006, MC-P009 | Implement required Agent Server and subordinate mailbox |
| [MC-F010](issues/MC-F010.md) | feature | MC-F005, MC-F009, MC-P007 | Implement full control recovery and active revision transitions |
| [MC-F011](issues/MC-F011.md) | feature | MC-F006, MC-F010 | Ship canonical public API MCP CLI and skill parity |
| [MC-F012](issues/MC-F012.md) | feature | MC-F007, MC-F008, MC-F011 | Implement sandbox interview and draft-to-workflow coordinator |
| [MC-F013](issues/MC-F013.md) | feature | MC-F005, MC-F010 | Implement durable Human Tasks feedback and notification intents |
| [MC-F014](issues/MC-F014.md) | feature | MC-F005, MC-P005 | Implement durable event aggregation WebSocket SSE and views |
| [MC-F015](issues/MC-F015.md) | feature | MC-F012, MC-F013, MC-F014 | Integrate both web dashboards and navigable artifacts decisions |
| [MC-F016](issues/MC-F016.md) | feature | MC-F011, MC-F013, MC-F014, MC-P008 | Build mobile client and durable fallback review controls |
| [MC-F017](issues/MC-F017.md) | feature | MC-F011, MC-F013, MC-P008 | Implement generative UI and standard MCP Apps resources |
| [MC-F018](issues/MC-F018.md) | feature | MC-F007, MC-F010, MC-P001 | Qualify and implement Python Cursor Cloud harness |
| [MC-F019](issues/MC-F019.md) | feature | MC-F005, MC-F008 | Implement bounded frontier routes and cost accounting |
| [MC-F020](issues/MC-F020.md) | feature | MC-F004, MC-F009, MC-F010 | Complete Swarm Optimizer Child Missions and recursive semantics |
| [MC-F021](issues/MC-F021.md) | feature | MC-P010, MC-F011, MC-F013 | Publish knowledge operation and deterministic PostgreSQL intent adapter |
| [MC-F022](issues/MC-F022.md) | feature | MC-P010, MC-F011, MC-F013 | Implement governed Neo4j intent writer and receipt marker |
| [MC-F023](issues/MC-F023.md) | feature | MC-P004, MC-F006, MC-F021 | Implement advisory memory admission and recall policy |
| [MC-F024](issues/MC-F024.md) | feature | MC-F007, MC-F013, MC-F021 | Implement sourced PDF production validation and revision workflow |
| [MC-F025](issues/MC-F025.md) | feature | MC-F020, MC-F021, MC-F019, MC-D002 | Implement reproducible retrieval and recommendation experiment runner |
| [MC-D001](issues/MC-D001.md) | data | MC-F003, MC-F006 | Prepare separate app seed catalogs policies and endpoint bindings |
| [MC-D002](issues/MC-D002.md) | data | none | Prepare licensed captured benchmark datasets and split manifest |
| [MC-D003](issues/MC-D003.md) | data | MC-F021, MC-F022, MC-D001 | Close knowledge-service availability and source readiness |
| [MC-I001](issues/MC-I001.md) | integration | MC-F009, MC-F010, MC-F019, MC-D001, MC-F011 | Qualify Deep Agents Agent Server parity in both installations |
| [MC-I002](issues/MC-I002.md) | integration | MC-F015, MC-F016, MC-F017, MC-F011 | Qualify all public surfaces and UI compatibility |
| [MC-I003](issues/MC-I003.md) | integration | MC-I001, MC-F018, MC-F024, MC-F025, MC-D003, MC-F023 | Qualify PDF coding seed and research experiment scenarios |
| [MC-R001](issues/MC-R001.md) | release | MC-I001, MC-I002, MC-I003, MC-P009 | Qualify security load failure recovery and accounting |
| [MC-R002](issues/MC-R002.md) | release | MC-R001 | Package release artifacts skill CLI MCP web mobile and schema |
| [MC-R003](issues/MC-R003.md) | release | MC-R002 | Prepare and execute separately authorized production qualification |
