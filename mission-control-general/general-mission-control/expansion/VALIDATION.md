---
type: Specification Annex
title: Packet validation and limits
description: "This is documentation/schema/backlog validation. It does not accept runtime, live provider or deployed capability gates."
tags: [mission-control, spec, expansion]
---
# Packet validation and limits

This is documentation/schema/backlog validation. It does not accept runtime, live provider or deployed capability gates.

Checks: all canonical/expansion Markdown links and fences; Python AST and JSON examples; JSON Schema Draft 2020-12 correctness/reference resolution; valid context/message/review/event-frame fixtures; negative unknown fields, stale generation shape and feedback-required fixtures; unique stable issues, required issue fields/generated view consistency; dependency existence/acyclicity/readiness; complete R01-R26 issue coverage; event and knowledge schema naming parity. Exact results are saved in [validation-results.json](validation-results.json) by [validate_packet.py](validate_packet.py).

Final recorded documentation result: 78 documents, 498 local links, 23 schema definitions, four valid/negative fixture families, 44 acyclic issues and all 26 requirements covered. The older seven-file AI Engineer pack now has bounded historical supersession notices pointing here; its prior content was preserved. Independent reviews resolved fractional-rate representation, actual topology qualification versus fake proof, review/UI literals, canonical event fields, and explicit interview-owned budget/effect persistence.

Independent read-only investigators inspected current production/proof source and current official Deep Agents, Cursor, MCP Apps/MCP-UI, FastAPI, AWS, LangSmith and related documentation. Runtime and UI reviewers checked final semantics/schema alignment. Source findings and gaps are in TECHNOLOGY-EVIDENCE. Existing passed evidence is attributed; no paid/runtime/DB/deploy proof test is rerun here.

All planning/spec files are local. Existing content received bounded current-scope amendments; no repository reset, broad rename, credentials, memories, external publication, commits/pushes or metered calls occurred. Proposed runtime/client/package paths are future issue ownership, not implementation changes. Generated artifacts are documentation/schema/dispatch artifacts, not a runtime engine.

Reproduce with the existing backend `.venv/Scripts/python.exe general-mission-control/expansion/validate_packet.py` from the Mission Control workspace. `build_packet.py` regenerates issue/schema views from their structured planning source. Do not edit generated views alone; change the authoring source and regenerate.

Remaining true runtime qualification blockers are explicit: Agent Server supported app-local persistence topology, atomic lost-launch/control/usage behavior, context projection/hydration, durable checkpoints, Cursor pin/bridge, domain transport/Neo4j marker, UI/mobile hosts and app identity/configuration. Production gates additionally need concrete targets, backup plans, grants and finite current-rate budgets. None blocks this completed specification/issue packet.
