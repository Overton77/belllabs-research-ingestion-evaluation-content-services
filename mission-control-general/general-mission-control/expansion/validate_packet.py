"""Offline documentation, schema, issue DAG and coverage checks. No runtime probes."""

import ast
import copy
import json
import re
from collections import Counter
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker

ROOT = Path(__file__).resolve().parent
pack = ROOT.parent
errors = []
markdown = [
    *list(pack.rglob("*.md")),
    pack.parent / "README.md",
    pack.parent / "index.md",
    pack.parent / "workflow-types" / "09-EVENTS_COMMANDS_AND_STREAMS.md",
]
previous = Path(
    r"C:\Users\Pinda\Proyectos\aiengineer\ai-engineer-meta\ai-engineer-architecture\specs\general-mission-control"
)
markdown += list(previous.glob("*.md"))
links = 0
for p in markdown:
    text = p.read_text(encoding="utf-8")
    if len(re.findall(r"^```", text, re.MULTILINE)) % 2:
        errors.append(f"unbalanced fence: {p}")
    for target in re.findall(r"\]\(([^)]+)\)", text):
        target = target.strip("<>")
        if "://" in target or target.startswith("#"):
            continue
        target = target.split("#")[0]
        if not (p.parent / target).exists():
            errors.append(f"broken link {p} -> {target}")
        links += 1
    for code in re.findall(r"```python\n(.*?)\n```", text, re.DOTALL):
        ast.parse(code)
    for code in re.findall(r"```json\n(.*?)\n```", text, re.DOTALL):
        json.loads(code)
for p in ROOT.glob("*.py"):
    ast.parse(p.read_text(encoding="utf-8"))
schemas = json.loads((ROOT / "schemas.json").read_text(encoding="utf-8"))
Draft202012Validator.check_schema(schemas)
for name, definition in schemas["$defs"].items():
    Draft202012Validator.check_schema(definition)

    def inspect(node, definition_name=name):
        if isinstance(node, dict):
            if (
                "$ref" in node
                and node["$ref"].startswith("#/$defs/")
                and node["$ref"][8:] not in schemas["$defs"]
            ):
                errors.append(f"unknown schema ref {definition_name}: {node['$ref']}")
            for child in node.values():
                inspect(child)
        elif isinstance(node, list):
            for child in node:
                inspect(child)

    inspect(definition)

fixture_id = "00000000-0000-4000-8000-000000000001"
scope = {"installation_id": fixture_id, "application_id": "biotech", "tenant_id": fixture_id}
artifact = {
    "resource_id": fixture_id,
    "scope": scope,
    "issuer": "fixture",
    "schema_ref": "fixture:payload.v1",
    "digest": "sha256:" + "a" * 64,
}
context = {
    "schema_version": "mc.context_selection.v1",
    "selection_id": fixture_id,
    "request_id": fixture_id,
    "scope": scope,
    "target_ref": "fixture:attempt",
    "purpose": "proof",
    "policy_ref": "fixture:policy",
    "tokenizer_ref": "fixture:tokenizer",
    "candidate_capture_refs": [artifact],
    "selected": [
        {"artifact": artifact, "locator": "page:1", "token_bound": 20, "trust": "admitted_evidence"}
    ],
    "omitted": [],
    "max_input_tokens": 100,
    "total_token_bound": 20,
    "file_manifest_ref": artifact,
    "grants_snapshot_ref": "fixture:grants",
}
message = {
    "schema_version": "mc.message.v1",
    "request_id": fixture_id,
    "scope": scope,
    "target_ref": "fixture:child",
    "target_generation": 1,
    "sender_ref": "fixture:parent",
    "client_message_id": fixture_id,
    "kind": "instruction",
    "boundary": "next_turn",
    "content": artifact,
    "deadline": None,
}
review = {
    "schema_version": "mc.review_decision.v1",
    "request_id": fixture_id,
    "human_task_id": fixture_id,
    "expected_task_version": 1,
    "target_digest": artifact["digest"],
    "decision": "request_changes",
    "feedback_ref": "fixture:feedback",
    "selected_evidence_refs": [],
}
event = {
    "schema_version": "mc.event.v1",
    "event_id": fixture_id,
    "scope": scope,
    "mission_id": fixture_id,
    "seq": 1,
    "ledger_commit_id": fixture_id,
    "occurred_at": "2026-10-03T00:00:00Z",
    "recorded_at": "2026-10-03T00:00:00Z",
    "event_type": "human_task.opened",
    "event_version": 1,
    "execution": {},
    "source": {"kind": "mission_control"},
    "payload_ref": artifact,
}
fixtures = {
    "ContextSelection": context,
    "Message": message,
    "ReviewDecision": review,
    "StreamFrame": {"schema": "mc.stream_frame.v1", "type": "event", "event": event},
}
for name, value in fixtures.items():
    v = Draft202012Validator(
        {"$ref": f"#/$defs/{name}", "$defs": schemas["$defs"]}, format_checker=FormatChecker()
    )
    v.validate(value)
    bad = copy.deepcopy(value)
    bad["unknown"] = True
    if v.is_valid(bad):
        errors.append(f"unknown field accepted: {name}")
bad = copy.deepcopy(message)
bad["target_generation"] = 0
if Draft202012Validator({"$ref": "#/$defs/Message", "$defs": schemas["$defs"]}).is_valid(bad):
    errors.append("invalid generation accepted")
bad = copy.deepcopy(review)
del bad["feedback_ref"]
if Draft202012Validator({"$ref": "#/$defs/ReviewDecision", "$defs": schemas["$defs"]}).is_valid(
    bad
):
    errors.append("request_changes without feedback accepted")

packet = json.loads((ROOT / "issues.json").read_text(encoding="utf-8"))
issues = packet["issues"]
by_id = {row["id"]: row for row in issues}
if len(by_id) != len(issues):
    errors.append("duplicate issue ID")
required = {
    "goal",
    "input_contracts",
    "output_contracts",
    "ownership_paths",
    "acceptance_tests",
    "proof_artifacts",
    "compute_profile",
    "concurrency",
    "stop_conditions",
    "non_goals",
}
covered = set()
for row in issues:
    if not required <= row.keys() or any(not row[k] for k in required):
        errors.append(f"incomplete issue {row['id']}")
    for dep in row["dependencies"]:
        if dep not in by_id or dep == row["id"]:
            errors.append(f"invalid dependency {row['id']}: {dep}")
    covered.update(row["requirements"])
    view = ROOT / "issues" / f"{row['id']}.md"
    if not view.exists():
        errors.append(f"missing issue view {row['id']}")
    else:
        body = view.read_text(encoding="utf-8")
        for value in [
            row["title"],
            row["repository"],
            *row["dependencies"],
            *row["acceptance_tests"],
            *row["requirements"],
        ]:
            if value not in body:
                errors.append(f"stale generated view {row['id']}: {value}")
    if row["category"] in ["feature", "integration", "release"] and not row["dependencies"]:
        errors.append(f"ungated implementation issue {row['id']}")
if covered != set(packet["requirements"]):
    errors.append(f"coverage mismatch: {covered.symmetric_difference(packet['requirements'])}")
remaining = set(by_id)
done = set()
waves = []
while remaining:
    ready = sorted(k for k in remaining if set(by_id[k]["dependencies"]) <= done)
    if not ready:
        errors.append("issue DAG contains cycle")
        break
    waves.append(ready)
    done.update(ready)
    remaining -= set(ready)
coverage = (ROOT / "REQUIREMENTS.md").read_text(encoding="utf-8")
for key in packet["requirements"]:
    if key not in coverage:
        errors.append(f"missing matrix row {key}")
for p in (ROOT / "issues").glob("*.md"):
    if p.stem not in by_id:
        errors.append(f"orphan issue file {p.name}")
event_doc = (pack.parent / "workflow-types" / "09-EVENTS_COMMANDS_AND_STREAMS.md").read_text(
    encoding="utf-8"
)
for field in ["schema_version", "ledger_commit_id", "event_type", "event_version", "execution:"]:
    if field not in event_doc:
        errors.append(f"event contract missing {field}")
knowledge = (ROOT / "KNOWLEDGE-SERVICES.md").read_text(encoding="utf-8")
if "mc.knowledge_request.v1" not in knowledge:
    errors.append("knowledge schema mismatch")
if (
    schemas["$defs"]["KnowledgeEnvelope"]["properties"]["schema_version"]["const"]
    != "mc.knowledge_request.v1"
):
    errors.append("knowledge schema mismatch")
if errors:
    print("\n".join(errors))
    raise SystemExit(1)
summary = {
    "documents": len(markdown),
    "local_links": links,
    "issues": len(issues),
    "requirements": len(packet["requirements"]),
    "schema_definitions": len(schemas["$defs"]),
    "fixture_sets": len(fixtures),
    "categories": dict(Counter(x["category"] for x in issues)),
    "ready_frontier": waves[0],
    "dependency_waves": waves,
    "status": "passed_documentation_checks",
    "runtime_tests_run": False,
    "metered_calls": 0,
}
(ROOT / "validation-results.json").write_text(
    json.dumps(summary, indent=2) + "\n", encoding="utf-8"
)
print(json.dumps(summary, indent=2))
