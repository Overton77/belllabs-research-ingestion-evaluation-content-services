import ast
import hashlib
import json
import re
from pathlib import Path

from pydantic import ValidationError

HERE = Path(__file__).resolve().parent
ROOT = Path(r"C:\Users\Pinda\Proyectos\aiengineer\ai-engineer-meta\ai-engineer-architecture\specs")
pack = ROOT / "general-mission-control"
files = [*list(pack.glob("*.md")), HERE / "README.md"]
errors = []
links = 0
for p in files:
    content = p.read_text(encoding="utf-8")
    if len(re.findall(r"^```", content, re.MULTILINE)) % 2:
        errors.append(f"unbalanced code fence: {p}")
    for target in re.findall(r"\]\(([^)]+)\)", content):
        if "://" in target or target.startswith("#"):
            continue
        target = target.split("#")[0]
        if not (p.parent / target).exists():
            errors.append(f"broken local link {p.name}: {target}")
        links += 1
    for code in re.findall(r"```python\n(.*?)\n```", content, re.DOTALL):
        ast.parse(code)
    for code in re.findall(r"```json\n(.*?)\n```", content, re.DOTALL):
        json.loads(code)
    for phrase in [
        "user chooses `MISSION_CONTROL_ROOT`",
        "If the user chooses the biotech",
        "no mandatory Cursor/Eve",
        "Cursor and Eve are not prerequisites",
        "Provider adapters beyond Deep Agents are later",
    ]:
        if phrase in content:
            errors.append(f"obsolete contract {p.name}: {phrase}")

# Validate Python contract seed with installed Pydantic, including rejection cases.
code = re.search(
    r"```python\n(.*?)\n```", (pack / "RUNTIME-CONTRACTS.md").read_text(encoding="utf-8"), re.DOTALL
).group(1)
ns = {}
exec(compile(code, "spec_contract_seed", "exec"), ns)  # noqa: S102 - executes the trusted local specification's contract fixture
RunStart = ns["RunStart"]
data = {
    "schema_version": "mc.run_start.v1",
    "request_id": "00000000-0000-4000-8000-000000000001",
    "expected_version": 1,
    "revision_id": "00000000-0000-4000-8000-000000000002",
    "input_manifest_ref": "fixture:manifest",
    "input_digest": "sha256:" + "a" * 64,
}
value = RunStart.model_validate(data)
RunStart.model_json_schema()
for invalid in [
    dict(data, unexpected=True),
    dict(data, input_digest="bad"),
    dict(data, expected_version=0),
]:
    try:
        RunStart.model_validate(invalid)
        errors.append("contract accepted invalid fixture")
    except ValidationError:
        pass
Fork = ns["ForkRequest"]
Fork.model_json_schema()
Fork.model_validate(
    {
        "schema_version": "mc.fork.v1",
        "request_id": data["request_id"],
        "expected_version": 1,
        "expected_generation": 1,
        "checkpoint_id": data["revision_id"],
        "checkpoint_digest": data["input_digest"],
        "target_kind": "run",
        "reason": "qualification fixture",
        "budget_profile_ref": "fixture:budget",
    }
)
if errors:
    print("\n".join(errors))
    raise SystemExit(1)
print(
    f"PASS: {len(files)} documents; {links} local links; balanced fences; Python AST/JSON; Pydantic schema and valid/invalid fixtures; obsolete-contract scan."
)
for p in files:
    print(f"{p.name}: {hashlib.sha256(p.read_bytes()).hexdigest()}")
