"""Executable ownership rules for the single general Mission Control distribution.

These inspect every source module, including function-local and TYPE_CHECKING imports.
They fail on an empty scan, so moving a directory cannot silently disable the gates.
"""

from __future__ import annotations

import ast
import importlib.util
import json
import subprocess
import tomllib
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
PACKAGE = ROOT / "src" / "mission_control"
PROVIDER_OR_TRANSPORT_SDKS = {
    "asyncpg",
    "psycopg",
    "psycopg_pool",
    "pymongo",
    "beanie",
    "motor",
    "temporalio",
    "fastapi",
    "starlette",
    "fastmcp",
    "mcp",
    "uvicorn",
    "langchain",
    "langchain_core",
    "langchain_openai",
    "langchain_anthropic",
    "langchain_mcp_adapters",
    "langgraph",
    "langgraph_sdk",
    "langgraph_api",
    "deepagents",
    "openai",
    "anthropic",
    "supabase",
    "neo4j",
    "redis",
    "httpx",
    "aiohttp",
    "aioboto3",
    "boto3",
    "botocore",
    "langsmith",
    "socketio",
    "requests",
}
EXTERNAL_APPLICATION_PACKAGES = {
    "app",
    "biotech_mission_adapters",
    "biotech_postgres_db_contract",
    "ai_engineer",
    "aiengineer",
    "experiments",
}


def sources(directory: Path = PACKAGE) -> list[Path]:
    files = sorted(directory.rglob("*.py"))
    assert files, f"Architecture scan found no Python sources: {directory}"
    return files


def imports(path: Path) -> list[tuple[int, str]]:
    module = ".".join(path.relative_to(PACKAGE.parent).with_suffix("").parts)
    package = (
        module.removesuffix(".__init__") if path.name == "__init__.py" else module.rsplit(".", 1)[0]
    )
    result = []
    for node in ast.walk(ast.parse(path.read_text(encoding="utf-8-sig"), filename=str(path))):
        if isinstance(node, ast.Import):
            result.extend((node.lineno, alias.name) for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            name = node.module or ""
            if node.level:
                name = importlib.util.resolve_name("." * node.level + name, package)
            result.append((node.lineno, name))
        elif isinstance(node, ast.Call) and node.args:
            # Literal dynamic imports must obey the same rules as ordinary imports.
            name = (
                node.func.id
                if isinstance(node.func, ast.Name)
                else node.func.attr
                if isinstance(node.func, ast.Attribute)
                else ""
            )
            if name in {"import_module", "__import__"}:
                argument = node.args[0]
                if isinstance(argument, ast.Constant) and isinstance(argument.value, str):
                    result.append((node.lineno, argument.value))
    return result


def violations(directory: Path, forbidden_layers: set[str]) -> list[str]:
    found = []
    for path in sources(directory):
        for line, module in imports(path):
            parts = module.split(".")
            if parts[0] in PROVIDER_OR_TRANSPORT_SDKS or (
                parts[0] == "mission_control" and len(parts) > 1 and parts[1] in forbidden_layers
            ):
                found.append(f"{path.relative_to(ROOT)}:{line}: {module}")
    return found


@pytest.mark.parametrize("area", ["contracts", "domain"])
def test_contracts_and_domain_are_framework_neutral(area):
    assert not (
        found := violations(PACKAGE / area, {"application", "adapters", "interfaces", "bootstrap"})
    ), "\n".join(found)


def test_application_depends_on_ports_not_concrete_implementations():
    assert not (
        found := violations(PACKAGE / "application", {"adapters", "interfaces", "bootstrap"})
    ), "\n".join(found)


def test_core_has_no_legacy_or_application_owned_imports():
    found = [
        f"{path.relative_to(ROOT)}:{line}: {module}"
        for path in sources()
        for line, module in imports(path)
        if module.split(".")[0] in EXTERNAL_APPLICATION_PACKAGES
    ]
    assert not found, "General runtime imports application or legacy code:\n" + "\n".join(found)
    # Personal untracked experiments are not part of the distribution or our cleanup scope.
    tracked = (
        subprocess.run(
            [
                "git",
                "-c",
                f"safe.directory={ROOT.as_posix()}",
                "ls-files",
                "-z",
                "--",
                "app",
                "mission_control",
            ],
            cwd=ROOT,
            check=True,
            capture_output=True,
        )
        .stdout.decode()
        .split("\0")
    )
    duplicates = [name for name in tracked if name.endswith(".py") and (ROOT / name).is_file()]
    assert not duplicates, f"Tracked second Python implementation remains: {duplicates}"


def test_all_internal_import_modules_exist_in_the_single_source_package():
    missing = []
    for path in sources():
        for line, module in imports(path):
            if module == "mission_control" or not module.startswith("mission_control."):
                continue
            target = PACKAGE.parent.joinpath(*module.split("."))
            if not target.with_suffix(".py").is_file() and not target.is_dir():
                missing.append(f"{path.relative_to(ROOT)}:{line}: {module}")
    assert not missing, "Imports missing from installed source package:\n" + "\n".join(missing)


def test_temporal_workflow_sources_do_not_import_application_or_provider_io():
    directory = PACKAGE / "adapters" / "temporal"
    workflows = []
    forbidden = PROVIDER_OR_TRANSPORT_SDKS - {"temporalio"}
    for path in sources(directory):
        tree = ast.parse(path.read_text(encoding="utf-8-sig"))
        aliases = {
            alias.asname or alias.name: f"{node.module}.{alias.name}"
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom)
            for alias in node.names
            if node.module == "temporalio"
        }
        if not any(
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "defn"
            and isinstance(node.func.value, ast.Name)
            and aliases.get(node.func.value.id) == "temporalio.workflow"
            for node in ast.walk(tree)
        ):
            continue
        workflows.append(path)
        bad = [
            module
            for _line, module in imports(path)
            if module.split(".")[0] in forbidden
            or module.startswith(
                (
                    "mission_control.application.",
                    "mission_control.interfaces.",
                    "mission_control.bootstrap.",
                    "mission_control.adapters.postgres.",
                    "mission_control.adapters.deep_agents.",
                )
            )
        ]
        assert not bad, f"Workflow imports provider/business IO: {path}: {bad}"
    assert workflows, "No Temporal workflow definitions found by the architecture gate"


def test_distribution_and_configured_entrypoints_use_only_the_new_namespace():
    project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    assert project["project"]["name"] == "mission-control"
    assert project["tool"]["hatch"]["build"]["targets"]["wheel"]["packages"] == [
        "src/mission_control"
    ]
    targets = list(project["project"].get("scripts", {}).values())
    configs = list(ROOT.glob("langgraph*.json")) + list((ROOT / "agent_server").glob("*.json"))
    assert configs, "Agent Server deployment configuration disappeared"
    for path in configs:
        config = json.loads(path.read_text(encoding="utf-8"))
        targets.extend(config.get("graphs", {}).values())
        for section, key in (("auth", "path"), ("http", "app")):
            if value := config.get(section, {}).get(key):
                targets.append(value)
    assert targets
    for target in targets:
        module, _separator, _symbol = target.partition(":")
        assert module.startswith("mission_control."), f"Stale configured entrypoint: {target}"
        source = PACKAGE.parent.joinpath(*module.split(".")).with_suffix(".py")
        assert source.is_file(), f"Configured entrypoint is absent from the wheel source: {target}"


def test_no_mongo_driver_can_reenter_the_runtime():
    roots = (PACKAGE, ROOT / "integrations" / "biotech" / "src" / "biotech_mission_adapters")
    failures = []
    for root in roots:
        for path in sources(root):
            tree = ast.parse(path.read_text(encoding="utf-8-sig"))
            for node in ast.walk(tree):
                names = []
                if isinstance(node, ast.Import):
                    names = [alias.name for alias in node.names]
                elif isinstance(node, ast.ImportFrom):
                    names = [node.module or ""]
                if any(name.split(".")[0] in {"beanie", "pymongo", "motor"} for name in names):
                    failures.append(f"{path.relative_to(ROOT)}:{node.lineno}")
    assert not failures, "Mongo persistence imports returned: " + ", ".join(failures)


def test_core_does_not_install_legacy_module_aliases():
    for path in sources():
        tree = ast.parse(path.read_text(encoding="utf-8-sig"))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Subscript):
                continue
            if not isinstance(node.value, ast.Attribute) or node.value.attr != "modules":
                continue
            if isinstance(node.slice, ast.Constant) and isinstance(node.slice.value, str):
                assert node.slice.value != "app" and not node.slice.value.startswith("app."), (
                    f"Legacy module alias access: {path}:{node.lineno}"
                )


def test_runtime_resource_scans_have_real_inputs():
    migrations = PACKAGE / "adapters" / "postgres" / "migrations"
    assert list(migrations.glob("*.sql")), "Runtime migration source scan became empty"
    assert not (PACKAGE / "domain" / "coordinator" / "reviewed_payloads").exists()
    payloads = (
        ROOT
        / "integrations"
        / "biotech"
        / "src"
        / "biotech_mission_adapters"
        / "resources"
        / "reviewed_payloads"
    )
    assert list((payloads.parent.parent / "migrations").glob("*.sql"))
    documents = list(payloads.glob("*.json"))
    assert documents, "Reviewed capability payloads disappeared"
    for document in documents:
        assert json.loads(document.read_text(encoding="utf-8"))
