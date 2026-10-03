"""AST scan for digests built from a JSON-mode dump of a contract (RRM-015).

`model_dump(mode="json")` lists every `set`/`frozenset` in per-process iteration order, so a
digest or exact-equality proof built from it is not an identity. This scanner finds every
JSON-mode dump (`model_dump(mode="json", ...)` or `model_dump_json(...)`) in runtime,
optional integration, or experiment sources whose
value flows into a digest-like sink, directly or through a local variable.
"""

from __future__ import annotations

import ast
import re
from dataclasses import dataclass
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SOURCE_ROOTS = (
    PROJECT_ROOT / "src" / "mission_control",
    PROJECT_ROOT / "integrations" / "biotech" / "src" / "biotech_mission_adapters",
    PROJECT_ROOT / "experiments",
)

# Callables that turn a payload into an identity: any name containing one of these tokens.
_SINK_NAME = re.compile(
    r"(sha256|digest|fingerprint|canonical_json|content_hash|_hash$|^hash$|reservation_token)",
    re.IGNORECASE,
)
# Modules that are out of this mission's scope (RRM-013 owns them) and are reported, not scanned.
EXCLUDED_PARTS = (
    "src/mission_control/adapters/agent_server/",
    "src/mission_control/application/subordinates/",
)
EXCLUDED_FILES = ("src/mission_control/adapters/deep_agents/async_subagents.py",)


@dataclass(frozen=True)
class DigestSite:
    path: str
    function: str
    receiver: str
    line: int
    call: str

    @property
    def key(self) -> tuple[str, str, str]:
        """Audit identity: the exact dump expression, so changing it forces a re-audit."""

        return (self.path, self.function, self.call)


def _classify_dump(node: ast.AST) -> tuple[ast.Call, str, str] | None:
    """Return (call, receiver text, kind) for a JSON-shaped dump; kind is json or dynamic.

    Detected: `x.model_dump_json()`, `x.model_dump(mode="json")`, `adapter.dump_python(x,
    mode="json")` and `to_jsonable_python(x)`. A `mode=` that is not a literal, or any `**kwargs`,
    is `dynamic`: the scan cannot tell which mode runs, so it is reported as unverifiable.
    """

    if not isinstance(node, ast.Call):
        return None
    func = node.func
    if isinstance(func, ast.Name | ast.Attribute) and (
        (func.id if isinstance(func, ast.Name) else func.attr) == "to_jsonable_python"
    ):
        return node, ast.unparse(node.args[0]) if node.args else "", "json"
    if not isinstance(func, ast.Attribute):
        return None
    if func.attr == "model_dump_json":
        return node, ast.unparse(func.value), "json"
    if func.attr not in {"model_dump", "dump_python"}:
        return None
    receiver = ast.unparse(func.value)
    if func.attr == "dump_python" and node.args:
        receiver = ast.unparse(node.args[0])
    if any(keyword.arg is None for keyword in node.keywords):
        return node, receiver, "dynamic"
    for keyword in node.keywords:
        if keyword.arg == "mode":
            if not isinstance(keyword.value, ast.Constant):
                return node, receiver, "dynamic"
            if keyword.value.value == "json":
                return node, receiver, "json"
    return None


def _call_name(node: ast.Call) -> str:
    func = node.func
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute):
        return func.attr
    return ""


def _is_sink(node: ast.AST) -> bool:
    return isinstance(node, ast.Call) and bool(_SINK_NAME.search(_call_name(node)))


class _Visitor(ast.NodeVisitor):
    def __init__(self, path: str) -> None:
        self.path = path
        self.scopes: list[str] = []
        self.sites: list[DigestSite] = []
        self.unverifiable: list[DigestSite] = []

    def _visit_scope(self, node: ast.AST, name: str) -> None:
        self.scopes.append(name)
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
            self._scan_function(node)
        self.generic_visit(node)
        self.scopes.pop()

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        self._visit_scope(node, node.name)

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self._visit_scope(node, node.name)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self._visit_scope(node, node.name)

    def _scan_function(self, function: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        parents: dict[ast.AST, ast.AST] = {}
        for parent in ast.walk(function):
            for child in ast.iter_child_nodes(parent):
                parents[child] = parent
        sink_arg_names: set[str] = set()
        for node in ast.walk(function):
            if _is_sink(node):
                assert isinstance(node, ast.Call)
                for argument in (*node.args, *(k.value for k in node.keywords)):
                    for inner in ast.walk(argument):
                        if isinstance(inner, ast.Name):
                            sink_arg_names.add(inner.id)
        qualname = ".".join(self.scopes)
        for node in ast.walk(function):
            classified = _classify_dump(node)
            if classified is None:
                continue
            dump, receiver, kind = classified
            site = DigestSite(
                path=self.path,
                function=qualname,
                receiver=receiver,
                line=dump.lineno,
                call=ast.unparse(dump),
            )
            if kind == "dynamic":
                self.unverifiable.append(site)
                continue
            flows = False
            cursor: ast.AST | None = dump
            while cursor is not None and cursor is not function:
                parent = parents.get(cursor)
                if parent is not None and _is_sink(parent) and cursor is not parent.func:
                    flows = True
                    break
                if isinstance(parent, ast.Assign | ast.AnnAssign | ast.NamedExpr):
                    targets = parent.targets if isinstance(parent, ast.Assign) else [parent.target]
                    names = {
                        t.id
                        for target in targets
                        for t in ast.walk(target)
                        if isinstance(t, ast.Name)
                    }
                    if names & sink_arg_names:
                        flows = True
                    break
                if isinstance(parent, ast.Return):
                    # `return self.model_dump(mode="json")` inside a digest-named function.
                    flows = bool(_SINK_NAME.search(function.name))
                    break
                if isinstance(parent, ast.stmt):
                    break
                cursor = parent
            if flows:
                self.sites.append(site)


def _scan(root: Path, *, repo: Path | None = None) -> list[tuple[str, _Visitor]]:
    results: list[tuple[str, _Visitor]] = []
    repo = repo or root.parent
    files = sorted(root.rglob("*.py"))
    assert files, f"Digest guard source scan is empty: {root}"
    for file in files:
        relative = file.relative_to(repo).as_posix()
        visitor = _Visitor(relative)
        visitor.visit(ast.parse(file.read_text(encoding="utf-8"), filename=str(file)))
        results.append((relative, visitor))
    return results


def _excluded(relative: str) -> bool:
    return relative in EXCLUDED_FILES or any(part in relative for part in EXCLUDED_PARTS)


def scan_digest_sites(root: Path | None = None) -> tuple[list[DigestSite], list[DigestSite]]:
    """Return (in-scope sites, sites in excluded RRM-013 areas)."""

    in_scope: list[DigestSite] = []
    excluded: list[DigestSite] = []
    scanned = (
        _scan(root)
        if root is not None
        else [entry for source in SOURCE_ROOTS for entry in _scan(source, repo=PROJECT_ROOT)]
    )
    for relative, visitor in scanned:
        (excluded if _excluded(relative) else in_scope).extend(visitor.sites)
    return in_scope, excluded


def scan_unverifiable_dumps(root: Path | None = None) -> list[DigestSite]:
    """JSON-capable dumps whose mode is not a literal or that pass `**kwargs` (any function)."""

    scanned = (
        _scan(root)
        if root is not None
        else [entry for source in SOURCE_ROOTS for entry in _scan(source, repo=PROJECT_ROOT)]
    )
    return [site for _relative, visitor in scanned for site in visitor.unverifiable]
