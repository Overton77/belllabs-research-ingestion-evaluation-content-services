"""AST scan for digests built from a JSON-mode dump of a contract (RRM-015).

`model_dump(mode="json")` lists every `set`/`frozenset` in per-process iteration order, so a
digest or exact-equality proof built from it is not an identity. This scanner finds every
JSON-mode dump (`model_dump(mode="json", ...)` or `model_dump_json(...)`) in `app/` whose
value flows into a digest-like sink, directly or through a local variable.
"""

from __future__ import annotations

import ast
import re
from dataclasses import dataclass
from pathlib import Path

APP_ROOT = Path(__file__).resolve().parents[2] / "app"

# Callables that turn a payload into an identity: any name containing one of these tokens.
_SINK_NAME = re.compile(
    r"(sha256|digest|fingerprint|canonical_json|content_hash|_hash$|^hash$|reservation_token)",
    re.IGNORECASE,
)
# Modules that are out of this mission's scope (RRM-013 owns them) and are reported, not scanned.
EXCLUDED_PARTS = ("app/agent_server/", "app/application/async_subagents/")
EXCLUDED_FILES = ("app/integrations/agents/deep_agents/async_subagents.py",)


@dataclass(frozen=True)
class DigestSite:
    path: str
    function: str
    receiver: str
    line: int

    @property
    def key(self) -> tuple[str, str, str]:
        return (self.path, self.function, self.receiver)


def _is_json_dump(node: ast.AST) -> ast.Call | None:
    if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
        return None
    if node.func.attr == "model_dump_json":
        return node
    if node.func.attr == "model_dump":
        for keyword in node.keywords:
            if (
                keyword.arg == "mode"
                and isinstance(keyword.value, ast.Constant)
                and keyword.value.value == "json"
            ):
                return node
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
            dump = _is_json_dump(node)
            if dump is None:
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
                assert isinstance(dump.func, ast.Attribute)
                self.sites.append(
                    DigestSite(
                        path=self.path,
                        function=qualname,
                        receiver=ast.unparse(dump.func.value),
                        line=dump.lineno,
                    )
                )


def scan_digest_sites(root: Path = APP_ROOT) -> tuple[list[DigestSite], list[DigestSite]]:
    """Return (in-scope sites, sites in excluded RRM-013 areas)."""

    in_scope: list[DigestSite] = []
    excluded: list[DigestSite] = []
    repo = root.parent
    for file in sorted(root.rglob("*.py")):
        relative = file.relative_to(repo).as_posix()
        visitor = _Visitor(relative)
        visitor.visit(ast.parse(file.read_text(encoding="utf-8"), filename=str(file)))
        skipped = relative in EXCLUDED_FILES or any(part in relative for part in EXCLUDED_PARTS)
        (excluded if skipped else in_scope).extend(visitor.sites)
    return in_scope, excluded
