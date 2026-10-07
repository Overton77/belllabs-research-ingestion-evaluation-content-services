"""Static exit-from-transitional-storage checks (no database needed).

These are NOT marked ``common_db``: they must also run in offline selections
(``-m "not common_db"``) so a legacy reference cannot slip in unnoticed. Every scan
asserts a nonempty input set.

Production-code scan (``src/mission_control``, code/config suffixes) forbids:
schema-qualified legacy usage (``belllabs_control.``, ``capability_search.``,
``belllabs_langgraph.``), any bare legacy schema name, ``belllabs.request_scope``,
``belllabs.catalog_scope``, ``transitional_local`` and the legacy migration runners
``apply_application_migrations`` / ``apply_capability_search_migrations``. Logical
identifiers that merely share a prefix (``belllabs_control_plane``,
``capability_search_repository``) or are dotted Python module paths
(``...capabilities.capability_search``) are not schema references and do not match.

Allowances are narrow: exact file + exact token + exact count + a regex every
allowed line must match (deny-list constants that REFUSE legacy schemas, and one
comment stating there is no fallback). A stale or widened allowance fails.

Exclusions: the historical chain ``adapters/postgres/migrations/`` (pinned byte for
byte by ``docs/organization/legacy-belllabs-control-chain.json``) and, only until the
lead retires it, ``bootstrap/installation.py`` (which must then be the only file
still carrying legacy references, or not exist).
"""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
PRODUCTION_ROOT = ROOT / "src" / "mission_control"
LEGACY_CHAIN_DIR = PRODUCTION_ROOT / "adapters" / "postgres" / "migrations"
LEGACY_CHAIN_ARCHIVE = ROOT / "docs" / "organization" / "legacy-belllabs-control-chain.json"
COMPONENT_MIGRATIONS = (
    ROOT / "packages" / "mission-control-db-contract" / "component" / "migrations"
)
SCANNED_SUFFIXES = {".py", ".sql", ".json", ".toml", ".yaml", ".yml", ".ini", ".cfg"}

_LEGACY_NAMES = r"(belllabs_control|capability_search|belllabs_langgraph)"
_Q = r'(?<![A-Za-z0-9_./])"?{name}"?\s*\.'
LEGACY_PATTERNS = {
    "belllabs_control.": re.compile(_Q.format(name="belllabs_control")),
    "capability_search.": re.compile(_Q.format(name="capability_search")),
    "belllabs_langgraph.": re.compile(_Q.format(name="belllabs_langgraph")),
    "legacy schema name": re.compile(r"(?<![A-Za-z0-9_./])" + _LEGACY_NAMES + r"(?![A-Za-z0-9_])"),
    "belllabs.request_scope": re.compile(r"belllabs\.request_scope"),
    "belllabs.catalog_scope": re.compile(r"belllabs\.catalog_scope"),
    "transitional_local": re.compile(r"transitional_local"),
    "apply_application_migrations": re.compile(r"apply_application_migrations"),
    "apply_capability_search_migrations": re.compile(r"apply_capability_search_migrations"),
}
LEGACY_TOKEN = LEGACY_PATTERNS["legacy schema name"]
# (file, bare legacy token) -> (exact occurrence count, regex every allowed line matches).
# Only deny-list constants that refuse legacy schemas and one "no fallback" comment.
_LIST_ITEM = r'^\s*"{token}",\s*$'
_PERSISTENCE = "src/mission_control/adapters/deep_agents/persistence.py"
_VERIFIER = "src/mission_control/adapters/deep_agents/runtime_persistence_verifier.py"
ALLOWANCES: dict[tuple[str, str], tuple[int, str]] = {
    **{
        (path, token): (1, _LIST_ITEM.format(token=token))
        for path in (_PERSISTENCE, _VERIFIER)
        for token in ("belllabs_control", "capability_search", "belllabs_langgraph")
    },
    ("src/mission_control/bootstrap/settings.py", "belllabs_langgraph"): (
        1,
        r"^\s*# fallback to `belllabs_langgraph` or `public`\.",
    ),
}
# Excluded only until the lead retires it; then it must disappear.
PENDING_RETIREMENT = "src/mission_control/bootstrap/installation.py"

OWNED = {"mission_control", "mission_control_search", "pg_catalog"}
# Explicitly non-owned namespaces seen on the live targets or in the legacy design.
FORBIDDEN_SCHEMAS = (
    # Supabase-managed and shared
    "auth",
    "storage",
    "util",
    "public",
    "extensions_private",
    "graphql",
    "graphql_public",
    "realtime",
    "vault",
    "net",
    "cron",
    "pgsodium",
    "supabase_functions",
    "supabase_migrations",
    # AI Engineer domain schemas observed read-only at G0 (supabase-blue-ocean identity.json)
    "api",
    "content",
    "corpus",
    "curriculum",
    "evaluation",
    "evidence",
    "knowledge",
    "knowledge_service",
    "observability",
    "orchestration",
    "provenance",
    "ranking",
    "research",
    "research_private",
    "retrieval",
    "staging",
    "taxonomy",
    "temporal",
    # Legacy transitional / other components
    "belllabs_control",
    "belllabs_langgraph",
    "capability_search",
    "biotech_mission_adapters",
    "mission_control_runtime",
    "mission_control_agent_server",
)
FORBIDDEN_REFERENCE = re.compile(
    r'(?<![A-Za-z0-9_."])"?(' + "|".join(FORBIDDEN_SCHEMAS) + r')"?\s*\.\s*"?[A-Za-z_]',
    re.IGNORECASE,
)
# A schema-qualified object after an object-introducing keyword must be owned.
KEYWORD_QUALIFIED = re.compile(
    r"\b(?:FROM|JOIN|INTO|UPDATE|REFERENCES|TABLE|ON|FUNCTION|PROCEDURE|VIEW|SEQUENCE|TYPE|"
    r"DOMAIN|TRIGGER|INDEX|EXECUTE\s+FUNCTION|USING)\s+(?:ONLY\s+)?(?:IF\s+(?:NOT\s+)?EXISTS\s+)?"
    r'"?([A-Za-z_][A-Za-z0-9_]*)"?\s*\.\s*"?([A-Za-z_][A-Za-z0-9_]*)',
    re.IGNORECASE,
)
# ``ON alias.column = ...`` in a join condition is a column reference, not an object.
COMPARISON_FOLLOWS = re.compile(r"\s*(=|<|>|!|IS\b|IN\b|AND\b|OR\b|\))", re.IGNORECASE)
EXTENSIONS_REFERENCE = re.compile(
    r'"?extensions"?\s*\.\s*"?([A-Za-z_][A-Za-z0-9_]*)', re.IGNORECASE
)
ALLOWED_EXTENSION_OBJECTS = re.compile(r"vector(_(l2|ip|cosine|l1)_ops)?", re.IGNORECASE)
GLOBAL_SETTINGS = re.compile(
    r"\bALTER\s+(DATABASE|SYSTEM)\b|\bALTER\s+ROLE\b[^;]*\bSET\b", re.IGNORECASE
)


def _strip_sql_comments(sql: str) -> str:
    sql = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL)
    return re.sub(r"--[^\n]*", "", sql)


def _production_files(*, include_pending: bool = False) -> list[Path]:
    files = []
    for path in sorted(PRODUCTION_ROOT.rglob("*")):
        if not path.is_file() or "__pycache__" in path.parts:
            continue
        if path.suffix not in SCANNED_SUFFIXES:
            continue
        if path.parent == LEGACY_CHAIN_DIR:
            continue
        if not include_pending and path.relative_to(ROOT).as_posix() == PENDING_RETIREMENT:
            continue
        files.append(path)
    return files


def _scan(files: list[Path]) -> tuple[list[str], dict[tuple[str, str], list[str]]]:
    """Return (unallowed hits, allowed lines per allowance key)."""
    hits: list[str] = []
    allowed: dict[tuple[str, str], list[str]] = {key: [] for key in ALLOWANCES}
    for path in files:
        relative = path.relative_to(ROOT).as_posix()
        text = path.read_text(encoding="utf-8", errors="replace")
        for number, line in enumerate(text.splitlines(), start=1):
            labels = [label for label, pattern in LEGACY_PATTERNS.items() if pattern.search(line)]
            if not labels:
                continue
            tokens = [m.group(1) for m in LEGACY_TOKEN.finditer(line)]
            if labels == ["legacy schema name"] and len(tokens) == 1:
                key = (relative, tokens[0])
                if key in ALLOWANCES and re.search(ALLOWANCES[key][1], line):
                    allowed[key].append(f"{relative}:{number}")
                    continue
            hits.append(f"{relative}:{number}: {', '.join(labels)}")
    return hits, allowed


def test_legacy_chain_exclusion_is_pinned_by_archive() -> None:
    archive = json.loads(LEGACY_CHAIN_ARCHIVE.read_text(encoding="utf-8"))
    pinned = {Path(item["path"]).name: item["sha256"] for item in archive["migrations"]}
    present = {
        path.name: hashlib.sha256(path.read_bytes()).hexdigest()
        for path in LEGACY_CHAIN_DIR.iterdir()
        if path.is_file() and path.suffix in SCANNED_SUFFIXES
    }
    assert archive["file_count"] == 40
    assert present == pinned, "legacy chain changed; the static-scan exclusion is no longer pinned"


def test_production_code_has_no_legacy_storage_references() -> None:
    files = _production_files()
    assert len(files) > 100, f"production scan input unexpectedly small: {len(files)}"
    hits, _allowed = _scan(files)
    files_hit = sorted({hit.split(":", 1)[0] for hit in hits})
    assert not hits, f"{len(hits)} legacy references in {len(files_hit)} files: {hits[:40]}"


def test_legacy_allowances_are_exact_and_not_stale() -> None:
    files = sorted({ROOT / relative for relative, _token in ALLOWANCES})
    assert all(path.is_file() for path in files), [p for p in files if not p.is_file()]
    _hits, allowed = _scan(files)
    wrong = {
        f"{relative} {token}": (len(allowed[(relative, token)]), count)
        for (relative, token), (count, _regex) in ALLOWANCES.items()
        if len(allowed[(relative, token)]) != count
    }
    assert not wrong, f"allowance (observed, declared) mismatch; tighten or remove: {wrong}"


def test_pending_installation_module_is_the_only_remaining_legacy_file() -> None:
    pending = ROOT / PENDING_RETIREMENT
    hits, _allowed = _scan(_production_files(include_pending=True))
    files_hit = {hit.split(":", 1)[0] for hit in hits}
    if not pending.exists():
        assert PENDING_RETIREMENT not in files_hit
        return
    assert files_hit <= {PENDING_RETIREMENT}, sorted(files_hit - {PENDING_RETIREMENT})


def _component_sql() -> list[tuple[Path, str]]:
    paths = sorted(COMPONENT_MIGRATIONS.glob("[0-9][0-9][0-9][0-9]_*.sql"))
    assert paths, "common component has no migrations to scan"
    return [(path, _strip_sql_comments(path.read_text(encoding="utf-8"))) for path in paths]


def test_component_sql_references_no_foreign_schema() -> None:
    hits: list[str] = []
    for path, sql in _component_sql():
        for match in FORBIDDEN_REFERENCE.finditer(sql):
            line = sql.count("\n", 0, match.start()) + 1
            hits.append(f"{path.name}:{line}: {match.group(0).strip()}")
        for match in KEYWORD_QUALIFIED.finditer(sql):
            schema = match.group(1).lower()
            if schema in OWNED or schema == "extensions":
                continue
            if COMPARISON_FOLLOWS.match(sql, match.end()):
                continue
            line = sql.count("\n", 0, match.start()) + 1
            hits.append(f"{path.name}:{line}: {match.group(1)}.{match.group(2)}")
        for match in EXTENSIONS_REFERENCE.finditer(sql):
            if not ALLOWED_EXTENSION_OBJECTS.fullmatch(match.group(1)):
                line = sql.count("\n", 0, match.start()) + 1
                hits.append(f"{path.name}:{line}: extensions.{match.group(1)}")
    assert not hits, hits


def test_component_sql_changes_no_global_role_or_database_settings() -> None:
    hits = [
        f"{path.name}: {match.group(0)}"
        for path, sql in _component_sql()
        for match in GLOBAL_SETTINGS.finditer(sql)
    ]
    assert not hits, hits


def test_scanners_detect_planted_violations() -> None:
    """Guard against a scanner that silently matches nothing."""
    planted = _strip_sql_comments(
        "-- auth.users in a comment is ignored\n"
        "SELECT 1 FROM storage.objects;\n"
        "CREATE TABLE x (e extensions.gen_random_uuid);\n"
        "ALTER ROLE r SET search_path = public;\n"
    )
    assert not re.search(r"auth\.users", planted)
    assert FORBIDDEN_REFERENCE.search(planted)
    assert any(m.group(1) == "storage" for m in KEYWORD_QUALIFIED.finditer(planted))
    join = "JOIN mission_control.a AS c ON c.installation_id = a.installation_id"
    assert all(
        COMPARISON_FOLLOWS.match(join, m.end())
        for m in KEYWORD_QUALIFIED.finditer(join)
        if m.group(1) == "c"
    )
    assert not COMPARISON_FOLLOWS.match("ON storage.objects FOR SELECT", len("ON storage.objects"))
    assert any(
        not ALLOWED_EXTENSION_OBJECTS.fullmatch(m.group(1))
        for m in EXTENSIONS_REFERENCE.finditer(planted)
    )
    assert GLOBAL_SETTINGS.search(planted)
    samples = {
        "belllabs_control.": "FROM belllabs_control.workflow_runs",
        "capability_search.": 'FROM "capability_search".docs',
        "belllabs_langgraph.": "belllabs_langgraph.checkpoints",
        "legacy schema name": "CREATE SCHEMA IF NOT EXISTS belllabs_control;",
        "belllabs.request_scope": "current_setting('belllabs.request_scope')",
        "belllabs.catalog_scope": "set_config('belllabs.catalog_scope', $1, true)",
        "transitional_local": 'storage_mode="transitional_local"',
        "apply_application_migrations": "await apply_application_migrations(pool)",
        "apply_capability_search_migrations": "await apply_capability_search_migrations(p)",
    }
    assert set(samples) == set(LEGACY_PATTERNS)
    assert all(LEGACY_PATTERNS[label].search(sample) for label, sample in samples.items())
    for logical in (
        'writer = "belllabs_control_plane"',
        "from x.capability_search_repository import Repo",
        "mission_control_search.documents",
        "from mission_control.application.capabilities.capability_search import Service",
        "mission_control.application.capabilities.capability_search.Service()",
    ):
        assert not any(p.search(logical) for p in LEGACY_PATTERNS.values()), logical
