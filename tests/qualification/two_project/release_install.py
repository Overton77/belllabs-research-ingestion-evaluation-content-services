"""Drive the real ``mission_control_db_contract`` installer against disposable targets.

The qualification never installs the common component with ad-hoc SQL. It snapshots
the component tree (migrations + release spec + runtime descriptor) into a temporary
release root, runs the package's own ``release_build`` (rolled-back scratch build on
the same loopback server), ``write_lock`` and ``load_release``, then uses
``plan_release`` / ``apply_release`` / ``verify_release`` with a generated
``environment = "disposable"`` target manifest. Repository files are never written.
"""

from __future__ import annotations

import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

ROOT = Path(__file__).resolve().parents[3]
PACKAGE_ROOT = ROOT / "packages" / "mission-control-db-contract"
READER_VERSION = "mission-control-runtime/1"
WRITER_VERSION = "mission-control-runtime/1"


@dataclass(frozen=True)
class BuiltRelease:
    release: Any  # mission_control_db_contract.integrity.Release
    component_root: Path
    deployments_root: Path
    build_report: dict[str, Any]


def snapshot_component(destination: Path) -> Path:
    """Copy the component inputs (never caches/generated outputs) to a scratch tree."""
    source_component = PACKAGE_ROOT / "component"
    component = destination / "component"
    (component / "migrations").mkdir(parents=True)
    for path in sorted((source_component / "migrations").glob("*.sql")):
        shutil.copyfile(path, component / "migrations" / path.name)
    shutil.copyfile(source_component / "release-spec.json", component / "release-spec.json")
    descriptor = PACKAGE_ROOT / "runtime" / "descriptor.json"
    if descriptor.is_file():
        (destination / "runtime").mkdir()
        shutil.copyfile(descriptor, destination / "runtime" / "descriptor.json")
    return component


async def build_release(scratch: Path, admin_dsn_env: str, app: str) -> BuiltRelease:
    from mission_control_db_contract.integrity import load_release
    from mission_control_db_contract.release import release_build, write_lock

    component = snapshot_component(scratch / "release")
    report = await release_build(component, admin_dsn_env)
    deployments = scratch / "deployments"
    lock = write_lock(component, deployments, app)
    release = load_release(Path(lock["lock_path"]), component.resolve())
    return BuiltRelease(release, component, deployments, report)


def relock(built: BuiltRelease, app: str) -> Any:
    """Same release bytes, lock for another app (each app pins its own lock)."""
    from mission_control_db_contract.integrity import load_release
    from mission_control_db_contract.release import write_lock

    lock = write_lock(built.component_root, built.deployments_root, app)
    return load_release(Path(lock["lock_path"]), built.component_root.resolve())


def write_target(
    directory: Path,
    *,
    app: str,
    database: str,
    server_dsn: str,
    installation_id: str,
    project_ref: str,
    env_name: str,
) -> dict[str, str]:
    """Generate a disposable target manifest and export its DSN under ``env_name``."""
    from mission_control_db_contract.target import load_target

    parsed = urlsplit(server_dsn)
    host = "127.0.0.1" if parsed.hostname == "localhost" else str(parsed.hostname)
    os.environ[env_name] = urlunsplit(
        parsed._replace(
            netloc=parsed.netloc.rsplit("@", 1)[0] + f"@{host}:{parsed.port or 5432}",
            path="/" + database,
        )
    )
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{app}-{database}.toml"
    path.write_text(
        "format_version = 1\n\n[target]\n"
        f'app = "{app}"\napplication_id = "{app}"\n'
        f'project_label = "qualification-{app}"\nproject_ref = "{project_ref}"\n'
        f'installation_id = "{installation_id}"\nenvironment = "disposable"\n'
        f'database_host = "{host}"\ndatabase_port = {parsed.port or 5432}\n'
        f'database_name = "{database}"\ndatabase_user = "{parsed.username}"\n'
        f'database_url_env = "{env_name}"\n\n[approval]\n'
        'approved_by = "qualification-disposable"\n'
        'identity_evidence = "tests/qualification/two_project (generated disposable target)"\n',
        encoding="utf-8",
    )
    return load_target(path)


async def plan_and_apply(target: dict[str, str], release: Any) -> dict[str, Any]:
    from mission_control_db_contract.deployment import apply_release, plan_release

    planned = await plan_release(
        target, release, reader_version=READER_VERSION, writer_version=WRITER_VERSION
    )
    applied = await apply_release(
        target,
        release,
        confirmation=f"{target['project_ref']}:{target['installation_id']}",
        expected_plan_digest=planned["plan_digest"],
        reader_version=READER_VERSION,
        writer_version=WRITER_VERSION,
    )
    return {"plan": planned, "apply": applied}


async def verify(target: dict[str, str], release: Any) -> dict[str, Any]:
    from mission_control_db_contract.deployment import verify_release

    return await verify_release(
        target, release, reader_version=READER_VERSION, writer_version=WRITER_VERSION
    )
