"""MP-22 acceptance: preflight refuses a database that is not the pinned mission-db release.

Real disposable PostgreSQL 17 (``MISSION_CONTROL_TEST_ADMIN_DSN``, loopback only). The release
is built and installed with the package's own ``release_build`` / ``write_lock`` /
``plan_release`` / ``apply_release`` into a generated ``mcqq_*`` database (as the two-project
qualification does); the fast common-component fixture database carries a
``fixture-unqualified`` attestation. Both databases are dropped afterwards.
"""

from __future__ import annotations

import json
import os
import shutil
from pathlib import Path

import asyncpg
import pytest

from mission_control.bootstrap.preflight import (
    ExpectedRelease,
    ReadinessInputs,
    compare_release,
    compose_launch_bindings,
    expected_release,
    observe_release,
    readiness,
)
from mission_control.bootstrap.settings import get_settings
from mission_control.domain.authoring.canonical import sha256_digest
from tests.fixtures.mission_control_common_db import common_database
from tests.qualification.two_project.disposable import (
    ADMIN_DSN_ENV,
    create_database,
    database_dsn,
    drop_database,
    new_suffix,
    require_admin_dsn,
)
from tests.qualification.two_project.release_install import (
    build_release,
    plan_and_apply,
    write_target,
)
from tests.unit.runtime.test_mp22_local_readiness import (
    AUTH_EXAMPLE,
    BINDINGS_EXAMPLE,
    PROFILE_EXAMPLE,
    owner_selections,
)

pytestmark = pytest.mark.common_db

ROOT = Path(__file__).resolve().parents[3]
DSN_ENV = "MP22_PREFLIGHT_DB_DSN"
INSTALLATION_ID = "0192a4f0-0000-7000-8000-0000000c0b22"


def local_profile(root: Path) -> Path:
    """A deep_agents-only profile beside the scratch release lock (FIXTURE owner selections)."""

    bindings = compose_launch_bindings(
        json.loads(BINDINGS_EXAMPLE.read_text(encoding="utf-8")), owner_selections()
    )
    (root / "bindings.json").write_text(bindings.model_dump_json(), encoding="utf-8")
    shutil.copyfile(AUTH_EXAMPLE, root / "auth.json")
    document = json.loads(PROFILE_EXAMPLE.read_text(encoding="utf-8"))
    document.update(
        lanes=[{"lane_profile": "deep_agents", "auth_profile_id": "deep-agents-openai-api"}],
        manifest_launch_bindings="bindings.json",
        auth_profiles="auth.json",
        temporal_clusters=document["temporal_clusters"][:1],
    )
    path = root / "profile.json"
    path.write_text(json.dumps(document), encoding="utf-8")
    return path


async def gate(root: Path, dsn: str) -> dict[str, object]:
    report = await readiness(
        ReadinessInputs(
            profile_path=local_profile(root),
            root=root,
            system="Linux",
            db_dsn_env=DSN_ENV,
            environ={DSN_ENV: dsn, "OPENAI_API_KEY": "present"},
        ),
        get_settings(),
    )
    return report.as_json()


@pytest.mark.asyncio
async def test_the_installed_release_passes_and_any_other_fingerprint_is_refused(
    tmp_path: Path,
) -> None:
    server = require_admin_dsn()
    built = await build_release(tmp_path, ADMIN_DSN_ENV, "biotech")
    lock = built.deployments_root / "biotech" / "release.lock.json"
    expected, issues = expected_release(lock)
    assert issues == [] and expected is not None
    committed = json.loads(
        (ROOT / "packages/mission-control-db-contract/component/manifest.json").read_text(
            encoding="utf-8"
        )
    )
    name = await create_database(server, new_suffix())
    previous = os.environ.get(DSN_ENV)
    try:
        target = write_target(
            tmp_path / "targets",
            app="biotech",
            database=name,
            server_dsn=server,
            installation_id=INSTALLATION_ID,
            project_ref="mcq-mp22-biotech",
            env_name=DSN_ENV,
        )
        dsn = database_dsn(server, name)
        # Provisioning precedes application (the installer holds otherwise).
        provisioning = await asyncpg.connect(dsn)
        try:
            await provisioning.execute(
                "CREATE SCHEMA IF NOT EXISTS extensions;"
                "CREATE EXTENSION IF NOT EXISTS vector SCHEMA extensions;"
                "CREATE EXTENSION IF NOT EXISTS pg_trgm SCHEMA extensions;"
            )
        finally:
            await provisioning.close()
        await plan_and_apply(target, built.release)
        observed = await observe_release(dsn, expected.component_version)
        assert observed is not None
        assert compare_release(expected, observed, pointer="db") == []
        # The scratch build reproduces the committed component manifest's fingerprint.
        assert observed.schema_fingerprint == committed["schema_fingerprint"]

        wrong = ExpectedRelease(
            expected.component_version,
            sha256_digest("another release"),
            expected.fingerprint_algorithm,
            lock,
        )
        (refused,) = compare_release(wrong, observed, pointer="db")
        assert refused.code == "DB_RELEASE_MISMATCH"

        # The whole gate on the scratch deployment root: the installed release passes.
        report = await gate(tmp_path, dsn)
        checks = report["checks"]
        assert isinstance(checks, dict) and checks["db_release"] == "passed", report
    finally:
        if previous is None:
            os.environ.pop(DSN_ENV, None)
        else:
            os.environ[DSN_ENV] = previous
        await drop_database(server, name)


@pytest.mark.asyncio
async def test_a_fixture_attested_database_is_refused_before_paid_work(tmp_path: Path) -> None:
    built = await build_release(tmp_path, ADMIN_DSN_ENV, "biotech")
    assert built.release is not None
    async with common_database() as database:
        report = await gate(tmp_path, database.owner_dsn)
        assert report["ready"] is False
        issues = report["issues"]
        assert isinstance(issues, list)
        (mismatch,) = [item for item in issues if item["code"] == "DB_RELEASE_MISMATCH"]
        assert str(mismatch["observed"]).startswith("fixture-unqualified:")
        assert mismatch["severity"] == "blocking"
    unreachable = await gate(tmp_path, "postgresql://nobody@127.0.0.1:1/none")
    found = unreachable["issues"]
    assert isinstance(found, list)
    assert "DB_UNREACHABLE" in {item["code"] for item in found}
