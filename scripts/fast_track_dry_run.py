"""Unpaid dry run of the three owner missions through the real API and CLI path.

For each application (``biotech`` for Missions 1 and 2, ``ai-engineer`` for Mission 3) on a
loopback PostgreSQL 17 + pgvector server:

1. a scratch database, the 1.1.0 component through ``mission-db`` (lock, plan, apply,
   verify) and the seeds (common, the app, and the opt-in ``qualification`` bundle whose
   synthetic tenant the dry run acts in);
2. restricted logins (runtime, family writer), a lexical-only catalog projection
   (``scripts/rebuild_capability_search_projection.py --lexical-only``; no embedding call);
3. optionally (``--with-fixture-rows``) the FT-E2 test stand-ins for the capabilities the
   production seeds do not carry, published into the scratch catalog;
4. the configured API (``mission_control.bootstrap.api:create_app``) with a throwaway RSA
   key, no Temporal and no worker;
5. ``missionctl mission compile`` for the three manifests, then ``mission submit``,
   ``mission start`` (refused without Temporal: no run can start, nothing is paid),
   ``run inspect`` and ``chain inspect``.

Everything is dropped afterwards (the scratch databases and the logins this run created).
No provider credential reaches any process it starts; tokens and DSNs are never printed.
The report lands in ``--out`` (default ``.scratch/fast-track-dry-run/<utc time>``).

Usage:
  MISSION_CONTROL_TEST_ADMIN_DSN=postgresql://...@127.0.0.1:55433/postgres \
  uv run --no-sync python scripts/fast_track_dry_run.py [--with-fixture-rows]
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import secrets
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlsplit, urlunsplit
from uuid import UUID

import asyncpg
import httpx

ROOT = Path(__file__).resolve().parents[1]
PACKAGE = ROOT / "packages" / "mission-control-db-contract"
MISSIONS = ROOT / "docs" / "specs" / "fast-track-2026-10" / "missions"
# The disposable identities the qualification tooling uses (never a live project).
APPS = {
    "biotech": ("mcdisposablebiotech", "0192a4f0-0000-7000-8000-00000000b10e"),
    "ai-engineer": ("mcdisposableaieng", "0192a4f0-0000-7000-8000-0000000a1e00"),
}
VERSIONS = [
    "--reader-version",
    "mission-control-runtime/1",
    "--writer-version",
    "mission-control-runtime/1",
]
ISSUER = "https://fast-track-dry-run.invalid/auth/v1"
PLAN = (
    ("biotech", "01-research-ingestion-deep-agents.yml"),
    ("biotech", "02-research-ingestion-cursor-cloud-chain.yml"),
    ("ai-engineer", "03-codebase-feature-cursor-local.yml"),
)
SECRET_WORDS = ("API_KEY", "TOKEN", "SECRET", "PASSWORD", "DSN", "DATABASE")


class DryRun:
    def __init__(self, admin_dsn: str, out: Path, *, port: int, fixture_rows: bool) -> None:
        if urlsplit(admin_dsn).hostname not in {"127.0.0.1", "localhost", "::1"}:
            raise SystemExit("the dry run accepts a loopback disposable server only")
        self.admin = admin_dsn
        self.out = out
        self.port = port
        self.fixture_rows = fixture_rows
        self.suffix = secrets.token_hex(4)
        self.databases: list[str] = []
        # No provider credential or database reference of the caller's shell is inherited.
        self.base_env = {
            key: value
            for key, value in os.environ.items()
            if not any(word in key.upper() for word in SECRET_WORDS)
        }
        self.report: dict[str, Any] = {"apps": {}, "cli": []}

    # -- database helpers ---------------------------------------------------------------------

    def _dsn(self, database: str, user: str | None = None, password: str | None = None) -> str:
        parsed = urlsplit(self.admin)
        netloc = parsed.netloc
        if user is not None:
            host = f"{parsed.hostname}:{parsed.port or 5432}"
            netloc = f"{quote(user, safe='')}:{quote(password or '', safe='')}@{host}"
        return urlunsplit(parsed._replace(netloc=netloc, path="/" + database))

    async def _create_database(self, app: str) -> str:
        name = f"mcdry_{app.replace('-', '')}_{self.suffix}"
        admin = await asyncpg.connect(self.admin)
        try:
            await admin.execute(f'CREATE DATABASE "{name}"')
        finally:
            await admin.close()
        self.databases.append(name)
        connection = await asyncpg.connect(self._dsn(name))
        try:
            await connection.execute("CREATE SCHEMA IF NOT EXISTS extensions")
            await connection.execute("CREATE EXTENSION IF NOT EXISTS vector SCHEMA extensions")
            await connection.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm SCHEMA extensions")
            await connection.execute("GRANT USAGE ON SCHEMA extensions TO PUBLIC")
        finally:
            await connection.close()
        return name

    async def _logins(self, app: str, database: str) -> dict[str, str]:
        dsns: dict[str, str] = {}
        connection = await asyncpg.connect(self._dsn(database))
        try:
            for role in ("runtime", "family_writer"):
                login = f"mcdry_{self.suffix}_{app.replace('-', '')}_{role}"
                password = secrets.token_hex(16)
                await connection.execute(
                    f'CREATE ROLE "{login}" LOGIN NOSUPERUSER NOBYPASSRLS NOCREATEDB '
                    f"NOCREATEROLE INHERIT PASSWORD '{password}' IN ROLE mission_control_{role}"
                )
                await connection.execute(f'GRANT CONNECT ON DATABASE "{database}" TO "{login}"')
                dsns[role] = self._dsn(database, login, password)
        finally:
            await connection.close()
        return dsns

    async def _tenant(self, database: str) -> UUID:
        connection = await asyncpg.connect(self._dsn(database))
        try:
            rows = await connection.fetch("SELECT tenant_id FROM mission_control.tenant")
        finally:
            await connection.close()
        if len(rows) != 1:
            raise RuntimeError("the qualification seed should create exactly one tenant")
        return UUID(str(rows[0]["tenant_id"]))

    async def _publish_fixture_rows(self, dsn: str, catalog_scope: str) -> list[str]:
        sys.path.insert(0, str(ROOT))
        from tests.fixtures.catalog.fast_track_catalog import fixture_definitions

        from mission_control.adapters.postgres.control_plane.definition_repository import (
            PostgresDefinitionRepository,
        )

        pool = await asyncpg.create_pool(dsn, min_size=1, max_size=2)
        outcome: list[str] = []
        try:
            repository = PostgresDefinitionRepository(pool, catalog_scope=catalog_scope)
            existing = {
                (ref.kind, ref.logical_id)
                for ref in await repository.list_published_definition_refs()
            }
            for definition in fixture_definitions():
                name = f"{definition.kind.value}:{definition.logical_id}"
                if (definition.kind, definition.logical_id) in existing:
                    continue
                try:
                    await repository.publish(
                        definition,
                        actor_id="fast-track-dry-run",
                        published_at=datetime.now(UTC),
                        expected_head_revision=0,
                    )
                except Exception as error:  # reported per row; the run continues
                    outcome.append(f"skipped {name} ({type(error).__name__})")
                    continue
                outcome.append(f"published {name}")
        finally:
            await pool.close()
        return outcome

    async def cleanup(self) -> None:
        admin = await asyncpg.connect(self.admin)
        try:
            for name in self.databases:
                await admin.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
            logins = await admin.fetch(
                "SELECT rolname FROM pg_roles WHERE rolname LIKE $1", f"mcdry\\_{self.suffix}\\_%"
            )
            for row in logins:
                await admin.execute(f'DROP ROLE IF EXISTS "{row["rolname"]}"')
        finally:
            await admin.close()

    # -- processes ----------------------------------------------------------------------------

    def _run(self, command: list[str], env: dict[str, str], timeout: int = 900) -> tuple[int, str]:
        done = subprocess.run(
            command, cwd=ROOT, env=env, capture_output=True, text=True, timeout=timeout, check=False
        )
        return done.returncode, done.stdout

    @staticmethod
    def _json(stdout: str) -> dict[str, Any]:
        text = stdout[stdout.find("{") :] if "{" in stdout else "{}"
        try:
            value = json.loads(text)
        except ValueError:
            return {"unparsed": stdout[-400:]}
        return value if isinstance(value, dict) else {"value": value}

    def install(self, app: str) -> tuple[str, dict[str, str], UUID]:
        project_ref, installation = APPS[app]
        database = asyncio.run(self._create_database(app))
        env_name = f"MC_DRY_{app.upper().replace('-', '_')}_MIGRATION_URL"
        env = {**self.base_env, env_name: self._dsn(database)}
        deployments = self.out / "deployments"
        target = deployments / app
        target.mkdir(parents=True, exist_ok=True)
        parsed = urlsplit(self.admin)
        (target / "target.toml").write_text(
            "format_version = 1\n\n[target]\n"
            f'app = "{app}"\napplication_id = "{app}"\nproject_label = "fast-track-dry-run"\n'
            f'project_ref = "{project_ref}"\ninstallation_id = "{installation}"\n'
            'environment = "disposable"\n'
            f'database_host = "{parsed.hostname}"\ndatabase_port = {parsed.port or 5432}\n'
            f'database_name = "{database}"\ndatabase_user = "{parsed.username}"\n'
            f'database_url_env = "{env_name}"\n\n[approval]\n'
            'approved_by = "fast-track-dry-run"\n'
            'identity_evidence = "loopback scratch database dropped after the dry run"\n',
            encoding="utf-8",
        )
        steps: list[dict[str, Any]] = []

        def mission_db(name: str, *args: str) -> dict[str, Any]:
            code, stdout = self._run(["uv", "run", "--no-sync", "mission-db", *args], env)
            body = self._json(stdout)
            steps.append({"step": name, "exit": code, "status": body.get("status")})
            if code:
                steps[-1]["holds"] = body.get("holds")
            return body

        common = ["--deployment-dir", str(target), *VERSIONS]
        confirm = f"{project_ref}:{installation}"
        mission_db(
            "lock",
            "lock",
            "--app",
            app,
            "--component-root",
            str(PACKAGE / "component"),
            "--deployments-root",
            str(deployments),
        )
        plan = mission_db("plan", "plan", *common)
        mission_db(
            "apply",
            "apply",
            *common,
            "--confirm-target",
            confirm,
            "--expected-plan-digest",
            str(plan.get("plan_digest", "")),
        )
        mission_db("verify", "verify", *common)
        bundles: list[str] = []
        for directory in ("common", app, "qualification"):
            bundles += ["--bundle", str(PACKAGE / "seeds" / directory)]
        seeded = mission_db(
            "seed-apply", "seed-apply", *common, *bundles, "--confirm-target", confirm
        )
        steps[-1]["bundles"] = [
            f"{item.get('seed_key')}@{item.get('seed_version')}: {item.get('outcome')}"
            for item in seeded.get("bundles", [])
        ]
        logins = asyncio.run(self._logins(app, database))
        tenant = asyncio.run(self._tenant(database))
        catalog_scope = f"mc/{installation}/{app}/catalog"
        if self.fixture_rows:
            published = asyncio.run(self._publish_fixture_rows(logins["runtime"], catalog_scope))
            steps.append({"step": "fixture-rows", "outcome": published})
        # The projection tooling runs on the runtime role (0022 grants).
        code, stdout = self._run(
            [
                "uv",
                "run",
                "--no-sync",
                "python",
                "scripts/rebuild_capability_search_projection.py",
                "--tenant",
                catalog_scope,
                "--lexical-only",
            ],
            {
                **self.base_env,
                "DATABASE_DIRECT": logins["runtime"],
                "MISSION_CONTROL_CATALOG_SCOPE": catalog_scope,
            },
        )
        rebuilt = self._json(stdout)
        steps.append(
            {
                "step": "projection-rebuild-lexical",
                "exit": code,
                "documents": rebuilt.get("rebuild", {}).get("selected_count"),
                "verified": rebuilt.get("verification", {}).get("valid"),
            }
        )
        self.report["apps"][app] = {"database": database, "steps": steps}
        return database, logins, tenant

    def run(self) -> int:
        from joserfc import jwt
        from joserfc.jwk import RSAKey

        from mission_control.adapters.auth.jwt import ActorGrant, ApplicationAuthentication
        from mission_control.application.installations.registry import ApplicationBinding
        from mission_control.bootstrap.api import ApplicationDeployment, MissionDeployment

        self.out.mkdir(parents=True, exist_ok=True)
        key = RSAKey.generate_key(2048, parameters={"kid": "fast-track-dry-run"})
        jwks = self.out / "public-jwks.json"
        jwks.write_text(json.dumps({"keys": [key.as_dict(private=False)]}), encoding="utf-8")
        api_env = dict(self.base_env)
        applications = []
        tokens: dict[str, str] = {}
        server: subprocess.Popen[bytes] | None = None
        try:
            for app, (project_ref, installation) in APPS.items():
                _database, logins, tenant = self.install(app)
                prefix = f"MC_DRY_{app.upper().replace('-', '_')}"
                api_env[f"{prefix}_RUNTIME_URL"] = logins["runtime"]
                api_env[f"{prefix}_FAMILY_URL"] = logins["family_writer"]
                binding = ApplicationBinding.seal(
                    application_id=app,
                    installation_id=UUID(installation),
                    binding_version="1",
                    supabase_project_ref=project_ref,
                    database_secret_ref=f"{prefix}_RUNTIME_URL",
                    accepted_issuers={ISSUER},
                    accepted_audiences={"authenticated"},
                    required_component_version="1.1.0",
                )
                subject = f"fast-track-dry-run-{app}"
                applications.append(
                    ApplicationDeployment(
                        authentication=ApplicationAuthentication(
                            binding=binding,
                            issuer=ISSUER,
                            audience="authenticated",
                            public_jwks_file=jwks,
                            grants=(
                                ActorGrant(
                                    subject=subject,
                                    actor_id="operator",
                                    tenant_ids={tenant},
                                    permissions={
                                        "workflow_run.read",
                                        "catalog:read",
                                        "mission.author",
                                        "mission.start",
                                    },
                                    authority_refs={"authority:fast-track-dry-run"},
                                    sponsorship_refs={"sponsorship:fast-track-dry-run"},
                                    approval_refs={"approval:fast-track-dry-run"},
                                ),
                            ),
                        ),
                        family_writer_secret_ref=f"{prefix}_FAMILY_URL",
                    )
                )
                tokens[app] = jwt.encode(
                    {"alg": "RS256", "kid": "fast-track-dry-run"},
                    {
                        "sub": subject,
                        "iss": ISSUER,
                        "aud": "authenticated",
                        "exp": int(time.time()) + 3600,
                        "app_metadata": {"application_id": app, "tenant_id": str(tenant)},
                    },
                    key,
                )
            deployment = MissionDeployment(
                storage_mode="production_common",
                max_request_bytes=4_000_000,
                applications=tuple(applications),
            )
            deployment_file = self.out / "deployment.json"
            deployment_file.write_text(deployment.model_dump_json(indent=2), encoding="utf-8")
            api_env["MISSION_CONTROL_DEPLOYMENT_FILE"] = str(deployment_file)
            with (self.out / "api.log").open("w", encoding="utf-8") as log:
                server = subprocess.Popen(
                    [
                        sys.executable,
                        "-m",
                        "uvicorn",
                        "mission_control.bootstrap.api:create_app",
                        "--factory",
                        "--host",
                        "127.0.0.1",
                        "--port",
                        str(self.port),
                    ],
                    cwd=ROOT,
                    env=api_env,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                )
                self.report["ready"] = self._wait_ready(server)
                if self.report["ready"] is not None:
                    self._missions(tokens)
        finally:
            if server is not None:
                if sys.platform == "win32":
                    subprocess.run(
                        ["taskkill", "/T", "/F", "/PID", str(server.pid)],
                        capture_output=True,
                        check=False,
                    )
                server.terminate()
                server.wait(timeout=30)
            asyncio.run(self.cleanup())
            self.report["dropped"] = list(self.databases)
            (self.out / "report.json").write_text(
                json.dumps(self.report, indent=2, default=str), encoding="utf-8"
            )
        return 0 if self.report.get("ready") is not None else 1

    def _wait_ready(self, server: subprocess.Popen[bytes]) -> dict[str, Any] | None:
        for _ in range(240):
            try:
                ready = httpx.get(f"http://127.0.0.1:{self.port}/health/ready", timeout=2)
                if ready.status_code == 200:
                    return dict(ready.json())
            except httpx.HTTPError:
                pass
            if server.poll() is not None:
                return None
            time.sleep(0.5)
        return None

    def _missionctl(self, app: str, token: str, *args: str) -> tuple[int, dict[str, Any]]:
        env = {
            **self.base_env,
            "MISSION_CONTROL_TOKEN": token,
            "MISSION_CONTROL_APPLICATION_ID": app,
            "MISSION_CONTROL_URL": f"http://127.0.0.1:{self.port}",
        }
        code, stdout = self._run(["uv", "run", "--no-sync", "missionctl", *args, "--json"], env)
        return code, self._json(stdout)

    def _missions(self, tokens: dict[str, str]) -> None:
        for app, name in PLAN:
            path = str(MISSIONS / name)
            code, body = self._missionctl(app, tokens[app], "mission", "compile", path)
            report = body.get("report", {})
            (self.out / f"{app}-compile-{name}.json").write_text(
                json.dumps(body, indent=2), encoding="utf-8"
            )
            entry: dict[str, Any] = {
                "app": app,
                "manifest": name,
                "compile": {
                    "exit": code,
                    "ok": report.get("ok"),
                    "blockers": [
                        f"{item.get('pointer')}: {item.get('message')}"
                        for item in report.get("blockers", [])
                    ],
                    "warnings": sorted(
                        {str(item.get("reason")) for item in report.get("warnings", [])}
                    ),
                },
            }
            code, body = self._missionctl(app, tokens[app], "mission", "submit", path)
            members = body.get("missions") or []
            entry["submit"] = {
                "exit": code,
                "chain_id": body.get("chain_id"),
                "runs": [member.get("run_id") for member in members if isinstance(member, dict)],
                "detail": body.get("detail", {}).get("code") if code else None,
            }
            runs = entry["submit"]["runs"]
            if runs:
                code, body = self._missionctl(app, tokens[app], "mission", "start", runs[0])
                entry["start"] = {"exit": code, "answer": body.get("detail", body)}
                code, body = self._missionctl(app, tokens[app], "run", "inspect", runs[0])
                entry["inspect"] = {
                    "exit": code,
                    "lifecycle": body.get("lifecycle"),
                    "phase": body.get("phase"),
                }
            if entry["submit"]["chain_id"]:
                code, body = self._missionctl(
                    app, tokens[app], "chain", "inspect", str(entry["submit"]["chain_id"])
                )
                chain = body.get("chain", {})
                entry["chain"] = {
                    "exit": code,
                    "links": [
                        f"{link.get('kind')}: {link.get('state')}"
                        for link in chain.get("links", [])
                    ],
                }
            self.report["cli"].append(entry)
            print(json.dumps(entry, default=str), flush=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--admin-dsn-env", default="MISSION_CONTROL_TEST_ADMIN_DSN")
    parser.add_argument("--out", type=Path)
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--with-fixture-rows", action="store_true")
    args = parser.parse_args()
    dotenv = ROOT / ".env"
    if dotenv.is_file() and any(
        line.split("=", 1)[0].strip() == "CAPABILITY_EMBEDDING_PROFILE"
        for line in dotenv.read_text(encoding="utf-8").splitlines()
    ):
        # Settings read the checkout's .env: compile would embed every search query (paid).
        raise SystemExit("unset CAPABILITY_EMBEDDING_PROFILE in .env for an unpaid dry run")
    admin = os.environ.get(args.admin_dsn_env)
    if not admin:
        raise SystemExit(f"{args.admin_dsn_env} must name a loopback PostgreSQL 17 admin DSN")
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    out = args.out or ROOT / ".scratch" / "fast-track-dry-run" / stamp
    dry = DryRun(admin, out.resolve(), port=args.port, fixture_rows=args.with_fixture_rows)
    status = dry.run()
    print(json.dumps({"report": str(out / "report.json"), "ready": dry.report.get("ready")}))
    return status


if __name__ == "__main__":
    sys.exit(main())
