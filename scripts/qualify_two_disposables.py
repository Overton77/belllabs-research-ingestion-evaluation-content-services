"""Two-disposable G2/G3 installation proof through the real `mission-db` CLI.

Each target is its own disposable PostgreSQL 17 cluster (roles are cluster-global,
like two Supabase projects). A non-superuser CREATEROLE migration login owns a
pre-existing protected domain fixture (as Supabase `postgres` owns app schemas), then
the CLI runs: qualify(before) -> plan -> apply -> replay(no-op) -> verify ->
runtime-plan/apply (+replay) -> seed-plan/apply (+replay) -> qualify(after). Evidence
files land in docs/qualification/two-project/<target>/<run-id>/. Only loopback admin
DSNs are accepted; no live project is ever contacted.

Usage:
  uv run --no-sync python scripts/qualify_two_disposables.py \
      --biotech-admin-dsn-env MC_PROOF_BIO_ADMIN --ai-engineer-admin-dsn-env MC_PROOF_AIE_ADMIN \
      --run-id <run-id>
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import secrets
import subprocess
import sys
from pathlib import Path
from urllib.parse import quote, urlsplit, urlunsplit

import asyncpg

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from tests.qualification.two_project.domain_fixtures import (  # noqa: E402
    create_domain_roles,
    install_domain,
)

PACKAGE = ROOT / "packages" / "mission-control-db-contract"
COMPONENT = PACKAGE / "component"
VERSIONS = [
    "--reader-version",
    "mission-control-runtime/1",
    "--writer-version",
    "mission-control-runtime/1",
]
TARGETS = {
    "biotech": {
        "label": "disposable-biotech",
        "project_ref": "mcdisposablebiotech",
        "installation_id": "0192a4f0-0000-7000-8000-00000000b10e",
    },
    "ai-engineer": {
        "label": "disposable-ai-engineer",
        "project_ref": "mcdisposableaieng",
        "installation_id": "0192a4f0-0000-7000-8000-0000000a1e00",
    },
}
CAPABILITY_ROLES = (
    "mission_control_runtime",
    "mission_control_family_writer",
    "mission_control_catalog_writer",
    "mission_control_outbox_worker",
    "mission_control_readonly",
    "mission_control_checkpointer",
)
# Exact expected additions; anything else in a protected object is a hold.
ALLOWED = {
    "format": "mission-control-allowed-differences/v1",
    "allowed": [
        *(
            {
                "path": f"roles/{role}",
                "change": "added",
                "reason": "release 0001 / runtime phase creates this restricted NOLOGIN role",
            }
            for role in CAPABILITY_ROLES
        ),
        *(
            {
                "path": f"memberships/{role}<-mc_migrator",
                "change": "added",
                "reason": "PostgreSQL 16+ grants the CREATEROLE creator ADMIN option only "
                "(no INHERIT/SET), so the migration login gains no capability",
            }
            for role in CAPABILITY_ROLES
        ),
    ],
}


def cli(*args: str, env: dict[str, str]) -> tuple[int, dict]:
    completed = subprocess.run(
        ["uv", "run", "--no-sync", "mission-db", *args],
        cwd=ROOT,
        env=env,
        check=False,
        capture_output=True,
        text=True,
        timeout=900,
    )
    lines = [line for line in completed.stdout.splitlines() if line.strip()]
    text = completed.stdout[completed.stdout.find("{") :] if "{" in completed.stdout else "{}"
    try:
        payload = json.loads(text)
    except ValueError:
        payload = {"raw": lines[-5:], "stderr": completed.stderr[-2000:]}
    return completed.returncode, payload


async def prepare_cluster(admin_dsn: str, app: str) -> str:
    """Create the migration login and the protected domain it owns; return its DSN."""
    parsed = urlsplit(admin_dsn)
    if parsed.hostname not in {"127.0.0.1", "localhost", "::1"}:
        raise SystemExit("disposable proof accepts loopback admin DSNs only")
    login, password = "mc_migrator", secrets.token_hex(16)
    admin = await asyncpg.connect(admin_dsn)
    try:
        await admin.execute(
            f"CREATE ROLE {login} LOGIN NOSUPERUSER CREATEROLE NOBYPASSRLS PASSWORD '{password}'"
        )
        await admin.execute(f"GRANT CREATE ON DATABASE postgres TO {login}")
        await admin.execute(f"GRANT CREATE ON SCHEMA public TO {login}")
        await admin.execute("CREATE SCHEMA IF NOT EXISTS extensions")
        await admin.execute("CREATE EXTENSION IF NOT EXISTS vector SCHEMA extensions")
        await admin.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm SCHEMA extensions")
        await admin.execute("GRANT USAGE ON SCHEMA extensions TO PUBLIC")
        roles = await create_domain_roles(admin, app.replace("-", ""))
        for role in roles.values():
            await admin.execute(f'GRANT "{role}" TO {login} WITH ADMIN OPTION')
        await admin.execute(f"SET ROLE {login}")
        await install_domain(admin, app, roles)
        await admin.execute("RESET ROLE")
    finally:
        await admin.close()
    host = "127.0.0.1" if parsed.hostname == "localhost" else parsed.hostname
    netloc = f"{login}:{quote(password, safe='')}@{host}:{parsed.port or 5432}"
    return urlunsplit(parsed._replace(netloc=netloc, path="/postgres"))


def write_target(directory: Path, app: str, dsn: str, env_name: str) -> None:
    facts = TARGETS[app]
    parsed = urlsplit(dsn)
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "target.toml").write_text(
        "format_version = 1\n\n[target]\n"
        f'app = "{app}"\napplication_id = "{app}"\n'
        f'project_label = "{facts["label"]}"\nproject_ref = "{facts["project_ref"]}"\n'
        f'installation_id = "{facts["installation_id"]}"\nenvironment = "disposable"\n'
        f'database_host = "{parsed.hostname}"\ndatabase_port = {parsed.port}\n'
        'database_name = "postgres"\ndatabase_user = "mc_migrator"\n'
        f'database_url_env = "{env_name}"\n\n[approval]\n'
        'approved_by = "lead:disposable-qualification"\n'
        f'identity_evidence = "local docker disposable cluster for {app}"\n',
        encoding="utf-8",
    )


def run_target(app: str, admin_env: str, run_id: str, scratch: Path) -> dict:
    admin_dsn = os.environ[admin_env]
    dsn = asyncio.run(prepare_cluster(admin_dsn, app))
    env_name = f"MC_PROOF_{app.upper().replace('-', '_')}_DSN"
    env = {**os.environ, env_name: dsn}
    deployments = scratch / "deployments"
    deployment = deployments / app
    write_target(deployment, app, dsn, env_name)
    out = ROOT / "docs" / "qualification" / "two-project" / TARGETS[app]["label"] / run_id
    out.mkdir(parents=True, exist_ok=True)
    allowed = scratch / "allowed-differences.json"
    allowed.write_text(json.dumps(ALLOWED, indent=2), encoding="utf-8")
    steps: dict[str, dict] = {}
    common = ["--deployment-dir", str(deployment), *VERSIONS]
    confirm = f"{TARGETS[app]['project_ref']}:{TARGETS[app]['installation_id']}"

    def step(name: str, *args: str) -> dict:
        code, payload = cli(*args, env=env)
        steps[name] = {"exit": code, "status": payload.get("status"), "result": payload}
        print(f"[{app}] {name}: exit={code} status={payload.get('status')}", flush=True)
        return payload

    step(
        "lock",
        "lock",
        "--app",
        app,
        "--component-root",
        str(COMPONENT),
        "--deployments-root",
        str(deployments),
    )
    step("qualify-before", "qualify", *common, "--phase", "before", "--out-dir", str(out))
    plan = step("plan", "plan", *common, "--out", str(scratch / f"{app}-plan.json"))
    step(
        "apply",
        "apply",
        *common,
        "--confirm-target",
        confirm,
        "--expected-plan-digest",
        str(plan.get("plan_digest", "")),
    )
    replan = step("replan", "plan", *common)
    step(
        "apply-replay",
        "apply",
        *common,
        "--confirm-target",
        confirm,
        "--expected-plan-digest",
        str(replan.get("plan_digest", "")),
    )
    step("verify", "verify", *common)
    descriptor = str(PACKAGE / "runtime" / "descriptor.json")
    step("runtime-plan", "runtime-plan", *common, "--descriptor", descriptor)
    step(
        "runtime-apply",
        "runtime-apply",
        *common,
        "--descriptor",
        descriptor,
        "--confirm-target",
        confirm,
        "--receipt-out",
        str(out / "runtime-receipts.json"),
    )
    step("runtime-replay", "runtime-plan", *common, "--descriptor", descriptor)
    bundles = [
        "--bundle",
        str(PACKAGE / "seeds" / "common"),
        "--bundle",
        str(PACKAGE / "seeds" / app),
        "--bundle",
        str(PACKAGE / "seeds" / "qualification"),
    ]
    seed_plan = step("seed-plan", "seed-plan", *common, *bundles)
    seeded = step("seed-apply", "seed-apply", *common, *bundles, "--confirm-target", confirm)
    seed_replay = step("seed-replay", "seed-apply", *common, *bundles, "--confirm-target", confirm)
    step(
        "qualify-after",
        "qualify",
        *common,
        "--phase",
        "after",
        "--out-dir",
        str(out),
        "--allowed",
        str(allowed),
    )
    (out / "seed-plan.json").write_text(json.dumps(seed_plan, indent=2, sort_keys=True), "utf-8")
    (out / "seed-receipts.json").write_text(
        json.dumps({"apply": seeded, "replay": seed_replay}, indent=2, sort_keys=True), "utf-8"
    )
    (out / "storage-reconciliation.json").write_text(
        json.dumps(
            {
                "status": "not_applicable_disposable",
                "reason": "Disposable PostgreSQL has no Supabase Storage service; bucket creation "
                "and storage.objects policy reconciliation were not exercised. Live targets "
                "require a separately approved storage plan.",
                "planned_private_buckets": ["mission-artifacts", "capability-bundles"],
            },
            indent=2,
        ),
        "utf-8",
    )
    (out / "recovery.json").write_text(
        json.dumps(
            {
                "status": "disposable_only",
                "tested": [
                    "SQL failure mid-release rolls back DDL, receipts and identity (package tests)",
                    "stale plan digest rejected (package tests)",
                    "interrupted concurrent runtime index held, no version row (package tests)",
                    "replayed apply, runtime and seed phases are no-ops (this run)",
                ],
                "live_recovery_point": "blocked: no approved backup/PITR recovery point for either "
                "live project",
            },
            indent=2,
        ),
        "utf-8",
    )
    (out / "cli-steps.json").write_text(
        json.dumps(
            {
                name: {k: v for k, v in item.items() if k != "result"} | {"status": item["status"]}
                for name, item in steps.items()
            },
            indent=2,
        ),
        "utf-8",
    )
    return steps


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--biotech-admin-dsn-env", required=True)
    parser.add_argument("--ai-engineer-admin-dsn-env", required=True)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    scratch = ROOT / ".scratch" / "two-disposable-proof" / args.run_id
    scratch.mkdir(parents=True, exist_ok=True)
    results = {
        "biotech": run_target("biotech", args.biotech_admin_dsn_env, args.run_id, scratch),
        "ai-engineer": run_target(
            "ai-engineer", args.ai_engineer_admin_dsn_env, args.run_id, scratch
        ),
    }
    failed = {
        app: [name for name, item in steps.items() if item["exit"] != 0]
        for app, steps in results.items()
    }
    print(json.dumps({"failed_steps": failed}, indent=2))
    return 0 if not any(failed.values()) else 2


if __name__ == "__main__":
    raise SystemExit(main())
