"""``mission-db`` administrator CLI.

Every command prints one structured, redacted JSON document. Exit 0 means the
requested operation passed; exit 2 means a blocked/failed gate (nothing beyond the
reported transaction committed). Credentials are only ever environment-variable
NAMES in target manifests; DSNs, row contents and driver messages are never printed.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from .canonical import read_json_object, write_json
from .errors import ContractError

EXIT_OK = 0
EXIT_BLOCKED = 2


def _emit(document: dict[str, Any]) -> None:
    sys.stdout.write(json.dumps(document, indent=2, sort_keys=True, default=str) + "\n")


def _add_target(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--target", type=Path, help="target.toml (default: <deployment-dir>/target.toml)"
    )
    parser.add_argument(
        "--deployment-dir",
        type=Path,
        help="deployments/<app> directory supplying target.toml and release.lock.json defaults",
    )


def _add_release(parser: argparse.ArgumentParser, *, versions: bool = True) -> None:
    parser.add_argument("--lock", type=Path, help="release.lock.json (default: <deployment-dir>)")
    parser.add_argument(
        "--release-root", type=Path, help="component release root (default: lock release_root)"
    )
    if versions:
        parser.add_argument("--reader-version", required=True, help="runtime reader version")
        parser.add_argument("--writer-version", required=True, help="runtime writer version")


def build_parser(prog: str = "mission-db") -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog=prog,
        description="Install, verify and qualify the common Mission Control database component. "
        "Exit 0 = passed, 2 = blocked gate.",
    )
    sub = parser.add_subparsers(dest="command", required=True, metavar="command")

    p = sub.add_parser("inspect", help="read-only inventory of a target (never mutates)")
    _add_target(p)

    p = sub.add_parser("plan", help="read-only plan bound to before-fingerprint/identity/release")
    _add_target(p)
    _add_release(p)
    p.add_argument("--out", type=Path, help="write plan.json here")

    p = sub.add_parser("apply", help="apply a reviewed plan atomically under the migration lock")
    _add_target(p)
    _add_release(p)
    p.add_argument(
        "--confirm-target", required=True, help="exact <project_ref>:<installation_uuid>"
    )
    p.add_argument("--expected-plan-digest", required=True, help="plan_digest from `plan`")
    p.add_argument("--statement-timeout-ms", type=int, default=120000)
    p.add_argument("--lock-timeout-ms", type=int, default=15000)

    p = sub.add_parser("verify", help="read-only verification of release, identity, roles")
    _add_target(p)
    _add_release(p)

    p = sub.add_parser("release-build", help="build manifest + generated contract on a scratch DB")
    p.add_argument("--component-root", type=Path, required=True)
    p.add_argument("--admin-dsn-env", required=True, help="NAME of env var with a loopback DSN")

    p = sub.add_parser("lock", help="write deployments/<app>/release.lock.json")
    p.add_argument("--app", required=True)
    p.add_argument("--component-root", type=Path, required=True)
    p.add_argument("--deployments-root", type=Path, required=True)

    p = sub.add_parser("snapshot", help="read-only protected-object snapshot (hashes only)")
    _add_target(p)
    p.add_argument("--out", type=Path, required=True)
    p.add_argument("--chunk-size", type=int, default=1000)
    p.add_argument("--statement-timeout-ms", type=int, default=60000)

    p = sub.add_parser("compare", help="diff two snapshots; nonzero exit on unallowed changes")
    p.add_argument("--before", type=Path, required=True)
    p.add_argument("--after", type=Path, required=True)
    p.add_argument("--allowed", type=Path, help="allowed-differences.json")
    p.add_argument("--out", type=Path)

    for name, help_text in (
        ("seed-plan", "read-only seed plan: replay/pending/conflict per bundle"),
        ("seed-apply", "apply seed bundles, one transaction per bundle"),
    ):
        p = sub.add_parser(name, help=help_text)
        _add_target(p)
        _add_release(p)
        p.add_argument(
            "--bundle",
            type=Path,
            action="append",
            required=True,
            help="bundle file or directory (repeatable)",
        )
        if name == "seed-apply":
            p.add_argument("--confirm-target", required=True)

    for name, help_text in (
        ("runtime-plan", "read-only runtime descriptor progress inspection"),
        ("runtime-apply", "run runtime descriptor steps on a dedicated session"),
    ):
        p = sub.add_parser(name, help=help_text)
        _add_target(p)
        _add_release(p)
        p.add_argument("--descriptor", type=Path, required=True)
        if name == "runtime-apply":
            p.add_argument("--confirm-target", required=True)
            p.add_argument("--receipt-out", type=Path, required=True)

    p = sub.add_parser("qualify", help="verify/plan + snapshot and write evidence files")
    _add_target(p)
    _add_release(p)
    p.add_argument("--phase", choices=("before", "after"), required=True)
    p.add_argument("--out-dir", type=Path, required=True)
    p.add_argument("--snapshot-target", type=Path, help="separate read-only snapshot target")
    p.add_argument("--allowed", type=Path, help="allowed-differences.json for protected-diff")
    p.add_argument("--chunk-size", type=int, default=1000)
    return parser


def _resolve_paths(args: argparse.Namespace, default_deployment_dir: Path | None) -> None:
    directory = getattr(args, "deployment_dir", None) or default_deployment_dir
    if hasattr(args, "target") and args.target is None:
        if directory is None:
            raise ContractError("--target or --deployment-dir is required")
        args.target = directory / "target.toml"
    if hasattr(args, "lock") and args.lock is None:
        if directory is None:
            raise ContractError("--lock or --deployment-dir is required")
        args.lock = directory / "release.lock.json"


def _release(args: argparse.Namespace) -> Any:
    from .integrity import load_release, resolve_release_root

    return load_release(args.lock, resolve_release_root(args.lock, args.release_root))


def _versions(args: argparse.Namespace) -> dict[str, str]:
    return {"reader_version": args.reader_version, "writer_version": args.writer_version}


async def _run(args: argparse.Namespace) -> tuple[int, dict[str, Any]]:
    from .target import load_target

    command = args.command
    if command == "release-build":
        from .release import release_build

        return EXIT_OK, await release_build(args.component_root, args.admin_dsn_env)
    if command == "lock":
        from .release import write_lock

        return EXIT_OK, write_lock(args.component_root, args.deployments_root, args.app)
    if command == "compare":
        from .snapshot import compare

        allowed = read_json_object(args.allowed) if args.allowed else None
        result = compare(read_json_object(args.before), read_json_object(args.after), allowed)
        if args.out:
            write_json(args.out, result)
        return (EXIT_OK if result["status"] != "blocked" else EXIT_BLOCKED), result
    target = load_target(args.target)
    if command == "inspect":
        from .deployment import inspect_target

        return EXIT_OK, await inspect_target(target)
    if command == "snapshot":
        from .snapshot import snapshot_target

        result = await snapshot_target(
            target, chunk_size=args.chunk_size, statement_timeout_ms=args.statement_timeout_ms
        )
        write_json(args.out, result)
        return EXIT_OK, {
            "status": "captured",
            "out": str(args.out),
            "schemas": sorted(result["schemas"]),
            "mutations": [],
        }
    release = _release(args)
    if command == "plan":
        from .deployment import plan_release

        result = await plan_release(target, release, **_versions(args))
        if args.out:
            write_json(args.out, result)
        return (EXIT_BLOCKED if result["plan"]["holds"] else EXIT_OK), result
    if command == "apply":
        from .deployment import apply_release

        return EXIT_OK, await apply_release(
            target,
            release,
            confirmation=args.confirm_target,
            expected_plan_digest=args.expected_plan_digest,
            statement_timeout_ms=args.statement_timeout_ms,
            lock_timeout_ms=args.lock_timeout_ms,
            **_versions(args),
        )
    if command == "verify":
        from .deployment import verify_release

        return EXIT_OK, await verify_release(target, release, **_versions(args))
    if command in {"seed-plan", "seed-apply"}:
        from .seeds import load_bundles, seed_apply, seed_plan

        bundles = load_bundles(args.bundle)
        if command == "seed-plan":
            result = await seed_plan(target, release, bundles, **_versions(args))
            return (EXIT_BLOCKED if result["status"] == "hold" else EXIT_OK), result
        return EXIT_OK, await seed_apply(
            target, release, bundles, confirmation=args.confirm_target, **_versions(args)
        )
    if command in {"runtime-plan", "runtime-apply"}:
        from .runtime import load_descriptor, runtime_apply, runtime_plan

        descriptor = load_descriptor(args.descriptor)
        if command == "runtime-plan":
            result = await runtime_plan(target, release, descriptor, **_versions(args))
            return (EXIT_BLOCKED if result["status"] == "hold" else EXIT_OK), result
        result = await runtime_apply(
            target,
            release,
            descriptor,
            confirmation=args.confirm_target,
            receipt_path=args.receipt_out,
            reader_version=args.reader_version,
            writer_version=args.writer_version,
        )
        return EXIT_OK, result
    if command == "qualify":
        from .qualify import qualify

        snapshot_target_manifest = (
            load_target(args.snapshot_target) if args.snapshot_target else None
        )
        allowed = read_json_object(args.allowed) if args.allowed else None
        result = await qualify(
            target,
            release,
            phase=args.phase,
            out_dir=args.out_dir,
            snapshot_from=snapshot_target_manifest,
            allowed=allowed,
            chunk_size=args.chunk_size,
            **_versions(args),
        )
        return (EXIT_OK if result["status"] == "pass" else EXIT_BLOCKED), result
    raise ContractError(f"Unknown command {command}")


def main(
    argv: Sequence[str] | None = None,
    *,
    prog: str = "mission-db",
    default_deployment_dir: Path | None = None,
) -> int:
    parser = build_parser(prog)
    args = parser.parse_args(argv)
    try:
        _resolve_paths(args, default_deployment_dir)
        code, document = asyncio.run(_run(args))
    except ContractError as exc:
        _emit({"status": "blocked", "code": exc.code, "error": str(exc), "mutations": []})
        return EXIT_BLOCKED
    except OSError:
        _emit(
            {
                "status": "blocked",
                "code": "LOCAL_INPUT",
                "error": "Cannot access required local input",
                "mutations": [],
            }
        )
        return EXIT_BLOCKED
    _emit(document)
    return code


if __name__ == "__main__":
    raise SystemExit(main())
