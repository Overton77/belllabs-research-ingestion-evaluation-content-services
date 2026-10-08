"""missionctl: a thin authenticated client, never a second lifecycle authority."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlsplit

import httpx

from mission_control.contracts.json import parse_json_object
from mission_control.domain.capabilities.bundles import build_bundle_manifest, skipped

_BUNDLE_PREFIXES = {"skill_bundle": "skill", "hook_script": "hook", "subagent_profile": "subagent"}


def strict_object(path: str) -> dict[str, Any]:
    return parse_json_object(Path(path).read_text(encoding="utf-8"))


class MissionClient:
    def __init__(self, client: httpx.Client, application_id: str) -> None:
        if not application_id or application_id in {".", ".."}:
            raise ValueError("application id must be a nonempty identifier")
        self.client = client
        self.prefix = f"/v1/applications/{quote(application_id, safe='')}"

    def inspection(self, run_id: str) -> httpx.Response:
        return self.client.get(f"{self.prefix}/runs/{self._id(run_id)}/inspection")

    def command(self, run_id: str, body: dict[str, Any]) -> httpx.Response:
        return self.client.post(
            f"{self.prefix}/runs/{self._id(run_id)}/commands",
            json=body,
            headers={"Idempotency-Key": str(body.get("request_id", ""))},
        )

    def commands(self, run_id: str) -> httpx.Response:
        return self.client.get(f"{self.prefix}/runs/{self._id(run_id)}/commands")

    def admit(self, body: dict[str, Any]) -> httpx.Response:
        return self.client.post(
            f"{self.prefix}/run-requests",
            json=body,
            headers={"Idempotency-Key": str(body.get("request_id", ""))},
        )

    def runtime_action(self, run_id: str, action: str, body: dict[str, Any]) -> httpx.Response:
        suffix = {
            "snapshot": "snapshots",
            "fork": "forks",
            "reconcile": "reconcile-unit",
            "start": "launch",
        }[action]
        headers = {"Idempotency-Key": str(body["request_id"])} if "request_id" in body else {}
        return self.client.post(
            f"{self.prefix}/runs/{self._id(run_id)}/{suffix}",
            json=body,
            headers=headers,
        )

    def catalog(self, action: str, body: dict[str, Any] | None = None) -> httpx.Response:
        if action == "list":
            return self.client.get(f"{self.prefix}/catalog/definitions")
        if action == "components":
            return self.client.post(f"{self.prefix}/catalog/components/search", json=body)
        if action not in {"resolve", "search", "discover", "inspect", "pin", "render"}:
            raise ValueError("unknown catalog action")
        return self.client.post(f"{self.prefix}/catalog/{action}", json=body)

    def lanes(self, profile: str | None = None) -> httpx.Response:
        """FT-G1: `lane list` and `lane describe PROFILE`."""

        if profile is None:
            return self.client.get(f"{self.prefix}/lanes")
        return self.client.get(f"{self.prefix}/lanes/{self._id(profile)}")

    def catalog_pin(self, pin: str) -> httpx.Response:
        """FT-A8: inspect one Capability Pin (body, host support, members)."""
        return self.client.get(f"{self.prefix}/catalog/pins/{quote(pin, safe='')}")

    def publish(self, phase: str, body: dict[str, Any]) -> httpx.Response:
        if phase not in {"prepare", "complete"}:
            raise ValueError("unknown publish phase")
        return self.client.post(f"{self.prefix}/catalog/publish:{phase}", json=body)

    @staticmethod
    def _id(value: str) -> str:
        if not value or value in {".", ".."}:
            raise ValueError("run id must be a nonempty identifier")
        return quote(value, safe="")


def read_bundle_directory(directory: Path) -> list[tuple[str, bytes]]:
    """Regular files under ``directory`` (no links), skipping ``.git`` and ``node_modules``."""
    root = directory.resolve()
    if not root.is_dir():
        raise ValueError(f"bundle directory not found: {directory}")
    files: list[tuple[str, bytes]] = []
    for path in sorted(root.rglob("*")):
        relative = path.relative_to(root).as_posix()
        if skipped(relative):
            continue
        if path.is_symlink():
            raise ValueError(f"bundle directories cannot contain links: {relative}")
        if path.is_file():
            files.append((relative, path.read_bytes()))
    if not files:
        raise ValueError("bundle directory is empty")
    return files


def publish_bundle(
    client: MissionClient,
    storage: httpx.Client,
    application: str,
    args: argparse.Namespace,
) -> tuple[dict[str, Any], int]:
    """`catalog publish`: hash, signed-URL upload of missing objects, register proposed."""
    directory = Path(args.dir)
    files = read_bundle_directory(directory)
    kind = str(args.kind)
    capability_id = getattr(args, "capability_id", None) or (
        f"{_BUNDLE_PREFIXES[kind]}.{directory.resolve().name}"
    )
    manifest = build_bundle_manifest(
        files,
        application_id=application,
        kind=kind,  # type: ignore[arg-type]
        capability_id=capability_id,
        version=getattr(args, "version", None) or "1",
        executable=frozenset(path for path, content in files if content.startswith(b"#!")),
    )
    definition = (
        strict_object(args.definition_file) if getattr(args, "definition_file", None) else {}
    )
    body = {"manifest": manifest.model_dump(mode="json"), "definition": definition}
    prepared = client.publish("prepare", body)
    if exit_status(prepared.status_code):
        return _json(prepared), exit_status(prepared.status_code)
    plan = prepared.json()
    contents = dict(files)
    prefix = str(plan["object_prefix"]) + "/"
    for upload in plan.get("uploads", []):
        relative = str(upload["path"]).removeprefix(prefix)
        url = urlsplit(str(upload["url"]))
        if (
            url.username
            or url.password
            or not (
                url.scheme == "https"
                or (url.scheme == "http" and url.hostname in {"localhost", "127.0.0.1", "::1"})
            )
        ):
            raise ValueError("signed upload URLs must be HTTPS (HTTP only for loopback)")
        response = storage.put(
            str(upload["url"]),
            files={"file": (relative.rsplit("/", 1)[-1], contents[relative])},
            headers={"x-upsert": "false", "cache-control": "max-age=3600"},
        )
        if response.status_code not in {200, 201, 409}:
            return {"error": "bundle_upload_failed", "path": relative}, 5
    completed = client.publish("complete", body)
    return _json(completed), exit_status(completed.status_code)


def catalog_flags_body(args: argparse.Namespace) -> dict[str, Any]:
    """Build a search or pin body from flags (``tenant_scope`` defaults on the server)."""
    query = getattr(args, "query", None)
    if not query:
        raise ValueError("--query or --request-file is required")
    body: dict[str, Any] = {"query": query}
    if getattr(args, "kinds", None):
        body["kinds"] = list(args.kinds)
    if getattr(args, "hosts", None):
        body["host_profiles"] = list(args.hosts)
    if getattr(args, "side_effects", None):
        body["side_effect_classes"] = list(args.side_effects)
    if getattr(args, "limit", None) is not None:
        body["limit"] = args.limit
    if getattr(args, "margin", None) is not None:
        body["margin"] = args.margin
    return body


def catalog_response(
    client: MissionClient, args: argparse.Namespace, body: dict[str, Any] | None
) -> httpx.Response:
    action = args.action
    if action in {"search", "pin"} and body is None:
        body = catalog_flags_body(args)
    if action == "inspect" and body is None:
        if not getattr(args, "pin", None):
            raise ValueError("catalog inspect needs --pin or --request-file")
        return client.catalog_pin(args.pin)
    if action == "render":
        body = {"pin": args.pin, "host": args.host}
    return client.catalog(action, body)


def _json(response: httpx.Response) -> dict[str, Any]:
    try:
        payload = response.json()
    except ValueError:
        return {"error": "non-JSON service response", "status": response.status_code}
    return payload if isinstance(payload, dict) else {"result": payload}


def exit_status(status: int) -> int:
    if 200 <= status < 300:
        return 0
    if status in {401, 403}:
        return 3
    if status == 409:
        return 4
    if status in {400, 404, 422}:
        return 2
    return 5


def main(argv: list[str] | None = None) -> int:
    # Common options at every depth allow flags before or after subcommands.
    common = argparse.ArgumentParser(add_help=False, argument_default=argparse.SUPPRESS)
    common.add_argument("--application")
    common.add_argument("--url")
    common.add_argument("--json", action="store_true")
    common.add_argument("--wait", type=float)
    parser = argparse.ArgumentParser(prog="missionctl", parents=[common])
    groups = parser.add_subparsers(dest="group", required=True)
    run = groups.add_parser("run", parents=[common])
    run_commands = run.add_subparsers(dest="action", required=True)
    inspect = run_commands.add_parser("inspect", parents=[common])
    inspect.add_argument("run_id")
    admission = run_commands.add_parser("admit", parents=[common])
    admission.add_argument("--request-file", required=True)
    for action in ("snapshot", "fork", "reconcile", "start"):
        operation = run_commands.add_parser(action, parents=[common])
        operation.add_argument("run_id")
        operation.add_argument("--request-file", required=True)
    command = groups.add_parser("command", parents=[common])
    commands = command.add_subparsers(dest="action", required=True)
    send = commands.add_parser("send", parents=[common])
    send.add_argument("run_id")
    send.add_argument("--request-file", required=True)
    listing = commands.add_parser("list", parents=[common])
    listing.add_argument("run_id")
    catalog = groups.add_parser("catalog", parents=[common])
    catalog_commands = catalog.add_subparsers(dest="action", required=True)
    catalog_commands.add_parser("list", parents=[common])
    for action in ("resolve", "discover", "components"):
        operation = catalog_commands.add_parser(action, parents=[common])
        operation.add_argument("--request-file", required=True)
    lane = groups.add_parser("lane", parents=[common])
    lane_commands = lane.add_subparsers(dest="action", required=True)
    lane_commands.add_parser("list", parents=[common])
    lane_describe = lane_commands.add_parser("describe", parents=[common])
    lane_describe.add_argument("lane_profile")
    # FT-A8: search and pin take flags (or a request file); inspect takes --pin or a file.
    for action in ("search", "pin"):
        operation = catalog_commands.add_parser(action, parents=[common])
        operation.add_argument("--request-file")
        operation.add_argument("--query")
        operation.add_argument("--kind", action="append", dest="kinds")
        operation.add_argument("--host", action="append", dest="hosts")
        operation.add_argument("--side-effect", action="append", dest="side_effects")
        operation.add_argument("--limit", type=int)
        if action == "pin":
            operation.add_argument("--margin", type=float)
    inspect_catalog = catalog_commands.add_parser("inspect", parents=[common])
    inspect_catalog.add_argument("--request-file")
    inspect_catalog.add_argument("--pin")
    render = catalog_commands.add_parser("render", parents=[common])
    render.add_argument("--pin", required=True)
    render.add_argument("--host", required=True)
    publish = catalog_commands.add_parser("publish", parents=[common])
    publish.add_argument("--dir", required=True)
    publish.add_argument(
        "--kind", required=True, choices=("skill_bundle", "hook_script", "subagent_profile")
    )
    publish.add_argument("--capability-id", dest="capability_id")
    publish.add_argument("--version")
    publish.add_argument("--definition-file", dest="definition_file")
    args = parser.parse_args(argv)
    application = getattr(args, "application", os.environ.get("MISSION_CONTROL_APPLICATION_ID"))
    base_url = getattr(args, "url", os.environ.get("MISSION_CONTROL_URL", "http://127.0.0.1:8000"))
    token = os.environ.get("MISSION_CONTROL_TOKEN")
    deadline_seconds = getattr(args, "wait", None)
    try:
        if not application or not token:
            raise ValueError("application and MISSION_CONTROL_TOKEN are required")
        parsed_url = urlsplit(base_url)
        if parsed_url.username or parsed_url.password or parsed_url.query or parsed_url.fragment:
            raise ValueError("API URL must not contain credentials, query, or fragment")
        if parsed_url.scheme != "https" and not (
            parsed_url.scheme == "http" and parsed_url.hostname in {"localhost", "127.0.0.1", "::1"}
        ):
            raise ValueError("API URL must use HTTPS (HTTP is allowed only for loopback)")
        if deadline_seconds is not None and not 0 < deadline_seconds <= 3600:
            raise ValueError("--wait must be greater than zero and at most 3600 seconds")
        if deadline_seconds is not None and (args.group != "run" or args.action != "inspect"):
            raise ValueError(
                "--wait is supported on run inspect; command admission is not completion"
            )
        body = strict_object(args.request_file) if getattr(args, "request_file", None) else None
        if args.group == "catalog" and args.action == "publish":
            with (
                httpx.Client(
                    base_url=base_url,
                    headers={"Authorization": f"Bearer {token}"},
                    timeout=30,
                    follow_redirects=False,
                ) as transport,
                # Storage uploads carry only the signed URL token, never the API bearer.
                httpx.Client(timeout=120, follow_redirects=False) as storage,
            ):
                result, status = publish_bundle(
                    MissionClient(transport, application), storage, application, args
                )
            print(json.dumps(result, allow_nan=False))
            return status
        with httpx.Client(
            base_url=base_url,
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
            follow_redirects=False,
        ) as transport:
            client = MissionClient(transport, application)
            deadline = time.monotonic() + (deadline_seconds or 0)
            while True:
                if deadline_seconds is not None:
                    transport.timeout = httpx.Timeout(
                        max(0.001, min(30, deadline - time.monotonic()))
                    )
                if args.group == "catalog":
                    response = catalog_response(client, args, body)
                elif args.group == "lane":
                    response = client.lanes(getattr(args, "lane_profile", None))
                elif args.action == "inspect":
                    response = client.inspection(args.run_id)
                elif args.action == "admit" and body is not None:
                    response = client.admit(body)
                elif args.group == "run" and body is not None:
                    response = client.runtime_action(args.run_id, args.action, body)
                elif body is not None:
                    response = client.command(args.run_id, body)
                else:
                    response = client.commands(args.run_id)
                try:
                    result = response.json()
                except ValueError:
                    result = {"error": "non-JSON service response", "status": response.status_code}
                status = exit_status(response.status_code)
                if status or deadline_seconds is None:
                    print(json.dumps(result, allow_nan=False))
                    return status
                if isinstance(result, dict) and result.get("lifecycle") == "completed":
                    print(json.dumps(result, allow_nan=False))
                    return 0 if result.get("execution_outcome") == "completed" else 6
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    print(json.dumps({"error": "wait_timeout", "inspection": result}))
                    return 6
                time.sleep(min(1, remaining))
    except (ValueError, OSError) as exc:
        print(json.dumps({"error": str(exc)}), file=sys.stderr)
        return 2
    except httpx.HTTPError:
        # Do not echo request URLs/headers or accidentally disclose credentials.
        print(json.dumps({"error": "service_unavailable"}), file=sys.stderr)
        return 5
