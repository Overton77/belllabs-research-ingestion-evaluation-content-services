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
        if action not in {"resolve", "search", "discover", "inspect"}:
            raise ValueError("unknown catalog action")
        return self.client.post(f"{self.prefix}/catalog/{action}", json=body)

    def subscriptions(
        self, action: str, body: dict[str, Any] | None, target: str | None
    ) -> httpx.Response:
        if action == "create":
            return self.client.post(f"{self.prefix}/subscriptions", json=body)
        if action == "list":
            return self.client.get(f"{self.prefix}/subscriptions")
        if action == "close" and target:
            return self.client.delete(f"{self.prefix}/subscriptions/{self._id(target)}")
        raise ValueError("subscribe close needs a subscription id")

    def events_path(self, mission_id: str) -> str:
        return f"{self.prefix}/missions/{self._id(mission_id)}/events"

    @staticmethod
    def _id(value: str) -> str:
        if not value or value in {".", ".."}:
            raise ValueError("run id must be a nonempty identifier")
        return quote(value, safe="")


def subscription_body(args: argparse.Namespace) -> dict[str, Any]:
    run_id, mission_id = getattr(args, "run", None), getattr(args, "mission", None)
    if (run_id is None) == (mission_id is None):
        raise ValueError("subscribe needs exactly one of --run or --mission")
    if not getattr(args, "webhook", None) or not getattr(args, "secret_ref", None):
        raise ValueError("subscribe needs --webhook URL and --secret-ref provider:KEY")
    events = [item for item in getattr(args, "events", "").split(",") if item]
    if not events:
        raise ValueError("subscribe needs --events a,b")
    body: dict[str, Any] = {
        "target": "run" if run_id else "mission",
        "target_id": run_id or mission_id,
        "events": events,
        "channel": {"kind": "webhook", "url": args.webhook, "secret_ref": args.secret_ref},
    }
    if getattr(args, "node_keys", None):
        body["node_keys"] = [item for item in args.node_keys.split(",") if item]
    if getattr(args, "after_seq", None) is not None:
        body["after_seq"] = args.after_seq
    return body


def watch_events(transport: httpx.Client, client: MissionClient, args: argparse.Namespace) -> int:
    """Print each mission event as one JSON line; exit 6 on resync_required."""

    params: dict[str, Any] = {}
    if getattr(args, "after_seq", None) is not None:
        params["after_seq"] = args.after_seq
    if getattr(args, "types", None):
        params["types"] = args.types
    limit = getattr(args, "max_events", None)
    seen = 0
    with transport.stream(
        "GET", client.events_path(args.mission_id), params=params, timeout=None
    ) as response:
        if response.status_code >= 300:
            response.read()
            print(json.dumps({"error": "events_unavailable", "status": response.status_code}))
            return exit_status(response.status_code)
        event = ""
        for line in response.iter_lines():
            if line.startswith("event: "):
                event = line[len("event: ") :]
            elif line.startswith("data: ") and event in {"mission_event", "resync_required"}:
                print(line[len("data: ") :], flush=True)
                if event == "resync_required":
                    return 6
                seen += 1
                if limit is not None and seen >= limit:
                    return 0
    return 0


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


def contract_schema_texts() -> dict[str, str]:
    """Generated JSON Schemas committed under ``src/mission_control/contracts/schemas``."""

    from mission_control.domain.authoring.manifest import (
        MANIFEST_SCHEMA_ID,
        manifest_json_schema_text,
    )
    from mission_control.domain.composition.chain import chain_contract_schemas

    texts = {MANIFEST_SCHEMA_ID: manifest_json_schema_text()}
    for name, schema in chain_contract_schemas().items():
        texts[name] = json.dumps(schema, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    return texts


def mission_local(args: argparse.Namespace) -> int:
    """Manifest verbs that need no service: JSON Schema export and the structural compile.

    ``compile`` validates shape, inheritance and chain links offline and persists nothing;
    catalog search-to-pin resolution (FT-E2) is reported as deferred.
    """

    try:
        if args.action == "schema":
            text = contract_schema_texts()[getattr(args, "contract", "mc.mission_manifest.v1")]
            if getattr(args, "out", None):
                Path(args.out).write_text(text, encoding="utf-8", newline="\n")
            else:
                sys.stdout.write(text)
            return 0
        from mission_control.application.chains.service import ManifestStructureService

        report = ManifestStructureService().compile(Path(args.file).read_text(encoding="utf-8"))
        if getattr(args, "json", False):
            print(report.model_dump_json(by_alias=True))
        else:
            print(json.dumps(report.model_dump(mode="json", by_alias=True), indent=2))
        return 0 if report.ok else 2
    except OSError as exc:
        print(json.dumps({"error": str(exc)}), file=sys.stderr)
        return 2


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
    mission = groups.add_parser("mission", parents=[common])
    mission_commands = mission.add_subparsers(dest="action", required=True)
    schema = mission_commands.add_parser("schema", parents=[common])
    schema.add_argument("--out")
    schema.add_argument(
        "--contract",
        choices=("mc.mission_manifest.v1", "mc.chain.v1", "mc.chain_link.v1"),
        default="mc.mission_manifest.v1",
    )
    compile_ = mission_commands.add_parser("compile", parents=[common])
    compile_.add_argument("file")
    subscribe = groups.add_parser("subscribe", parents=[common])
    subscribe.add_argument("action", nargs="?", choices=("create", "list", "close"))
    subscribe.add_argument("subscription_id", nargs="?")
    subscribe.add_argument("--run")
    subscribe.add_argument("--mission")
    subscribe.add_argument("--webhook")
    subscribe.add_argument("--secret-ref")
    subscribe.add_argument("--events", default="")
    subscribe.add_argument("--node-keys")
    subscribe.add_argument("--after-seq", type=int)
    events = groups.add_parser("events", parents=[common])
    events_commands = events.add_subparsers(dest="action", required=True)
    watch = events_commands.add_parser("watch", parents=[common])
    watch.add_argument("mission_id")
    watch.add_argument("--after-seq", type=int)
    watch.add_argument("--types")
    watch.add_argument("--max-events", type=int)
    catalog = groups.add_parser("catalog", parents=[common])
    catalog_commands = catalog.add_subparsers(dest="action", required=True)
    catalog_commands.add_parser("list", parents=[common])
    for action in ("resolve", "search", "discover", "inspect", "components"):
        operation = catalog_commands.add_parser(action, parents=[common])
        operation.add_argument("--request-file", required=True)
    args = parser.parse_args(argv)
    if args.group == "mission":
        return mission_local(args)
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
        if args.group == "subscribe":
            args.action = args.action or "create"
        body = strict_object(args.request_file) if hasattr(args, "request_file") else None
        if args.group == "subscribe" and args.action == "create":
            body = subscription_body(args)
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
                if args.group == "events":
                    return watch_events(transport, client, args)
                if args.group == "subscribe":
                    response = client.subscriptions(
                        args.action, body, getattr(args, "subscription_id", None)
                    )
                elif args.group == "catalog":
                    response = client.catalog(args.action, body)
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
