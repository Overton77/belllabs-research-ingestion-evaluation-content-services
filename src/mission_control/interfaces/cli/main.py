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

    def transcript(self, run_id: str, params: dict[str, Any], *, markdown: bool) -> httpx.Response:
        return self.client.get(
            f"{self.prefix}/runs/{self._id(run_id)}/transcript",
            params=params,
            headers={"Accept": "text/markdown" if markdown else "application/x-ndjson"},
        )

    def frames_tail_url(self, run_id: str) -> str:
        return f"{self.prefix}/runs/{self._id(run_id)}/frames/tail"

    @staticmethod
    def _id(value: str) -> str:
        if not value or value in {".", ".."}:
            raise ValueError("run id must be a nonempty identifier")
        return quote(value, safe="")


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


TRANSCRIPT_NEXT_CURSOR = "x-transcript-next-cursor"
DEFAULT_FOLLOW_SECONDS = 60.0


def _transcript_params(args: argparse.Namespace, since: str | None) -> dict[str, Any]:
    params: dict[str, Any] = {"limit": args.limit}
    for name in ("activation", "node", "kinds", "subordinate"):
        value = getattr(args, name, None)
        if value:
            params[name] = value
    if since:
        params["since"] = since
    if args.canonical_only:
        params["canonical_only"] = "true"
    if args.full:
        params["full"] = "true"
    return params


def _transcript_error(response: httpx.Response) -> int:
    try:
        detail = response.json().get("detail", {})
    except ValueError:
        detail = {}
    code = detail.get("code") if isinstance(detail, dict) else None
    print(json.dumps({"error": code or "transcript_failed", "status": response.status_code}))
    if code == "CURSOR_EXPIRED":
        return 2
    return exit_status(response.status_code) or 5


def _write_utf8(text: str) -> None:
    """Transcripts carry ✓ and other non-ASCII text; never depend on the console code page."""

    buffer = getattr(sys.stdout, "buffer", None)
    if buffer is not None:
        buffer.write(text.encode("utf-8"))
        buffer.flush()
    else:
        sys.stdout.write(text)
        sys.stdout.flush()


def run_transcript(client: MissionClient, args: argparse.Namespace, wait: float | None) -> int:
    """`run transcript`: print entries (JSONL or Markdown); `--follow` tails new entries."""

    markdown = args.format == "md"
    since = args.since
    deadline = time.monotonic() + (wait or DEFAULT_FOLLOW_SECONDS)
    first = True
    while True:
        response = client.transcript(
            args.run_id, _transcript_params(args, since), markdown=markdown
        )
        if response.status_code != 200:
            return _transcript_error(response)
        text = response.text
        next_cursor = response.headers.get(TRANSCRIPT_NEXT_CURSOR) or since
        # Follow prints only pages that advanced the cursor (Markdown always has a header).
        if text and (first or next_cursor != since):
            _write_utf8(text)
        first = False
        since = next_cursor
        if not args.follow:
            return 0
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            print(json.dumps({"error": "wait_timeout", "next_cursor": since}), file=sys.stderr)
            return 6
        time.sleep(min(1.0, remaining))


def run_frames_tail(client: MissionClient, args: argparse.Namespace, wait: float | None) -> int:
    """`run frames --tail`: the non-canonical SSE tail, printed as JSON lines."""

    seconds = wait or DEFAULT_FOLLOW_SECONDS
    params: dict[str, Any] = {"timeout": seconds}
    if args.after:
        params["after"] = args.after
    with client.client.stream(
        "GET", client.frames_tail_url(args.run_id), params=params, timeout=seconds + 30
    ) as response:
        if response.status_code != 200:
            response.read()
            return _transcript_error(response)
        event = ""
        for line in response.iter_lines():
            if line.startswith("event: "):
                event = line.removeprefix("event: ")
            elif line.startswith("data: "):
                data = json.loads(line.removeprefix("data: "))
                print(json.dumps({"event": event, **data}, ensure_ascii=False))
                if event == "end":
                    return 6 if data.get("reason") == "timeout" and args.until_end else 0
    return 0


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
    # SPEC-03 (C3): the run transcript and the non-canonical frame tail.
    transcript = run_commands.add_parser("transcript", parents=[common])
    transcript.add_argument("run_id")
    transcript.add_argument("--format", choices=("jsonl", "md"), default="jsonl")
    transcript.add_argument("--since")
    transcript.add_argument("--activation")
    transcript.add_argument("--node")
    transcript.add_argument("--kinds", help="comma-separated entry kinds")
    transcript.add_argument("--canonical-only", action="store_true")
    transcript.add_argument("--subordinate")
    transcript.add_argument("--limit", type=int, default=500)
    transcript.add_argument("--follow", action="store_true")
    transcript.add_argument("--full", action="store_true")
    frames = run_commands.add_parser("frames", parents=[common])
    frames.add_argument("run_id")
    frames.add_argument("--tail", action="store_true", required=True)
    frames.add_argument("--after")
    frames.add_argument("--until-end", action="store_true", help="exit 6 when the tail times out")
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
    for action in ("resolve", "search", "discover", "inspect", "components"):
        operation = catalog_commands.add_parser(action, parents=[common])
        operation.add_argument("--request-file", required=True)
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
        if deadline_seconds is not None and (
            args.group != "run" or args.action not in {"inspect", "transcript", "frames"}
        ):
            raise ValueError(
                "--wait is supported on run inspect, transcript and frames; "
                "command admission is not completion"
            )
        body = strict_object(args.request_file) if hasattr(args, "request_file") else None
        with httpx.Client(
            base_url=base_url,
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
            follow_redirects=False,
        ) as transport:
            client = MissionClient(transport, application)
            if args.group == "run" and args.action == "transcript":
                return run_transcript(client, args, deadline_seconds)
            if args.group == "run" and args.action == "frames":
                return run_frames_tail(client, args, deadline_seconds)
            deadline = time.monotonic() + (deadline_seconds or 0)
            while True:
                if deadline_seconds is not None:
                    transport.timeout = httpx.Timeout(
                        max(0.001, min(30, deadline - time.monotonic()))
                    )
                if args.group == "catalog":
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
