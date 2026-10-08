"""missionctl: a thin authenticated client, never a second lifecycle authority."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import uuid
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

    def transcript(self, run_id: str, params: dict[str, Any], *, markdown: bool) -> httpx.Response:
        return self.client.get(
            f"{self.prefix}/runs/{self._id(run_id)}/transcript",
            params=params,
            headers={"Accept": "text/markdown" if markdown else "application/x-ndjson"},
        )

    def frames_tail_url(self, run_id: str) -> str:
        return f"{self.prefix}/runs/{self._id(run_id)}/frames/tail"

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


MAILBOX_FILE_FIELDS = frozenset(
    {
        "kind",
        "boundary",
        "text",
        "content",
        "content_ref",
        "content_digest",
        "media_type",
        "size_bytes",
        "expand",
        "node_key",
        "deadline",
        "reason",
        "request_id",
    }
)


def mailbox_payload(path: str, args: argparse.Namespace) -> tuple[str, dict[str, Any], str, str]:
    """`command queue --file`: a JSON object (`text` or `content`, `boundary`, `node_key`,
    `expand`, `deadline`, `reason`, `request_id`) or any other file read as instruction text.

    Returns (kind, payload, reason, request_id). Flags override nothing the file states.
    """

    raw = Path(path).read_text(encoding="utf-8")
    spec: dict[str, Any] | None = None
    if raw.lstrip().startswith("{"):
        spec = parse_json_object(raw)
        unknown = set(spec) - MAILBOX_FILE_FIELDS
        if unknown:
            raise ValueError(f"unknown queue file fields: {sorted(unknown)}")
    spec = spec or {"text": raw}
    kind = str(spec.get("kind") or ("add_context" if args.add_context else "queue_instruction"))
    if kind not in {"queue_instruction", "add_context"}:
        raise ValueError("command queue sends queue_instruction or add_context")
    if "content" in spec:
        content = spec["content"]
    elif "text" in spec:
        content = {"text": spec["text"]}
        if "media_type" in spec:
            content["media_type"] = spec["media_type"]
    elif "content_ref" in spec:
        content = {
            "artifact_ref": spec["content_ref"],
            "content_digest": spec.get("content_digest"),
            "media_type": spec.get("media_type", "text/markdown"),
            "size_bytes": spec.get("size_bytes", 0),
        }
    else:
        raise ValueError("a queue file needs text, content or content_ref")
    payload: dict[str, Any] = {
        "boundary": spec.get("boundary") or args.boundary,
        "content": content,
    }
    node_key = spec.get("node_key") or args.node_key
    if node_key:
        payload["node_key"] = node_key
    if spec.get("deadline"):
        payload["deadline"] = spec["deadline"]
    if kind == "add_context":
        payload["expand"] = spec.get("expand") or args.expand
    reason = str(spec.get("reason") or args.reason)
    request_id = str(spec.get("request_id") or args.request_id or uuid.uuid4())
    return kind, payload, reason, request_id


def instruction_content(path: str) -> dict[str, Any]:
    """An instruction file: JSON `content` / `text` / `content_ref`, or plain text."""

    raw = Path(path).read_text(encoding="utf-8")
    if not raw.lstrip().startswith("{"):
        return {"text": raw}
    spec = parse_json_object(raw)
    if "content" in spec:
        content = spec["content"]
        if not isinstance(content, dict):
            raise ValueError("instruction content must be an object")
        return content
    if "text" in spec:
        return {
            "text": spec["text"],
            **({"media_type": spec["media_type"]} if "media_type" in spec else {}),
        }
    if "content_ref" in spec:
        return {
            "artifact_ref": spec["content_ref"],
            "content_digest": spec.get("content_digest"),
            "media_type": spec.get("media_type", "text/markdown"),
            "size_bytes": spec.get("size_bytes", 0),
        }
    raise ValueError("an instruction file needs text, content or content_ref")


def fork_body(args: argparse.Namespace, body: dict[str, Any] | None) -> dict[str, Any]:
    """`run fork`: the request file (if any) plus the FT-F4 flags; flags fill what it omits."""

    fork = dict(body or {})
    fork.setdefault("schema_version", "mc.runtime_fork.v1")
    fork.setdefault("request_id", args.fork_request_id or str(uuid.uuid4()))
    if args.from_snapshot:
        fork["snapshot_id"] = args.from_snapshot
    if args.instruction_file:
        fork["instruction"] = instruction_content(args.instruction_file)
    if args.sponsorship_ref:
        fork["sponsorship_ref"] = args.sponsorship_ref
    if args.approval_refs:
        fork["approval_refs"] = list(args.approval_refs)
    if args.reason:
        fork["reason"] = args.reason
    fork.setdefault("reason", "forked by missionctl")
    if "sponsorship_ref" not in fork:
        raise ValueError("run fork needs --sponsorship-ref (or a request file naming it)")
    return fork


def queue_command_body(client: MissionClient, args: argparse.Namespace) -> dict[str, Any]:
    """Bind the queued command to the Run's current version and Generation (read first)."""

    kind, payload, reason, request_id = mailbox_payload(args.file, args)
    inspection = client.inspection(args.run_id)
    if inspection.status_code >= 300:
        raise ValueError(f"run inspection failed with HTTP {inspection.status_code}")
    current = _json(inspection)
    return {
        "schema_version": "mc.command.v1",
        "request_id": request_id,
        "expected_version": current["version"],
        "expected_generation": current["execution_generation"],
        "target": {"kind": "run", "id": args.run_id},
        "kind": kind,
        "payload": payload,
        "reason": reason,
    }


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


WAIT_POLL_SECONDS = 0.5


def inspection_wait_state(result: object) -> tuple[object, ...]:
    """What `run inspect --wait` watches: lifecycle, phase and the terminal outcome."""

    if not isinstance(result, dict):
        return (None, None, None)
    return (result.get("lifecycle"), result.get("phase"), result.get("execution_outcome"))


def exit_status(status: int) -> int:
    if 200 <= status < 300:
        return 0
    if status in {401, 403}:
        return 3
    if status == 409:
        return 4
    if status in {400, 404, 413, 422}:
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
    for action in ("snapshot", "reconcile", "start"):
        operation = run_commands.add_parser(action, parents=[common])
        operation.add_argument("run_id")
        operation.add_argument("--request-file", required=True)
    # FT-F4: fork from a Snapshot (default: the latest safe one) with a queued instruction.
    fork = run_commands.add_parser("fork", parents=[common])
    fork.add_argument("run_id")
    fork.add_argument("--request-file")
    fork.add_argument("--from-snapshot", dest="from_snapshot")
    fork.add_argument("--instruction-file", dest="instruction_file")
    fork.add_argument("--sponsorship-ref", dest="sponsorship_ref")
    fork.add_argument("--approval-ref", dest="approval_refs", action="append")
    fork.add_argument("--reason")
    fork.add_argument("--request-id", dest="fork_request_id")
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
    # FT-F1: queue an instruction (or, with --add-context, context) for the next boundary.
    queue = commands.add_parser("queue", parents=[common])
    queue.add_argument("run_id")
    queue.add_argument("--file", required=True)
    queue.add_argument("--add-context", action="store_true")
    queue.add_argument("--boundary", choices=("next_turn", "next_iteration"), default="next_turn")
    queue.add_argument("--node-key")
    queue.add_argument(
        "--expand", choices=("inline", "reference", "materialize", "auto"), default="auto"
    )
    queue.add_argument("--reason", default="queued by missionctl")
    queue.add_argument("--request-id")
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
        if deadline_seconds is not None and (
            args.group != "run" or args.action not in {"inspect", "transcript", "frames"}
        ):
            raise ValueError(
                "--wait is supported on run inspect, transcript and frames; "
                "command admission is not completion"
            )
        body = strict_object(args.request_file) if getattr(args, "request_file", None) else None
        if args.group == "run" and args.action == "fork":
            body = fork_body(args, body)
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
        if args.group == "subscribe":
            args.action = args.action or "create"
        if args.group == "subscribe" and args.action == "create":
            body = subscription_body(args)
        with httpx.Client(
            base_url=base_url,
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
            follow_redirects=False,
        ) as transport:
            client = MissionClient(transport, application)
            if args.group == "command" and args.action == "queue":
                body = queue_command_body(client, args)
            if args.group == "run" and args.action == "transcript":
                return run_transcript(client, args, deadline_seconds)
            if args.group == "run" and args.action == "frames":
                return run_frames_tail(client, args, deadline_seconds)
            deadline = time.monotonic() + (deadline_seconds or 0)
            # FT-F6: `run inspect --wait` returns early on a lifecycle, phase or terminal
            # change from the first observation (polled well under a second apart).
            initial: tuple[object, ...] | None = None
            while True:
                if deadline_seconds is not None:
                    transport.timeout = httpx.Timeout(
                        max(0.001, min(30, deadline - time.monotonic()))
                    )
                if args.group == "events":
                    return watch_events(transport, client, args)
                if args.group == "catalog":
                    response = catalog_response(client, args, body)
                elif args.group == "lane":
                    response = client.lanes(getattr(args, "lane_profile", None))
                elif args.group == "subscribe":
                    response = client.subscriptions(
                        args.action, body, getattr(args, "subscription_id", None)
                    )
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
                observed = inspection_wait_state(result)
                if initial is None:
                    initial = observed
                elif observed != initial:
                    print(json.dumps(result, allow_nan=False))
                    return 0
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    print(json.dumps({"error": "wait_timeout", "inspection": result}))
                    return 6
                time.sleep(min(WAIT_POLL_SECONDS, remaining))
    except (ValueError, OSError) as exc:
        print(json.dumps({"error": str(exc)}), file=sys.stderr)
        return 2
    except httpx.HTTPError:
        # Do not echo request URLs/headers or accidentally disclose credentials.
        print(json.dumps({"error": "service_unavailable"}), file=sys.stderr)
        return 5
