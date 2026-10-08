"""Mission Control Kernel Hook for Cursor: ``.mission/hooks/kernel.py`` (SPEC-07 section 5.5).

The `cursor_local` lane writes this file verbatim into each leased workspace. Cursor's
``.cursor/hooks.json`` runs ``python .mission/hooks/kernel.py <mc_event> <kernel_hook_id>``
first for every event (fail-closed on permission events). The script forwards the native
stdin payload to the worker's loopback callback with the task token read from the lease
(``.mission/bin/.token``, mode 0600) and answers in Cursor's native output shape. A callback
that cannot be reached denies: the permission hooks are fail-closed, so the action is blocked.

Standard library only: the workspace's Python runs it without Mission Control installed. It
never prints the token and never forwards environment variables.
"""

from __future__ import annotations

import json
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

PERMISSION_EVENTS = {"before_tool", "before_shell", "before_mcp", "subagent_start", "before_prompt"}
TIMEOUT_SECONDS = 25
MAX_PAYLOAD_BYTES = 1_000_000


def native_output(event: str, result: dict[str, Any]) -> tuple[str, int]:
    """A merged ``mc.hook_result.v1`` in Cursor's stdout shape and exit code.

    Headless Cursor does not enforce ``ask``, so ``defer`` is a deny; exit 2 blocks the action.
    """

    decision = result.get("decision", "allow")
    refused = decision in {"deny", "defer"}
    out: dict[str, Any] = {}
    if event == "before_prompt":
        out["continue"] = not refused
    elif event in PERMISSION_EVENTS:
        out["permission"] = "deny" if refused else "allow"
    if refused and result.get("reason"):
        out["agent_message"] = result["reason"]
    if result.get("message"):
        out["user_message"] = result["message"]
    if result.get("updated_input") is not None:
        out["updated_input"] = result["updated_input"]
    if result.get("additional_context"):
        out["additional_context"] = result["additional_context"]
    return json.dumps(out), 2 if refused and event in PERMISSION_EVENTS else 0


def _call(context: dict[str, Any], root: Path, body: bytes) -> dict[str, Any]:
    callback = context["callback"]
    token = (root.parent.parent / callback["token_path"]).read_text(encoding="utf-8").strip()
    request = urllib.request.Request(
        callback["url"],
        data=body,
        method="POST",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
    )
    # Loopback only: never route the callback through a proxy from the environment.
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open(request, timeout=TIMEOUT_SECONDS) as response:
        loaded = json.loads(response.read().decode("utf-8"))
    return loaded if isinstance(loaded, dict) else {"decision": "deny", "reason": "bad response"}


def main(argv: list[str]) -> int:
    if len(argv) != 3:
        sys.stderr.write("usage: kernel.py <mc_event> <kernel_hook_id>\n")
        return 2
    _, event, hook_id = argv
    root = Path(__file__).resolve().parent
    try:
        context = json.loads((root / "context.json").read_text(encoding="utf-8"))
        raw = sys.stdin.read(MAX_PAYLOAD_BYTES)
        payload = json.loads(raw) if raw.strip() else {}
        if not isinstance(payload, dict):
            payload = {"value": payload}
        call = {
            "schema_version": "mc.kernel_hook_call.v1",
            "kernel_hook_id": hook_id,
            "event": event,
            "scope": context["scope"],
            "harness_execution_id": context["harness_execution_id"],
            "generation": context["generation"],
            "payload": payload,
        }
        result = _call(context, root, json.dumps(call).encode("utf-8"))
    except (OSError, ValueError, KeyError, urllib.error.URLError) as error:
        result = {
            "decision": "deny",
            "reason": f"kernel hook callback failed: {type(error).__name__}",
        }
    text, code = native_output(event, result)
    sys.stdout.write(text + "\n")
    return code


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
