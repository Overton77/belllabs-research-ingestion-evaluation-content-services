"""Mission Control policy template hook (catalog row ``hook.mc-policy-template``).

Reads ``mc.hook_input.v1`` on stdin and writes ``mc.hook_result.v1`` on stdout. It denies
``rm -rf /`` (any flag order), force pushes, and file writes outside the declared write roots.
Write roots come from ``details.declared_paths`` in the hook input, else ``policy.json`` next
to this script (``{"write_roots": [...]}``), else the workspace root. Standard library only,
so it runs on every lane without Mission Control installed. Unknown input fails closed.
"""

from __future__ import annotations

import json
import re
import shlex
import sys
from pathlib import Path, PurePosixPath, PureWindowsPath

WRITE_TOOLS = {"write_file", "edit_file", "Write", "Edit", "MultiEdit", "apply_patch"}
PATH_KEYS = ("file_path", "path", "target_file", "filename")
SHELL_TOOLS = {"shell", "execute", "bash", "Bash", "run_terminal_cmd"}
_FORCE_PUSH = re.compile(
    r"(?:^|[\s;&|])git\s+(?:-\S+(?:\s+[^-\s;&|]\S*)?\s+)*push\b(?P<rest>[^;&|]*)"
)


def _result(decision: str, reason: str | None = None) -> dict[str, object]:
    result: dict[str, object] = {"schema_version": "mc.hook_result.v1", "decision": decision}
    if reason:
        result["reason"] = reason
    return result


def _command(tool: dict[str, object]) -> str | None:
    payload = tool.get("input")
    if not isinstance(payload, dict):
        return None
    for key in ("command", "cmd", "script"):
        value = payload.get(key)
        if isinstance(value, str):
            return value
        if isinstance(value, list) and all(isinstance(item, str) for item in value):
            return " ".join(value)
    return None


def _destroys_root(command: str) -> bool:
    for segment in re.split(r"[;&|]+", command):
        try:
            words = shlex.split(segment, posix=True)
        except ValueError:
            words = segment.split()
        while words and words[0] in {"sudo", "doas", "command", "builtin", "exec"}:
            words = words[1:]
        if not words or PurePosixPath(words[0]).name != "rm":
            continue
        flags = "".join(word.lstrip("-") for word in words[1:] if word.startswith("-"))
        targets = [word for word in words[1:] if not word.startswith("-")]
        recursive = "r" in flags.lower() or "recursive" in flags
        forced = "f" in flags or "force" in flags
        if recursive and forced and any(_is_root(target) for target in targets):
            return True
        if "no-preserve-root" in flags:
            return True
    return False


def _is_root(target: str) -> bool:
    cleaned = target.strip("'\"")
    return cleaned in {"/", "/*", "~", "~/", "$HOME", "${HOME}"} or bool(
        re.fullmatch(r"[A-Za-z]:[\\/]?\*?", cleaned)
    )


def _force_push(command: str) -> bool:
    for match in _FORCE_PUSH.finditer(command):
        rest = match.group("rest").split()
        if any(
            word in {"--force", "-f", "--force-with-lease", "--force-if-includes"}
            or word.startswith("--force-with-lease=")
            or (word.startswith("-") and not word.startswith("--") and "f" in word[1:])
            or (word.startswith("+") and len(word) > 1)
            for word in rest
        ):
            return True
    return False


def _write_roots(hook_input: dict[str, object], workspace: Path) -> list[Path]:
    details = hook_input.get("details")
    declared = details.get("declared_paths") if isinstance(details, dict) else None
    if not declared:
        config = Path(__file__).with_name("policy.json")
        if config.is_file():
            declared = json.loads(config.read_text(encoding="utf-8")).get("write_roots")
    roots = declared if isinstance(declared, list) and declared else ["."]
    return [_resolve(str(root), workspace) for root in roots]


def _resolve(raw: str, workspace: Path) -> Path:
    windows = PureWindowsPath(raw)
    candidate = Path(raw)
    if not (candidate.is_absolute() or windows.is_absolute()):
        candidate = workspace / raw
    return Path(_normalize(str(candidate)))


def _normalize(raw: str) -> str:
    parts: list[str] = []
    text = raw.replace("\\", "/")
    anchor = ""
    match = re.match(r"^([A-Za-z]:)?/", text)
    if match:
        anchor = match.group(0)
        text = text[len(anchor) :]
    for part in text.split("/"):
        if part in {"", "."}:
            continue
        if part == "..":
            if parts:
                parts.pop()
            continue
        parts.append(part)
    return anchor + "/".join(parts)


def _outside(path: Path, roots: list[Path]) -> bool:
    target = _normalize(str(path)).casefold()
    for root in roots:
        base = _normalize(str(root)).casefold().rstrip("/")
        if target == base or target.startswith(base + "/"):
            return False
    return True


def evaluate(hook_input: dict[str, object]) -> dict[str, object]:
    if hook_input.get("schema_version") != "mc.hook_input.v1":
        return _result("deny", "policy template received an unknown hook input")
    tool = hook_input.get("tool")
    if not isinstance(tool, dict):
        return _result("allow")
    name = str(tool.get("name", ""))
    command = _command(tool)
    if command is not None:
        if _destroys_root(command):
            return _result("deny", "policy: recursive forced delete of a root path is forbidden")
        if _force_push(command):
            return _result("deny", "policy: force push is forbidden")
    if name in WRITE_TOOLS or name not in SHELL_TOOLS:
        payload = tool.get("input")
        workspace = Path(str(hook_input.get("workspace_root") or "."))
        if isinstance(payload, dict):
            for key in PATH_KEYS:
                value = payload.get(key)
                if isinstance(value, str) and name in WRITE_TOOLS:
                    if _outside(_resolve(value, workspace), _write_roots(hook_input, workspace)):
                        return _result("deny", f"policy: write outside declared paths: {value}")
    return _result("allow")


def main() -> int:
    try:
        hook_input = json.loads(sys.stdin.read() or "{}")
        if not isinstance(hook_input, dict):
            raise ValueError("hook input is not an object")
        result = evaluate(hook_input)
    except Exception as error:
        result = _result("deny", f"policy template could not evaluate the call: {error}")
    sys.stdout.write(json.dumps(result) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
