from __future__ import annotations

import argparse
import base64
import hashlib
import json
import re
from pathlib import PurePosixPath
from typing import Any

PROMPT_INJECTION = re.compile(
    r"(?:ignore|disregard|override)\s+(?:all\s+)?(?:previous|prior|system)"
    r"|(?:grant|expand|escalate)\s+(?:tool|network|secret|write|execution|authority)"
    r"|(?:bypass|disable)\s+(?:policy|guardrail|approval|sandbox)"
    r"|(?:run|execute|install)\s+(?:this|the following)\s+(?:command|script|package)",
    re.IGNORECASE,
)
UNSAFE_SCRIPT = re.compile(
    r"\b(?:child_process|subprocess|os\.system|eval\s*\(|exec\s*\(|"
    r"invoke-webrequest|start-process|curl\s+https?://|wget\s+https?://|"
    r"npm\s+install|npx\s+|pip\s+install|powershell(?:\.exe)?\s+-)"
    r"|(?:rm|del)\s+(?:-rf\s+)?(?:/|~|%userprofile%)",
    re.IGNORECASE,
)
SCRIPT_SUFFIXES = {
    ".bat",
    ".cmd",
    ".js",
    ".mjs",
    ".ps1",
    ".py",
    ".sh",
}
BINARY_SUFFIXES = {".dll", ".exe", ".msi", ".node", ".so"}


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    return parser.parse_args()


def _digest(value: bytes) -> str:
    return f"sha256:{hashlib.sha256(value).hexdigest()}"


def _finding(
    code: str,
    severity: str,
    summary: str,
    *,
    evidence: bytes | None = None,
) -> dict[str, str]:
    finding = {
        "code": code,
        "severity": severity,
        "summary": summary,
    }
    if evidence is not None:
        finding["evidence_digest"] = _digest(evidence)
    return finding


def _scan(payload: dict[str, Any]) -> dict[str, Any]:
    metadata = payload.get("metadata")
    files = payload.get("files")
    if not isinstance(metadata, dict) or not isinstance(files, list):
        raise ValueError("static inspection input is malformed")
    findings: list[dict[str, str]] = [
        _finding(
            "STATIC_METADATA_INSPECTED",
            "info",
            "Sanitized candidate metadata was inspected as untrusted data.",
        )
    ]
    metadata_bytes = json.dumps(
        metadata,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    if PROMPT_INJECTION.search(metadata_bytes.decode(errors="replace")):
        findings.append(
            _finding(
                "PROMPT_INJECTION_METADATA",
                "error",
                "Candidate metadata contains instructions that attempt to change authority.",
                evidence=metadata_bytes,
            )
        )

    files_inspected = 0
    downloaded_bytes = 0
    largest_file_bytes = 0
    manifest_valid = False
    for item in files:
        if not isinstance(item, dict):
            raise ValueError("static inspection file entry is malformed")
        path_value = item.get("path")
        content_value = item.get("content_base64")
        if not isinstance(path_value, str) or not isinstance(content_value, str):
            raise ValueError("static inspection file entry is malformed")
        path = PurePosixPath(path_value)
        if path.is_absolute() or ".." in path.parts:
            findings.append(
                _finding(
                    "UNSAFE_BUNDLE_PATH",
                    "error",
                    "Candidate bundle contains an unsafe file path.",
                )
            )
            continue
        content = base64.b64decode(content_value, validate=True)
        if path.name.casefold() in {
            "manifest.json",
            "mcp.json",
            "package.json",
            "server.json",
            "skill.md",
        }:
            manifest_valid = True
        files_inspected += 1
        downloaded_bytes += len(content)
        largest_file_bytes = max(largest_file_bytes, len(content))
        suffix = path.suffix.casefold()
        if suffix in BINARY_SUFFIXES or b"\x00" in content[:8_192]:
            findings.append(
                _finding(
                    "BINARY_BUNDLE_CONTENT",
                    "error",
                    "Candidate bundle contains binary or native executable content.",
                    evidence=content,
                )
            )
            continue
        if suffix in SCRIPT_SUFFIXES:
            decoded = content.decode(errors="replace")
            if UNSAFE_SCRIPT.search(decoded):
                findings.append(
                    _finding(
                        "UNSAFE_BUNDLED_SCRIPT",
                        "error",
                        "Candidate bundle contains a script with unsafe execution indicators.",
                        evidence=content,
                    )
                )
            else:
                findings.append(
                    _finding(
                        "EXECUTABLE_SCRIPT_REQUIRES_REVIEW",
                        "warning",
                        "Candidate bundle contains an executable script requiring human review.",
                        evidence=content,
                    )
                )

    if not files:
        findings.append(
            _finding(
                "METADATA_ONLY_INSPECTION",
                "warning",
                "No immutable candidate bundle was available; inspection is metadata-only.",
            )
        )
    if not manifest_valid:
        findings.append(
            _finding(
                "MANIFEST_UNAVAILABLE",
                "warning",
                "No recognized immutable candidate manifest was available for validation.",
            )
        )
    return {
        "manifest_valid": manifest_valid,
        "findings": findings,
        "downloaded_bytes": downloaded_bytes,
        "files_inspected": files_inspected,
        "largest_file_bytes": largest_file_bytes,
        "network_requests": 0,
        "network_hosts_contacted": [],
        "tools_list_probe_used": False,
    }


def main() -> None:
    args = _arguments()
    with open(args.input, encoding="utf-8") as stream:
        payload = json.load(stream)
    print(json.dumps(_scan(payload), sort_keys=True, separators=(",", ":")))


if __name__ == "__main__":
    main()
