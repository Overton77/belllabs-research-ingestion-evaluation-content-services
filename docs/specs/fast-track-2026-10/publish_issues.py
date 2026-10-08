#!/usr/bin/env python3
"""Promote the fast-track ticket drafts under issues/ to Linear and link blockers.

Reads every ``issues/<ID>-*.md`` (ID like ``A1`` .. ``I4``), creates one Linear issue
per ticket in team OVE / project "Mission Control" with labels ``Feature`` and
``ready-for-agent``, one parent issue per epic, native ``blocks`` relations from the
``**Blocked by:**`` line, a ``## Provenance`` section, and writes ``Linear: OVE-NNN``
back into each draft. Idempotent: a draft that already carries a ``Linear:`` line is
skipped for creation (relations are still ensured).

Usage:
  python docs/specs/fast-track-2026-10/publish_issues.py --dry-run
  python docs/specs/fast-track-2026-10/publish_issues.py

Reads LINEAR_API_KEY from mission-control/.env; never prints it.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[2]
ISSUES = HERE / "issues"
API = "https://api.linear.app/graphql"
TEAM_KEY = "OVE"
PROJECT_NAME = "Mission Control"
LABELS = ("Feature", "ready-for-agent")

EPICS = {
    "A": ("Capabilities", "SPEC-01-capabilities-catalog.md"),
    "B": ("Context", "SPEC-02-context-packet.md"),
    "C": ("Mission state", "SPEC-03-mission-state-and-transcript.md"),
    "D": ("Chains", "SPEC-04-mission-chains.md"),
    "E": ("Manifest", "SPEC-05-mission-manifest.md"),
    "F": ("Control", "SPEC-06-interventions-inspection-subscriptions.md"),
    "G": ("Lanes", "SPEC-07-harness-and-cursor-lane.md"),
    "H": ("Skills", "SPEC-08-agent-skills.md"),
    "I": ("Missions", "00-ARCHITECTURE.md"),
}


def read_key() -> str:
    for line in (REPO / ".env").read_text(encoding="utf-8").splitlines():
        if line.startswith("LINEAR_API_KEY="):
            return line.split("=", 1)[1].strip().strip('"').strip("'")
    raise SystemExit("LINEAR_API_KEY not found in .env")


def gql(key: str, query: str, variables: dict | None = None) -> dict:
    body = json.dumps({"query": query, "variables": variables or {}}).encode()
    req = urllib.request.Request(
        API, data=body, headers={"Content-Type": "application/json", "Authorization": key}
    )
    for attempt in range(5):
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                data = json.load(resp)
            if "errors" in data:
                raise RuntimeError(json.dumps(data["errors"])[:500])
            return data["data"]
        except Exception as exc:
            if attempt == 4:
                raise
            time.sleep(2 * (attempt + 1))
            last = exc
    raise RuntimeError(str(last))


def commit_sha() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"], cwd=REPO, text=True
        ).strip()
    except Exception:
        return "worktree"


def parse_draft(path: Path) -> dict:
    text = path.read_text(encoding="utf-8")
    m = re.search(r"^# \[FT-([A-I]\d)\] (.+)$", text, re.MULTILINE)
    if not m:
        raise SystemExit(f"{path}: missing '# [FT-ID] Title' heading")
    tid, title = m.group(1), m.group(2).strip()
    blocked = re.search(r"^\*\*Blocked by:\*\*\s*(.+)$", text, re.MULTILINE)
    blockers: list[str] = []
    if blocked and "None" not in blocked.group(1):
        blockers = re.findall(r"FT-([A-I]\d)", blocked.group(1))
    linear = re.search(r"^Linear:\s*(OVE-\d+)", text, re.MULTILINE)
    return {
        "id": tid,
        "title": title,
        "body": text,
        "blockers": blockers,
        "linear": linear.group(1) if linear else None,
        "path": path,
    }


def provenance(draft: dict, sha: str) -> str:
    epic, spec = EPICS[draft["id"][0]]
    rel = draft["path"].relative_to(REPO).as_posix()
    return (
        "\n\n## Provenance\n\n"
        "- Conversation: Claude Code fast-track planning session 2026-10-07 (owner interview; recommendations accepted)\n"
        f"- Documents: {rel} @ {sha}; docs/specs/fast-track-2026-10/{spec} @ {sha}; "
        f"docs/specs/fast-track-2026-10/00-ARCHITECTURE.md @ {sha}; docs/specs/fast-track-2026-10/TEAM-WORKSPACE.md @ {sha}\n"
        "- Decisions: ADR-0023 to ADR-0034 (and ADR-0018, ADR-0019, ADR-0022 as accepted)\n"
        f"- Terms: see GLOSSARY.md; epic {epic}\n"
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    drafts = sorted(
        (parse_draft(p) for p in ISSUES.glob("*.md")), key=lambda d: (d["id"][0], int(d["id"][1:]))
    )
    if not drafts:
        raise SystemExit("no drafts found")
    ids = {d["id"] for d in drafts}
    for d in drafts:
        for b in d["blockers"]:
            if b not in ids:
                raise SystemExit(f"{d['id']} blocked by unknown {b}")
    print(f"{len(drafts)} drafts parsed; epics: {sorted({d['id'][0] for d in drafts})}")
    if args.dry_run:
        for d in drafts:
            print(f"  FT-{d['id']}: {d['title']}  <- {d['blockers'] or '-'}  {d['linear'] or ''}")
        return 0

    key = read_key()
    sha = commit_sha()
    team = gql(
        key,
        """query($k:String!){ teams(filter:{key:{eq:$k}}){ nodes{ id projects{ nodes{ id name } } labels{ nodes{ id name } } } } }""",
        {"k": TEAM_KEY},
    )["teams"]["nodes"][0]
    team_id = team["id"]
    project_id = next(p["id"] for p in team["projects"]["nodes"] if p["name"] == PROJECT_NAME)
    label_ids = [label["id"] for label in team["labels"]["nodes"] if label["name"] in LABELS]
    if len(label_ids) != len(LABELS):
        raise SystemExit("labels Feature/ready-for-agent not found")

    # Existing FT issues (idempotency by title prefix)
    existing = gql(
        key,
        """query($p:String!){ issues(filter:{project:{name:{eq:$p}}, title:{startsWith:"[FT-"}}, first:200){ nodes{ id identifier title parent{ id } } } }""",
        {"p": PROJECT_NAME},
    )["issues"]["nodes"]
    by_prefix: dict[str, dict] = {}
    for iss in existing:
        m = re.match(r"\[FT-([A-I]\d|EPIC-[A-I])\]", iss["title"])
        if m:
            by_prefix[m.group(1)] = iss

    # Epic parents
    epic_ids: dict[str, str] = {}
    for letter, (name, spec) in EPICS.items():
        if not any(d["id"][0] == letter for d in drafts):
            continue
        pref = f"EPIC-{letter}"
        if pref in by_prefix:
            epic_ids[letter] = by_prefix[pref]["id"]
            continue
        desc = (
            f"Fast-track epic **{name}**. Specification: `docs/specs/fast-track-2026-10/{spec}`. "
            f"Architecture: `docs/specs/fast-track-2026-10/00-ARCHITECTURE.md` section 9. "
            f"Team workspace: `docs/specs/fast-track-2026-10/TEAM-WORKSPACE.md`.\n\n"
            "## Provenance\n\n- Conversation: Claude Code fast-track planning session 2026-10-07\n"
            f"- Documents: docs/specs/fast-track-2026-10/{spec} @ {sha}\n- Decisions: ADR-0023 to ADR-0034\n- Terms: GLOSSARY.md\n"
        )
        res = gql(
            key,
            """mutation($i:IssueCreateInput!){ issueCreate(input:$i){ success issue{ id identifier } } }""",
            {
                "i": {
                    "teamId": team_id,
                    "projectId": project_id,
                    "title": f"[FT-{pref}] {name}",
                    "description": desc,
                    "labelIds": label_ids[:1],
                }
            },
        )["issueCreate"]["issue"]
        epic_ids[letter] = res["id"]
        print(f"epic {letter}: {res['identifier']}")

    # Tickets
    issue_ids: dict[str, str] = {}
    identifiers: dict[str, str] = {}
    for d in drafts:
        if d["id"] in by_prefix:
            issue_ids[d["id"]] = by_prefix[d["id"]]["id"]
            identifiers[d["id"]] = by_prefix[d["id"]]["identifier"]
            continue
        body = d["body"] + provenance(d, sha)
        res = gql(
            key,
            """mutation($i:IssueCreateInput!){ issueCreate(input:$i){ success issue{ id identifier } } }""",
            {
                "i": {
                    "teamId": team_id,
                    "projectId": project_id,
                    "parentId": epic_ids[d["id"][0]],
                    "title": f"[FT-{d['id']}] {d['title']}",
                    "description": body,
                    "labelIds": label_ids,
                }
            },
        )["issueCreate"]["issue"]
        issue_ids[d["id"]] = res["id"]
        identifiers[d["id"]] = res["identifier"]
        print(f"FT-{d['id']}: {res['identifier']}")
        time.sleep(0.3)

    # Blocking relations
    rel_count = 0
    for d in drafts:
        for b in d["blockers"]:
            try:
                gql(
                    key,
                    """mutation($i:IssueRelationCreateInput!){ issueRelationCreate(input:$i){ success } }""",
                    {
                        "i": {
                            "issueId": issue_ids[b],
                            "relatedIssueId": issue_ids[d["id"]],
                            "type": "blocks",
                        }
                    },
                )
                rel_count += 1
            except RuntimeError as exc:
                if "already" in str(exc).lower() or "duplicate" in str(exc).lower():
                    continue
                raise
            time.sleep(0.2)
    print(f"{rel_count} blocking relations created")

    # Write back Linear identifiers
    for d in drafts:
        if d["linear"]:
            continue
        text = d["path"].read_text(encoding="utf-8")
        text = text.replace("\n\n**Epic:**", f"\n\nLinear: {identifiers[d['id']]}\n\n**Epic:**", 1)
        d["path"].write_text(text, encoding="utf-8", newline="\n")
    print("drafts updated with Linear identifiers")
    return 0


if __name__ == "__main__":
    sys.exit(main())
