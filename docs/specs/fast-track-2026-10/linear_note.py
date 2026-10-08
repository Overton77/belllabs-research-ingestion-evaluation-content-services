#!/usr/bin/env python3
"""Post a claim or handoff comment on a fast-track ticket and optionally move its state.

Usage:
  python docs/specs/fast-track-2026-10/linear_note.py OVE-22 --state "In Progress" --file claim.md
  python docs/specs/fast-track-2026-10/linear_note.py OVE-22 --state "In Review" --file handoff.md
  python docs/specs/fast-track-2026-10/linear_note.py OVE-22 --show

Reads LINEAR_API_KEY from the primary checkout's .env (worktrees carry no .env), or from
MC_ENV_FILE when set; never prints it.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import publish_issues as pub  # noqa: E402


def _env_file() -> Path:
    override = os.environ.get("MC_ENV_FILE")
    if override:
        return Path(override)
    repo = HERE.parents[2]
    for candidate in (repo / ".env", repo.parent / "mission-control" / ".env"):
        if candidate.is_file():
            return candidate
    raise SystemExit("no .env found; set MC_ENV_FILE")


def _key() -> str:
    for line in _env_file().read_text(encoding="utf-8").splitlines():
        if line.startswith("LINEAR_API_KEY="):
            return line.split("=", 1)[1].strip().strip('"').strip("'")
    raise SystemExit("LINEAR_API_KEY not found")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("identifier")
    ap.add_argument("--state", default="")
    ap.add_argument("--file", default="")
    ap.add_argument("--show", action="store_true")
    args = ap.parse_args()
    key = _key()
    issue = pub.gql(
        key,
        """query($id:String!){ issue(id:$id){ id identifier title state{ name }
             team{ states{ nodes{ id name } } }
             comments(first:50){ nodes{ body createdAt } } } }""",
        {"id": args.identifier},
    )["issue"]
    if args.show:
        print(f"{issue['identifier']} {issue['title']} [{issue['state']['name']}]")
        for c in issue["comments"]["nodes"]:
            print(f"--- {c['createdAt']}\n{c['body']}\n")
        return 0
    if args.file:
        body = Path(args.file).read_text(encoding="utf-8")
        pub.gql(
            key,
            """mutation($i:CommentCreateInput!){ commentCreate(input:$i){ success } }""",
            {"i": {"issueId": issue["id"], "body": body}},
        )
        print(f"commented on {issue['identifier']}")
    if args.state:
        states = {s["name"].lower(): s["id"] for s in issue["team"]["states"]["nodes"]}
        state_id = states.get(args.state.lower())
        if not state_id:
            raise SystemExit(f"unknown state {args.state!r}; have {sorted(states)}")
        pub.gql(
            key,
            """mutation($id:String!,$i:IssueUpdateInput!){ issueUpdate(id:$id,input:$i){ success } }""",
            {"id": issue["id"], "i": {"stateId": state_id}},
        )
        print(f"{issue['identifier']} -> {args.state}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
