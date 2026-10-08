#!/usr/bin/env python3
"""Push the local ticket drafts back to their Linear issues (description sync).

For every ``issues/<ID>-*.md`` that carries a ``Linear: OVE-NNN`` line, rewrite the
Linear issue description as the local body plus the Provenance section, and optionally
post one comment on every epic. Idempotent. Reads LINEAR_API_KEY from .env; never prints it.

Usage:
  python docs/specs/fast-track-2026-10/sync_issues.py --dry-run
  python docs/specs/fast-track-2026-10/sync_issues.py [--only A6,G7] [--epic-comment FILE]
"""

from __future__ import annotations

import argparse
import re
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from publish_issues import (
    EPICS,
    PROJECT_NAME,
    commit_sha,
    gql,
    parse_draft,
    provenance,
    read_key,
)

HERE = Path(__file__).resolve().parent
ISSUES = HERE / "issues"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--only", default="")
    ap.add_argument("--epic-comment", default="")
    args = ap.parse_args()
    only = {s.strip() for s in args.only.split(",") if s.strip()}
    drafts = [parse_draft(p) for p in sorted(ISSUES.glob("*.md"))]
    drafts = [d for d in drafts if d["linear"] and (not only or d["id"] in only)]
    print(f"{len(drafts)} drafts to sync")
    if args.dry_run:
        for d in drafts:
            print(f"  {d['linear']} <- FT-{d['id']}")
        return 0
    key = read_key()
    sha = commit_sha()
    ids = gql(
        key,
        """query($p:String!){ issues(filter:{project:{name:{eq:$p}}, title:{startsWith:"[FT-"}}, first:200){ nodes{ id identifier title } } }""",
        {"p": PROJECT_NAME},
    )["issues"]["nodes"]
    by_identifier = {i["identifier"]: i for i in ids}
    for d in drafts:
        node = by_identifier.get(d["linear"])
        if not node:
            print(f"WARN {d['linear']} not found")
            continue
        body = re.sub(
            r"^Linear: OVE-\d+\n\n", "", d["body"], count=1, flags=re.MULTILINE
        ) + provenance(d, sha)
        gql(
            key,
            """mutation($id:String!,$i:IssueUpdateInput!){ issueUpdate(id:$id,input:$i){ success } }""",
            {"id": node["id"], "i": {"description": body}},
        )
        print(f"synced {d['linear']} (FT-{d['id']})")
        time.sleep(0.25)
    if args.epic_comment:
        text = Path(args.epic_comment).read_text(encoding="utf-8")
        for letter in EPICS:
            pref = f"[FT-EPIC-{letter}]"
            node = next((i for i in ids if i["title"].startswith(pref)), None)
            if not node:
                continue
            gql(
                key,
                """mutation($i:CommentCreateInput!){ commentCreate(input:$i){ success } }""",
                {"i": {"issueId": node["id"], "body": text}},
            )
            print(f"commented on {node['identifier']}")
            time.sleep(0.25)
    return 0


if __name__ == "__main__":
    sys.exit(main())
