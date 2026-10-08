#!/usr/bin/env python3
"""Cursor lane qualification (FT-G6): `make lane-qualify PROFILE=cursor_local [LIVE=1]`.

Always runs the offline evidence first, with no network and no credentials:

- the recorded-fixture suite (`tests/unit/harness/test_lane_qualification_fixtures.py`);
- the describe-honesty suite (`tests/unit/harness/test_describe_honesty.py`);
- the lane Temporal histories replayed with `Replayer`
  (`tests/integration/temporal/test_lane_replay_histories.py`).

With `--live` (`LIVE=1`) it then runs the paid drill for the profile
(`tests/integration/cursor/test_lane_qualification_live.py`), which needs
`CURSOR_API_KEY`, a finite `MC_PAID_BUDGET_USD` and, for `cursor_cloud`, a throwaway
repository in `MC_CURSOR_CLOUD_REPO`; the repeated cancel-latency and busy drills also need
`MC_LANE_DRILL_APPROVAL_URL` (the owner's Linear approval comment). The drill writes scrubbed
recordings under `tests/integration/cursor/recordings/` and the qualification record
`docs/qualification/lanes/<profile>-<date>.md`. Nothing here flips `qualified`: that is a
reviewed release citing the record.

Credentials are read from the environment only and never printed.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PROFILES = ("cursor_local", "cursor_cloud")
OFFLINE = (
    "tests/unit/harness/test_lane_qualification_fixtures.py",
    "tests/unit/harness/test_describe_honesty.py",
    "tests/integration/temporal/test_lane_replay_histories.py",
)
LIVE_TEST = "tests/integration/cursor/test_lane_qualification_live.py"


def _pytest(selection: list[str], env: dict[str, str]) -> int:
    command = [sys.executable, "-m", "pytest", "-q", *selection]
    return subprocess.run(command, cwd=ROOT, env=env, check=False).returncode


def _budget() -> float:
    try:
        value = float(os.environ.get("MC_PAID_BUDGET_USD", "") or "0")
    except ValueError:
        return 0.0
    return value if 0 < value < float("inf") else 0.0


def _live_preconditions(profile: str) -> list[str]:
    missing = []
    if not os.environ.get("CURSOR_API_KEY"):
        missing.append("CURSOR_API_KEY")
    if _budget() <= 0:
        missing.append("MC_PAID_BUDGET_USD (a finite, owner-approved amount)")
    if profile == "cursor_cloud" and not os.environ.get("MC_CURSOR_CLOUD_REPO"):
        missing.append("MC_CURSOR_CLOUD_REPO (a throwaway repository)")
    return missing


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--profile", choices=PROFILES, required=True)
    parser.add_argument("--live", action="store_true", help="run the paid drill after the suite")
    args = parser.parse_args(argv)

    env = dict(os.environ)
    offline = {key: value for key, value in env.items() if key != "CURSOR_API_KEY"}
    offline.pop("MC_LIVE_CURSOR_QUALIFICATION", None)
    print(f"[lane-qualify] offline evidence for {args.profile} (no network, no credentials)")
    code = _pytest(list(OFFLINE), offline)
    if code != 0:
        print("[lane-qualify] the offline suite failed; no live drill runs")
        return code
    if not args.live:
        print("[lane-qualify] offline evidence passed; the profile stays unqualified")
        print("[lane-qualify] the live drill: LIVE=1 with CURSOR_API_KEY and MC_PAID_BUDGET_USD")
        return 0
    missing = _live_preconditions(args.profile)
    if missing:
        print("[lane-qualify] live drill refused; missing: " + ", ".join(missing))
        return 2
    approval = env.get("MC_LANE_DRILL_APPROVAL_URL", "")
    print(
        f"[lane-qualify] live drill for {args.profile}, budget {_budget()} USD, "
        f"repeated drills {'approved' if approval else 'not approved (single runs only)'}"
    )
    live = {
        **env,
        "MC_LIVE_CURSOR_QUALIFICATION": "1",
        "MC_LANE_QUALIFY_PROFILE": args.profile,
    }
    return _pytest([LIVE_TEST, "-rs"], live)


if __name__ == "__main__":
    raise SystemExit(main())
