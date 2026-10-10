#!/usr/bin/env python3
"""Lane qualification (FT-G6, MP-07, MP-08):
`make lane-qualify PROFILE=cursor_local|cursor_cloud|claude_agent_sdk|codex [LIVE=1]`.

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

`PROFILE=codex` (MP-08) runs the codex offline suites (`tests/unit/codex`, describe honesty)
and, with `--live`, `tests/integration/codex/test_codex_live_qualification.py`, which needs a
Linux/WSL worker with `codex-cli 0.162.0`, `MC_CODEX_AUTH_ROUTE` (`api_key` with
`OPENAI_API_KEY`, or `owner_cli_login` with `MC_CODEX_OWNER_HOME`), the owner's auth profile
document (`MISSION_CONTROL_AUTH_PROFILES_PATH`, `MC_CODEX_AUTH_PROFILE`) and a finite
`MC_PAID_BUDGET_USD`; it writes `docs/qualification/lanes/codex-<date>.md`.

`PROFILE=claude_agent_sdk` (MP-07) runs `tests/unit/claude` and the describe-honesty and replay
suites offline, then `tests/integration/claude/test_claude_live_qualification.py`, which needs a
finite `MC_PAID_BUDGET_USD`, an explicit `MC_CLAUDE_AUTH_ROUTE` (`api_key` with
`ANTHROPIC_API_KEY`, or `owner_cli_login` with `MC_CLAUDE_OWNER_ATTESTATION_REF`), the owner's
`MISSION_CONTROL_AUTH_PROFILES_PATH` and `MC_CLAUDE_AUTH_PROFILE`, and a Linux/WSL 2/macOS host;
it writes `docs/qualification/lanes/claude_agent_sdk-<date>.md`.

Credentials are read from the environment only and never printed.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PROFILES = ("cursor_local", "cursor_cloud", "claude_agent_sdk", "codex")
OFFLINE = (
    "tests/unit/harness/test_lane_qualification_fixtures.py",
    "tests/unit/harness/test_describe_honesty.py",
    "tests/integration/temporal/test_lane_replay_histories.py",
)
LIVE_TEST = "tests/integration/cursor/test_lane_qualification_live.py"
OFFLINE_BY_PROFILE = {
    "codex": ("tests/unit/codex", "tests/unit/harness/test_describe_honesty.py"),
    "claude_agent_sdk": (
        "tests/unit/claude",
        "tests/unit/harness/test_describe_honesty.py",
        "tests/integration/temporal/test_lane_replay_histories.py",
    ),
}
LIVE_TEST_BY_PROFILE = {
    "codex": "tests/integration/codex/test_codex_live_qualification.py",
    "claude_agent_sdk": "tests/integration/claude/test_claude_live_qualification.py",
}
LIVE_FLAG_BY_PROFILE = {
    "codex": "MC_LIVE_CODEX_QUALIFICATION",
    "claude_agent_sdk": "MC_LIVE_CLAUDE_QUALIFICATION",
}
# Never handed to the offline suite (names only; values are never printed).
CREDENTIAL_ENV = (
    "CURSOR_API_KEY",
    "OPENAI_API_KEY",
    "CODEX_API_KEY",
    "ANTHROPIC_API_KEY",
    "ANTHROPIC_AUTH_TOKEN",
    "CLAUDE_CODE_OAUTH_TOKEN",
)


def _pytest(selection: list[str], env: dict[str, str]) -> int:
    command = [sys.executable, "-m", "pytest", "-q", *selection]
    return subprocess.run(command, cwd=ROOT, env=env, check=False).returncode


def _budget() -> float:
    try:
        value = float(os.environ.get("MC_PAID_BUDGET_USD", "") or "0")
    except ValueError:
        return 0.0
    return value if 0 < value < float("inf") else 0.0


def _claude_preconditions() -> list[str]:
    missing = []
    if _budget() <= 0:
        missing.append("MC_PAID_BUDGET_USD (a finite, owner-approved amount)")
    route = os.environ.get("MC_CLAUDE_AUTH_ROUTE", "")
    if route not in {"api_key", "owner_cli_login"}:
        missing.append("MC_CLAUDE_AUTH_ROUTE (api_key or owner_cli_login; no default)")
    if route == "api_key" and not os.environ.get("ANTHROPIC_API_KEY"):
        missing.append("ANTHROPIC_API_KEY (api_key route)")
    if route == "owner_cli_login" and not os.environ.get("MC_CLAUDE_OWNER_ATTESTATION_REF"):
        missing.append("MC_CLAUDE_OWNER_ATTESTATION_REF (owner_cli_login is policy_restricted)")
    for name in ("MISSION_CONTROL_AUTH_PROFILES_PATH", "MC_CLAUDE_AUTH_PROFILE"):
        if not os.environ.get(name):
            missing.append(name)
    if sys.platform == "win32":
        missing.append("a Linux/WSL 2/macOS worker host (LANE_UNSUPPORTED_OS)")
    return missing


def _live_preconditions(profile: str) -> list[str]:
    if profile == "claude_agent_sdk":
        return _claude_preconditions()
    missing = []
    if profile == "codex":
        if _budget() <= 0:
            missing.append("MC_PAID_BUDGET_USD (a finite, owner-approved amount)")
        route = os.environ.get("MC_CODEX_AUTH_ROUTE", "")
        if route not in {"api_key", "owner_cli_login"}:
            missing.append("MC_CODEX_AUTH_ROUTE (api_key or owner_cli_login; no default)")
        if route == "api_key" and not os.environ.get("OPENAI_API_KEY"):
            missing.append("OPENAI_API_KEY (api_key route)")
        if route == "owner_cli_login" and not os.environ.get("MC_CODEX_OWNER_HOME"):
            missing.append("MC_CODEX_OWNER_HOME (the owner's CODEX_HOME)")
        for name in ("MISSION_CONTROL_AUTH_PROFILES_PATH", "MC_CODEX_AUTH_PROFILE"):
            if not os.environ.get(name):
                missing.append(name)
        if sys.platform == "win32":
            missing.append("a Linux/WSL worker (the lane spawns `codex app-server`)")
        return missing
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
    offline = {key: value for key, value in env.items() if key not in CREDENTIAL_ENV}
    offline.pop("MC_LIVE_CURSOR_QUALIFICATION", None)
    offline.pop("MC_LIVE_CODEX_QUALIFICATION", None)
    offline.pop("MC_LIVE_CLAUDE_QUALIFICATION", None)
    print(f"[lane-qualify] offline evidence for {args.profile} (no network, no credentials)")
    code = _pytest(list(OFFLINE_BY_PROFILE.get(args.profile, OFFLINE)), offline)
    if code != 0:
        print("[lane-qualify] the offline suite failed; no live drill runs")
        return code
    if not args.live:
        print("[lane-qualify] offline evidence passed; the profile stays unqualified")
        print("[lane-qualify] the live drill: LIVE=1 with the profile's credentials and budget")
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
    flag = LIVE_FLAG_BY_PROFILE.get(args.profile, "MC_LIVE_CURSOR_QUALIFICATION")
    live = {**env, flag: "1", "MC_LANE_QUALIFY_PROFILE": args.profile}
    return _pytest([LIVE_TEST_BY_PROFILE.get(args.profile, LIVE_TEST), "-rs"], live)


if __name__ == "__main__":
    raise SystemExit(main())
