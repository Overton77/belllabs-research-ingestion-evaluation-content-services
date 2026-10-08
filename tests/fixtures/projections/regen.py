"""Regenerate Host Projection goldens: ``uv run python -m tests.fixtures.projections.regen``.

Review the diff before committing; the goldens are the contract each lane provider reads.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

from tests.fixtures.projections.rows import INSTRUCTION, PACKET_INDEX, fixture_rows

from mission_control.application.agentic_components.projections import render_host_files
from mission_control.domain.agentic_components.projection import HostProjection
from mission_control.domain.capabilities.host_support import LaneProfile

ROOT = Path(__file__).resolve().parent
REPORT = "_projection.json"


def project(profile: LaneProfile) -> HostProjection:
    return render_host_files(fixture_rows(), profile, INSTRUCTION, PACKET_INDEX)


def summary(projection: HostProjection) -> bytes:
    document = {
        "files": {item.path: oct(item.mode) for item in projection.files},
        "report": projection.report.model_dump(mode="json"),
        "send_options": projection.send_options,
        "in_process": (
            None if projection.in_process is None else projection.in_process.model_dump(mode="json")
        ),
    }
    return (json.dumps(document, indent=2, sort_keys=True) + "\n").encode("utf-8")


def main() -> None:
    for profile in LaneProfile:
        target = ROOT / profile.value
        if target.exists():
            shutil.rmtree(target)
        projection = project(profile)
        for item in projection.files:
            path = target / item.path
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(item.content)
        (target / REPORT).write_bytes(summary(projection))
        print(f"{profile.value}: {len(projection.files)} files")


if __name__ == "__main__":
    main()
