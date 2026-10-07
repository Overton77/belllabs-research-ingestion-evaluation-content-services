"""Per-target qualification evidence (plan section 12 file names where applicable).

``before`` writes identity.json, release.json, plan.json, schema-before.json and
protected-before.json. ``after`` verifies, then writes identity.json, release.json,
schema-after.json, protected-after.json, migration-receipts.json, roles.json,
protected-diff.json (when a before snapshot exists) and decision.json. All files are
redacted reports: no DSNs, credentials or row contents.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .canonical import read_json_object, write_json
from .deployment import inspect_target, plan_release, verify_release
from .errors import ContractError
from .integrity import Release
from .snapshot import compare, snapshot_target
from .target import public_identity


async def qualify(
    target: dict[str, str],
    release: Release,
    *,
    phase: str,
    out_dir: Path,
    snapshot_from: dict[str, str] | None,
    allowed: dict[str, Any] | None,
    chunk_size: int,
    reader_version: str,
    writer_version: str,
) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    versions = {"reader_version": reader_version, "writer_version": writer_version}
    inspection = await inspect_target(target)
    state = inspection["state"]
    write_json(
        out_dir / "identity.json",
        {
            "target": public_identity(target),
            "observed_installation": state["identity"],
            "identity_matches_target": inspection["identity_matches_target"],
            "server_version_num": state["server_version_num"],
            "note": "Route/project identity evidence is the referenced identity_evidence; "
            "an inserted installation row alone does not prove which project was reached.",
        },
    )
    write_json(out_dir / "release.json", {**release.summary(), "manifest": release.manifest})
    snapshot = await snapshot_target(snapshot_from or target, chunk_size=chunk_size)
    written = ["identity.json", "release.json"]
    holds: list[str] = []
    if phase == "before":
        plan = await plan_release(target, release, **versions)
        write_json(out_dir / "plan.json", plan)
        write_json(out_dir / "schema-before.json", state["fingerprint"] or {"present": False})
        write_json(out_dir / "protected-before.json", snapshot)
        written += ["plan.json", "schema-before.json", "protected-before.json"]
        holds += plan["plan"]["holds"]
        decision: dict[str, Any] = {"phase": "before", "plan_digest": plan["plan_digest"]}
    else:
        try:
            verified: dict[str, Any] = await verify_release(target, release, **versions)
        except ContractError as exc:
            verified = {"status": "blocked", "error": str(exc)}
            holds.append(str(exc))
        write_json(out_dir / "schema-after.json", state["fingerprint"] or {"present": False})
        write_json(out_dir / "protected-after.json", snapshot)
        write_json(
            out_dir / "migration-receipts.json",
            {
                "receipts": state["receipts"],
                "attestations": state["attestations"],
                "verification": verified,
            },
        )
        write_json(
            out_dir / "roles.json",
            {
                "mission_control_roles": state["mission_control_roles"],
                "declared_runtime_roles": release.manifest["runtime_roles"],
            },
        )
        written += [
            "schema-after.json",
            "protected-after.json",
            "migration-receipts.json",
            "roles.json",
        ]
        decision = {"phase": "after", "verification": verified.get("status")}
        before_path = out_dir / "protected-before.json"
        if before_path.exists():
            diff = compare(read_json_object(before_path), snapshot, allowed)
            write_json(out_dir / "protected-diff.json", diff)
            written.append("protected-diff.json")
            decision["protected_diff"] = diff["status"]
            if diff["status"] == "blocked":
                holds.append("Protected objects changed outside the allowed differences")
        else:
            holds.append("No protected-before.json in the evidence directory")
    decision.update({"status": "pass" if not holds else "hold", "holds": holds})
    write_json(out_dir / "decision.json", decision)
    written.append("decision.json")
    return {
        "status": decision["status"],
        "phase": phase,
        "holds": holds,
        "out_dir": str(out_dir),
        "files": sorted(written),
        "mutations": [],
    }
