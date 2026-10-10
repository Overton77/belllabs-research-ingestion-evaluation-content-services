"""Read-only validation of the configured Mission Control installation.

``python -m mission_control.bootstrap.preflight`` composes the configured API once (FT-G7).
Given a local run profile (``--profile`` or ``MISSION_CONTROL_LOCAL_RUN_PROFILE``) it first runs
the MP-22 readiness gate and stops before any composition, worker start or paid provider call
when an input is missing or drifted.

``... preflight readiness --profile <file>`` prints only the readiness report
(``mc.local_readiness.v1``): every unresolved pointer of a real local run. It checks:

* the ``mc.local_run_profile.v1`` file itself (lanes, auth profile ids, bindings file, auth
  profiles file, Temporal cluster bindings and the cluster new runs target);
* that every selected lane can run on this host's operating system;
* the ``mc.manifest_launch_bindings.v1`` file: it validates, carries no ``OWNER-SELECT:``
  placeholder, and binds only components the capability pins and settings serve;
* the auth route of every lane: the profile exists, matches the lane, the route is documented,
  and a ``env:`` credential reference names a variable that is present (never its value);
* every capability pin against the bytes under an explicit workspace root, naming the pin,
  locator, expected and computed digests;
* the deployment's release lock and, given a DSN environment variable name, the installed
  ``release_attestation`` fingerprint against the pinned ``mission-db`` release.

``... preflight guard-launch`` is the cross-cluster launch guard of a Temporal outage drill:
it binds a run to one cluster in a durable ledger before any start and refuses a launch in
any other cluster while the run is bound elsewhere, has already left ``pending``, or its
workflow exists in another cluster. There is no automatic cross-cluster replay.
"""

from __future__ import annotations

import argparse
import asyncio
import copy
import json
import os
import platform
import re
import sys
from collections.abc import Awaitable, Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any, Final, Literal

import asyncpg
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator
from temporalio.client import Client
from temporalio.service import RPCError, RPCStatusCode

from mission_control.adapters.capabilities.capability_bundles import bytes_digest
from mission_control.adapters.capabilities.capability_pins import (
    WORKSPACE_SCHEME,
    CapabilityPinError,
    CapabilityPins,
    read_skill_bundle,
    workspace_path,
    workspace_root,
)
from mission_control.adapters.temporal.client import (
    TemporalConnectionError,
    resolve_temporal_connection,
)
from mission_control.application.authoring.manifest_launch_inputs import (
    ManifestLaunchBindings,
    ServedComponents,
)
from mission_control.application.execution.auth_admission import (
    LOGIN_ROUTES,
    AuthProfile,
    route_spec,
)
from mission_control.bootstrap.api import MissionDeployment, create_app, load_deployment
from mission_control.bootstrap.manifests import served_components
from mission_control.bootstrap.settings import PROJECT_ROOT, Settings, get_settings
from mission_control.domain.authoring.canonical import sha256_digest
from mission_control.domain.execution.contracts import (
    DeepAgentExecutionPlacementProfile,
    DeepAgentProfile,
)
from mission_control.domain.execution.lanes import HOSTED_PROFILES, LaneProfileName

LOCAL_RUN_PROFILE_SCHEMA: Final = "mc.local_run_profile.v1"
READINESS_SCHEMA: Final = "mc.local_readiness.v1"
RUN_CLUSTER_SCHEMA: Final = "mc.run_cluster_binding.v1"
PROFILE_ENV: Final = "MISSION_CONTROL_LOCAL_RUN_PROFILE"
WORKSPACE_ROOT_ENV: Final = "MISSION_CONTROL_PREFLIGHT_WORKSPACE_ROOT"
PLACEHOLDER_PREFIX: Final = "OWNER-SELECT:"
UNSET_DIGEST: Final = "sha256:" + "0" * 64
EXIT_BLOCKED: Final = 2

Severity = Literal["blocking", "advisory"]
CheckState = Literal["passed", "failed", "skipped"]


def run_workflow_id(run_id: str) -> str:
    """The root workflow id `RunLaunchService` derives from a run (`belllabs-run/{run_id}`)."""

    return f"belllabs-run/{run_id}"


# --- Report --------------------------------------------------------------------------------


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class PreflightIssue(_Strict):
    """One unresolved input; `pointer` names the file and JSON pointer (or host fact)."""

    code: str
    pointer: str
    message: str
    severity: Severity = "blocking"
    expected: str | None = None
    observed: str | None = None


class ReadinessReport(_Strict):
    schema_version: Literal["mc.local_readiness.v1"] = READINESS_SCHEMA
    host: dict[str, str | bool]
    checks: dict[str, CheckState]
    issues: tuple[PreflightIssue, ...]

    @property
    def ready(self) -> bool:
        return not any(issue.severity == "blocking" for issue in self.issues)

    def as_json(self) -> dict[str, object]:
        return {
            **self.model_dump(mode="json"),
            "ready": self.ready,
            "unresolved_pointers": sorted(
                {issue.pointer for issue in self.issues if issue.severity == "blocking"}
            ),
        }


# --- The local run profile (`mc.local_run_profile.v1`) --------------------------------------


class LaneSelection(_Strict):
    lane_profile: LaneProfileName
    auth_profile_id: str | None = Field(default=None, min_length=1)


class ClusterBinding(_Strict):
    """One Temporal cluster a run can be pinned to (address, namespace and task queue)."""

    cluster_id: str = Field(pattern=r"^[a-z][a-z0-9_-]*$")
    target: Literal["local", "cloud"]
    address: str = Field(min_length=1)
    namespace: str = Field(min_length=1)
    task_queue: str = Field(min_length=1)


class LocalRunProfile(_Strict):
    """What a real local run of one deployment app needs; paths are repository-relative."""

    schema_version: Literal["mc.local_run_profile.v1"] = LOCAL_RUN_PROFILE_SCHEMA
    app: str = Field(pattern=r"^[a-z][a-z0-9_-]*$")
    lanes: tuple[LaneSelection, ...] = Field(min_length=1)
    manifest_launch_bindings: str = Field(min_length=1)
    auth_profiles: str = Field(min_length=1)
    temporal_clusters: tuple[ClusterBinding, ...] = Field(min_length=1)
    new_run_cluster: str

    @model_validator(mode="after")
    def clusters_are_declared_once(self) -> LocalRunProfile:
        ids = [item.cluster_id for item in self.temporal_clusters]
        if len(ids) != len(set(ids)):
            raise ValueError("a Temporal cluster is declared twice")
        if self.new_run_cluster not in ids:
            raise ValueError(f"new_run_cluster {self.new_run_cluster} is not declared")
        return self

    def cluster(self, cluster_id: str) -> ClusterBinding:
        for item in self.temporal_clusters:
            if item.cluster_id == cluster_id:
                return item
        raise KeyError(cluster_id)


def _display(path: Path, root: Path) -> str:
    """A repository-relative POSIX label when `path` lies under `root`."""

    resolved = (root / path).resolve() if not path.is_absolute() else path.resolve()
    try:
        return resolved.relative_to(root.resolve()).as_posix()
    except ValueError:
        return str(resolved)


def _resolve(root: Path, value: str) -> Path:
    path = Path(value)
    return path if path.is_absolute() else root / path


def _escape(key: object) -> str:
    return str(key).replace("~", "~0").replace("/", "~1")


def _placeholders(document: object, prefix: str = "") -> Iterator[tuple[str, str]]:
    if isinstance(document, Mapping):
        for key, value in document.items():
            yield from _placeholders(value, f"{prefix}/{_escape(key)}")
    elif isinstance(document, list | tuple):
        for index, value in enumerate(document):
            yield from _placeholders(value, f"{prefix}/{index}")
    elif isinstance(document, str) and document.startswith(PLACEHOLDER_PREFIX):
        yield prefix, document.removeprefix(PLACEHOLDER_PREFIX)
    elif document == UNSET_DIGEST:
        yield prefix, "an exact reviewed digest"


def _set_pointer(document: dict[str, Any], pointer: str, value: object) -> None:
    if not pointer.startswith("/"):
        raise ValueError(f"selection {pointer} is not a JSON pointer")
    parts = [part.replace("~1", "/").replace("~0", "~") for part in pointer[1:].split("/")]
    node: Any = document
    for part in parts[:-1]:
        node = node[int(part)] if isinstance(node, list) else node.setdefault(part, {})
    last = parts[-1]
    if isinstance(node, list):
        node[int(last)] = value
    else:
        node[last] = value


def compose_launch_bindings(
    base: Mapping[str, Any], selections: Mapping[str, object]
) -> ManifestLaunchBindings:
    """Apply owner selections (JSON pointer -> value) to a bindings document and re-seal it.

    The scaffold profile and placement carry content digests, so a hand edit inside them
    invalidates the file; this recomputes both digests after the selections are applied.
    """

    document: dict[str, Any] = copy.deepcopy(dict(base))
    for pointer, value in selections.items():
        _set_pointer(document, pointer, value)
    scaffold = document["deep_agents"]
    placement = {k: v for k, v in scaffold["placement"].items() if k != "placement_digest"}
    scaffold["placement"] = DeepAgentExecutionPlacementProfile.create(**placement).model_dump(
        mode="json"
    )
    profile = {k: v for k, v in scaffold["profile"].items() if k != "profile_digest"}
    scaffold["profile"] = DeepAgentProfile.create(**profile).model_dump(mode="json")
    return ManifestLaunchBindings.model_validate(document)


def _read_json(path: Path) -> object:
    return json.loads(path.read_text(encoding="utf-8"))


def _validation_issues(code: str, label: str, error: ValidationError) -> list[PreflightIssue]:
    return [
        PreflightIssue(
            code=code,
            pointer=f"{label}#/" + "/".join(str(part) for part in item["loc"]),
            message=item["msg"],
        )
        for item in error.errors()
    ]


def load_profile(path: Path | None) -> tuple[LocalRunProfile | None, list[PreflightIssue]]:
    if path is None:
        return None, [
            PreflightIssue(
                code="PROFILE_MISSING",
                pointer=f"env:{PROFILE_ENV}",
                message="no mc.local_run_profile.v1 file is selected (--profile)",
            )
        ]
    label = path.as_posix()
    try:
        document = _read_json(path)
    except (OSError, json.JSONDecodeError) as error:
        return None, [
            PreflightIssue(
                code="PROFILE_MISSING",
                pointer=label,
                message=f"the local run profile is unavailable: {type(error).__name__}",
            )
        ]
    issues = [
        PreflightIssue(
            code="PROFILE_UNRESOLVED",
            pointer=f"{label}#{pointer}",
            message=f"owner must choose {what}",
        )
        for pointer, what in _placeholders(document)
    ]
    try:
        return LocalRunProfile.model_validate(document), issues
    except ValidationError as error:
        return None, [*issues, *_validation_issues("PROFILE_INVALID", label, error)]


# --- Lane host support ---------------------------------------------------------------------

_PROACTOR = "https://docs.python.org/3/library/asyncio-platforms.html#windows"
_RUNBOOK_B6 = "docs/specs/fast-track-2026-10/OWNER-FIXTURE-RUNBOOK.md (B6)"


@dataclass(frozen=True)
class LaneHost:
    systems: frozenset[str]
    reason: str
    sources: tuple[str, ...] = ()


_POSIX = frozenset({"Linux", "Darwin"})
_ANY = frozenset({"Linux", "Darwin", "Windows"})
_LOCAL_SUBPROCESS = (
    "the worker runs a SelectorEventLoop on Windows (psycopg, bootstrap/worker.run_cli) and "
    "asyncio on Windows spawns subprocesses only on the Proactor loop; run this lane's worker "
    "under WSL or Linux"
)
LANE_HOSTS: Final[Mapping[str, LaneHost]] = {
    "deep_agents": LaneHost(_ANY, "in-worker Deep Agents runtime"),
    "cursor_local": LaneHost(
        _POSIX, _LOCAL_SUBPROCESS, (_PROACTOR, _RUNBOOK_B6, "adapters/cursor/bridge.py")
    ),
    "claude_agent_sdk": LaneHost(_POSIX, _LOCAL_SUBPROCESS, (_PROACTOR,)),
    "codex": LaneHost(_POSIX, _LOCAL_SUBPROCESS, (_PROACTOR,)),
    "cursor_cloud": LaneHost(_ANY, "provider-hosted; the worker only calls the vendor API"),
}
_OUTCOME_3 = ("claude_cloud", "codex_cloud")


def host_facts(system: str | None = None) -> dict[str, str | bool]:
    name = system or platform.system()
    release = platform.release().lower()
    return {
        "system": name,
        "release": platform.release(),
        "python": platform.python_version(),
        "wsl": name == "Linux" and ("microsoft" in release or "wsl" in release),
    }


def check_lane_hosts(
    profile: LocalRunProfile, system: str, *, profile_label: str = "profile"
) -> list[PreflightIssue]:
    issues: list[PreflightIssue] = []
    for index, lane in enumerate(profile.lanes):
        pointer = f"{profile_label}#/lanes/{index}/lane_profile"
        name = str(lane.lane_profile)
        if name in _OUTCOME_3:
            issues.append(
                PreflightIssue(
                    code="LANE_UNQUALIFIED",
                    pointer=pointer,
                    message=(
                        f"{name} is a provider-hosted product that stays unqualified "
                        f"(docs/qualification/lanes/{name}/FEASIBILITY.md)"
                    ),
                )
            )
            continue
        host = LANE_HOSTS[name]
        if system not in host.systems:
            issues.append(
                PreflightIssue(
                    code="LANE_UNSUPPORTED_OS",
                    pointer=pointer,
                    message=f"{name} cannot run on a {system} worker: {host.reason}",
                    expected=" | ".join(sorted(host.systems)),
                    observed=system,
                )
            )
        if name in HOSTED_PROFILES:
            issues.append(
                PreflightIssue(
                    code="LANE_HOSTED",
                    pointer=pointer,
                    severity="advisory",
                    message=f"{name} runs in the provider's cloud, not on this host",
                )
            )
    return issues


def check_registered_lane_hosts(profiles: Sequence[str], system: str) -> list[PreflightIssue]:
    """Advisory host issues for the lanes a worker registers without a local run profile."""

    issues: list[PreflightIssue] = []
    for name in profiles:
        host = LANE_HOSTS.get(name)
        if host is None or system in host.systems:
            continue
        issues.append(
            PreflightIssue(
                code="LANE_UNSUPPORTED_OS",
                pointer=f"settings#/lanes/{name}",
                severity="advisory",
                message=f"{name} cannot run on a {system} worker: {host.reason}",
                expected=" | ".join(sorted(host.systems)),
                observed=system,
            )
        )
    return issues


# --- Launch bindings -----------------------------------------------------------------------


def check_bindings(
    path: Path,
    served: ServedComponents,
    pins: CapabilityPins,
    settings: Settings,
    *,
    label: str | None = None,
) -> tuple[ManifestLaunchBindings | None, list[PreflightIssue]]:
    label = label or str(path)
    try:
        document = _read_json(path)
    except (OSError, json.JSONDecodeError) as error:
        return None, [
            PreflightIssue(
                code="BINDINGS_MISSING",
                pointer=label,
                message=f"the mc.manifest_launch_bindings.v1 file is unavailable: "
                f"{type(error).__name__}",
            )
        ]
    issues = [
        PreflightIssue(
            code="BINDING_UNRESOLVED",
            pointer=f"{label}#{pointer}",
            message=f"owner must choose {what}",
        )
        for pointer, what in _placeholders(document)
    ]
    try:
        bindings = ManifestLaunchBindings.model_validate(document)
    except ValidationError as error:
        return None, [*issues, *_validation_issues("BINDINGS_INVALID", label, error)]

    def unserved(pointer: str, kind: str, digest: str, pool: frozenset[str]) -> None:
        if digest not in pool:
            issues.append(
                PreflightIssue(
                    code="COMPONENT_UNSERVED",
                    pointer=f"{label}#{pointer}",
                    message=f"the {kind} is neither pinned nor registered by this deployment",
                    observed=digest,
                )
            )

    profile = bindings.deep_agents.profile
    unserved(
        "/deep_agents/profile/checkpointer_ref",
        "checkpointer",
        profile.checkpointer_ref.digest,
        served.checkpointers,
    )
    unserved("/deep_agents/profile/store_ref", "store", profile.store_ref.digest, served.stores)
    for name, model in bindings.model_profiles.items():
        unserved(f"/model_profiles/{name}/ref", "model", model.ref.digest, served.models)
        if model.provider not in settings.manifest_provider_secret_env:
            issues.append(
                PreflightIssue(
                    code="AUTH_SECRET_ENV_UNDECLARED",
                    pointer=f"{label}#/model_profiles/{name}/provider",
                    message=f"no credential reference is declared for provider {model.provider} "
                    "(MANIFEST_PROVIDER_SECRET_ENV)",
                )
            )
    for name, sandbox in bindings.sandbox_profiles.items():
        unserved(f"/sandbox_profiles/{name}/ref", "sandbox", sandbox.ref.digest, served.sandboxes)
    pinned = {
        "mcp_server": {item.server_id for item in pins.mcp_servers},
        "skill": {item.skill_name for item in pins.skills},
        "tool": {item.tool_name for item in pins.tools},
    }
    for capability_id, binding in bindings.capabilities.items():
        pointer = f"/capabilities/{capability_id}"
        if binding.pinned is not None:
            if binding.pinned not in pinned[binding.kind]:
                issues.append(
                    PreflightIssue(
                        code="COMPONENT_UNPINNED",
                        pointer=f"{label}#{pointer}/pinned",
                        message=f"{binding.kind} {binding.pinned} is not in the capability pins",
                    )
                )
        elif binding.mcp_server is not None:
            unserved(pointer, "MCP server", binding.mcp_server.ref.digest, served.mcp_servers)
        elif binding.skill is not None:
            unserved(pointer, "Skill", binding.skill.bundle_digest, served.skills)
        elif binding.tool is not None:
            unserved(pointer, "tool", binding.tool.ref.digest, served.tools)
    return bindings, issues


# --- Auth routes ---------------------------------------------------------------------------


def check_auth(
    profile: LocalRunProfile,
    path: Path,
    environ: Mapping[str, str],
    *,
    label: str | None = None,
    profile_label: str = "profile",
) -> list[PreflightIssue]:
    label = label or str(path)
    try:
        document = _read_json(path)
        raw = document.get("profiles") if isinstance(document, dict) else None
        if not isinstance(raw, list):
            raise ValueError("no profiles list")
    except (OSError, ValueError) as error:
        return [
            PreflightIssue(
                code="AUTH_PROFILES_MISSING",
                pointer=label,
                message=f"the mc.auth_profile.v1 registry file is unavailable: "
                f"{type(error).__name__}",
            )
        ]
    issues: list[PreflightIssue] = []
    registry: dict[str, tuple[int, AuthProfile]] = {}
    for index, item in enumerate(raw):
        try:
            parsed = AuthProfile.model_validate(item)
        except ValidationError as error:
            issues.extend(
                _validation_issues("AUTH_PROFILE_INVALID", f"{label}#/profiles/{index}", error)
            )
            continue
        registry[parsed.profile_id] = (index, parsed)
    for index, lane in enumerate(profile.lanes):
        pointer = f"{profile_label}#/lanes/{index}/auth_profile_id"
        if lane.auth_profile_id is None or lane.auth_profile_id not in registry:
            issues.append(
                PreflightIssue(
                    code="AUTH_PROFILE_MISSING",
                    pointer=pointer,
                    message=f"lane {lane.lane_profile} selects no registered auth profile",
                    observed=lane.auth_profile_id,
                )
            )
            continue
        position, auth = registry[lane.auth_profile_id]
        where = f"{label}#/profiles/{position}"
        if auth.lane_profile != lane.lane_profile:
            issues.append(
                PreflightIssue(
                    code="AUTH_PROFILE_LANE_MISMATCH",
                    pointer=f"{where}/lane_profile",
                    message="the auth profile belongs to another lane",
                    expected=str(lane.lane_profile),
                    observed=str(auth.lane_profile),
                )
            )
            continue
        spec = route_spec(auth.lane_profile, auth.route)
        if spec.support != "documented":
            issues.append(
                PreflightIssue(
                    code=f"AUTH_ROUTE_{spec.support.upper()}",
                    pointer=f"{where}/route",
                    message=f"{auth.route} on {auth.lane_profile}: {spec.note}",
                )
            )
        if auth.route in LOGIN_ROUTES:
            issues.append(
                PreflightIssue(
                    code="AUTH_STATUS_UNPROBED",
                    pointer=f"{where}/route",
                    severity="advisory",
                    message="a sign-in route is admitted only by the launch-time status probe",
                )
            )
        elif auth.credential_ref is not None and auth.credential_ref.startswith("env:"):
            name = auth.credential_ref.removeprefix("env:")
            if not environ.get(name):
                issues.append(
                    PreflightIssue(
                        code="AUTH_CREDENTIAL_ABSENT",
                        pointer=f"{where}/credential_ref",
                        message=f"the environment variable {name} is not set in this process",
                    )
                )
    return issues


# --- Capability pins against an explicit workspace root ------------------------------------


def _locate(root: Path, locator: str) -> Path:
    # The worker's own resource-root contract (relocated `.agents`/`.tools` links resolve
    # beneath their declared targets; every other escape is refused).
    return workspace_path(locator, root=root)


def _file_digest(path: Path) -> str:
    return "sha256:" + sha256(path.read_bytes()).hexdigest()


def check_pins(pins: CapabilityPins, root: Path, pins_label: str) -> list[PreflightIssue]:
    """Every pinned module, Skill bundle and tool entrypoint against the bytes under `root`.

    The worker's own resolution (`CapabilityPins`) uses the checkout's parent; this check takes
    the workspace root explicitly so an operator can assess another host layout.
    """

    issues: list[PreflightIssue] = []

    def drift(pointer: str, locator: str, what: str, expected: str, computed: str) -> None:
        issues.append(
            PreflightIssue(
                code="PIN_DRIFT",
                pointer=f"{pins_label}#{pointer}",
                message=f"{what} at {locator} differs from its reviewed pin",
                expected=expected,
                observed=computed,
            )
        )

    def unavailable(pointer: str, locator: str, error: Exception) -> None:
        issues.append(
            PreflightIssue(
                code="PIN_UNAVAILABLE",
                pointer=f"{pins_label}#{pointer}",
                message=f"{locator} is unavailable under {root}: {type(error).__name__}",
            )
        )

    for index, server in enumerate(pins.mcp_servers):
        pointer = f"/mcp_servers/{index}/module_digest"
        try:
            computed = _file_digest(_locate(root, server.module_locator))
        except (OSError, CapabilityPinError) as error:
            unavailable(pointer, server.module_locator, error)
            continue
        if computed != server.module_digest:
            drift(
                pointer,
                server.module_locator,
                f"MCP module {server.server_id}",
                server.module_digest,
                computed,
            )
    for index, tool in enumerate(pins.tools):
        pointer = f"/tools/{index}/entrypoint_digest"
        try:
            computed = _file_digest(_locate(root, tool.entrypoint_locator))
        except (OSError, CapabilityPinError) as error:
            unavailable(pointer, tool.entrypoint_locator, error)
            continue
        if computed != tool.entrypoint_digest:
            drift(
                pointer,
                tool.entrypoint_locator,
                f"tool {tool.tool_name}",
                tool.entrypoint_digest,
                computed,
            )
    for index, skill in enumerate(pins.skills):
        if not skill.source_locator.startswith(WORKSPACE_SCHEME):
            continue
        pointer = f"/skills/{index}"
        try:
            bundle = read_skill_bundle(
                _locate(root, skill.source_locator), digest_format=skill.digest_format
            )
        except (OSError, CapabilityPinError) as error:
            unavailable(pointer, skill.source_locator, error)
            continue
        if bundle.bundle_digest != skill.bundle_digest:
            drift(
                f"{pointer}/bundle_digest",
                skill.source_locator,
                f"Skill bundle {skill.skill_name}",
                skill.bundle_digest,
                bundle.bundle_digest,
            )
        skill_md = dict(bundle.files).get("SKILL.md")
        if skill_md is None:
            unavailable(f"{pointer}/skill_md_digest", skill.source_locator, FileNotFoundError())
            continue
        computed_md = (
            bytes_digest(skill_md)
            if skill.digest_format == "bytes_v1"
            else sha256_digest(skill_md.decode("utf-8"))
        )
        if computed_md != skill.skill_md_digest:
            drift(
                f"{pointer}/skill_md_digest",
                skill.source_locator + "/SKILL.md",
                f"SKILL.md of {skill.skill_name}",
                skill.skill_md_digest,
                computed_md,
            )
    return issues


# --- DB release ----------------------------------------------------------------------------


@dataclass(frozen=True)
class ExpectedRelease:
    component_version: str
    schema_fingerprint: str
    fingerprint_algorithm: str
    lock_path: Path


@dataclass(frozen=True)
class ObservedRelease:
    component_version: str
    schema_fingerprint: str
    fingerprint_algorithm: str


def expected_release(
    lock_path: Path, *, label: str | None = None
) -> tuple[ExpectedRelease | None, list[PreflightIssue]]:
    """The release `deployments/<app>/release.lock.json` pins (`mission-db lock` output)."""

    label = label or str(lock_path)
    try:
        lock = _read_json(lock_path)
        if not isinstance(lock, dict):
            raise ValueError("not an object")
        manifest_path = lock_path.parent / lock["release_root"] / lock["manifest_path"]
        content = manifest_path.read_bytes()
        manifest = json.loads(content)
    except (OSError, ValueError, KeyError) as error:
        return None, [
            PreflightIssue(
                code="RELEASE_LOCK_MISSING",
                pointer=label,
                message=f"the release lock or its manifest is unavailable: {type(error).__name__}",
            )
        ]
    issues: list[PreflightIssue] = []
    pinned = lock.get("files", {}).get(lock["manifest_path"])
    computed = sha256(content).hexdigest()
    if pinned != computed:
        issues.append(
            PreflightIssue(
                code="RELEASE_LOCK_DRIFT",
                pointer=f"{label}#/files/{lock['manifest_path']}",
                message="the component manifest differs from the release lock "
                "(rebuild and re-lock with `mission-db release-build` / `mission-db lock`)",
                expected=pinned,
                observed=computed,
            )
        )
    return (
        ExpectedRelease(
            component_version=manifest["component_version"],
            schema_fingerprint=manifest["schema_fingerprint"],
            fingerprint_algorithm=manifest["schema_fingerprint_algorithm"],
            lock_path=lock_path,
        ),
        issues,
    )


def compare_release(
    expected: ExpectedRelease, observed: ObservedRelease | None, *, pointer: str
) -> list[PreflightIssue]:
    if observed is None:
        return [
            PreflightIssue(
                code="DB_RELEASE_UNATTESTED",
                pointer=pointer,
                message=f"release {expected.component_version} is not attested in this database",
                expected=expected.schema_fingerprint,
            )
        ]
    if (
        observed.schema_fingerprint != expected.schema_fingerprint
        or observed.fingerprint_algorithm != expected.fingerprint_algorithm
    ):
        return [
            PreflightIssue(
                code="DB_RELEASE_MISMATCH",
                pointer=pointer,
                message=(
                    f"the installed release {observed.component_version} "
                    f"({observed.fingerprint_algorithm}) is not the pinned mission-db release"
                ),
                expected=f"{expected.fingerprint_algorithm}:{expected.schema_fingerprint}",
                observed=f"{observed.fingerprint_algorithm}:{observed.schema_fingerprint}",
            )
        ]
    return []


async def observe_release(dsn: str, component_version: str) -> ObservedRelease | None:
    """The installed attestation of `component_version` (read-only transaction)."""

    connection = await asyncpg.connect(dsn, timeout=10)
    try:
        async with connection.transaction(readonly=True):
            present = await connection.fetchval(
                "SELECT to_regclass('mission_control.release_attestation') IS NOT NULL"
            )
            if not present:
                return None
            row = await connection.fetchrow(
                """
                SELECT component_version, schema_fingerprint, fingerprint_algorithm
                FROM mission_control.release_attestation WHERE component_version = $1
                """,
                component_version,
            )
    finally:
        await connection.close()
    if row is None:
        return None
    return ObservedRelease(
        component_version=row["component_version"],
        schema_fingerprint=row["schema_fingerprint"],
        fingerprint_algorithm=row["fingerprint_algorithm"],
    )


# --- The readiness gate --------------------------------------------------------------------


@dataclass(frozen=True)
class ReadinessInputs:
    profile_path: Path | None
    root: Path = PROJECT_ROOT
    workspace_root: Path | None = None
    system: str | None = None
    db_dsn_env: str | None = None
    environ: Mapping[str, str] | None = None


async def readiness(inputs: ReadinessInputs, settings: Settings) -> ReadinessReport:
    """Every check that can refuse a real local run before any composition or paid call."""

    environ = inputs.environ if inputs.environ is not None else os.environ
    facts = host_facts(inputs.system)
    system = str(facts["system"])
    checks: dict[str, CheckState] = {}
    issues: list[PreflightIssue] = []

    def record(name: str, found: Sequence[PreflightIssue]) -> None:
        issues.extend(found)
        blocking = any(item.severity == "blocking" for item in found)
        checks[name] = "failed" if blocking else "passed"

    profile, found = load_profile(inputs.profile_path)
    record("profile", found)
    profile_label = _display(inputs.profile_path, inputs.root) if inputs.profile_path else "profile"
    pins_label = _display(settings.capability_pins_path, inputs.root)
    try:
        pins = CapabilityPins.load(settings.capability_pins_path)
    except (CapabilityPinError, ValidationError) as error:
        pins = None
        record(
            "capability_pins",
            [
                PreflightIssue(
                    code="PINS_MISSING",
                    pointer=pins_label,
                    message=f"capability pins are unavailable: {type(error).__name__}",
                )
            ],
        )
    if pins is not None:
        root = inputs.workspace_root or workspace_root()
        record("capability_pins", check_pins(pins, root, pins_label))
        facts["workspace_root"] = str(root)
    if profile is None:
        for name in ("lane_hosts", "launch_bindings", "auth_routes", "db_release", "temporal"):
            checks[name] = "skipped"
        return ReadinessReport(host=facts, checks=checks, issues=tuple(issues))

    record("lane_hosts", check_lane_hosts(profile, system, profile_label=profile_label))
    if pins is None:
        checks["launch_bindings"] = "skipped"
    else:
        bindings_path = _resolve(inputs.root, profile.manifest_launch_bindings)
        _bindings, found = check_bindings(
            bindings_path,
            served_components(pins, settings),
            pins,
            settings,
            label=_display(bindings_path, inputs.root),
        )
        record("launch_bindings", found)
    auth_path = _resolve(inputs.root, profile.auth_profiles)
    record(
        "auth_routes",
        check_auth(
            profile,
            auth_path,
            environ,
            label=_display(auth_path, inputs.root),
            profile_label=profile_label,
        ),
    )

    lock_path = inputs.root / "deployments" / profile.app / "release.lock.json"
    expected, found = expected_release(lock_path, label=_display(lock_path, inputs.root))
    found = list(found)
    if expected is not None and inputs.db_dsn_env is not None:
        dsn = environ.get(inputs.db_dsn_env)
        pointer = f"env:{inputs.db_dsn_env}#mission_control.release_attestation"
        if not dsn:
            found.append(
                PreflightIssue(
                    code="DB_DSN_ABSENT",
                    pointer=f"env:{inputs.db_dsn_env}",
                    message="the named DSN environment variable is not set",
                )
            )
        else:
            try:
                observed = await observe_release(dsn, expected.component_version)
            except (OSError, asyncpg.PostgresError, TimeoutError) as error:
                found.append(
                    PreflightIssue(
                        code="DB_UNREACHABLE",
                        pointer=pointer,
                        message=f"the release attestation is unreadable: {type(error).__name__}",
                    )
                )
            else:
                found.extend(compare_release(expected, observed, pointer=pointer))
    elif expected is not None:
        found.append(
            PreflightIssue(
                code="DB_RELEASE_UNCHECKED",
                pointer="--db-dsn-env",
                severity="advisory",
                message="no DSN environment variable named; the installed fingerprint is unread",
                expected=f"{expected.fingerprint_algorithm}:{expected.schema_fingerprint}",
            )
        )
    record("db_release", found)
    selected = profile.cluster(profile.new_run_cluster)
    record(
        "temporal",
        [
            PreflightIssue(
                code="NEW_RUNS_TARGET",
                pointer=f"{profile_label}#/new_run_cluster",
                severity="advisory",
                message=f"new runs target {selected.cluster_id} ({selected.target} "
                f"{selected.address}/{selected.namespace}, queue {selected.task_queue}); active "
                "runs stay on the cluster they are bound to",
            )
        ],
    )
    return ReadinessReport(host=facts, checks=checks, issues=tuple(issues))


# --- Cross-cluster launch guard ------------------------------------------------------------

Presence = Literal["present", "absent", "unknown"]
PresenceProbe = Callable[[ClusterBinding, str], Awaitable[Presence]]


class RunClusterRecord(_Strict):
    """The cluster a run was bound to at its first admitted launch (durable, write-once)."""

    schema_version: Literal["mc.run_cluster_binding.v1"] = RUN_CLUSTER_SCHEMA
    run_id: str = Field(min_length=1)
    workflow_id: str = Field(min_length=1)
    cluster: ClusterBinding
    recorded_at: datetime


class ClusterBindingLedger:
    """Write-once run-to-cluster records on local durable storage (one file per run).

    A record is created with an exclusive create, so two launchers racing for one run bind it
    once. Production persistence belongs beside ``mission_run`` (proposed 0032+ delta).
    """

    def __init__(self, directory: Path) -> None:
        self._directory = directory

    def _path(self, run_id: str) -> Path:
        return self._directory / (sha256(run_id.encode()).hexdigest() + ".json")

    def get(self, run_id: str) -> RunClusterRecord | None:
        try:
            content = self._path(run_id).read_text(encoding="utf-8")
        except FileNotFoundError:
            return None
        record = RunClusterRecord.model_validate_json(content)
        if record.run_id != run_id:
            raise ValueError(f"ledger record does not belong to run {run_id}")
        return record

    def bind(self, record: RunClusterRecord) -> RunClusterRecord:
        self._directory.mkdir(parents=True, exist_ok=True)
        try:
            with self._path(record.run_id).open("x", encoding="utf-8") as handle:
                handle.write(record.model_dump_json(indent=2))
                handle.flush()
                os.fsync(handle.fileno())
        except FileExistsError:
            existing = self.get(record.run_id)
            if existing is None:  # pragma: no cover - removed between create and read
                raise
            return existing
        return record


class ClusterLaunchDecision(_Strict):
    admitted: bool
    code: str
    message: str
    workflow_id: str
    cluster_id: str
    bound_cluster_id: str | None = None
    unverified_clusters: tuple[str, ...] = ()


async def guard_cluster_launch(
    run_id: str,
    target: ClusterBinding,
    *,
    clusters: Sequence[ClusterBinding],
    ledger: ClusterBindingLedger,
    run_phase: str,
    probe: PresenceProbe,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> ClusterLaunchDecision:
    """Admit a launch of `run_id` in `target` only if no copy can exist in another cluster.

    The run is identified across clusters by its derived workflow id; the ledger binding is
    written before any start, so a run without one was never started by a guarded launcher.
    A run bound to another cluster is never launched here: recover that cluster, or reconcile
    explicitly through a successor run with its own id. A workflow found in another cluster
    without a binding refuses the launch; a cluster that cannot be probed is reported in
    `unverified_clusters`, never treated as having been checked.
    """

    workflow_id = run_workflow_id(run_id)

    def decide(admitted: bool, code: str, message: str, bound: str | None) -> ClusterLaunchDecision:
        return ClusterLaunchDecision(
            admitted=admitted,
            code=code,
            message=message,
            workflow_id=workflow_id,
            cluster_id=target.cluster_id,
            bound_cluster_id=bound,
        )

    record = ledger.get(run_id)
    if record is not None:
        if record.cluster.cluster_id != target.cluster_id:
            return decide(
                False,
                "RUN_BOUND_TO_OTHER_CLUSTER",
                f"run {run_id} is bound to {record.cluster.cluster_id}; no automatic "
                "cross-cluster replay",
                record.cluster.cluster_id,
            )
        return decide(
            True,
            "BOUND_HERE",
            "the run is bound to this cluster; the workflow-id policy deduplicates the start",
            record.cluster.cluster_id,
        )
    if run_phase != "pending":
        return decide(
            False,
            "RUN_NOT_PENDING",
            f"run {run_id} is {run_phase}: it was launched before; reconcile explicitly",
            None,
        )
    unverified: list[str] = []
    for cluster in clusters:
        if cluster.cluster_id == target.cluster_id:
            continue
        presence = await probe(cluster, workflow_id)
        if presence == "present":
            return decide(
                False,
                "ACTIVE_IN_OTHER_CLUSTER",
                f"{workflow_id} exists in {cluster.cluster_id} without a ledger binding",
                cluster.cluster_id,
            )
        if presence == "unknown":
            unverified.append(cluster.cluster_id)
    bound = ledger.bind(
        RunClusterRecord(run_id=run_id, workflow_id=workflow_id, cluster=target, recorded_at=now())
    )
    if bound.cluster.cluster_id != target.cluster_id:
        return decide(
            False,
            "RUN_BOUND_TO_OTHER_CLUSTER",
            f"run {run_id} was bound to {bound.cluster.cluster_id} concurrently",
            bound.cluster.cluster_id,
        )
    decision = decide(
        True, "BOUND", f"run {run_id} is now bound to {target.cluster_id}", target.cluster_id
    )
    return decision.model_copy(update={"unverified_clusters": tuple(unverified)})


ClientFactory = Callable[[ClusterBinding], Awaitable[Client]]


def temporal_presence(connect: ClientFactory, *, timeout_s: float = 10) -> PresenceProbe:
    """Probe one cluster for a workflow id: any execution (open or closed) is `present`."""

    async def probe(cluster: ClusterBinding, workflow_id: str) -> Presence:
        try:
            async with asyncio.timeout(timeout_s):
                client = await connect(cluster)
                await client.get_workflow_handle(workflow_id).describe()
        except RPCError as error:
            return "absent" if error.status == RPCStatusCode.NOT_FOUND else "unknown"
        except (OSError, RuntimeError, TimeoutError):
            return "unknown"
        return "present"

    return probe


def settings_client_factory(settings: Settings) -> ClientFactory:
    async def connect(cluster: ClusterBinding) -> Client:
        if cluster.target == "local":
            return await Client.connect(cluster.address, namespace=cluster.namespace)
        connection = resolve_temporal_connection(
            settings.model_copy(update={"temporal_target": "cloud"}),
            address=cluster.address,
            namespace=cluster.namespace,
        )
        return await Client.connect(
            connection.address,
            namespace=connection.namespace,
            tls=connection.tls,
            api_key=connection.api_key,
        )

    return connect


# --- Configured-application preflight (FT-G7) ----------------------------------------------


def temporal_targets(
    deployment: MissionDeployment, settings: Settings
) -> list[dict[str, str | bool]]:
    """FT-G7: which Temporal target (local or cloud) each application connects to.

    Reports the target, address, namespace and whether TLS and an API key are used; never
    the key itself.
    """

    targets: list[dict[str, str | bool]] = []
    for item in deployment.applications:
        if item.temporal is None:
            continue
        try:
            summary = resolve_temporal_connection(
                settings, address=item.temporal.address, namespace=item.temporal.namespace
            ).describe()
        except TemporalConnectionError as error:
            summary = {"target": settings.temporal_target, "error": str(error)}
        targets.append({"application_id": item.authentication.binding.application_id, **summary})
    return targets


async def configured_preflight() -> dict[str, object]:
    try:
        async with asyncio.timeout(30):
            temporal = temporal_targets(load_deployment(), get_settings())
            application = create_app()
            async with application.router.lifespan_context(application):
                return {
                    "ready": bool(application.state.mission_control_ready),
                    "storage_mode": "production_common",
                    "production_ready": all(
                        item.readiness.production_ready
                        for item in application.state.mission_control_compositions.values()
                    ),
                    "application_tenants": len(application.state.mission_control_compositions),
                    "schema_changes": False,
                    "temporal": temporal,
                }
    except Exception as exc:
        # Connection errors and validation inputs can contain credential values.
        return {"ready": False, "error_type": type(exc).__name__}


# --- CLI -----------------------------------------------------------------------------------

_ENV_NAME = re.compile(r"^[A-Z][A-Z0-9_]*$")


def _env_name(value: str) -> str:
    if not _ENV_NAME.match(value):
        raise argparse.ArgumentTypeError("name an environment variable, never a value")
    return value


def _optional_path(value: str | None) -> Path | None:
    return Path(value) if value else None


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m mission_control.bootstrap.preflight")
    parser.add_argument("--profile", default=os.environ.get(PROFILE_ENV))
    parser.add_argument("--workspace-root", default=os.environ.get(WORKSPACE_ROOT_ENV))
    parser.add_argument("--db-dsn-env", type=_env_name)
    parser.add_argument("--platform", dest="system", help="assess another OS (Linux/Windows)")
    sub = parser.add_subparsers(dest="command")
    sub.add_parser("readiness", help="print the mc.local_readiness.v1 report only")
    guard = sub.add_parser("guard-launch", help="bind a run to a cluster or refuse the launch")
    guard.add_argument("--run-id", required=True)
    guard.add_argument("--cluster", required=True, help="cluster_id from the profile")
    guard.add_argument("--run-phase", required=True, help="the run's current phase")
    guard.add_argument("--ledger", required=True, type=Path)
    compose = sub.add_parser(
        "compose-bindings", help="apply reviewed selections to a bindings file and re-seal it"
    )
    compose.add_argument("--base", required=True, type=Path)
    compose.add_argument("--selections", required=True, type=Path, help="{pointer: value} JSON")
    compose.add_argument("--out", required=True, type=Path)
    return parser


async def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    settings = get_settings()
    inputs = ReadinessInputs(
        profile_path=_optional_path(args.profile) or settings.mission_control_local_run_profile,
        workspace_root=(
            _optional_path(args.workspace_root) or settings.mission_control_preflight_workspace_root
        ),
        system=args.system,
        db_dsn_env=args.db_dsn_env,
    )
    if args.command == "compose-bindings":
        base = _read_json(args.base)
        selections = _read_json(args.selections)
        if not isinstance(base, dict) or not isinstance(selections, dict):
            raise SystemExit("base and selections are JSON objects")
        composed = compose_launch_bindings(base, selections)
        args.out.write_text(composed.model_dump_json(indent=2) + "\n", encoding="utf-8")
        remaining = [pointer for pointer, _what in _placeholders(composed.model_dump(mode="json"))]
        print(json.dumps({"out": str(args.out), "unresolved_pointers": remaining}, indent=2))
        return 0 if not remaining else EXIT_BLOCKED
    if args.command == "guard-launch":
        profile, issues = load_profile(inputs.profile_path)
        if profile is None:
            print(json.dumps([item.model_dump(mode="json") for item in issues], indent=2))
            return EXIT_BLOCKED
        decision = await guard_cluster_launch(
            args.run_id,
            profile.cluster(args.cluster),
            clusters=profile.temporal_clusters,
            ledger=ClusterBindingLedger(args.ledger),
            run_phase=args.run_phase,
            probe=temporal_presence(settings_client_factory(settings)),
        )
        print(decision.model_dump_json(indent=2))
        return 0 if decision.admitted else EXIT_BLOCKED
    if args.command == "readiness" or inputs.profile_path is not None:
        report = await readiness(inputs, settings)
        if args.command == "readiness" or not report.ready:
            print(json.dumps(report.as_json(), indent=2, sort_keys=True))
            return 0 if report.ready else EXIT_BLOCKED
    result = await configured_preflight()
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["ready"] else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main(sys.argv[1:])))
