"""MP-05 composition: deployment auth profiles, the local auth preflight and capacity bounds.

The worker composes one :class:`AuthAdmissionService` from settings. Profiles come from the
operator-reviewed ``mc.auth_profile.v1`` document; the preflight observes names, paths and
read-only status commands only (never credential values, never a login). Provider child
processes receive an explicit environment built by :func:`provider_child_environment`, so a
subscription route never inherits an API key the worker holds for another lane.
"""

from __future__ import annotations

import json
import os
import sys
from collections.abc import Mapping
from pathlib import Path, PurePath

from pydantic import TypeAdapter, ValidationError

from mission_control.adapters.provider_auth.detection import PATH_ENV_ALLOWLIST, WorkerAuthContext
from mission_control.adapters.provider_auth.preflight import (
    LocalAuthPreflight,
    StatusRunner,
    SubprocessStatusRunner,
)
from mission_control.application.execution.auth_admission import (
    AuthAdmission,
    AuthAdmissionService,
    AuthProfile,
    InMemoryAuthProfileRegistry,
)
from mission_control.application.execution.usage_admission import LimitWaitPolicy
from mission_control.bootstrap.settings import Settings

_PROFILES = TypeAdapter(list[AuthProfile])

# Settings fields whose bound value a lane may read as a credential (``settings:<field>``).
_BOUND_CREDENTIAL_SETTINGS: tuple[str, ...] = (
    "openai_api_key",
    "anthropic_api_key",
    "cursor_api_key",
)


class AuthProfilesUnavailable(RuntimeError):
    """The configured profiles document is missing, unreadable or not ``mc.auth_profile.v1``."""


def load_auth_profiles(path: Path) -> dict[str, AuthProfile]:
    """The profiles of a ``{"profiles": [...]}`` document keyed by ``profile_id`` (unique)."""

    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        raise AuthProfilesUnavailable(f"auth profiles document unreadable: {path}") from error
    if not isinstance(document, dict) or not isinstance(document.get("profiles"), list):
        raise AuthProfilesUnavailable(f"auth profiles document needs a 'profiles' list: {path}")
    try:
        profiles = _PROFILES.validate_python(document["profiles"])
    except ValidationError as error:
        raise AuthProfilesUnavailable(f"auth profiles document invalid: {path}: {error}") from error
    by_id: dict[str, AuthProfile] = {}
    for profile in profiles:
        if profile.profile_id in by_id:
            raise AuthProfilesUnavailable(f"duplicate auth profile id {profile.profile_id!r}")
        by_id[profile.profile_id] = profile
    return by_id


def worker_platform() -> str:
    """The platform vocabulary the detection surface uses (``linux``/``darwin``/``win32``)."""

    return (
        "darwin" if sys.platform == "darwin" else ("win32" if sys.platform == "win32" else "linux")
    )


def worker_auth_context(
    settings: Settings,
    environ: Mapping[str, str],
    *,
    home: PurePath | None = None,
    platform: str | None = None,
    project_root: PurePath | None = None,
) -> WorkerAuthContext:
    """What the preflight may see on this worker: variable names, bound-setting names and the
    locations of the vendor settings files whose key names are inspected (values never)."""

    home_path = home if home is not None else PurePath(Path.home())
    root = project_root if project_root is not None else PurePath(Path.cwd())
    # Same path flavour as `home` (tests pass POSIX paths on any host).
    claude_dir = (
        type(home_path)(environ["CLAUDE_CONFIG_DIR"])
        if environ.get("CLAUDE_CONFIG_DIR")
        else home_path / ".claude"
    )
    bound = tuple(
        name for name in _BOUND_CREDENTIAL_SETTINGS if getattr(settings, name) is not None
    )
    return WorkerAuthContext.from_environment(
        environ,
        home=home_path,
        platform=platform if platform is not None else worker_platform(),
        bound_settings=bound,
        settings_files=(
            claude_dir / "settings.json",
            root / ".claude" / "settings.json",
            root / ".claude" / "settings.local.json",
        ),
    )


def capacity_policy(settings: Settings) -> LimitWaitPolicy:
    """The finite capacity-wait bounds the operation workflow applies (1:1 with settings)."""

    return LimitWaitPolicy(
        max_waits=settings.mission_control_capacity_max_waits,
        max_total_wait_s=settings.mission_control_capacity_max_total_wait_s,
        reset_margin_s=settings.mission_control_capacity_reset_margin_s,
        fallback_backoff_s=settings.mission_control_capacity_fallback_backoff_s,
        max_backoff_s=settings.mission_control_capacity_max_backoff_s,
    )


def status_probe_environment(environ: Mapping[str, str]) -> dict[str, str]:
    """The environment a read-only vendor status command runs with: ``PATH``/``HOME``-class
    variables and the allow-listed location variables only; no credential variables."""

    keep = {"PATH", "HOME", "USERPROFILE", "SYSTEMROOT", "TEMP", "TMP", "LANG", "LC_ALL", "TERM"}
    keep |= PATH_ENV_ALLOWLIST
    return {name: value for name, value in environ.items() if name.upper() in keep}


def compose_auth_admission(
    settings: Settings,
    *,
    environ: Mapping[str, str] | None = None,
    status_runner: StatusRunner | None = None,
    home: PurePath | None = None,
    platform: str | None = None,
    project_root: PurePath | None = None,
) -> AuthAdmissionService | None:
    """The worker's auth admission, or ``None`` when no profiles document is configured.

    The status probe is composed only when ``MISSION_CONTROL_AUTH_STATUS_PROBE`` is on (or a
    runner is injected); it runs the surface's read-only status command with a bounded timeout.
    """

    path = settings.mission_control_auth_profiles_path
    if path is None:
        return None
    env = dict(environ if environ is not None else os.environ)
    profiles = load_auth_profiles(path)
    context = worker_auth_context(
        settings, env, home=home, platform=platform, project_root=project_root
    )
    runner = status_runner
    if runner is None and settings.mission_control_auth_status_probe:
        runner = SubprocessStatusRunner(
            status_probe_environment(env),
            timeout_s=settings.mission_control_auth_status_timeout_s,
        )
    preflight = LocalAuthPreflight(lambda _profile: context, status_runner=runner)
    return AuthAdmissionService(
        InMemoryAuthProfileRegistry(profiles),
        preflight,
        allow_unqualified_routes=settings.mission_control_allow_unqualified_auth_routes,
    )


def provider_child_environment(
    environ: Mapping[str, str], admission: AuthAdmission, *, extra: Mapping[str, str] | None = None
) -> dict[str, str]:
    """An explicit child environment for the admitted provider process.

    Drops every variable the admission asked to unset (the higher-precedence credentials that
    would shadow the admitted route, e.g. ``ANTHROPIC_API_KEY`` on a subscription route) and
    applies ``extra`` afterwards. Launchers must pass this instead of the worker's process
    environment; an ``extra`` key that re-introduces an unset name is refused.
    """

    unset = {name.upper() for name in admission.env_unset}
    child = {name: value for name, value in environ.items() if name.upper() not in unset}
    for name, value in (extra or {}).items():
        if name.upper() in unset:
            raise ValueError(f"child environment may not re-introduce {name}")
        child[name] = value
    return child


__all__ = [
    "AuthProfilesUnavailable",
    "capacity_policy",
    "compose_auth_admission",
    "load_auth_profiles",
    "provider_child_environment",
    "status_probe_environment",
    "worker_auth_context",
    "worker_platform",
]
