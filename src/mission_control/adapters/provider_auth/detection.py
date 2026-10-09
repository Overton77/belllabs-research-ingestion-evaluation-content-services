"""Per-profile auth surfaces and the secret-free route probe.

A surface lists, in the vendor's documented precedence, every credential signal that can
decide which route a provider process authenticates with: environment variables (by name),
settings keys, stored sign-in files (by existence) and Mission Control settings fields bound
for the lane. The probe never reads a credential value, never opens a credential file and
never attempts a login; it sees environment-variable *names*, file *existence*, settings-file
*key names* and (optionally) a parsed read-only status command.

Sources (retrieved with `npx ctx7@latest docs`, 2026-10-08):

* Claude Code / Agent SDK precedence 1-7, `.credentials.json` locations, `CLAUDE_CONFIG_DIR`,
  `apiKeyHelper` and settings `env` blocks: https://code.claude.com/docs/en/authentication
  (`/websites/code_claude`); the SDK honours the same variables:
  https://code.claude.com/docs/en/agent-sdk/quickstart (`/websites/code_claude_en_agent-sdk`).
  macOS keeps the login in the Keychain, so a missing file there is not "signed out".
* Codex `OPENAI_API_KEY` / `CODEX_API_KEY` / `CODEX_ACCESS_TOKEN` and the auth file under
  `CODEX_HOME` (`login_with_api_key` writes `auth.json` there):
  https://github.com/openai/codex/blob/main/codex-rs/login/src/auth/manager.rs (`/openai/codex`).
  The relative precedence of these variables over stored auth for the app-server is not
  documented, so they are treated as overriding (conservative). The keyring store mode
  (`cli_auth_credentials_store`) may leave no file at all.
* Cursor: `CURSOR_API_KEY` / `agent login` / `agent status`:
  https://cursor.com/docs/cli/reference/authentication (`/websites/cursor_cli`). Mission
  Control's bridge passes the settings-bound key with `allow_api_key_env_fallback=False`.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path, PurePath
from typing import Final

from mission_control.adapters.provider_auth.status import (
    PARSERS,
    StatusOutput,
    StatusParser,
    StatusReading,
    stored_route_for,
)
from mission_control.application.execution.auth_admission import (
    AuthObservation,
    AuthProfile,
    AuthRouteKind,
    ObservationSource,
)
from mission_control.domain.execution.lanes import LaneProfileName

# Only these path-valued variables are ever read by value (to locate files).
PATH_ENV_ALLOWLIST: Final[frozenset[str]] = frozenset({"CLAUDE_CONFIG_DIR", "CODEX_HOME"})


@dataclass(frozen=True)
class EnvSignal:
    name: str
    route: AuthRouteKind | None  # None: a credential whose precedence is not classified
    rank: int  # lower wins


@dataclass(frozen=True)
class StoredLogin:
    """A stored sign-in located by `dir_env` (or `default_dir` under the home directory)."""

    default_dir: str
    filename: str
    rank: int
    dir_env: str | None = None
    # The route a present file means; None: the file's mode needs a status reading.
    route: AuthRouteKind | None = None
    keychain_platforms: frozenset[str] = frozenset()


@dataclass(frozen=True)
class SettingsSignal:
    key: str
    route: AuthRouteKind | None
    rank: int


@dataclass(frozen=True)
class ProviderAuthSurface:
    lane_profile: LaneProfileName
    login_route: AuthRouteKind | None
    env: tuple[EnvSignal, ...] = ()
    stored_login: StoredLogin | None = None
    settings_keys: tuple[SettingsSignal, ...] = ()
    settings_env_block: bool = False
    bound_settings: tuple[SettingsSignal, ...] = ()
    status_argv: tuple[str, ...] | None = None
    status_parser: StatusParser | None = None
    sources: tuple[str, ...] = ()


_CLAUDE_ENV: Final = (
    EnvSignal("CLAUDE_CODE_USE_BEDROCK", "cloud_provider_credentials", 1),
    EnvSignal("CLAUDE_CODE_USE_VERTEX", "cloud_provider_credentials", 1),
    EnvSignal("CLAUDE_CODE_USE_FOUNDRY", "cloud_provider_credentials", 1),
    EnvSignal("CLAUDE_CODE_USE_ANTHROPIC_AWS", "cloud_provider_credentials", 1),
    EnvSignal("ANTHROPIC_AUTH_TOKEN", "gateway_token", 2),
    EnvSignal("ANTHROPIC_API_KEY", "api_key", 3),
    EnvSignal("CLAUDE_CODE_OAUTH_TOKEN", "oauth_token_env", 5),
    # Ranks 6 only when named, otherwise below /login: not classified here.
    EnvSignal("ANTHROPIC_PROFILE", None, 6),
)
_CLAUDE_SETTINGS: Final = (SettingsSignal("apiKeyHelper", "api_key", 4),)
_CLAUDE_SOURCES: Final = (
    "https://code.claude.com/docs/en/authentication",
    "https://code.claude.com/docs/en/agent-sdk/quickstart",
)
_CODEX_ENV: Final = (
    EnvSignal("CODEX_API_KEY", "api_key", 1),
    EnvSignal("OPENAI_API_KEY", "api_key", 1),
    EnvSignal("CODEX_ACCESS_TOKEN", "oauth_token_env", 2),
)
_CODEX_SOURCES: Final = (
    "https://github.com/openai/codex/blob/main/codex-rs/cli/src/login.rs",
    "https://github.com/openai/codex/blob/main/codex-rs/login/src/auth/manager.rs",
)
_CURSOR_SOURCES: Final = (
    "https://cursor.com/docs/cli/reference/authentication",
    "src/mission_control/adapters/cursor/bridge.py",
)

SURFACES: Final[Mapping[LaneProfileName, ProviderAuthSurface]] = {
    "deep_agents": ProviderAuthSurface(
        lane_profile="deep_agents",
        login_route=None,
        bound_settings=(
            SettingsSignal("openai_api_key", "api_key", 1),
            SettingsSignal("anthropic_api_key", "api_key", 1),
        ),
        sources=("src/mission_control/bootstrap/settings.py",),
    ),
    "cursor_local": ProviderAuthSurface(
        lane_profile="cursor_local",
        login_route="owner_cli_login",
        bound_settings=(SettingsSignal("cursor_api_key", "api_key", 1),),
        status_argv=("agent", "status"),
        status_parser="cursor_agent_status",
        sources=_CURSOR_SOURCES,
    ),
    "cursor_cloud": ProviderAuthSurface(
        lane_profile="cursor_cloud",
        login_route=None,
        bound_settings=(SettingsSignal("cursor_api_key", "api_key", 1),),
        sources=_CURSOR_SOURCES,
    ),
    "claude_agent_sdk": ProviderAuthSurface(
        lane_profile="claude_agent_sdk",
        login_route="owner_cli_login",
        env=_CLAUDE_ENV,
        stored_login=StoredLogin(
            default_dir=".claude",
            filename=".credentials.json",
            rank=7,
            dir_env="CLAUDE_CONFIG_DIR",
            route="owner_cli_login",
            keychain_platforms=frozenset({"darwin"}),
        ),
        settings_keys=_CLAUDE_SETTINGS,
        settings_env_block=True,
        status_argv=("claude", "auth", "status", "--json"),
        status_parser="claude_auth_status",
        sources=_CLAUDE_SOURCES,
    ),
    "claude_cloud": ProviderAuthSurface(
        lane_profile="claude_cloud",
        login_route="hosted_product_signin",
        # Cloud sessions do not read ANTHROPIC_API_KEY/ANTHROPIC_AUTH_TOKEN/apiKeyHelper; the
        # local CLI submits with the claude.ai sign-in (claude_cloud FEASIBILITY).
        env=(EnvSignal("CLAUDE_CODE_OAUTH_TOKEN", "oauth_token_env", 5),),
        stored_login=StoredLogin(
            default_dir=".claude",
            filename=".credentials.json",
            rank=7,
            dir_env="CLAUDE_CONFIG_DIR",
            route="hosted_product_signin",
            keychain_platforms=frozenset({"darwin"}),
        ),
        status_argv=("claude", "auth", "status", "--json"),
        status_parser="claude_auth_status",
        sources=(*_CLAUDE_SOURCES, "docs/qualification/lanes/claude_cloud/FEASIBILITY.md"),
    ),
    "codex": ProviderAuthSurface(
        lane_profile="codex",
        login_route="owner_cli_login",
        env=_CODEX_ENV,
        stored_login=StoredLogin(
            default_dir=".codex", filename="auth.json", rank=9, dir_env="CODEX_HOME"
        ),
        status_argv=("codex", "login", "status"),
        status_parser="codex_login_status",
        sources=_CODEX_SOURCES,
    ),
    "codex_cloud": ProviderAuthSurface(
        lane_profile="codex_cloud",
        login_route="hosted_product_signin",
        env=_CODEX_ENV,
        stored_login=StoredLogin(
            default_dir=".codex", filename="auth.json", rank=9, dir_env="CODEX_HOME"
        ),
        status_argv=("codex", "login", "status"),
        status_parser="codex_login_status",
        sources=(*_CODEX_SOURCES, "docs/qualification/lanes/codex_cloud/FEASIBILITY.md"),
    ),
}


def env_names_of(environ: Mapping[str, str]) -> frozenset[str]:
    """The names of non-empty variables; values are dropped here and never kept."""

    return frozenset(name for name, value in environ.items() if value.strip())


def settings_key_names(path: Path) -> tuple[frozenset[str], frozenset[str]] | None:
    """Top-level key names and `env`-block key names of a settings JSON file (no values)."""

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if not isinstance(payload, dict):
        return None
    env_block = payload.get("env")
    env_keys = (
        frozenset(str(key) for key in env_block) if isinstance(env_block, dict) else frozenset()
    )
    return frozenset(str(key) for key in payload), env_keys


@dataclass(frozen=True)
class WorkerAuthContext:
    """What the probe may see: names, paths and existence - never values."""

    env_names: frozenset[str]
    home: PurePath
    platform: str = "linux"
    path_env: Mapping[str, str] = field(default_factory=dict)
    bound_settings: frozenset[str] = frozenset()
    settings_files: tuple[PurePath, ...] = ()
    exists: Callable[[PurePath], bool] = lambda path: Path(path).exists()
    settings_keys: Callable[[PurePath], tuple[frozenset[str], frozenset[str]] | None] = (
        lambda path: settings_key_names(Path(path))
    )

    def __post_init__(self) -> None:
        unexpected = set(self.path_env) - PATH_ENV_ALLOWLIST
        if unexpected:
            raise ValueError(f"path_env only carries {sorted(PATH_ENV_ALLOWLIST)}")

    @classmethod
    def from_environment(
        cls,
        environ: Mapping[str, str],
        *,
        home: PurePath,
        platform: str,
        bound_settings: Iterable[str] = (),
        settings_files: Iterable[PurePath] = (),
    ) -> WorkerAuthContext:
        return cls(
            env_names=env_names_of(environ),
            home=home,
            platform=platform,
            path_env={k: environ[k] for k in PATH_ENV_ALLOWLIST if environ.get(k)},
            bound_settings=frozenset(bound_settings),
            settings_files=tuple(settings_files),
        )


@dataclass(frozen=True)
class _Present:
    rank: int
    route: AuthRouteKind | None
    label: str
    kind: str  # env | settings | settings_env | stored | bound


def _stored_login_path(stored: StoredLogin, context: WorkerAuthContext) -> PurePath:
    base = context.path_env.get(stored.dir_env or "")
    root = type(context.home)(base) if base else context.home / stored.default_dir
    return root / stored.filename


def probe(
    profile: AuthProfile,
    context: WorkerAuthContext,
    *,
    status: StatusOutput | None = None,
    source: ObservationSource | None = None,
    fixture: bool = False,
    probe_versions: Mapping[str, str] | None = None,
    now: datetime | None = None,
) -> AuthObservation:
    """Classify which route the provider would use for `profile` in `context`."""

    surface = SURFACES[profile.lane_profile]
    present: list[_Present] = []
    env_by_name = {signal.name: signal for signal in surface.env}
    for signal in surface.env:
        if signal.name in context.env_names:
            present.append(_Present(signal.rank, signal.route, signal.name, "env"))
    for bound in surface.bound_settings:
        if bound.key in context.bound_settings:
            present.append(_Present(bound.rank, bound.route, f"settings:{bound.key}", "bound"))
    for path in context.settings_files:
        keys = context.settings_keys(path)
        if keys is None:
            continue
        top, env_keys = keys
        for setting in surface.settings_keys:
            if setting.key in top:
                present.append(
                    _Present(setting.rank, setting.route, f"settings:{setting.key}", "settings")
                )
        if surface.settings_env_block:
            for name in sorted(env_keys):
                env_signal = env_by_name.get(name)
                if env_signal is not None:
                    present.append(
                        _Present(
                            env_signal.rank,
                            env_signal.route,
                            f"settings.env:{name}",
                            "settings_env",
                        )
                    )

    reading: StatusReading | None = None
    if status is not None and surface.status_parser is not None:
        reading = PARSERS[surface.status_parser](status)

    # The stored sign-in: present per the status reading when there is one, else per the
    # file (a keychain platform without a file is unknown). File existence alone never
    # proves `signed_in`; only a status reading does.
    stored = surface.stored_login
    stored_route: AuthRouteKind | None = stored.route if stored is not None else None
    if reading is not None and surface.login_route is not None:
        stored_route = stored_route_for(reading, surface.login_route) or stored_route
    stored_present: bool | None
    if reading is not None and reading.signed_in is not None:
        stored_present = reading.signed_in
    elif stored is None:
        stored_present = None if surface.login_route is not None else False
    elif context.exists(_stored_login_path(stored, context)):
        stored_present = True
    elif context.platform in stored.keychain_platforms:
        stored_present = None
    else:
        stored_present = False
    if stored_present:
        rank = stored.rank if stored is not None else 9
        present.append(_Present(rank, stored_route, "stored_login", "stored"))

    signed_in: bool | None = None
    if surface.login_route is not None and profile.route == surface.login_route:
        if reading is not None and reading.signed_in is not None:
            signed_in = reading.signed_in and stored_route == surface.login_route
        elif stored_present is False:
            signed_in = False

    present.sort(key=lambda item: item.rank)
    unclassified = tuple(
        item.label for item in present if item.route is None and item.kind == "env"
    )
    effective = present[0].route if present else None
    requested_rank = min(
        (item.rank for item in present if item.route == profile.route), default=None
    )
    shadowing = [
        item
        for item in present
        if item.route != profile.route
        and item.route is not None
        and (requested_rank is None or item.rank < requested_rank)
    ]
    shadowing_env = tuple(dict.fromkeys(item.label for item in shadowing if item.kind == "env"))
    shadowing_settings = tuple(
        dict.fromkeys(item.label for item in shadowing if item.kind != "env")
    )
    remaining = [item for item in present if item.kind != "env" or item.label not in shadowing_env]
    route_without = remaining[0].route if remaining else None

    credential_present: bool | None
    ref = profile.credential_ref
    if ref is None:
        credential_present = stored_present
    elif ref.startswith("env:"):
        credential_present = ref.removeprefix("env:") in context.env_names
    elif ref.startswith("settings:"):
        credential_present = ref.removeprefix("settings:") in context.bound_settings
    else:
        # secret:/keyring: refs are resolved by the secret broker at launch, not here.
        credential_present = any(item.route == profile.route for item in present) or None

    if source is None:
        source = "status_command" if reading is not None else "environment_scan"
    if fixture:
        source = "fixture"
    summary = reading.summary if reading is not None else None
    if reading is not None and not reading.format_documented:
        summary = f"{summary} [status format undocumented]"
    return AuthObservation(
        lane_profile=profile.lane_profile,
        source=source,
        fixture=fixture,
        effective_route=effective,
        route_without_shadowing=route_without,
        present_routes=tuple(
            dict.fromkeys(item.route for item in present if item.route is not None)
        ),
        shadowing_env=shadowing_env,
        shadowing_settings=shadowing_settings,
        unclassified_credentials=unclassified,
        credential_present=credential_present,
        signed_in=signed_in,
        status_summary=summary,
        account_fingerprint=reading.account_fingerprint if reading is not None else None,
        probe_versions=dict(probe_versions or {}),
        source_refs=surface.sources,
        observed_at=now or datetime.now(UTC),
    )


__all__ = [
    "PATH_ENV_ALLOWLIST",
    "SURFACES",
    "EnvSignal",
    "ProviderAuthSurface",
    "SettingsSignal",
    "StoredLogin",
    "WorkerAuthContext",
    "env_names_of",
    "probe",
    "settings_key_names",
]
