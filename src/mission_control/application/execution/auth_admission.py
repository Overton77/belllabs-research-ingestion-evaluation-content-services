"""Auth route admission: profiles, preflight observations and pointed rejections (MP-05).

An auth profile names *how* a lane authenticates and what that route is believed to bill,
separately from the model pin and the execution environment (SPEC-02 "Authentication,
subscription usage and limits"). Admission runs before any provider session is created: it
intersects the profile with the vendor-documented route table below and with a secret-free
preflight observation of the worker, and either returns an `mc.auth_admission.v1` record (the
source of the binding's `AuthPin`) or refuses with JSON-pointer issues.

Two rules are load-bearing:

* **No silent subscription-to-API fallback.** If the credential the provider would actually
  pick (its documented precedence) is not the requested route, or bills a mode the profile did
  not consent to, admission refuses with `AUTH_ROUTE_SHADOWED` / `AUTH_BILLING_NOT_CONSENTED`.
  The owner's subscription priority is a preference, never permission to meter-bill.
* **Credentials never cross this boundary.** Profiles and observations carry references and
  environment-variable *names*; any value that looks like a credential is refused at
  validation (`looks_secret`), and records are re-checked before they are returned.

Auth admission does not qualify a lane. Hosted `claude_cloud`/`codex_cloud` stay Outcome 3
(unqualified) whatever the route; lane admission (`lane_requirements`) still applies.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Final, Literal, Protocol

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    ValidationInfo,
    field_validator,
    model_validator,
)

from mission_control.domain.authoring.canonical import stable_json_digest
from mission_control.domain.execution.bindings import AuthPin, BillingMode
from mission_control.domain.execution.lanes import DIGEST_PATTERN, LaneProfileName

AUTH_PROFILE_SCHEMA: Final = "mc.auth_profile.v1"
AUTH_OBSERVATION_SCHEMA: Final = "mc.auth_observation.v1"
AUTH_ADMISSION_SCHEMA: Final = "mc.auth_admission.v1"

AuthRouteKind = Literal[
    # The vendor CLI's personal sign-in on this machine (subscription OAuth).
    "owner_cli_login",
    # A long-lived OAuth/access token supplied through an environment variable.
    "oauth_token_env",
    # A metered API key (Console / platform key).
    "api_key",
    # A bearer token for an LLM gateway or proxy (billing is the gateway's).
    "gateway_token",
    # Bedrock / Vertex / Foundry style cloud-provider credentials.
    "cloud_provider_credentials",
    # The provider-hosted product's own account sign-in (hosted lanes).
    "hosted_product_signin",
]
AUTH_ROUTE_KINDS: Final[tuple[AuthRouteKind, ...]] = (
    "owner_cli_login",
    "oauth_token_env",
    "api_key",
    "gateway_token",
    "cloud_provider_credentials",
    "hosted_product_signin",
)
# Routes whose credential is a stored sign-in; the others resolve a `credential_ref`.
LOGIN_ROUTES: Final[frozenset[AuthRouteKind]] = frozenset(
    {"owner_cli_login", "hosted_product_signin"}
)

RouteSupport = Literal["documented", "policy_restricted", "unqualified", "unsupported"]

# Issue codes (upper-snake, as `lane_requirements`).
AUTH_PROFILE_MISSING: Final = "AUTH_PROFILE_MISSING"
AUTH_PROFILE_UNKNOWN: Final = "AUTH_PROFILE_UNKNOWN"
AUTH_PROFILE_LANE_MISMATCH: Final = "AUTH_PROFILE_LANE_MISMATCH"
AUTH_ROUTE_UNSUPPORTED: Final = "AUTH_ROUTE_UNSUPPORTED"
AUTH_ROUTE_UNQUALIFIED: Final = "AUTH_ROUTE_UNQUALIFIED"
AUTH_ROUTE_POLICY_RESTRICTED: Final = "AUTH_ROUTE_POLICY_RESTRICTED"
AUTH_BILLING_MODE_MISMATCH: Final = "AUTH_BILLING_MODE_MISMATCH"
AUTH_BILLING_NOT_CONSENTED: Final = "AUTH_BILLING_NOT_CONSENTED"
AUTH_PREFLIGHT_MISSING: Final = "AUTH_PREFLIGHT_MISSING"
AUTH_CREDENTIAL_MISSING: Final = "AUTH_CREDENTIAL_MISSING"
AUTH_NOT_SIGNED_IN: Final = "AUTH_NOT_SIGNED_IN"
AUTH_STATUS_UNKNOWN: Final = "AUTH_STATUS_UNKNOWN"
AUTH_ROUTE_SHADOWED: Final = "AUTH_ROUTE_SHADOWED"
AUTH_ROUTE_AMBIGUOUS: Final = "AUTH_ROUTE_AMBIGUOUS"
AUTH_ACCOUNT_MISMATCH: Final = "AUTH_ACCOUNT_MISMATCH"
AUTH_OBSERVATION_NOT_REDACTED: Final = "AUTH_OBSERVATION_NOT_REDACTED"
AUTH_OBSERVATION_STALE: Final = "AUTH_OBSERVATION_STALE"


# --- Credential-shaped value detection ---------------------------------------------------------

# Shapes that are never allowed in a profile, observation, record, log line or report. The
# provider_auth adapter's redactor is built on the same list.
SECRET_PATTERNS: Final[tuple[tuple[str, re.Pattern[str]], ...]] = (
    ("anthropic_key", re.compile(r"(?<![A-Za-z0-9])sk-ant-[A-Za-z0-9_\-]{6,}")),
    ("openai_key", re.compile(r"(?<![A-Za-z0-9])sk-(?:proj-|svcacct-|admin-)?[A-Za-z0-9_\-*]{6,}")),
    ("cursor_key", re.compile(r"\b(?:key|crsr)_[A-Za-z0-9]{16,}")),
    ("bearer", re.compile(r"(?i)\bbearer\s+[A-Za-z0-9._~+/=\-]{8,}")),
    ("jwt", re.compile(r"\beyJ[A-Za-z0-9_\-]{6,}\.[A-Za-z0-9_\-]{6,}\.[A-Za-z0-9_\-]*")),
    ("github_token", re.compile(r"\b(?:gh[pousr]|github_pat)_[A-Za-z0-9_]{16,}")),
    ("aws_access_key", re.compile(r"\b(?:AKIA|ASIA)[0-9A-Z]{16}\b")),
    ("email", re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}")),
    # Long mixed-case alphanumeric runs (opaque tokens). Lower-case identifiers and
    # `sha256:<hex>` content digests are not matched.
    (
        "opaque_token",
        re.compile(
            r"(?<![A-Za-z0-9:_\-])"
            r"(?=[A-Za-z0-9_\-]*[a-z])(?=[A-Za-z0-9_\-]*[A-Z])(?=[A-Za-z0-9_\-]*[0-9])"
            r"[A-Za-z0-9_\-]{40,}(?![A-Za-z0-9_\-])"
        ),
    ),
)


def secret_kinds(text: str) -> tuple[str, ...]:
    """Which credential shapes occur in `text` (empty: none)."""

    return tuple(name for name, pattern in SECRET_PATTERNS if pattern.search(text))


def looks_secret(text: str) -> bool:
    return bool(secret_kinds(text))


def _refuse_secret(value: str | None, field: str) -> str | None:
    if value is not None and looks_secret(value):
        raise ValueError(f"{field} must be a reference, never a credential, token or address")
    return value


_REF_PATTERN: Final = re.compile(r"^(env|settings|secret|keyring):[A-Za-z0-9_.\-/]{1,190}$")
_ENV_NAME_PATTERN: Final = re.compile(r"^[A-Z][A-Z0-9_]{0,127}$")


class AuthContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


# --- mc.auth_profile.v1 ------------------------------------------------------------------------


class AuthProfile(AuthContract):
    """A deployment-registry auth profile: the route, its declared billing mode, the account
    reference and the explicit billing consent set. Secret-free by construction.

    * `credential_ref` names where a credential-route secret lives (`env:NAME`,
      `settings:field`, `secret:<path>`, `keyring:<entry>`); login routes carry none.
    * `allowed_billing_modes` is the owner's explicit consent; it defaults to exactly the
      declared `billing_mode`, so a subscription profile never admits API billing unless the
      owner lists `api` here. A profile can never consent to `unknown` implicitly.
    * `owner_attestation_ref` records the owner's acknowledgement for a
      `policy_restricted` route (a reference to a recorded decision, not free text).
    * `shadowing="unset"` asks the launcher to remove the named higher-precedence
      environment variables from the provider child process instead of refusing; the
      removal list is recorded on the admission and must be applied by the adapter.
    """

    schema_version: Literal["mc.auth_profile.v1"] = AUTH_PROFILE_SCHEMA
    profile_id: str = Field(min_length=1, max_length=192)
    lane_profile: LaneProfileName
    route: AuthRouteKind
    billing_mode: BillingMode
    allowed_billing_modes: tuple[BillingMode, ...] = ()
    account_ref: str | None = Field(default=None, min_length=1, max_length=256)
    account_fingerprint: str | None = Field(default=None, pattern=DIGEST_PATTERN)
    entitlement_refs: tuple[str, ...] = ()
    credential_ref: str | None = Field(default=None, min_length=1, max_length=200)
    owner_attestation_ref: str | None = Field(default=None, min_length=1, max_length=512)
    shadowing: Literal["reject", "unset"] = "reject"
    max_observation_age_s: int = Field(default=300, ge=1, le=86_400)

    @field_validator("profile_id", "account_ref", "owner_attestation_ref")
    @classmethod
    def _no_secret_text(cls, value: str | None, info: ValidationInfo) -> str | None:
        return _refuse_secret(value, info.field_name or "value")

    @field_validator("entitlement_refs")
    @classmethod
    def _no_secret_entitlements(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        for item in value:
            _refuse_secret(item, "entitlement_refs")
        return value

    @model_validator(mode="after")
    def _route_shape(self) -> AuthProfile:
        if self.route in LOGIN_ROUTES:
            if self.credential_ref is not None:
                raise ValueError(f"a {self.route} profile resolves a stored sign-in, not a ref")
        else:
            if self.credential_ref is None:
                raise ValueError(f"a {self.route} profile names its credential_ref")
            if not _REF_PATTERN.match(self.credential_ref) or looks_secret(
                self.credential_ref.split(":", 1)[1]
            ):
                raise ValueError(
                    "credential_ref is env:/settings:/secret:/keyring:<name>, never a value"
                )
        if "unknown" in self.allowed_billing_modes and self.billing_mode != "unknown":
            raise ValueError("consenting to unknown billing requires billing_mode unknown")
        return self

    @property
    def consented_billing(self) -> frozenset[BillingMode]:
        return frozenset(self.allowed_billing_modes or (self.billing_mode,))


# --- Vendor route table --------------------------------------------------------------------------


@dataclass(frozen=True)
class RouteSpec:
    """One documented (or explicitly refused) route for one lane profile."""

    support: RouteSupport
    billing_mode: BillingMode
    sources: tuple[str, ...]
    note: str


_CLAUDE_AUTH = "https://code.claude.com/docs/en/authentication"
_CLAUDE_SDK_QUICKSTART = "https://code.claude.com/docs/en/agent-sdk/quickstart"
_CODEX_LOGIN_SRC = "https://github.com/openai/codex/blob/main/codex-rs/cli/src/login.rs"
_CODEX_AUTH_SRC = "https://github.com/openai/codex/blob/main/codex-rs/protocol/src/auth.rs"
_CURSOR_CLI_AUTH = "https://cursor.com/docs/cli/reference/authentication"
_CURSOR_SDK_CHANGELOG = "https://cursor.com/docs/sdk/changelog"
_CLAUDE_CLOUD_FEAS = "docs/qualification/lanes/claude_cloud/FEASIBILITY.md"
_CODEX_CLOUD_FEAS = "docs/qualification/lanes/codex_cloud/FEASIBILITY.md"
_CURSOR_BRIDGE = "src/mission_control/adapters/cursor/bridge.py"

# Retrieved with `npx ctx7@latest docs` on 2026-10-08 (/websites/code_claude_en_agent-sdk,
# /websites/code_claude, /openai/codex, /websites/cursor_cli, /websites/cursor) plus the
# repository's hosted feasibility studies. Absent (profile, route) pairs are unsupported.
ROUTE_TABLE: Final[Mapping[LaneProfileName, Mapping[AuthRouteKind, RouteSpec]]] = {
    "deep_agents": {
        "api_key": RouteSpec(
            "documented",
            "api",
            ("src/mission_control/bootstrap/settings.py",),
            "model-provider API keys bound in settings",
        ),
    },
    "cursor_local": {
        "api_key": RouteSpec(
            "documented",
            "unknown",
            (_CURSOR_CLI_AUTH, _CURSOR_BRIDGE),
            "CURSOR_API_KEY bound in settings and passed explicitly; the bridge launches with "
            "allow_api_key_env_fallback=False. Plan/usage billing is not observable: unknown",
        ),
        "owner_cli_login": RouteSpec(
            "unqualified",
            "unknown",
            (_CURSOR_CLI_AUTH, _CURSOR_SDK_CHANGELOG),
            "`agent login` is documented for the CLI and SDK browser login for TypeScript "
            "(1.0.27); the pinned Python SDK bridge path binds an explicit key",
        ),
    },
    "cursor_cloud": {
        "api_key": RouteSpec(
            "documented",
            "unknown",
            (_CURSOR_CLI_AUTH,),
            "Cloud Agents API key; account billing is not observable: unknown",
        ),
    },
    "claude_agent_sdk": {
        "api_key": RouteSpec(
            "documented",
            "api",
            (_CLAUDE_SDK_QUICKSTART, _CLAUDE_AUTH),
            "ANTHROPIC_API_KEY, sent as X-Api-Key; precedence 3",
        ),
        "cloud_provider_credentials": RouteSpec(
            "documented",
            "provider_account",
            (_CLAUDE_SDK_QUICKSTART,),
            "CLAUDE_CODE_USE_BEDROCK / _VERTEX / _FOUNDRY / _ANTHROPIC_AWS; precedence 1",
        ),
        "gateway_token": RouteSpec(
            "unqualified",
            "unknown",
            (_CLAUDE_AUTH,),
            "ANTHROPIC_AUTH_TOKEN bearer for an LLM gateway; precedence 2",
        ),
        "oauth_token_env": RouteSpec(
            "policy_restricted",
            "subscription",
            (_CLAUDE_AUTH, _CLAUDE_SDK_QUICKSTART),
            "CLAUDE_CODE_OAUTH_TOKEN from `claude setup-token`; precedence 5. Anthropic does "
            "not allow third-party products built on the Agent SDK to offer claude.ai login "
            "or rate limits unless previously approved",
        ),
        "owner_cli_login": RouteSpec(
            "policy_restricted",
            "subscription",
            (_CLAUDE_AUTH, _CLAUDE_SDK_QUICKSTART),
            "subscription OAuth from `/login` (lowest precedence, 7); every higher-precedence "
            "credential in the child environment overrides it. Same Agent SDK policy limit",
        ),
    },
    "codex": {
        "owner_cli_login": RouteSpec(
            "documented",
            "subscription",
            (_CODEX_LOGIN_SRC, _CODEX_AUTH_SRC),
            "ChatGPT sign-in (`codex login`; AuthMode Chatgpt) stored under CODEX_HOME",
        ),
        "api_key": RouteSpec(
            "documented",
            "api",
            (_CODEX_LOGIN_SRC, _CODEX_AUTH_SRC),
            "OPENAI_API_KEY / CODEX_API_KEY or `login_api_key` (AuthMode ApiKey)",
        ),
        "oauth_token_env": RouteSpec(
            "unqualified",
            "unknown",
            (_CODEX_AUTH_SRC,),
            "CODEX_ACCESS_TOKEN / personal access token modes",
        ),
    },
    "claude_cloud": {
        "hosted_product_signin": RouteSpec(
            "documented",
            "subscription",
            (_CLAUDE_CLOUD_FEAS, _CLAUDE_AUTH),
            "claude.ai account (`claude auth login`); cloud sessions draw subscription limits",
        ),
        "oauth_token_env": RouteSpec(
            "unqualified",
            "subscription",
            (_CLAUDE_CLOUD_FEAS,),
            "whether a `claude setup-token` token drives --cloud is undocumented",
        ),
    },
    "codex_cloud": {
        "hosted_product_signin": RouteSpec(
            "documented",
            "subscription",
            (_CODEX_CLOUD_FEAS, _CODEX_AUTH_SRC),
            "ChatGPT account sign-in only; cloud tasks require uses_codex_backend()",
        ),
    },
}

_UNSUPPORTED_NOTES: Final[Mapping[tuple[str, str], tuple[str, tuple[str, ...]]]] = {
    ("claude_cloud", "api_key"): (
        "API keys, Bedrock, Vertex and Foundry are rejected by the hosted product",
        (_CLAUDE_CLOUD_FEAS,),
    ),
    ("claude_cloud", "cloud_provider_credentials"): (
        "cloud-provider credentials are rejected by the hosted product",
        (_CLAUDE_CLOUD_FEAS,),
    ),
    ("codex_cloud", "api_key"): (
        "API-key auth does not drive Codex cloud tasks (init_backend: 'Not signed in')",
        (_CODEX_CLOUD_FEAS,),
    ),
}


def route_spec(lane_profile: LaneProfileName, route: AuthRouteKind) -> RouteSpec:
    """The route's support for this profile; absent pairs are `unsupported`."""

    spec = ROUTE_TABLE.get(lane_profile, {}).get(route)
    if spec is not None:
        return spec
    note, sources = _UNSUPPORTED_NOTES.get(
        (lane_profile, route), ("no documented route for this lane profile", ())
    )
    return RouteSpec("unsupported", "unknown", sources, note)


# --- mc.auth_observation.v1 ---------------------------------------------------------------------

ObservationSource = Literal["fixture", "status_command", "environment_scan", "not_probed"]


class AuthObservation(AuthContract):
    """What a preflight saw on the worker, without any credential value.

    `effective_route` is the route the provider would actually use with the child
    environment as given (documented precedence); `route_without_shadowing` is the route
    after removing `shadowing_env`. `signed_in` is `None` when sign-in cannot be proven
    without reading a credential (e.g. a keychain store and no status probe). `fixture`
    marks recorded fixtures; admissions built from them carry the label forward.
    """

    schema_version: Literal["mc.auth_observation.v1"] = AUTH_OBSERVATION_SCHEMA
    lane_profile: LaneProfileName
    source: ObservationSource
    fixture: bool = False
    effective_route: AuthRouteKind | None = None
    route_without_shadowing: AuthRouteKind | None = None
    present_routes: tuple[AuthRouteKind, ...] = ()
    shadowing_env: tuple[str, ...] = ()
    shadowing_settings: tuple[str, ...] = ()
    unclassified_credentials: tuple[str, ...] = ()
    credential_present: bool | None = None
    signed_in: bool | None = None
    status_summary: str | None = Field(default=None, max_length=256)
    account_fingerprint: str | None = Field(default=None, pattern=DIGEST_PATTERN)
    probe_versions: dict[str, str] = Field(default_factory=dict)
    source_refs: tuple[str, ...] = ()
    observed_at: AwareDatetime

    @field_validator("shadowing_env", "unclassified_credentials")
    @classmethod
    def _names_only(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        for name in value:
            if not _ENV_NAME_PATTERN.match(name):
                raise ValueError(f"{name!r} is not an environment-variable name")
        return value

    def secret_free(self) -> bool:
        return not looks_secret(self.model_dump_json())


# --- mc.auth_admission.v1 -----------------------------------------------------------------------


class AuthAdmission(AuthContract):
    """The admitted route: the binding's `AuthPin` plus what admission relied on."""

    schema_version: Literal["mc.auth_admission.v1"] = AUTH_ADMISSION_SCHEMA
    profile_id: str
    lane_profile: LaneProfileName
    route: AuthRouteKind
    route_support: RouteSupport
    billing_mode: BillingMode
    account_ref: str | None = None
    entitlement_refs: tuple[str, ...] = ()
    credential_ref: str | None = None
    owner_attestation_ref: str | None = None
    env_unset: tuple[str, ...] = ()
    observation_digest: str = Field(pattern=DIGEST_PATTERN)
    observation_source: ObservationSource
    fixture: bool
    source_refs: tuple[str, ...] = ()
    disclosures: tuple[str, ...] = ()
    admitted_at: AwareDatetime

    def auth_pin(self) -> AuthPin:
        return AuthPin(
            profile=self.profile_id,
            billing_mode=self.billing_mode,
            account_ref=self.account_ref,
            entitlement_refs=self.entitlement_refs,
        )


class AuthIssue(AuthContract):
    code: str = Field(min_length=1)
    pointer: str
    lane_profile: str
    message: str = Field(min_length=1)


class AuthAdmissionRejected(ValueError):
    """Auth admission refused; raised before any provider session is created."""

    def __init__(self, issues: tuple[AuthIssue, ...]) -> None:
        if not issues:
            raise ValueError("a rejection carries at least one issue")
        super().__init__("; ".join(f"{i.code} at {i.pointer}: {i.message}" for i in issues))
        self.issues = issues

    @property
    def codes(self) -> tuple[str, ...]:
        return tuple(issue.code for issue in self.issues)


# --- Pure admission -----------------------------------------------------------------------------


def admit_auth(
    profile: AuthProfile | None,
    observation: AuthObservation | None,
    *,
    lane_profile: LaneProfileName,
    pointer: str = "/environment",
    requested_profile: str | None = None,
    allow_unqualified_routes: bool = False,
    now: datetime | None = None,
) -> AuthAdmission | tuple[AuthIssue, ...]:
    """Admit `profile` for `lane_profile` against `observation`, or return the issues.

    `pointer` is the JSON pointer of the effective environment block; issues point at
    `{pointer}/auth/profile` (the profile reference) or `{pointer}/auth` (the route).
    """

    at = now or datetime.now(UTC)
    profile_ptr = f"{pointer}/auth/profile"
    route_ptr = f"{pointer}/auth"
    issues: list[AuthIssue] = []

    def refuse(code: str, ptr: str, message: str) -> None:
        issues.append(AuthIssue(code=code, pointer=ptr, lane_profile=lane_profile, message=message))

    if profile is None:
        if requested_profile is None:
            refuse(
                AUTH_PROFILE_MISSING,
                profile_ptr,
                f"lane profile {lane_profile} needs an explicit auth profile; Mission Control "
                "does not pick a credential from the worker's ambient environment",
            )
        else:
            refuse(
                AUTH_PROFILE_UNKNOWN,
                profile_ptr,
                f"auth profile {requested_profile!r} is not in the deployment registry",
            )
        return tuple(issues)
    if profile.lane_profile != lane_profile:
        refuse(
            AUTH_PROFILE_LANE_MISMATCH,
            profile_ptr,
            f"auth profile {profile.profile_id} is for {profile.lane_profile}, not {lane_profile}",
        )
        return tuple(issues)

    spec = route_spec(lane_profile, profile.route)
    if spec.support == "unsupported":
        refuse(
            AUTH_ROUTE_UNSUPPORTED,
            route_ptr,
            f"route {profile.route} is unsupported for {lane_profile}: {spec.note}",
        )
    elif spec.support == "unqualified" and not allow_unqualified_routes:
        refuse(
            AUTH_ROUTE_UNQUALIFIED,
            route_ptr,
            f"route {profile.route} is unqualified for {lane_profile}: {spec.note}",
        )
    elif spec.support == "policy_restricted" and profile.owner_attestation_ref is None:
        refuse(
            AUTH_ROUTE_POLICY_RESTRICTED,
            route_ptr,
            f"route {profile.route} for {lane_profile} is restricted by vendor policy "
            f"({spec.note}); the profile must cite owner_attestation_ref",
        )
    if spec.support != "unsupported" and profile.billing_mode != spec.billing_mode:
        refuse(
            AUTH_BILLING_MODE_MISMATCH,
            route_ptr,
            f"route {profile.route} bills {spec.billing_mode} for {lane_profile}; the profile "
            f"declares {profile.billing_mode}",
        )
    if issues:
        return tuple(issues)

    if observation is None or observation.source == "not_probed":
        refuse(
            AUTH_PREFLIGHT_MISSING,
            route_ptr,
            "no auth preflight observation; admission cannot prove the route before launch",
        )
        return tuple(issues)
    if observation.lane_profile != lane_profile:
        refuse(
            AUTH_PROFILE_LANE_MISMATCH,
            route_ptr,
            f"preflight observed {observation.lane_profile}, not {lane_profile}",
        )
        return tuple(issues)
    if not observation.secret_free():
        refuse(
            AUTH_OBSERVATION_NOT_REDACTED,
            route_ptr,
            "the preflight observation contains a credential-shaped value; it is discarded",
        )
        return tuple(issues)
    age = (at - observation.observed_at).total_seconds()
    if age > profile.max_observation_age_s or age < -60:
        refuse(
            AUTH_OBSERVATION_STALE,
            route_ptr,
            f"preflight is {int(age)}s old; the profile admits at most "
            f"{profile.max_observation_age_s}s",
        )
        return tuple(issues)

    if observation.unclassified_credentials:
        refuse(
            AUTH_ROUTE_AMBIGUOUS,
            route_ptr,
            "the child environment carries credentials whose precedence is not classified: "
            + ", ".join(observation.unclassified_credentials),
        )
        return tuple(issues)

    env_unset: tuple[str, ...] = ()
    effective = observation.effective_route
    if effective != profile.route:
        shadowed_by_env = (
            bool(observation.shadowing_env)
            and not observation.shadowing_settings
            and observation.route_without_shadowing == profile.route
        )
        if profile.shadowing == "unset" and shadowed_by_env:
            env_unset = observation.shadowing_env
            effective = profile.route
        elif effective is not None:
            sources = [*observation.shadowing_env, *observation.shadowing_settings]
            effective_billing = route_spec(lane_profile, effective).billing_mode
            refuse(
                AUTH_ROUTE_SHADOWED,
                route_ptr,
                f"the provider would authenticate with {effective} (bills {effective_billing})"
                f" instead of the requested {profile.route}"
                + (f", because {', '.join(sources)} take precedence" if sources else "")
                + "; Mission Control never falls back from the requested route to another "
                "billing route",
            )
            return tuple(issues)

    if profile.route in LOGIN_ROUTES:
        if observation.signed_in is False:
            refuse(
                AUTH_NOT_SIGNED_IN,
                route_ptr,
                f"{lane_profile} is not signed in for {profile.route}; sign in on the worker "
                "out of band (Mission Control never attempts a login)",
            )
        elif observation.signed_in is None or effective is None:
            refuse(
                AUTH_STATUS_UNKNOWN,
                route_ptr,
                f"sign-in for {profile.route} cannot be proven from the preflight "
                f"({observation.status_summary or 'no status probe'})",
            )
    elif observation.credential_present is not True or effective is None:
        refuse(
            AUTH_CREDENTIAL_MISSING,
            route_ptr,
            f"credential {profile.credential_ref} for {profile.route} is not present",
        )
    if issues:
        return tuple(issues)

    observed_billing = route_spec(lane_profile, profile.route).billing_mode
    if observed_billing not in profile.consented_billing:
        refuse(
            AUTH_BILLING_NOT_CONSENTED,
            route_ptr,
            f"route {profile.route} bills {observed_billing}; the profile consents only to "
            f"{', '.join(sorted(profile.consented_billing))}",
        )
        return tuple(issues)
    if (
        profile.account_fingerprint is not None
        and observation.account_fingerprint is not None
        and profile.account_fingerprint != observation.account_fingerprint
    ):
        refuse(
            AUTH_ACCOUNT_MISMATCH,
            route_ptr,
            "the signed-in account is not the profile's account",
        )
        return tuple(issues)

    disclosures: list[str] = []
    if observed_billing == "unknown":
        disclosures.append("billing_unknown: cost is not observable on this route")
    if spec.support != "documented":
        disclosures.append(f"route_{spec.support}: {spec.note}")
    if profile.account_fingerprint is not None and observation.account_fingerprint is None:
        disclosures.append("account_unverified: the preflight could not identify the account")
    if env_unset:
        disclosures.append("env_unset: " + ", ".join(env_unset))
    if observation.fixture:
        disclosures.append("fixture: admitted from a recorded fixture, not a live preflight")

    admission = AuthAdmission(
        profile_id=profile.profile_id,
        lane_profile=lane_profile,
        route=profile.route,
        route_support=spec.support,
        billing_mode=observed_billing,
        account_ref=profile.account_ref,
        entitlement_refs=profile.entitlement_refs,
        credential_ref=profile.credential_ref,
        owner_attestation_ref=profile.owner_attestation_ref,
        env_unset=env_unset,
        observation_digest=stable_json_digest(observation),
        observation_source=observation.source,
        fixture=observation.fixture,
        source_refs=tuple(dict.fromkeys((*spec.sources, *observation.source_refs))),
        disclosures=tuple(disclosures),
        admitted_at=at,
    )
    if looks_secret(admission.model_dump_json()):  # pragma: no cover - guarded by validators
        raise RuntimeError("auth admission record would carry a credential-shaped value")
    return admission


# --- Ports and the application service ---------------------------------------------------------


class AuthProfileRegistry(Protocol):
    async def get(self, profile_id: str) -> AuthProfile | None: ...


class AuthPreflightPort(Protocol):
    """Observes the worker without reading credential values or attempting a login."""

    async def preflight(self, profile: AuthProfile) -> AuthObservation: ...


class InMemoryAuthProfileRegistry:
    def __init__(self, profiles: Mapping[str, AuthProfile] | None = None) -> None:
        self._profiles = dict(profiles or {})

    async def get(self, profile_id: str) -> AuthProfile | None:
        return self._profiles.get(profile_id)


class AuthAdmissionService:
    """Resolve the selected profile, preflight it, admit it; raise before any launch."""

    def __init__(
        self,
        registry: AuthProfileRegistry,
        preflight: AuthPreflightPort,
        *,
        allow_unqualified_routes: bool = False,
    ) -> None:
        self._registry = registry
        self._preflight = preflight
        self._allow_unqualified = allow_unqualified_routes

    async def admit(
        self,
        lane_profile: LaneProfileName,
        profile_id: str | None,
        *,
        pointer: str = "/environment",
        now: datetime | None = None,
    ) -> AuthAdmission:
        profile = await self._registry.get(profile_id) if profile_id is not None else None
        observation: AuthObservation | None = None
        if profile is not None and profile.lane_profile == lane_profile:
            observation = await self._preflight.preflight(profile)
        result = admit_auth(
            profile,
            observation,
            lane_profile=lane_profile,
            pointer=pointer,
            requested_profile=profile_id,
            allow_unqualified_routes=self._allow_unqualified,
            now=now,
        )
        if isinstance(result, AuthAdmission):
            return result
        raise AuthAdmissionRejected(result)


__all__ = [
    "AUTH_ACCOUNT_MISMATCH",
    "AUTH_ADMISSION_SCHEMA",
    "AUTH_BILLING_MODE_MISMATCH",
    "AUTH_BILLING_NOT_CONSENTED",
    "AUTH_CREDENTIAL_MISSING",
    "AUTH_NOT_SIGNED_IN",
    "AUTH_OBSERVATION_NOT_REDACTED",
    "AUTH_OBSERVATION_SCHEMA",
    "AUTH_OBSERVATION_STALE",
    "AUTH_PREFLIGHT_MISSING",
    "AUTH_PROFILE_LANE_MISMATCH",
    "AUTH_PROFILE_MISSING",
    "AUTH_PROFILE_SCHEMA",
    "AUTH_PROFILE_UNKNOWN",
    "AUTH_ROUTE_AMBIGUOUS",
    "AUTH_ROUTE_KINDS",
    "AUTH_ROUTE_POLICY_RESTRICTED",
    "AUTH_ROUTE_SHADOWED",
    "AUTH_ROUTE_UNQUALIFIED",
    "AUTH_ROUTE_UNSUPPORTED",
    "AUTH_STATUS_UNKNOWN",
    "LOGIN_ROUTES",
    "ROUTE_TABLE",
    "SECRET_PATTERNS",
    "AuthAdmission",
    "AuthAdmissionRejected",
    "AuthAdmissionService",
    "AuthIssue",
    "AuthObservation",
    "AuthPreflightPort",
    "AuthProfile",
    "AuthProfileRegistry",
    "AuthRouteKind",
    "InMemoryAuthProfileRegistry",
    "RouteSpec",
    "RouteSupport",
    "admit_auth",
    "looks_secret",
    "route_spec",
    "secret_kinds",
]
