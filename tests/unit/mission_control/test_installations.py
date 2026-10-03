from uuid import UUID

import pytest

from mission_control.application.installations.registry import (
    ApplicationBinding,
    ApplicationRegistry,
    ApplicationScopeDenied,
    InstallationObservation,
    InstallationUnavailable,
    VerifiedApplicationIdentity,
    request_scope,
)

INSTALLATION = UUID("00000000-0000-0000-0000-000000000001")
TENANT = UUID("00000000-0000-0000-0000-000000000002")


def setup_registry():
    binding = ApplicationBinding.seal(
        application_id="biotech",
        installation_id=INSTALLATION,
        binding_version="1",
        supabase_project_ref="biotech-project",
        database_secret_ref="BIOTECH_DATABASE_URL",
        accepted_issuers=frozenset({"https://biotech.invalid/auth/v1"}),
        accepted_audiences=frozenset({"authenticated"}),
        required_component_version="1.0.0",
    )
    identity = VerifiedApplicationIdentity(
        issuer="https://biotech.invalid/auth/v1",
        audiences=frozenset({"authenticated"}),
        application_id="biotech",
        installation_id=INSTALLATION,
        tenant_id=TENANT,
    )
    return ApplicationRegistry((binding,)), identity


def test_verified_installation_required_and_revocation_is_immediate():
    registry, identity = setup_registry()
    with pytest.raises(InstallationUnavailable):
        registry.resolve("biotech", identity)
    registry.observe(
        "biotech",
        InstallationObservation(
            INSTALLATION,
            "biotech",
            "biotech-project",
            frozenset({"1.0.0"}),
        ),
    )
    assert registry.resolve("biotech", identity).database_secret_ref == "BIOTECH_DATABASE_URL"
    assert request_scope(identity) == (
        "mc/00000000-0000-0000-0000-000000000001/biotech/00000000-0000-0000-0000-000000000002"
    )
    registry.disable("biotech")
    with pytest.raises(InstallationUnavailable):
        registry.resolve("biotech", identity)


def test_application_switch_cannot_select_other_database():
    registry, identity = setup_registry()
    with pytest.raises(ApplicationScopeDenied):
        registry.resolve("ai-engineer", identity)


def test_mismatched_project_never_marks_application_ready():
    registry, identity = setup_registry()
    with pytest.raises(InstallationUnavailable):
        registry.observe(
            "biotech",
            InstallationObservation(
                INSTALLATION,
                "biotech",
                "other-project",
                frozenset({"1.0.0"}),
            ),
        )
    with pytest.raises(InstallationUnavailable):
        registry.resolve("biotech", identity)


def test_immutable_pin_cannot_silently_rotate_to_another_binding():
    registry, identity = setup_registry()
    registry.observe(
        "biotech",
        InstallationObservation(
            INSTALLATION,
            "biotech",
            "biotech-project",
            frozenset({"1.0.0"}),
        ),
    )
    with pytest.raises(InstallationUnavailable):
        registry.resolve("biotech", identity, pinned_binding_digest="sha256:" + "b" * 64)


def test_binding_digest_detects_project_and_authority_drift():
    registry, _ = setup_registry()
    binding = registry.bindings["biotech"]
    for field, value in (
        ("supabase_project_ref", "other-project"),
        ("database_secret_ref", "OTHER_DATABASE_URL"),
        ("accepted_issuers", frozenset({"https://other.invalid/auth/v1"})),
    ):
        payload = binding.model_dump(mode="python")
        payload[field] = value
        with pytest.raises(ValueError, match="digest does not match"):
            ApplicationBinding.model_validate(payload)
