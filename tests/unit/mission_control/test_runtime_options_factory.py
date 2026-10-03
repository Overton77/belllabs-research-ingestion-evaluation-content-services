"""Operator startup hooks cannot be selected by requests or silently misbind apps."""

from types import ModuleType
from uuid import uuid4

import pytest

from mission_control.adapters.auth.jwt import ApplicationAuthentication
from mission_control.adapters.storage.control_plane_payloads import UnavailablePayloadStore
from mission_control.application.execution.service import AdmissionPolicyRegistry
from mission_control.application.installations.registry import ApplicationBinding
from mission_control.bootstrap.api import (
    ApplicationDeployment,
    MissionDeployment,
    RuntimeOptions,
    load_runtime_options,
)
from mission_control.domain.authoring.extensions import ExtensionRegistry


def deployment(tmp_path):
    binding = ApplicationBinding.seal(
        application_id="biotech",
        installation_id=uuid4(),
        binding_version="1",
        supabase_project_ref="local-test",
        database_secret_ref="MC_TEST_DSN",
        accepted_issuers={"https://issuer.invalid"},
        accepted_audiences={"authenticated"},
        required_component_version="transitional-local-v1",
    )
    auth = ApplicationAuthentication(
        binding=binding,
        issuer="https://issuer.invalid",
        audience="authenticated",
        public_jwks_file=tmp_path / "public.json",
    )
    return MissionDeployment(
        storage_mode="transitional_local",
        max_request_bytes=1000,
        applications=(ApplicationDeployment(authentication=auth),),
    )


def test_reviewed_factory_receives_typed_deployment_and_returns_exact_app_map(
    tmp_path, monkeypatch
):
    import sys

    configured = deployment(tmp_path)
    options = RuntimeOptions(
        AdmissionPolicyRegistry(), ExtensionRegistry(), UnavailablePayloadStore()
    )
    module = ModuleType("mission_control_test_operator")

    def build(value):
        assert value == configured
        return {"biotech": options}

    module.build = build
    monkeypatch.setitem(sys.modules, module.__name__, module)
    monkeypatch.setenv("MISSION_CONTROL_RUNTIME_OPTIONS_FACTORY", module.__name__ + ":build")
    assert load_runtime_options(configured) == {"biotech": options}


@pytest.mark.parametrize("result", [{}, {"other": object()}, {"biotech": object()}, []])
def test_incomplete_unknown_or_untyped_mapping_rejected(tmp_path, monkeypatch, result):
    import sys

    module = ModuleType("mission_control_test_operator_invalid")
    module.build = lambda _: result
    monkeypatch.setitem(sys.modules, module.__name__, module)
    with pytest.raises(ValueError, match="exactly"):
        load_runtime_options(deployment(tmp_path), module.__name__ + ":build")


@pytest.mark.parametrize(
    "value", ["../operator.py:build", "module:build()", "module:object.method"]
)
def test_factory_syntax_cannot_contain_paths_or_expressions(tmp_path, value):
    with pytest.raises(ValueError, match="module:callable"):
        load_runtime_options(deployment(tmp_path), value)
