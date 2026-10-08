"""FT-G7: temporalio 1.34 connection, worker versioning and Search Attribute declarations."""

from __future__ import annotations

from importlib.metadata import version
from types import SimpleNamespace
from typing import Any, cast

import pytest
from pydantic import SecretStr
from temporalio.common import VersioningBehavior

from mission_control.adapters.temporal import client as temporal_client
from mission_control.adapters.temporal.client import (
    MEMO_WARN_SIZE,
    PAYLOADS_WARN_SIZE,
    TemporalConnection,
    TemporalConnectionError,
    connect_temporal,
    mission_control_payload_limits,
    resolve_temporal_connection,
)
from mission_control.adapters.temporal.search_attributes import (
    BELLLABS_SEARCH_ATTRIBUTE_KEYS,
    merged_search_attributes,
    mission_visibility_attributes,
)
from mission_control.adapters.temporal.versioning import (
    release_build_id,
    worker_deployment_config,
)
from mission_control.bootstrap.api import MissionDeployment
from mission_control.bootstrap.preflight import temporal_targets
from mission_control.domain.programs.search_attributes import (
    BELLLABS_CORE_SEARCH_ATTRIBUTES,
    BELLLABS_SEARCH_ATTRIBUTES,
    MISSION_VISIBILITY_SEARCH_ATTRIBUTES,
    SQL_VISIBILITY_SLOTS,
    MissionVisibilityValues,
    lane_for_runtime,
    phase_for_disposition,
)
from tests.fixtures.isolated_settings import isolated_settings

KEY = "tmprl-cloud-key-never-printed"


def test_sdk_is_upgraded_to_1_34() -> None:
    major, minor = (int(part) for part in version("temporalio").split(".")[:2])
    assert (major, minor) >= (1, 34)


def test_payload_limits_use_the_renamed_connect_fields_and_warn_sizes() -> None:
    limits = mission_control_payload_limits()
    assert limits.payloads_warn_size == PAYLOADS_WARN_SIZE == 512 * 1024
    assert limits.memo_warn_size == MEMO_WARN_SIZE == 2 * 1024


def test_no_data_converter_payload_limit_configuration_remains() -> None:
    from pathlib import Path

    root = Path(temporal_client.__file__).resolve().parents[2]
    offenders = [
        path
        for path in root.rglob("*.py")
        if "DataConverter(" in path.read_text("utf-8")
        and "payload_limits" in path.read_text("utf-8")
    ]
    assert offenders == []


def test_local_is_the_default_even_when_the_cloud_key_is_present() -> None:
    settings = isolated_settings(
        temporal_cloud_api_key=SecretStr(KEY),
        temporal_address="namespace.acct.tmprl.cloud:7233",
        temporal_namespace="namespace.acct",
    )
    connection = resolve_temporal_connection(
        settings, address="127.0.0.1:7233", namespace="default"
    )
    assert connection == TemporalConnection("local", "127.0.0.1:7233", "default")
    assert connection.tls is False and connection.api_key is None


def test_cloud_target_connects_with_api_key_and_tls_to_the_configured_frontend() -> None:
    settings = isolated_settings(
        temporal_target="cloud",
        temporal_cloud_api_key=SecretStr(KEY),
        temporal_address="namespace.acct.tmprl.cloud:7233",
        temporal_namespace="namespace.acct",
    )
    connection = resolve_temporal_connection(settings)
    assert connection.target == "cloud" and connection.tls is True
    assert connection.address == "namespace.acct.tmprl.cloud:7233"
    assert connection.namespace == "namespace.acct"
    assert connection.api_key == KEY
    # The key never renders.
    assert KEY not in repr(connection)
    assert KEY not in str(connection.describe())
    assert connection.describe()["api_key"] == "set"


def test_cloud_target_refuses_a_missing_key_and_a_split_deployment() -> None:
    with pytest.raises(TemporalConnectionError, match="TEMPORAL_CLOUD_API_KEY"):
        resolve_temporal_connection(isolated_settings(temporal_target="cloud"))
    settings = isolated_settings(
        temporal_target="cloud",
        temporal_cloud_api_key=SecretStr(KEY),
        temporal_address="namespace.acct.tmprl.cloud:7233",
        temporal_namespace="namespace.acct",
    )
    with pytest.raises(TemporalConnectionError, match="address"):
        resolve_temporal_connection(settings, address="127.0.0.1:7233")
    with pytest.raises(TemporalConnectionError, match="namespace"):
        resolve_temporal_connection(
            settings, address="namespace.acct.tmprl.cloud:7233", namespace="default"
        )


@pytest.mark.asyncio
async def test_connect_passes_limits_tls_and_key(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, Any]] = []

    async def fake_connect(address: str, **kwargs: Any) -> str:
        calls.append({"address": address, **kwargs})
        return "client"

    monkeypatch.setattr(temporal_client.Client, "connect", fake_connect)
    cloud = TemporalConnection("cloud", "ns.acct.tmprl.cloud:7233", "ns.acct", KEY)
    assert await connect_temporal(cloud) == "client"
    local = TemporalConnection("local", "127.0.0.1:7233", "default")
    await connect_temporal(local)
    assert calls[0]["api_key"] == KEY and calls[0]["tls"] is True
    assert calls[0]["namespace"] == "ns.acct"
    assert calls[1]["api_key"] is None and calls[1]["tls"] is False
    for call in calls:
        assert call["payload_limits"].payloads_warn_size == PAYLOADS_WARN_SIZE
        assert call["payload_limits"].memo_warn_size == MEMO_WARN_SIZE


def test_preflight_reports_the_target_per_application() -> None:
    def app(application_id: str, address: str | None) -> SimpleNamespace:
        return SimpleNamespace(
            authentication=SimpleNamespace(binding=SimpleNamespace(application_id=application_id)),
            temporal=(
                SimpleNamespace(address=address, namespace="default")
                if address is not None
                else None
            ),
        )

    deployment = cast(
        MissionDeployment,
        SimpleNamespace(applications=(app("biotech", "127.0.0.1:7233"), app("plain", None))),
    )
    assert temporal_targets(deployment, isolated_settings(temporal_cloud_api_key=KEY)) == [
        {
            "application_id": "biotech",
            "target": "local",
            "address": "127.0.0.1:7233",
            "namespace": "default",
            "tls": False,
            "api_key": "absent",
        }
    ]
    cloud = temporal_targets(deployment, isolated_settings(temporal_target="cloud"))
    assert cloud[0]["target"] == "cloud" and "TEMPORAL_CLOUD_API_KEY" in str(cloud[0]["error"])
    assert KEY not in str(cloud)


def test_worker_deployment_config_is_built_from_the_release() -> None:
    settings = isolated_settings(temporal_build_id="2026.10.07+abc")
    config = worker_deployment_config(settings)
    assert config is not None
    assert config.use_worker_versioning is True
    assert config.version.deployment_name == "mission-control"
    assert config.version.build_id == "2026.10.07+abc"
    assert config.default_versioning_behavior == VersioningBehavior.AUTO_UPGRADE
    assert release_build_id(isolated_settings()) == version("mission-control")
    assert worker_deployment_config(isolated_settings(temporal_worker_versioning=False)) is None


def test_search_attribute_registry_fits_the_sql_visibility_slots() -> None:
    assert set(MISSION_VISIBILITY_SEARCH_ATTRIBUTES) == {
        "mc_mission_id",
        "mc_run_id",
        "mc_lane",
        "mc_phase",
        "ForkedFromRunId",
    }
    assert set(BELLLABS_SEARCH_ATTRIBUTES) == {
        *BELLLABS_CORE_SEARCH_ATTRIBUTES,
        *MISSION_VISIBILITY_SEARCH_ATTRIBUTES,
    }
    used: dict[str, int] = {}
    for kind in BELLLABS_SEARCH_ATTRIBUTES.values():
        used[kind] = used.get(kind, 0) + 1
    for kind, count in used.items():
        assert count <= SQL_VISIBILITY_SLOTS[kind], kind
    assert {key.name for key in BELLLABS_SEARCH_ATTRIBUTE_KEYS} == set(BELLLABS_SEARCH_ATTRIBUTES)


def test_mission_visibility_values_map_to_typed_attributes() -> None:
    values = MissionVisibilityValues(
        run_id="run-2",
        mission_id="mission-1",
        lane="cursor_local",
        phase="executing",
        forked_from_run_ids=("run-1",),
    )
    assert values.as_mapping() == {
        "mc_run_id": ["run-2"],
        "mc_mission_id": "mission-1",
        "mc_lane": "cursor_local",
        "mc_phase": "executing",
        "ForkedFromRunId": ["run-1"],
    }
    assert MissionVisibilityValues(run_id="run-2").as_mapping() == {"mc_run_id": ["run-2"]}
    typed = mission_visibility_attributes(values)
    assert {pair.key.name for pair in typed.search_attributes} == set(values.as_mapping())
    merged = merged_search_attributes(None, typed, None)
    assert merged is not None and len(merged.search_attributes) == 5
    assert merged_search_attributes(None, None) is None


@pytest.mark.parametrize(
    "kwargs",
    [
        {"run_id": ""},
        {"run_id": "r", "mission_id": ""},
        {"run_id": "r", "lane": "codex"},
        {"run_id": "r", "phase": "paused"},
        {"run_id": "r", "forked_from_run_ids": ("r",)},
        {"run_id": "r", "forked_from_run_ids": ("",)},
    ],
)
def test_mission_visibility_values_reject_undeclared_values(kwargs: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        MissionVisibilityValues(**kwargs)


def test_lane_and_phase_vocabulary() -> None:
    assert lane_for_runtime("deep_agent") == "deep_agents"
    assert lane_for_runtime("native") is None
    assert lane_for_runtime("cursor", "cursor_cloud") == "cursor_cloud"
    with pytest.raises(ValueError):
        lane_for_runtime("cursor", "cursor_remote")
    assert phase_for_disposition("completed") == "completed"
    assert phase_for_disposition("in_doubt") == "in_doubt"
    assert phase_for_disposition("unexpected") == "failed"
