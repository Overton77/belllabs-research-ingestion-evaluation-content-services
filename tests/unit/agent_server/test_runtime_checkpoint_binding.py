"""Offline proofs for the private runtime saver/store binding and its pinned descriptor."""

from __future__ import annotations

import hashlib
import importlib.util
import json
from contextlib import asynccontextmanager
from pathlib import Path
from types import ModuleType

import pytest
from psycopg.conninfo import conninfo_to_dict
from pydantic import SecretStr, ValidationError

from mission_control.adapters.deep_agents.persistence import (
    RUNTIME_SCHEMA,
    RuntimeConninfoError,
    StandalonePersistenceLifespan,
    conninfo_search_path_schema,
    runtime_checkpoint_conninfo,
)
from mission_control.adapters.deep_agents.runtime_persistence_verifier import (
    EXPECTED_COLUMNS,
    EXPECTED_INDEXES,
    PINNED_SAVER_VERSIONS,
    PINNED_STORE_VERSIONS,
)
from mission_control.bootstrap.settings import RUNTIME_CHECKPOINT_SCHEMA
from tests.fixtures.isolated_settings import isolated_settings

ROOT = Path(__file__).resolve().parents[3]
RUNTIME_ROOT = ROOT / "packages" / "mission-control-db-contract" / "runtime"
OPTION = "-c search_path=mission_control_runtime,pg_temp"


def _generator() -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        "mc_runtime_descriptor_generator", RUNTIME_ROOT / "generate_descriptor.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _descriptor() -> dict:
    return json.loads((RUNTIME_ROOT / "descriptor.json").read_text(encoding="utf-8"))


# ------------------------------------------------------------------ DSN builder


@pytest.mark.parametrize(
    "dsn",
    [
        "postgresql://ckpt:secret@127.0.0.1:5432/app",
        "postgresql://ckpt:secret@127.0.0.1:5432/app?connect_timeout=10&sslmode=disable",
        "host=127.0.0.1 port=5432 dbname=app user=ckpt password=secret",
    ],
)
def test_builder_sets_exactly_one_private_search_path_option(dsn: str) -> None:
    conninfo = runtime_checkpoint_conninfo(dsn)
    parsed = conninfo_to_dict(conninfo)
    assert parsed["options"] == OPTION
    assert conninfo.count("options=") == 1
    assert parsed["dbname"] == "app" and parsed["user"] == "ckpt"
    assert conninfo_search_path_schema(conninfo) == RUNTIME_SCHEMA


@pytest.mark.parametrize(
    "dsn",
    [
        "postgresql://ckpt:secret@127.0.0.1/app?options=-c%20statement_timeout%3D5s",
        "postgresql://ckpt:secret@127.0.0.1/app?options=-c%20search_path%3Dpublic",
        "host=127.0.0.1 dbname=app user=ckpt password=secret options='-c work_mem=4MB'",
        "host=127.0.0.1 dbname=app user=ckpt password=secret options='-csearch_path=public'",
        "postgresql://ckpt:secret@127.0.0.1/app?search_path=public",
    ],
)
def test_builder_rejects_preexisting_options_instead_of_overriding(dsn: str) -> None:
    with pytest.raises(RuntimeConninfoError) as raised:
        runtime_checkpoint_conninfo(dsn)
    assert "secret" not in str(raised.value)


def test_builder_never_echoes_an_invalid_dsn() -> None:
    with pytest.raises(RuntimeConninfoError) as raised:
        runtime_checkpoint_conninfo("postgresql://ckpt:hunter2@127.0.0.1/app?bogus_key=1")
    assert "hunter2" not in str(raised.value)
    assert raised.value.__cause__ is None


@pytest.mark.parametrize(
    "schema", ["public", "belllabs_langgraph", "mission_control", "pg_temp", "Bad-Name", ""]
)
def test_builder_rejects_fallback_or_invalid_schemas(schema: str) -> None:
    with pytest.raises(RuntimeConninfoError):
        runtime_checkpoint_conninfo("postgresql://ckpt@127.0.0.1/app", schema)


# ------------------------------------------------------------------ settings


def test_settings_default_to_the_private_runtime_schema() -> None:
    settings = isolated_settings(
        langgraph_checkpoint_database_direct=SecretStr("postgresql://ckpt:pw@127.0.0.1/app"),
    )
    assert RUNTIME_CHECKPOINT_SCHEMA == RUNTIME_SCHEMA == "mission_control_runtime"
    assert settings.langgraph_checkpoint_schema == RUNTIME_SCHEMA
    assert conninfo_to_dict(settings.langgraph_checkpoint_dsn)["options"] == OPTION


def test_settings_reject_a_checkpoint_dsn_that_carries_options() -> None:
    settings = isolated_settings(
        langgraph_checkpoint_database_direct=SecretStr(
            "postgresql://ckpt:pw@127.0.0.1/app?options=-c%20search_path%3Dpublic"
        ),
    )
    with pytest.raises(RuntimeConninfoError):
        _ = settings.langgraph_checkpoint_dsn


@pytest.mark.parametrize("schema", ["public", "belllabs_langgraph", "mission_control"])
def test_settings_reject_fallback_schemas(schema: str) -> None:
    with pytest.raises(ValidationError, match="forbidden fallback schema"):
        isolated_settings(langgraph_checkpoint_schema=schema)


def test_vendor_setup_is_never_accepted_for_runtime_schema_or_production() -> None:
    with pytest.raises(ValidationError, match="runtime-apply"):
        isolated_settings(langgraph_checkpoint_setup=True)
    with pytest.raises(ValidationError, match="production checkpoints"):
        isolated_settings(
            bell_labs_environment="production", langgraph_checkpoint_schema="disposable_ckpt"
        )
    with pytest.raises(ValidationError, match="production checkpoints"):
        isolated_settings(
            bell_labs_environment="production",
            langgraph_checkpoint_schema="disposable_ckpt",
            langgraph_checkpoint_setup=True,
        )
    production = isolated_settings(bell_labs_environment="production")
    assert production.langgraph_checkpoint_schema == RUNTIME_SCHEMA
    assert not production.langgraph_checkpoint_setup
    # Disposable development schemas may still use the explicit test-only switch.
    assert isolated_settings(
        langgraph_checkpoint_schema="disposable_ckpt", langgraph_checkpoint_setup=True
    ).langgraph_checkpoint_setup


# ------------------------------------------------------------------ lifespan


class _Resource:
    def __init__(self) -> None:
        self.setup_calls = 0

    async def setup(self) -> None:
        self.setup_calls += 1


def _factory(created: list[_Resource]):  # type: ignore[no-untyped-def]
    @asynccontextmanager
    async def context(_conninfo):  # type: ignore[no-untyped-def]
        resource = _Resource()
        created.append(resource)
        yield resource

    return context


@pytest.mark.asyncio
async def test_production_lifespan_never_calls_vendor_setup() -> None:
    created: list[_Resource] = []
    conninfo = runtime_checkpoint_conninfo("postgresql://ckpt@127.0.0.1/app")
    async with StandalonePersistenceLifespan(
        conninfo, saver_factory=_factory(created), store_factory=_factory(created)
    ):
        pass
    assert [resource.setup_calls for resource in created] == [0, 0]


@pytest.mark.parametrize(
    "conninfo",
    [
        runtime_checkpoint_conninfo("postgresql://ckpt@127.0.0.1/app"),
        "postgresql://ckpt@127.0.0.1/app",
        "postgresql://ckpt@127.0.0.1/app?options=-c%20search_path%3Dpublic",
    ],
)
def test_test_only_setup_refuses_runtime_public_or_implicit_schema(conninfo: str) -> None:
    with pytest.raises(RuntimeConninfoError, match="test-only vendor setup"):
        StandalonePersistenceLifespan(conninfo, run_setup=True)


# ------------------------------------------------------------------ descriptor


def test_descriptor_matches_installed_pinned_vendor_source() -> None:
    assert _generator().check() == []


def test_descriptor_steps_are_hashed_ordered_and_flag_concurrent_indexes() -> None:
    descriptor = _descriptor()
    assert descriptor["component"] == "mission_control_runtime"
    assert descriptor["schema"] == RUNTIME_SCHEMA
    assert descriptor["role"] == "mission_control_checkpointer"
    assert descriptor["pins"]["langgraph-checkpoint-postgres"] == "3.1.1"
    assert descriptor["pins"]["langgraph"] == "1.2.10"
    assert descriptor["session"]["runtime_libpq_options"] == OPTION
    steps = descriptor["steps"]
    ids = [step["id"] for step in steps]
    assert ids == [
        "prerequisite.schema_role",
        *(f"saver.v{v}" for v in PINNED_SAVER_VERSIONS),
        "store.ledger",
        *(f"store.v{v}" for v in PINNED_STORE_VERSIONS),
        "post.grants",
    ]
    for step in steps:
        assert hashlib.sha256(step["sql"].encode("utf-8")).hexdigest() == step["sha256"]
        assert step["kind"] in {"sql", "vendor"}
        assert step["transactional"] is not step["concurrent"]
        assert all(query.lstrip().upper().startswith("SELECT") for query in step["verify"])
    concurrent = {step["id"] for step in steps if step["concurrent"]}
    assert concurrent == {"saver.v6", "saver.v7", "saver.v8", "store.v1"}
    for step in steps:
        if step["concurrent"]:
            assert "CONCURRENTLY" in step["sql"]
            assert any("indisvalid AND i.indisready" in query for query in step["verify"])
            assert any("NOT (i.indisvalid AND i.indisready)" in q for q in step["precheck"])
        recorded = step["records_version"]
        if step["kind"] == "vendor" and step["id"] != "store.ledger":
            saver = step["id"].startswith("saver")
            ledger = "checkpoint_migrations" if saver else "store_migrations"
            version = int(step["id"].rsplit(".v", 1)[1])
            assert recorded == {
                "table": ledger,
                "v": version,
                "sql": f"INSERT INTO mission_control_runtime.{ledger} (v) VALUES ({version})",
            }
            assert step["verify_recorded"]
        else:
            assert recorded is None


def test_verifier_expectations_agree_with_the_descriptor() -> None:
    descriptor = _descriptor()
    text = json.dumps(descriptor)
    for index in EXPECTED_INDEXES:
        assert f"c.relname = '{index}'" in text
    final = " ".join(descriptor["final_verify"])
    for table, columns in EXPECTED_COLUMNS.items():
        assert f"'mission_control_runtime.{table}'" in final
        assert f"AND NOT a.attisdropped) = {len(columns)}" in final
    grants = next(step for step in descriptor["steps"] if step["id"] == "post.grants")
    assert (
        "GRANT SELECT, INSERT, UPDATE, DELETE ON mission_control_runtime.checkpoints"
        in (grants["sql"])
    )
    prerequisite = descriptor["steps"][0]
    assert "CREATE SCHEMA mission_control_runtime;" in prerequisite["sql"]
    assert "REVOKE ALL ON SCHEMA mission_control_runtime FROM PUBLIC;" in prerequisite["sql"]
    assert "NOLOGIN" in prerequisite["sql"]
    assert "GRANT CREATE" not in prerequisite["sql"]
