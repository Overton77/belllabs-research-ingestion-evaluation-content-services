"""The authoritative Biotech SDL is located by configuration or the BellLabs layout, never
copied into Mission Control, and admitted only when it equals the published reference."""

from __future__ import annotations

from pathlib import Path

import pytest

from biotech_mission_adapters.bootstrap.schema_source import (
    SCHEMA_SOURCE_ENV,
    WORKSPACE_SCHEMA,
    AuthoritativeSchemaMismatch,
    AuthoritativeSchemaUnavailable,
    locate_authoritative_schema,
    published_source_reference,
    read_authoritative_schema,
)


def test_default_follows_the_belllabs_layout(tmp_path: Path) -> None:
    project_root = tmp_path / "platform" / "mission-control"
    project_root.mkdir(parents=True)
    expected = tmp_path.joinpath(*WORKSPACE_SCHEMA.parts)
    expected.parent.mkdir(parents=True)
    expected.write_bytes(b"type A { id: ID! }")

    assert locate_authoritative_schema(project_root=project_root, environ={}) == expected


def test_explicit_configuration_wins_and_never_falls_back(tmp_path: Path) -> None:
    project_root = tmp_path / "platform" / "mission-control"
    layout = tmp_path.joinpath(*WORKSPACE_SCHEMA.parts)
    layout.parent.mkdir(parents=True)
    layout.write_bytes(b"type A { id: ID! }")
    configured = tmp_path / "configured.graphql"
    configured.write_bytes(b"type B { id: ID! }")

    environ = {SCHEMA_SOURCE_ENV: str(configured)}
    assert locate_authoritative_schema(project_root=project_root, environ=environ) == configured
    missing = {SCHEMA_SOURCE_ENV: str(tmp_path / "absent.graphql")}
    with pytest.raises(AuthoritativeSchemaUnavailable, match=SCHEMA_SOURCE_ENV):
        locate_authoritative_schema(project_root=project_root, environ=missing)


def test_absent_layout_names_the_configuration(tmp_path: Path) -> None:
    with pytest.raises(AuthoritativeSchemaUnavailable, match=SCHEMA_SOURCE_ENV):
        locate_authoritative_schema(project_root=tmp_path / "platform" / "mc", environ={})


def test_bytes_other_than_the_published_reference_fail_closed(tmp_path: Path) -> None:
    edited = tmp_path / "current_biotech_schema.graphql"
    edited.write_bytes(b"type Edited { id: ID! }")
    reference = published_source_reference()

    with pytest.raises(AuthoritativeSchemaMismatch, match=str(reference["sha256"])):
        read_authoritative_schema(edited)
