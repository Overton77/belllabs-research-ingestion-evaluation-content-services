"""Disposable database lifecycle for the independent qualification suite.

Authority comes only from ``MISSION_CONTROL_TEST_ADMIN_DSN`` naming a loopback
PostgreSQL 17 server with pgvector. Absence or a non-loopback host FAILS the test
(``pytest.fail``); it never skips. Databases are ``mcqq_<hex>``; domain roles are
``mcqd_<hex>_*``. Both are created and dropped by exact generated name only.
"""

from __future__ import annotations

import os
import re
import secrets
from urllib.parse import urlsplit, urlunsplit

import asyncpg
import pytest

ADMIN_DSN_ENV = "MISSION_CONTROL_TEST_ADMIN_DSN"
LOOPBACK = {"127.0.0.1", "localhost", "::1"}
GENERATED = re.compile(r"mcq[qd]_[0-9a-f]{10}(_[a-z]+)?")


def require_admin_dsn() -> str:
    value = os.environ.get(ADMIN_DSN_ENV)
    if not value:
        pytest.fail(
            f"{ADMIN_DSN_ENV} is required: two-project qualification is a mandatory "
            "common-schema proof and must fail, not skip, without a disposable server"
        )
    if urlsplit(value).hostname not in LOOPBACK:
        pytest.fail(f"{ADMIN_DSN_ENV} must name a loopback disposable server")
    return value


def database_dsn(server_dsn: str, database: str) -> str:
    parsed = urlsplit(server_dsn)
    host = "127.0.0.1" if parsed.hostname == "localhost" else parsed.hostname
    credentials = parsed.netloc.rsplit("@", 1)[0] + "@" if "@" in parsed.netloc else ""
    return urlunsplit(
        parsed._replace(netloc=f"{credentials}{host}:{parsed.port or 5432}", path="/" + database)
    )


def new_suffix() -> str:
    return secrets.token_hex(5)


async def server_major(server_dsn: str) -> int:
    connection = await asyncpg.connect(server_dsn)
    try:
        return int(await connection.fetchval("SHOW server_version_num")) // 10000
    finally:
        await connection.close()


async def create_database(server_dsn: str, suffix: str) -> str:
    name = f"mcqq_{suffix}"
    if not GENERATED.fullmatch(name):
        raise ValueError("unexpected generated database name")
    admin = await asyncpg.connect(server_dsn)
    try:
        await admin.execute(f'CREATE DATABASE "{name}"')
    finally:
        await admin.close()
    return name


async def drop_database(server_dsn: str, name: str, roles: tuple[str, ...] = ()) -> None:
    if not GENERATED.fullmatch(name) or any(not GENERATED.fullmatch(role) for role in roles):
        raise ValueError("refusing to drop a non-generated name")
    admin = await asyncpg.connect(server_dsn)
    try:
        await admin.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
        for role in roles:
            await admin.execute(f'DROP ROLE IF EXISTS "{role}"')
    finally:
        await admin.close()
