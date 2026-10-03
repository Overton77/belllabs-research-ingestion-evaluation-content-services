"""Settings built only from explicit values, for tests that assert on configuration.

`get_settings()` and a bare `Settings()` read the developer `.env` and share an `lru_cache`
with every other test, so an assertion on a default value passes or fails with the machine
and the test order. These helpers skip the `.env` file entirely.
"""

from __future__ import annotations

from typing import Any

from pydantic import SecretStr

from mission_control.bootstrap.settings import Settings

_REQUIRED: dict[str, Any] = {
    "supabase_url": "https://settings.invalid",
    "supabase_publishable_key": SecretStr("publishable"),
    "supabase_secret_key": SecretStr("secret"),
    "openai_api_key": SecretStr("sk-test"),
}


def isolated_settings(**overrides: Any) -> Settings:
    """Settings from the required fields plus `overrides`, never from a `.env` file."""

    return Settings(_env_file=None, **{**_REQUIRED, **overrides})
