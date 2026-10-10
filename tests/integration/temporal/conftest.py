"""Psycopg's Windows selector loop for the Temporal integration modules on the PostgreSQL stack.

The root ``tests/conftest.py`` keeps its own module list; modules here that open the
production PostgreSQL stack (LangGraph's Psycopg saver) are listed below. Every other item
falls through to the root hook.
"""

from __future__ import annotations

import asyncio
import sys
from typing import Any

_PSYCOPG_SELECTOR_MODULES = frozenset(
    {
        "test_manifest_launch_production.py",
        "test_mp22_local_profile_start.py",
        "test_manifest_v2_provider_launch.py",
        "test_mp20_workflow_parity.py",
    }
)


def pytest_asyncio_loop_factories(config: Any, item: Any) -> dict[str, Any] | None:
    del config
    if sys.platform == "win32" and item.path.name in _PSYCOPG_SELECTOR_MODULES:
        return {"windows-selector": asyncio.SelectorEventLoop}
    return None
