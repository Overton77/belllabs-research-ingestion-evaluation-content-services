from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

from app.domain.graph_runtime.identities import DIGEST_PATTERN


def validate_common_run_state(state: Mapping[str, Any]) -> None:
    """Shared Agent Server run-identity checks used by non-family graphs."""

    if not state["request_scope"] or not state["belllabs_run_id"]:
        raise ValueError("qualified BellLabs run identity is required")
    if state["execution_epoch"] < 1:
        raise ValueError("execution epoch must be positive")
    if re.fullmatch(DIGEST_PATTERN, state["graph_assembly_digest"]) is None:
        raise ValueError("invalid graph assembly digest")
    if re.fullmatch(DIGEST_PATTERN, state["run_plan_digest"]) is None:
        raise ValueError("invalid RunPlan digest")


__all__ = ["validate_common_run_state"]
