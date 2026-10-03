"""Compatibility exports for the promoted production schema-agent prompts."""

from biotech_mission_adapters.adapters.infrastructure.schema_agent_prompts import (
    QUERY_PLANNER_INSTRUCTIONS,
    REVIEWER_INSTRUCTIONS,
    SELECTOR_INSTRUCTIONS,
)

__all__ = [
    "QUERY_PLANNER_INSTRUCTIONS",
    "REVIEWER_INSTRUCTIONS",
    "SELECTOR_INSTRUCTIONS",
]
