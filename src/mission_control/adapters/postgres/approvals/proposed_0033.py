"""Pointer only: the MP-11 tables are released in common migration 0033 (component 1.2.0).

The DDL that used to live here as a proposal is now section 1 of
``packages/mission-control-db-contract/component/migrations/0033_approvals_coordinator_inbox_lane_describes.sql``
(``approval_correlation``, ``governed_effect_intent``, their guards, forced RLS and grants).
That file is the only copy; schema changes go through a new migration in that package, never
through this module. No DDL is kept here so nothing can drift from the released bytes.

Approval tasks themselves need no new table: they are ``human_task`` rows of kind
``approval:<origin>`` (inline ``mc.approval_task.v1`` packet) answered once through
``human_resolution``. ``operation_intent``/``operation_receipt`` are not reused for governed
effects: they are written only by the atomic operation journal under reducer authority, keyed
to an operation's ``action_ref``, and an intent awaiting a human is not a claimed operation.
"""

from __future__ import annotations

from typing import Final

RELEASED_MIGRATION: Final = (
    "packages/mission-control-db-contract/component/migrations/"
    "0033_approvals_coordinator_inbox_lane_describes.sql"
)
RELEASED_SECTION: Final = "MP-11 native approval correlations and governed effect intents"

__all__ = ["RELEASED_MIGRATION", "RELEASED_SECTION"]
