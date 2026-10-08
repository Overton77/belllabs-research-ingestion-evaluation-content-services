"""The bounded run-list filter grammar and its Temporal Visibility translation (SPEC-03, C4).

``missionctl run list --query "lane='deep_agents' AND phase='executing'"`` and
``GET /v1/applications/{app}/runs?query=`` accept exactly::

    query  := clause ( AND clause )*
    clause := key '=' 'value'            (lane, phase, mission_id, forked_from, status)
            | started_after ( '=' | '>' ) 'ISO-8601 instant'

Keys map to the FT-G7 typed Search Attributes (``mc_lane``, ``mc_phase``,
``mc_mission_id``, ``ForkedFromRunId``) and to Temporal's ``ExecutionStatus`` and
``StartTime``; anything else is rejected with :class:`RunQueryInvalid` (HTTP 422). The
translated filter is always bound to the caller: the installation workflow id prefix
(``mc/<installation>/<application>/``, shared by every root, family and operation of a
mission run) and the scope hash of the request scope, so one application or tenant can
never list another's runs. No ``mc_application_id`` attribute exists (the namespace's
Keyword slots are full, FT-G7); the prefix and the scope hash carry that isolation.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Final

from mission_control.contracts.identities import parse_request_scope
from mission_control.domain.programs.search_attributes import (
    FORKED_FROM_RUN_ID,
    MC_LANE,
    MC_LANES,
    MC_MISSION_ID,
    MC_PHASE,
    MC_PHASES,
    RUN_ID,
    SCOPE_HASH,
    WORKFLOW_KIND,
    search_attribute_scope_hash,
)

RUN_QUERY_KEYS: Final = ("lane", "phase", "mission_id", "forked_from", "status", "started_after")
MAX_QUERY_LENGTH: Final = 1_024
MAX_CLAUSES: Final = 12
MAX_ROOT_LOOKUP: Final = 500

# Temporal ExecutionStatus names; `paused` is accepted by the server's list filter although
# the Python SDK enum has no PAUSED member (FT-C4 decodes statuses defensively).
STATUS_VALUES: Final = {
    "running": "Running",
    "completed": "Completed",
    "failed": "Failed",
    "canceled": "Canceled",
    "cancelled": "Canceled",
    "terminated": "Terminated",
    "continued_as_new": "ContinuedAsNew",
    "timed_out": "TimedOut",
    "paused": "Paused",
}
_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,255}$")
_CLAUSE = re.compile(
    r"^\s*(?P<key>[a-z_]+)\s*(?P<op>=|>)\s*'(?P<value>[^'\\\n]*)'\s*$",
)
_AND = re.compile(r"\s+AND\s+", re.IGNORECASE)


class RunQueryInvalid(ValueError):
    """A run-list filter outside the bounded grammar (HTTP 422 ``invalid_run_query``)."""

    code = "invalid_run_query"


@dataclass(frozen=True)
class RunQueryClause:
    key: str
    value: str


@dataclass(frozen=True)
class RunQuery:
    clauses: tuple[RunQueryClause, ...] = ()

    def values(self, key: str) -> tuple[str, ...]:
        return tuple(clause.value for clause in self.clauses if clause.key == key)


def parse_run_query(text: str | None) -> RunQuery:
    """Parse and validate a filter; an empty filter lists every visible run."""

    if text is None or not text.strip():
        return RunQuery()
    if len(text) > MAX_QUERY_LENGTH:
        raise RunQueryInvalid("run query is too long")
    parts = _AND.split(text.strip())
    if len(parts) > MAX_CLAUSES:
        raise RunQueryInvalid("run query has too many clauses")
    clauses: list[RunQueryClause] = []
    for part in parts:
        match = _CLAUSE.fullmatch(part)
        if match is None:
            raise RunQueryInvalid(f"unsupported clause: {part.strip()[:80]}")
        key, op, value = match.group("key"), match.group("op"), match.group("value")
        if key not in RUN_QUERY_KEYS:
            raise RunQueryInvalid(f"unsupported key: {key}")
        if op == ">" and key != "started_after":
            raise RunQueryInvalid(f"{key} accepts only '='")
        clauses.append(RunQueryClause(key, _validated(key, value)))
    return RunQuery(tuple(clauses))


def _validated(key: str, value: str) -> str:
    if key == "lane":
        if value not in MC_LANES:
            raise RunQueryInvalid(f"unknown lane: {value}")
        return value
    if key == "phase":
        if value not in MC_PHASES:
            raise RunQueryInvalid(f"unknown phase: {value}")
        return value
    if key == "status":
        if value.lower() not in STATUS_VALUES:
            raise RunQueryInvalid(f"unknown status: {value}")
        return value.lower()
    if key == "started_after":
        try:
            instant = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as error:
            raise RunQueryInvalid("started_after must be an ISO-8601 instant") from error
        if instant.tzinfo is None:
            raise RunQueryInvalid("started_after must carry a timezone")
        return instant.astimezone(UTC).isoformat().replace("+00:00", "Z")
    if not _IDENTIFIER.fullmatch(value):
        raise RunQueryInvalid(f"{key} must be an identifier")
    return value


def installation_prefix(request_scope: str) -> str:
    """``mc/<installation>/<application>/``: every mission workflow id of the application."""

    scope = parse_request_scope(request_scope)
    return f"mc/{scope.installation_id}/{scope.application_id}/"


def visibility_filter(query: RunQuery, request_scope: str) -> str:
    """The Temporal list filter: scope bindings first, then the caller's clauses."""

    parts = [
        f"WorkflowId STARTS_WITH {_quote(installation_prefix(request_scope))}",
        f"{SCOPE_HASH} = {_quote(search_attribute_scope_hash(request_scope))}",
    ]
    for clause in query.clauses:
        if clause.key == "lane":
            parts.append(f"{MC_LANE} = {_quote(clause.value)}")
        elif clause.key == "phase":
            parts.append(f"{MC_PHASE} = {_quote(clause.value)}")
        elif clause.key == "mission_id":
            parts.append(f"{MC_MISSION_ID} = {_quote(clause.value)}")
        elif clause.key == "forked_from":
            parts.append(f"{FORKED_FROM_RUN_ID} = {_quote(clause.value)}")
        elif clause.key == "status":
            parts.append(f"ExecutionStatus = {_quote(STATUS_VALUES[clause.value])}")
        elif clause.key == "started_after":
            parts.append(f"StartTime > {_quote(clause.value)}")
    return " AND ".join(parts)


def root_filter(run_ids: tuple[str, ...], request_scope: str) -> str:
    """The roots of already-listed runs (their status), under the same scope bindings."""

    if not run_ids or len(run_ids) > MAX_ROOT_LOOKUP:
        raise RunQueryInvalid("root lookup needs 1 to 500 run ids")
    for run_id in run_ids:
        if not _IDENTIFIER.fullmatch(run_id):
            raise RunQueryInvalid("run ids must be identifiers")
    listed = ", ".join(_quote(run_id) for run_id in run_ids)
    return (
        f"{visibility_filter(RunQuery(), request_scope)} AND {WORKFLOW_KIND} = 'root' "
        f"AND {RUN_ID} IN ({listed})"
    )


def _quote(value: str) -> str:
    if "'" in value or "\\" in value or "\n" in value:
        raise RunQueryInvalid("filter values must not contain quotes or escapes")
    return f"'{value}'"
