"""Native identity writers: harness_execution, agent_session and session_turn (SPEC-03).

Called by the frame repository inside the frame-append transaction for each newly stored
frame of an identity kind, so identity and evidence never disagree:

- `session_init` (or the first frame of an unseen session) opens the `agent_session`;
- `turn_started` records the open turn in `harness_execution.native_identity` (the
  `session_turn` table is immutable in the common component, so the turn row is written
  once, when the turn closes);
- `turn_ended` writes the immutable `session_turn` row with the started and ended frame
  ids, the usage report folded from the turn's closing `usage` frames, the stop reason and
  the result summary reference;
- `run_result` closes the session and ends the harness execution.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any
from uuid import UUID, uuid5

import asyncpg

from mission_control.adapters.postgres.run_control.canonical import SCOPE
from mission_control.application.frames.lineage import counts_usage_frame, usage_body
from mission_control.domain.frames.body import frame_body_object
from mission_control.domain.frames.contracts import FrameKind, ProviderFrame, native_event_ref
from mission_control.domain.frames.usage import usage_report

_SESSION_NAMESPACE = UUID("a9f3c1d2-5b6e-4f70-8a91-2c3d4e5f6a7b")

_OUTCOMES = {
    "succeeded": "succeeded",
    "success": "succeeded",
    "finished": "succeeded",
    "completed": "succeeded",
    "end_turn": "succeeded",
    "stop": "succeeded",
    "failed": "failed",
    "error": "failed",
    "cancelled": "interrupted",
    "interrupted": "interrupted",
    "expired": "failed",
}


def session_id_for(harness_execution_id: UUID, native_session_ref: str) -> UUID:
    return uuid5(_SESSION_NAMESPACE, f"{harness_execution_id}|{native_session_ref}")


def turn_id_for(session_id: UUID, turn_no: int) -> UUID:
    return uuid5(_SESSION_NAMESPACE, f"{session_id}|turn|{turn_no}")


def body_object(frame: ProviderFrame) -> dict[str, Any]:
    """The frame body when its excerpt holds the whole canonical JSON object, else {}."""

    return frame_body_object(frame.body_excerpt, frame.body_bytes)


def _identity(value: Any) -> dict[str, Any]:
    if isinstance(value, str):
        value = json.loads(value)
    return dict(value) if isinstance(value, Mapping) else {}


async def apply(
    connection: asyncpg.Connection,
    args: tuple[Any, ...],
    execution: asyncpg.Record,
    frame: ProviderFrame,
    *,
    actor_ref: str,
) -> None:
    session_id = await ensure_session(connection, args, execution, frame, actor_ref=actor_ref)
    if frame.subordinate_ref is not None:
        # A provider subagent's turns and result never open, close or end the parent's.
        return
    if frame.kind == FrameKind.TURN_STARTED:
        await _open_turn(connection, args, frame)
    elif frame.kind == FrameKind.TURN_ENDED:
        await _close_turn(connection, args, session_id, frame, actor_ref=actor_ref)
    elif frame.kind == FrameKind.RUN_RESULT:
        await connection.execute(
            f"""
            UPDATE mission_control.agent_session
            SET state = 'closed', ended_at = $5, version = version + 1, updated_at = $5
            WHERE {SCOPE} AND session_id = $4 AND state <> 'closed'
            """,
            *args,
            session_id,
            frame.observed_at,
        )
        await connection.execute(
            f"""
            UPDATE mission_control.harness_execution
            SET lifecycle = 'ended', version = version + 1, updated_at = clock_timestamp()
            WHERE {SCOPE} AND harness_execution_id = $4
            """,
            *args,
            frame.harness_execution_id,
        )


async def ensure_session(
    connection: asyncpg.Connection,
    args: tuple[Any, ...],
    execution: asyncpg.Record,
    frame: ProviderFrame,
    *,
    actor_ref: str,
) -> UUID:
    """Open (idempotently) the session a frame belongs to and return its id.

    `agent_session.native_session_ref` is unique per tenant scope: a lane that resumes the
    same native session under a new harness execution reuses the recorded session.
    """

    session_id = session_id_for(frame.harness_execution_id, frame.native_session_ref)
    await connection.execute(
        """
        INSERT INTO mission_control.agent_session (
            installation_id, application_id, tenant_id, session_id, attempt_id,
            harness_execution_id, native_session_ref, state, checkpoint_namespace,
            checkpoint_ref, predecessor_session_id, version, updated_at, created_at,
            created_by_actor_ref
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, 'open', NULL, NULL, NULL, 1, $8, $8, $9)
        ON CONFLICT (installation_id, application_id, tenant_id, native_session_ref) DO NOTHING
        """,
        *args,
        session_id,
        execution["attempt_id"],
        frame.harness_execution_id,
        frame.native_session_ref,
        frame.observed_at,
        actor_ref,
    )
    recorded = await connection.fetchval(
        f"""
        SELECT session_id FROM mission_control.agent_session
        WHERE {SCOPE} AND native_session_ref = $4
        """,
        *args,
        frame.native_session_ref,
    )
    assert isinstance(recorded, UUID)
    if frame.kind == FrameKind.SESSION_INIT:
        await connection.execute(
            f"""
            UPDATE mission_control.harness_execution
            SET native_identity = COALESCE(native_identity, '{{}}'::jsonb) || $5::jsonb,
                version = version + 1, updated_at = clock_timestamp()
            WHERE {SCOPE} AND harness_execution_id = $4
            """,
            *args,
            frame.harness_execution_id,
            json.dumps(
                {
                    "native_session_ref": frame.native_session_ref,
                    "session_init_frame_id": str(frame.frame_id),
                }
            ),
        )
    return recorded


async def _open_turn(
    connection: asyncpg.Connection, args: tuple[Any, ...], frame: ProviderFrame
) -> None:
    current = _identity(
        await connection.fetchval(
            f"""
            SELECT native_identity FROM mission_control.harness_execution
            WHERE {SCOPE} AND harness_execution_id = $4
            """,
            *args,
            frame.harness_execution_id,
        )
    )
    refs = list(current.get("native_turn_refs") or [])
    if frame.native_turn_ref and frame.native_turn_ref not in refs:
        refs.append(frame.native_turn_ref)
    current.update(
        {
            "native_turn_refs": refs,
            "open_turn": {
                "native_turn_ref": frame.native_turn_ref,
                "started_frame_id": str(frame.frame_id),
                "started_ordinal": frame.arrival_ordinal,
                "generation": frame.generation,
            },
        }
    )
    await connection.execute(
        f"""
        UPDATE mission_control.harness_execution
        SET native_identity = $5::jsonb, lifecycle = 'live', version = version + 1,
            updated_at = clock_timestamp()
        WHERE {SCOPE} AND harness_execution_id = $4
        """,
        *args,
        frame.harness_execution_id,
        json.dumps(current, sort_keys=True),
    )


async def _close_turn(
    connection: asyncpg.Connection,
    args: tuple[Any, ...],
    session_id: UUID,
    frame: ProviderFrame,
    *,
    actor_ref: str,
) -> None:
    current = _identity(
        await connection.fetchval(
            f"""
            SELECT native_identity FROM mission_control.harness_execution
            WHERE {SCOPE} AND harness_execution_id = $4
            """,
            *args,
            frame.harness_execution_id,
        )
    )
    if frame.native_turn_ref is not None and await connection.fetchval(
        f"""
        SELECT 1 FROM mission_control.session_turn
        WHERE {SCOPE} AND session_id = $4 AND native_turn_ref = $5 AND execution_generation = $6
        """,
        *args,
        session_id,
        frame.native_turn_ref,
        frame.generation,
    ):
        return
    opened = current.get("open_turn") if isinstance(current.get("open_turn"), dict) else None
    if opened is not None and int(opened.get("generation", frame.generation)) != frame.generation:
        opened = None
    started_ordinal = int(opened["started_ordinal"]) if opened else 0
    usage_rows = await connection.fetch(
        f"""
        SELECT frame_id, body_excerpt, body_bytes, subordinate_ref
        FROM mission_control.provider_frame
        WHERE {SCOPE} AND harness_execution_id = $4 AND generation = $5 AND kind = 'usage'
          AND arrival_ordinal > $6 AND arrival_ordinal < $7
        ORDER BY arrival_ordinal
        """,
        *args,
        frame.harness_execution_id,
        frame.generation,
        started_ordinal,
        frame.arrival_ordinal,
    )
    lane = frame.lane_profile
    usage_bodies: list[tuple[str, Mapping[str, Any]]] = []
    for row in usage_rows:
        if not counts_usage_frame(lane, row["subordinate_ref"]):
            continue
        text = bytes(row["body_excerpt"]).decode("utf-8", errors="replace")
        try:
            parsed = json.loads(text) if len(text.encode("utf-8")) >= row["body_bytes"] else {}
        except ValueError:
            parsed = {}
        usage_bodies.append(
            (str(row["frame_id"]), usage_body(lane, parsed if isinstance(parsed, dict) else {}))
        )
    body = body_object(frame)
    if not usage_bodies and body:
        usage_bodies.append((str(frame.frame_id), usage_body(lane, body)))
    report = usage_report(frame.lane_profile, usage_bodies)
    turn_no = int(
        await connection.fetchval(
            f"""
            SELECT COALESCE(max(turn_no), 0) + 1 FROM mission_control.session_turn
            WHERE {SCOPE} AND session_id = $4
            """,
            *args,
            session_id,
        )
    )
    started_frame_id = UUID(str(opened["started_frame_id"])) if opened else None
    outcome_key = str(body.get("outcome") or body.get("status") or body.get("stop_reason") or "")
    stop_reason = body.get("stop_reason")
    summary_ref = body.get("result_summary_ref")
    await connection.execute(
        """
        INSERT INTO mission_control.session_turn (
            installation_id, application_id, tenant_id, turn_id, session_id, turn_no,
            native_turn_ref, execution_generation, observation_refs, usage_refs,
            execution_outcome, created_at, created_by_actor_ref, started_frame_id,
            ended_frame_id, usage, result_summary_ref, stop_reason
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16::jsonb,
                $17, $18)
        """,
        *args,
        turn_id_for(session_id, turn_no),
        session_id,
        turn_no,
        frame.native_turn_ref,
        frame.generation,
        [
            *([native_event_ref(started_frame_id)] if started_frame_id else []),
            native_event_ref(frame.frame_id),
        ],
        [native_event_ref(frame_id) for frame_id, _body in usage_bodies],
        _OUTCOMES.get(outcome_key.lower(), "unknown") if outcome_key else "unknown",
        frame.observed_at,
        actor_ref,
        started_frame_id,
        frame.frame_id,
        report.model_dump_json(),
        str(summary_ref) if isinstance(summary_ref, str) and summary_ref else None,
        str(stop_reason) if isinstance(stop_reason, str) and stop_reason else None,
    )
    current.pop("open_turn", None)
    current["turns_closed"] = int(current.get("turns_closed") or 0) + 1
    await connection.execute(
        f"""
        UPDATE mission_control.harness_execution
        SET native_identity = $5::jsonb, version = version + 1, updated_at = clock_timestamp()
        WHERE {SCOPE} AND harness_execution_id = $4
        """,
        *args,
        frame.harness_execution_id,
        json.dumps(current, sort_keys=True),
    )
