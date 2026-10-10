"""Recovery 2026-10-09: connection-scoped Codex events never collide across relaunches.

A JSON-RPC request id restarts with every app-server process, and one turn may compact more
than once; their MP-13 keys carry the connection epoch and event sequence when live, so the
frame store's dedupe never drops a legitimate frame. History replays keep stable keys.
"""

from __future__ import annotations

from mission_control.adapters.codex.frames import observations_for
from mission_control.adapters.codex.transport import InboundEvent


def _event(seq: int, method: str, params: dict[str, object]) -> InboundEvent:
    return InboundEvent(seq=seq, kind="notification", method=method, params=params)


def _key(event: InboundEvent, epoch: str | None) -> str:
    (observation,) = observations_for(event, root_thread_id="thr", counter=None, epoch=epoch)
    return observation.provider_key


def test_a_reused_request_id_after_a_relaunch_is_a_new_frame() -> None:
    resolved = {"threadId": "thr", "requestId": 7}
    first = _key(_event(4, "serverRequest/resolved", resolved), "epoch-1")
    second = _key(_event(4, "serverRequest/resolved", resolved), "epoch-2")
    assert first != second


def test_two_compactions_of_one_turn_are_two_frames() -> None:
    params = {"threadId": "thr", "turnId": "turn-1"}
    assert _key(_event(10, "thread/compacted", params), "e") != _key(
        _event(20, "thread/compacted", params), "e"
    )


def test_deltas_carry_the_epoch_and_history_keys_stay_stable() -> None:
    delta = {"threadId": "thr", "turnId": "turn-1", "itemId": "item-1", "delta": "x"}
    live = _key(_event(3, "item/agentMessage/delta", delta), "epoch-1")
    assert live != _key(_event(3, "item/agentMessage/delta", delta), "epoch-2")
    replayed = _key(_event(3, "item/agentMessage/delta", delta), None)
    assert replayed == _key(_event(3, "item/agentMessage/delta", delta), None)
