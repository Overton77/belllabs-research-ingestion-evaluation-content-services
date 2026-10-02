"""RRM-018: the immutable identity the MongoDB GoalDirected repository compares on a duplicate.

Hermetic counterpart of `tests/integration/mongodb/test_goal_directed_documents_mongodb.py`
(MongoDB-gated). The same Goal Revision re-persisted at a later iteration differs only by its
observation time and, once read back from MongoDB, by container types (a dataclass payload
holds tuples, MongoDB returns lists). Neither is part of the identity; content still is.
"""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import timedelta
from typing import Any

from app.application.orchestration.goal_directed import document_payload
from app.application.orchestration.mongo_goal_directed_repository import _immutable_identity
from app.domain.control_plane.canonical import sha256_digest
from app.domain.orchestration.contracts import GoalRevision
from app.models.goal_directed import GoalRevisionDocument
from tests.fixtures.goal_directed_journaled import SCOPE, goal_revision
from tests.unit.run_control.test_run_control import NOW

RUN = "run-rrm-018"


def _document(revision: GoalRevision, payload: dict[str, Any], **values: Any) -> Any:
    return GoalRevisionDocument.model_construct(
        request_scope=SCOPE,
        run_id=RUN,
        goal_revision_id=revision.revision_id,
        revision=revision.revision,
        envelope_digest=revision.envelope_digest,
        document_digest=sha256_digest(payload),
        payload=payload,
        **values,
    )


def test_observation_time_and_container_types_are_not_identity() -> None:
    revision = goal_revision(RUN)
    payload = document_payload(revision)
    assert any(isinstance(value, tuple) for value in payload.values())
    written = _document(revision, payload, recorded_at=NOW)
    # As MongoDB returns it: arrays are lists; a later iteration observes it at a later time.
    read_back = _document(revision, json.loads(json.dumps(payload)), recorded_at=NOW)
    again = _document(revision, payload, recorded_at=NOW + timedelta(minutes=1))
    assert _immutable_identity(read_back) == _immutable_identity(written)
    assert _immutable_identity(again) == _immutable_identity(read_back)


def test_changed_revision_content_is_a_different_identity() -> None:
    revision = goal_revision(RUN)
    changed = replace(revision, tactical_changes=("tactic:other",))
    original = _document(revision, document_payload(revision), recorded_at=NOW)
    other = _document(changed, document_payload(changed), recorded_at=NOW)
    assert _immutable_identity(other) != _immutable_identity(original)
    # A stored document whose digest drifted from its payload is a different identity too.
    drifted = _document(revision, document_payload(revision), recorded_at=NOW)
    drifted.document_digest = sha256_digest({"other": True})
    assert _immutable_identity(drifted) != _immutable_identity(original)
