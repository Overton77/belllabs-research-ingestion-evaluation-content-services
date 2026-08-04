from __future__ import annotations

from datetime import datetime
from typing import Any

from beanie import Document
from pymongo import ASCENDING, DESCENDING, IndexModel


class ExternalDiscoveryEvidenceDocument(Document):
    evidence_id: str
    source: str
    source_version: str
    query: str
    retrieved_at: datetime
    raw_response_digest: str
    raw_response_size_bytes: int
    record_digest: str
    payload: dict[str, Any]
    recorded_at: datetime

    class Settings:
        name = "external_discovery_evidence"
        indexes = [
            IndexModel([("evidence_id", ASCENDING)], unique=True),
            IndexModel([("record_digest", ASCENDING)], unique=True),
            IndexModel(
                [
                    ("source", ASCENDING),
                    ("query", ASCENDING),
                    ("retrieved_at", DESCENDING),
                ]
            ),
            IndexModel([("raw_response_digest", ASCENDING)]),
        ]


class ExternalDiscoveryCandidateDocument(Document):
    candidate_record_id: str
    candidate_id: str
    evidence_id: str
    source: str
    upstream_identity: str
    upstream_version: str | None = None
    query: str
    discovered_at: datetime
    raw_response_digest: str
    content_digest: str
    payload: dict[str, Any]
    recorded_at: datetime

    class Settings:
        name = "external_discovery_candidates"
        indexes = [
            IndexModel([("candidate_record_id", ASCENDING)], unique=True),
            IndexModel([("content_digest", ASCENDING)], unique=True),
            IndexModel(
                [
                    ("candidate_id", ASCENDING),
                    ("discovered_at", DESCENDING),
                ]
            ),
            IndexModel([("evidence_id", ASCENDING)]),
            IndexModel(
                [
                    ("source", ASCENDING),
                    ("upstream_identity", ASCENDING),
                    ("upstream_version", ASCENDING),
                ]
            ),
        ]


class ExternalCandidateInspectionWorkspaceDocument(Document):
    workspace_id: str
    candidate_id: str
    candidate_record_id: str
    content_digest: str
    payload: dict[str, Any]
    allocated_at: datetime
    expires_at: datetime

    class Settings:
        name = "external_candidate_inspection_workspaces"
        indexes = [
            IndexModel([("workspace_id", ASCENDING)], unique=True),
            IndexModel([("content_digest", ASCENDING)], unique=True),
            IndexModel(
                [
                    ("candidate_id", ASCENDING),
                    ("allocated_at", DESCENDING),
                ]
            ),
            IndexModel([("expires_at", ASCENDING)]),
        ]


class ExternalCandidateInspectionReportDocument(Document):
    inspection_id: str
    candidate_id: str
    candidate_record_id: str
    workspace_id: str
    status: str
    report_digest: str
    payload: dict[str, Any]
    requested_at: datetime
    completed_at: datetime

    class Settings:
        name = "external_candidate_inspection_reports"
        indexes = [
            IndexModel([("inspection_id", ASCENDING)], unique=True),
            IndexModel([("report_digest", ASCENDING)], unique=True),
            IndexModel(
                [
                    ("candidate_id", ASCENDING),
                    ("completed_at", DESCENDING),
                ]
            ),
            IndexModel([("workspace_id", ASCENDING)], unique=True),
        ]
