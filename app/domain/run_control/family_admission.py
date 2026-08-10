from __future__ import annotations

import json
from typing import Literal, Self

from pydantic import AwareDatetime, Field, model_validator

from app.domain.control_plane.canonical import canonical_json
from app.domain.run_control.contracts import DIGEST_PATTERN, CommandResult, CommandStatus, Contract

MAX_FAMILY_MUTATION_BYTES = 65_536
SAFE_KIND_PATTERN = r"^[a-z][a-z0-9_.-]{0,127}$"
SAFE_MUTATION_ID_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9._~-]{0,511}$"
OPAQUE_OPERATION_REQUEST_REF_PATTERN = (
    r"^[a-z][a-z0-9_.-]{0,63}:[A-Za-z0-9][A-Za-z0-9._~-]{0,511}$"
)


class FamilyVersionConflict(Exception):
    """The family projection advanced before an atomic admission committed."""


class AuthorityStateConflict(Exception):
    """Budget or effect authority changed without advancing the run version."""


class AtomicFamilyMutation(Contract):
    """Family-neutral envelope persisted with one reducer-authorized command."""

    schema_version: Literal["1"] = "1"
    family_kind: str = Field(pattern=SAFE_KIND_PATTERN)
    mutation_kind: str = Field(pattern=SAFE_KIND_PATTERN)
    mutation_id: str = Field(pattern=SAFE_MUTATION_ID_PATTERN)
    request_scope: str = Field(min_length=1, max_length=512)
    run_id: str = Field(min_length=1, max_length=512)
    expected_family_version: int = Field(ge=0)
    exact_operation_request_ref: str = Field(
        pattern=OPAQUE_OPERATION_REQUEST_REF_PATTERN,
        max_length=576,
    )
    decided_at: AwareDatetime

    @model_validator(mode="after")
    def serialized_mutation_is_bounded(self) -> Self:
        database_json = json.dumps(
            self.model_dump(mode="json"),
            sort_keys=True,
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
        if (
            len(canonical_json(self)) > MAX_FAMILY_MUTATION_BYTES
            or len(database_json) > MAX_FAMILY_MUTATION_BYTES
        ):
            raise ValueError(
                f"family mutation exceeds {MAX_FAMILY_MUTATION_BYTES} serialized bytes"
            )
        return self


class FamilyMutationReceipt(Contract):
    family_kind: str
    mutation_kind: str
    mutation_id: str
    mutation_fingerprint: str = Field(pattern=DIGEST_PATTERN)
    family_version: int = Field(ge=1)
    exact_operation_request_ref: str


class FamilyAdmissionReceipt(Contract):
    command_result: CommandResult
    family_mutation_fingerprint: str = Field(pattern=DIGEST_PATTERN)
    family_receipt: FamilyMutationReceipt | None = None

    @model_validator(mode="after")
    def family_receipt_matches_acceptance(self) -> Self:
        accepted = self.command_result.status == CommandStatus.ACCEPTED
        if accepted != (self.family_receipt is not None):
            raise ValueError("family receipt must be present exactly for an accepted command")
        return self
