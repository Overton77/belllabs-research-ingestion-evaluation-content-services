"""Capability Pin: ``<capability_id>@<version>#sha256:<manifest_digest>`` (SPEC-01).

A committed revision never carries a mutable reference, so the parser rejects any pin that
lacks one of the three parts.
"""

from __future__ import annotations

import re

from pydantic import BaseModel, ConfigDict, Field, model_validator

CAPABILITY_ID_PATTERN = r"[a-z0-9][a-z0-9._:-]*"
VERSION_PATTERN = r"[A-Za-z0-9][A-Za-z0-9._+-]*"
_PIN = re.compile(
    rf"^(?P<capability_id>{CAPABILITY_ID_PATTERN})@(?P<version>{VERSION_PATTERN})"
    r"#(?P<digest>sha256:[0-9a-f]{64})$"
)


class CapabilityPinError(ValueError):
    """A pin string is missing a part or has a malformed one."""


class CapabilityPin(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    capability_id: str = Field(pattern=rf"^{CAPABILITY_ID_PATTERN}$")
    version: str = Field(pattern=rf"^{VERSION_PATTERN}$")
    digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")

    @model_validator(mode="before")
    @classmethod
    def accept_pin_string(cls, value: object) -> object:
        if isinstance(value, str):
            return _parts(value)
        return value

    @classmethod
    def parse(cls, value: str) -> CapabilityPin:
        return cls(**_parts(value))

    def render(self) -> str:
        return f"{self.capability_id}@{self.version}#{self.digest}"

    def __str__(self) -> str:
        return self.render()


def _parts(value: str) -> dict[str, str]:
    match = _PIN.fullmatch(value.strip()) if isinstance(value, str) else None
    if match is None:
        raise CapabilityPinError(
            "a Capability Pin must be '<capability_id>@<version>#sha256:<64 hex>'"
        )
    return match.groupdict()


def is_pin(value: str) -> bool:
    return _PIN.fullmatch(value.strip()) is not None
