"""Content custody contract independent of the chosen object-store implementation."""

from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True, slots=True)
class ContentAddress:
    uri: str
    digest: str
    size: int
    version_id: str | None = None


class ContentAddressedPayloadStore(Protocol):
    async def put(self, payload: bytes) -> ContentAddress: ...

    async def retrieve(self, address: ContentAddress) -> bytes: ...
