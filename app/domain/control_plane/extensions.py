from __future__ import annotations

from collections.abc import Callable

from pydantic import BaseModel, ConfigDict

from app.domain.control_plane.contracts import NamespacedExtension
from app.domain.control_plane.errors import UnsupportedExtension


class ExtensionPayload(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


ExtensionValidator = Callable[[dict[str, object]], BaseModel]


class ExtensionRegistry:
    def __init__(self) -> None:
        self._validators: dict[tuple[str, str, str], ExtensionValidator] = {}

    def register(
        self,
        namespace: str,
        schema_version: str,
        discriminator: str,
        model: type[ExtensionPayload],
    ) -> None:
        key = (namespace, schema_version, discriminator)
        if key in self._validators:
            raise ValueError(f"extension already registered: {key}")
        self._validators[key] = model.model_validate

    def validate(self, extension: NamespacedExtension) -> NamespacedExtension:
        key = (extension.namespace, extension.schema_version, extension.discriminator)
        validator = self._validators.get(key)
        if validator is None:
            raise UnsupportedExtension(f"unregistered executable extension: {key}")
        payload = validator(extension.payload)
        return extension.model_copy(update={"payload": payload.model_dump(mode="python")})

    def validate_all(
        self, extensions: tuple[NamespacedExtension, ...]
    ) -> tuple[NamespacedExtension, ...]:
        return tuple(self.validate(extension) for extension in extensions)
