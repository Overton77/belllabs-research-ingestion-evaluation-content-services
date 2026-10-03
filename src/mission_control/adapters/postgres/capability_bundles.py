"""Immutable, installation catalog-scoped admission of already verified skill bytes.

No automatic admission occurs on worker startup. Operators first stage bytes, then
register the exact reviewed definition pin. Failed registration leaves orphan objects
which cannot be mounted; this adapter never deletes or adopts them automatically.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType

import asyncpg

from mission_control.adapters.capabilities.capability_bundles import (
    BundleError,
    BundleManifest,
    BundleReader,
    safe_storage_namespace,
)
from mission_control.adapters.capabilities.capability_pins import CapabilityPins, PinnedSkill
from mission_control.domain.authoring.canonical import stable_json_dump
from mission_control.domain.authoring.contracts import DefinitionKind

BundleContents = tuple[BundleManifest, tuple[tuple[str, bytes], ...]]


@dataclass(frozen=True)
class AdmittedBundleReader:
    """Run-start snapshot of verified bytes; no external I/O or mutable aliases."""

    _bundles: Mapping[str, BundleContents]

    def load(self, digest: str) -> BundleContents:
        try:
            return self._bundles[digest]
        except KeyError:
            raise BundleError("bundle manifest was not admitted for this deployment") from None


class PostgresCapabilityBundleAdmissions:
    def __init__(self, pool: asyncpg.Pool, *, catalog_scope: str, storage_namespace: str) -> None:
        if not catalog_scope.strip():
            raise BundleError("trusted installation catalog scope is required")
        safe_storage_namespace(storage_namespace)
        self._pool = pool
        self._scope = catalog_scope
        self._namespace = storage_namespace

    @staticmethod
    def _digest(pin: PinnedSkill) -> str:
        if pin.ref.kind != DefinitionKind.SKILL or pin.digest_format != "bytes_v1":
            raise BundleError("remote skill admission requires an exact bytes_v1 skill pin")
        if not pin.source_locator.startswith("capability-bundles://sha256:"):
            raise BundleError("remote admission requires an immutable manifest locator")
        return pin.source_locator.removeprefix("capability-bundles://")

    async def _scope_connection(self, connection: asyncpg.Connection) -> None:
        await connection.execute(
            "SELECT set_config('belllabs.catalog_scope', $1, true)", self._scope
        )

    async def register(self, pin: PinnedSkill, *, reader: BundleReader) -> BundleManifest:
        """Verify uploaded bytes first, then atomically publish immutable scoped identity.

        Callers must supply a server-configured reader for this storage namespace, not
        a request-controlled URL/client. No credential values enter the row or manifest.
        """
        if getattr(reader, "storage_namespace", None) != self._namespace:
            raise BundleError("storage reader is not bound to the admitted namespace")
        digest = self._digest(pin)
        manifest, files = await asyncio.to_thread(reader.load, digest)
        frozen = AdmittedBundleReader(MappingProxyType({digest: (manifest, files)}))
        pin.bundle(store=frozen)  # Checks asset/version, complete bytes and entrypoint.
        async with self._pool.acquire() as connection, connection.transaction():
            await self._scope_connection(connection)
            await connection.execute(
                """INSERT INTO belllabs_control.capability_bundle_admissions
                   (catalog_scope,asset_id,version,definition_digest,manifest_digest,
                    bundle_digest,skill_md_digest,storage_namespace,manifest)
                   VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb)
                   ON CONFLICT (catalog_scope,asset_id,version) DO NOTHING""",
                self._scope,
                pin.ref.logical_id,
                pin.ref.revision,
                pin.ref.digest,
                digest,
                pin.bundle_digest,
                pin.skill_md_digest,
                self._namespace,
                json.dumps(stable_json_dump(manifest), allow_nan=False),
            )
            await self._verify_on(connection, pin)
        return manifest

    async def _verify_on(self, connection: asyncpg.Connection, pin: PinnedSkill) -> BundleManifest:
        digest = self._digest(pin)
        row = await connection.fetchrow(
            """SELECT definition_digest,manifest_digest,bundle_digest,skill_md_digest,
                      storage_namespace,manifest
               FROM belllabs_control.capability_bundle_admissions
               WHERE catalog_scope=$1 AND asset_id=$2 AND version=$3""",
            self._scope,
            pin.ref.logical_id,
            pin.ref.revision,
        )
        if row is None:
            raise BundleError("skill version is not admitted in this installation catalog")
        expected = (pin.ref.digest, digest, pin.bundle_digest, pin.skill_md_digest, self._namespace)
        actual = tuple(
            row[name]
            for name in (
                "definition_digest",
                "manifest_digest",
                "bundle_digest",
                "skill_md_digest",
                "storage_namespace",
            )
        )
        if actual != expected:
            raise BundleError("immutable skill version conflicts with deployment pin")
        payload = row["manifest"]
        manifest = (
            BundleManifest.model_validate_json(payload)
            if isinstance(payload, str)
            else BundleManifest.model_validate(payload)
        )
        if (
            manifest.digest != digest
            or manifest.asset_id != pin.ref.logical_id
            or manifest.version != pin.ref.revision
            or manifest.bundle_digest != pin.bundle_digest
        ):
            raise BundleError("admitted skill manifest integrity mismatch")
        return manifest

    async def resolve(self, pin: PinnedSkill, *, reader: BundleReader) -> BundleContents:
        if getattr(reader, "storage_namespace", None) != self._namespace:
            raise BundleError("storage reader is not bound to the admitted namespace")
        # Verify scoped registration before any remote fetch (no existence side channel).
        async with self._pool.acquire() as connection, connection.transaction():
            await self._scope_connection(connection)
            admitted = await self._verify_on(connection, pin)
        manifest, files = await asyncio.to_thread(reader.load, self._digest(pin))
        if manifest != admitted:
            raise BundleError("remote manifest differs from admitted catalog manifest")
        pin.bundle(
            store=AdmittedBundleReader(MappingProxyType({manifest.digest: (manifest, files)}))
        )
        return manifest, files

    async def resolve_pins(
        self, pins: CapabilityPins, *, reader: BundleReader
    ) -> AdmittedBundleReader:
        bundles = {}
        for pin in pins.skills:
            # In remote mode every skill must be admitted. No workspace/local fallback.
            manifest, files = await self.resolve(pin, reader=reader)
            bundles[manifest.digest] = (manifest, files)
        return AdmittedBundleReader(MappingProxyType(bundles))
