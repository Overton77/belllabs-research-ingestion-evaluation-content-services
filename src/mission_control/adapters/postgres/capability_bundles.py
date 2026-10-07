"""Immutable, installation catalog-scoped admission of already verified skill bytes.

No automatic admission occurs on worker startup. Operators first stage bytes, then
register the exact reviewed definition pin. Failed registration leaves orphan objects
which cannot be mounted; this adapter never deletes or adopts them automatically.

Admission writes the canonical ``mission_control.asset_version`` (asset
``skill-bundle:<logical_id>``, ``version`` = revision, ``manifest_digest`` = the exact
whole-directory manifest digest) plus an ``admit`` ``asset_decision`` and the support
``capability_bundle_admission`` pin, in one transaction under the catalog scope.
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
from mission_control.adapters.postgres.control_plane.catalog_assets import (
    CATALOG_SERVICE_ACTOR,
    SKILL_BUNDLE_CONTRACT,
    skill_bundle_asset_id,
)
from mission_control.adapters.postgres.scope import apply_catalog_scope, parse_catalog_scope
from mission_control.contracts.identities import uuid7
from mission_control.domain.authoring.canonical import stable_json_dump
from mission_control.domain.authoring.contracts import DefinitionKind

BundleContents = tuple[BundleManifest, tuple[tuple[str, bytes], ...]]
BUNDLE_ADMISSION_POLICY = "mission-control.skill-bundle-admission/1"


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
    def __init__(
        self,
        pool: asyncpg.Pool,
        *,
        catalog_scope: str,
        storage_namespace: str,
        actor_ref: str = CATALOG_SERVICE_ACTOR,
    ) -> None:
        if not catalog_scope.strip():
            raise BundleError("trusted installation catalog scope is required")
        try:
            self._installation_id, self._application_id = parse_catalog_scope(catalog_scope)
        except ValueError as error:
            raise BundleError("trusted installation catalog scope is malformed") from error
        safe_storage_namespace(storage_namespace)
        self._pool = pool
        self._scope = catalog_scope
        self._namespace = storage_namespace
        self._actor = actor_ref

    @staticmethod
    def _digest(pin: PinnedSkill) -> str:
        if pin.ref.kind != DefinitionKind.SKILL or pin.digest_format != "bytes_v1":
            raise BundleError("remote skill admission requires an exact bytes_v1 skill pin")
        if not pin.source_locator.startswith("capability-bundles://sha256:"):
            raise BundleError("remote admission requires an immutable manifest locator")
        return pin.source_locator.removeprefix("capability-bundles://")

    async def _scope_connection(self, connection: asyncpg.Connection) -> None:
        await apply_catalog_scope(connection, self._installation_id, self._application_id)

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
        encoded = json.dumps(stable_json_dump(manifest), allow_nan=False)
        scope = (self._installation_id, self._application_id)
        async with self._pool.acquire() as connection, connection.transaction():
            await self._scope_connection(connection)
            # Exactly one winner per (installation, skill, version); replays verify below.
            await connection.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
                f"skill-bundle:{self._scope}:{pin.ref.logical_id}:{pin.ref.revision}",
            )
            existing = await connection.fetchval(
                """SELECT asset_version_id FROM mission_control.asset_version
                   WHERE installation_id=$1 AND application_id=$2 AND asset_id=$3
                     AND version=$4""",
                *scope,
                skill_bundle_asset_id(pin.ref.logical_id),
                str(pin.ref.revision),
            )
            if existing is None:
                asset_version_id = uuid7()
                await connection.execute(
                    """INSERT INTO mission_control.asset_version
                       (installation_id, application_id, asset_version_id, asset_id, version,
                        kind, contract, manifest_ref, manifest_digest, manifest,
                        required_compatibility, status, version_no, updated_at, created_at,
                        created_by_actor_ref)
                       VALUES ($1,$2,$3,$4,$5,'skill',$6,$7,$8,$9::jsonb,'{}'::text[],
                               'admitted',1,clock_timestamp(),clock_timestamp(),$10)""",
                    *scope,
                    asset_version_id,
                    skill_bundle_asset_id(pin.ref.logical_id),
                    str(pin.ref.revision),
                    SKILL_BUNDLE_CONTRACT,
                    pin.source_locator,
                    digest,
                    encoded,
                    self._actor,
                )
                await connection.execute(
                    """INSERT INTO mission_control.asset_decision
                       (installation_id, application_id, asset_decision_id, asset_version_id,
                        decision, disposition, actor_ref, evidence_refs, policy_ref,
                        decided_at, created_at, created_by_actor_ref)
                       VALUES ($1,$2,$3,$4,'admit','bytes-verified',$5,$6::text[],$7,
                               clock_timestamp(),clock_timestamp(),$5)""",
                    *scope,
                    uuid7(),
                    asset_version_id,
                    self._actor,
                    [pin.source_locator],
                    BUNDLE_ADMISSION_POLICY,
                )
                await connection.execute(
                    """INSERT INTO mission_control.capability_bundle_admission
                       (installation_id, application_id, bundle_admission_id, asset_version_id,
                        skill_logical_id, bundle_version, definition_digest, manifest_digest,
                        bundle_digest, skill_md_digest, storage_namespace, manifest,
                        registered_at, created_at, created_by_actor_ref)
                       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::jsonb,
                               clock_timestamp(),clock_timestamp(),$13)""",
                    *scope,
                    uuid7(),
                    asset_version_id,
                    pin.ref.logical_id,
                    pin.ref.revision,
                    pin.ref.digest,
                    digest,
                    pin.bundle_digest,
                    pin.skill_md_digest,
                    self._namespace,
                    encoded,
                    self._actor,
                )
            await self._verify_on(connection, pin)
        return manifest

    async def _verify_on(self, connection: asyncpg.Connection, pin: PinnedSkill) -> BundleManifest:
        digest = self._digest(pin)
        row = await connection.fetchrow(
            """SELECT b.definition_digest, b.manifest_digest, b.bundle_digest,
                      b.skill_md_digest, b.storage_namespace, b.manifest,
                      a.manifest_digest AS asset_manifest_digest, a.status
               FROM mission_control.capability_bundle_admission AS b
               JOIN mission_control.asset_version AS a
                 ON a.installation_id = b.installation_id
                AND a.application_id = b.application_id
                AND a.asset_version_id = b.asset_version_id
               WHERE b.installation_id=$1 AND b.application_id=$2 AND b.skill_logical_id=$3
                 AND b.bundle_version=$4""",
            self._installation_id,
            self._application_id,
            pin.ref.logical_id,
            pin.ref.revision,
        )
        if row is None or row["status"] != "admitted":
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
        if actual != expected or row["asset_manifest_digest"] != digest:
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
