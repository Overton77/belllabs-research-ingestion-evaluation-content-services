"""Digest/revision pins of the search and browser capabilities a deployment mounts (RRM-009).

The pin file (`infra/capability-pins/research-capabilities.json`) is the deployment's frozen
statement of which MCP servers, Agent Skill bundles and tools its workers may materialize:
every entry names the exact definition reference, the module or bundle digest and, for an
MCP server, the exact tool filter with each tool's input-schema digest. Workers refuse any
binding whose digests differ (REQ-CP-DA-005), and `scripts/pin_research_capabilities.py`
is the only writer: it reads the reviewed modules and bundles from the workspace and records
what it observed. Credentials appear only as environment reference names.

Locators are workspace-relative (`workspace://...`, the parent of `PROJECT_ROOT`), so the
same pin file is valid on every host that lays the workspace out the same way.

Resource-root contract (`workspace_path`). A locator is lexically safe (no `..`, absolute,
drive, device or ADS spelling) and must resolve, after every link is followed, inside its
containment root:

- for a locator under a declared resource root (`WORKSPACE_RESOURCE_ROOTS`: `.agents`,
  `.tools`), the real path of `<workspace>/<root>`. That top-level entry may be a directory
  link (the BellLabs layout keeps `platform/.agents` and `platform/.tools` as junctions to
  the Biotech-owned directories), but its target must keep the resource root's name, and
  nothing beneath it may resolve outside that target;
- for any other locator, the real path of the workspace itself, so every other link that
  leaves the workspace (including undeclared top-level links) is refused.

The function returns the verified real path, so bundle readers that refuse links in a path
(`directory_files`) read exactly what was checked.
"""

from __future__ import annotations

import json
from hashlib import sha256
from pathlib import Path, PurePosixPath
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from mission_control.adapters.capabilities.capability_bundles import (
    BundleError,
    BundleReader,
    DirectoryBundleStore,
    bundle_digest,
    directory_files,
    safe_relative_path,
)
from mission_control.adapters.deep_agents.materializer import ResolvedSkillBundle
from mission_control.bootstrap.settings import PROJECT_ROOT, Settings
from mission_control.domain.authoring.canonical import sha256_digest
from mission_control.domain.authoring.contracts import DefinitionKind, ExactDefinitionRef, SecretRef
from mission_control.domain.execution.contracts import (
    DeepAgentMCPServerComponent,
    DeepAgentMCPToolComponent,
    DeepAgentSkillComponent,
    DeepAgentToolComponent,
)
from mission_control.domain.execution.errors import DeepAgentRuntimeDrift

DIGEST_PATTERN = r"^sha256:[0-9a-f]{64}$"
WORKSPACE_SCHEME = "workspace://"
PIN_SCHEMA_VERSION: Literal["belllabs.capability-pins.v1"] = "belllabs.capability-pins.v1"
MAX_SKILL_BUNDLE_FILES = 64
MAX_SKILL_FILE_BYTES = 256_000
# Top-level workspace entries that may be relocated behind a directory link (see module
# docstring). Adding a name here widens what a pin file can address; it is a reviewed change.
WORKSPACE_RESOURCE_ROOTS: frozenset[str] = frozenset({".agents", ".tools"})


class CapabilityPinError(ValueError):
    """The pin file is malformed, or the workspace artifact differs from its pin."""


class PinnedExactRef(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    kind: DefinitionKind
    logical_id: str = Field(min_length=1)
    revision: int = Field(ge=1)
    digest: str = Field(pattern=DIGEST_PATTERN)

    def as_ref(self) -> ExactDefinitionRef:
        return ExactDefinitionRef(
            kind=self.kind, logical_id=self.logical_id, revision=self.revision, digest=self.digest
        )


class PinnedMCPTool(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    tool_name: str = Field(min_length=1)
    schema_digest: str = Field(pattern=DIGEST_PATTERN)


class PinnedMCPServer(BaseModel):
    """One stdio MCP server run as a worker-side subprocess with a sanitized environment."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    server_id: str = Field(min_length=1)
    server_name: str = Field(pattern=r"^[a-z][a-z0-9_-]*$")
    ref: PinnedExactRef
    package_name: str = Field(min_length=1)
    package_version: str = Field(min_length=1)
    module_locator: str = Field(pattern=r"^workspace://.+")
    module_digest: str = Field(pattern=DIGEST_PATTERN)
    schema_digest: str = Field(pattern=DIGEST_PATTERN)
    credential_env: str | None = Field(default=None, pattern=r"^[A-Z][A-Z0-9_]*$")
    tools: tuple[PinnedMCPTool, ...] = Field(min_length=1)

    def module_path(self) -> Path:
        return workspace_path(self.module_locator)

    def verify_module(self) -> Path:
        module = self.module_path()
        try:
            content = module.read_bytes()
        except OSError as error:
            raise CapabilityPinError(
                f"pinned MCP module is unavailable: {self.module_locator}"
            ) from error
        if "sha256:" + sha256(content).hexdigest() != self.module_digest:
            raise CapabilityPinError(
                f"pinned MCP module {self.package_name} differs from its reviewed digest"
            )
        package_path = module.parent.parent / "package.json"
        try:
            package = json.loads(package_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise CapabilityPinError(
                f"pinned MCP package metadata is unavailable: {self.package_name}"
            ) from error
        if (
            package.get("name") != self.package_name
            or package.get("version") != self.package_version
        ):
            raise CapabilityPinError(
                f"{self.package_name} must be pinned to {self.package_version}"
            )
        return module

    def component(
        self, *, node_executable: Path, attachment_target: str = "agent.main"
    ) -> DeepAgentMCPServerComponent:
        """The exact Deep Agent MCP component: stdio through the pinned node module."""

        module = self.verify_module()
        return DeepAgentMCPServerComponent(
            ref=self.ref.as_ref(),
            server_name=self.server_name,
            transport="stdio",
            command=str(node_executable.resolve(strict=True)),
            arguments=(str(module),),
            credential_refs=(
                (SecretRef(provider="environment", key=self.credential_env),)
                if self.credential_env is not None
                else ()
            ),
            tools=tuple(
                DeepAgentMCPToolComponent(
                    tool_name=tool.tool_name, schema_digest=tool.schema_digest
                )
                for tool in self.tools
            ),
            schema_digest=self.schema_digest,
            attachment_target=attachment_target,
        )


class PinnedSkill(BaseModel):
    """One Agent Skill bundle read from the workspace `.agents/skills/<name>` directory."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    ref: PinnedExactRef
    skill_name: str = Field(pattern=r"^[a-z0-9][a-z0-9-]*$")
    source_locator: str = Field(pattern=r"^(workspace|capability-bundles)://.+")
    digest_format: Literal["legacy_text_v1", "bytes_v1"] = "legacy_text_v1"
    bundle_digest: str = Field(pattern=DIGEST_PATTERN)
    skill_md_digest: str = Field(pattern=DIGEST_PATTERN)
    mount_root: str = Field(pattern=r"^/.*[^/]$")

    @property
    def manifest_digest(self) -> str:
        return self.bundle_digest

    def bundle(self, *, store: BundleReader | None = None) -> ResolvedSkillBundle:
        if self.source_locator.startswith("capability-bundles://"):
            selected_store = store or DirectoryBundleStore(
                PROJECT_ROOT / ".runtime" / "capability-bundles"
            )
            try:
                manifest, files = selected_store.load(
                    self.source_locator.removeprefix("capability-bundles://")
                )
                if (manifest.asset_id, manifest.version) != (
                    self.ref.logical_id,
                    self.ref.revision,
                ):
                    raise BundleError("pinned skill version differs from stored manifest")
                if self.digest_format != "bytes_v1":
                    raise BundleError("stored bundles require bytes_v1 digests")
                bundle = ResolvedSkillBundle(manifest.bundle_digest, files, "bytes_v1")
            except (ValueError, OSError) as error:
                raise CapabilityPinError(str(error)) from error
        else:
            bundle = read_skill_bundle(
                workspace_path(self.source_locator), digest_format=self.digest_format
            )
        if bundle.bundle_digest != self.bundle_digest:
            raise CapabilityPinError(
                f"Skill bundle {self.skill_name} differs from its pinned bundle digest"
            )
        try:
            bundle.verify(skill_md_digest=self.skill_md_digest)
        except DeepAgentRuntimeDrift as error:
            raise CapabilityPinError(str(error)) from error
        return bundle

    def component(self, *, attachment_target: str = "agent.main") -> DeepAgentSkillComponent:
        return DeepAgentSkillComponent(
            ref=self.ref.as_ref(),
            skill_name=self.skill_name,
            bundle_digest=self.bundle_digest,
            skill_md_digest=self.skill_md_digest,
            mount_root=self.mount_root,
            attachment_target=attachment_target,
        )


class PinnedTool(BaseModel):
    """One exact host tool (today: the agent-browser page tool over the pinned CLI)."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    ref: PinnedExactRef
    tool_name: str = Field(min_length=1)
    kind: Literal["agent_browser_page"]
    schema_digest: str = Field(pattern=DIGEST_PATTERN)
    entrypoint_locator: str = Field(pattern=r"^workspace://.+")
    entrypoint_digest: str = Field(pattern=DIGEST_PATTERN)
    package_version: str = Field(min_length=1)

    def verify_entrypoint(self) -> Path:
        entrypoint = workspace_path(self.entrypoint_locator)
        try:
            content = entrypoint.read_bytes()
        except OSError as error:
            raise CapabilityPinError(
                f"pinned tool entrypoint is unavailable: {self.entrypoint_locator}"
            ) from error
        if "sha256:" + sha256(content).hexdigest() != self.entrypoint_digest:
            raise CapabilityPinError(
                f"pinned tool entrypoint {self.tool_name} differs from its reviewed digest"
            )
        return entrypoint

    def component(self, *, attachment_target: str = "agent.main") -> DeepAgentToolComponent:
        return DeepAgentToolComponent(
            ref=self.ref.as_ref(),
            tool_name=self.tool_name,
            schema_digest=self.schema_digest,
            attachment_target=attachment_target,
        )


class PinnedModel(BaseModel):
    """An exact model definition the deployment serves through its provider factory."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    ref: PinnedExactRef
    provider: Literal["openai"]


class PinnedSandbox(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    ref: PinnedExactRef
    backend: Literal["state", "docker", "langsmith"]


class PinnedPersistence(BaseModel):
    """A checkpointer or store definition served by the deployment's persistent saver/store."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    ref: PinnedExactRef


class CapabilityPins(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["belllabs.capability-pins.v1"] = PIN_SCHEMA_VERSION
    mcp_servers: tuple[PinnedMCPServer, ...] = ()
    skills: tuple[PinnedSkill, ...] = ()
    tools: tuple[PinnedTool, ...] = ()
    models: tuple[PinnedModel, ...] = ()
    sandboxes: tuple[PinnedSandbox, ...] = ()
    checkpointers: tuple[PinnedPersistence, ...] = ()
    stores: tuple[PinnedPersistence, ...] = ()

    @classmethod
    def load(cls, path: Path) -> CapabilityPins:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise CapabilityPinError(f"capability pins are unavailable: {path}") from error
        pins = cls.model_validate(payload)
        identities = [item.server_id for item in pins.mcp_servers]
        if len(identities) != len(set(identities)):
            raise CapabilityPinError("capability pins name an MCP server twice")
        return pins

    @classmethod
    def empty(cls) -> CapabilityPins:
        return cls()

    @classmethod
    def from_settings(cls, settings: Settings, *, required: bool = True) -> CapabilityPins:
        """The deployment's pins. The production compositions (launch-enabled workers, the
        Temporal-enabled API) require the file: a missing pin file fails closed instead of
        silently mounting nothing (RRM-009 review). `required=False` is for tooling only."""

        path = settings.capability_pins_path
        if path.exists():
            return cls.load(path)
        if required:
            raise CapabilityPinError(f"capability pins are required but absent: {path}")
        return cls.empty()

    def mcp_server(self, server_id: str) -> PinnedMCPServer:
        for server in self.mcp_servers:
            if server.server_id == server_id:
                return server
        raise CapabilityPinError(f"MCP server {server_id} is not pinned")

    def skill(self, skill_name: str) -> PinnedSkill:
        for skill in self.skills:
            if skill.skill_name == skill_name:
                return skill
        raise CapabilityPinError(f"Skill {skill_name} is not pinned")

    def tool(self, tool_name: str) -> PinnedTool:
        for tool in self.tools:
            if tool.tool_name == tool_name:
                return tool
        raise CapabilityPinError(f"tool {tool_name} is not pinned")

    def mcp_schema_digests(self) -> dict[str, str]:
        return {server.server_id: server.schema_digest for server in self.mcp_servers}

    def asset_manifest_digests(self) -> dict[str, str]:
        return {
            f"{skill.ref.kind.value}:{skill.ref.logical_id}:{skill.ref.revision}": (
                skill.manifest_digest
            )
            for skill in self.skills
        }

    def disclosure(self) -> dict[str, object]:
        """A secret-free record of the mounted capabilities for sanitized lineage."""

        return {
            "schema_version": self.schema_version,
            "mcp_servers": [
                {
                    "server_id": server.server_id,
                    "ref": server.ref.model_dump(mode="json"),
                    "package": f"{server.package_name}@{server.package_version}",
                    "module_locator": server.module_locator,
                    "module_digest": server.module_digest,
                    "schema_digest": server.schema_digest,
                    "credential_ref": (
                        f"environment:{server.credential_env}"
                        if server.credential_env is not None
                        else None
                    ),
                    "tools": [tool.model_dump(mode="json") for tool in server.tools],
                    "egress": "worker_mediated_stdio_subprocess",
                }
                for server in self.mcp_servers
            ],
            "skills": [skill.model_dump(mode="json") for skill in self.skills],
            "tools": [tool.model_dump(mode="json") for tool in self.tools],
        }


def workspace_root() -> Path:
    return PROJECT_ROOT.parent


def workspace_path(locator: str, *, root: Path | None = None) -> Path:
    """The verified real path of a `workspace://` locator under `root` (default: the
    checkout's parent). Enforces the resource-root contract in the module docstring."""

    if not locator.startswith(WORKSPACE_SCHEME):
        raise CapabilityPinError(f"locator is not workspace-relative: {locator}")
    raw = locator.removeprefix(WORKSPACE_SCHEME)
    base = workspace_root() if root is None else root
    try:
        parts = PurePosixPath(safe_relative_path(raw)).parts
        workspace = base.resolve()
        containment = workspace
        if parts[0] in WORKSPACE_RESOURCE_ROOTS:
            containment = (base / parts[0]).resolve()
            relocated = not containment.is_relative_to(workspace)
            if relocated and containment.name != parts[0]:
                raise BundleError("resource root links to a directory of another name")
        result = base.joinpath(*parts).resolve()
        if not result.is_relative_to(containment):
            raise BundleError("locator escapes its containment root through a link")
    except (BundleError, OSError, RuntimeError) as error:
        raise CapabilityPinError(f"locator escapes or aliases the workspace: {locator}") from error
    return result


def read_skill_bundle(
    directory: Path, *, digest_format: Literal["legacy_text_v1", "bytes_v1"] = "legacy_text_v1"
) -> ResolvedSkillBundle:
    """Read complete directory bytes; legacy pins keep their original digest algorithm."""
    try:
        files = directory_files(directory)
        if digest_format == "bytes_v1":
            digest = bundle_digest(files)
        else:
            manifest = [
                {
                    "path": relative,
                    "digest": sha256_digest(content.decode("utf-8")),
                    "size_bytes": len(content),
                }
                for relative, content in files
            ]
            digest = sha256_digest(manifest)
    except (ValueError, OSError) as error:
        raise CapabilityPinError(str(error)) from error
    return ResolvedSkillBundle(bundle_digest=digest, files=files, digest_format=digest_format)
