"""Agent-skill seed definitions (FT-A7, SPEC-01 "Seeds", SPEC-08, ADR-0024).

Imported by ``generate_catalog_seeds.py``, ``scripts/seeds_publish_bundles.py`` and tests.

Skill bundles (``skill_bundle``), the Mission Control policy hook (``hook_script``) and the
``plugin.web-research`` plugin, each a published definition (revision 1) whose bytes live in
the private ``capability-bundles`` bucket under the digest path of an
``mc.capability_bundle_manifest.v1`` (``<application>/<kind>/<id>/<version>/<manifest
sha256>/<path>``). The manifest names its application, so every row is per application: the
same bytes have one manifest digest and one object prefix per application, and the rows ship
in ``mc.app.<app>.agent-skills`` seed bundles (no common bundle can carry them).

Sources (bytes are never edited; third-party bundles are kept verbatim):

- ``skill.agent-browser``: ``vercel-labs/agent-browser`` ``skills/agent-browser`` at the
  commit of tag ``v0.38.2`` (``tests/fixtures/skills/agent-browser``); the thin stub that
  runs ``agent-browser skills get core``.
- ``skill.biomcp``: ``biomcp skill install`` output of ``biomcp-cli`` 0.9.1
  (``tests/fixtures/skills/biomcp``; captured on Linux, the installer refuses Windows).
- ``skill.edgartools``: ``edgar.ai.install_skill()`` output of ``edgartools`` 5.61.1
  (``tests/fixtures/skills/edgartools``), captured on Linux: every file equals the upstream
  ``edgar/ai/skills`` blob at the 5.61.1 commit (on Windows the copy mode rewrites
  ``SKILL.md`` with CRLF line endings, so a Windows capture is not canonical). Its upstream
  ``SKILL.md`` declares ``name: EdgarTools``, which breaks the Agent Skills name rule; the
  bundle stays verbatim
  and the catalog's ``skill_name`` is the directory name ``edgartools`` (recorded in
  ``AGENT_CAPABILITY_SEED_NOTES.md``).
- ``skill.mission-control`` and the five ``skill.mission-control-*`` bundles: ``skills/``
  (LF bytes; per-file digests equal ``make skills-manifest``).
- ``hook.mc-policy-template``: ``scripts/hooks/policy_template`` (FT-A5).

Admission per application (SPEC-01): every skill, the hook and the plugin in both
applications, ``skill.biomcp`` in biotech only and ``skill.edgartools`` in ai-engineer only.
``scripts/seeds_publish_bundles.py`` uploads the bytes through the FT-A2 custody path.
"""

from __future__ import annotations

import importlib.util
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[3]
SEEDS_ROOT = Path(__file__).resolve().parent
AGENT_SKILL_SEED_TIME = datetime(2026, 10, 8, tzinfo=UTC)
SEED_VERSION = "1.0.0"
APPS = ("biotech", "ai-engineer")
APP_KEYS = {app: f"mc.app.{app}.agent-skills" for app in APPS}
KEY_PREFIX = "agent-skill"

SKILL_MD_NAME_RULE = r"^[a-z0-9]([a-z0-9-]{0,62}[a-z0-9])?$"

THIRD_PARTY_SKILLS: tuple[dict[str, Any], ...] = (
    {
        "id": "skill.agent-browser",
        "directory": "tests/fixtures/skills/agent-browser",
        "version": "0.38.2",
        "apps": APPS,
        "provenance": {
            "source": "git",
            "locator": (
                "https://github.com/vercel-labs/agent-browser/tree/"
                "39a74c70d7759d5a6de7a22c04570bb626bbd081/skills/agent-browser"
            ),
            "upstream_identity": "vercel-labs/agent-browser",
            "upstream_version": "0.38.2",
            "commit_digest": "39a74c70d7759d5a6de7a22c04570bb626bbd081",
            "license": "Apache-2.0",
        },
        "tags": ["browser", "automation", "web", "testing"],
    },
    {
        "id": "skill.biomcp",
        "directory": "tests/fixtures/skills/biomcp",
        "version": "0.9.1",
        "apps": ("biotech",),
        "provenance": {
            "source": "local",
            "locator": "pypi:biomcp-cli==0.9.1 (biomcp skill install)",
            "upstream_identity": "genomoncology/biomcp",
            "upstream_version": "0.9.1",
            "license": "MIT",
        },
        "tags": ["biomedical", "genes", "variants", "trials", "pubmed", "literature"],
    },
    {
        "id": "skill.edgartools",
        "directory": "tests/fixtures/skills/edgartools",
        "version": "5.61.1",
        "apps": ("ai-engineer",),
        "skill_name": "edgartools",
        "provenance": {
            "source": "local",
            "locator": "pypi:edgartools==5.61.1 (edgar.ai.install_skill)",
            "upstream_identity": "dgunning/edgartools",
            "upstream_version": "5.61.1",
            "commit_digest": "7338aa335f6442c52dfa0695422cf1cd27e946e0",
            "license": "MIT",
        },
        "tags": ["sec", "edgar", "filings", "financials", "xbrl"],
    },
)
MISSION_CONTROL_SKILLS: tuple[str, ...] = (
    "mission-control",
    "mission-control-author",
    "mission-control-catalog",
    "mission-control-compose",
    "mission-control-intervene",
    "mission-control-observe",
)
HOOK: dict[str, Any] = {
    "id": "hook.mc-policy-template",
    "directory": "scripts/hooks/policy_template",
    "version": "1.0.0",
    "apps": APPS,
    "title": "Mission Control policy template hook",
    "description": (
        "Fail-closed policy hook for every lane: denies rm -rf of the root (any flag order), "
        "force pushes, and file writes outside the declared write paths. Reads "
        "mc.hook_input.v1 on stdin and writes mc.hook_result.v1 on stdout."
    ),
    "events": ["before_shell", "before_tool"],
    "fail_closed": True,
    "entrypoint": "policy.py",
    "interpreter": "python",
    "timeout_seconds": 10,
}
PLUGIN: dict[str, Any] = {
    "id": "plugin.web-research",
    "apps": APPS,
    "title": "Web research plugin (Tavily, Firecrawl, agent-browser)",
    "description": (
        "Composition of exact pins: Tavily and Firecrawl MCP servers for search, extraction "
        "and crawling, the agent-browser skill stub, and the agent-browser MCP server "
        "(optional) for a real browser. No install command or marketplace reference."
    ),
    "members": [
        ("mcp.tavily", "mcp_server", False),
        ("mcp.firecrawl", "mcp_server", False),
        ("skill.agent-browser", "skill", False),
        ("mcp.agent-browser", "mcp_server", True),
    ],
}


def _agent_capabilities() -> Any:
    spec = importlib.util.spec_from_file_location(
        "mc_seed_agent_capabilities_for_skills", SEEDS_ROOT / "agent_capabilities.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def directory_files(relative: str, *, lf: bool) -> list[tuple[str, bytes]]:
    """Every file of a source directory (POSIX relative paths), bytes as committed.

    Third-party captures are read verbatim (their fixtures are ``-text`` in git);
    Mission Control's ``skills/`` bundles are LF-canonical, like ``make skills-manifest``.
    """

    root = REPO_ROOT / relative
    files: list[tuple[str, bytes]] = []
    for path in sorted(root.rglob("*")):
        if not path.is_file() or "__pycache__" in path.parts:
            continue
        content = path.read_bytes()
        if lf:
            content = content.replace(b"\r\n", b"\n")
        files.append((path.relative_to(root).as_posix(), content))
    return files


def bundle_sources() -> list[dict[str, Any]]:
    """Every seeded bundle: id, kind, version, applications, files and definition fields."""

    from mission_control.domain.capabilities.host_support import all_profiles

    support = all_profiles().model_dump(mode="json")
    sources: list[dict[str, Any]] = []
    for spec in THIRD_PARTY_SKILLS:
        fields: dict[str, Any] = {
            "source_provenance": spec["provenance"],
            "host_support": support,
            "review_status": "approved",
            "maturity": "experimental",
        }
        if "skill_name" in spec:
            fields["skill_name"] = spec["skill_name"]
        sources.append(
            {
                "id": spec["id"],
                "kind": "skill_bundle",
                "version": spec["version"],
                "apps": spec["apps"],
                "directory": spec["directory"],
                "files": directory_files(spec["directory"], lf=False),
                "fields": fields,
            }
        )
    for name in MISSION_CONTROL_SKILLS:
        directory = f"skills/{name}"
        manifest = json.loads((REPO_ROOT / directory / "manifest.json").read_text("utf-8"))
        sources.append(
            {
                "id": f"skill.{name}",
                "kind": "skill_bundle",
                "version": manifest["version"],
                "apps": APPS,
                "directory": directory,
                "files": directory_files(directory, lf=True),
                "fields": {
                    "source_provenance": {
                        "source": "local",
                        "locator": f"repo://{directory}",
                        "upstream_identity": f"skill.{name}",
                        "upstream_version": manifest["version"],
                        "license": "proprietary",
                    },
                    "host_support": support,
                    "review_status": "approved",
                    "maturity": "qualified",
                },
            }
        )
    sources.append(
        {
            "id": HOOK["id"],
            "kind": "hook_script",
            "version": HOOK["version"],
            "apps": HOOK["apps"],
            "directory": HOOK["directory"],
            "files": directory_files(HOOK["directory"], lf=True),
            "fields": {
                "title": HOOK["title"],
                "description": HOOK["description"],
                "events": HOOK["events"],
                "fail_closed": HOOK["fail_closed"],
                "entrypoint": HOOK["entrypoint"],
                "interpreter": HOOK["interpreter"],
                "timeout_seconds": HOOK["timeout_seconds"],
                "side_effect_class": "read_only",
                "host_support": support,
                "source_provenance": {
                    "source": "local",
                    "locator": f"repo://{HOOK['directory']}",
                    "upstream_identity": HOOK["id"],
                    "upstream_version": HOOK["version"],
                    "license": "proprietary",
                },
            },
        }
    )
    return sources


def bundle_manifest(source: dict[str, Any], application_id: str) -> Any:
    from mission_control.domain.capabilities.bundles import build_bundle_manifest

    executable = (
        frozenset(path for path, content in source["files"] if content.startswith(b"#!"))
        if source["kind"] == "hook_script"
        else frozenset()
    )
    return build_bundle_manifest(
        source["files"],
        application_id=application_id,
        kind=source["kind"],
        capability_id=source["id"],
        version=source["version"],
        executable=executable,
    )


def bundle_definition_for(source: dict[str, Any], application_id: str) -> tuple[Any, Any]:
    """``(manifest, definition)`` of one bundle for one application (FT-A2 shapes)."""

    from mission_control.application.capabilities.bundle_custody import bundle_definition

    manifest = bundle_manifest(source, application_id)
    definition = bundle_definition(manifest, source["files"], dict(source["fields"]))
    return manifest, definition


def definition_pin(definition: Any) -> str:
    """The Capability Pin the catalog computes for the published row (revision 1).

    Same rule as ``domain/capabilities/catalog_entry.capability_pin``: the upstream version
    when the definition records one, else the catalog revision.
    """

    from mission_control.domain.authoring.canonical import sha256_digest
    from mission_control.domain.authoring.contracts import ExactDefinitionRef, PublishedDefinition
    from mission_control.domain.capabilities.catalog_entry import capability_pin

    published = PublishedDefinition(
        ref=ExactDefinitionRef(
            kind=definition.kind,
            logical_id=definition.logical_id,
            revision=1,
            digest=sha256_digest(definition),
        ),
        definition=definition,
        published_at=AGENT_SKILL_SEED_TIME,
        published_by="seed:mission-control-catalog",
    )
    return capability_pin(published).render()


def plugin_definition(application_id: str, skill_pins: dict[str, str]) -> Any:
    from mission_control.domain.authoring.contracts import PluginDefinition
    from mission_control.domain.capabilities.host_support import (
        CapabilityHostSupport,
        all_profiles,
    )
    from mission_control.domain.capabilities.pins import CapabilityPin
    from mission_control.domain.capabilities.plugins import PluginManifest

    capabilities = _agent_capabilities()
    pins = {**capabilities.seed_pins(), **skill_pins}
    servers = {spec["id"]: spec for spec in capabilities.SERVERS}
    manifest = PluginManifest.model_validate(
        {
            "members": [
                {"pin": pins[member], "role": role, "optional": optional}
                for member, role, optional in PLUGIN["members"]
            ]
        }
    )

    def member_support(pin: CapabilityPin) -> CapabilityHostSupport:
        spec = servers.get(pin.capability_id)
        if spec is None:
            return all_profiles()
        return CapabilityHostSupport.model_validate(spec["host_support"])

    return PluginDefinition.model_validate(
        {
            "logical_id": PLUGIN["id"],
            "title": PLUGIN["title"],
            "description": PLUGIN["description"],
            "manifest": manifest.model_dump(mode="json"),
            "host_support": manifest.host_support(member_support).model_dump(mode="json"),
            "secret_refs": sorted(
                {
                    ref
                    for member, _role, _optional in PLUGIN["members"]
                    for ref in servers.get(member, {}).get("secret_refs", [])
                }
            ),
        }
    )


def application_definitions(application_id: str) -> list[dict[str, Any]]:
    """Every seeded definition of one application, in seed order, with its custody facts."""

    entries: list[dict[str, Any]] = []
    skill_pins: dict[str, str] = {}
    for source in bundle_sources():
        if application_id not in source["apps"]:
            continue
        manifest, definition = bundle_definition_for(source, application_id)
        pin = definition_pin(definition)
        if source["kind"] == "skill_bundle":
            skill_pins[source["id"]] = pin
        entries.append(
            {
                "source": source,
                "manifest": manifest,
                "definition": definition,
                "pin": pin,
            }
        )
    if application_id in PLUGIN["apps"]:
        plugin = plugin_definition(application_id, skill_pins)
        entries.append(
            {
                "source": None,
                "manifest": None,
                "definition": plugin,
                "pin": definition_pin(plugin),
            }
        )
    return entries


def asset_key(definition: Any) -> str:
    return f"{KEY_PREFIX}:{definition.kind.value.replace('_', '-')}:{definition.logical_id}/1"


def bundles(published_records: Any, seed_format: str, compatibility: str, actor: str) -> dict:
    """One seed bundle per application (manifests and object prefixes are per application)."""

    from mission_control.domain.authoring.contracts import DefinitionKind

    capabilities = _agent_capabilities()
    result: dict[str, Any] = {}
    for app in APPS:
        records: list[dict[str, Any]] = []
        plugin_key: str | None = None
        for entry in application_definitions(app):
            definition = entry["definition"]
            source = entry["source"]
            evidence = [
                "packages/mission-control-db-contract/seeds/agent_skills.py",
                "docs/specs/fast-track-2026-10/research/seed-capabilities-and-formats.md",
            ]
            if source is not None:
                evidence.insert(1, source["directory"])
            records += published_records(definition, KEY_PREFIX, evidence)
            if definition.kind == DefinitionKind.PLUGIN:
                plugin_key = asset_key(definition)
        assert plugin_key is not None
        for position, (member, role, optional) in enumerate(PLUGIN["members"]):
            member_key = (
                f"{KEY_PREFIX}:skill:{member}/1"
                if member.startswith("skill.")
                else f"agent-capability:mcp-server:{member}/1"
            )
            records.append(
                {
                    "kind": "capability_plugin_member",
                    "logical_key": f"{plugin_key}:member:{position}",
                    "fields": {
                        "plugin": plugin_key,
                        "member": member_key,
                        "position": position,
                        "role": role,
                        "optional": optional,
                    },
                }
            )
        names = ", ".join(entry["definition"].logical_id for entry in application_definitions(app))
        result[f"{app}/{APP_KEYS[app]}-{SEED_VERSION}.json"] = {
            "format": seed_format,
            "seed_key": APP_KEYS[app],
            "seed_version": SEED_VERSION,
            "component_compatibility": compatibility,
            "depends_on": [
                {"seed_key": capabilities.COMMON_KEY, "seed_version": capabilities.SEED_VERSION},
                {"seed_key": capabilities.APP_KEYS[app], "seed_version": capabilities.SEED_VERSION},
            ],
            "actor_ref": actor,
            "description": (
                f"Agent skills, the policy hook and the web-research plugin admitted in '{app}' "
                f"(FT-A7): {names}. Bytes live in the capability-bundles bucket under each "
                "manifest's digest path (upload with scripts/seeds_publish_bundles.py); the "
                "plugin pins its members exactly and records them in capability_plugin_member. "
                "Requires migration 0025."
            ),
            "records": records,
        }
    return result


def custody_index(application_id: str) -> list[dict[str, Any]]:
    """What ``scripts/seeds_publish_bundles.py`` uploads and the seed rows reference."""

    index = []
    for entry in application_definitions(application_id):
        manifest = entry["manifest"]
        if manifest is None:
            continue
        index.append(
            {
                "capability_id": manifest.capability_id,
                "kind": manifest.kind,
                "version": manifest.version,
                "pin": entry["pin"],
                "manifest_digest": manifest.digest,
                "object_prefix": manifest.object_prefix,
                "computed_hash": manifest.computed_hash,
                "files": len(manifest.files),
                "bytes": manifest.total_bytes,
            }
        )
    return index
