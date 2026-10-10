"""MP-20 FIXTURES: one production stack with every local lane, scripted at the provider seam.

What is production: the API/worker composition of `open_postgres_production_stack` (real
PostgreSQL 17 common component, real Temporal dev server), the public `missions:submit` /
`missions:start` router, the manifest compile/submit services, the production launch author
(`compose_manifest_launch_inputs` over a `mc.manifest_launch_bindings.v2` file), the Stage
Graph / GoalDirected families, `OperationWorkflow`, the production operation boundary, the
production `LaneTurnService` over PostgreSQL frames/lane state, the production workspace
candidate capture service (through `WorkspaceCandidateLaneOutputs`, the MP-20 custody port)
and the production chain release hook and relay pump.

What is FIXTURE (injected at the provider client/launcher seam only):

* Deep Agents: the parent chat model (`ParityModel`, a `ChainModel` that also scripts Stage
  Graph stages); every tool, middleware and workspace path is the real Deep Agents runtime.
* Claude Agent SDK: `ResponderClient` (a `tests/unit/claude/fixtures.FixtureClient` whose
  turns are computed from the leased workspace instead of a JSONL file).
* Codex: `ResponderLauncher` (the `tests/unit/codex/fixture_app_server` app-server whose turns
  are computed the same way).
* Cursor local: `ResponderBridgeLauncher` (the `tests/fixtures/cursor_local` replaying bridge
  whose runs are computed from the lease on each send). Cursor cloud: `ResponderCloudApi` (the
  `tests/fixtures/cursor_cloud` Cloud Agents API v1 fake behind `httpx.MockTransport`, whose runs
  are computed from the published run branch and whose artifacts are what the scripted agent
  wrote under `outputs/`). The real `CursorLocalHarness` / `CursorCloudHarness` run over them,
  with the production `RenderedProjectionSource(CatalogRows(...))` re-rendering and verifying
  the launch author's sealed `mc.cursor_binding.v1` projection at `prepare`. Production
  `GitBranchPublisher` commits a later unit's packet onto the per-run branch.
* The lanes' tmp-dir workspace ports (the Cursor target repository is a tmp git repository and
  a tmp bare remote the deployment file maps the manifest repository to) and static auth
  admissions, the deployment bindings file, the `application: biotech` transform.

A scripted provider "writes" a file the way the real agent would: into the lease's `outputs/`
before its terminal message, and answers with one JSON object (the lane Completion Candidate).
Nothing is spawned, nothing is paid for, and none of this is a live qualification.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import stat
import tempfile
from collections.abc import AsyncIterable, AsyncIterator, Mapping
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass, field, replace
from hashlib import sha256
from pathlib import Path
from typing import Any, cast
from uuid import uuid4

import httpx
from claude_agent_sdk.types import ClaudeAgentOptions
from fastapi import FastAPI
from langchain_core.messages import BaseMessage, HumanMessage, ToolMessage
from langchain_core.outputs import ChatResult
from pydantic import SecretStr
from temporalio.worker import Worker
from tests.fixtures import mission_control_production_stack as production_stack_module
from tests.fixtures.catalog.fast_track_catalog import fast_track_catalog
from tests.fixtures.cursor_cloud import API_KEY as CURSOR_API_KEY
from tests.fixtures.cursor_cloud import PRESIGNED, FakeCloudApi, make_remote
from tests.fixtures.cursor_local import MemoryArtifacts as CursorArtifacts
from tests.fixtures.cursor_local import ReplayBridge, ReplayBridgeLauncher, make_repository
from tests.fixtures.manifest_runtime import ChainModel, ChainScript, GoalScript
from tests.fixtures.mission_control_production_stack import open_postgres_production_stack
from tests.fixtures.rrm009_production_harness import ProductionStack
from tests.fixtures.rrm009_production_stack import (
    AGENT_COGNITIVE_QUEUE,
    SCOPE,
    TechnicalBinding,
    technical_binding,
)
from tests.integration.temporal.test_manifest_launch_production import host_pins, missions_app
from tests.integration.temporal.test_manifest_v2_provider_launch import (
    AUTH_PROFILES,
    _PayloadInputs,
    provider_lane,
)
from tests.unit.authoring.manifest_launch_fixture import fixture_bindings
from tests.unit.claude.fixtures import (
    FIXTURE_ENVIRON,
    FixtureClient,
    FixtureClientFactory,
    FixtureScript,
    FixtureWorkspace,
    ScriptRecord,
    fixture_admission,
)
from tests.unit.codex.fixture_app_server import (
    FixtureAppServer,
    FixtureLaunch,
    FixtureLauncher,
    channel_pair,
)
from tests.unit.codex.support import MemoryArtifacts, StaticAuth, TmpLeaser, fixture_settings
from tests.unit.codex.support import admission as codex_admission

from mission_control.adapters.claude.harness import (
    ClaudeAgentSdkHarness,
    ClaudeLaneSettings,
    StaticAuthAdmitter,
)
from mission_control.adapters.codex.harness import CodexLocalHarness
from mission_control.adapters.codex.launcher import LaunchedAppServer, LaunchSpec
from mission_control.adapters.codex.transport import AppServerConnection
from mission_control.adapters.cursor.cloud import CursorCloudHarness
from mission_control.adapters.cursor.cloud_api import CloudAgentsClient
from mission_control.adapters.cursor.hooks_callback import CursorHookMapper
from mission_control.adapters.cursor.local import CursorLocalHarness, CursorLocalSettings
from mission_control.adapters.cursor.projection import (
    CatalogRows,
    RenderedProjectionSource,
    hook_context_index,
    static_rows,
)
from mission_control.adapters.cursor.scm import GitBranchPublisher
from mission_control.adapters.cursor.sse import SseEvent
from mission_control.adapters.cursor.workspace import GitWorktreeLeaser, git
from mission_control.adapters.postgres.chains.store import HOOK_NAME, ChainReleaseHook
from mission_control.adapters.postgres.control_plane.definition_repository import (
    PostgresDefinitionRepository,
)
from mission_control.adapters.postgres.control_plane.manifest_submission import (
    PostgresManifestSubmissionRepository,
)
from mission_control.adapters.postgres.run_control.canonical import register_post_append_hook
from mission_control.adapters.postgres.run_control.stop_fence import PostgresStopFenceRepository
from mission_control.adapters.postgres.subscriptions.store import PostgresSubscriptionStore
from mission_control.adapters.storage.control_plane_payloads import InMemoryPayloadStore
from mission_control.adapters.temporal.registration.activities import agent_cognitive_activities
from mission_control.application.authoring.cursor_launch import CursorCloudLane, CursorLocalLane
from mission_control.application.authoring.manifest_launch_inputs import (
    ManifestLaunchBindings,
    ManifestLaunchInputAuthor,
)
from mission_control.application.authoring.manifest_service import (
    ManifestCompileService,
    ManifestProgramCompiler,
    MissionManifestService,
)
from mission_control.application.authoring.manifest_submit import (
    ManifestSubmitService,
    register_manifest_admission_policies,
)
from mission_control.application.authoring.provider_launch import (
    LAUNCH_BINDINGS_SCHEMA_V2,
    ProviderLanes,
)
from mission_control.application.execution.harness.hook_callbacks import (
    HookCallbackService,
    InMemoryHookIntentLedger,
    InMemoryHookTokenStore,
)
from mission_control.application.execution.harness.leases import InMemoryWorkspaceLeaseStore
from mission_control.application.execution.harness.registry import LaneRegistry
from mission_control.application.execution.operations.lane_outputs import (
    WorkspaceCandidateLaneOutputs,
)
from mission_control.application.execution.run_launch import RunLaunchService
from mission_control.application.execution.service import (
    AdmissionPolicyRegistry,
    RunControlService,
)
from mission_control.application.execution.stop_fence import KernelHookFenceGate
from mission_control.application.frames.sink import InMemoryFrameStore
from mission_control.application.subscriptions.service import SubscriptionService
from mission_control.bootstrap.manifests import compose_manifest_launch_inputs
from mission_control.bootstrap.provider_auth import provider_child_environment
from mission_control.bootstrap.technical_api import api
from mission_control.domain.authoring.extensions import ExtensionRegistry
from mission_control.domain.authoring.manifest import canonical_manifest_bytes

LOCAL_PROFILES = ("deep_agents", "claude_agent_sdk", "codex")
SESSION_PROFILES = ("claude_agent_sdk", "codex")
CURSOR_PROFILES = ("cursor_local", "cursor_cloud")
REPO = {"url": "https://github.com/Overton77/mission-control-fixture", "ref": "main"}
SECRET_MARKERS = tuple(
    value for key, value in FIXTURE_ENVIRON.items() if key not in {"PATH", "HOME"}
)
_MISSION = re.compile(r"Mission (\S+?) (?:stage (\S+?):|Goal Loop (executor|verifier))")
_ACTIVATION = re.compile(r"activation `([^`]+)`")
_ITERATION = re.compile(r"goal-iteration/(\d+)/(executor|verifier)")


def lane_environment(profile: str) -> dict[str, Any]:
    """A node/mission `environment` selecting one lane through the deployment profiles."""

    if profile == "deep_agents":
        return {
            "lane": "deep_agents",
            "model": {"profile": "frontier.default"},
            "sandbox": {"profile": "research.standard"},
        }
    if profile == "cursor_local":
        return {
            "lane": profile,
            "model": {"profile": "frontier.default"},
            "auth": {"profile": "cursor-owner"},
            "execution_environment": {"kind": "local_workspace", "profile": "worker.linux.wsl"},
            "workspace": {
                "repo": {**REPO, "path": "/srv/repos/mission-control-fixture"},
                "policy": {"mode": "managed_worktree", "reuse": "within_run"},
            },
        }
    if profile == "cursor_cloud":
        return {
            "lane": profile,
            "model": {"profile": "frontier.default"},
            "auth": {"profile": "cursor-owner"},
            "execution_environment": {
                "kind": "provider_hosted",
                "provider": "cursor",
                "environment_ref": "cursor-env.fixture",
                "expected_revision": "sha256:" + "0" * 64,
                "setup": {"pin": "cursor-setup@1"},
            },
            "workspace": {
                "repo": REPO,
                "policy": {
                    "mode": "provider_workspace",
                    "reuse": "within_session",
                    "dirty_input": "reject",
                    "cleanup": "retain_until_artifacts_registered",
                },
            },
        }
    model, host = {
        "claude_agent_sdk": ("claude.default", "worker.linux.default"),
        "codex": ("codex.default", "worker.linux.wsl"),
    }[profile]
    return {
        "lane": profile,
        "model": {"profile": model},
        "auth": {"profile": AUTH_PROFILES[profile]},
        "execution_environment": {"kind": "local_workspace", "profile": host},
        "workspace": {"repo": REPO, "policy": {"mode": "managed_worktree", "reuse": "within_run"}},
    }


MISSION_REQUIRES = {
    "controls": ["cancel"],
    "approvals": ["workflow_gate"],
    "observation": ["terminal_result"],
}


def _mission_environment(profile: str, *, iterations: int = 4) -> dict[str, Any]:
    return {
        **lane_environment(profile),
        "requires": MISSION_REQUIRES,
        "budget": {"tokens": 400000, "wall_clock": "2h", "tool_calls": 200},
        "governors": {"depth": 1, "fan_out": 2, "iterations": iterations, "patience": 2},
        "side_effects": ["read_only", "workspace_write"],
    }


def manifest_text(document: Mapping[str, Any]) -> str:
    return canonical_manifest_bytes(dict(document)).decode("utf-8")


def handoff_stage_graph(
    key: str, producer: str, consumer: str, *, mission_lane: str | None = None
) -> dict[str, Any]:
    """Stage `produce` on `producer` hands its accepted `draft` to `consume` on `consumer`.

    The mission environment is the producer's unless `mission_lane` names another: a node's
    `execution_environment` overlay of another placement kind (provider_hosted over
    local_workspace, or the reverse) does not compile today (MP-20 Cursor finding), so a
    `cursor_cloud` stage mixed with a worker-hosted one inherits from a Deep Agents mission.
    """

    return {
        "manifest": "mission/v2",
        "mission": {
            "key": key,
            "title": f"MP-20 handoff {producer} to {consumer}",
            "application": "biotech",
            "goals": [
                {
                    "key": "handed_off",
                    "description": "The consumer reports on the producer's accepted draft",
                    "objectives": [{"key": "drafted", "description": "Write the draft"}],
                    "criteria": [
                        {
                            "key": "report_written",
                            "description": "The report cites the draft digest",
                            "evidence": ["report"],
                            "acceptance": {"schema": "report_note@1"},
                        }
                    ],
                }
            ],
            "environment": _mission_environment(mission_lane or producer),
            "program": {
                "key": "root",
                "behavior": "stage_graph",
                "objectives": ["handed_off"],
                "nodes": [
                    {
                        "key": "produce",
                        "behavior": "agent_executor",
                        "objectives": ["drafted"],
                        "instruction": "Write the draft under outputs/.",
                        "environment": lane_environment(producer),
                        "outputs": [{"name": "draft", "schema": "draft_note@1"}],
                    },
                    {
                        "key": "consume",
                        "behavior": "agent_executor",
                        "objectives": ["handed_off"],
                        "depends_on": ["produce"],
                        "inputs": [
                            {"name": "draft", "from": "produce.draft", "expand": "materialize"}
                        ],
                        "instruction": "Read the draft and write the report.",
                        "environment": lane_environment(consumer),
                        "outputs": [{"name": "report", "schema": "report_note@1"}],
                    },
                ],
            },
        },
    }


def gated_stage_graph(key: str, producer: str, consumer: str) -> dict[str, Any]:
    """`produce` -> Human Gate `review` (the owner) -> `consume`, each provider its own node."""

    document = handoff_stage_graph(key, producer, consumer)
    nodes = document["mission"]["program"]["nodes"]
    produce, consume = nodes
    consume["depends_on"] = ["review"]
    review = {
        "key": "review",
        "behavior": "human_gate",
        "depends_on": ["produce"],
        "task": {
            "kind": "REVIEW",
            "prompt": "Accept the draft for the report?",
            "reviewers": ["owner"],
            "packet": ["produce.draft"],
            "on_timeout": "keep_waiting",
        },
    }
    document["mission"]["program"]["nodes"] = [produce, review, consume]
    return document


def goal_loop(key: str, executor: str, verifier: str, *, iterations: int = 3) -> dict[str, Any]:
    """A Goal Loop whose executor runs on `executor` and its independent verifier on
    `verifier` (the verifier binding passes the same admission as the executor)."""

    program: dict[str, Any] = {
        "key": "root",
        "behavior": "goal_loop",
        "objective": "converged",
        "action_space": ["root"],
        "verifier": {"independent": True},
        "outputs": [{"name": "finding", "schema": "finding_note@1", "required": True}],
    }
    if verifier != executor:
        program["verifier"] = {
            "independent": True,
            "environment": lane_environment(verifier),
        }
    return {
        "manifest": "mission/v2",
        "mission": {
            "key": key,
            "title": f"MP-20 goal loop {executor}/{verifier}",
            "application": "biotech",
            "goals": [
                {
                    "key": "converged",
                    "description": "The finding converges under independent verification",
                    "criteria": [
                        {
                            "key": "finding_recorded",
                            "description": "The finding names its evidence",
                            "evidence": ["finding"],
                            "acceptance": {"schema": "finding_note@1"},
                        }
                    ],
                }
            ],
            "environment": _mission_environment(executor, iterations=iterations),
            "program": program,
        },
    }


def goal_chain(
    supplier_key: str, supplier: str, consumer_key: str, consumer: str
) -> dict[str, Any]:
    """Two linked Goal Loops: `supplier` supplies its accepted `finding` to `consumer`."""

    first = goal_loop(supplier_key, supplier, supplier)["mission"]
    second = goal_loop(consumer_key, consumer, consumer)["mission"]
    second["program"]["outputs"] = [
        {"name": "summary", "schema": "summary_note@1", "required": True}
    ]
    second["goals"][0]["criteria"][0]["evidence"] = ["summary"]
    second["goals"][0]["criteria"][0]["acceptance"] = {"schema": "summary_note@1"}
    second["program"]["inputs"] = [
        {"name": "finding", "from": f"{supplier_key}.finding", "expand": "materialize"}
    ]
    return {
        "manifest": "mission/v2",
        "missions": [first, second],
        "links": [
            {
                "from": supplier_key,
                "to": consumer_key,
                "kind": "supplies",
                "outputs": ["finding"],
                "on": {"goal_accepted": {"goal_key": "converged"}},
            }
        ],
    }


# --- the shared script ---------------------------------------------------------------------------


@dataclass(frozen=True)
class StagePlan:
    output: str
    obligations: tuple[str, ...] = ()
    hold: bool = False
    padded: bool = False


@dataclass
class Observation:
    """What one scripted provider turn saw in its workspace (V14 evidence)."""

    profile: str
    mission_key: str
    node: str
    activation: str
    context_text: str
    inputs_text: str
    inputs: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class ParityScript(ChainScript):
    """`ChainScript` (Deep Agents goals keyed by configuration digest) plus Stage Graph plans
    and Goal Loop plans for the lane responders, keyed by mission key."""

    stages: dict[tuple[str, str], StagePlan] = field(default_factory=dict)
    goals: dict[str, GoalScript] = field(default_factory=dict)
    observations: list[Observation] = field(default_factory=list)
    written: dict[tuple[str, str, str], bytes] = field(default_factory=dict)
    lane_turns: list[tuple[str, str, str]] = field(default_factory=list)

    def goal(self, mission: Mapping[str, Any], plan: GoalScript) -> None:
        self.by_configuration[str(mission["effective_configuration_digest"])] = plan
        self.goals[str(mission["mission_key"])] = plan


# Above the packer's `auto` materialize floor (65 536 bytes): a padded handoff is delivered as
# a digest-checked, read-only file rather than inlined into the context index.
HANDOFF_PADDING = 70_000


def stage_document(
    mission_key: str, node: str, profile: str, inputs: list[dict[str, Any]], *, padded: bool
) -> bytes:
    return json.dumps(
        {
            "schema": "stage_note@1",
            "mission": mission_key,
            "node": node,
            "lane": profile,
            "inputs": sorted(str(item.get("content_digest")) for item in inputs),
            "body": "draft line. " * (HANDOFF_PADDING // 12) if padded else "",
        },
        sort_keys=True,
    ).encode("utf-8")


def goal_document(goal: GoalScript, iteration: int, profile: str) -> bytes:
    return json.dumps(
        {
            "schema": f"{goal.output_name}@1",
            "obligation": goal.obligation,
            "iteration": iteration,
            "lane": profile,
        },
        sort_keys=True,
    ).encode("utf-8")


def executor_observation(goal: GoalScript, iteration: int) -> dict[str, Any]:
    accepted = goal.accept_at is not None and iteration >= goal.accept_at
    return {
        "schema_version": "belllabs.goal-executor-observation.v1",
        "disposition": "completed",
        # Reconciled by the lane settlement to what this attempt registered.
        "output_refs": [],
        "completion_claim": accepted,
        "accepted_fact_refs": [f"fact:{goal.obligation}:{iteration}"],
        "evidence_refs": [f"evidence:{goal.obligation}:executor:{iteration}"],
        "handoff": None,
        "output_contract_ref": goal.output_contract,
    }


def verifier_observation(goal: GoalScript, iteration: int) -> dict[str, Any]:
    accepted = goal.accept_at is not None and iteration >= goal.accept_at
    return {
        "schema_version": "belllabs.goal-verifier-observation.v1",
        "decision": "accepted" if accepted else "rejected",
        "progress_made": True,
        "accepted_obligation_refs": [goal.obligation] if accepted else [],
        "findings": [],
        "evidence_refs": [f"evidence:{goal.obligation}:verifier:{iteration}"],
        "unmet_obligations": [] if accepted else [goal.obligation],
        "obligation_applicability": [[goal.obligation, True]],
        "output_contract_ref": goal.output_contract,
    }


@dataclass(frozen=True)
class LaneReply:
    files: dict[str, bytes]
    final: str
    hold: bool = False


def _inspect_inputs(root: Path, inputs_text: str) -> list[dict[str, Any]]:
    """Each materialized input: its digest against the manifest and whether it is read-only."""

    try:
        manifest = json.loads(inputs_text) if inputs_text else {}
    except ValueError:
        return []
    found: list[dict[str, Any]] = []
    for item in manifest.get("inputs", ()):
        path = root / str(item["path"]).lstrip("/")
        content = path.read_bytes() if path.is_file() else b""
        found.append(
            {
                **item,
                "exists": path.is_file(),
                "digest_ok": path.is_file()
                and f"sha256:{sha256(content).hexdigest()}" == item["content_digest"],
                "read_only": path.is_file() and not (path.stat().st_mode & stat.S_IWRITE),
                "content": content.decode("utf-8", errors="replace"),
            }
        )
    return found


def lane_reply(script: ParityScript, profile: str, root: Path, prompt: str) -> LaneReply:
    """What a scripted provider does on one turn, from its leased workspace and prompt."""

    # The lane contract (operating contract, first turn, hooks): the packet is `.mission/` at
    # the lease root for every unit, Stage Graph stage and Goal Loop role alike, and the
    # packet's own index pointer names that same place.
    pointer = re.search(r"This index: (\S*?)/\.mission/context\.md", prompt)
    assert pointer is None or not pointer.group(1).strip("/"), pointer.group(0)
    context_path, inputs_path = root / ".mission/context.md", root / ".mission/inputs.json"
    context = context_path.read_text(encoding="utf-8") if context_path.is_file() else ""
    inputs_text = inputs_path.read_text(encoding="utf-8") if inputs_path.is_file() else ""
    mission = _MISSION.search(prompt) or _MISSION.search(context)
    activation_match = _ACTIVATION.search(context)
    activation = activation_match.group(1) if activation_match else ""
    if mission is None:
        return LaneReply(files={}, final="unscripted")
    mission_key = mission.group(1)
    inputs = _inspect_inputs(root, inputs_text)
    if mission.group(2):
        node = mission.group(2)
        script.observations.append(
            Observation(profile, mission_key, node, activation, context, inputs_text, inputs)
        )
        script.lane_turns.append((profile, mission_key, node))
        plan = script.stages[(mission_key, node)]
        content = stage_document(mission_key, node, profile, inputs, padded=plan.padded)
        script.written[(mission_key, node, profile)] = content
        return LaneReply(
            files={f"outputs/{plan.output}.json": content},
            # The Completion Candidate names its outputs; the settlement reconciles the refs.
            final=json.dumps({"obligation_refs": list(plan.obligations), "output_refs": []}),
            hold=plan.hold,
        )
    role = mission.group(3)
    iteration_match = _ITERATION.search(activation) or _ITERATION.search(context)
    goal = script.goals[mission_key]
    iteration = (
        int(iteration_match.group(1))
        if iteration_match
        else 1 + sum(1 for item in script.lane_turns if item[1:] == (mission_key, role))
    )
    script.lane_turns.append((profile, mission_key, role))
    script.observations.append(
        Observation(profile, mission_key, role, activation, context, inputs_text, inputs)
    )
    if role == "executor":
        content = goal_document(goal, iteration, profile)
        script.written[(mission_key, f"executor:{iteration}", profile)] = content
        return LaneReply(
            files={f"outputs/{goal.output_name}.json": content},
            final=json.dumps(executor_observation(goal, iteration)),
        )
    return LaneReply(files={}, final=json.dumps(verifier_observation(goal, iteration)))


def _write(root: Path, files: Mapping[str, bytes]) -> None:
    for relative, content in files.items():
        target = root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)


# --- Deep Agents (the fixture parent model) ------------------------------------------------------


def _numbered(text: str) -> str:
    """A Deep Agents `read_file` result without its `cat -n` style line numbers."""

    return re.sub(r"(?m)^\s*\d+(?:\.\d+)?(?:\t|  )", "", text)


class ParityModel(ChainModel):
    """`ChainModel` for Goal Loops; Stage Graph stages read the packet, write their output into
    the writable slot and answer with the obligation refs the plan names."""

    def _reply(self, messages: list[BaseMessage]) -> ChatResult:
        if "goal-iteration/" in self.operation_id:
            return super()._reply(messages)
        self._record("parent", messages)
        script = cast(ParityScript, self.script)
        text = "\n".join(str(item.content) for item in messages if isinstance(item, HumanMessage))
        mission = _MISSION.search(text)
        if mission is None or not mission.group(2):
            return self._final({"answer": "unscripted", "facts": {}})
        mission_key, node = mission.group(1), mission.group(2)
        plan = script.stages[(mission_key, node)]
        tools = [item for item in messages if isinstance(item, ToolMessage)]
        index = re.search(r"This index: (\S*?)/\.mission/context\.md", text)
        reads = (
            [f"{index.group(1)}/.mission/context.md", f"{index.group(1)}/.mission/inputs.json"]
            if index
            else []
        )
        if len(tools) < len(reads):
            return self._call("read_file", {"file_path": reads[len(tools)]})
        manifest_text = _numbered(str(tools[1].content)) if len(reads) == 2 else ""
        paths = re.findall(r'"path": "([^"]+)"', manifest_text)
        if len(tools) < len(reads) + len(paths):
            return self._call(
                f"read_file_{len(tools)}", {"file_path": paths[len(tools) - len(reads)]}
            )
        inputs = [
            {
                "path": path,
                "content_digest": digest,
                "content": _numbered(str(tools[len(reads) + i].content)),
            }
            for i, (path, digest) in enumerate(
                zip(paths, re.findall(r'"content_digest": "([^"]+)"', manifest_text), strict=False)
            )
        ]
        root = next(
            (str(root) for _name, root in self.slots if str(root).rstrip("/").endswith("output")),
            str(self.slots[0][1]),
        )
        path = f"{root.rstrip('/')}/{plan.output}.json"
        written = len(tools) - len(reads) - len(paths)
        if written == 0:
            script.lane_turns.append(("deep_agents", mission_key, node))
            script.observations.append(
                Observation(
                    "deep_agents",
                    mission_key,
                    node,
                    self.operation_id,
                    str(tools[0].content) if reads else "",
                    manifest_text,
                    inputs,
                )
            )
            content = stage_document(mission_key, node, "deep_agents", inputs, padded=plan.padded)
            script.written[(mission_key, node, "deep_agents")] = content
            return self._call("write_file", {"file_path": path, "content": content.decode()})
        # No `output_refs` key: the runtime's registered captures are the stage's outputs.
        return self._final({"obligation_refs": list(plan.obligations)})

    def _call(self, name: str, args: dict[str, Any]) -> ChatResult:
        # `read_file_<n>` keeps every tool call id distinct while calling `read_file`.
        result = super()._call(name, args)
        if name.startswith("read_file_"):
            message = result.generations[0].message
            call = dict(message.tool_calls[0])  # type: ignore[attr-defined]
            call["name"] = "read_file"
            message.tool_calls = [call]  # type: ignore[attr-defined]
        return result


def parity_components(
    technical: TechnicalBinding, script: ParityScript, model_log: list[dict[str, Any]]
) -> Any:
    base = technical.components(model_log)

    def parent(bound: Any, _secrets: Any) -> ParityModel:
        return ParityModel(
            run_id=bound.run_id,
            operation_id=bound.operation_id,
            log=model_log,
            script=script,
            configuration_digest=bound.erc_digest,
            attempt=bound.operation_attempt,
            slots=tuple(
                (slot.slot_name, slot.logical_path)
                for slot in bound.workspace.slot_bindings
                if slot.access == "exclusive_write"
            ),
        )

    return replace(
        base,
        model_factories={**base.model_factories, technical.binding.model.ref.digest: parent},
    )


# --- Claude Agent SDK (the fixture SDK client) -------------------------------------------------


_RESULT = {
    "type": "result",
    "subtype": "success",
    "duration_ms": 1200,
    "duration_api_ms": 1100,
    "is_error": False,
    "num_turns": 1,
    "stop_reason": "end_turn",
    "total_cost_usd": 0.001,
    "usage": {
        "input_tokens": 300,
        "output_tokens": 40,
        "cache_read_input_tokens": 0,
        "cache_creation_input_tokens": 0,
    },
    "terminal_reason": "completed",
    "permission_denials": [],
}


class ResponderClient(FixtureClient):
    """FIXTURE `ClaudeSDKClient`: each turn is computed by `lane_reply` from the lease."""

    def __init__(
        self,
        script: ParityScript,
        options: ClaudeAgentOptions,
        environment: Mapping[str, str],
        session_id: str,
    ) -> None:
        init = {
            "type": "system",
            "subtype": "init",
            "session_id": session_id,
            "uuid": f"init-{session_id}",
            "model": "claude-sonnet-4-5",
            "tools": ["Bash", "Read", "Write"],
            "mcp_servers": [],
            "permissionMode": "default",
            "cwd": "<workspace>",
            "apiKeySource": "none",
        }
        super().__init__(
            FixtureScript(
                name="mp20-responder",
                marker={"recorded": False, "note": "FIXTURE responder"},
                records=[ScriptRecord(turn=None, session=True, message=init)],
            ),
            options,
            environment,
        )
        self.parity = script
        self.session_ref = session_id

    async def query(
        self, prompt: str | AsyncIterable[dict[str, Any]], session_id: str = "default"
    ) -> None:
        del session_id
        if isinstance(prompt, str):
            uuid, text = None, prompt
        else:
            messages = [message async for message in prompt]
            uuid = messages[0].get("uuid")
            text = str(messages[0]["message"]["content"])
        self.sent.append((uuid, text))
        self.turns_played += 1
        self.interrupted = False
        self.held.clear()
        self.release.clear()
        root = Path(str(self.options.cwd))
        reply = lane_reply(self.parity, "claude_agent_sdk", root, text)
        await asyncio.to_thread(_write, root, reply.files)
        turn = self.turns_played
        assistant = {
            "type": "assistant",
            "uuid": f"a-{self.session_ref}-{turn}",
            "session_id": self.session_ref,
            "parent_tool_use_id": None,
            "message": {
                "id": f"msg-{turn}",
                "role": "assistant",
                "model": "claude-sonnet-4-5",
                "content": [{"type": "text", "text": reply.final}],
                "stop_reason": "end_turn",
                "usage": {"input_tokens": 300, "output_tokens": 40},
            },
        }
        result = {
            **_RESULT,
            "session_id": self.session_ref,
            "uuid": f"r-{self.session_ref}-{turn}",
            "result": reply.final,
        }
        records = [
            ScriptRecord(turn=turn, message=assistant, hold=reply.hold),
            ScriptRecord(turn=turn, message=result),
            # Played only when the held turn is interrupted (the SDK's aborted result).
            ScriptRecord(
                turn=turn,
                after_interrupt=True,
                message={
                    **result,
                    "uuid": f"r-{self.session_ref}-{turn}-interrupted",
                    "result": "interrupted",
                    "terminal_reason": "aborted_tools",
                },
            ),
        ]
        self._player = asyncio.create_task(self._play(records))


@dataclass
class ResponderClientFactory(FixtureClientFactory):
    parity: ParityScript | None = None

    def create(
        self, options: ClaudeAgentOptions, *, environment: Mapping[str, str]
    ) -> FixtureClient:
        assert self.parity is not None
        client = ResponderClient(
            self.parity, options, environment, session_id=f"sess-mp20-{uuid4().hex[:12]}"
        )
        self.clients.append(client)
        self.created.set()
        return client


# --- Codex (the fixture app-server) ------------------------------------------------------------


@dataclass
class ResponderAppServer(FixtureAppServer):
    parity: ParityScript | None = None
    hold_next: bool = False

    async def _run_turn(self, thread_id: str, turn_id: str) -> None:
        assert self.parity is not None
        thread = self.disk.threads[thread_id]
        root = Path(str(thread["cwd"]))
        turn = self._turn_record(thread_id, turn_id)
        text = "".join(
            str(part.get("text", ""))
            for item in turn["items"]
            if item.get("type") == "userMessage"
            for part in item.get("content", ())
        )
        reply = lane_reply(self.parity, "codex", root, text)
        await asyncio.to_thread(_write, root, reply.files)
        steps: list[dict[str, Any]] = []
        if reply.hold:
            steps.append({"step": "hold"})
        steps += [
            {
                "step": "item",
                "item": {"type": "agentMessage", "text": reply.final, "phase": "final_answer"},
            },
            {"step": "usage", "input": 300, "output": 40},
            {"step": "complete", "status": "completed"},
        ]
        self.scripts.insert(0, steps)
        await super()._run_turn(thread_id, turn_id)


@dataclass
class ResponderLauncher(FixtureLauncher):
    parity: ParityScript | None = None

    async def launch(self, spec: LaunchSpec) -> LaunchedAppServer:
        client, remote = channel_pair()
        server = ResponderAppServer(disk=self.disk, scripts=[], parity=self.parity)
        task = asyncio.create_task(server.serve(remote), name="mp20-codex-app-server")
        connection = AppServerConnection(client, request_timeout_s=self.request_timeout_s)
        await connection.open()
        self.launches.append(FixtureLaunch(server, client, spec, task, connection))
        self.launched.set()
        return LaunchedAppServer(connection=connection, versions=dict(self.versions))


# --- Cursor local (the fixture bridge) -----------------------------------------------------------

CURSOR_MODEL = "composer-2"  # the model the recorded Cursor fixtures report (FIXTURE)
_CURSOR_USAGE = {"inputTokens": 300, "outputTokens": 40, "totalTokens": 340}


class ResponderBridge(ReplayBridge):
    """FIXTURE `cursor-sdk` bridge: each send's run is computed by `lane_reply` from the lease
    and appended to the replay records (events, run state, the files the agent writes)."""

    async def send(self, agent_id: str, text: str, *, idempotency_key: str) -> str:
        launcher = cast(ResponderBridgeLauncher, self.launcher)
        prior = launcher.sent_runs.get(idempotency_key)
        run_id = await super().send(agent_id, text, idempotency_key=idempotency_key)
        if prior is None:
            assert launcher.parity is not None
            reply = lane_reply(launcher.parity, "cursor_local", self.workspace, text)
            launcher.script_run(run_id, agent_id, reply)
        return run_id


@dataclass
class ResponderBridgeLauncher(ReplayBridgeLauncher):
    parity: ParityScript | None = None

    async def launch(self, *, workspace: Path, state_root: Path) -> ReplayBridge:
        self.launches.append((workspace, state_root))
        return ResponderBridge(self, workspace, state_root)

    def script_run(self, run_id: str, agent_id: str, reply: LaneReply) -> None:
        ids = {"agentId": agent_id, "runId": run_id}
        envelopes: list[dict[str, Any]] = [
            {
                "sdkMessage": {
                    "type": "system",
                    "subtype": "init",
                    **ids,
                    "model": {"id": CURSOR_MODEL},
                }
            },
            {"sdkMessage": {"type": "status", **ids, "status": "RUNNING"}},
            {
                "sdkMessage": {
                    "type": "assistant",
                    **ids,
                    "message": {
                        "role": "assistant",
                        "content": [{"type": "text", "text": reply.final}],
                    },
                }
            },
            {"interactionUpdate": {"type": "turn-ended", "usage": _CURSOR_USAGE}},
            {
                "result": {
                    "result": {
                        **ids,
                        "status": "finished",
                        "result": reply.final,
                        "durationMs": 1200,
                        "model": {"id": CURSOR_MODEL},
                        "usage": _CURSOR_USAGE,
                    }
                }
            },
            {"done": {}},
        ]
        self.records.extend(
            {"kind": "event", "run": run_id, "offset": str(offset), "envelope": envelope}
            for offset, envelope in enumerate(envelopes, start=1)
        )
        self.records.append(
            {
                "kind": "run_state",
                "run": run_id,
                "status": "finished",
                "result": reply.final,
                "duration_ms": 1200,
                "model": CURSOR_MODEL,
                "usage": {"input_tokens": 300, "output_tokens": 40, "total_tokens": 340},
            }
        )
        self.records.extend(
            {"kind": "workspace_write", "run": run_id, "path": path, "content": content.decode()}
            for path, content in reply.files.items()
        )


def cursor_bridge(script: ParityScript) -> ResponderBridgeLauncher:
    return ResponderBridgeLauncher(
        records=[
            {"kind": "meta", "synthetic": True, "agent_id": "agent-mp20", "run_id": "run-mp20-1"},
            {
                "kind": "usage",
                "input_tokens": 300,
                "output_tokens": 40,
                "total_tokens": 340,
                "cost_micros_usd": None,
            },
        ],
        parity=script,
    )


# --- Cursor cloud (the fixture Cloud Agents API) -------------------------------------------------


@dataclass
class ResponderCloudApi(FakeCloudApi):
    """FIXTURE Cloud Agents API v1: a run's reply is computed by `lane_reply` from the run
    branch the lane published (cloned from the bare remote at the agent's `startingRef`), its
    stream carries that reply, and the agent's artifacts are the files it wrote."""

    parity: ParityScript | None = None
    replies: dict[str, LaneReply] = field(default_factory=dict)
    branches: dict[str, str] = field(default_factory=dict)

    def _register_run(self, run: dict[str, Any], body: dict[str, Any]) -> None:
        super()._register_run(run, body)
        assert self.parity is not None
        agent = self.agents[run["agentId"]]
        (repo,) = body.get("repos") or agent["repos"]
        branch = str(repo["startingRef"])
        prompt = str((body.get("prompt") or {}).get("text") or "")
        with tempfile.TemporaryDirectory(prefix="mp20-cursor-cloud-") as scratch:
            work = Path(scratch) / "clone"
            git(
                "clone",
                "--quiet",
                "--branch",
                branch,
                str(self.remote),
                str(work),
                cwd=Path(scratch),
            )
            reply = lane_reply(self.parity, "cursor_cloud", work, prompt)
        self.replies[run["id"]] = reply
        self.branches[run["id"]] = branch
        result = {
            "runId": run["id"],
            "status": "FINISHED",
            "text": reply.final,
            "durationMs": 1200,
            "git": {"branches": [{"repoUrl": str(self.remote), "branch": branch, "prUrl": None}]},
        }
        self.run_events[run["id"]] = [
            SseEvent("status", json.dumps({"runId": run["id"], "status": "CREATING"})),
            SseEvent("status", json.dumps({"runId": run["id"], "status": "RUNNING"}), "1"),
            SseEvent("assistant", json.dumps({"text": reply.final}), "2"),
            SseEvent(
                "interaction_update",
                json.dumps({"type": "turn-ended", "usage": _CURSOR_USAGE}),
                "3",
            ),
            SseEvent("result", json.dumps(result), "4"),
            SseEvent("done", "{}", "5"),
        ]

    def _finish(self, agent: dict[str, Any], run: dict[str, Any]) -> None:
        agent["status"] = "IDLE"
        if run.get("status") == "CANCELLED":
            return
        reply = self.replies.get(run["id"])
        run.update(
            {
                "status": "FINISHED",
                "durationMs": 1200,
                "result": reply.final if reply is not None else "",
                "git": {
                    "branches": [
                        {"repoUrl": str(self.remote), "branch": self.branches.get(run["id"], "")}
                    ]
                },
            }
        )

    def _artifacts(self, agent_id: str) -> dict[str, bytes]:
        files: dict[str, bytes] = {}
        for run_id, run in self.runs.items():
            reply = self.replies.get(run_id)
            if run.get("agentId") == agent_id and reply is not None:
                files.update(reply.files)
        return files

    def handle(self, request: httpx.Request) -> httpx.Response:
        url = request.url
        if url.host == "s3.fake.invalid":
            self.requests.append(request)
            assert "authorization" not in {key.lower() for key in request.headers}
            self.downloads.append(url.params["path"])
            return httpx.Response(
                200, content=self._artifacts(url.params["agent"])[url.params["path"]]
            )
        parts = url.path.strip("/").split("/")
        if parts[:2] == ["v1", "agents"] and len(parts) >= 4 and parts[2] in self.agents:
            agent_id, rest = parts[2], parts[3:]
            if rest == ["artifacts"] and request.method == "GET":
                self.requests.append(request)
                assert request.headers["Authorization"] == f"Bearer {CURSOR_API_KEY}"
                items = [
                    {"path": path, "sizeBytes": len(content), "updatedAt": "2026-10-09T12:00:00Z"}
                    for path, content in self._artifacts(agent_id).items()
                ]
                return httpx.Response(200, json={"items": items})
            if rest == ["artifacts", "download"] and request.method == "GET":
                self.requests.append(request)
                assert request.headers["Authorization"] == f"Bearer {CURSOR_API_KEY}"
                path = url.params["path"]
                return httpx.Response(
                    200,
                    json={
                        "url": f"{PRESIGNED}?agent={agent_id}&path={path}&sig=abc",
                        "expiresAt": "later",
                    },
                )
            if rest == ["usage"] and request.method == "GET":
                self.requests.append(request)
                runs = [
                    {"id": run_id, "usage": _CURSOR_USAGE}
                    for run_id, run in self.runs.items()
                    if run.get("agentId") == agent_id
                ]
                total = {key: value * len(runs) for key, value in _CURSOR_USAGE.items()}
                return httpx.Response(200, json={"totalUsage": total, "runs": runs})
        return super().handle(request)


def cursor_cloud_api(script: ParityScript, remote: Path) -> ResponderCloudApi:
    record = {
        "run": {"id": "run-mp20-cloud-1"},
        "retention_seconds": 3600,
        "artifacts": {},
        "usage": {"totalUsage": _CURSOR_USAGE, "runs": []},
        "agent_branch_commit": {"path": "unused", "content": ""},
    }
    return ResponderCloudApi(events=[], record=record, remote=remote, parity=script)


# --- the stack -----------------------------------------------------------------------------------


@dataclass
class ParityStack:
    stack: ProductionStack
    script: ParityScript
    author: ManifestLaunchInputAuthor
    app: FastAPI
    lane_queues: dict[str, str]
    claude: ResponderClientFactory
    codex: ResponderLauncher
    leases: Path
    cursor_local: ResponderBridgeLauncher | None = None
    cursor_cloud: ResponderCloudApi | None = None


def _harnesses(
    tmp_path: Path,
    script: ParityScript,
    inputs: _PayloadInputs,
    fences: PostgresStopFenceRepository,
    outputs: WorkspaceCandidateLaneOutputs,
) -> tuple[ClaudeAgentSdkHarness, ResponderClientFactory, CodexLocalHarness, ResponderLauncher]:
    projections = RenderedProjectionSource(static_rows(()))
    claude_clients = ResponderClientFactory(
        FixtureScript(name="unused", marker={"recorded": False}, records=[]), parity=script
    )
    claude = ClaudeAgentSdkHarness(
        clients=claude_clients,
        workspaces=FixtureWorkspace(tmp_path / "leases" / "claude"),
        projections=projections,
        auth=StaticAuthAdmitter(
            fixture_admission().model_copy(update={"profile_id": AUTH_PROFILES["claude_agent_sdk"]})
        ),
        child_environment=provider_child_environment,
        environ=FIXTURE_ENVIRON,
        fences=fences,
        intents=InMemoryHookIntentLedger(),
        inputs=inputs,
        settings=ClaudeLaneSettings(
            require_executables=False, drain_timeout_s=5.0, init_timeout_s=5.0
        ),
        outputs=outputs,
    )
    launcher = ResponderLauncher(parity=script)
    codex = CodexLocalHarness(
        launcher=launcher,
        leaser=TmpLeaser(tmp_path / "leases" / "codex"),
        projections=projections,
        artifacts=MemoryArtifacts(),
        auth=StaticAuth(codex_admission(profile_id=AUTH_PROFILES["codex"])),
        settings=fixture_settings(tmp_path),
        child_environment=provider_child_environment,
        inputs=inputs,
        outputs=outputs,
    )
    return claude, claude_clients, codex, launcher


CURSOR_LOCAL_PATH = "/srv/repos/mission-control-fixture"  # what `lane_environment` names
CURSOR_ENVIRONMENT = "cursor-env.fixture"


def cursor_lanes(
    bindings: ManifestLaunchBindings,
    queues: Mapping[str, str],
    *,
    local_repository: Path,
    remote: Path,
) -> dict[str, CursorLocalLane | CursorCloudLane]:
    """The example file's Cursor entries on the test queues and the stack's scaffold refs, with
    the fixture model, auth/host/environment names `lane_environment` selects, and the
    manifest repository mapped to the tmp repository (local) and tmp bare remote (cloud)."""

    from tests.integration.temporal.test_manifest_v2_provider_launch import BINDINGS_EXAMPLE

    document = json.loads(BINDINGS_EXAMPLE.read_text(encoding="utf-8"))
    scaffold = bindings.deep_agents
    shared = {
        "agent_profile_ref": scaffold.agent_profile_ref.model_dump(mode="json"),
        "tracing_policy_ref": scaffold.tracing_policy_ref,
        "sensitive_data_policy_ref": scaffold.sensitive_data_policy_ref,
        "snapshot_policy_ref": scaffold.snapshot_policy_ref,
        "workspace": scaffold.workspace.model_dump(mode="json"),
        "model_profiles": {
            "frontier.default": {"profile": "frontier.default", "model_id": CURSOR_MODEL}
        },
        "auth_profiles": {"cursor-owner": {"profile": "cursor-owner", "billing_mode": "unknown"}},
    }
    local = {
        **document["providers"]["cursor_local"],
        **shared,
        "task_queue": queues["cursor_local"],
        "repositories": {
            CURSOR_LOCAL_PATH: {"repo_url": str(local_repository), "base_refs": ["main"]}
        },
    }
    cloud = {
        **document["providers"]["cursor_cloud"],
        **shared,
        "task_queue": queues["cursor_cloud"],
        "repositories": {REPO["url"]: {"repo_url": str(remote), "base_refs": ["main"]}},
        "environments": {
            CURSOR_ENVIRONMENT: {
                "environment": {
                    "kind": "provider_hosted",
                    "provider": "cursor",
                    "environment_ref": CURSOR_ENVIRONMENT,
                    "expected_revision": "sha256:" + "0" * 64,
                    "setup_pin": "cursor-setup@1",
                },
                "options": {"auto_create_pr": False},
            }
        },
        "v1_environment": CURSOR_ENVIRONMENT,
    }
    return {
        "cursor_local": CursorLocalLane.model_validate(local),
        "cursor_cloud": CursorCloudLane.model_validate(cloud),
    }


def _cursor_harnesses(
    tmp_path: Path,
    script: ParityScript,
    inputs: _PayloadInputs,
    fences: PostgresStopFenceRepository,
    outputs: WorkspaceCandidateLaneOutputs,
    catalog: PostgresDefinitionRepository,
    remote: Path,
) -> tuple[CursorLocalHarness, ResponderBridgeLauncher, CursorCloudHarness, ResponderCloudApi]:
    """The real Cursor harnesses over the FIXTURE bridge and Cloud API. The projection source
    is the composed one (`deployment_composition`): `CatalogRows` over the deployment catalog,
    Kernel Hooks on `cursor_local`, none on `cursor_cloud`."""

    hooks = HookCallbackService(
        tokens=InMemoryHookTokenStore(),
        intents=InMemoryHookIntentLedger(),
        mapper=CursorHookMapper(),
        fences=KernelHookFenceGate(fences),
        frames=InMemoryFrameStore(),
        context_reader=hook_context_index,
    )
    bridge = cursor_bridge(script)
    lease_root = tmp_path / "leases" / "cursor_local"
    local = CursorLocalHarness(
        launcher=bridge,
        leaser=GitWorktreeLeaser(InMemoryWorkspaceLeaseStore(), lease_root=lease_root),
        projections=RenderedProjectionSource(CatalogRows(catalog)),
        hooks=hooks,
        artifacts=CursorArtifacts(),
        inputs=inputs,
        outputs=outputs,
        settings=CursorLocalSettings(lease_root=lease_root),
    )
    hooks.set_stop_policy(local)
    api = cursor_cloud_api(script, remote)
    cloud = CursorCloudHarness(
        client=CloudAgentsClient(SecretStr(CURSOR_API_KEY), transport=api.transport()),
        # TEST-LOCAL STAND-IN (reported delta): later units of a run reach the run branch.
        publisher=GitBranchPublisher(tmp_path / "cursor-cloud-mirrors"),
        projections=RenderedProjectionSource(CatalogRows(catalog), kernel_hooks=()),
        artifacts=CursorArtifacts(),
        inputs=inputs,
        outputs=outputs,
        expired_poll_interval_s=0.01,
    )
    return local, bridge, cloud, api


def isolate_temporal(monkeypatch: Any) -> None:
    """Opt-in test isolation (`MP20_TEMPORAL_PORT`): the production-stack fixture starts (or
    joins) a dev server on a fixed port and polls fixed coordinator queues, so a concurrent run
    of another suite on that port takes this run's workflow tasks against its own database.
    A distinct port gives this run its own dev server."""

    port = os.environ.get("MP20_TEMPORAL_PORT")
    if not port:
        return
    start = production_stack_module.start_local

    async def start_local(database: Path, *, port_: int = int(port)) -> Any:
        return await start(database, port=port_)

    monkeypatch.setattr(production_stack_module, "TEMPORAL_PORT", int(port))
    monkeypatch.setattr(production_stack_module, "start_local", start_local)


@asynccontextmanager
async def open_parity_stack(tmp_path: Path, monkeypatch: Any) -> AsyncIterator[ParityStack]:
    """The production stack with Deep Agents, Claude, Codex and Cursor lanes, each Session Lane
    on its own task queue served by the production `LaneTurnService` (see the module docstring)."""

    script = ParityScript(request_scope=SCOPE)
    technical = technical_binding()
    model_log: list[dict[str, Any]] = []
    components = parity_components(technical, script, model_log)
    queues = {
        profile: f"mp20-{profile.replace('_', '-')}-{uuid4().hex[:8]}"
        for profile in (*SESSION_PROFILES, *CURSOR_PROFILES)
    }
    base = fixture_bindings(task_queue=AGENT_COGNITIVE_QUEUE)
    # The Cursor target repository (cursor_local leases worktrees from it) and the bare remote
    # the cursor_cloud lane publishes run branches to and the fixture cloud agent clones.
    local_repository = make_repository(tmp_path / "cursor-repository")
    remote = make_remote(tmp_path / "cursor-remote")
    bindings = ManifestLaunchBindings.model_validate(
        {
            **base.model_dump(mode="python"),
            "schema_version": LAUNCH_BINDINGS_SCHEMA_V2,
            "model_profiles": {"frontier.default": technical.binding.model},
            "sandbox_profiles": {"research.standard": technical.binding.sandbox},
            "providers": ProviderLanes(
                **{
                    profile: provider_lane(base, profile, queues[profile])
                    for profile in SESSION_PROFILES
                },
                **cursor_lanes(base, queues, local_repository=local_repository, remote=remote),
            ),
        }
    )
    bindings_path = tmp_path / "manifest-launch-bindings.json"
    bindings_path.write_text(bindings.model_dump_json(indent=2), encoding="utf-8")
    isolate_temporal(monkeypatch)
    async with open_postgres_production_stack(
        root=tmp_path,
        monkeypatch=monkeypatch,
        technical_override=technical,
        components=components,
        model_log=model_log,
        extra_environment={
            "MANIFEST_LAUNCH_BINDINGS_PATH": str(bindings_path),
            "CAPABILITY_PINS_PATH": str(host_pins(tmp_path / "capability-pins.json")),
            # Implemented but unqualified profiles: a local proof admits them only under the
            # documented flag (no live drill exists; nothing here qualifies a profile).
            "MISSION_CONTROL_ALLOW_UNQUALIFIED_LANES": "true",
        },
    ) as production:
        unregister = register_post_append_hook(HOOK_NAME, ChainReleaseHook())
        try:
            run_control = cast(RunControlService, api.state.run_control_service)
            register_manifest_admission_policies(
                cast(AdmissionPolicyRegistry, api.state.admission_policy_registry)
            )
            author = compose_manifest_launch_inputs(
                production.worker_pool,
                settings=production.settings,
                run_control=run_control,
                control_plane=production.control_plane,
                additional=components,
            )
            assert author is not None
            definitions, search = await fast_track_catalog()
            catalog = PostgresDefinitionRepository(
                production.worker_pool,
                catalog_scope=production.settings.mission_control_catalog_scope or "",
            )
            programs = ManifestProgramCompiler(catalog, ExtensionRegistry(), InMemoryPayloadStore())
            compiler = ManifestCompileService(
                definitions=definitions, search=search, programs=programs
            )
            lifecycle = MissionManifestService(
                compiler=compiler,
                request_scope=SCOPE,
                lifecycle=ManifestSubmitService(
                    compiler=compiler,
                    programs=programs,
                    run_control=run_control,
                    submissions=PostgresManifestSubmissionRepository(production.worker_pool),
                    request_scope=SCOPE,
                    launches=cast(RunLaunchService, api.state.run_launch_service),
                    launch_inputs=author,
                    subscriptions=SubscriptionService(
                        PostgresSubscriptionStore(production.worker_pool, SCOPE)
                    ),
                ),
            )
            operation = production.factory.operation
            assert operation is not None
            fences = PostgresStopFenceRepository(production.worker_pool)
            # The MP-20 custody port over the deployment's own candidate capture service and
            # workspace materializer (what the proposed composition delta wires).
            outputs = WorkspaceCandidateLaneOutputs(
                workspaces=operation.service._sandbox, candidates=operation.candidates
            )
            claude, claude_clients, codex, launcher = _harnesses(
                tmp_path, script, _PayloadInputs(operation.payloads), fences, outputs
            )
            # The FIXTURE harnesses join the production boundary's own registry: the real
            # ones are composed only on an opted-in Linux/WSL worker.
            registry: LaneRegistry = operation.service._lanes
            registry.register(claude)
            registry.register(codex)
            cursor_local, bridge, cursor_cloud, cloud_api = _cursor_harnesses(
                tmp_path,
                script,
                _PayloadInputs(operation.payloads),
                fences,
                outputs,
                catalog,
                remote,
            )
            # The deployment composes Cursor lanes when a Cursor credential is bound (real
            # harnesses over the pinned SDK and Cloud API, or unqualified stubs): the FIXTURE
            # responder harnesses take their place in the same registry.
            for harness in (cursor_local, cursor_cloud):
                profile = harness.describe().lane_profile
                registry._harnesses[profile] = harness
                registry._describes[profile] = harness.describe()
            # The production operation activities, whose `LaneTurnService` is the deployment's
            # own (mailbox, injections, Stop Fence, MP-12 continuation coordinator, frame
            # facts), serve each lane queue: only the provider harness is a fixture.
            activities = production.composition.operation
            async with AsyncExitStack() as workers:
                for queue in queues.values():
                    await workers.enter_async_context(
                        Worker(
                            production.client,
                            task_queue=queue,
                            activities=agent_cognitive_activities(activities),
                        )
                    )
                yield ParityStack(
                    stack=production,
                    script=script,
                    author=author,
                    app=missions_app(lifecycle),
                    lane_queues=queues,
                    claude=claude_clients,
                    codex=launcher,
                    leases=tmp_path / "leases",
                    cursor_local=bridge,
                    cursor_cloud=cloud_api,
                )
            await launcher.shutdown()
        finally:
            unregister()
