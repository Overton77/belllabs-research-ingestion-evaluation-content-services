from __future__ import annotations

import json
import os
import re
import shutil
import uuid
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import Any, cast

import docker
from agents import AgentOutputSchema, ModelSettings, RunConfig, Runner, ToolExecutionConfig
from agents.sandbox import SandboxAgent, SandboxRunConfig
from agents.sandbox.capabilities import Filesystem, Shell
from agents.sandbox.sandboxes import DockerSandboxClient, DockerSandboxClientOptions
from docker.types import Mount as DockerMount
from openai.types.shared.reasoning import Reasoning
from pydantic import Field, create_model

from app.application.schema_context_selection import AgentRunOutput
from app.application.schema_workspace import workspace_profile_paths
from app.domain.schema_context.canonicalization import sha256_digest, write_json
from app.domain.schema_context.contracts import (
    GraphReconciliationEvidence,
    PropertyIntentHint,
    SchemaContextSelection,
    SchemaContextSelectionDraft,
    SchemaContextSelectionRequest,
    SchemaSelectionReview,
)
from app.experiments.schema_context_selection.prompts import (
    QUERY_PLANNER_INSTRUCTIONS,
    REVIEWER_INSTRUCTIONS,
    SELECTOR_INSTRUCTIONS,
)
from app.experiments.schema_context_selection.workspace import sandbox_manifest


def _usage(result: Any) -> dict[str, int]:
    usage = result.context_wrapper.usage
    return {
        "input_tokens": int(usage.input_tokens),
        "output_tokens": int(usage.output_tokens),
        "total_tokens": int(usage.total_tokens),
        "requests": int(usage.requests),
    }


class SandboxAgentHarness:
    def __init__(self, *, model: str, image: str = "python:3.12-slim") -> None:
        self.model = model
        self._docker = docker.from_env()
        self._client = DockerSandboxClient(self._docker)
        self._options = DockerSandboxClientOptions(image=image)

    def close(self) -> None:
        self._docker.close()

    async def _run(
        self,
        *,
        name: str,
        instructions: str,
        output_type: type[Any],
        run_root: Path,
        paths: tuple[str, ...],
        user_input: str,
        max_turns: int,
        tools: list[Any] | None = None,
        workflow_name: str,
    ) -> AgentRunOutput:
        run_attempt = uuid.uuid4().hex[:12]
        bind_workspace = os.name == "nt"
        if bind_workspace:
            workspace_source = _materialize_allowlisted_workspace(
                run_root,
                paths,
                workflow_name=workflow_name,
            )
            session_manifest = sandbox_manifest(workspace_source, ())
            agent_manifest = sandbox_manifest(run_root, paths)
            client: DockerSandboxClient = _ReadOnlyBindDockerSandboxClient(
                self._docker,
                workspace_source=workspace_source,
            )
        else:
            session_manifest = sandbox_manifest(run_root, paths)
            agent_manifest = session_manifest
            client = self._client
        agent = SandboxAgent(
            name=name,
            model=self.model,
            instructions=instructions,
            default_manifest=agent_manifest,
            capabilities=[Filesystem(), Shell()],
            tools=tools or [],
            output_type=AgentOutputSchema(output_type, strict_json_schema=True),
            model_settings=ModelSettings(
                reasoning=Reasoning(effort="low"),
                verbosity="low",
                include_usage=True,
                max_tokens=12_000,
            ),
        )
        session = await client.create(manifest=session_manifest, options=self._options)
        try:
            if bind_workspace:
                # Docker PTY exit detection races on Docker Desktop/Windows and reports even
                # completed `sed`/`ls` commands as indefinitely running. Force the SDK shell
                # capability onto its reliable one-shot exec transport for these sandboxes.
                inner_session = getattr(session, "_inner", None)
                if inner_session is None:
                    raise RuntimeError("sandbox session wrapper has no inner session")
                inner_session.supports_pty = lambda: False
            await session.start()
            if bind_workspace:
                # The files are already present through the read-only Docker bind. Publish the
                # matching declarative manifest so SandboxAgent describes the allowlisted tree
                # without asking the SDK to stream/materialize those files again on Windows.
                session.state = session.state.model_copy(update={"manifest": agent_manifest})
            workspace_root = str(session.state.manifest.root)
            if bind_workspace:
                write_probe = await session.exec(
                    "touch",
                    f"{workspace_root}/.read-only-probe",
                    shell=False,
                    user="root",
                )
                if write_probe.ok():
                    await session.exec(
                        "rm",
                        "-f",
                        f"{workspace_root}/.read-only-probe",
                        shell=False,
                        user="root",
                    )
                    raise RuntimeError("sandbox workspace bind is unexpectedly writable")
            else:
                made_read_only = await session.exec(
                    "chmod",
                    "-R",
                    "a-w",
                    "--",
                    workspace_root,
                    shell=False,
                    user="root",
                )
                if not made_read_only.ok():
                    diagnostic = made_read_only.stderr.decode("utf-8", errors="replace")[:500]
                    raise RuntimeError(
                        "failed to enforce read-only sandbox workspace: " + diagnostic
                    )
            file_paths = tuple(path for path in paths if (run_root / path).is_file())
            readable = await session.exec(
                "wc",
                "-c",
                *file_paths,
                shell=False,
            )
            preflight = {
                "workflow_name": workflow_name,
                "exit_code": readable.exit_code,
                "stdout": readable.stdout.decode("utf-8", errors="replace")[:12000],
                "stderr": readable.stderr.decode("utf-8", errors="replace")[:4000],
                "allowlisted_file_count": len(file_paths),
                "read_only_enforced": True,
                "pty_disabled_for_windows_transport": bind_workspace,
            }
            write_json(
                run_root
                / "agent-runs"
                / (f"{_workflow_slug(workflow_name)}-{run_attempt}-workspace-preflight.json"),
                preflight,
            )
            if not readable.ok():
                raise RuntimeError("sandbox allowlisted files failed readability preflight")
            result = await Runner.run(
                agent,
                user_input,
                max_turns=max_turns,
                run_config=RunConfig(
                    tracing_disabled=False,
                    trace_include_sensitive_data=False,
                    workflow_name=workflow_name,
                    sandbox=SandboxRunConfig(
                        client=client,
                        options=self._options,
                        session=session,
                    ),
                    tool_execution=ToolExecutionConfig(max_function_tool_concurrency=1),
                ),
            )
            write_json(
                run_root
                / "agent-runs"
                / f"{_workflow_slug(workflow_name)}-{run_attempt}-transcript.json",
                _run_diagnostics(result, workflow_name=workflow_name),
            )
            return AgentRunOutput(output=result.final_output, usage=_usage(result))
        finally:
            await client.delete(session)

    async def select(
        self, run_root: Path, *, revision_feedback: str | None = None
    ) -> AgentRunOutput:
        instructions = SELECTOR_INSTRUCTIONS
        if revision_feedback:
            instructions += (
                "\nThis is the single allowed revision round. Produce revision 2 with "
                f"parent_selection_id from the prior draft. Bounded findings:\n{revision_feedback}"
            )
        selection_output_type = _selection_draft_output_type(run_root)
        run = await self._run(
            name="Schema context selector",
            instructions=instructions,
            output_type=selection_output_type,
            run_root=run_root,
            paths=("inputs/request.json", "inputs/report.md")
            + workspace_profile_paths(run_root, "selection-candidates"),
            user_input="Inspect the mounted workspace and produce the purpose-bound selection.",
            max_turns=20,
            workflow_name="SchemaContextSelectionWorkflow.selector",
        )
        draft = run.output
        if not isinstance(draft, SchemaContextSelectionDraft):
            draft = SchemaContextSelectionDraft.model_validate(draft)
        request = SchemaContextSelectionRequest.model_validate_json(
            (run_root / "inputs" / "request.json").read_text(encoding="utf-8")
        )
        revision = 2 if revision_feedback else 1
        parent_selection_id: str | None = None
        if revision == 2:
            prior = SchemaContextSelection.model_validate_json(
                (run_root / "selection" / "draft.json").read_text(encoding="utf-8")
            )
            parent_selection_id = prior.selection_id
        semantic_payload = draft.model_dump(mode="json")
        semantic_digest = sha256_digest(
            {
                "request_id": request.request_id,
                "revision": revision,
                "semantic_selection": semantic_payload,
            }
        )
        hints = tuple(
            PropertyIntentHint(
                node_type=hint.node_type,
                properties=tuple(sorted(hint.properties)),
            )
            for hint in sorted(draft.property_intent_hints, key=lambda item: item.node_type)
        )
        selection = SchemaContextSelection(
            selection_id=(f"selection-{request.request_id}-r{revision}-{semantic_digest[7:19]}"),
            revision=revision,
            purpose=request.purpose,
            schema_definition_ref=request.schema_definition_ref,
            schema_definition_digest=request.schema_definition_digest,
            catalog_digest=request.catalog_digest,
            report_ref=request.report_ref,
            report_digest=request.report_digest,
            selected_node_types=tuple(
                sorted(_enum_string(value) for value in draft.selected_node_types)
            ),
            selected_relationship_types=tuple(
                sorted(_enum_string(value) for value in draft.selected_relationship_types)
            ),
            property_intent_hints=hints,
            coverage_obligations=request.coverage_obligations,
            rationale=draft.rationale,
            evidence_locators=draft.evidence_locators,
            explicit_exclusions=draft.explicit_exclusions,
            unresolved_mappings=draft.unresolved_mappings,
            near_miss_candidates=draft.near_miss_candidates,
            parent_selection_id=parent_selection_id,
            created_at=datetime.now(UTC),
        )
        return AgentRunOutput(output=selection, usage=run.usage)

    async def review(self, run_root: Path, *, retry_reason: str | None = None) -> AgentRunOutput:
        draft = json.loads((run_root / "selection" / "draft.json").read_text(encoding="utf-8"))
        selection_id = str(draft["selection_id"])
        user_input = (
            "Independently review the mounted draft without mutating it. "
            f"The authoritative selection_id is `{selection_id}`; return it byte-for-byte."
        )
        if retry_reason:
            user_input += f" The prior review was discarded by the host: {retry_reason}"
        return await self._run(
            name="Independent schema selection reviewer",
            instructions=REVIEWER_INSTRUCTIONS,
            output_type=SchemaSelectionReview,
            run_root=run_root,
            paths=(
                "inputs/request.json",
                "inputs/report.md",
                "selection/draft.json",
                "selection/deterministic-validation.json",
            )
            + workspace_profile_paths(run_root, "selection-candidates"),
            user_input=user_input,
            max_turns=12,
            workflow_name="SchemaContextSelectionWorkflow.reviewer",
        )

    async def plan_queries(
        self,
        run_root: Path,
        *,
        execute_tool: Any,
        max_turns: int,
        retry_reason: str | None = None,
    ) -> AgentRunOutput:
        user_input = "Execute the minimum bounded read intents needed and return final evidence."
        if retry_reason:
            user_input += (
                " This is a mandatory retry because the prior planner attempt violated a "
                f"host invariant: {retry_reason}. Call execute_read_intent before returning."
            )
        return await self._run(
            name="Graph reconciliation query planner",
            instructions=QUERY_PLANNER_INSTRUCTIONS,
            output_type=GraphReconciliationEvidence,
            run_root=run_root,
            paths=(
                "inputs/report.md",
                "selection/accepted.json",
                "selection/expanded-slice.json",
                "selection/operation-projection.json",
                "selection/query-brief.json",
                "schema/runtime/live-schema.json",
                "schema/runtime/live-indexes.json",
            ),
            user_input=user_input,
            max_turns=max_turns,
            tools=[execute_tool],
            workflow_name="ReportGraphReconciliationWorkflow.query_planner",
        )


def _materialize_allowlisted_workspace(
    run_root: Path,
    paths: tuple[str, ...],
    *,
    workflow_name: str,
) -> Path:
    slug = _workflow_slug(workflow_name)
    destination_root = run_root / ".sandbox-views" / f"{slug}-{uuid.uuid4().hex[:12]}"
    destination_root.mkdir(parents=True, exist_ok=False)
    resolved_run_root = run_root.resolve()
    for relative in sorted(set(paths)):
        source = (run_root / relative).resolve()
        try:
            source.relative_to(resolved_run_root)
        except ValueError as error:
            raise ValueError("sandbox source escaped the run artifact root") from error
        contains_symlink = source.is_dir() and any(path.is_symlink() for path in source.rglob("*"))
        if source.is_symlink() or contains_symlink:
            raise ValueError(f"sandbox source contains a forbidden symlink: {relative}")
        destination = destination_root / relative
        if source.is_dir():
            shutil.copytree(source, destination, dirs_exist_ok=True)
        elif source.is_file():
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, destination)
        else:
            raise FileNotFoundError(source)
    return destination_root.resolve()


def _selection_draft_output_type(run_root: Path) -> type[SchemaContextSelectionDraft]:
    tier0 = json.loads(
        (run_root / "schema" / "overview" / "tier0.json").read_text(encoding="utf-8")
    )
    nodes = tuple(sorted(tier0["node_names"]))
    relationships = tuple(sorted(tier0["relationships"]))
    node_enum = Enum(  # type: ignore[misc]
        "SchemaNodeType",
        {f"NODE_{index}": value for index, value in enumerate(nodes)},
        type=str,
    )
    relationship_enum = Enum(  # type: ignore[misc]
        "SchemaRelationshipType",
        {f"REL_{index}": value for index, value in enumerate(relationships)},
        type=str,
    )
    node_tuple = tuple[node_enum, ...]
    relationship_tuple = tuple[relationship_enum, ...]
    constrained = create_model(
        "CatalogConstrainedSchemaContextSelectionDraft",
        __base__=SchemaContextSelectionDraft,
        selected_node_types=(node_tuple, Field(max_length=16)),
        selected_relationship_types=(relationship_tuple, Field(max_length=24)),
    )
    return cast(type[SchemaContextSelectionDraft], constrained)


def _enum_string(value: object) -> str:
    if isinstance(value, Enum):
        return str(value.value)
    return str(value)


def _workflow_slug(workflow_name: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "-", workflow_name).strip("-").lower()


def _run_diagnostics(result: Any, *, workflow_name: str) -> dict[str, Any]:
    items: list[dict[str, Any]] = []
    for item in result.new_items:
        raw_item = getattr(item, "raw_item", None)
        if raw_item is not None and hasattr(raw_item, "model_dump"):
            payload: Any = raw_item.model_dump(mode="json")
        else:
            payload = str(raw_item)
        items.append(
            {
                "item_type": type(item).__name__,
                "payload": _truncate_diagnostic_value(payload),
            }
        )
    return {
        "workflow_name": workflow_name,
        "items": items,
        "usage": _usage(result),
    }


def _truncate_diagnostic_value(value: Any) -> Any:
    if isinstance(value, str):
        return value[:8000]
    if isinstance(value, dict):
        return {str(key): _truncate_diagnostic_value(child) for key, child in value.items()}
    if isinstance(value, list | tuple):
        return [_truncate_diagnostic_value(child) for child in value]
    return value


class _ReadOnlyBindDockerSandboxClient(DockerSandboxClient):
    """Windows-safe Docker provider using an allowlisted read-only bind workspace."""

    def __init__(self, docker_client: Any, *, workspace_source: Path) -> None:
        super().__init__(docker_client)
        self._workspace_source = workspace_source

    async def _create_container(
        self,
        image: str,
        *,
        manifest: Any = None,
        exposed_ports: tuple[int, ...] = (),
        session_id: uuid.UUID | None = None,
    ) -> Any:
        del session_id
        if not self.image_exists(image):
            self.docker_client.images.pull(image)
        environment = await manifest.environment.resolve() if manifest is not None else None
        create_kwargs: dict[str, Any] = {
            "entrypoint": ["tail"],
            "image": image,
            "detach": True,
            "command": ["-f", "/dev/null"],
            "environment": environment,
            "mounts": [
                DockerMount(
                    target="/workspace",
                    source=str(self._workspace_source),
                    type="bind",
                    read_only=True,
                )
            ],
        }
        if exposed_ports:
            create_kwargs["ports"] = {f"{port}/tcp": ("127.0.0.1", None) for port in exposed_ports}
        return self.docker_client.containers.create(**create_kwargs)
