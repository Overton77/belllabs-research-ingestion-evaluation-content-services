"""RRM-004 persistent recovery stack shared by the crashed worker process and its replacement.

Both processes compose the production operation boundary over the same durable stores:
application PostgreSQL (run control, operation journal, checkpoint lineage via
`compose_postgres_operation_recovery`), a real `AsyncPostgresSaver` in a dedicated schema,
and a content-addressed file payload store (a stand-in for the S3 artifact store that both
processes can reach). Cognition is a real `create_deep_agent` graph with the deterministic
scripted model; every model call is appended to a shared log with the calling process ID.
"""

from __future__ import annotations

import asyncio
import json
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Any
from urllib.parse import quote

import asyncpg
from langchain_core.messages import BaseMessage
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from langgraph.store.memory import InMemoryStore

from app.application.operations.journaled_operation_execution import (
    JournaledOperationExecutionCoordinator,
)
from app.application.operations.operation_execution import (
    InMemoryOperationBindingRepository,
    OperationExecutionService,
)
from app.application.operations.operation_journal import OperationJournalService
from app.application.operations.operation_recovery_composition import (
    OperationRecoveryComposition,
    compose_postgres_operation_recovery,
)
from app.application.operations.postgres_operation_journal import (
    PostgresAtomicOperationJournalRepository,
)
from app.application.run_control.postgres_run_control_repository import PostgresRunControlRepository
from app.application.run_control.service import RunControlService
from app.application.workspaces.artifact_promotion import ArtifactPayloadAddress
from app.domain.operation_execution.contracts import DeepAgentExecutionBinding
from app.domain.operation_execution.errors import WorkspaceDigestMismatch
from app.integrations.agents.deep_agents import (
    DeepAgentRuntimeAdapter,
    ExactComponentRegistry,
    ExactDeepAgentMaterializer,
    StateSandboxFactory,
)
from app.integrations.conformance_operation_runtime import (
    ConformanceAssetVerifier,
    ConformanceBudgetAuthority,
    ConformanceEventSink,
    ConformanceSandbox,
    ConformanceSecretResolver,
)
from tests.fixtures.checkpoint_recovery import AcceptingAuthority, ScriptedRecoveryModel
from tests.unit.operations.test_operation_execution import MCP_DIGEST, SKILL_DIGEST
from tests.unit.run_control.test_run_control import actor
from tests.unit.run_control.test_run_control import service as run_control_service

SAVER_SCHEMA = "rrm004_restart_saver"
WORKFLOW_TASK_QUEUE = "rrm004-operation-workflows"
CLAIMED_BY = "operation-runtime:rrm-004"  # deployment-stable, never per worker


@dataclass(frozen=True)
class StackPaths:
    root: Path

    @property
    def results(self) -> Path:
        return self.root / "results"

    @property
    def model_log(self) -> Path:
        return self.root / "model-calls.jsonl"

    @property
    def crash_marker(self) -> Path:
        return self.root / "crashed-after-checkpoint"


class FileArtifactPayloadStore:
    """Content-addressed payloads on a shared directory (S3 stand-in across processes)."""

    def __init__(self, root: Path) -> None:
        self._root = root
        root.mkdir(parents=True, exist_ok=True)

    async def stage(
        self,
        *,
        artifact_id: str,
        content: bytes,
        content_digest: str,
        media_type: str,
    ) -> ArtifactPayloadAddress:
        del artifact_id, media_type
        if f"sha256:{sha256(content).hexdigest()}" != content_digest:
            raise WorkspaceDigestMismatch("payload bytes do not match their digest")
        path = self._root / content_digest.removeprefix("sha256:")
        if path.exists() and path.read_bytes() != content:
            raise WorkspaceDigestMismatch("content address contains conflicting bytes")
        path.write_bytes(content)
        return ArtifactPayloadAddress(
            object_ref=f"file-artifacts://{path.name}",
            content_digest=content_digest,
            size_bytes=len(content),
        )

    async def retrieve(self, address: ArtifactPayloadAddress) -> bytes:
        path = self._root / address.object_ref.removeprefix("file-artifacts://")
        content = path.read_bytes()
        if (
            f"sha256:{sha256(content).hexdigest()}" != address.content_digest
            or len(content) != address.size_bytes
        ):
            raise WorkspaceDigestMismatch("stored payload does not match its address")
        return content


class LoggedScriptedModel(ScriptedRecoveryModel):
    """The scripted model, recording every call (process, prompts, tools) in a shared log."""

    def _observe(self, messages: list[BaseMessage]) -> tuple[int, int]:
        index, tools = super()._observe(messages)
        human, _ = self.calls[-1]
        log = Path(os.environ["RRM004_MODEL_LOG"])
        with log.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps({"pid": os.getpid(), "human": human, "tools": tools}) + "\n")
        return index, tools


class HangAfterCheckpointSaver(AsyncPostgresSaver):
    """A real `AsyncPostgresSaver` whose worker stalls forever after its N-th checkpoint.

    The checkpoint is durable before the stall; the test then kills the worker process.
    """

    hang_after_put: int | None = None
    crash_marker: Path | None = None
    _puts: int = 0

    async def aput(self, config: Any, checkpoint: Any, metadata: Any, new_versions: Any) -> Any:
        written = await super().aput(config, checkpoint, metadata, new_versions)
        self._puts += 1
        if self._puts == self.hang_after_put:
            assert self.crash_marker is not None
            self.crash_marker.write_text(str(os.getpid()), encoding="utf-8")
            await asyncio.Event().wait()
        return written


@dataclass
class PersistentStack:
    pool: asyncpg.Pool
    saver: HangAfterCheckpointSaver
    run_control: RunControlService
    recovery: OperationRecoveryComposition
    service: OperationExecutionService
    binding: DeepAgentExecutionBinding


def saver_dsn(dsn: str) -> str:
    return f"{dsn}?options=" + quote(f"-c search_path={SAVER_SCHEMA}")


@asynccontextmanager
async def open_persistent_stack(
    dsn: str, paths: StackPaths, *, hang_after_put: int | None = None
) -> AsyncIterator[PersistentStack]:
    from tests.acceptance.control_plane.test_wp_cp_040 import exact_fixture

    os.environ["RRM004_MODEL_LOG"] = str(paths.model_log)
    pool = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=6)
    try:
        run_control, _ = run_control_service(PostgresRunControlRepository(pool))  # type: ignore[arg-type]
        async with HangAfterCheckpointSaver.from_conn_string(saver_dsn(dsn)) as saver:
            saver.hang_after_put = hang_after_put
            saver.crash_marker = paths.crash_marker
            binding, _profile, bundle = exact_fixture()
            model = LoggedScriptedModel()
            adapter = DeepAgentRuntimeAdapter(
                ExactDeepAgentMaterializer(
                    ExactComponentRegistry(
                        model_factories={binding.model.ref.digest: lambda _b, _s: model},
                        skill_bundles={bundle.bundle_digest: bundle},
                        sandbox_factories={binding.sandbox.ref.digest: StateSandboxFactory()},
                        checkpointers={binding.checkpointer_ref.digest: saver},
                        stores={binding.store_ref.digest: InMemoryStore()},
                    )
                )
            )
            recovery = compose_postgres_operation_recovery(pool, run_control=run_control)
            assets = ConformanceAssetVerifier(
                mcp_schema_digests={"fixture-mcp": MCP_DIGEST},
                asset_manifest_digests={"skill:fixture.skill:1": SKILL_DIGEST},
            )
            service = OperationExecutionService(
                authority=AcceptingAuthority(),
                bindings=InMemoryOperationBindingRepository(),
                runtime=adapter,
                sandbox=ConformanceSandbox(),
                assets=assets,
                mcp=assets,
                secrets=ConformanceSecretResolver({"environment:OPENAI_API_KEY": "unused"}),
                events=ConformanceEventSink(),
                budget=ConformanceBudgetAuthority(),
                journal=JournaledOperationExecutionCoordinator(
                    journal=OperationJournalService(PostgresAtomicOperationJournalRepository(pool)),
                    run_control=run_control,
                    results=FileArtifactPayloadStore(paths.results),
                    actor=actor(),
                ),
                journal_claimed_by=CLAIMED_BY,
                lineage=recovery.lineage,
            )
            yield PersistentStack(
                pool=pool,
                saver=saver,
                run_control=run_control,
                recovery=recovery,
                service=service,
                binding=binding,
            )
    finally:
        await pool.close()
