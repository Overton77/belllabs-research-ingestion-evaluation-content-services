from __future__ import annotations

import json
import os
import re
import stat
from collections.abc import Mapping
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Literal
from uuid import NAMESPACE_URL, uuid4, uuid5

from app.domain.operation_execution.errors import WorkspaceSlotConflict
from app.domain.run_control.errors import IdempotencyConflict

_DIGEST_PATTERN = re.compile(r"^sha256:[0-9a-f]{64}$")


class GoalScopeViolation(ValueError):
    """A proposed handoff does not preserve the frozen run scope."""


@dataclass(frozen=True)
class ScopeDigest:
    name: str
    digest: str

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("protected scope digest names cannot be empty")
        if not _DIGEST_PATTERN.fullmatch(self.digest):
            raise ValueError(f"protected scope digest {self.name!r} is not a SHA-256 digest")


@dataclass(frozen=True, init=False)
class GoalWorkspaceSpec:
    namespace_id: str
    run_id: str
    objective: str
    acceptance_contract: str
    protected_scope_digests: tuple[ScopeDigest, ...]

    def __init__(
        self,
        *,
        run_id: str,
        objective: str,
        acceptance_contract: str,
        protected_scope_digests: Mapping[str, str],
        namespace_id: str = "default",
    ) -> None:
        if not namespace_id or not run_id:
            raise ValueError("goal workspaces require namespace_id and run_id")
        if not objective or not acceptance_contract:
            raise ValueError("goal workspaces require an objective and acceptance contract")

        supplied = dict(protected_scope_digests)
        derived = {
            "objective": _digest_text(objective),
            "acceptance": _digest_text(acceptance_contract),
        }
        for name, digest in derived.items():
            if name in supplied and supplied[name] != digest:
                raise ValueError(f"protected {name} digest does not match authored content")
            supplied[name] = digest
        scope = tuple(ScopeDigest(name, digest) for name, digest in sorted(supplied.items()))

        object.__setattr__(self, "namespace_id", namespace_id)
        object.__setattr__(self, "run_id", run_id)
        object.__setattr__(self, "objective", objective)
        object.__setattr__(self, "acceptance_contract", acceptance_contract)
        object.__setattr__(self, "protected_scope_digests", scope)

    @property
    def scope_map(self) -> dict[str, str]:
        return {item.name: item.digest for item in self.protected_scope_digests}


@dataclass(frozen=True)
class GoalWorkspace:
    workspace_id: str
    spec: GoalWorkspaceSpec
    root: Path
    goal_directory: Path
    goal_markdown_path: Path
    goal_state_path: Path
    work_directory: Path
    checkpoints_directory: Path
    handoffs_directory: Path
    agents_directory: Path
    goal_view_digest: str


@dataclass(frozen=True)
class GoalWorkspaceLease:
    lease_id: str
    workspace_id: str
    owner_id: str
    agent_run_id: str


@dataclass(frozen=True)
class AgentRunWorkspace:
    agent_run_id: str
    agent_directory: Path
    workspace: GoalWorkspace
    lease: GoalWorkspaceLease


@dataclass(frozen=True)
class GoalCheckpoint:
    checkpoint_id: str
    workspace_id: str
    agent_run_id: str
    iteration: int
    kind: Literal["agent", "fallback"]
    content: str
    content_digest: str
    goal_view_digest: str
    protected_scope_digests: tuple[ScopeDigest, ...]
    idempotency_key_digest: str
    failure_reason: str | None = None


@dataclass(frozen=True)
class GoalHandoff:
    handoff_id: str
    workspace_id: str
    from_agent_run_id: str
    iteration: int
    summary: str
    instructions: str
    checkpoint_id: str
    content_digest: str
    goal_view_digest: str
    protected_scope_digests: tuple[ScopeDigest, ...]
    idempotency_key_digest: str
    accepted: Literal[True] = True


@dataclass(frozen=True)
class GoalPromptProjection:
    prompt: str
    content_digest: str
    goal_view_digest: str
    latest_handoff_id: str | None


class GoalWorkspaceService:
    """Filesystem-backed, host-governed workspace for sequential goal agent runs."""

    def __init__(self, base_path: Path) -> None:
        self._base_path = base_path.resolve()
        self._workspaces_path = self._base_path / "workspaces"
        self._authority_path = self._base_path / ".authority"
        self._workspaces_path.mkdir(parents=True, exist_ok=True)
        self._authority_path.mkdir(parents=True, exist_ok=True)

    def initialize(self, spec: GoalWorkspaceSpec) -> GoalWorkspace:
        workspace_id = _stable_id("goal-workspace", spec.namespace_id, spec.run_id)
        root = self._workspaces_path / workspace_id
        workspace = self._workspace(root, workspace_id, spec)
        authority = _canonical_json_bytes(self._authority_payload(workspace))
        _atomic_create_immutable(
            self._authority_path / f"{workspace_id}.json",
            authority,
            conflict_message="goal workspace identity is already bound to different frozen truth",
        )

        for directory in (
            workspace.goal_directory,
            workspace.work_directory,
            workspace.checkpoints_directory,
            workspace.handoffs_directory,
            workspace.agents_directory,
        ):
            directory.mkdir(parents=True, exist_ok=True)
        return self._regenerate_goal_truth(workspace)

    def project_prompt(self, workspace: GoalWorkspace) -> GoalPromptProjection:
        current = self.initialize(workspace.spec)
        goal_markdown = current.goal_markdown_path.read_text(encoding="utf-8")
        goal_state = current.goal_state_path.read_text(encoding="utf-8")
        latest = self.latest_handoff(current)
        if latest is None:
            handoff = "No accepted handoff exists. Begin from the frozen goal view."
            handoff_id = None
        else:
            handoff = _canonical_json_bytes(_handoff_payload(latest)).decode("utf-8").rstrip()
            handoff_id = latest.handoff_id
        prompt = (
            "# Governed Goal Workspace\n\n"
            f"Workspace ID: {current.workspace_id}\n"
            "The host-authored goal view below is frozen. Do not broaden its objective, "
            "acceptance, inputs, authority, budgets, or prohibited work.\n\n"
            '<goal-view path="goal/GOAL.md">\n'
            f"{goal_markdown}"
            "</goal-view>\n\n"
            '<goal-state path="goal/state.json">\n'
            f"{goal_state}"
            "</goal-state>\n\n"
            "<latest-accepted-handoff>\n"
            f"{handoff}\n"
            "</latest-accepted-handoff>\n"
        )
        return GoalPromptProjection(
            prompt=prompt,
            content_digest=_digest_text(prompt),
            goal_view_digest=current.goal_view_digest,
            latest_handoff_id=handoff_id,
        )

    def begin_agent_run(
        self,
        workspace: GoalWorkspace,
        *,
        agent_run_id: str,
        lease_owner_id: str,
    ) -> AgentRunWorkspace:
        current = self.initialize(workspace.spec)
        lease = self.acquire_write_lease(
            current,
            owner_id=lease_owner_id,
            agent_run_id=agent_run_id,
        )
        agent_directory = current.agents_directory / _stable_id(
            "goal-agent-run", current.workspace_id, agent_run_id
        )
        agent_directory.mkdir(parents=True, exist_ok=True)
        _atomic_create_immutable(
            agent_directory / "agent.json",
            _canonical_json_bytes(
                {
                    "agent_run_id": agent_run_id,
                    "lease_id": lease.lease_id,
                    "workspace_id": current.workspace_id,
                }
            ),
            conflict_message="agent run identity is already bound to different metadata",
        )
        return AgentRunWorkspace(
            agent_run_id=agent_run_id,
            agent_directory=agent_directory,
            workspace=current,
            lease=lease,
        )

    def end_agent_run(self, agent_run: AgentRunWorkspace) -> None:
        self.release_write_lease(agent_run.workspace, agent_run.lease)

    def acquire_write_lease(
        self,
        workspace: GoalWorkspace,
        *,
        owner_id: str,
        agent_run_id: str,
    ) -> GoalWorkspaceLease:
        if not owner_id or not agent_run_id:
            raise ValueError("writable leases require owner_id and agent_run_id")
        lease = GoalWorkspaceLease(
            lease_id=_stable_id(
                "goal-workspace-lease",
                workspace.workspace_id,
                owner_id,
                agent_run_id,
            ),
            workspace_id=workspace.workspace_id,
            owner_id=owner_id,
            agent_run_id=agent_run_id,
        )
        path = workspace.root / ".write-lease.json"
        payload = _canonical_json_bytes(_lease_payload(lease))
        try:
            _atomic_create(path, payload)
        except FileExistsError:
            try:
                prior = _lease_from_payload(_read_json(path))
            except (OSError, ValueError, TypeError, KeyError) as error:
                raise WorkspaceSlotConflict(
                    "goal workspace contains an unreadable writable lease"
                ) from error
            if prior != lease:
                raise WorkspaceSlotConflict(
                    "goal workspace writable paths are leased by another agent run"
                ) from None
        return lease

    def release_write_lease(
        self,
        workspace: GoalWorkspace,
        lease: GoalWorkspaceLease,
    ) -> None:
        path = workspace.root / ".write-lease.json"
        if not path.is_file():
            return
        prior = _lease_from_payload(_read_json(path))
        if prior != lease:
            raise WorkspaceSlotConflict("cannot release another agent run's writable lease")
        path.chmod(stat.S_IWRITE | stat.S_IREAD)
        path.unlink()

    def write_checkpoint(
        self,
        workspace: GoalWorkspace,
        *,
        agent_run_id: str,
        iteration: int,
        content: str,
        idempotency_key: str,
        kind: Literal["agent", "fallback"] = "agent",
        failure_reason: str | None = None,
    ) -> GoalCheckpoint:
        if iteration < 0:
            raise ValueError("checkpoint iteration cannot be negative")
        if not agent_run_id or not content or not idempotency_key:
            raise ValueError("checkpoint identity, content, and idempotency key are required")
        checkpoint = GoalCheckpoint(
            checkpoint_id=_stable_id("goal-checkpoint", workspace.workspace_id, idempotency_key),
            workspace_id=workspace.workspace_id,
            agent_run_id=agent_run_id,
            iteration=iteration,
            kind=kind,
            content=content,
            content_digest=_digest_text(content),
            goal_view_digest=workspace.goal_view_digest,
            protected_scope_digests=workspace.spec.protected_scope_digests,
            idempotency_key_digest=_digest_text(idempotency_key),
            failure_reason=failure_reason,
        )
        path = workspace.checkpoints_directory / f"{checkpoint.checkpoint_id}.json"
        _atomic_create_immutable(
            path,
            _canonical_json_bytes(_checkpoint_payload(checkpoint)),
            conflict_message="checkpoint idempotency key was reused with different content",
        )
        return checkpoint

    def record_handoff_failure(
        self,
        workspace: GoalWorkspace,
        *,
        agent_run_id: str,
        iteration: int,
        idempotency_key: str,
        failure_reason: str,
        last_agent_output: str = "",
    ) -> GoalCheckpoint:
        content = (
            "Agent handoff generation failed. Resume from the frozen goal and durable workspace. "
            f"Failure: {failure_reason}"
        )
        if last_agent_output:
            content += f"\n\nLast agent output:\n{last_agent_output}"
        return self.write_checkpoint(
            workspace,
            agent_run_id=agent_run_id,
            iteration=iteration,
            content=content,
            idempotency_key=idempotency_key,
            kind="fallback",
            failure_reason=failure_reason,
        )

    def accept_handoff(
        self,
        workspace: GoalWorkspace,
        *,
        from_agent_run_id: str,
        iteration: int,
        summary: str,
        instructions: str,
        checkpoint_id: str,
        proposed_scope_digests: Mapping[str, str],
        idempotency_key: str,
    ) -> GoalHandoff:
        if dict(proposed_scope_digests) != workspace.spec.scope_map:
            raise GoalScopeViolation("handoff attempted to change frozen protected scope digests")
        if iteration < 0:
            raise ValueError("handoff iteration cannot be negative")
        if not all((from_agent_run_id, summary, instructions, checkpoint_id, idempotency_key)):
            raise ValueError("handoff identity and content fields are required")
        content_digest = _digest_json(
            {
                "summary": summary,
                "instructions": instructions,
                "checkpoint_id": checkpoint_id,
            }
        )
        handoff = GoalHandoff(
            handoff_id=_stable_id("goal-handoff", workspace.workspace_id, idempotency_key),
            workspace_id=workspace.workspace_id,
            from_agent_run_id=from_agent_run_id,
            iteration=iteration,
            summary=summary,
            instructions=instructions,
            checkpoint_id=checkpoint_id,
            content_digest=content_digest,
            goal_view_digest=workspace.goal_view_digest,
            protected_scope_digests=workspace.spec.protected_scope_digests,
            idempotency_key_digest=_digest_text(idempotency_key),
        )
        path = workspace.handoffs_directory / f"{handoff.handoff_id}.json"
        _atomic_create_immutable(
            path,
            _canonical_json_bytes(_handoff_payload(handoff)),
            conflict_message="handoff idempotency key was reused with different content",
        )
        return handoff

    def latest_handoff(self, workspace: GoalWorkspace) -> GoalHandoff | None:
        accepted = [
            _handoff_from_payload(_read_json(path))
            for path in workspace.handoffs_directory.glob("*.json")
            if path.is_file()
        ]
        if not accepted:
            return None
        return max(accepted, key=lambda item: (item.iteration, item.handoff_id))

    def _workspace(
        self,
        root: Path,
        workspace_id: str,
        spec: GoalWorkspaceSpec,
    ) -> GoalWorkspace:
        goal_directory = root / "goal"
        markdown = self._goal_markdown(workspace_id, spec)
        state_without_view_digest = {
            "schema_version": 1,
            "namespace_id": spec.namespace_id,
            "run_id": spec.run_id,
            "workspace_id": workspace_id,
            "objective_digest": _digest_text(spec.objective),
            "acceptance_digest": _digest_text(spec.acceptance_contract),
            "protected_scope_digests": spec.scope_map,
            "goal_markdown_digest": _digest_text(markdown),
        }
        goal_view_digest = _digest_json(state_without_view_digest)
        return GoalWorkspace(
            workspace_id=workspace_id,
            spec=spec,
            root=root,
            goal_directory=goal_directory,
            goal_markdown_path=goal_directory / "GOAL.md",
            goal_state_path=goal_directory / "state.json",
            work_directory=root / "work",
            checkpoints_directory=root / "checkpoints",
            handoffs_directory=root / "handoffs",
            agents_directory=root / "agents",
            goal_view_digest=goal_view_digest,
        )

    def _regenerate_goal_truth(self, workspace: GoalWorkspace) -> GoalWorkspace:
        markdown = self._goal_markdown(workspace.workspace_id, workspace.spec)
        state = {
            "schema_version": 1,
            "namespace_id": workspace.spec.namespace_id,
            "run_id": workspace.spec.run_id,
            "workspace_id": workspace.workspace_id,
            "objective_digest": _digest_text(workspace.spec.objective),
            "acceptance_digest": _digest_text(workspace.spec.acceptance_contract),
            "protected_scope_digests": workspace.spec.scope_map,
            "goal_markdown_digest": _digest_text(markdown),
            "goal_view_digest": workspace.goal_view_digest,
        }
        _atomic_replace_read_only(workspace.goal_markdown_path, markdown.encode("utf-8"))
        _atomic_replace_read_only(workspace.goal_state_path, _canonical_json_bytes(state))
        return workspace

    def _authority_payload(self, workspace: GoalWorkspace) -> dict[str, object]:
        return {
            "schema_version": 1,
            "namespace_id": workspace.spec.namespace_id,
            "run_id": workspace.spec.run_id,
            "workspace_id": workspace.workspace_id,
            "objective": workspace.spec.objective,
            "acceptance_contract": workspace.spec.acceptance_contract,
            "protected_scope_digests": workspace.spec.scope_map,
            "goal_view_digest": workspace.goal_view_digest,
        }

    @staticmethod
    def _goal_markdown(workspace_id: str, spec: GoalWorkspaceSpec) -> str:
        scope_lines = "\n".join(
            f"- `{item.name}`: `{item.digest}`" for item in spec.protected_scope_digests
        )
        return (
            "# Governed Goal\n\n"
            f"- Run: `{spec.run_id}`\n"
            f"- Namespace: `{spec.namespace_id}`\n"
            f"- Workspace: `{workspace_id}`\n\n"
            "## Objective\n\n"
            f"{spec.objective}\n\n"
            "## Acceptance Contract\n\n"
            f"{spec.acceptance_contract}\n\n"
            "## Frozen Protected Scope\n\n"
            f"{scope_lines}\n"
        )


def _checkpoint_payload(checkpoint: GoalCheckpoint) -> dict[str, object]:
    return {
        "schema_version": 1,
        "checkpoint_id": checkpoint.checkpoint_id,
        "workspace_id": checkpoint.workspace_id,
        "agent_run_id": checkpoint.agent_run_id,
        "iteration": checkpoint.iteration,
        "kind": checkpoint.kind,
        "content": checkpoint.content,
        "content_digest": checkpoint.content_digest,
        "goal_view_digest": checkpoint.goal_view_digest,
        "protected_scope_digests": {
            item.name: item.digest for item in checkpoint.protected_scope_digests
        },
        "idempotency_key_digest": checkpoint.idempotency_key_digest,
        "failure_reason": checkpoint.failure_reason,
    }


def _handoff_payload(handoff: GoalHandoff) -> dict[str, object]:
    return {
        "schema_version": 1,
        "handoff_id": handoff.handoff_id,
        "workspace_id": handoff.workspace_id,
        "from_agent_run_id": handoff.from_agent_run_id,
        "iteration": handoff.iteration,
        "summary": handoff.summary,
        "instructions": handoff.instructions,
        "checkpoint_id": handoff.checkpoint_id,
        "content_digest": handoff.content_digest,
        "goal_view_digest": handoff.goal_view_digest,
        "protected_scope_digests": {
            item.name: item.digest for item in handoff.protected_scope_digests
        },
        "idempotency_key_digest": handoff.idempotency_key_digest,
        "accepted": handoff.accepted,
    }


def _handoff_from_payload(payload: Mapping[str, object]) -> GoalHandoff:
    scope = payload["protected_scope_digests"]
    if not isinstance(scope, dict):
        raise ValueError("handoff protected scope must be an object")
    return GoalHandoff(
        handoff_id=str(payload["handoff_id"]),
        workspace_id=str(payload["workspace_id"]),
        from_agent_run_id=str(payload["from_agent_run_id"]),
        iteration=int(str(payload["iteration"])),
        summary=str(payload["summary"]),
        instructions=str(payload["instructions"]),
        checkpoint_id=str(payload["checkpoint_id"]),
        content_digest=str(payload["content_digest"]),
        goal_view_digest=str(payload["goal_view_digest"]),
        protected_scope_digests=tuple(
            ScopeDigest(str(name), str(digest)) for name, digest in sorted(scope.items())
        ),
        idempotency_key_digest=str(payload["idempotency_key_digest"]),
        accepted=True,
    )


def _lease_payload(lease: GoalWorkspaceLease) -> dict[str, str]:
    return {
        "lease_id": lease.lease_id,
        "workspace_id": lease.workspace_id,
        "owner_id": lease.owner_id,
        "agent_run_id": lease.agent_run_id,
    }


def _lease_from_payload(payload: Mapping[str, object]) -> GoalWorkspaceLease:
    return GoalWorkspaceLease(
        lease_id=str(payload["lease_id"]),
        workspace_id=str(payload["workspace_id"]),
        owner_id=str(payload["owner_id"]),
        agent_run_id=str(payload["agent_run_id"]),
    )


def _read_json(path: Path) -> dict[str, object]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{path.name} must contain a JSON object")
    return value


def _canonical_json_bytes(value: object) -> bytes:
    return (
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n"
    ).encode("utf-8")


def _digest_json(value: object) -> str:
    return _digest_bytes(_canonical_json_bytes(value))


def _digest_text(value: str) -> str:
    return _digest_bytes(value.encode("utf-8"))


def _digest_bytes(value: bytes) -> str:
    return f"sha256:{sha256(value).hexdigest()}"


def _stable_id(*parts: str) -> str:
    return str(uuid5(NAMESPACE_URL, ":".join(parts)))


def _atomic_replace_read_only(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        path.chmod(stat.S_IWRITE | stat.S_IREAD)
        if path.read_bytes() == content:
            _mark_read_only(path)
            return
    temporary = path.parent / f".{path.name}.{uuid4()}.tmp"
    try:
        with temporary.open("xb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        _mark_read_only(path)
    finally:
        temporary.unlink(missing_ok=True)


def _atomic_create_immutable(path: Path, content: bytes, *, conflict_message: str) -> None:
    try:
        _atomic_create(path, content)
    except FileExistsError:
        if path.read_bytes() != content:
            raise IdempotencyConflict(conflict_message) from None
    _mark_read_only(path)


def _atomic_create(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.parent / f".{path.name}.{uuid4()}.tmp"
    try:
        with temporary.open("xb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.link(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _mark_read_only(path: Path) -> None:
    path.chmod(stat.S_IREAD | stat.S_IRGRP | stat.S_IROTH)
