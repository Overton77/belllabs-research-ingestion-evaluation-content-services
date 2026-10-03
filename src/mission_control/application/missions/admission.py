"""Scoped admission and governed launch through the existing transactional services."""

from __future__ import annotations

import json
from dataclasses import asdict
from datetime import UTC, datetime

from mission_control.application.execution.run_launch import (
    RunLaunchReceipt,
    RunLaunchRequest,
    RunLaunchService,
)
from mission_control.application.execution.service import RunControlService
from mission_control.contracts.admission_contracts import (
    MissionAdmissionRequest,
    MissionLaunchRequest,
)
from mission_control.contracts.contracts import MissionControlRejected
from mission_control.domain.policies.contracts import ActorContext, AdmissionDecision, RunRequest


class MissionAdmissionService:
    def __init__(
        self,
        run_control: RunControlService,
        *,
        request_scope: str,
        launch_service: RunLaunchService | None = None,
    ) -> None:
        if not request_scope:
            raise ValueError("authenticated request scope is required")
        self._run_control = run_control
        self._scope = request_scope
        self._launch = launch_service

    @property
    def request_scope(self) -> str:
        return self._scope

    async def admit(
        self,
        request: MissionAdmissionRequest,
        actor: ActorContext,
        *,
        sponsorship_refs: frozenset[str],
        approval_refs: frozenset[str],
    ) -> AdmissionDecision:
        if "workflow_run.admit" not in actor.permissions:
            raise MissionControlRejected("unauthorized", "admission permission required")
        request = MissionAdmissionRequest.model_validate(request.model_dump(mode="python"))
        if request.sponsorship_ref not in sponsorship_refs:
            raise MissionControlRejected("unauthorized", "sponsorship was not granted")
        if not set(request.approval_refs) <= approval_refs:
            raise MissionControlRejected("unauthorized", "approval was not granted")
        if not request.delegation_authority_refs <= actor.authority_refs:
            raise MissionControlRejected("unauthorized", "delegation authority was not granted")
        body = request.model_dump(mode="python", exclude={"schema_version", "request_id"})
        return await self._run_control.admit(
            RunRequest(
                **body,
                request_scope=self._scope,
                idempotency_issuer=json.dumps(
                    ["mc.runtime_admission.v1", actor.actor_id], separators=(",", ":")
                ),
                request_id=str(request.request_id),
                actor=actor,
                requested_at=datetime.now(UTC),
                correlation_id=str(request.request_id),
            )
        )

    async def launch(
        self, run_id: str, request: MissionLaunchRequest, actor: ActorContext
    ) -> RunLaunchReceipt:
        if "workflow_run.start" not in actor.permissions:
            raise MissionControlRejected("unauthorized", "launch permission required")
        request = MissionLaunchRequest.model_validate(request.model_dump(mode="python"))
        if self._launch is None:
            raise MissionControlRejected(
                "unavailable", "application Temporal launcher is not configured"
            )
        return await self._launch.launch(
            RunLaunchRequest(
                request_scope=self._scope,
                run_id=run_id,
                family=request.family,
                stagegraph=asdict(request.stagegraph) if request.stagegraph is not None else None,
                goal_directed=asdict(request.goal_directed)
                if request.goal_directed is not None
                else None,
                source_semantic_input_binding_ref=request.source_semantic_input_binding_ref,
            ),
            actor,
        )
