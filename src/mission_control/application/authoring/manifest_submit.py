"""Mission Manifest submit and start (SPEC-05 "Submit and start", FT-E3).

``submit`` compiles; refuses on blockers (nothing is written); publishes each mission's lowered
definitions and compiles its Effective Run Configuration into the catalog (content-addressed,
idempotent); builds and verifies each mission's Run Request exactly as an ordinary admission
does; and commits everything in one application transaction (revision, typed definition rows,
authoring provenance, the admitted Run or the Mission Chain with its first Run and the frozen
admissions of later members). It is idempotent on ``request_id`` and never starts anything.

``start`` is the governed launch of an admitted run through ``RunLaunchService`` (the mission id
reaches the root's ``mc_mission_id`` search attribute) and registers the manifest's
``controls.subscriptions`` on the run before the launch, so the first events are delivered.
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal, Protocol
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from mission_control.application.authoring.manifest_service import (
    ManifestCompilation,
    ManifestCompileService,
    ManifestPermissionDenied,
    ManifestProgramCompiler,
    ManifestScope,
    require_any,
)
from mission_control.application.execution.run_launch import (
    RunLaunchRequest,
    RunLaunchService,
)
from mission_control.application.execution.service import (
    REQUIRED_SHARED_BUDGET_DIMENSIONS,
    AdmissionPolicyRegistry,
    RunControlService,
)
from mission_control.domain.authoring.contracts import EffectiveRunConfiguration, ExactDefinitionRef
from mission_control.domain.authoring.manifest import ManifestErrorCode, ManifestIssue
from mission_control.domain.authoring.manifest_lowering import (
    MANIFEST_INPUT_CONTRACT,
    MANIFEST_INVARIANT,
    lower_mission,
    lowering_digests,
)
from mission_control.domain.authoring.mission_definition import MissionDefinition
from mission_control.domain.composition.chain import ChainResolution
from mission_control.domain.policies.contracts import (
    ActorContext,
    BudgetApplicability,
    BudgetDimensionLimit,
    BudgetEnvelope,
    RunRequest,
    VerifiedRunConfiguration,
)
from mission_control.domain.policies.errors import (
    AdmissionRejected,
    ConfigurationVerificationFailed,
)
from mission_control.domain.subscriptions.contracts import (
    McpSessionChannel,
    StreamTicketChannel,
    Subscription,
    SubscriptionRequest,
    WebhookChannel,
)

SUBMIT_GRANTS = frozenset({"workflow_run.admit", "mission.author"})
START_GRANTS = frozenset({"workflow_run.start", "mission.start"})
SUBMIT_ISSUER = "mc.manifest_submit.v1"


class ManifestBlocked(Exception):
    """The manifest compiled with blockers (or failed admission); nothing was written."""

    code = "manifest_blocked"

    def __init__(self, compilation: ManifestCompilation | None, issues: Iterable[ManifestIssue]):
        self.compilation = compilation
        self.issues = tuple(issues)
        super().__init__("; ".join(f"{item.code.value}: {item.message}" for item in self.issues))


class ManifestIdempotencyConflict(Exception):
    code = "IDEMPOTENCY_CONFLICT"


class ManifestStartUnavailable(Exception):
    code = "start_unavailable"


class SubmitContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class SubmittedMission(SubmitContract):
    mission_key: str
    mission_id: UUID
    revision_id: UUID
    revision_no: int = Field(ge=1)
    family: Literal["StageGraph", "GoalDirected"]
    effective_configuration_digest: str
    run_id: str | None = None
    """The admitted run's run-control identity (the key ``start`` and commands address)."""
    run_uuid: UUID | None = None


class ManifestSubmissionReceipt(SubmitContract):
    """``mc.manifest_submission.v1``: what a submit committed (or found unchanged)."""

    schema_version: Literal["mc.manifest_submission.v1"] = "mc.manifest_submission.v1"
    request_id: str
    manifest_digest: str
    unchanged: bool = False
    chain_id: UUID | None = None
    missions: tuple[SubmittedMission, ...]


@dataclass(frozen=True)
class MissionSubmission:
    """One mission of a submit, compiled (persisted ERC) and verified for admission."""

    mission_key: str
    title: str
    definition: MissionDefinition
    family: Literal["StageGraph", "GoalDirected"]
    effective_configuration_digest: str
    workflow_type_ref: ExactDefinitionRef
    lowering: dict[str, str]
    initial_goal: str | None
    run_request: RunRequest
    verified: VerifiedRunConfiguration
    autostart: bool = True


@dataclass(frozen=True)
class SubmissionPlan:
    request_scope: str
    request_id: str
    actor_ref: str
    manifest_digest: str
    manifest_yaml: str
    resolution: dict[str, Any]
    at: datetime
    missions: tuple[MissionSubmission, ...]
    chain: ChainResolution | None = None
    chain_title: str | None = None


class StartedSubscription(SubmitContract):
    subscription_id: UUID
    events: tuple[str, ...]
    channel: str


class ManifestStartReceipt(SubmitContract):
    """``mc.manifest_start.v1``: the launch receipt and the registered subscriptions."""

    schema_version: Literal["mc.manifest_start.v1"] = "mc.manifest_start.v1"
    run_id: str
    mission_id: UUID
    family: Literal["StageGraph", "GoalDirected"]
    workflow_id: str
    temporal_run_id: str | None = None
    semantic_input_binding_ref: str
    accepted_run_version: int
    subscriptions: tuple[StartedSubscription, ...] = ()
    pending_subscriptions: tuple[str, ...] = ()


class SubmissionRepository(Protocol):
    async def receipt(self, request_scope: str, actor_ref: str, request_id: str) -> Any: ...

    async def commit(self, plan: Any) -> Any: ...

    async def run_subscriptions(self, request_scope: str, run_key: str) -> Any: ...

    async def head_run(self, request_scope: str, mission_id: UUID) -> str | None: ...


class LaunchInputPort(Protocol):
    """The admitted run's family input; production prepares it with its semantic binding."""

    async def family_input(
        self,
        *,
        request_scope: str,
        run_id: str,
        family: Literal["StageGraph", "GoalDirected"],
        initial_goal: str | None,
    ) -> dict[str, Any]: ...


class SubscriptionPort(Protocol):
    async def subscribe(
        self, request: SubscriptionRequest, actor: ActorContext
    ) -> Subscription: ...

    async def list(
        self, actor: ActorContext, *, mission_id: UUID | None = None, run_id: UUID | None = None
    ) -> tuple[Subscription, ...]: ...


@dataclass(frozen=True)
class SubmitRequest:
    manifest_yaml: str
    request_id: UUID
    actor: ActorContext
    sponsorship_refs: frozenset[str]
    approval_refs: frozenset[str] = frozenset()


def budget_envelope(configuration: EffectiveRunConfiguration) -> BudgetEnvelope:
    """Hard caps at the compiled ceilings; every shared dimension declared (unbounded unless
    the manifest bounds it); concurrency bounded by the effective authority."""

    ceilings = configuration.effective_authority.budgets.dimensions
    dimensions = []
    for name in sorted(REQUIRED_SHARED_BUDGET_DIMENSIONS | set(ceilings)):
        if name == "concurrency.slots":
            dimensions.append(
                BudgetDimensionLimit(
                    dimension=name,
                    applicability=BudgetApplicability.BOUNDED,
                    hard_cap=configuration.effective_authority.max_concurrency,
                )
            )
        elif name in ceilings:
            dimensions.append(
                BudgetDimensionLimit(
                    dimension=name,
                    applicability=BudgetApplicability.BOUNDED,
                    hard_cap=ceilings[name],
                )
            )
        else:
            dimensions.append(
                BudgetDimensionLimit(dimension=name, applicability=BudgetApplicability.UNBOUNDED)
            )
    return BudgetEnvelope(dimensions=tuple(dimensions))


class ManifestSubmitService:
    def __init__(
        self,
        *,
        compiler: ManifestCompileService,
        programs: ManifestProgramCompiler,
        run_control: RunControlService,
        submissions: SubmissionRepository,
        request_scope: str,
        launches: RunLaunchService | None = None,
        launch_inputs: LaunchInputPort | None = None,
        subscriptions: SubscriptionPort | None = None,
        clock: Any = None,
    ) -> None:
        self._compiler = compiler
        self._programs = programs
        self._run_control = run_control
        self._submissions = submissions
        self._scope = request_scope
        self._launches = launches
        self._launch_inputs = launch_inputs
        self._subscriptions = subscriptions
        self._clock = clock or (lambda: datetime.now(UTC))

    @property
    def request_scope(self) -> str:
        return self._scope

    # -- submit --------------------------------------------------------------------------

    async def submit(self, request: SubmitRequest) -> tuple[Any, bool]:
        """Returns ``(receipt, replayed)``; ``replayed`` is an exact replay of ``request_id``."""

        require_any(request.actor.permissions, SUBMIT_GRANTS, "manifest submit")
        request_id = str(request.request_id)
        actor_ref = request.actor.actor_id
        at = self._clock()
        compilation = await self._compiler.compile(
            request.manifest_yaml,
            ManifestScope(request_scope=self._scope, actor_id=actor_ref, at=at),
        )
        prior = await self._submissions.receipt(self._scope, actor_ref, request_id)
        if prior is not None:
            digest, receipt = prior
            if digest != compilation.report.manifest_digest:
                raise ManifestIdempotencyConflict(
                    "request_id was already used for a different manifest digest"
                )
            return receipt, True
        if not compilation.ok:
            raise ManifestBlocked(compilation, compilation.report.blockers)
        sponsorship = sorted(request.sponsorship_refs)
        if not sponsorship:
            raise ManifestBlocked(
                compilation,
                (
                    ManifestIssue(
                        code=ManifestErrorCode.AUTHORITY_DENIED,
                        pointer="",
                        message="the principal holds no sponsorship for a run",
                        reason="no_sponsorship",
                    ),
                ),
            )
        assert compilation.manifest is not None and compilation.resolution is not None
        chain = compilation.resolution.chain
        order = list(chain.order) if chain is not None else [compilation.definitions[0].mission_key]
        by_key = {item.mission_key: item for item in compilation.programs}
        definitions = {item.mission_key: item for item in compilation.definitions}
        missions = []
        for index, key in enumerate(order):
            compiled = by_key[key]
            configuration, workflow_ref = await self._programs.compile(
                compiled.lowered,
                request_scope=self._scope,
                actor_id=actor_ref,
                at=at,
                persist=True,
                compilation_suffix=request_id,
            )
            run_request = RunRequest(
                request_scope=self._scope,
                idempotency_issuer=json.dumps([SUBMIT_ISSUER, actor_ref], separators=(",", ":")),
                request_id=f"{request_id}:{key}",
                actor=request.actor,
                effective_configuration_digest=configuration.digest,
                workflow_type_ref=workflow_ref,
                input_manifest=configuration.input_manifest,
                budget_envelope=budget_envelope(configuration),
                requested_at=at,
                correlation_id=f"manifest:{compilation.report.manifest_digest}:{key}",
                sponsorship_ref=sponsorship[0],
                approval_refs=tuple(sorted(request.approval_refs)),
                admission_evidence_refs=(f"manifest:{compilation.report.manifest_digest}",),
            )
            verified = await self._verify(run_request, compilation, key)
            definition = definitions[key]
            missions.append(
                _mission_submission(
                    definition,
                    compiled.lowered.family,
                    configuration,
                    workflow_ref,
                    compiled.lowered,
                    run_request,
                    verified,
                    autostart=_autostart(definition),
                    first=index == 0,
                )
            )
        plan = SubmissionPlan(
            request_scope=self._scope,
            request_id=request_id,
            actor_ref=actor_ref,
            manifest_digest=str(compilation.report.manifest_digest),
            manifest_yaml=request.manifest_yaml,
            resolution=compilation.resolution.model_dump(mode="json", by_alias=True),
            at=at,
            missions=tuple(missions),
            chain=chain,
            chain_title=" + ".join(definitions[key].title for key in order) if chain else None,
        )
        return await self._submissions.commit(plan), False

    async def _verify(
        self, request: RunRequest, compilation: ManifestCompilation, key: str
    ) -> VerifiedRunConfiguration:
        try:
            return await self._run_control.verify_admission(request)
        except (AdmissionRejected, ConfigurationVerificationFailed) as error:
            pointer = next(
                (
                    item.pointer
                    for item in compilation.report.definitions
                    if item.mission_key == key
                ),
                "",
            )
            raise ManifestBlocked(
                compilation,
                (
                    ManifestIssue(
                        code=ManifestErrorCode.AUTHORITY_DENIED,
                        pointer=pointer,
                        message=f"admission of {key} would be rejected: {error}",
                        reason="admission_rejected",
                    ),
                ),
            ) from error

    async def head_run(self, mission_id: UUID) -> str | None:
        return await self._submissions.head_run(self._scope, mission_id)

    # -- start ---------------------------------------------------------------------------

    async def start(
        self,
        run_id: str,
        actor: ActorContext,
        *,
        family_input: dict[str, Any] | None = None,
    ) -> ManifestStartReceipt:
        require_any(actor.permissions, START_GRANTS, "run start")
        if self._launches is None:
            raise ManifestStartUnavailable("the Temporal launcher is not configured")
        info = await self._submissions.run_subscriptions(self._scope, run_id)
        if info is None:
            raise ManifestStartUnavailable(f"run {run_id} was not submitted from a manifest")
        definition = MissionDefinition.model_validate(info["definition"])
        lowered = lower_mission(definition)
        if family_input is None:
            if self._launch_inputs is None:
                raise ManifestStartUnavailable(
                    "no launch input author is composed: pass the family input, or compose a "
                    "semantic input binding author for manifest runs"
                )
            family_input = await self._launch_inputs.family_input(
                request_scope=self._scope,
                run_id=run_id,
                family=lowered.family,
                initial_goal=lowered.initial_goal,
            )
        subscribed, pending = await self._register_subscriptions(
            definition, info["run_uuid"], actor
        )
        launch_actor = actor.model_copy(
            update={"permissions": actor.permissions | {"workflow_run.start"}}
        )
        receipt = await self._launches.launch(
            RunLaunchRequest(
                request_scope=self._scope,
                run_id=run_id,
                family=lowered.family,
                stagegraph=family_input if lowered.family == "StageGraph" else None,
                goal_directed=family_input if lowered.family == "GoalDirected" else None,
                mission_id=str(info["mission_id"]),
            ),
            launch_actor,
        )
        return ManifestStartReceipt(
            run_id=run_id,
            mission_id=info["mission_id"],
            family=receipt.family,
            workflow_id=receipt.workflow_id,
            temporal_run_id=receipt.temporal_run_id,
            semantic_input_binding_ref=receipt.semantic_input_binding_ref,
            accepted_run_version=receipt.accepted_run_version,
            subscriptions=subscribed,
            pending_subscriptions=pending,
        )

    async def _register_subscriptions(
        self, definition: MissionDefinition, run_uuid: UUID, actor: ActorContext
    ) -> tuple[tuple[StartedSubscription, ...], tuple[str, ...]]:
        declared = definition.policies.subscriptions
        if not declared:
            return (), ()
        if self._subscriptions is None:
            return (), tuple(",".join(item.events) for item in declared)
        reader = actor.model_copy(update={"permissions": actor.permissions | {"workflow_run.read"}})
        existing = await self._subscriptions.list(reader, run_id=run_uuid)
        started: list[StartedSubscription] = []
        pending: list[str] = []
        for index, item in enumerate(declared):
            channel = _channel(item.channel, run_uuid, index)
            if channel is None:
                pending.append(",".join(item.events))
                continue
            match = next(
                (
                    sub
                    for sub in existing
                    if tuple(sub.filters.event_types) == tuple(item.events)
                    and sub.channel.kind == channel.kind
                ),
                None,
            )
            subscription = match or await self._subscriptions.subscribe(
                SubscriptionRequest(
                    target="run",
                    target_id=run_uuid,
                    events=tuple(item.events),
                    node_keys=tuple(item.node_keys),
                    channel=channel,
                ),
                reader,
            )
            started.append(
                StartedSubscription(
                    subscription_id=subscription.subscription_id,
                    events=tuple(item.events),
                    channel=channel.kind,
                )
            )
        return tuple(started), tuple(pending)


def _channel(
    channel: Any, run_uuid: UUID, index: int
) -> StreamTicketChannel | McpSessionChannel | WebhookChannel | None:
    if channel == "stream":
        return StreamTicketChannel(ticket_id=f"manifest:{run_uuid}:{index}")
    if channel == "mcp":
        return McpSessionChannel(session_ref=f"manifest:{run_uuid}:{index}")
    webhook = getattr(channel, "webhook", None)
    secret_ref = getattr(channel, "secret_ref", None)
    if webhook is None or secret_ref is None:
        return None  # a webhook without a secret reference cannot be signed
    return WebhookChannel(url=webhook, secret_ref=secret_ref)


def _autostart(definition: MissionDefinition) -> bool:
    value = definition.policies.chain_autostart
    return True if value is None else value


def _mission_submission(
    definition: MissionDefinition,
    family: Literal["StageGraph", "GoalDirected"],
    configuration: EffectiveRunConfiguration,
    workflow_ref: Any,
    lowered: Any,
    run_request: RunRequest,
    verified: VerifiedRunConfiguration,
    *,
    autostart: bool,
    first: bool,
) -> Any:
    del first
    return MissionSubmission(
        mission_key=definition.mission_key,
        title=definition.title,
        definition=definition,
        family=family,
        effective_configuration_digest=configuration.digest,
        workflow_type_ref=workflow_ref,
        lowering=lowering_digests(lowered),
        initial_goal=lowered.initial_goal,
        run_request=run_request,
        verified=verified,
        autostart=autostart,
    )


__all__ = [
    "ManifestBlocked",
    "ManifestIdempotencyConflict",
    "ManifestPermissionDenied",
    "ManifestStartReceipt",
    "ManifestStartUnavailable",
    "ManifestSubmitService",
    "SubmitRequest",
    "register_manifest_admission_policies",
]


def _manifest_input(request: RunRequest, configuration: VerifiedRunConfiguration) -> str | None:
    """A manifest-lowered Workflow Type admits only runs a manifest submit produced."""

    del configuration
    if not any(ref.startswith("manifest:sha256:") for ref in request.admission_evidence_refs):
        return "manifest runs are admitted only through a Mission Manifest submit"
    return None


def _manifest_invariant(request: RunRequest, configuration: VerifiedRunConfiguration) -> str | None:
    del request, configuration
    return None  # invariants are compiled into the definition and its Compiled Program


def register_manifest_admission_policies(registry: AdmissionPolicyRegistry) -> None:
    """Register the admission contract of manifest-lowered Workflow Types (idempotent)."""

    for contract_ref, validator in (
        (MANIFEST_INPUT_CONTRACT, _manifest_input),
        (MANIFEST_INVARIANT, _manifest_invariant),
    ):
        try:
            registry.register(contract_ref, validator)
        except ValueError:
            continue  # already composed
