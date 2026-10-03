"""RRM-015: every contract digest is independent of set iteration order.

Each test builds two equal contracts whose sets iterate in different orders (shared helper
`tests/fixtures/set_order.py`) and asserts that the digest computed by the production site is
equal. `assert_json_dumps_differ` proves the probe is effective: the JSON-mode dumps differ,
which is exactly what made the old `sha256_digest(x.model_dump(mode="json"))` seed-dependent.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

from mission_control.domain.authoring.canonical import (
    contract_fingerprint,
    sha256_digest,
    stable_json_digest,
    stable_json_dump,
    stored_payload_matches,
)
from mission_control.domain.graph_runtime.definitions import (
    MCPServerDefinition,
    StageCapabilityRequirement,
)
from tests.fixtures.set_order import (
    assert_json_dumps_differ,
    equal_sets_with_different_iteration_order,
    with_different_set_orders,
)

DIGEST = "sha256:" + "a" * 64


def _requirement() -> StageCapabilityRequirement:
    return StageCapabilityRequirement(
        stage_id="collect",
        operation_contract_ref="operation:collect@1",
        required_capability_ids=frozenset({"literature_search"}),
        optional_capability_ids=frozenset({"citation_lookup"}),
        input_contract_ref="contract:input@1",
        output_contract_ref="contract:output@1",
        context_purpose="research",
        effect_class="read_only",
        delegation_modes_allowed=frozenset({"sync"}),
        resource_class_ref="resource:default@1",
        verification_contract_ref="verification:collect@1",
        degradation_contract_ref="degradation:collect@1",
        speculation_policy_ref="policy:speculation:disabled",
    )


def _mcp_server() -> MCPServerDefinition:
    return MCPServerDefinition(
        logical_id="mcp.web",
        title="Web MCP",
        description="Governed web research MCP server.",
        transport="streamable_http",
        endpoint_ref="endpoint:web",
        tool_schema_digest=DIGEST,
        allowed_tools=frozenset({"search"}),
        session_policy="stateless",
        elicitation_policy="deny",
        timeout_seconds=30,
        max_retries=1,
    )


# --- the shared helper and the canonical primitives ---------------------------------------


def test_helper_builds_equal_sets_with_different_iteration_orders() -> None:
    first, second = equal_sets_with_different_iteration_order(frozenset({"a", "b"}))
    assert first == second
    assert list(first) != list(second)


def test_stable_json_dump_sorts_sets_and_matches_json_mode_for_set_free_contracts() -> None:
    requirement = _requirement()
    # With sets of one element the JSON-mode dump is already canonical, so the stable dump,
    # and every digest persisted before RRM-015, is unchanged.
    assert stable_json_dump(requirement) == requirement.model_dump(mode="json")
    assert stable_json_digest(requirement) == sha256_digest(requirement.model_dump(mode="json"))

    first, second = with_different_set_orders(requirement)
    assert_json_dumps_differ(first, second)
    assert stable_json_dump(first) == stable_json_dump(second)
    assert stable_json_digest(first) == stable_json_digest(second)
    for name in ("required_capability_ids", "optional_capability_ids"):
        values = stable_json_dump(first)[name]
        assert values == sorted(values)
    # The documented contract-fingerprint form (python-mode dump) is equally order-stable.
    assert contract_fingerprint(first) == contract_fingerprint(second)


def test_stable_json_dump_sorts_sets_nested_in_dicts_and_tuples() -> None:
    from pydantic import BaseModel

    class Inner(BaseModel):
        names: frozenset[str]

    class Outer(BaseModel):
        by_key: dict[str, Inner]
        groups: tuple[Inner, ...]
        loose: set[str]

    value = Outer(
        by_key={"k": Inner(names=frozenset({"b", "a"}))},
        groups=(Inner(names=frozenset({"d", "c"})),),
        loose={"z", "y"},
    )
    dumped = stable_json_dump(value)
    assert dumped == {
        "by_key": {"k": {"names": ["a", "b"]}},
        "groups": [{"names": ["c", "d"]}],
        "loose": ["y", "z"],
    }
    first, second = with_different_set_orders(value)
    assert_json_dumps_differ(first, second)
    assert stable_json_digest(first) == stable_json_digest(second)
    json.dumps(dumped)  # JSON-compatible


def test_contract_fingerprint_handles_sets_of_models_and_keeps_python_dump_values() -> None:
    from pydantic import BaseModel, ConfigDict

    class Member(BaseModel):
        model_config = ConfigDict(frozen=True)
        name: str

    class WithModels(BaseModel):
        members: frozenset[Member]
        other: int

    class WithStrings(BaseModel):
        names: frozenset[str]
        other: int

    # A Python-mode dump cannot represent a set of models ("unhashable type: 'dict'").
    models = WithModels(members=frozenset({Member(name="b"), Member(name="a")}), other=1)
    assert contract_fingerprint(models) == contract_fingerprint(models.model_copy())
    assert contract_fingerprint(models, exclude={"other"}) != contract_fingerprint(models)
    first, second = with_different_set_orders(models)
    assert contract_fingerprint(first) == contract_fingerprint(second)

    # For contracts the Python-mode dump could represent, the fingerprint value is unchanged,
    # so fingerprints stored by RRM-004 stay valid.
    strings = WithStrings(names=frozenset({"x", "y"}), other=2)
    assert contract_fingerprint(strings, exclude={"other"}) == sha256_digest(
        strings.model_dump(mode="python", exclude={"other"})
    )


def test_stored_payload_matches_ignores_stored_set_order() -> None:
    from pydantic import BaseModel

    class Stored(BaseModel):
        name: str
        members: frozenset[str]

    first, second = with_different_set_orders(Stored(name="x", members=frozenset({"a"})))
    stored = first.model_dump(mode="json")
    assert stored != second.model_dump(mode="json")
    assert stored_payload_matches(stored, second)
    assert not stored_payload_matches({**stored, "name": "other"}, second)
    assert not stored_payload_matches({"unrelated": True}, second)


# --- digest sites ----------------------------------------------------------------------------


def test_runtime_definition_digest_is_independent_of_set_iteration_order() -> None:
    """`RuntimeDefinition.digest` identifies every graph runtime definition revision."""

    first, second = with_different_set_orders(_mcp_server())
    assert_json_dumps_differ(first, second)
    assert first.digest == second.digest
    # A single-element set keeps the pre-RRM-015 JSON-mode digest, so persisted refs stay valid.
    original = _mcp_server()
    assert original.digest == sha256_digest(original.model_dump(mode="json"))


def test_launch_proposal_digest_is_independent_of_set_iteration_order() -> None:
    """`WorkflowLaunchProposal.digest` freezes the launch intent (admission authority sets)."""

    import asyncio

    from tests.unit.coordinator.test_coordinator_launch_preparation import launch_fixture

    _preparation, _tickets, proposal, _context = asyncio.run(launch_fixture("StageGraph"))
    first, second = with_different_set_orders(proposal)
    assert_json_dumps_differ(first, second)
    assert first.digest == second.digest


def test_structural_compiler_requirement_digest_is_independent_of_set_iteration_order() -> None:
    """Stage requirement reference digests (`compile_structural_graph_assembly`)."""

    from mission_control.application.recovery.runtime_run_plan import (
        compile_structural_graph_assembly,
    )
    from mission_control.domain.authoring.stagegraph_builder import (
        StageGraphStageSpec,
        build_stagegraph_v2,
    )
    from mission_control.domain.graph_runtime.definitions import (
        OperationAssemblySpec,
        RuntimeDefinitionKind,
        StageExecutionBinding,
    )
    from tests.unit.runtime.test_graph_runtime_contracts import ref

    implementation_ref = ref(RuntimeDefinitionKind.GRAPH_ASSEMBLY, "operation.collect")
    manifest_ref = ref(RuntimeDefinitionKind.CAPABILITY_MANIFEST, "disabled.manifest")
    assembly = OperationAssemblySpec.create(
        operation_assembly_id="assembly.collect.v1",
        operation_contract_ref="operation:collect@1",
        implementation_kind="native",
        implementation_ref=implementation_ref,
        model_policy_ref=manifest_ref,
        prompt_manifest_ref=manifest_ref,
        middleware_manifest_ref=manifest_ref,
        tool_manifest_ref=manifest_ref,
        mcp_manifest_ref=manifest_ref,
        skill_manifest_ref=manifest_ref,
        context_assembly_ref=manifest_ref,
        delegation_policy_ref=manifest_ref,
        workspace_policy_ref=manifest_ref,
        sandbox_profile_ref=manifest_ref,
        verifier_ref=manifest_ref,
        resource_envelope_ref=manifest_ref,
        effect_policy_ref=manifest_ref,
        fallback_policy_ref=manifest_ref,
        trace_redaction_policy_ref=manifest_ref,
        capability_manifest_ref=manifest_ref,
        compatibility_manifest_ref=manifest_ref,
    )
    first, second = with_different_set_orders(
        _requirement().model_copy(
            update={"delegation_modes_allowed": frozenset(), "optional_capability_ids": frozenset()}
        ),
        skip=frozenset({"delegation_modes_allowed", "optional_capability_ids"}),
    )
    assert_json_dumps_differ(first, second)
    binding = StageExecutionBinding(
        stage_id="collect",
        stage_requirement_ref=manifest_ref.model_copy(update={"digest": stable_json_digest(first)}),
        operation_assembly_ref=implementation_ref.model_copy(
            update={"digest": assembly.operation_assembly_digest}
        ),
        operation_assembly_digest=assembly.operation_assembly_digest,
        input_projection_ref="projection:input@1",
        output_projection_ref="projection:output@1",
        resource_envelope_ref=manifest_ref,
        compatibility_key="stagegraph-v2",
    )
    blueprint = build_stagegraph_v2(
        logical_id="blueprint.collect",
        title="Collect",
        description="One exact stage",
        stages=(StageGraphStageSpec(stage_id="collect"),),
    )

    def compile_with(requirement: StageCapabilityRequirement) -> object:
        return compile_structural_graph_assembly(
            blueprint=blueprint,
            graph_assembly_ref=ref(RuntimeDefinitionKind.GRAPH_ASSEMBLY, "stagegraph.v2"),
            state_schema_digest=DIGEST,
            reducer_registry_digest=DIGEST,
            operation_registry_digest=DIGEST,
            requirements=(requirement,),
            bindings=(binding,),
            assemblies={binding.operation_assembly_ref.logical_id: assembly},
            allowed_capability_ids=requirement.required_capability_ids
            | requirement.optional_capability_ids,
            disabled_capability_ids=frozenset(),
            compatibility_manifest_digest=DIGEST,
        )

    # The binding's digest came from one ordering; the other ordering must be accepted.
    assert compile_with(first) is not None
    assert compile_with(second) is not None


@pytest.mark.asyncio
async def test_linked_run_request_fingerprint_is_independent_of_set_iteration_order() -> None:
    """`LinkedRunService.request_child` replays an exact request across processes."""

    from tests.integration.temporal.test_linked_runs import linked_request, service_fixture

    service, _control, run_control, _repository = service_fixture()
    first, second = with_different_set_orders(linked_request())
    assert_json_dumps_differ(first, second)

    link = await service.request_child(first)
    replayed = await service.request_child(second)

    assert replayed == link
    assert link.request_fingerprint == stable_json_digest(first, exclude={"requested_at"})
    assert len(run_control.admissions) == 1


@pytest.mark.asyncio
async def test_bootstrap_authority_projection_digests_are_independent_of_set_iteration_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`PostgresBootstrapAuthority.load` digests the run and budget projections."""

    from types import SimpleNamespace

    from mission_control.adapters.postgres.runtime import (
        runtime_authority as postgres_runtime_authority,
    )
    from mission_control.adapters.postgres.runtime.runtime_authority import (
        PostgresBootstrapAuthority,
    )
    from tests.unit.run_control.test_run_control import request, service

    run_service, repository = service()
    admitted = await run_service.admit(request(request_id="bootstrap-digest"))
    assert admitted.run_id is not None
    run = await repository.get_run("tenant-1", admitted.run_id)
    budget = await repository.get_budget("tenant-1", admitted.run_id)
    run_a, run_b = with_different_set_orders(run)
    budget_a, budget_b = with_different_set_orders(budget)
    assert_json_dumps_differ(run_a, run_b)
    assert_json_dumps_differ(budget_a, budget_b)

    monkeypatch.setattr(
        postgres_runtime_authority,
        "AuthoritativeRuntimeProjection",
        lambda **values: SimpleNamespace(**values),
    )

    class Bindings:
        async def get_binding(self, _epoch: object) -> object:
            return SimpleNamespace(binding_id="binding-1")

    def authority_for(run_value: object, budget_value: object) -> PostgresBootstrapAuthority:
        class Runs:
            async def get_run(self, _scope: str, _run_id: str) -> object:
                return run_value

            async def get_budget(self, _scope: str, _run_id: str) -> object:
                return budget_value

        authority = object.__new__(PostgresBootstrapAuthority)
        authority._bindings = Bindings()  # type: ignore[assignment]
        authority._runs = Runs()  # type: ignore[assignment]
        return authority

    epoch = SimpleNamespace(request_scope="tenant-1", belllabs_run_id=admitted.run_id)
    first = await authority_for(run_a, budget_a).load(epoch)  # type: ignore[arg-type]
    second = await authority_for(run_b, budget_b).load(epoch)  # type: ignore[arg-type]

    assert first.lifecycle_projection_digest == second.lifecycle_projection_digest
    assert first.budget_projection_ref == second.budget_projection_ref
    assert first.decision_projection_ref == second.decision_projection_ref
    assert first.lifecycle_projection_digest == stable_json_digest(run_a)


@pytest.mark.asyncio
async def test_graph_admission_and_reconciliation_ids_are_independent_of_set_iteration_order() -> (
    None
):
    """`SchemaGraphAdmissionService.decide` ids and the reconciliation request digest."""

    from biotech_mission_adapters.application.schema.schema_workspace_binding import (
        SchemaGraphAdmissionService,
    )
    from biotech_mission_adapters.application.schema.supporting_graph_reconciliation import (
        _reconciliation_request_digest,
    )

    from tests.unit.schema.test_schema_grounding_services import _reconciliation_fixture

    request, _records, _factory = await _reconciliation_fixture()
    first_request, second_request = with_different_set_orders(request)
    assert_json_dumps_differ(first_request, second_request)

    admission = SchemaGraphAdmissionService()
    first = await admission.decide(first_request.admission)
    second = await admission.decide(second_request.admission)
    assert first.decision_id == second.decision_id
    assert _reconciliation_request_digest(first_request, None) == _reconciliation_request_digest(
        second_request, None
    )


@pytest.mark.asyncio
async def test_sandbox_snapshot_identities_are_independent_of_set_iteration_order() -> None:
    """Snapshot creation identity and clone request fingerprint replay exactly."""

    from tests.unit.workspaces.test_sandbox_snapshots import (
        clone_request,
        create_request,
        service,
    )

    snapshots, _repository, _payloads, sandbox, _authority = service()
    create_a, create_b = with_different_set_orders(create_request())
    assert_json_dumps_differ(create_a, create_b)
    source = await snapshots.create(create_a)
    assert await snapshots.create(create_b) == source
    assert sandbox.captures == 1
    assert source.creation_identity == stable_json_digest(create_a, exclude={"created_at"})

    clone_a, clone_b = with_different_set_orders(clone_request("workspace-target", "clone-1"))
    assert_json_dumps_differ(clone_a, clone_b)
    cloned = await snapshots.clone_restore(clone_a)
    assert await snapshots.clone_restore(clone_b) == cloned


async def _prepared_exact_refs(provider: object, family: str, initial_goal: str | None) -> tuple:
    """Exact input refs the real launch preparation freezes for a semantic binding provider."""

    from mission_control.application.coordinator.coordinator_semantic_bindings import (
        WorkflowSemanticBindingProviderRouter,
    )
    from tests.unit.coordinator.test_coordinator_launch_preparation import launch_fixture

    captured: list[object] = []

    class Recording:
        async def prepare(self, proposal: object, configuration: object) -> object:
            plan = await provider.prepare(proposal, configuration)  # type: ignore[attr-defined]
            captured.append(plan)
            return plan

    router = WorkflowSemanticBindingProviderRouter(
        {
            "schema-context-selection": Recording(),  # type: ignore[dict-item]
            "supporting-graph-reconciliation": Recording(),  # type: ignore[dict-item]
        }
    )
    preparation, _tickets, proposal, context = await launch_fixture(
        family, initial_goal=initial_goal, semantic_bindings=router
    )
    await preparation.prepare(proposal, context)
    (plan,) = captured
    return plan.exact_input_refs  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_semantic_binding_operation_template_refs_are_independent_of_set_order() -> None:
    """Both schema binding providers digest every operation request template."""

    from mission_control.application.execution.operations.operation_execution import (
        InMemoryOperationBindingRepository,
    )
    from tests.unit.coordinator.test_coordinator_semantic_binding_integration import (
        _schema_context_provider,
        _supporting_graph_provider,
    )

    for family, initial_goal, build in (
        ("StageGraph", None, lambda repo: _schema_context_provider(repo)),
        ("GoalDirected", "Reconcile the bounded supporting graph.", _supporting_graph_provider),
    ):
        provider = build(InMemoryOperationBindingRepository())
        if family == "GoalDirected":
            provider = await provider
        templates = provider._inputs.operation_bindings
        first, second = with_different_set_orders(templates)
        for template_a, template_b in zip(
            first.operations.values(), second.operations.values(), strict=True
        ):
            assert_json_dumps_differ(template_a, template_b)

        results = []
        for ordered in (first, second):
            variant = provider
            variant._inputs = provider._inputs.model_copy(update={"operation_bindings": ordered})
            results.append(await _prepared_exact_refs(variant, family, initial_goal))
        assert results[0] == results[1], family
        assert any("operation-execution-request-template:" in ref for ref in results[0])


@pytest.mark.asyncio
async def test_web_research_record_digests_are_independent_of_set_iteration_order() -> None:
    """`_append` derives record id and content digest from a typed payload with sets."""

    from types import SimpleNamespace

    from biotech_mission_adapters.application.web_research.web_research_semantic_handlers import (
        _append,
    )
    from biotech_mission_adapters.domain.coordinator.web_research_runtime import PublicGoalAdmission

    from tests.unit.web_research.test_web_research_semantic_handlers import (
        request as stage_request,
    )

    class Records:
        async def append(self, record: object) -> object:
            return record

    admission = PublicGoalAdmission(goal_digest=DIGEST)
    first, second = with_different_set_orders(admission)
    assert_json_dumps_differ(first, second)
    stage = stage_request("search")
    stage = SimpleNamespace(
        identity=stage.identity,
        idempotency_key=stage.idempotency_key,
        request_scope=stage.request_scope,
    )
    record_a = await _append(Records(), stage, "admission", first)  # type: ignore[arg-type]
    record_b = await _append(Records(), stage, "admission", second)  # type: ignore[arg-type]

    assert record_a.record_id == record_b.record_id  # type: ignore[attr-defined]
    assert record_a.content_digest == record_b.content_digest  # type: ignore[attr-defined]
    assert record_a.payload == record_b.payload  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_effective_run_configuration_persistence_is_independent_of_set_order() -> None:
    """The persisted ERC record (and its externalised payload ref) replays exactly."""

    from mission_control.adapters.storage.control_plane_payloads import InMemoryPayloadStore
    from mission_control.application.authoring.control_plane_repository import (
        InMemoryDefinitionRepository,
    )
    from mission_control.application.authoring.service import ControlPlaneService
    from mission_control.domain.authoring.extensions import ExtensionRegistry
    from tests.unit.control_plane.test_control_plane import configured_service, invocation

    compiler, _repository, records = await configured_service()
    erc = await compiler.compile(invocation(records))
    first, second = with_different_set_orders(erc)
    assert_json_dumps_differ(first, second)

    for externalize_above_bytes in (10_000_000, 0):  # inline payload, then externalised ref
        repository = InMemoryDefinitionRepository()
        service = ControlPlaneService(
            repository,
            ExtensionRegistry(),
            InMemoryPayloadStore(),
            externalize_above_bytes=externalize_above_bytes,
        )
        await service._persist_erc(first)
        # An exact replay in another process lists sets in another order; it must not collide.
        await service._persist_erc(second)
        record = await repository.get_erc_record(first.digest)
        assert (record["payload"] is None) == (record["payload_ref"] is not None)
        if externalize_above_bytes == 0:
            assert record["payload_ref"] is not None


# --- review fixes: strict stored-payload match, declared types, datetimes, replay sites ------


def test_stored_payload_matches_is_as_strict_as_json_equality_except_list_order() -> None:
    from pydantic import BaseModel

    class Stored(BaseModel):
        name: str
        count: int = 3
        members: frozenset[str]
        steps: tuple[str, ...]

    expected = Stored(name="x", members=frozenset({"a", "b"}), steps=("one", "two"))
    exact = expected.model_dump(mode="json")
    assert stored_payload_matches({**exact, "members": ["b", "a"]}, expected)
    # Omitting a defaulted field, or relying on lax coercion, is not the stored contract.
    assert not stored_payload_matches({k: v for k, v in exact.items() if k != "count"}, expected)
    assert not stored_payload_matches({**exact, "count": "3"}, expected)
    # Tuple order is semantic.
    assert not stored_payload_matches({**exact, "steps": ["two", "one"]}, expected)
    assert not stored_payload_matches({**exact, "name": "y"}, expected)
    # A corrupt payload is a mismatch, never an exception.
    assert not stored_payload_matches(None, expected)
    assert not stored_payload_matches("not json", expected)
    assert not stored_payload_matches({"members": 5, "steps": {}}, expected)


def test_stable_json_dump_follows_the_declared_type_like_model_dump() -> None:
    from pydantic import BaseModel

    class Base(BaseModel):
        name: str

    class Extended(Base):
        extra: frozenset[str]

    class Holder(BaseModel):
        item: Base
        items: tuple[Base, ...]

    extended = Extended(name="n", extra=frozenset({"x"}))
    holder = Holder(item=extended, items=(extended,))
    # Pydantic dumps by the declared field type, dropping the subclass-only field.
    assert holder.model_dump(mode="json") == {"item": {"name": "n"}, "items": [{"name": "n"}]}
    assert stable_json_dump(holder) == holder.model_dump(mode="json")
    # A top-level subclass instance dumps with all of its own fields, as model_dump does.
    assert stable_json_dump(extended) == extended.model_dump(mode="json")


def test_stable_json_dump_normalizes_aware_datetimes_inside_sets() -> None:
    from datetime import datetime, timedelta, timezone

    from pydantic import BaseModel

    class Moments(BaseModel):
        at: frozenset[datetime]
        single: datetime

    instant = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
    east = instant.astimezone(timezone(timedelta(hours=5)))
    assert east == instant and east.utcoffset() != instant.utcoffset()
    first = Moments(at=frozenset({instant, east}), single=east)
    assert len(first.at) == 1
    # Whichever representative the set kept, the dump is the same.
    kept_east = Moments(at=frozenset({east}), single=east)
    kept_utc = Moments(at=frozenset({instant}), single=east)
    assert stable_json_dump(kept_east)["at"] == stable_json_dump(kept_utc)["at"]
    assert stable_json_dump(first)["at"] == stable_json_dump(kept_utc)["at"]
    # Outside sets the instant is dumped as written, exactly like `model_dump(mode="json")`.
    assert stable_json_dump(first)["single"] == first.model_dump(mode="json")["single"]


async def _run_control_replay_material():  # type: ignore[no-untyped-def]
    """A real accepted start transition, command result, outbox event and budget."""

    from tests.unit.run_control.test_run_control import StartAction, command, request, service

    run_service, repository = service()
    admitted = await run_service.admit(request(request_id="replay-sites"))
    assert admitted.run_id is not None
    result = await run_service.execute(command(admitted.run_id, 1, "start", StartAction()))
    run = await repository.get_run("tenant-1", admitted.run_id)
    budget = await repository.get_budget("tenant-1", admitted.run_id)
    transition = repository._transitions[admitted.run_id][-1]
    event = list(repository._outbox.values())[-1].envelope
    return run, budget, transition, result, event


@pytest.mark.asyncio
async def test_journal_replay_proofs_accept_reordered_sets_and_reject_changed_fields() -> None:
    """The transition and outbox proofs in `_commit_run_control` (postgres_operation_journal)."""

    from types import SimpleNamespace

    from mission_control.adapters.postgres.operations.operation_journal import (
        PostgresAtomicOperationJournalRepository,
    )
    from mission_control.domain.policies.errors import IdempotencyConflict
    from tests.fixtures.set_order import json_with_reversed_sets

    run, budget, transition, result, event = await _run_control_replay_material()
    reordered_transition = json_with_reversed_sets(transition)
    reordered_event = json_with_reversed_sets(event)
    assert reordered_transition != stable_json_dump(transition)
    assert reordered_event != stable_json_dump(event)

    class Connection:
        def __init__(self, stored_transition: object, stored_event: object) -> None:
            self.stored_transition = stored_transition
            self.stored_event = stored_event

        async def fetchval(self, sql: str, *_args: object) -> object:
            if "budget_accounts" in sql:
                return budget.model_dump(mode="json")
            if "lifecycle_transitions" in sql:
                return self.stored_transition
            if "outbox" in sql:
                return self.stored_event
            raise AssertionError(sql)

        async def fetchrow(self, *_args: object) -> None:
            return None

        async def execute(self, *_args: object) -> None:
            return None

    mutation = SimpleNamespace(
        resulting_run=run,
        resulting_budget=budget,
        transition=transition,
        command_result=result,
        expected_run_version=run.version - 1,
        belllabs_run_id=run.run_id,
        claim=SimpleNamespace(effect_claim_id="claim-1", claimed_at=run.updated_at),
        ledger_entries=(),
        outbox_events=(event,),
    )
    current = run.model_copy(update={"version": run.version - 1})
    journal = object.__new__(PostgresAtomicOperationJournalRepository)

    async def commit(stored_transition: object, stored_event: object) -> None:
        await journal._commit_run_control(  # type: ignore[arg-type]
            Connection(stored_transition, stored_event), mutation, current_run=current
        )

    # Equal contracts stored with differently ordered sets are an exact replay.
    await commit(reordered_transition, reordered_event)
    with pytest.raises(IdempotencyConflict, match="lifecycle transition collision"):
        await commit({**reordered_transition, "command_id": "other"}, reordered_event)
    with pytest.raises(IdempotencyConflict, match="outbox event collision"):
        await commit(reordered_transition, {**reordered_event, "event_type": "other"})
