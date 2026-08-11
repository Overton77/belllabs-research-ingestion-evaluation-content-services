from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import replace

from app.domain.control_plane.canonical import sha256_digest
from app.domain.control_plane.contracts import (
    DependencyClass,
    JoinKind,
    StageDependency,
    StageGraphBlueprint,
    StageJoin,
    StageOperationSlot,
)
from app.domain.orchestration.contracts import (
    AcceptedResultFact,
    CandidateOrderingKey,
    DependencyDisposition,
    DependencyProjection,
    ExecutionIdentity,
    FairnessCursorState,
    JoinDisposition,
    LateResultFacts,
    ProducerLiability,
    ResultDecision,
    ResultDispositionProposal,
    StageCandidateIdentity,
    StageExecutionIdentity,
    StageGraphAcceptedProjection,
    StageGraphCompletionProposal,
    StageInstanceProjection,
    StageInvalidationProposal,
    StageOperationAdmissionProposal,
    StageResultObservation,
    StageStatus,
    WorkflowInvalidationProposal,
)

TERMINAL_DEPENDENCY_DISPOSITIONS = frozenset(
    {
        DependencyDisposition.FULFILLED,
        DependencyDisposition.DEGRADED,
        DependencyDisposition.OMITTED,
        DependencyDisposition.FAILED,
        DependencyDisposition.CANCELLED,
        DependencyDisposition.INVALID,
    }
)


class StageGraphExecutionError(ValueError):
    pass


class StageGraphInterpreter:
    """Pure proposal interpreter for one immutable normalized StageGraph V2."""

    def __init__(self, blueprint: StageGraphBlueprint, *, effective_max_concurrency: int) -> None:
        if effective_max_concurrency < 1:
            raise StageGraphExecutionError("effective concurrency must be positive")
        oversized = [
            (stage.stage_id, slot.operation_slot_id)
            for stage in blueprint.stages
            for slot in stage.operation_slots
            if slot.concurrency_slots > effective_max_concurrency
        ]
        if oversized:
            raise StageGraphExecutionError(
                f"stages exceed the effective concurrency ceiling: {sorted(oversized)}"
            )
        self.blueprint = blueprint
        self.stages = {stage.stage_id: stage for stage in blueprint.stages}
        self.max_concurrency = effective_max_concurrency
        self.dependencies = {item.dependency_id: item for item in blueprint.dependencies}
        self.joins = {
            (item.consumer_stage_id, item.join_id): item for item in blueprint.joins
        }
        self.stage_joins: dict[str, tuple[StageJoin, ...]] = {
            stage_id: tuple(
                item for item in blueprint.joins if item.consumer_stage_id == stage_id
            )
            for stage_id in self.stages
        }
        self.descendants = self._descendants()
        self.group_ring = self._weighted_group_ring()

    def initial_projection(
        self,
        identity: ExecutionIdentity,
        *,
        run_version: int,
    ) -> StageGraphAcceptedProjection:
        stages: dict[str, StageInstanceProjection] = {}
        for stage in self.blueprint.stages:
            initial_status: StageStatus = (
                "ready" if not self.stage_joins[stage.stage_id] else "blocked"
            )
            for operation_slot in stage.operation_slots:
                candidate = StageCandidateIdentity(
                    stage_id=stage.stage_id,
                    mapped_instance_presence=0,
                    mapped_instance_id="NO_MAPPED_INSTANCE",
                    workflow_cycle_ordinal=0,
                    stage_cycle_ordinal=0,
                    operation_slot_id=operation_slot.operation_slot_id,
                )
                stages[candidate.semantic_prefix] = StageInstanceProjection(
                    candidate=candidate,
                    status=initial_status,
                )
        return StageGraphAcceptedProjection(
            identity=identity,
            family_version=0,
            run_version=run_version,
            stages=stages,
            dependencies={
                item.dependency_id: DependencyProjection(dependency_id=item.dependency_id)
                for item in self.blueprint.dependencies
            },
            fairness=FairnessCursorState(
                group_ring_cursor=0,
                candidate_cursors={
                    group.group_id: None for group in self.blueprint.fairness_groups
                },
            ),
        )

    @staticmethod
    def dependency_satisfies(
        dependency_class: DependencyClass,
        disposition: DependencyDisposition,
    ) -> bool | None:
        if dependency_class == DependencyClass.ADVISORY:
            return None
        if disposition == DependencyDisposition.UNRESOLVED:
            return False
        if dependency_class == DependencyClass.REQUIRED:
            return disposition == DependencyDisposition.FULFILLED
        if dependency_class == DependencyClass.DEGRADABLE:
            return disposition in {
                DependencyDisposition.FULFILLED,
                DependencyDisposition.DEGRADED,
            }
        return disposition in TERMINAL_DEPENDENCY_DISPOSITIONS

    def join_disposition(
        self,
        join: StageJoin,
        projection: StageGraphAcceptedProjection,
    ) -> JoinDisposition:
        edges = [self.dependencies[item] for item in join.dependency_ids]
        non_advisory = [
            edge for edge in edges if edge.dependency_class != DependencyClass.ADVISORY
        ]
        satisfied = 0
        unresolved = 0
        for edge in non_advisory:
            disposition = projection.dependencies[edge.dependency_id].disposition
            if disposition == DependencyDisposition.UNRESOLVED:
                unresolved += 1
            elif self.dependency_satisfies(edge.dependency_class, disposition):
                satisfied += 1
        count = len(non_advisory)
        if join.kind == JoinKind.ALL:
            if satisfied == count:
                return JoinDisposition.SATISFIED
            if satisfied + unresolved < count:
                return JoinDisposition.IMPOSSIBLE
            return JoinDisposition.PENDING
        if join.kind == JoinKind.ANY:
            if satisfied >= 1:
                return JoinDisposition.SATISFIED
            if unresolved == 0:
                return JoinDisposition.IMPOSSIBLE
            return JoinDisposition.PENDING
        minimum = join.minimum
        if minimum is None:
            raise StageGraphExecutionError("minimum join is missing its frozen threshold")
        if satisfied >= minimum:
            return JoinDisposition.SATISFIED
        if satisfied + unresolved < minimum:
            return JoinDisposition.IMPOSSIBLE
        return JoinDisposition.PENDING

    def frontier(
        self,
        projection: StageGraphAcceptedProjection,
        *,
        available_concurrency: int,
        blocked_candidate_keys: frozenset[str] = frozenset(),
    ) -> tuple[StageOperationAdmissionProposal, ...]:
        if available_concurrency < 0:
            raise StageGraphExecutionError("available concurrency cannot be negative")
        candidates_by_group: dict[
            str, list[tuple[CandidateOrderingKey, StageInstanceProjection]]
        ] = defaultdict(list)
        for instance in projection.stages.values():
            if instance.status not in {"blocked", "ready"}:
                continue
            joins = self.stage_joins[instance.candidate.stage_id]
            dispositions = [self.join_disposition(item, projection) for item in joins]
            if any(item == JoinDisposition.IMPOSSIBLE for item in dispositions):
                continue
            if any(item != JoinDisposition.SATISFIED for item in dispositions):
                continue
            stage = self.stages[instance.candidate.stage_id]
            slot = next(
                item
                for item in stage.operation_slots
                if item.operation_slot_id == instance.candidate.operation_slot_id
            )
            key = CandidateOrderingKey(priority=slot.priority, identity=instance.candidate)
            candidates_by_group[stage.fairness_group_id].append((key, instance))
        for candidates in candidates_by_group.values():
            candidates.sort(key=lambda item: item[0].as_tuple())

        proposals: list[StageOperationAdmissionProposal] = []
        fairness = projection.fairness
        remaining = min(available_concurrency, self.max_concurrency)
        while remaining and candidates_by_group:
            selected = self._select_next_candidate(
                candidates_by_group,
                fairness,
                blocked_candidate_keys,
                remaining,
            )
            if selected is None:
                break
            ring_index, group_id, key, instance, slots = selected
            identity = StageExecutionIdentity(
                run_id=projection.identity.run_id,
                execution_epoch=projection.identity.execution_epoch,
                candidate=instance.candidate,
                semantic_attempt=instance.semantic_attempt + 1,
            )
            reservation = dict(slots.reservation)
            reservation["operation.attempts"] = 1
            reservation["concurrency.slots"] = slots.concurrency_slots
            next_candidate_cursors = dict(fairness.candidate_cursors)
            next_candidate_cursors[group_id] = key
            fairness = FairnessCursorState(
                group_ring_cursor=(ring_index + 1) % len(self.group_ring),
                candidate_cursors=next_candidate_cursors,
            )
            input_refs = self._input_refs(instance.candidate.stage_id, projection)
            request_key = (
                f"{instance.candidate.stage_id}/"
                f"{instance.candidate.operation_slot_id}/"
                f"{slots.allowed_variants[0].operation_variant_id}"
            )
            exact_ref = (
                "operation-request:"
                f"{sha256_digest(identity.semantic_key).removeprefix('sha256:')}"
            )
            proposals.append(
                StageOperationAdmissionProposal(
                    ordering_key=key,
                    identity=identity,
                    operation_request_key=request_key,
                    exact_operation_request_ref=exact_ref,
                    reservation_id=f"reservation:{identity.semantic_key}",
                    reservation=reservation,
                    frozen_input_refs=input_refs,
                    selected_ring_index=ring_index,
                    next_fairness=fairness,
                    objective_override=instance.objective_override,
                )
            )
            remaining -= slots.concurrency_slots
            candidates_by_group[group_id] = [
                item for item in candidates_by_group[group_id] if item[0] != key
            ]
            if not candidates_by_group[group_id]:
                del candidates_by_group[group_id]
        return tuple(proposals)

    def running_concurrency(self, projection: StageGraphAcceptedProjection) -> int:
        """Return authoritative concurrency slots represented by admitted running stages."""
        total = 0
        for instance in projection.stages.values():
            if instance.status != "running":
                continue
            stage = self.stages[instance.candidate.stage_id]
            slot = next(
                item
                for item in stage.operation_slots
                if item.operation_slot_id == instance.candidate.operation_slot_id
            )
            total += slot.concurrency_slots
        return total

    def apply_admission(
        self,
        projection: StageGraphAcceptedProjection,
        proposal: StageOperationAdmissionProposal,
        *,
        next_run_version: int,
        next_family_version: int,
    ) -> StageGraphAcceptedProjection:
        key = proposal.identity.candidate.semantic_prefix
        current = projection.stages.get(key)
        if current is None or current.status not in {"blocked", "ready"}:
            raise StageGraphExecutionError("operation admission does not match a ready candidate")
        stages = dict(projection.stages)
        stages[key] = replace(
            current,
            status="running",
            semantic_attempt=proposal.identity.semantic_attempt,
            admitted_operation_request_ref=proposal.exact_operation_request_ref,
            frozen_input_refs=proposal.frozen_input_refs,
        )
        liabilities = dict(projection.producer_liabilities)
        liabilities[proposal.identity.semantic_key] = ProducerLiability(
            semantic_attempt_id=proposal.identity.semantic_key,
            reservation_id=proposal.reservation_id,
            reserved_amounts=proposal.reservation,
        )
        return replace(
            projection,
            family_version=next_family_version,
            run_version=next_run_version,
            stages=stages,
            fairness=proposal.next_fairness,
            producer_liabilities=liabilities,
        )

    def apply_result_decision(
        self,
        projection: StageGraphAcceptedProjection,
        observation: StageResultObservation,
        proposal: ResultDispositionProposal,
        *,
        next_run_version: int,
        next_family_version: int,
    ) -> StageGraphAcceptedProjection:
        if observation.identity != proposal.identity:
            raise StageGraphExecutionError("result observation and decision identities differ")
        key = observation.identity.candidate.semantic_prefix
        current = projection.stages.get(key)
        if (
            current is None
            or current.status != "running"
            or current.semantic_attempt != observation.identity.semantic_attempt
        ):
            raise StageGraphExecutionError("result does not match the active semantic attempt")
        liabilities = dict(projection.producer_liabilities)
        liability = liabilities.get(observation.identity.semantic_key)
        if liability is None or liability.result_decision is not None:
            raise StageGraphExecutionError("producer result already has a decision or no liability")
        liabilities[observation.identity.semantic_key] = ProducerLiability(
            semantic_attempt_id=liability.semantic_attempt_id,
            reservation_id=liability.reservation_id,
            reserved_amounts=liability.reserved_amounts,
            child_closed_or_quiesced=observation.child_closed_or_quiesced,
            reservations_and_usage_settled=observation.reservations_and_usage_settled,
            effects_settled=observation.effects_settled,
            cancellation_reconciled=observation.cancellation_reconciled,
            result_decision=proposal.decision,
        )
        result = observation.operation_result
        raw_output_refs = result.get("output_refs", ())
        output_refs = (
            tuple(str(item) for item in raw_output_refs)
            if isinstance(raw_output_refs, list | tuple)
            else ()
        )
        stages = dict(projection.stages)
        stages[key] = replace(
            current,
            status=self._stage_status_for_result(proposal),
            output_refs=output_refs if proposal.decision == ResultDecision.ADMIT else (),
        )
        dependencies = dict(projection.dependencies)
        for edge in self._producer_edges(current.candidate.stage_id):
            existing = dependencies[edge.dependency_id]
            if existing.disposition != DependencyDisposition.UNRESOLVED:
                continue
            dependencies[edge.dependency_id] = replace(
                existing,
                disposition=proposal.dependency_dispositions[edge.dependency_id],
                evidence_refs=output_refs if proposal.decision == ResultDecision.ADMIT else (),
            )
        accepted_results = projection.accepted_results
        if proposal.decision == ResultDecision.ADMIT:
            accepted_results += (
                AcceptedResultFact(
                    identity=observation.identity,
                    operation_result=observation.operation_result,
                    accepted_at_order=observation.accepted_order,
                ),
            )
        accepted_results = tuple(
            sorted(
                accepted_results,
                key=lambda item: (
                    item.identity.candidate.stage_id.encode("utf-8"),
                    item.identity.candidate.mapped_instance_presence,
                    item.identity.candidate.mapped_instance_id.encode("utf-8"),
                    item.identity.candidate.workflow_cycle_ordinal,
                    item.identity.candidate.stage_cycle_ordinal,
                    item.identity.candidate.operation_slot_id.encode("utf-8"),
                    item.identity.semantic_attempt,
                ),
            )
        )
        raw_obligation_refs = observation.operation_result.get("obligation_refs", ())
        obligation_refs = (
            frozenset(str(item) for item in raw_obligation_refs)
            if proposal.decision == ResultDecision.ADMIT
            and isinstance(raw_obligation_refs, list | tuple)
            else frozenset()
        )
        return replace(
            projection,
            family_version=next_family_version,
            run_version=next_run_version,
            stages=stages,
            dependencies=dependencies,
            producer_liabilities=liabilities,
            accepted_results=accepted_results,
            accepted_obligation_evidence=(
                frozenset(projection.accepted_obligation_evidence) | obligation_refs
            ),
        )

    def workflow_invalidation(
        self,
        projection: StageGraphAcceptedProjection,
        *,
        invalidation_frontier: tuple[str, ...],
        next_objective: str,
    ) -> WorkflowInvalidationProposal:
        policy = self.blueprint.workflow_cycle_policy
        if policy is None or projection.workflow_cycle_ordinal >= policy.max_cycles:
            raise StageGraphExecutionError("workflow cycle limit exceeded")
        frontier = set(invalidation_frontier)
        if not frontier or not frontier <= self.stages.keys():
            raise StageGraphExecutionError("workflow cycle has an invalid invalidation frontier")
        if not next_objective:
            raise StageGraphExecutionError("a workflow cycle requires a new typed objective")
        invalidated = frontier | {
            descendant for stage_id in frontier for descendant in self.descendants[stage_id]
        }
        reused: dict[str, tuple[str, ...]] = {}
        for instance in projection.stages.values():
            if instance.candidate.stage_id not in invalidated and instance.output_refs:
                reused[instance.candidate.semantic_prefix] = instance.output_refs
        return WorkflowInvalidationProposal(
            next_workflow_cycle_ordinal=projection.workflow_cycle_ordinal + 1,
            invalidation_frontier=tuple(
                sorted(frontier, key=lambda item: item.encode("utf-8"))
            ),
            invalidated_stage_ids=tuple(
                sorted(invalidated, key=lambda item: item.encode("utf-8"))
            ),
            reused_output_refs=reused,
            next_objective=next_objective,
        )

    def stage_invalidation(
        self,
        projection: StageGraphAcceptedProjection,
        *,
        stage_id: str,
        next_objective: str,
    ) -> StageInvalidationProposal:
        stage = self.stages.get(stage_id)
        if stage is None or stage.stage_cycle_policy is None:
            raise StageGraphExecutionError("stage cycle is not authored")
        current = max(
            (
                item.candidate.stage_cycle_ordinal
                for item in projection.stages.values()
                if item.candidate.stage_id == stage_id
                and item.candidate.workflow_cycle_ordinal
                == projection.workflow_cycle_ordinal
            ),
            default=0,
        )
        if current >= stage.stage_cycle_policy.max_cycles:
            raise StageGraphExecutionError("stage cycle limit exceeded")
        if not next_objective:
            raise StageGraphExecutionError("a stage cycle requires a new typed objective")
        current_objectives = {
            item.objective_override
            for item in projection.stages.values()
            if item.candidate.stage_id == stage_id
            and item.candidate.workflow_cycle_ordinal
            == projection.workflow_cycle_ordinal
            and item.candidate.stage_cycle_ordinal == current
            and item.objective_override is not None
        }
        if next_objective in current_objectives:
            raise StageGraphExecutionError(
                "stage cycle made no objective progress under the frozen stopping rule"
            )
        invalidated = {stage_id, *self.descendants[stage_id]}
        reused = {
            item.candidate.semantic_prefix: item.output_refs
            for item in projection.stages.values()
            if item.candidate.stage_id not in invalidated and item.output_refs
        }
        prior = tuple(
            ref
            for item in projection.stages.values()
            if item.candidate.stage_id == stage_id
            for ref in item.output_refs
        )
        unmet = tuple(
            slot.obligation_ref
            for slot in stage.obligation_slots
            if slot.obligation_ref not in projection.accepted_obligation_evidence
        )
        return StageInvalidationProposal(
            stage_id=stage_id,
            prior_stage_cycle_ordinal=current,
            next_stage_cycle_ordinal=current + 1,
            invalidated_stage_ids=tuple(
                sorted(invalidated, key=lambda item: item.encode("utf-8"))
            ),
            reused_output_refs=reused,
            unmet_obligation_refs=unmet,
            accepted_evidence_refs=tuple(sorted(projection.accepted_obligation_evidence)),
            allowed_input_refs=self._input_refs(stage_id, projection),
            prior_result_refs=prior,
            next_objective=next_objective,
        )

    def apply_stage_invalidation(
        self,
        projection: StageGraphAcceptedProjection,
        proposal: StageInvalidationProposal,
        *,
        next_run_version: int,
        next_family_version: int,
    ) -> StageGraphAcceptedProjection:
        invalidated = frozenset(proposal.invalidated_stage_ids)
        stages = dict(projection.stages)
        for key, instance in tuple(stages.items()):
            if instance.candidate.stage_id in invalidated:
                stages[key] = replace(instance, status="invalidated")
        for stage_id in sorted(invalidated, key=lambda item: item.encode("utf-8")):
            stage = self.stages[stage_id]
            for slot in stage.operation_slots:
                candidate = StageCandidateIdentity(
                    stage_id=stage_id,
                    mapped_instance_presence=0,
                    mapped_instance_id="NO_MAPPED_INSTANCE",
                    workflow_cycle_ordinal=projection.workflow_cycle_ordinal,
                    stage_cycle_ordinal=proposal.next_stage_cycle_ordinal,
                    operation_slot_id=slot.operation_slot_id,
                )
                stages[candidate.semantic_prefix] = StageInstanceProjection(
                    candidate=candidate,
                    status="ready" if not self.stage_joins[stage_id] else "blocked",
                    objective_override=(
                        proposal.next_objective if stage_id == proposal.stage_id else None
                    ),
                )
        dependencies = dict(projection.dependencies)
        for edge in self.blueprint.dependencies:
            if edge.producer_stage_id not in invalidated:
                continue
            prior = dependencies[edge.dependency_id]
            dependencies[edge.dependency_id] = DependencyProjection(
                dependency_id=edge.dependency_id,
                generation=prior.generation + 1,
                disposition=DependencyDisposition.UNRESOLVED,
                supersedes_generation=prior.generation,
            )
        return replace(
            projection,
            family_version=next_family_version,
            run_version=next_run_version,
            stages=stages,
            dependencies=dependencies,
            invalidated_stage_ids=invalidated,
        )

    def apply_workflow_invalidation(
        self,
        projection: StageGraphAcceptedProjection,
        proposal: WorkflowInvalidationProposal,
        *,
        next_run_version: int,
        next_family_version: int,
    ) -> StageGraphAcceptedProjection:
        """Apply one accepted workflow-cycle decision without overwriting prior lineage."""
        if proposal.next_workflow_cycle_ordinal != projection.workflow_cycle_ordinal + 1:
            raise StageGraphExecutionError("workflow invalidation cycle is not the next ordinal")
        invalidated = frozenset(proposal.invalidated_stage_ids)
        if not invalidated or not invalidated <= self.stages.keys():
            raise StageGraphExecutionError("workflow invalidation names an unknown stage")
        stages = dict(projection.stages)
        for key, instance in tuple(stages.items()):
            if instance.candidate.stage_id not in invalidated:
                continue
            stages[key] = replace(instance, status="invalidated")
        for stage_id in sorted(invalidated, key=lambda item: item.encode("utf-8")):
            stage = self.stages[stage_id]
            status: StageStatus = "ready" if not self.stage_joins[stage_id] else "blocked"
            for slot in stage.operation_slots:
                candidate = StageCandidateIdentity(
                    stage_id=stage_id,
                    mapped_instance_presence=0,
                    mapped_instance_id="NO_MAPPED_INSTANCE",
                    workflow_cycle_ordinal=proposal.next_workflow_cycle_ordinal,
                    stage_cycle_ordinal=0,
                    operation_slot_id=slot.operation_slot_id,
                )
                stages[candidate.semantic_prefix] = StageInstanceProjection(
                    candidate=candidate,
                    status=status,
                    objective_override=(
                        proposal.next_objective
                        if stage_id in proposal.invalidation_frontier
                        else None
                    ),
                )
        dependencies = dict(projection.dependencies)
        for edge in self.blueprint.dependencies:
            if edge.producer_stage_id not in invalidated:
                continue
            prior = dependencies[edge.dependency_id]
            dependencies[edge.dependency_id] = DependencyProjection(
                dependency_id=edge.dependency_id,
                generation=prior.generation + 1,
                disposition=DependencyDisposition.UNRESOLVED,
                supersedes_generation=prior.generation,
            )
        return replace(
            projection,
            family_version=next_family_version,
            run_version=next_run_version,
            workflow_cycle_ordinal=proposal.next_workflow_cycle_ordinal,
            stages=stages,
            dependencies=dependencies,
            invalidated_stage_ids=invalidated,
        )

    def late_result_decision(
        self,
        identity: StageExecutionIdentity,
        edge: StageDependency,
        facts: LateResultFacts,
        *,
        slow_sibling_route: str,
    ) -> ResultDispositionProposal:
        vetoes = (
            (
                facts.run_terminal or facts.terminalization_started,
                "run_terminal_or_terminalization_started",
                ResultDecision.QUARANTINE,
            ),
            (
                facts.generation_superseded
                or facts.producer_invalidated
                or facts.evidence_invalid,
                "generation_or_evidence_invalid",
                ResultDecision.QUARANTINE,
            ),
            (facts.run_cancelling, "run_cancelling", ResultDecision.QUARANTINE),
            (
                facts.dependency_terminally_disposed,
                "dependency_terminally_disposed",
                ResultDecision.REJECT,
            ),
        )
        for matched, reason, decision in vetoes:
            if matched:
                return ResultDispositionProposal(
                    identity=identity,
                    decision=decision,
                    dependency_dispositions={
                        edge.dependency_id: self._negative_disposition(edge)
                    },
                    matched_veto=reason,
                    quarantine_reason=reason if decision == ResultDecision.QUARANTINE else None,
                )
        if facts.consumer_already_admitted and slow_sibling_route == "quarantine":
            return ResultDispositionProposal(
                identity=identity,
                decision=ResultDecision.QUARANTINE,
                dependency_dispositions={
                    edge.dependency_id: self._negative_disposition(edge)
                },
                quarantine_reason="slow_sibling_route",
            )
        if facts.consumer_already_admitted:
            rule = next(
                item
                for item in self.blueprint.late_result_policy.rules
                if item.trigger == "consumer_already_admitted"
            )
            decision = ResultDecision(rule.decision)
            return ResultDispositionProposal(
                identity=identity,
                decision=decision,
                dependency_dispositions={
                    edge.dependency_id: (
                        DependencyDisposition.FULFILLED
                        if decision == ResultDecision.ADMIT
                        else self._negative_disposition(edge)
                    )
                },
                matched_rule_id=rule.rule_id,
                quarantine_reason=(
                    rule.rule_id if decision == ResultDecision.QUARANTINE else None
                ),
            )
        return ResultDispositionProposal(
            identity=identity,
            decision=ResultDecision.ADMIT,
            dependency_dispositions={
                edge.dependency_id: DependencyDisposition.FULFILLED
            },
        )

    def result_decision(
        self,
        identity: StageExecutionIdentity,
        facts: LateResultFacts,
        *,
        operation_disposition: str = "completed",
    ) -> ResultDispositionProposal:
        edges = self._producer_edges(identity.candidate.stage_id)
        if not edges:
            return ResultDispositionProposal(
                identity=identity,
                decision=ResultDecision.ADMIT,
                dependency_dispositions={},
            )
        decisions = [
            self.late_result_decision(
                identity,
                edge,
                facts,
                slow_sibling_route=self._slow_sibling_route(edge),
            )
            for edge in edges
        ]
        precedence = {
            ResultDecision.ADMIT: 0,
            ResultDecision.REJECT: 1,
            ResultDecision.QUARANTINE: 2,
        }
        first = max(decisions, key=lambda item: precedence[item.decision])
        has_absolute_veto = any(item.matched_veto is not None for item in decisions)
        if operation_disposition != "completed" and not has_absolute_veto:
            first = ResultDispositionProposal(
                identity=identity,
                decision=ResultDecision.REJECT,
                dependency_dispositions={},
                matched_veto=f"operation_{operation_disposition}",
            )
        final_dispositions = {
            edge.dependency_id: (
                decision.dependency_dispositions[edge.dependency_id]
                if first.decision == ResultDecision.ADMIT
                else self._negative_disposition(
                    edge,
                    cancelled=operation_disposition == "cancelled",
                )
            )
            for edge, decision in zip(edges, decisions, strict=True)
        }
        return ResultDispositionProposal(
            identity=identity,
            decision=first.decision,
            dependency_dispositions=final_dispositions,
            matched_veto=first.matched_veto,
            matched_rule_id=first.matched_rule_id,
            quarantine_reason=first.quarantine_reason,
        )

    def completion(
        self,
        projection: StageGraphAcceptedProjection,
    ) -> StageGraphCompletionProposal:
        required_obligations = {
            item.evidence_slot_id
            for item in self.blueprint.obligation_matrix
            if item.required
        }
        pending_dependencies = tuple(
            sorted(
                (
                    dependency_id
                    for dependency_id, item in projection.dependencies.items()
                    if item.disposition == DependencyDisposition.UNRESOLVED
                    and self.dependencies[dependency_id].dependency_class
                    != DependencyClass.ADVISORY
                ),
                key=lambda item: item.encode("utf-8"),
            )
        )
        open_liabilities = tuple(
            sorted(
                (
                    liability_id
                    for liability_id, item in projection.producer_liabilities.items()
                    if not item.closed
                ),
                key=lambda item: item.encode("utf-8"),
            )
        )
        outputs = tuple(
            sorted(
                {
                    output_ref
                    for instance in projection.stages.values()
                    for output_ref in instance.output_refs
                },
                key=lambda item: item.encode("utf-8"),
            )
        )
        return StageGraphCompletionProposal(
            required_obligations_accepted=(
                required_obligations
                <= frozenset(projection.accepted_obligation_evidence)
            ),
            pending_dependency_ids=pending_dependencies,
            open_producer_liability_ids=open_liabilities,
            valid_output_refs=outputs,
        )

    def _select_next_candidate(
        self,
        candidates_by_group: dict[
            str, list[tuple[CandidateOrderingKey, StageInstanceProjection]]
        ],
        fairness: FairnessCursorState,
        blocked_candidate_keys: frozenset[str],
        remaining_concurrency: int,
    ) -> tuple[
        int,
        str,
        CandidateOrderingKey,
        StageInstanceProjection,
        StageOperationSlot,
    ] | None:
        for offset in range(len(self.group_ring)):
            ring_index = (fairness.group_ring_cursor + offset) % len(self.group_ring)
            group_id = self.group_ring[ring_index]
            candidates = candidates_by_group.get(group_id, ())
            if not candidates:
                continue
            cursor = fairness.candidate_cursors.get(group_id)
            cursor_tuple = cursor.as_tuple() if cursor is not None else None
            after = [
                item
                for item in candidates
                if cursor_tuple is None or item[0].as_tuple() > cursor_tuple
            ]
            ordered = [*after, *(item for item in candidates if item not in after)]
            for key, instance in ordered:
                stage = self.stages[instance.candidate.stage_id]
                slot = next(
                    item
                    for item in stage.operation_slots
                    if item.operation_slot_id == instance.candidate.operation_slot_id
                )
                if (
                    instance.candidate.semantic_prefix in blocked_candidate_keys
                    or slot.concurrency_slots > remaining_concurrency
                ):
                    continue
                return ring_index, group_id, key, instance, slot
        return None

    def _weighted_group_ring(self) -> tuple[str, ...]:
        groups = sorted(
            self.blueprint.fairness_groups,
            key=lambda item: item.group_id.encode("utf-8"),
        )
        ring = tuple(
            group.group_id
            for round_number in range(1, max(item.weight for item in groups) + 1)
            for group in groups
            if group.weight >= round_number
        )
        if not ring:
            raise StageGraphExecutionError("weighted fairness ring cannot be empty")
        return ring

    def _input_refs(
        self,
        stage_id: str,
        projection: StageGraphAcceptedProjection,
    ) -> tuple[str, ...]:
        refs = {
            evidence_ref
            for edge in self.blueprint.dependencies
            if edge.consumer_stage_id == stage_id
            for evidence_ref in projection.dependencies[edge.dependency_id].evidence_refs
        }
        return tuple(sorted(refs, key=lambda item: item.encode("utf-8")))

    def _producer_edges(self, stage_id: str) -> tuple[StageDependency, ...]:
        return tuple(
            item for item in self.blueprint.dependencies if item.producer_stage_id == stage_id
        )

    def _slow_sibling_route(self, edge: StageDependency) -> str:
        return self.joins[
            (edge.consumer_stage_id, edge.join_id)
        ].slow_sibling_policy.arrival_route

    @staticmethod
    def _negative_disposition(
        edge: StageDependency,
        *,
        cancelled: bool = False,
    ) -> DependencyDisposition:
        if edge.dependency_class == DependencyClass.ADVISORY:
            return DependencyDisposition.UNRESOLVED
        if edge.dependency_class == DependencyClass.OPTIONAL:
            return DependencyDisposition.OMITTED
        if cancelled:
            return DependencyDisposition.CANCELLED
        return DependencyDisposition.FAILED

    @staticmethod
    def _stage_status_for_result(proposal: ResultDispositionProposal) -> StageStatus:
        if proposal.decision == ResultDecision.ADMIT:
            return (
                "degraded"
                if DependencyDisposition.DEGRADED
                in proposal.dependency_dispositions.values()
                else "completed"
            )
        return "failed"

    def _descendants(self) -> dict[str, frozenset[str]]:
        direct: dict[str, set[str]] = defaultdict(set)
        for dependency in self.blueprint.dependencies:
            direct[dependency.producer_stage_id].add(dependency.consumer_stage_id)
        result: dict[str, frozenset[str]] = {}
        for root in self.stages:
            descendants: set[str] = set()
            queue = deque(direct[root])
            while queue:
                stage_id = queue.popleft()
                if stage_id in descendants:
                    continue
                descendants.add(stage_id)
                queue.extend(direct[stage_id])
            result[root] = frozenset(descendants)
        return result
