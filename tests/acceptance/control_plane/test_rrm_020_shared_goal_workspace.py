"""RRM-020 production proof: a `shared` GoalDirected workspace across two iterations.

The deployment's API and workers (RRM-009's `open_production_stack`: `compose_runtime_control`,
`ProductionWorkerActivityCompositionFactory`, `create_production_workers`, a persistent
`start_local` namespace, the disposable PostgreSQL and MongoDB) run a two-iteration GoalDirected
run whose blueprint keeps the default `workspace_mode = "shared"`. Every operation materializes
its workspace through `BindingWorkspaceMaterializer` over `MongoWorkspaceManifestRepository`.
The executor and the verifier each keep one workspace identity; the second iteration's role root
joins it as one new manifest revision, and the first root keeps its owner. Cognition is RRM-009's
deterministic technical model, which writes its report into its role's governed `/work` slot.
No company input, no live model. Opt-in through `TEST_APPLICATION_POSTGRES_DSN` and
`TEST_MONGODB_URI`.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from app.application.orchestration.mongo_goal_directed_repository import (
    MongoGoalDirectedDocumentRepository,
)
from app.domain.operation_execution.contracts import (
    WorkspaceMaterializationManifest,
    WorkspaceOwnerKind,
)
from app.domain.operation_execution.materialization import verify_workspace_manifest
from app.domain.run_control.contracts import RunOutcome
from app.models.workspace_materialization import (
    WorkspaceMaterializationManifestDocument,
    WorkspaceSlotReservationDocument,
)
from tests.acceptance.control_plane.test_rrm_009_production_composition import (
    ProductionStack,
    _admit,
    _calls,
    _launch,
    _replay,
    _run,
    _terminal,
    _wait_for,
    mongo_database,  # noqa: F401 - the per-test Mongo database fixture
    open_production_stack,
)
from tests.fixtures.rrm009_production_stack import (
    SCOPE,
    goal_input,
    goal_templates,
    publish_technical_catalog,
    technical_binding,
)


@pytest.fixture
async def stack(
    test_application_postgres_dsn: str,
    test_mongodb_uri: str,
    mongo_database: str,  # noqa: F811 - the imported fixture
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncIterator[ProductionStack]:
    technical = technical_binding()
    model_log: list[dict[str, Any]] = []
    async with open_production_stack(
        dsn=test_application_postgres_dsn,
        mongo_uri=test_mongodb_uri,
        mongo_database=mongo_database,
        root=tmp_path,
        monkeypatch=monkeypatch,
        technical=technical,
        components=technical.components(model_log),
        model_log=model_log,
    ) as production:
        yield production


async def _manifests(namespace_id: str) -> dict[str, list[WorkspaceMaterializationManifest]]:
    documents = (
        await WorkspaceMaterializationManifestDocument.find(
            WorkspaceMaterializationManifestDocument.namespace_id == namespace_id
        )
        .sort("+revision")
        .to_list()
    )
    by_workspace: dict[str, list[WorkspaceMaterializationManifest]] = {}
    for document in documents:
        manifest = WorkspaceMaterializationManifest.model_validate(document.payload)
        verify_workspace_manifest(manifest)
        by_workspace.setdefault(manifest.workspace_id, []).append(manifest)
    return by_workspace


def _slot_revisions(lineage: list[WorkspaceMaterializationManifest]) -> list[int]:
    """The revisions at which the workspace's slot set changed (materializations)."""

    return [
        manifest.revision
        for index, manifest in enumerate(lineage)
        if index == 0 or manifest.slots != lineage[index - 1].slots
    ]


@pytest.mark.asyncio
async def test_shared_goal_workspace_spans_two_iterations_on_the_production_composition(
    stack: ProductionStack,
) -> None:
    catalog = await publish_technical_catalog(
        stack.control_plane,
        family="GoalDirected",
        now=datetime.now(UTC),
        goal_workspace_mode="shared",
    )
    assert catalog.blueprint.workspace_policy.workspace_mode == "shared"
    run_id = await _admit(stack, catalog, "rrm020-shared-goal")
    binding_ref = f"semantic-input:rrm020:{run_id}"
    templates = goal_templates(stack.technical, catalog)
    await MongoGoalDirectedDocumentRepository().persist_templates(
        request_scope=SCOPE,
        semantic_input_binding_ref=binding_ref,
        executor=templates["executor"],
        verifier=templates["verifier"],
        recorded_at=datetime.now(UTC),
    )
    objective = "Produce one independently verified technical record in a shared workspace."
    await _launch(
        stack,
        run_id,
        {
            "request_scope": SCOPE,
            "run_id": run_id,
            "family": "GoalDirected",
            "goal_directed": asdict(goal_input(catalog, run_id, binding_ref, 1, objective)),
        },
    )
    await _wait_for(stack, run_id, lambda: _terminal(stack, run_id), 300)

    # The run completes: both iterations' executor and verifier settled through run control.
    run = await _run(stack, run_id)
    assert (run["phase"], run["terminal_outcome"]) == ("terminal", RunOutcome.COMPLETED.value)
    assert [item["output_ref"] for item in run["accepted_output_evidence"]] == [
        "artifact:rrm009-goal:2"
    ]
    assert len(run["accepted_operation_settlement_evidence"]) == 4
    calls = _calls(stack, run_id)
    assert sorted(calls) == [
        "goal-iteration/1/executor",
        "goal-iteration/1/verifier",
        "goal-iteration/2/executor",
        "goal-iteration/2/verifier",
    ], calls
    assert all(value == {"parent": 4, "child": 1} for value in calls.values()), calls

    # One workspace identity per role across both iterations, on the Mongo manifests.
    namespace_id = f"run/{run_id}"
    executor_id = f"run/{run_id}/execution-epoch/1/goal/workspace/1"
    verifier_id = f"{executor_id}:verifier"
    lineages = await _manifests(namespace_id)
    assert set(lineages) == {executor_id, verifier_id}, sorted(lineages)
    summary: dict[str, Any] = {}
    for workspace_id, role, kind in (
        (executor_id, "executor", WorkspaceOwnerKind.ITERATION),
        (verifier_id, "verifier", WorkspaceOwnerKind.EVALUATOR),
    ):
        lineage = lineages[workspace_id]
        # An unbroken revision chain, each revision linked to its predecessor.
        assert [item.revision for item in lineage] == list(range(1, len(lineage) + 1))
        assert all(
            later.prior_manifest_digest == earlier.manifest_digest
            for earlier, later in zip(lineage, lineage[1:], strict=False)
        )
        current = lineage[-1]
        # Earlier roots keep their owners; the next iteration's root joined after them.
        assert [(slot.logical_path, slot.owner.owner_id) for slot in current.slots] == [
            (f"/goal/1/{role}/work", f"goal-iteration/1/{role}"),
            (f"/goal/2/{role}/work", f"goal-iteration/2/{role}"),
        ]
        assert {slot.owner.kind for slot in current.slots} == {kind}
        # Exactly once: the slot set changed twice (iteration 1's materialization and
        # iteration 2's one extension revision), however often the workspace was resolved.
        slot_revisions = _slot_revisions(lineage)
        assert len(slot_revisions) == 2 and slot_revisions[0] == 1, slot_revisions
        candidates = sorted(
            (entry.logical_path, entry.owner.owner_id)
            for entry in current.entries
            if entry.kind == "local_candidate"
        )
        # Each iteration's report is governed by its own root's slot and owner.
        assert candidates == [
            (f"/goal/1/{role}/work/report.md", f"goal-iteration/1/{role}"),
            (f"/goal/2/{role}/work/report.md", f"goal-iteration/2/{role}"),
        ]
        summary[role] = {
            "workspace": workspace_id,
            "revisions": len(lineage),
            "slot_revisions": slot_revisions,
            "slots": [slot.logical_path for slot in current.slots],
            "candidates": [path for path, _ in candidates],
        }

    # Writable roots stay disjoint and owned (REQ-BP-GD-004, REQ-CP-DA-013).
    reservations = await WorkspaceSlotReservationDocument.find(
        WorkspaceSlotReservationDocument.namespace_id == namespace_id
    ).to_list()
    owned = {item.logical_path: (item.workspace_id, item.owner_id) for item in reservations}
    assert owned == {
        "/goal/1/executor": (executor_id, "goal-iteration/1/executor"),
        "/goal/2/executor": (executor_id, "goal-iteration/2/executor"),
        "/goal/1/verifier": (verifier_id, "goal-iteration/1/verifier"),
        "/goal/2/verifier": (verifier_id, "goal-iteration/2/verifier"),
    }
    assert len(reservations) == 4

    replayed = await _replay(stack.client, [f"belllabs-run/{run_id}", f"family/{run_id}/1"])
    budget = await stack.http.get(
        f"/run-control/v1/runs/{run_id}/budget", params={"request_scope": SCOPE}
    )
    assert budget.status_code == 200
    print(
        "RRM-020 EVIDENCE shared_goal_workspace:",
        json.dumps(
            {
                "run": run_id,
                "workspace_mode": catalog.blueprint.workspace_policy.workspace_mode,
                "outcome": run["terminal_outcome"],
                "accepted_outputs": [
                    item["output_ref"] for item in run["accepted_output_evidence"]
                ],
                "settlements": len(run["accepted_operation_settlement_evidence"]),
                "workspaces": summary,
                "reservations": {path: owner for path, (_, owner) in sorted(owned.items())},
                "model_calls": calls,
                "consumed": budget.json()["consumed"],
                "replayed_events": replayed,
            },
            sort_keys=True,
        ),
    )
