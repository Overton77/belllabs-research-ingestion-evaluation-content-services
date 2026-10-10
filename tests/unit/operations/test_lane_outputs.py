"""MP-20: a Session Lane's declared outputs and Completion Candidate reach the families.

Offline (G1): the custody port registers `outputs/` files as workspace candidates of the exact
operation binding (digest-checked, at the slot path the binding declares), and the lane
settlement reads the final answer exactly as the Deep Agents lane does, with `output_refs`
reconciled to what the attempt registered.
"""

from __future__ import annotations

import json
from hashlib import sha256
from typing import Any

import pytest

from mission_control.application.artifacts.workspace_candidates import (
    InMemoryWorkspaceCandidateContents,
    WorkspaceCandidateCaptureService,
)
from mission_control.application.artifacts.workspace_materialization import (
    BindingWorkspaceMaterializer,
    InMemoryDurableWorkspaceInputs,
    InMemoryWorkspaceManifestRepository,
    WorkspaceMaterializationService,
)
from mission_control.application.execution.operations.lane_outputs import (
    WorkspaceCandidateLaneOutputs,
    declared_output_path,
)
from mission_control.application.execution.operations.operation_execution import (
    MAX_COMPLETION_CANDIDATE_CHARS,
    _lane_settlement,
    bind_operation_execution_request,
    lane_completion_candidate,
    lane_final_text,
)
from mission_control.domain.authoring.canonical import sha256_digest
from mission_control.domain.context.refs import (
    parse_workspace_candidate_ref,
    workspace_candidate_ref,
)
from mission_control.domain.execution.contracts import (
    MaterializedWorkspace,
    OperationExecutionRequest,
    WorkspaceOwner,
    WorkspaceOwnerKind,
    WorkspaceSlotBinding,
)
from mission_control.domain.execution.lane_turns import MAX_EXCERPT_CHARS, ClosingFacts
from mission_control.domain.execution.lanes import UsageReport
from tests.unit.operations.test_operation_execution import operation_request

OWNER = WorkspaceOwner(kind=WorkspaceOwnerKind.STAGE, owner_id="stage:patch")
REGISTERED = workspace_candidate_ref("candidate-1")


def compiled(*paths: str) -> OperationExecutionRequest:
    """The base operation with compiled writable slots (what a manifest template carries)."""

    base = operation_request()
    workspace = base.workspace.model_copy(
        update={
            "workflow_contract_digest": sha256_digest("contract"),
            "exclusive_write_paths": paths,
            "slot_bindings": tuple(
                WorkspaceSlotBinding(
                    slot_name=f"slot{index}",
                    logical_path=path,
                    access="exclusive_write",
                    owner=OWNER,
                )
                for index, path in enumerate(paths)
            ),
        }
    )
    return base.model_copy(update={"workspace": workspace})


class _Provisioner:
    async def provision(self, request: Any, manifest: Any, durable_inputs: Any) -> Any:
        return MaterializedWorkspace(
            workspace_id=request.workspace_id,
            namespace_id=request.namespace_id,
            provider=request.provider,
            runtime_digest=request.runtime_digest,
            image_digest=request.image_digest,
            mount_manifest_digest=manifest.manifest_digest,
            manifest_revision=manifest.revision,
        )


def custody() -> tuple[WorkspaceCandidateLaneOutputs, InMemoryWorkspaceCandidateContents]:
    materializer = WorkspaceMaterializationService(
        manifests=InMemoryWorkspaceManifestRepository(),
        provisioner=_Provisioner(),
        durable_inputs=InMemoryDurableWorkspaceInputs(),
    )
    contents = InMemoryWorkspaceCandidateContents()
    port = WorkspaceCandidateLaneOutputs(
        workspaces=BindingWorkspaceMaterializer(materializer),
        candidates=WorkspaceCandidateCaptureService(materializer=materializer, contents=contents),
    )
    return port, contents


def facts(text: str, *, status: str = "finished", refs: tuple[str, ...] = ()) -> ClosingFacts:
    return ClosingFacts(
        native_status=status,  # type: ignore[arg-type]
        result_excerpt=text,
        output_refs=refs,
        patch_ref="file-artifact://patch#sha256:" + "a" * 64 + ":3",
        usage=UsageReport(disposition="settled", input_tokens=3, output_tokens=2, total_tokens=5),
    )


# --- custody ----------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("paths", "file", "expected"),
    [
        # Manifest Stage Graph / Goal Loop slots: the first writable slot receives the file.
        (
            ("/workspace/output",),
            "outputs/patch_report.json",
            "/workspace/output/patch_report.json",
        ),
        (("/work",), "outputs/a/b.md", "/work/a/b.md"),
        # FT-G lane operations declare their outputs under `outputs/` themselves.
        (("/mnt/outputs/report.md",), "outputs/report.md", "/mnt/outputs/report.md"),
        (("/mnt/outputs",), "outputs/x/y.txt", "/mnt/outputs/x/y.txt"),
        # Never outside `outputs/`, never a traversal or an absolute path.
        (("/workspace/output",), "src/main.py", None),
        (("/workspace/output",), "outputs/../secret", None),
        (("/workspace/output",), "/outputs/x", None),
        ((), "outputs/x", None),
    ],
)
def test_a_lane_file_maps_to_the_slot_its_binding_declares(
    paths: tuple[str, ...], file: str, expected: str | None
) -> None:
    workspace = compiled(*paths).workspace
    assert declared_output_path(workspace, file, mount_root="/mnt") == expected


@pytest.mark.asyncio
async def test_a_declared_output_becomes_a_digest_checked_candidate_of_the_exact_binding() -> None:
    port, contents = custody()
    operation = compiled("/workspace/output")
    content = b'{"schema": "patch_report@1"}'

    ref = await port.register(operation, "outputs/patch_report.json", content)

    assert ref is not None
    candidate_id = parse_workspace_candidate_ref(ref)
    assert candidate_id is not None
    candidate = await contents.describe(candidate_id)
    binding = bind_operation_execution_request(operation)
    assert candidate.logical_path == "/workspace/output/patch_report.json"
    assert candidate.workspace_id == binding.workspace.workspace_id
    assert candidate.content_digest == f"sha256:{sha256(content).hexdigest()}"
    assert await contents.get(candidate_id) == content
    # The same file registers to the same ref (an `end_session` retry is idempotent).
    assert await port.register(operation, "outputs/patch_report.json", content) == ref


@pytest.mark.asyncio
async def test_an_uncompiled_workspace_or_undeclared_path_keeps_the_lane_staging() -> None:
    port, _contents = custody()
    assert await port.register(operation_request(), "outputs/report.md", b"x") is None
    assert await port.register(compiled("/workspace/output"), "notes.md", b"x") is None


# --- completion candidate -----------------------------------------------------------------------


def test_the_final_json_answer_is_the_candidate_with_registered_output_refs_only() -> None:
    answer = json.dumps(
        {"obligation_refs": ["patched"], "output_refs": ["outputs/patch_report.json"]}
    )
    structured, warnings = lane_completion_candidate(answer, (REGISTERED, "payload://patch"))

    assert structured == {"obligation_refs": ["patched"], "output_refs": [REGISTERED]}
    assert warnings == (
        {
            "kind": "provenance",
            "dropped_output_ref": "outputs/patch_report.json",
            "reason": "not_registered_by_this_attempt",
        },
    )


def test_a_candidate_without_output_refs_is_left_as_written() -> None:
    verdict = {"decision": "accepted", "accepted_obligation_refs": ["converged"]}
    assert lane_completion_candidate(json.dumps(verdict), (REGISTERED,)) == (verdict, ())


@pytest.mark.parametrize("text", ["Patched the parser.", "[1, 2]", "{not json", ""])
def test_a_non_object_answer_is_no_candidate(text: str) -> None:
    assert lane_completion_candidate(text, (REGISTERED,)) == (None, ())


def test_settlement_carries_the_candidate_only_for_a_completed_attempt() -> None:
    operation = compiled("/workspace/output")
    binding = bind_operation_execution_request(operation)
    answer = json.dumps({"obligation_refs": ["patched"], "output_refs": []})

    settled = _lane_settlement(binding, facts(answer, refs=(REGISTERED,)), native_turn_ref="t")
    assert settled.status == "completed"
    assert settled.structured_output == {
        "obligation_refs": ["patched"],
        "output_refs": [REGISTERED],
    }
    # Custody refs (the patch) stay on the settlement, never on the candidate.
    assert settled.output_refs[0] == REGISTERED and len(settled.output_refs) == 2
    assert settled.event_payloads == (
        {"lane_closing_facts": facts(answer, refs=(REGISTERED,)).model_dump(mode="json")},
    )

    failed = _lane_settlement(
        binding, facts(answer, status="error", refs=(REGISTERED,)), native_turn_ref="t"
    )
    assert failed.status == "failed" and failed.structured_output is None

    plain = _lane_settlement(binding, facts("done", refs=(REGISTERED,)), native_turn_ref="t")
    assert plain.structured_output is None
    assert "provenance_warnings" not in plain.event_payloads[0]


# --- MP-20: the candidate is read from the full final text, never a truncated excerpt -----------


def _long_answer(chars: int) -> str:
    """One JSON object of at least `chars` characters (a long typed result)."""

    answer = json.dumps({"obligation_refs": ["patched"], "output_refs": [], "note": ""})
    padding = max(chars - len(answer), 0)
    return json.dumps({"obligation_refs": ["patched"], "output_refs": [], "note": "x" * padding})


def test_a_final_json_longer_than_the_excerpt_is_parsed_from_the_full_text() -> None:
    binding = bind_operation_execution_request(compiled("/workspace/output"))
    answer = _long_answer(MAX_EXCERPT_CHARS * 3)
    closing = facts(answer[:MAX_EXCERPT_CHARS], refs=(REGISTERED,))

    settled = _lane_settlement(binding, closing, native_turn_ref="t", final_text=answer)

    assert settled.structured_output is not None
    assert settled.structured_output["obligation_refs"] == ["patched"]
    assert settled.structured_output["output_refs"] == [REGISTERED]
    assert len(str(settled.structured_output["note"])) > MAX_EXCERPT_CHARS


def test_an_excerpt_at_the_cap_without_the_full_text_fails_closed() -> None:
    binding = bind_operation_execution_request(compiled("/workspace/output"))
    answer = _long_answer(MAX_EXCERPT_CHARS * 2)
    closing = facts(answer[:MAX_EXCERPT_CHARS], refs=(REGISTERED,))

    settled = _lane_settlement(binding, closing, native_turn_ref="t")

    assert settled.status == "completed" and settled.structured_output is None
    (warning,) = settled.event_payloads[0]["provenance_warnings"]  # type: ignore[misc]
    assert warning["reason"] == "completion_candidate_truncated"
    # A short closing text is the whole text: it is still the candidate without a final text.
    short = _lane_settlement(binding, facts('{"obligation_refs": ["p"]}'), native_turn_ref="t")
    assert short.structured_output == {"obligation_refs": ["p"]}


def test_a_final_text_over_the_bound_fails_closed() -> None:
    binding = bind_operation_execution_request(compiled("/workspace/output"))
    answer = _long_answer(MAX_COMPLETION_CANDIDATE_CHARS + 1)
    closing = facts(answer[:MAX_EXCERPT_CHARS], refs=(REGISTERED,))

    settled = _lane_settlement(binding, closing, native_turn_ref="t", final_text=answer)

    assert settled.structured_output is None
    (warning,) = settled.event_payloads[0]["provenance_warnings"]  # type: ignore[misc]
    assert warning["reason"] == "completion_candidate_too_large"
    # Plain prose over the bound is no candidate and no warning.
    text, warnings = lane_final_text(closing, "x" * (MAX_COMPLETION_CANDIDATE_CHARS + 1))
    assert (text, warnings) == (None, ())
