from __future__ import annotations

from app.application.run_control import AdmissionPolicyRegistry
from app.domain.run_control.contracts import RunRequest, VerifiedRunConfiguration

SELECTION_ADMISSION = "admission:schema-context-selection:v1"
RECONCILIATION_ADMISSION = "admission:supporting-graph-reconciliation:v1"
SELECTION_INVARIANTS = (
    "invariant:schema-selection-independent-review:v1",
    "invariant:schema-selection-exact-lineage:v1",
)
RECONCILIATION_INVARIANTS = (
    "invariant:exact-schema-deployment-compatibility:v1",
    "invariant:independent-graph-capability:v1",
    "invariant:no-arbitrary-cypher:v1",
    "invariant:observational-no-graph-mutation:v1",
)


def register_schema_grounding_admission_policies(
    registry: AdmissionPolicyRegistry,
) -> None:
    registry.register(SELECTION_ADMISSION, _validate_selection_admission)
    registry.register(RECONCILIATION_ADMISSION, _validate_reconciliation_admission)
    for contract_ref in SELECTION_INVARIANTS:
        registry.register(contract_ref, _validate_selection_admission)
    for contract_ref in RECONCILIATION_INVARIANTS:
        registry.register(contract_ref, _validate_reconciliation_admission)


def _validate_selection_admission(
    request: RunRequest,
    configuration: VerifiedRunConfiguration,
) -> str | None:
    if configuration.workflow_type_ref.logical_id != "schema-context-selection":
        return "selection admission contract is bound to another Workflow Type"
    return _require_evidence_prefixes(
        request,
        (
            "schema-definition:",
            "schema-catalog-build:",
            "semantic-overlay:",
            "sensitive-data-policy:",
        ),
    )


def _validate_reconciliation_admission(
    request: RunRequest,
    configuration: VerifiedRunConfiguration,
) -> str | None:
    if configuration.workflow_type_ref.logical_id != "supporting-graph-reconciliation":
        return "reconciliation admission contract is bound to another Workflow Type"
    return _require_evidence_prefixes(
        request,
        (
            "schema-definition:",
            "schema-catalog-build:",
            "schema-deployment-manifest:",
            "schema-workspace-binding:",
            "graph-capability:",
            "sensitive-data-policy:",
        ),
    )


def _require_evidence_prefixes(
    request: RunRequest,
    prefixes: tuple[str, ...],
) -> str | None:
    missing = tuple(
        prefix
        for prefix in prefixes
        if not any(reference.startswith(prefix) for reference in request.admission_evidence_refs)
    )
    if missing:
        return "missing exact admission evidence: " + ", ".join(missing)
    return None
