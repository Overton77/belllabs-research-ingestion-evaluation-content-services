from __future__ import annotations

import pytest

from scripts.evaluate_coordinator_acceptance import (
    RATE_METRICS,
    REQUIRED_METRICS,
    SCENARIO_REQUIRED_FIELDS,
    _is_empty_evidence_value,
    _retrieval_case_result,
    _validate_metrics,
)


def test_retrieval_case_requires_expected_workflow_and_every_capability() -> None:
    case = {
        "case_id": "web",
        "expected_workflow_type": "web-research",
        "expected_capability_assets": ["one", "two"],
    }
    complete = {
        "web": {
            "case_id": "web",
            "workflow_rank": 1,
            "found_capabilities": ["one", "two"],
        }
    }
    missing = {
        "web": {
            "case_id": "web",
            "workflow_rank": 1,
            "found_capabilities": ["one"],
        }
    }

    assert _retrieval_case_result(case, complete)["passed"] is True
    assert _retrieval_case_result(case, missing)["passed"] is False


def test_acceptance_metric_contract_contains_every_specified_metric() -> None:
    assert REQUIRED_METRICS == {
        "workflow_type_recall_at_k",
        "capability_recall_at_k",
        "web_capability_recall_at_k",
        "exact_identifier_mrr",
        "unauthorized_exposure_count",
        "candidate_direct_execution_count",
        "launch_preparation_success_rate",
        "admission_failure_classification_accuracy",
        "idempotency_violations",
        "prompt_injection_policy_violations",
        "unexpected_mcp_tool_exposure_count",
        "required_web_provider_use_rate",
        "browser_verification_evidence_rate",
        "median_search_latency_ms",
        "median_prepare_latency_ms",
        "catalog_tokens_loaded",
        "operator_corrections_per_plan",
    }


def test_acceptance_metrics_reject_missing_numeric_integrity() -> None:
    valid = {name: 0 for name in REQUIRED_METRICS}
    _validate_metrics(valid)

    with pytest.raises(ValueError, match="must be numeric"):
        _validate_metrics({**valid, "catalog_tokens_loaded": None})
    with pytest.raises(ValueError, match="finite and non-negative"):
        _validate_metrics({**valid, "median_prepare_latency_ms": float("nan")})
    with pytest.raises(ValueError, match="at most 1"):
        _validate_metrics(
            {**valid, next(iter(RATE_METRICS)): 1.01}
        )


def test_live_scenario_contract_requires_authoritative_evidence_fields() -> None:
    assert SCENARIO_REQUIRED_FIELDS["scenario_d"] >= {
        "temporal_run_id",
        "provider_evidence_refs",
        "browser_evidence_refs",
        "screenshot_s3_ref",
    }
    assert _is_empty_evidence_value([])
    assert _is_empty_evidence_value("  ")
    assert not _is_empty_evidence_value(0)
    assert not _is_empty_evidence_value(["evidence"])
