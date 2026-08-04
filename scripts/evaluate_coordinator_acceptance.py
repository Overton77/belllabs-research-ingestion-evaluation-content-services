from __future__ import annotations

import argparse
import json
import math
import subprocess
import sys
from collections.abc import Mapping
from pathlib import Path
from statistics import median
from time import perf_counter
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DATASET = (
    PROJECT_ROOT / "tests" / "fixtures" / "coordinator_retrieval_evaluation.json"
)

REQUIRED_METRICS = frozenset(
    {
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
)
RATE_METRICS = frozenset(
    {
        "workflow_type_recall_at_k",
        "capability_recall_at_k",
        "web_capability_recall_at_k",
        "exact_identifier_mrr",
        "launch_preparation_success_rate",
        "admission_failure_classification_accuracy",
        "required_web_provider_use_rate",
        "browser_verification_evidence_rate",
    }
)
SCENARIO_REQUIRED_FIELDS: dict[str, frozenset[str]] = {
    "scenario_a": frozenset(
        {
            "workflow_id",
            "temporal_run_id",
            "launch_ticket_ref",
            "oeb_refs",
            "result_ref",
            "audit_refs",
        }
    ),
    "scenario_b": frozenset(
        {
            "artifact_ref",
            "candidate_ref",
            "inspection_report_ref",
            "promotion_request_ref",
        }
    ),
    "scenario_c": frozenset(
        {
            "workflow_id",
            "temporal_run_id",
            "launch_ticket_ref",
            "oeb_refs",
            "result_ref",
            "audit_refs",
            "iteration_count",
            "independent_verifier_ref",
        }
    ),
    "scenario_d": frozenset(
        {
            "workflow_id",
            "temporal_run_id",
            "launch_ticket_ref",
            "oeb_refs",
            "result_ref",
            "audit_refs",
            "provider_evidence_refs",
            "browser_evidence_refs",
            "screenshot_s3_ref",
        }
    ),
}


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Execute every coordinator evaluation case and merge durable live "
            "Scenario A/B/C/D evidence."
        )
    )
    parser.add_argument("--dataset", type=Path, default=DEFAULT_DATASET)
    parser.add_argument("--retrieval-report", type=Path, required=True)
    parser.add_argument("--live-evidence", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    parser.add_argument(
        "--basetemp",
        type=Path,
        default=PROJECT_ROOT / ".pytest-tmp-coordinator-acceptance",
    )
    return parser.parse_args()


def _run_evidence_module(
    evidence_test: str,
    *,
    basetemp: Path,
) -> dict[str, Any]:
    target = (PROJECT_ROOT / evidence_test).resolve()
    if not target.is_relative_to(PROJECT_ROOT) or not target.is_file():
        raise ValueError(f"invalid evidence test path: {evidence_test}")
    # Each pytest process owns and may recursively clean its basetemp. Keep
    # module roots as siblings so one process can never remove another
    # process's parent while the acceptance runner advances sequentially.
    module_temp = basetemp.with_name(f"{basetemp.name}-{target.stem}")
    command = (
        sys.executable,
        "-m",
        "pytest",
        "-q",
        str(target),
        f"--basetemp={module_temp}",
    )
    started = perf_counter()
    try:
        completed = subprocess.run(
            command,
            cwd=PROJECT_ROOT,
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=600,
        )
    except subprocess.TimeoutExpired as error:
        elapsed_ms = (perf_counter() - started) * 1_000
        return {
            "evidence_test": evidence_test,
            "command": list(command),
            "exit_code": None,
            "passed": False,
            "elapsed_ms": round(elapsed_ms, 3),
            "stdout_tail": _timeout_text(error.stdout)[-4_000:],
            "stderr_tail": _timeout_text(error.stderr)[-4_000:],
            "failure": "pytest evidence module exceeded 600 seconds",
        }
    elapsed_ms = (perf_counter() - started) * 1_000
    return {
        "evidence_test": evidence_test,
        "command": list(command),
        "exit_code": completed.returncode,
        "passed": completed.returncode == 0,
        "elapsed_ms": round(elapsed_ms, 3),
        "stdout_tail": completed.stdout[-4_000:],
        "stderr_tail": completed.stderr[-4_000:],
    }


def _timeout_text(value: str | bytes | None) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


def _retrieval_case_result(
    case: Mapping[str, Any],
    retrieval_by_id: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    case_id = str(case["case_id"])
    result = retrieval_by_id.get(case_id)
    if result is None:
        return {
            "case_id": case_id,
            "kind": "retrieval",
            "passed": False,
            "failure": "retrieval report omitted this query case",
        }
    expected_workflow = case.get("expected_workflow_type")
    expected_capabilities = {
        str(item) for item in case.get("expected_capability_assets", ())
    }
    found_capabilities = {
        str(item) for item in result.get("found_capabilities", ())
    }
    passed = (
        (not expected_workflow or result.get("workflow_rank") is not None)
        and expected_capabilities.issubset(found_capabilities)
    )
    return {
        "case_id": case_id,
        "kind": "retrieval",
        "passed": passed,
        "retrieval_evidence": dict(result),
    }


def _evaluate(
    *,
    dataset: Mapping[str, Any],
    retrieval_report: Mapping[str, Any],
    live_evidence: Mapping[str, Any],
    basetemp: Path,
) -> dict[str, Any]:
    retrieval_by_id = {
        str(item["case_id"]): item for item in retrieval_report.get("cases", ())
    }
    evidence_modules = sorted(
        {
            str(case["evidence_test"])
            for case in dataset["cases"]
            if isinstance(case.get("evidence_test"), str)
        }
    )
    module_results = {
        evidence_test: _run_evidence_module(
            evidence_test,
            basetemp=basetemp,
        )
        for evidence_test in evidence_modules
    }

    cases: list[dict[str, Any]] = []
    for case in dataset["cases"]:
        if isinstance(case.get("query"), str):
            cases.append(_retrieval_case_result(case, retrieval_by_id))
            continue
        evidence_test = str(case["evidence_test"])
        evidence = module_results[evidence_test]
        cases.append(
            {
                "case_id": str(case["case_id"]),
                "kind": "executed_pytest_evidence",
                "passed": evidence["passed"],
                "evidence_test": evidence_test,
                "module_execution_ref": f"module:{evidence_test}",
            }
        )

    retrieval_metrics = dict(retrieval_report.get("metrics", {}))
    live_metrics = dict(live_evidence.get("metrics", {}))
    metrics = {**retrieval_metrics, **live_metrics}
    missing_metrics = sorted(REQUIRED_METRICS.difference(metrics))
    unexpected_metrics = sorted(
        set(metrics).difference(REQUIRED_METRICS).difference(
            {"median_search_latency"}
        )
    )
    if "median_search_latency" in metrics and "median_search_latency_ms" not in metrics:
        metrics["median_search_latency_ms"] = metrics.pop("median_search_latency")
        missing_metrics = sorted(REQUIRED_METRICS.difference(metrics))
    if unexpected_metrics:
        raise ValueError(
            "live/retrieval evidence contains unknown metrics: "
            + ", ".join(unexpected_metrics)
        )
    _validate_metrics(metrics)

    live_scenarios = live_evidence.get("scenarios")
    if not isinstance(live_scenarios, Mapping):
        raise ValueError("live evidence must contain a scenarios object")
    required_scenarios = set(SCENARIO_REQUIRED_FIELDS)
    missing_scenarios = sorted(required_scenarios.difference(live_scenarios))
    invalid_scenario_types = sorted(
        name
        for name in required_scenarios.intersection(live_scenarios)
        if not isinstance(live_scenarios[name], Mapping)
    )
    if invalid_scenario_types:
        raise ValueError(
            "live scenario evidence must be objects: "
            + ", ".join(invalid_scenario_types)
        )
    incomplete_scenarios = sorted(
        name
        for name in required_scenarios.intersection(live_scenarios)
        if not bool(live_scenarios[name].get("passed"))
    )
    missing_scenario_fields = {
        name: sorted(
            SCENARIO_REQUIRED_FIELDS[name].difference(live_scenarios[name])
        )
        for name in required_scenarios.intersection(live_scenarios)
        if isinstance(live_scenarios[name], Mapping)
        and SCENARIO_REQUIRED_FIELDS[name].difference(live_scenarios[name])
    }
    empty_scenario_fields = {
        name: sorted(
            field
            for field in SCENARIO_REQUIRED_FIELDS[name]
            if field in live_scenarios[name]
            and _is_empty_evidence_value(live_scenarios[name][field])
        )
        for name in required_scenarios.intersection(live_scenarios)
        if isinstance(live_scenarios[name], Mapping)
        and any(
            field in live_scenarios[name]
            and _is_empty_evidence_value(live_scenarios[name][field])
            for field in SCENARIO_REQUIRED_FIELDS[name]
        )
    }

    executed_evidence_count = sum(
        1 for item in cases if item["kind"] == "executed_pytest_evidence"
    )
    passed = (
        not missing_metrics
        and not missing_scenarios
        and not incomplete_scenarios
        and not missing_scenario_fields
        and not empty_scenario_fields
        and all(bool(item["passed"]) for item in cases)
    )
    return {
        "dataset_version": dataset["dataset_version"],
        "passed": passed,
        "metrics": metrics,
        "coverage": {
            "dataset_case_count": len(cases),
            "executed_retrieval_case_count": sum(
                1 for item in cases if item["kind"] == "retrieval"
            ),
            "executed_pytest_evidence_case_count": executed_evidence_count,
            "executed_pytest_module_count": len(module_results),
            "unexecuted_case_count": 0,
            "missing_metrics": missing_metrics,
            "missing_scenarios": missing_scenarios,
            "incomplete_scenarios": incomplete_scenarios,
            "missing_scenario_fields": missing_scenario_fields,
            "empty_scenario_fields": empty_scenario_fields,
        },
        "cases": cases,
        "pytest_modules": list(module_results.values()),
        "live_evidence": dict(live_evidence),
        "timing": {
            "median_evidence_module_latency_ms": (
                round(
                    median(
                        float(item["elapsed_ms"]) for item in module_results.values()
                    ),
                    3,
                )
                if module_results
                else 0.0
            )
        },
    }


def _validate_metrics(metrics: Mapping[str, Any]) -> None:
    for name in REQUIRED_METRICS.intersection(metrics):
        value = metrics[name]
        if isinstance(value, bool) or not isinstance(value, int | float):
            raise ValueError(f"acceptance metric {name} must be numeric")
        if not math.isfinite(value) or value < 0:
            raise ValueError(
                f"acceptance metric {name} must be finite and non-negative"
            )
        if name in RATE_METRICS and value > 1:
            raise ValueError(f"acceptance rate metric {name} must be at most 1")


def _is_empty_evidence_value(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    if isinstance(value, list | tuple | set | frozenset | dict):
        return not value
    return False


def main() -> None:
    args = _arguments()
    dataset = json.loads(args.dataset.resolve(strict=True).read_text(encoding="utf-8"))
    retrieval_report = json.loads(
        args.retrieval_report.resolve(strict=True).read_text(encoding="utf-8")
    )
    live_evidence = json.loads(
        args.live_evidence.resolve(strict=True).read_text(encoding="utf-8")
    )
    report = _evaluate(
        dataset=dataset,
        retrieval_report=retrieval_report,
        live_evidence=live_evidence,
        basetemp=args.basetemp.resolve(),
    )
    rendered = json.dumps(report, sort_keys=True, indent=2) + "\n"
    if args.output is not None:
        output = args.output.resolve()
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    if not report["passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
