"""Benchmark-only no-fault rehearsals, bound to the application seed and harness.

A red healthy-baseline oracle cannot measure repair. Keep its raw verdict as
an audit record, exclude it from grading, and still require collateral/traffic.
No oracle facts cross into production SDO requests or operational memory.
"""

from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path  # noqa: TC003
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable


class OracleBaselines:
    def __init__(self, path: Path, identity: str) -> None:
        if not identity:
            raise ValueError("oracle rehearsal requires an app/seed/harness identity")
        self.path = path
        self.identity = identity
        self.grades: dict[str, dict[str, Any]] = {}
        if path.exists():
            saved = json.loads(path.read_text())
            if saved.get("identity") == identity:
                self.grades = saved["grades"]

    def rehearse(
        self, problem_id: str, evaluate: Callable[[], dict[str, Any]], *, fresh: bool = False
    ) -> dict[str, Any]:
        """Evaluate once while healthy, before any of the planned faults is injected."""
        if problem_id not in self.grades or fresh:
            grade = evaluate()
            details = grade.get("details", {})
            # An unhealthy app is not evidence that a fault oracle is invalid.
            if details.get("collateral_ok") is False or details.get("traffic_ok") is False:
                return grade
            if problem_id in self.grades:
                return self.apply(problem_id, grade)
            self.grades[problem_id] = deepcopy(grade)
            self.path.parent.mkdir(parents=True, exist_ok=True)
            temporary = self.path.with_suffix(".tmp")
            temporary.write_text(json.dumps({"identity": self.identity, "grades": self.grades}, indent=2) + "\n")
            temporary.replace(self.path)
        return self.apply(problem_id, self.grades[problem_id])

    def apply(self, problem_id: str, grade: dict[str, Any]) -> dict[str, Any]:
        baseline = self.grades.get(problem_id)
        if baseline is None:
            return grade
        result = deepcopy(grade)
        details = result.setdefault("details", {})
        healthy = baseline.get("details", {})
        baseline_parts = healthy.get("oracles")
        if isinstance(baseline_parts, list):
            parts = details.get("oracles", [])
            if len(parts) != len(baseline_parts):
                raise ValueError("oracle component contract changed since healthy rehearsal")
            invalid = [i for i, part in enumerate(baseline_parts) if part.get("success") is not True]
            if not invalid:
                return grade
            if any(
                part.get("name") != reference.get("name") for part, reference in zip(parts, baseline_parts, strict=True)
            ):
                raise ValueError("oracle component identities changed since healthy rehearsal")
            for i in invalid:
                parts[i]["raw_verdict"] = deepcopy(parts[i])
                parts[i]["status"] = "oracle_invalid"
                parts[i]["baseline_verdict"] = deepcopy(baseline_parts[i])
                parts[i]["success"] = None
                details[f"fault_{i}_verdict"] = None
                details[f"fault_{i}_status"] = "oracle_invalid"
                details.pop(f"fault_{i}_infra_error", None)
            valid = [part for i, part in enumerate(parts) if i not in invalid]
            details["faults_total"] = len(valid)
            details["faults_fixed"] = sum(part.get("success") is True for part in valid)
            details["oracle_invalid"] = len(invalid)
            if invalid:
                result["success"] = (
                    all(part.get("success") is True for part in valid)
                    and details.get("collateral_ok") is True
                    and details.get("traffic_ok") is True
                )
                if not any(part.get("infra_error") for part in valid):
                    details.pop("infra_error", None)
                details["success"] = result["success"]
                details["accuracy"] = 100.0 if result["success"] else 0.0
        else:
            invalid_single = baseline.get("success") is not True
            if not invalid_single:
                return grade
            details["oracle_invalid"] = 1
            if invalid_single:
                details.update(status="oracle_invalid", baseline_verdict=healthy, faults_total=0, faults_fixed=0)
                details["raw_verdict"] = deepcopy(grade.get("details", {}))
                details["success"] = None
                result["success"] = True
        return result


def install_oracle_baselines(
    worker: Any, baselines: OracleBaselines, *, confirm_healthy: Callable[[Any], bool] | None = None
) -> Callable[..., dict[str, Any]]:
    """Bind factory identities and the real worker's grading consumer to one cache."""
    factory, grade = worker._problem_factory, worker._grade

    def identified(problem_id: str) -> Any:
        problem = factory(problem_id)
        problem._baseline_problem_id = problem_id
        return problem

    def filtered(problem: Any, oracle: Any = None) -> dict[str, Any]:
        result = grade(problem, oracle)
        # Component-only probes retain their raw verdict; exclusions apply to
        # aggregate scoring, where fault counts and collateral are available.
        if oracle is not None:
            return result
        filtered_grade = baselines.apply(problem._baseline_problem_id, result)
        if filtered_grade.get("details", {}).get("status") == "oracle_invalid" and confirm_healthy is not None:
            # A plain problem has no aggregate collateral/traffic oracle. Once
            # its fault oracle is excluded, grade the current application.
            filtered_grade["success"] = confirm_healthy(problem)
        return filtered_grade

    def rehearse(problem_id: str, *, fresh: bool = False) -> dict[str, Any]:
        def evaluate() -> dict[str, Any]:
            problem = identified(problem_id)
            if confirm_healthy is not None and not confirm_healthy(problem):
                return {"kind": "health-check", "success": False, "details": {"collateral_ok": False}}
            return grade(problem)

        return baselines.rehearse(problem_id, evaluate, fresh=fresh)

    worker._problem_factory = identified
    worker._grade = filtered
    return rehearse
