"""A fault whose own source reads Secret data stops the row at start, not after onboarding."""

from pathlib import Path

import pytest

from benchmarks.sregym.fastloop.lane_guard import FaultNotInstallableError, LaneGuard, assert_faults_installable
from benchmarks.sregym.faults.description import FaultSpec, ProblemDescription

READS_DATA = """
class ReadsSecretData:
    def check(self):
        return self._run(f"kubectl get secret {self.secret_name} -n {self.namespace} -o json")


class ReadsNamesOnly:
    def check(self):
        return self._run(f"kubectl get secret {self.secret_name} -n {self.namespace} -o name")
"""


def description(class_name: str) -> dict[str, ProblemDescription]:
    fault = FaultSpec(kind="kafka_queue_problems", component="kafka", class_name=class_name)
    return {"synthetic": ProblemDescription(problem_id="synthetic", faults=(fault,), injection_order=(0,))}


@pytest.fixture
def sregym_dir(tmp_path: Path) -> Path:
    problems = tmp_path / "sregym" / "conductor" / "problems"
    problems.mkdir(parents=True)
    (problems / "synthetic.py").write_text(READS_DATA)
    return tmp_path


def guard() -> LaneGuard:
    return LaneGuard("luna-pilot-50", Path("/owned"), "row-start")


def test_fault_that_reads_secret_data_is_not_installable(sregym_dir):
    with pytest.raises(FaultNotInstallableError, match=r"fault_not_installable: ReadsSecretData.*-o json"):
        assert_faults_installable(description("ReadsSecretData"), sregym_dir, guard())


def test_fault_that_reads_secret_names_only_is_installable(sregym_dir):
    assert_faults_installable(description("ReadsNamesOnly"), sregym_dir, guard())
    assert_faults_installable(description("NotInTheSource"), sregym_dir, guard())
