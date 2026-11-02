import shutil
from pathlib import Path

import pytest

from benchmarks.sregym.fastloop.judge_script import judge_script
from sdo.contracts import HealthJudgeRequest

SEED = Path(__file__).resolve().parents[5] / "apps/toy/seed/.sdo"
REQUEST = HealthJudgeRequest(application="toy", namespace="sdo-toy", accepted_source_commit="a" * 40)


def test_toy_seed_gets_explicit_objective_detector_from_its_own_policy(tmp_path):
    shutil.copytree(SEED, tmp_path / ".sdo")
    turn = judge_script(checkout=tmp_path, request=REQUEST).turns[0]
    assert turn.role == "health-judge"
    assert turn.result["detector_ids"] == ["health-objective", "toy-endpoints"]
    assert turn.result["authored"] is True
    writes = {step.path: step.text for step in turn.steps}
    assert sorted(writes) == [
        ".sdo/diagnostics/detectors/health/health-objective/detector.go",
        ".sdo/diagnostics/detectors/health/health-objective/manifest.yaml",
    ]
    assert all("toy-endpoints" not in text for text in writes.values())


def test_judge_script_is_idempotent_once_the_objective_detector_exists(tmp_path):
    shutil.copytree(SEED, tmp_path / ".sdo")
    health = tmp_path / ".sdo/diagnostics/detectors/health"
    shutil.copytree(health / "toy-endpoints", health / "health-objective")
    manifest = health / "health-objective/manifest.yaml"
    manifest.write_text(manifest.read_text().replace("toy-endpoints", "health-objective"))
    assert judge_script(checkout=tmp_path, request=REQUEST).turns[0].steps == []


def test_judge_script_refuses_a_seed_without_judge_owned_health_detectors(tmp_path):
    (tmp_path / ".sdo").mkdir()
    with pytest.raises(ValueError, match="judge-owned health detectors"):
        judge_script(checkout=tmp_path, request=REQUEST)
