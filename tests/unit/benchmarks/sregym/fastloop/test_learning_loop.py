"""A live launch requires a complete proof bound to its code and image content."""

import copy

import pytest

from benchmarks.sregym.fastloop.learning_loop import STAGES, validate_report


def passing_report():
    return {
        "schema_version": "sdo.learning-loop-proof/v1",
        "passed": True,
        "code_revision": "abc",
        "lane": "lane-53",
        "images": {"controller": "sha256:c", "responder": "sha256:r", "validator": "sha256:v"},
        "live_model_calls": False,
        "seconds": 299,
        "stages": {name: {"passed": True, "evidence": {"receipt": "measured"}} for name in STAGES},
    }


def test_complete_proof_matches_code_lane_and_image_content():
    report = passing_report()
    validate_report(report, revision="abc", lane="lane-53", images=report["images"])


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("passed", False),
        ("code_revision", "old"),
        ("lane", "other"),
        ("images", {"controller": "changed"}),
        ("live_model_calls", True),
        ("seconds", 301),
        ("schema_version", "old"),
    ],
)
def test_stale_incomplete_or_model_backed_proof_is_rejected(field, value):
    expected = passing_report()
    report = copy.deepcopy(expected)
    report[field] = value
    with pytest.raises(ValueError, match="learning-loop"):
        validate_report(report, revision="abc", lane="lane-53", images=expected["images"])


@pytest.mark.parametrize("stage", STAGES)
def test_every_stage_needs_a_passing_receipt(stage):
    report = passing_report()
    report["stages"][stage] = {"passed": True}
    with pytest.raises(ValueError, match=stage):
        validate_report(report, revision="abc", lane="lane-53", images=report["images"])


def test_live_run_refuses_missing_proof_before_credentials_or_cluster_writes(tmp_path, monkeypatch):
    from benchmarks.sregym.fastloop import cli
    from benchmarks.sregym.fastloop.environment import FastloopEnvironment

    environment = FastloopEnvironment(
        run_dir=tmp_path,
        cluster="lane-53",
        kubeconfig=tmp_path / "kubeconfig",
        namespace="hotel",
        application="hotel",
        workspace=tmp_path / "workspace",
        sregym_dir=tmp_path / "sregym",
        private_tmp=None,
    )
    environment.save()

    def unexpected(*args, **kwargs):
        pytest.fail("live run accessed Kubernetes before requiring the proof")

    monkeypatch.setattr(cli, "kubectl", unexpected)
    with pytest.raises(ValueError, match="learning-loop-proof --run-dir"):
        cli.main(["run", "--run-dir", str(tmp_path), "--agent", "sdo"])
