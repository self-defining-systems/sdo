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


def validator_job():
    return {
        "metadata": {"name": "validator-1"},
        "status": {"succeeded": 1},
        "spec": {
            "template": {
                "spec": {
                    "volumes": [{"name": "application", "persistentVolumeClaim": {"claimName": "repo"}}],
                    "containers": [
                        {
                            "name": "validator",
                            "env": [{"name": "SDO_CANDIDATE_TREE", "value": "tree"}],
                            "volumeMounts": [
                                {"name": "application", "mountPath": path, "readOnly": True}
                                for path in ("/workspace", "/sdo/healthy-baseline")
                            ],
                        }
                    ],
                }
            }
        },
    }


def test_real_validator_proof_requires_single_pvc_and_bound_candidate():
    from benchmarks.sregym.fastloop.learning_loop import validator_jobs

    job = validator_job()
    assert validator_jobs({"items": [job]}, repository_pvc="repo", candidate_tree="tree") == ["validator-1"]
    assert validator_jobs({"items": [job]}, repository_pvc="repo", candidate_tree="old") == []
    job["spec"]["template"]["spec"]["volumes"].append(
        {"name": "healthy-baseline", "persistentVolumeClaim": {"claimName": "repo"}}
    )
    assert validator_jobs({"items": [job]}, repository_pvc="repo", candidate_tree="tree") == []


@pytest.mark.parametrize("defect", ["not_started", "no_baseline", "writable", "wrong_pvc"])
def test_validator_success_cannot_hide_mount_or_execution_defects(defect):
    from benchmarks.sregym.fastloop.learning_loop import validator_jobs

    job = validator_job()
    spec = job["spec"]["template"]["spec"]
    if defect == "not_started":
        job["status"] = {"active": 1}
    elif defect == "no_baseline":
        spec["containers"][0]["volumeMounts"].pop()
    elif defect == "writable":
        spec["containers"][0]["volumeMounts"][0]["readOnly"] = False
    else:
        spec["volumes"][0]["persistentVolumeClaim"]["claimName"] = "other"
    assert validator_jobs({"items": [job]}, repository_pvc="repo", candidate_tree="tree") == []


def test_dirty_code_cannot_reuse_a_proof_with_the_same_dirty_label():
    report = passing_report()
    report["code_revision"] = "abc-dirty"
    with pytest.raises(ValueError, match="committed code"):
        validate_report(report, revision="abc-dirty", lane="lane-53", images=report["images"])


def test_validator_watch_retains_terminal_evidence_after_job_deletion(tmp_path):
    import json
    import sys
    import time

    from benchmarks.sregym.fastloop.learning_loop_watch import ValidatorWatch

    job = validator_job()
    job["kind"] = "Job"
    job["metadata"]["uid"] = "unique"
    event = {"type": "DELETED", "object": job}
    watch = ValidatorWatch(tmp_path / "kubeconfig", "control", tmp_path / "events.jsonl")
    watch.command = [
        sys.executable,
        "-c",
        "import time; print(" + repr(json.dumps(event, indent=2)) + ", flush=True); time.sleep(10)",
    ]
    with watch:
        deadline = time.monotonic() + 3
        while not watch.path.exists() or not watch.path.read_text().strip():
            assert time.monotonic() < deadline
            time.sleep(0.01)
        assert watch.jobs()["items"] == [job]


def test_supervisor_rejects_a_green_report_from_a_failed_worker(tmp_path, monkeypatch):
    import json
    import subprocess
    import sys

    from benchmarks.sregym.fastloop import dry_run

    real_popen = subprocess.Popen
    report = tmp_path / "eval-dry-run/report.json"
    script = (
        "import json; from pathlib import Path; "
        f"Path({str(report)!r}).write_text(json.dumps({{'passed': True}})); raise SystemExit(1)"
    )
    monkeypatch.setattr(
        dry_run.subprocess,
        "Popen",
        lambda argv, **kwargs: real_popen(
            [sys.executable, "-c", script] if argv[0] == sys.executable else argv, **kwargs
        ),
    )
    assert dry_run.execute(tmp_path, 5, 5) == 1
    assert json.loads(report.read_text())["passed"] is False


def test_toy_recovery_never_deletes_other_application_or_cluster_faults(tmp_path, monkeypatch):
    import json
    from types import SimpleNamespace

    from benchmarks.sregym.fastloop import clean_start

    own = {"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "own", "namespace": "proof"}}
    other = {"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "pending", "namespace": "hotel"}}
    cluster = {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRole", "metadata": {"name": "pending"}}
    objects = [own, other, cluster]
    mutations = []
    monkeypatch.setattr(clean_start, "_injected_objects", lambda _: list(objects))

    def run(args, **kwargs):
        mutations.append((args, kwargs["namespace"]))
        objects.remove(own)
        return SimpleNamespace(stdout=json.dumps({}))

    monkeypatch.setattr(clean_start, "kubectl", run)
    clean_start.remove_injected_objects(tmp_path / "kubeconfig", only_namespace="proof")
    assert objects == [other, cluster]
    assert mutations == [
        (
            [
                "delete",
                "configmap/own",
                "--ignore-not-found=true",
                "--cascade=foreground",
                "--wait=true",
                "--timeout=60s",
            ],
            "proof",
        )
    ]


def test_proof_namespace_is_separate_and_stable_for_the_lane_environment(tmp_path):
    from benchmarks.sregym.fastloop.learning_loop_fixture import namespace_for

    assert namespace_for(tmp_path).startswith("sdo-proof-")
    assert namespace_for(tmp_path) == namespace_for(tmp_path / ".")
    assert namespace_for(tmp_path) != namespace_for(tmp_path / "other")
