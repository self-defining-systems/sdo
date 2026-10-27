"""Hand-written shell admission evidence: no Go programs or prober exist in this arm."""

import json
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from benchmarks.sregym.adapter import eval_guards
from benchmarks.sregym.fastloop import admission, hotel_helpers, rbac

COMMIT = "a" * 40
SCRIPT = ".sdo/diagnostics/detectors/health/application/ready.sh"
BOUNDARY = "2026-10-26T08:47:00+00:00"
AT = "2026-10-26T08:48:00+00:00"


def shell_evidence():
    return {
        "accepted_commit": COMMIT,
        "scripts": [SCRIPT],
        "report": {
            "version": 1,
            "accepted_commit": COMMIT,
            "activated_at": BOUNDARY,
            "at": AT,
            "healthy": True,
            "results": [{"script": SCRIPT, "exit_code": 0, "at": AT, "fired": False}],
        },
    }


def test_shell_runner_proof_accepts_its_real_evidence_shape():
    assert eval_guards.shell_runner_proof(shell_evidence(), BOUNDARY, AT)["passed"]


@pytest.mark.parametrize(
    "mutation",
    [
        "empty",
        "missing",
        "wrong_commit",
        "unhealthy",
        "error",
        "nonzero",
        "timeout",
        "stale",
        "future",
        "duplicate",
        "incident_only",
        "activation_after_run",
        "invalid_version",
    ],
)
def test_shell_proof_rejects_incomplete_or_unhealthy_evidence(mutation):
    evidence = shell_evidence()
    report = evidence["report"]
    row = report["results"][0]
    if mutation == "empty":
        evidence["scripts"] = []
        report["results"] = []
    elif mutation == "missing":
        evidence["scripts"].append(SCRIPT.replace("ready", "traffic"))
    elif mutation == "wrong_commit":
        report["accepted_commit"] = "b" * 40
    elif mutation == "unhealthy":
        report["healthy"] = False
    elif mutation == "error":
        row["error"] = "runner crashed"
    elif mutation == "nonzero":
        row["exit_code"] = 1  # a first blip is not a fired finding, but is not healthy
    elif mutation == "timeout":
        row["timed_out"] = True
    elif mutation == "stale":
        row["at"] = "2026-10-26T08:46:00Z"
    elif mutation == "future":
        row["at"] = "2026-10-26T09:48:00Z"
    elif mutation == "duplicate":
        report["results"].append(deepcopy(row))
    elif mutation == "incident_only":
        evidence["scripts"] = [SCRIPT.replace("/health/", "/incidents/")]
        row["script"] = evidence["scripts"][0]
    elif mutation == "activation_after_run":
        report["activated_at"] = "2026-10-26T08:49:00Z"
    else:
        report["version"] = 2
    proof = eval_guards.shell_runner_proof(evidence, BOUNDARY, AT)
    assert proof["passed"] is False
    assert proof["reason"]


@pytest.mark.parametrize("code_path", ["shell", "go"])
@pytest.mark.parametrize(
    ("runner_healthy", "controller_clear", "traffic_healthy"),
    [(True, True, True), (False, True, True), (True, False, True), (True, True, False)],
)
def test_public_admission_uses_selected_arm_and_keeps_independent_traffic(
    tmp_path, monkeypatch, code_path, runner_healthy, controller_clear, traffic_healthy
):
    traffic = tmp_path / ".sdo/diagnostics/traffic/workloads"
    traffic.mkdir(parents=True)
    (traffic / "health.yaml").write_text("purpose: health-probe\nscenarios:\n- id: frontend\n")
    environment = SimpleNamespace(
        workspace=tmp_path, kubeconfig=tmp_path / "kubeconfig", namespace="app", deploy_problem="fixture"
    )
    calls = []
    keys = (
        {"shell.detectors"}
        if code_path == "shell"
        else {"workload-health", "service-endpoints", "service-reachability", "traffic-health"}
    )

    def state(_control):
        return {
            "history": [
                {
                    "detector_id": key,
                    "status": "clear" if controller_clear else "error",
                    "evaluated_at": datetime.now(timezone.utc).isoformat(),
                }
                for key in keys
            ]
        }

    def request(operation, **kwargs):
        assert operation == "baseline_oracle"
        assert kwargs["verify_traffic"] is True
        calls.append("oracle")
        return {"success": traffic_healthy}

    gate = admission.LaneAdmission(
        environment,
        SimpleNamespace(request=request),
        SimpleNamespace(runtime_state=state),
        tmp_path / "admission",
        code_path=code_path,
    )
    complete = {"complete": {"acceptedCommit": "a" * 40, "objects": [{}]}, "pending": None}
    monkeypatch.setattr(admission.LaneAdmission, "_onboarding", lambda self, control: complete)
    monkeypatch.setattr(gate, "check_resources", lambda *args: None)
    monkeypatch.setattr(rbac, "verify_rbac", lambda *args: {"passed": True})
    monkeypatch.setattr(hotel_helpers, "admit_install", lambda *args: {"passed": True})
    monkeypatch.setattr(admission, "workload_identity", lambda *args: {"Deployment/app": {"generation": 1}})
    monkeypatch.setattr(admission, "assert_clean_baseline", lambda *args, **kwargs: None)

    def results(_control):
        assert code_path == "shell", "Go admission must not use shell evidence"
        evidence = shell_evidence()
        at = datetime.now(timezone.utc)
        evidence["report"].update(
            at=at.isoformat(), activated_at=(at - timedelta(seconds=30)).isoformat(), healthy=runner_healthy
        )
        evidence["report"]["results"][0]["at"] = at.isoformat()
        return evidence

    monkeypatch.setattr(gate, "_shell_results", results)

    def windows(_control):
        assert code_path == "go", "Shell admission must not require the Go prober"
        calls.append("prober")
        return {
            "windows": {"health": {"scenarios": [{"scenario": {"id": "frontend"}, "qualified": True, "samples": [{}]}]}}
        }

    monkeypatch.setattr(gate, "_windows", windows)

    def wait(sample, record, **kwargs):
        proof = sample()
        record(proof)
        return proof

    monkeypatch.setattr(admission, "wait_for_proof", wait)
    proof = gate.prove("app-sdo")
    ready = controller_clear and (runner_healthy or code_path == "go")
    assert proof["passed"] is (ready and traffic_healthy)
    assert ("oracle" in calls) is ready
    assert ("prober" in calls) is (code_path == "go")
    assert json.loads((gate.directory / "stabilization-001.jsonl").read_text())["code_path"] == code_path


@pytest.mark.parametrize("code_path", ["go", "shell"])
def test_public_cli_passes_the_same_code_path_to_runtime_and_admission(tmp_path, monkeypatch, code_path):
    from benchmarks.sregym.fastloop import cli
    from benchmarks.sregym.fastloop.environment import FastloopEnvironment

    environment = FastloopEnvironment(
        run_dir=tmp_path,
        cluster="fixture",
        kubeconfig=tmp_path / "kubeconfig",
        namespace="app",
        application="fixture",
        workspace=tmp_path,
        sregym_dir=tmp_path,
        private_tmp=None,
    )
    monkeypatch.setenv("SREGYM_BASE_KUBECONFIG", str(environment.kubeconfig))
    monkeypatch.setenv("KUBECONFIG", str(environment.kubeconfig))
    args = cli.build_parser().parse_args(
        ["run", "--run-dir", str(tmp_path), "--agent", "sdo", "--code-path", code_path]
    )
    gates = []
    constructor = admission.LaneAdmission

    def capture(*args, **kwargs):
        gate = constructor(*args, **kwargs)
        gates.append(gate)
        return gate

    monkeypatch.setattr(admission, "LaneAdmission", capture)
    agent = cli._sdo_agent(args, environment, tmp_path / "results", worker=object())
    assert agent.stage_inputs("stage", tmp_path, "fp").runtime_config.code_path == code_path
    assert len(gates) == 1
    assert gates[0].code_path == code_path


def test_shell_transport_reads_committed_executable_inventory_not_dirty_files(tmp_path, monkeypatch):
    import contextlib
    import io
    import subprocess
    import urllib.request

    for args in (["init"], ["config", "user.name", "fixture"], ["config", "user.email", "fixture@invalid"]):
        subprocess.run(["git", "-C", str(tmp_path), *args], check=True, capture_output=True)
    script = tmp_path / SCRIPT
    script.parent.mkdir(parents=True)
    script.write_text("exit 0\n")
    script.chmod(0o755)
    note = script.with_name("notes.sh")
    note.write_text("not executable\n")
    subprocess.run(["git", "-C", str(tmp_path), "add", "."], check=True)
    subprocess.run(["git", "-C", str(tmp_path), "commit", "-qm", "fixture"], check=True)
    committed = subprocess.check_output(["git", "-C", str(tmp_path), "rev-parse", "HEAD"], text=True).strip()
    script.unlink()
    note.chmod(0o755)
    note.with_name("unaccepted.sh").write_text("exit 1\n")
    report = shell_evidence()["report"]
    report["accepted_commit"] = committed

    def read(url, timeout):
        assert url == "http://sdo-shell-detectors:8080/results"
        assert timeout == 5
        return io.StringIO(json.dumps(report))

    monkeypatch.setattr(urllib.request, "urlopen", read)

    def kubectl(args, **kwargs):
        assert kwargs["namespace"] == "app-sdo"
        if args[0] == "get":
            assert args[1] == "job/sdo-controller-run"
            return SimpleNamespace(
                stdout=json.dumps(
                    {
                        "spec": {
                            "template": {
                                "spec": {"containers": [{"name": "controller", "args": ["--app", str(tmp_path)]}]}
                            }
                        }
                    }
                )
            )
        assert args[:7] == ["exec", "job/sdo-controller-run", "-c", "controller", "--", "python3", "-c"]
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            exec(compile(args[7], "admission-read", "exec"), {})
        return SimpleNamespace(stdout=output.getvalue())

    monkeypatch.setattr(admission, "kubectl", kubectl)
    env = SimpleNamespace(workspace=tmp_path, kubeconfig=tmp_path / "kubeconfig")
    gate = admission.LaneAdmission(env, object(), object(), tmp_path / "admission", code_path="shell")
    evidence = gate._shell_results("app-sdo")
    assert evidence["accepted_commit"] == committed
    assert evidence["scripts"] == [SCRIPT]
    assert eval_guards.shell_runner_proof(evidence, BOUNDARY, AT)["passed"]
