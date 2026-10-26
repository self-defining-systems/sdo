"""Toy streams and reusable faults load without SREGym or a cluster."""

from pathlib import Path

import pytest

from apps.toy.faults import SERVICES, fault_spec
from benchmarks.sregym.fastloop.cli import build_parser
from benchmarks.sregym.fastloop.stream import load_stream_config

ROOT = Path(__file__).resolve().parents[5]


def test_toy_recovery_meets_the_warm_lane_protocol(tmp_path, monkeypatch):
    import json
    from types import SimpleNamespace

    from benchmarks.sregym.fastloop.baseline_recovery import ApplicationBaseline
    from benchmarks.sregym.fastloop.fault_driver import SregymFaultDriver
    from benchmarks.sregym.fastloop.toy import ToyWorker

    deployments = [{"spec": {"replicas": 1}, "status": {"readyReplicas": 1}}] * 3
    monkeypatch.setattr("benchmarks.sregym.fastloop.toy.command", lambda *args: json.dumps({"items": deployments}))
    env = SimpleNamespace(kubeconfig=tmp_path / "config", namespace="toy", workspace=tmp_path)
    cluster = SimpleNamespace(list_objects=lambda namespace: [], delete=lambda *args: None, apply=lambda *args: None)
    driver = SregymFaultDriver(
        ToyWorker(env),
        namespace="toy",
        baseline=ApplicationBaseline(namespace="toy", objects=[]),
        cluster=cluster,
        require_full_cleanup=True,
    )
    driver.recover()
    assert driver.last_full_cleanup_verified


def test_toy_latent_stream_covers_three_services_and_health_control():
    stream = load_stream_config(ROOT / "tests/fixtures/sregym/fastloop/toy_latent_repeat.json")
    specs = [fault_spec(ids[0]) for ids in stream.incidents]
    assert [spec.service for spec in specs[:3]] == list(SERVICES)
    assert all(spec.kind == "target-port" for spec in specs[:3])
    assert specs[3].kind == "image"
    assert stream.diagnosis_grading == "off"
    assert fault_spec("toy_readiness_backend").kind == "readiness"
    with pytest.raises(ValueError, match="unsupported toy fault"):
        fault_spec("toy_target_port_not-a-service")


def test_toy_up_does_not_require_sregym_seed():
    args = build_parser().parse_args(["up", "--app", "toy", "--run-dir", "/tmp/toy"])
    assert args.app == "toy"
    assert args.seed is None


def test_scripted_source_restore_repairs_declared_readiness(monkeypatch):
    """A readiness fault must be repairable through the same accepted-source tool."""
    import json
    from types import SimpleNamespace

    from benchmarks.sregym.adapter import scripted_worker

    probe = {"httpGet": {"path": "/ready", "port": 8080}, "periodSeconds": 1}
    source = {
        "spec": {
            "template": {"spec": {"containers": [{"name": "backend", "image": "toy:v1", "readinessProbe": probe}]}}
        }
    }
    monkeypatch.setattr(scripted_worker, "_tool", lambda *args: {"source": source})
    calls = []
    monkeypatch.setattr(scripted_worker, "_command", lambda args, **kwargs: calls.append(args))
    request = SimpleNamespace(namespace="toy", state_changes=None)
    scripted_worker._patch_from_source(request, {"kind": "Deployment", "name": "backend"})
    patched = json.loads(calls[0][-1])["spec"]["template"]["spec"]["containers"][0]
    assert patched["readinessProbe"] == probe
    assert calls[1][:5] == ["kubectl", "-n", "toy", "rollout", "status"]


def test_toy_seed_has_broker_metadata():
    import yaml

    seed = ROOT / "apps/toy/seed/.sdo"
    for filename in ("goal.md", "arch.md", "playbooks/health-objective/README.md"):
        content = (seed / filename).read_text()
        assert content.startswith("---\n"), filename
        metadata = yaml.safe_load(content.split("---", 2)[1])
        assert metadata["schema_version"] == 1
    assert yaml.safe_load((seed / "goal.md").read_text().split("---", 2)[1])["owner"] == "human"


def test_readiness_uses_latest_evaluation_after_a_closed_incident():
    from benchmarks.sregym.fastloop.toy import baseline_ready

    state = {
        "evaluation_iteration": 3,
        "incident_open": False,
        "ownership_adoption": {"source_commit": "accepted"},
        "history": [
            {"detector_id": "drift/investigator", "status": "firing"},
            {"detector_id": "drift/investigator", "status": "clear"},
            {"detector_id": "toy-endpoints", "status": "clear"},
        ],
    }
    assert baseline_ready(state, "accepted")
    assert not baseline_ready(state, "different")
    state["history"].append({"detector_id": "toy-endpoints", "status": "firing"})
    assert not baseline_ready(state, "accepted")


def test_toy_kind_configuration_requires_network_policy_enforcement(tmp_path, monkeypatch):
    from subprocess import CompletedProcess

    import yaml

    from benchmarks.sregym.fastloop.dev import prepare_dev_mount

    worktree = tmp_path / "source"
    (worktree / "sdo/operator").mkdir(parents=True)
    (worktree / "sdo/operator/go.mod").write_text("module fixture\n")

    def build(*args, **kwargs):
        Path(args[0][args[0].index("-o") + 1]).write_bytes(b"synthetic operator")
        return CompletedProcess(args, 0)

    monkeypatch.setattr("benchmarks.sregym.fastloop.dev.subprocess.run", build)
    config = yaml.safe_load(prepare_dev_mount(worktree, None, tmp_path).read_text())
    assert config["networking"]["disableDefaultCNI"]
    assert config["networking"]["podSubnet"] == "192.168.0.0/16"


def test_network_policy_oracle_checks_actual_cluster_policy(monkeypatch):
    import json

    from apps.toy import faults

    spec = faults.fault_spec("toy_network_policy_backend")
    assert spec.kind == "network-policy"
    policies = [{"metadata": {"name": "isolate-backend"}}]
    calls = []

    def command(*args, **kwargs):
        calls.append((args, kwargs))
        return json.dumps({"items": policies})

    monkeypatch.setattr(faults, "command", command)
    assert not spec.oracle("config", "toy")
    policies.clear()
    assert spec.oracle("config", "toy")
    spec.inject("config", "toy")
    manifest = json.loads(calls[-1][1]["input_text"])
    assert manifest["spec"] == {
        "podSelector": {"matchLabels": {"app": "backend"}},
        "policyTypes": ["Ingress"],
        "ingress": [],
    }


def test_sweep_stream_models_an_unowned_service_without_changing_controller():
    config = load_stream_config(ROOT / "tests/fixtures/sregym/fastloop/toy_sweep.json")
    spec = fault_spec(config.incidents[0][0])
    assert spec.kind == "target-port"
    assert spec.clear_field_ownership
    with pytest.raises(ValueError, match="only target-port"):
        fault_spec("toy_readiness_unowned_backend")


@pytest.mark.parametrize(
    "change", ["none", "proof", "namespace", "objective", "topology", "missing-detector", "changed-detector"]
)
def test_toy_scripted_health_refresh_needs_no_rehearsal_slot(tmp_path, monkeypatch, change):
    import shutil

    import yaml

    from benchmarks.sregym.adapter import scripted_worker
    from sdo.contracts import HealthJudgeRequest, SourceResource

    monkeypatch.delenv("SDO_SCRIPTED_REPLAY_SLOT", raising=False)
    seed = ROOT / "apps/toy/seed/.sdo"
    shutil.copytree(seed, tmp_path / ".sdo")
    body = (seed / "goal.md").read_text().split("---\n", 2)[2].strip()
    namespace = "sdo-proof-0123456789" if change == "proof" else "sdo-toy"
    resources = []
    for path in sorted((ROOT / "apps/toy/kubernetes").glob("*.yaml")):
        resources.extend(
            SourceResource(
                kind=obj["kind"],
                namespace=obj["metadata"].get("namespace", namespace),
                name=obj["metadata"]["name"],
                source=f"kubernetes/{path.name}",
            )
            for obj in yaml.safe_load_all(path.read_text())
        )
    request = HealthJudgeRequest(
        namespace="other" if change == "namespace" else namespace,
        health_objective="changed" if change == "objective" else body,
        accepted_source_commit="a" * 40,
        source_resources=resources,
    )
    detector = tmp_path / ".sdo/diagnostics/detectors/health/toy-endpoints/detector.go"
    if change == "topology":
        request.source_resources.add(
            kind="Deployment", namespace="sdo-toy", name="unexpected", source="kubernetes/new.yaml"
        )
    if change == "missing-detector":
        detector.unlink()
    if change == "changed-detector":
        detector.write_text("package toy\n")
    if change in {"none", "proof"}:
        result = scripted_worker.scripted_health_judge(request, tmp_path)
        assert result.authored
        assert "toy-endpoints" in result.detector_ids
        assert "health-objective" in result.detector_ids
        assert (tmp_path / ".sdo/diagnostics/detectors/health/health-objective/detector.go").is_file()
    else:
        with pytest.raises(ValueError, match="scripted toy health refresh"):
            scripted_worker.scripted_health_judge(request, tmp_path)


def test_ready_baseline_retains_verified_install_identity_after_health_refresh():
    from benchmarks.sregym.fastloop.toy import baseline_ready

    state = {
        "evaluation_iteration": 2,
        "history": [{"detector_id": "health", "status": "clear"}],
        "ownership_adoption": {"source_commit": "health-refresh"},
        "namespace_onboarding": {"accepted_commit": "installed", "objects": [{"name": "frontend"}]},
    }
    assert baseline_ready(state, "installed")
    assert not baseline_ready(state, "unrelated")
    state["namespace_onboarding"]["objects"] = []
    assert not baseline_ready(state, "installed")
