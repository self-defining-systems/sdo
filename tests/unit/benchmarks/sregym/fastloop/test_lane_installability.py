"""Plan admission uses recorded lane facts before any setup."""

import pytest

from benchmarks.sregym.fastloop.lane_installability import LaneTopology, lane_plan
from benchmarks.sregym.faults.description import FaultSpec, ProblemDescription


def description(kind, class_name=""):
    return ProblemDescription(
        problem_id="synthetic", faults=(FaultSpec(kind, "frontend", class_name=class_name),), injection_order=(0,)
    )


@pytest.mark.parametrize(
    ("kind", "feature"),
    [
        ("internal_traffic_policy_local_astronomy_shop", "two worker"),
        ("secret_rotation_stale_env_credentials_astronomy_shop", "Secret"),
        ("hpa_missing_effective_cpu_request", "metrics-server"),
        ("network_policy", "network-policy"),
    ],
)
def test_missing_lane_prerequisites_are_plan_results(kind, feature, tmp_path):
    report = lane_plan({"synthetic": description(kind)}, LaneTopology(worker_nodes=1), tmp_path)
    assert report["synthetic"].status == "not_runnable_on_lane"
    assert feature in " ".join(report["synthetic"].reasons)


def test_two_workers_admit_local_traffic_and_unrelated_fault(tmp_path):
    catalog = {
        "local": description("internal_traffic_policy_local_astronomy_shop"),
        "other": description("target_port"),
    }
    assert all(
        item.status == "runnable" for item in lane_plan(catalog, LaneTopology(worker_nodes=2), tmp_path).values()
    )


def test_secret_source_uses_real_guard(tmp_path):
    source = tmp_path / "sregym/conductor/problems"
    source.mkdir(parents=True)
    (source / "synthetic.py").write_text('class SyntheticFault:\n    command = "kubectl get secret test -o json"\n')
    result = lane_plan(
        {"synthetic": description("target_port", "SyntheticFault")}, LaneTopology(worker_nodes=2), tmp_path
    )
    assert result["synthetic"].status == "not_runnable_on_lane"
    assert "Secret" in " ".join(result["synthetic"].reasons)


def test_rejected_row_never_wipes_or_starts_setup(tmp_path, monkeypatch):
    from benchmarks.sregym.fastloop import cli, fault_tracker
    from benchmarks.sregym.fastloop.environment import FastloopEnvironment
    from benchmarks.sregym.fastloop.lane_guard import FaultNotInstallableError

    env = FastloopEnvironment(
        run_dir=tmp_path,
        cluster="synthetic-1",
        kubeconfig=tmp_path / "config",
        namespace="test",
        application="test",
        workspace=tmp_path / "source",
        sregym_dir=tmp_path / "harness",
        private_tmp=None,
        worker_env={"SREGYM_KIND_WORKER_NODES": "1"},
    )
    env.save()
    monkeypatch.setattr(
        fault_tracker,
        "describe_sregym_problems",
        lambda *a, **kw: {"synthetic": description("internal_traffic_policy_local_astronomy_shop")},
    )
    monkeypatch.setattr(cli, "_wipe_sdo_and_harness", lambda *a: pytest.fail("wipe before plan admission"))
    monkeypatch.setattr(cli, "kubectl", lambda *a, **kw: pytest.fail("cluster access before plan admission"))
    monkeypatch.setattr(cli, "_worker", lambda *a: pytest.fail("worker before plan admission"))
    args = cli.build_parser().parse_args(
        ["run", "--run-dir", str(tmp_path), "--agent", "sdo", "--problem", "synthetic", "--run-id", "rejected"]
    )
    with pytest.raises(FaultNotInstallableError, match="not_runnable_on_lane"):
        cli._run(args)
    import json

    assert (
        json.loads((tmp_path / "results/rejected/lane-plan.json").read_text())["synthetic"]["status"]
        == "not_runnable_on_lane"
    )


def test_all_source_declared_two_worker_prerequisites_have_admission():
    import ast
    from pathlib import Path

    harness = Path(__file__).resolve().parents[5] / "third_party/sregym"
    found = []
    for source in (harness / "sregym/conductor/problems").glob("*.py"):
        tree = ast.parse(source.read_text())
        for node in tree.body:
            if isinstance(node, ast.ClassDef) and any(
                isinstance(value, ast.Constant)
                and isinstance(value.value, str)
                and "requires at least two worker nodes" in value.value
                for value in ast.walk(node)
            ):
                found.append(node.name)
                result = lane_plan(
                    {"synthetic": description("target_port", node.name)},
                    LaneTopology(worker_nodes=1, features=frozenset({"calico-bgp"})),
                    harness,
                )
                assert result["synthetic"].status == "not_runnable_on_lane", node.name
    assert len(found) >= 3  # A missing/uninitialized pin must never silently pass.


def test_plan_filter_uses_lane_admission(tmp_path, monkeypatch):
    import json

    from benchmarks.sregym.fastloop import probe_coverage
    from benchmarks.sregym.fastloop.environment import FastloopEnvironment

    plan = tmp_path / "plan.json"
    plan.write_text(json.dumps([{"problem_id": "blocked"}, {"problem_id": "allowed"}]))
    monkeypatch.setattr(
        probe_coverage,
        "describe_sregym_problems",
        lambda *_a, **_kw: {
            "blocked": description("internal_traffic_policy_local_astronomy_shop"),
            "allowed": description("target_port"),
        },
    )
    env = FastloopEnvironment(
        run_dir=tmp_path,
        cluster="synthetic-1",
        kubeconfig=tmp_path / "config",
        namespace="test",
        application="test",
        workspace=tmp_path / "source",
        sregym_dir=tmp_path / "harness",
        private_tmp=None,
        worker_env={"SREGYM_KIND_WORKER_NODES": "1"},
    )
    report = probe_coverage.check_plan(plan, sregym_dir=env.sregym_dir, environment=env)
    assert report["runnable_problem_ids"] == ["allowed"]
    assert report["lane_plan"]["blocked"]["status"] == "not_runnable_on_lane"


def test_topology_capture_records_actual_nodes_and_available_features(tmp_path):
    import json
    import subprocess

    from benchmarks.sregym.fastloop.lane_installability import capture_topology

    responses = iter(
        [
            {
                "items": [
                    {"metadata": {"labels": {"node-role.kubernetes.io/control-plane": ""}}},
                    {"metadata": {}},
                    {"metadata": {}},
                ]
            },
            {"status": {"conditions": [{"type": "Available", "status": "True"}]}},
            {"data": {"calico_backend": "bird"}},
            {
                "items": [
                    {"metadata": {"name": "bgpconfigurations.crd.projectcalico.org"}},
                    {"metadata": {"name": "bgppeers.crd.projectcalico.org"}},
                ]
            },
        ]
    )

    def run(command, **kwargs):
        assert "secret" not in " ".join(command).lower()
        if "exec" in command:
            return subprocess.CompletedProcess(command, 0, "BIRD ready", "")
        return subprocess.CompletedProcess(command, 0, json.dumps(next(responses)), "")

    topology = capture_topology(tmp_path / "config", network_policy=True, run=run)
    assert topology.worker_nodes == 2
    assert topology.control_plane_nodes == 1
    assert topology.features == frozenset({"network-policy", "metrics-server", "calico-bgp"})


def test_missing_topology_evidence_cannot_invent_features(tmp_path):
    import subprocess

    from benchmarks.sregym.fastloop.lane_installability import capture_topology

    topology = capture_topology(
        tmp_path / "config",
        network_policy=False,
        run=lambda command, **kwargs: subprocess.CompletedProcess(command, 1, "", "unavailable"),
    )
    assert topology.worker_nodes is None
    assert not topology.features


def test_calico_configuration_without_bgp_crds_cannot_admit_bgp_fault(tmp_path):
    import json
    import subprocess

    from benchmarks.sregym.fastloop.lane_installability import capture_topology

    def run(command, **kwargs):
        return subprocess.CompletedProcess(command, 0, json.dumps({"data": {"calico_backend": "bird"}}), "")

    assert "calico-bgp" not in capture_topology(tmp_path / "config", network_policy=True, run=run).features


def test_secret_rotation_admission_uses_guard_policy_owner(tmp_path):
    from pathlib import Path

    from benchmarks.sregym.fastloop.lane_guard import LaneGuard

    class InjectorReadAllowed(LaneGuard):
        def command(self, args):
            return args

    result = lane_plan(
        {"synthetic": description("secret_rotation_stale_env_credentials_astronomy_shop")},
        LaneTopology(worker_nodes=1),
        tmp_path,
        guard=InjectorReadAllowed("synthetic", Path("/config"), "plan"),
    )
    assert result["synthetic"].status == "runnable"
