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
