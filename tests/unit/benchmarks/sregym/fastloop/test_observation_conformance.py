import json
from types import SimpleNamespace

import pytest

from benchmarks.sregym.fastloop.observation_conformance import check_observations


def test_setup_probe_uses_installed_controller_and_preserves_named_failure(monkeypatch, tmp_path):
    from benchmarks.sregym import adapter

    calls = []
    report = {
        "passed": False,
        "observations": [
            {
                "primitive": "DatabaseUsers",
                "target": {"pod": {"name": "mongo-geo"}, "container": "mongo"},
                "passed": False,
                "failure_stage": "authentication",
                "error": "database authentication: rejected",
            }
        ],
    }

    def api(args, **kwargs):
        calls.append((args, kwargs))
        return SimpleNamespace(returncode=1, stdout=json.dumps(report), stderr="excluded upstream error")

    monkeypatch.setattr(adapter, "kubectl", api)
    environment = SimpleNamespace(namespace="hotel", kubeconfig=tmp_path / "kubeconfig")
    with pytest.raises(ValueError, match="DatabaseUsers mongo-geo/mongo: authentication"):
        check_observations(environment, tmp_path)
    assert calls[0][0][-3:] == ["/usr/local/bin/sdo-observation-check", "--namespace", "hotel"]
    assert calls[0][1]["namespace"] == "hotel-sdo"
    assert json.loads((tmp_path / "observation-conformance.json").read_text()) == report


def test_no_matching_workloads_is_an_explicit_empty_probe_inventory(monkeypatch, tmp_path):
    from benchmarks.sregym import adapter

    monkeypatch.setattr(
        adapter,
        "kubectl",
        lambda *args, **kwargs: SimpleNamespace(returncode=0, stdout='{"passed":true,"observations":[]}', stderr=""),
    )
    report = check_observations(SimpleNamespace(namespace="toy", kubeconfig=tmp_path / "k"), tmp_path)
    assert report["observations"] == 0
