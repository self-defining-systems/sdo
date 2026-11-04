"""A row-start recreation leaves the application with the fixture `reset` restores."""

from pathlib import Path
from types import SimpleNamespace

import pytest

from benchmarks.sregym.fastloop import cli, social_fixture


class Worker:
    def request(self, op, **_kwargs):
        assert op == "deploy"
        return {"deployed": True, "health": {"healthy": True}}


@pytest.mark.parametrize(("namespace", "seeded"), [("social-network", True), ("hotel-reservation", False)])
def test_recreated_application_restores_the_social_fixture(monkeypatch, tmp_path, namespace, seeded):
    calls = []
    monkeypatch.setattr(social_fixture, "restore_social_fixture", lambda config: calls.append(("fixture", config)))
    monkeypatch.setattr(cli, "_recapture_application_baseline", lambda _env: calls.append(("baseline", None)))

    def run_tier2(_reason, _tier1, *, deploy, **_kwargs):
        deploy()
        return SimpleNamespace(write=lambda _directory: None)

    monkeypatch.setattr(cli, "run_tier2", run_tier2)
    monkeypatch.setattr(cli, "application_namespaces", lambda *_args: (namespace,))
    environment = SimpleNamespace(
        namespace=namespace,
        kubeconfig=Path("/lane/kubeconfig"),
        deploy_problem="problem",
        baseline_path=tmp_path / "baseline.json",
        worker_env={},
    )
    monkeypatch.setattr(cli, "HostLimits", SimpleNamespace(from_env=lambda _env: None))
    monkeypatch.setattr(cli, "LaneInjectionState", lambda *_args: None)
    monkeypatch.setattr(cli, "KubectlLaneCluster", lambda *_args: None)
    cli.recreate_application(environment, Worker(), object(), object(), tmp_path)
    # The fixture is seeded before the new baseline is captured, so the baseline includes it.
    expected = [("fixture", Path("/lane/kubeconfig")), ("baseline", None)] if seeded else [("baseline", None)]
    assert calls == expected


def test_goal_addendum_extends_only_a_given_requirement():
    assert cli.lifecycle_objective("Deployments remain available.", None) == "Deployments remain available."
    assert cli.lifecycle_objective("Deployments remain available.", "  ") == "Deployments remain available."
    assert (
        cli.lifecycle_objective("Deployments remain available.", "Users can post and read back posts.")
        == "Deployments remain available. Users can post and read back posts."
    )
