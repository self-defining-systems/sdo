"""A reset reuses only the production-installed controller template."""

from copy import deepcopy

from benchmarks.sregym.fastloop.toy_reset import restart_job


def test_restart_uses_existing_spec_without_server_identity():
    original = {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": "sdo-controller-run",
            "namespace": "toy-sdo",
            "uid": "old",
            "resourceVersion": "7",
            "annotations": {"sdo.dev/fingerprint": "accepted"},
        },
        "spec": {
            "selector": {"matchLabels": {"controller-uid": "old"}},
            "template": {
                "metadata": {
                    "labels": {"controller-uid": "old", "batch.kubernetes.io/controller-uid": "old", "app": "sdo"}
                },
                "spec": {
                    "containers": [
                        {
                            "name": "controller",
                            "command": ["sdo-operator"],
                            "args": ["controller", "--broker-mode=native"],
                        }
                    ]
                },
            },
        },
        "status": {"active": 1},
    }
    before = deepcopy(original)
    restored = restart_job(original)
    assert original == before
    assert restored["metadata"] == {
        "name": "sdo-controller-run",
        "namespace": "toy-sdo",
        "annotations": {"sdo.dev/fingerprint": "accepted"},
    }
    assert "selector" not in restored["spec"]
    assert restored["spec"]["template"]["metadata"]["labels"] == {"app": "sdo"}
    assert restored["spec"]["template"]["spec"] == original["spec"]["template"]["spec"]


def test_reset_without_registry_discards_prior_runtime_state_before_install(tmp_path, monkeypatch):
    from contextlib import nullcontext
    from types import SimpleNamespace

    from benchmarks.sregym.fastloop import cli, reset, toy, toy_reset
    from benchmarks.sregym.fastloop.environment import FastloopEnvironment

    environment = FastloopEnvironment(
        app="toy",
        run_dir=tmp_path,
        cluster="sdo-toy",
        kubeconfig=tmp_path / "kubeconfig",
        namespace="sdo-toy",
        application="toy",
        workspace=tmp_path / "workspace",
        sregym_dir=tmp_path / "unused",
        private_tmp=None,
        seed_commit="a" * 40,
    )
    environment.save()
    retained = {"sdo-controller-state", "sdo-controller-state-drift-verdicts", "sdo-controller-maintenance"}
    lease_exists = True
    workers_stopped = False

    def reset_environment(*_args):
        nonlocal workers_stopped
        workers_stopped = True

    def command(config, namespace, *args):
        nonlocal lease_exists
        assert workers_stopped
        assert config == str(environment.kubeconfig)
        assert namespace == "sdo-toy-sdo"
        if args[:2] == ("delete", "configmap"):
            retained.difference_update(args[2:])
        elif args[:2] == ("delete", "lease"):
            lease_exists = False
        else:
            raise AssertionError(args)
        return ""

    def install(_environment):
        assert not retained, "reset must discard old state even without a saved controller registry"
        assert not lease_exists
        return "new-controller"

    monkeypatch.setattr(cli, "_worker", lambda _env: nullcontext(object()))
    monkeypatch.setattr(cli, "_restore_seed", lambda _env: None)
    monkeypatch.setattr(reset, "reset_environment", reset_environment)
    monkeypatch.setattr(toy_reset, "command", command)
    monkeypatch.setattr(toy, "install_controller", install)
    monkeypatch.setattr(toy, "record_controller", lambda _env, config: config == "new-controller")

    assert cli._reset(SimpleNamespace(run_dir=tmp_path, dev_mount=None)) == 0
