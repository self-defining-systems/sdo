"""Permissive suites retain capability gaps without weakening the default gate."""

import json

import pytest

from benchmarks.sregym.faults.description import FaultSpec, ProblemDescription, validate_suite
from tests.unit.benchmarks.sregym.fastloop.test_fastloop_loop import FakeAgent, FakeClock, FakeDriver


def catalog():
    return {
        "synthetic": ProblemDescription(
            problem_id="synthetic",
            composite=True,
            faults=(
                FaultSpec(
                    "readiness", "frontend", class_name="Reviewed", required_capabilities=("application-workloads",)
                ),
                FaultSpec(
                    "readiness",
                    "backend",
                    declared_index=1,
                    class_name="NewFault",
                    required_capabilities=("unreviewed:NewFault",),
                ),
                FaultSpec(
                    "readiness",
                    "checkout",
                    declared_index=2,
                    class_name="WebhookFault",
                    required_capabilities=("Webhook-configuration", "foreign-namespace"),
                ),
            ),
            injection_order=(2, 0, 1),
        )
    }


def test_default_still_fails_closed():
    with pytest.raises(ValueError, match="unreviewed:NewFault"):
        validate_suite(catalog())


def test_permissive_evidence_keeps_every_fault_and_its_missing_scope():
    evidence = validate_suite(catalog(), capability_mode="permissive")
    assert evidence.mode == "permissive"
    assert [(f.declared_index, f.status) for f in evidence.faults] == [
        (2, "unsupported"),
        (0, "supported"),
        (1, "unreviewed"),
    ]
    assert evidence.faults[0].missing_capabilities == ("Webhook-configuration", "foreign-namespace")
    assert evidence.faults[2].missing_capabilities == ("unreviewed:NewFault",)


def test_permissive_does_not_waive_requested_tracking():
    from benchmarks.sregym.faults.worker import describe_problem
    from tests.unit.benchmarks.sregym.fastloop.test_sdo3_descriptions import part

    with pytest.raises(ValueError, match="tracking unsupported"):
        validate_suite(
            {"new": describe_problem("new", part("NewFault"))}, capability_mode="permissive", require_tracking=True
        )


@pytest.mark.parametrize("fail", [False, True])
def test_manifest_and_each_incident_row_record_capability_gaps(tmp_path, fail):
    from benchmarks.sregym.fastloop.loop import LoopConfig, run_incidents
    from benchmarks.sregym.fastloop.records import load_records

    clock = FakeClock()
    evidence = validate_suite(catalog(), capability_mode="permissive")
    config = LoopConfig("test", ("synthetic",), 1, tmp_path / "incidents.jsonl", capability_evidence=evidence)
    records = run_incidents(FakeAgent(clock, fail_on={0} if fail else set()), FakeDriver(clock), config)
    manifest = json.loads((tmp_path / "run_manifest.json").read_text())
    assert manifest["fault_capabilities"] == evidence.model_dump(mode="json")
    assert records[0].capability_mode == "permissive"
    assert records[0].fault_capabilities == list(evidence.faults)
    assert load_records(config.results_path)[0].fault_capabilities == list(evidence.faults)
    assert "unreviewed:NewFault" in config.results_path.with_suffix(".csv").read_text()


def test_sdo3_stream_explicitly_opts_in():
    from pathlib import Path

    from benchmarks.sregym.fastloop.stream import load_stream_config

    path = Path(__file__).resolve().parents[5] / "tests/fixtures/sregym/fastloop/sdo3_permissive.json"
    stream = load_stream_config(path)
    assert stream.capability_mode == "permissive"
    assert stream.permissive_responder is True
    assert stream.responder_namespaces == ("policy-system",)
    assert len(stream.incidents) == 10


def test_legacy_stream_defaults_to_strict(tmp_path):
    from benchmarks.sregym.fastloop.stream import load_stream_config

    path = tmp_path / "stream.json"
    path.write_text('["synthetic"]')
    stream = load_stream_config(path)
    assert stream.capability_mode == "strict"
    assert stream.permissive_responder is False


@pytest.mark.parametrize("mode", ["strict", "permissive"])
def test_cli_records_permissive_gaps_before_worker_launch(tmp_path, monkeypatch, mode):
    from types import SimpleNamespace

    from benchmarks.sregym.fastloop import cli
    from benchmarks.sregym.fastloop.environment import FastloopEnvironment

    environment = FastloopEnvironment(
        run_dir=tmp_path,
        cluster="test",
        kubeconfig=tmp_path / "kubeconfig",
        namespace="test-app",
        application="test-app",
        workspace=tmp_path,
        sregym_dir=tmp_path,
        private_tmp=None,
    )
    monkeypatch.setattr(FastloopEnvironment, "load", lambda _: environment)
    monkeypatch.setattr(cli, "kubectl", lambda *args, **kwargs: SimpleNamespace(returncode=0))
    monkeypatch.setattr("benchmarks.sregym.fastloop.fault_tracker.describe_sregym_problems", lambda *a, **kw: catalog())
    stream = tmp_path / "stream.json"
    stream.write_text(
        json.dumps(
            {
                "incidents": ["synthetic"],
                "capability_mode": mode,
                "permissive_responder": mode == "permissive",
                "responder_namespaces": ["policy-system"] if mode == "permissive" else [],
            }
        )
    )

    class WorkerReached(RuntimeError):
        pass

    def worker(_):
        assert mode == "permissive"
        manifest = json.loads((tmp_path / "results/test/run_manifest.json").read_text())
        assert manifest["fault_capabilities"]["mode"] == mode
        assert len(manifest["fault_capabilities"]["faults"]) == 3
        assert manifest["permissive_responder"] is True
        assert manifest["responder_namespaces"] == ["policy-system"]
        raise WorkerReached

    monkeypatch.setattr(cli, "_worker", worker)
    expected = WorkerReached if mode == "permissive" else ValueError
    with pytest.raises(expected):
        cli.main(["run", "--run-dir", str(tmp_path), "--stream", str(stream), "--agent", "sdo", "--run-id", "test"])
