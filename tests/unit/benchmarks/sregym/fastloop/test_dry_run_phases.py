"""Cold setup has its own bound; every steady health check retains five minutes."""

import pytest

from benchmarks.sregym.fastloop import dry_run
from benchmarks.sregym.fastloop.cli import build_parser


def test_cold_setup_cannot_consume_steady_health_budget():
    assert dry_run.phase_deadline({}, 10, 900, 300) == 910
    assert dry_run.phase_deadline({"steady_started": 800}, 10, 900, 300) == 1100


def test_setup_budget_is_independently_bounded(tmp_path):
    args = build_parser().parse_args(["eval-dry-run", "--run-dir", str(tmp_path), "--setup-timeout", "900"])
    assert args.setup_timeout == 900
    assert args.timeout == 300
    with pytest.raises(SystemExit):
        build_parser().parse_args(["eval-dry-run", "--run-dir", str(tmp_path), "--setup-timeout", "901"])


def test_scripted_setup_creates_dedicated_empty_credentials_without_reading_secrets(monkeypatch, tmp_path):
    from types import SimpleNamespace

    from benchmarks.sregym import adapter

    calls = []

    def api(args, **kwargs):
        calls.append((args, kwargs))
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(adapter, "kubectl", api)
    name = dry_run.prepare_scripted_credentials(tmp_path / "kubeconfig", "application-sdo")
    assert name == "sdo-scripted-credentials"
    assert any(args[:3] == ["create", "secret", "generic"] for args, _ in calls)
    assert all(not (args[0] == "get" and "secret" in " ".join(args)) for args, _ in calls)
    assert all("--from-literal" not in " ".join(args) for args, _ in calls)


@pytest.mark.parametrize(
    "message",
    [
        'Error from server (AlreadyExists): secrets "sdo-scripted-credentials" already exists',
        'error: failed to create secret secrets "sdo-scripted-credentials" already exists',
    ],
)
def test_scripted_credentials_reuse_is_independent_of_cli_error_wording(monkeypatch, tmp_path, message):
    from types import SimpleNamespace

    from benchmarks.sregym import adapter

    calls = []

    def api(args, **kwargs):
        calls.append(args)
        if args[0] == "create":
            return SimpleNamespace(returncode=1, stdout="", stderr=message)
        if args[0] == "get":
            return SimpleNamespace(returncode=0, stdout="secret/sdo-scripted-credentials\n", stderr="")
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(adapter, "kubectl", api)
    assert (
        dry_run.prepare_scripted_credentials(tmp_path / "kubeconfig", "application-sdo") == "sdo-scripted-credentials"
    )
    assert calls[-1] == ["get", "secret", "sdo-scripted-credentials", "-o", "name", "--ignore-not-found=true"]


@pytest.mark.parametrize(
    ("observed", "returncode"),
    [
        ("", 0),
        ("secret/unrelated", 0),
        ("secret/sdo-scripted-credentials\nsecret/unrelated", 0),
        ("secret/sdo-scripted-credentials", 1),
    ],
)
def test_scripted_credentials_creation_failure_requires_exact_existence(monkeypatch, tmp_path, observed, returncode):
    from types import SimpleNamespace

    from benchmarks.sregym import adapter

    def api(args, **kwargs):
        if args[0] == "create":
            return SimpleNamespace(returncode=1, stdout="", stderr="create denied")
        return SimpleNamespace(returncode=returncode, stdout=observed, stderr="")

    monkeypatch.setattr(adapter, "kubectl", api)
    with pytest.raises(RuntimeError, match="create denied"):
        dry_run.prepare_scripted_credentials(tmp_path / "kubeconfig", "application-sdo")


def test_reused_admission_receives_full_steady_budget(monkeypatch, tmp_path):
    import json
    from contextlib import nullcontext
    from types import SimpleNamespace

    from benchmarks.sregym import adapter
    from benchmarks.sregym.fastloop import admission, cli

    receipt = tmp_path / "receipt.json"
    receipt.write_text(json.dumps({"report": {"invariants": {}}}))
    report = dry_run.DryRunReport("synthetic")
    report.setup_proof = {"receipt": str(receipt)}
    monkeypatch.setattr(dry_run, "assert_active_intake", lambda env: None)
    monkeypatch.setattr(cli, "_worker", lambda env: nullcontext(object()))

    def prove(gate, control):
        assert gate.timeout_seconds == 300

    monkeypatch.setattr(admission.LaneAdmission, "prove", prove)
    probes = []

    def probe(args, **kwargs):
        probes.append((args, kwargs))
        return SimpleNamespace(returncode=0, stdout='{"passed":true,"observations":[]}', stderr="")

    monkeypatch.setattr(adapter, "kubectl", probe)
    environment = SimpleNamespace(namespace="app", kubeconfig=tmp_path / "kubeconfig")
    dry_run.refresh_reused_proof(environment, tmp_path, report, lambda *a, **kw: None, 300)
    assert len(probes) == 1
    assert probes[0][0][-3:] == ["/usr/local/bin/sdo-observation-check", "--namespace", "app"]
    assert probes[0][1]["namespace"] == "app-sdo"
    assert probes[0][1]["kubeconfig"] == environment.kubeconfig


def _probe_without(missing):
    import json
    from types import SimpleNamespace

    calls = []

    def run(command, **kwargs):
        calls.append(command)
        script = command[-1]
        error = f"{missing}: ModuleNotFoundError: No module named '{missing}'" if missing in script else None
        return SimpleNamespace(returncode=0, stdout=json.dumps({"agentshim": None, "import_error": error}), stderr="")

    return run, calls


def test_scripted_preflight_rejects_production_responder_image():
    run, calls = _probe_without("benchmarks.sregym.adapter.scripted_worker")
    with pytest.raises(ValueError, match="cannot run the scripted registrations") as error:
        dry_run.assert_responder_runs_registrations(
            "sdo-sregym-responder:le-p4-h7", dry_run.SCRIPTED_REGISTRATIONS, run=run
        )
    message = str(error.value)
    assert "sdo-sregym-responder:le-p4-h7" in message
    assert "benchmarks.sregym.adapter.scripted_worker" in message
    assert "SDO_SCRIPTED_WORKER=1" in message
    assert "SDO_IMAGE_TAG=le-p4-h7-scripted" in message
    assert len(calls) == 1
    network = calls[0].index("--network")
    assert calls[0][network + 1] == "none"


def test_scripted_preflight_covers_every_registered_worker_module():
    run, calls = _probe_without("absent.module")
    evidence = dry_run.assert_responder_runs_registrations(
        "sdo-sregym-responder:scripted", dry_run.SCRIPTED_REGISTRATIONS, run=run
    )
    assert evidence["image"] == "sdo-sregym-responder:scripted"
    assert evidence["modules"] == ["benchmarks.sregym.adapter.scripted_worker"]
    assert "benchmarks.sregym.adapter.scripted_worker" in calls[0][-1]


def test_dry_run_fails_on_responder_image_before_any_lane_mutation(monkeypatch, tmp_path):
    import json
    from types import SimpleNamespace

    from benchmarks.sregym import adapter
    from benchmarks.sregym.fastloop import environment, setup_proof

    lane = SimpleNamespace(
        run_dir=tmp_path,
        cluster="lane",
        kubeconfig=tmp_path / "kubeconfig",
        dev_mount=None,
        images=SimpleNamespace(responder="sdo-sregym-responder:prod"),
    )
    monkeypatch.setattr(environment.FastloopEnvironment, "load", classmethod(lambda cls, path: lane))
    monkeypatch.setattr(adapter, "assert_image_entrypoints", _missing_scripted_worker)

    def reached(*args, **kwargs):
        raise AssertionError("dry run touched the lane before checking its responder image")

    monkeypatch.setattr(setup_proof, "check_reuse", lambda env: (None, {"reused": False}))
    monkeypatch.setattr(adapter, "assert_lane_resources", reached)
    assert dry_run.run_lane(tmp_path, 300, 900) == 1
    report = json.loads((tmp_path / "eval-dry-run" / "report.json").read_text())
    assert report["passed"] is False
    assert "SDO_SCRIPTED_WORKER=1" in report["invariants"]["failure"]["reason"]
    assert report["seconds"] < 60


def _missing_scripted_worker(ref, modules, **kwargs):
    raise ValueError(f"eval: image {ref} entrypoint smoke failed: {modules[0]}: ModuleNotFoundError")
