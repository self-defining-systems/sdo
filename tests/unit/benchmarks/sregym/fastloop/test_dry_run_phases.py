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
