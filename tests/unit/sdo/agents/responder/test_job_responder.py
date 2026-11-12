from __future__ import annotations

import json
import os
from pathlib import Path
from typing import TYPE_CHECKING

from sdo.agents.responder import job

if TYPE_CHECKING:
    import pytest


def test_application_kubeconfig_defaults_to_the_incident_application_namespace(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service_account = tmp_path / "serviceaccount"
    service_account.mkdir()
    (service_account / "token").write_text("token\n", encoding="utf-8")
    (service_account / "ca.crt").write_text("ca\n", encoding="utf-8")
    monkeypatch.setattr(job, "SERVICE_ACCOUNT_ROOT", service_account)
    monkeypatch.setenv("KUBERNETES_SERVICE_HOST", "10.0.0.1")
    monkeypatch.setenv("TMPDIR", str(tmp_path))

    kubeconfig = job.write_application_kubeconfig("app-ns")

    assert kubeconfig is not None
    config = json.loads(kubeconfig.read_text(encoding="utf-8"))
    (context,) = config["contexts"]
    assert context["context"]["namespace"] == "app-ns"
    assert config["clusters"][0]["cluster"]["server"] == "https://10.0.0.1:443"
    assert config["users"][0]["user"] == {"tokenFile": str(service_account / "token")}


def test_application_kubeconfig_outside_a_pod_is_left_to_the_ambient_kubeconfig(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(job, "SERVICE_ACCOUNT_ROOT", tmp_path / "missing")

    assert job.write_application_kubeconfig("app-ns") is None


def test_proxy_kubeconfig_is_loopback_without_client_credentials(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    service_account = tmp_path / "serviceaccount"
    service_account.mkdir()
    (service_account / "token").write_text("synthetic-token", encoding="utf-8")
    monkeypatch.setattr(job, "SERVICE_ACCOUNT_ROOT", service_account)
    monkeypatch.setenv("KUBERNETES_SERVICE_HOST", "10.0.0.1")
    monkeypatch.setenv("SDO_API_PROXY_URL", "http://127.0.0.1:18080")
    kubeconfig = job.write_application_kubeconfig("app-ns")
    assert kubeconfig is not None
    config = json.loads(kubeconfig.read_text())
    assert config["clusters"][0]["cluster"] == {"server": "http://127.0.0.1:18080"}
    assert config["users"][0]["user"] == {}
    assert config["contexts"][0]["context"]["namespace"] == "app-ns"


def test_proxy_kubeconfig_does_not_depend_on_a_token_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(job, "SERVICE_ACCOUNT_ROOT", tmp_path / "missing")
    monkeypatch.delenv("KUBERNETES_SERVICE_HOST", raising=False)
    monkeypatch.setenv("TMPDIR", str(tmp_path))
    monkeypatch.setenv("SDO_API_PROXY_URL", "http://127.0.0.1:18080")

    kubeconfig = job.write_application_kubeconfig("app-ns")

    assert kubeconfig is not None
    config = json.loads(kubeconfig.read_text(encoding="utf-8"))
    assert config["clusters"][0]["cluster"] == {"server": "http://127.0.0.1:18080"}
    assert config["users"][0]["user"] == {}


def test_every_agent_worker_points_kubectl_at_the_recording_proxy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("TMPDIR", str(tmp_path))
    monkeypatch.setenv("SDO_API_PROXY_URL", "http://127.0.0.1:18080")
    monkeypatch.delenv("KUBECONFIG", raising=False)

    job.export_proxy_kubeconfig("sdo")

    config = json.loads(Path(os.environ["KUBECONFIG"]).read_text(encoding="utf-8"))
    assert config["clusters"][0]["cluster"] == {"server": "http://127.0.0.1:18080"}
    monkeypatch.delenv("KUBECONFIG", raising=False)


def test_without_a_proxy_the_ambient_kubeconfig_is_untouched(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SDO_API_PROXY_URL", raising=False)
    monkeypatch.setenv("KUBECONFIG", "/ambient")

    job.export_proxy_kubeconfig("sdo")

    assert os.environ["KUBECONFIG"] == "/ambient"
