from __future__ import annotations

from pathlib import Path

import pytest

from sdo.agents.responder.job import run_job
from sdo.agents.views import (
    IncidentRequest,
    IncidentResult,
)

FIXTURE_DIR = Path(__file__).resolve().parents[4] / "fixtures" / "sdo" / "contracts"


class RecordingPublisher:
    def __init__(self) -> None:
        self.published: list[tuple[str, IncidentResult]] = []

    def publish(self, name: str, result: IncidentResult) -> None:
        self.published.append((name, result))


def test_job_responder_executes_mounted_request_and_publishes_validated_result() -> None:
    request = IncidentRequest.model_validate_json((FIXTURE_DIR / "incident_request.json").read_text(encoding="utf-8"))
    result = IncidentResult.model_validate_json((FIXTURE_DIR / "incident_result.json").read_text(encoding="utf-8"))
    publisher = RecordingPublisher()

    returned = run_job(
        request_path=FIXTURE_DIR / "incident_request.json",
        result_name="incident-result",
        publisher=publisher,
        executor=lambda received: result if received == request else pytest.fail("request changed"),
    )

    assert returned == result
    assert publisher.published == [("incident-result", result)]


def test_job_responder_rejects_mismatched_result_before_publish() -> None:
    result = IncidentResult.model_validate_json((FIXTURE_DIR / "incident_result.json").read_text(encoding="utf-8"))
    bad_result = result.model_copy(update={"incident_id": "other"})
    publisher = RecordingPublisher()

    with pytest.raises(RuntimeError, match="incident_id"):
        run_job(
            request_path=FIXTURE_DIR / "incident_request.json",
            result_name="incident-result",
            publisher=publisher,
            executor=lambda _request: bad_result,
        )

    assert publisher.published == []


def test_responder_kubectl_defaults_to_the_incident_application_namespace(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import json

    from sdo.agents.responder import job

    service_account = tmp_path / "serviceaccount"
    service_account.mkdir()
    (service_account / "token").write_text("token\n", encoding="utf-8")
    (service_account / "ca.crt").write_text("ca\n", encoding="utf-8")
    monkeypatch.setattr(job, "SERVICE_ACCOUNT_ROOT", service_account)
    monkeypatch.setenv("KUBERNETES_SERVICE_HOST", "10.0.0.1")
    monkeypatch.setenv("TMPDIR", str(tmp_path))
    monkeypatch.delenv("KUBECONFIG", raising=False)
    request = IncidentRequest.model_validate_json((FIXTURE_DIR / "incident_request.json").read_text(encoding="utf-8"))
    result = IncidentResult.model_validate_json((FIXTURE_DIR / "incident_result.json").read_text(encoding="utf-8"))
    observed: dict[str, str] = {}

    def executor(received: IncidentRequest) -> IncidentResult:
        observed["kubeconfig"] = job.os.environ["KUBECONFIG"]
        return result

    run_job(
        request_path=FIXTURE_DIR / "incident_request.json",
        result_name="incident-result",
        publisher=RecordingPublisher(),
        executor=executor,
    )

    config = json.loads(Path(observed["kubeconfig"]).read_text(encoding="utf-8"))
    (context,) = config["contexts"]
    assert context["context"]["namespace"] == request.namespace
    assert config["clusters"][0]["cluster"]["server"] == "https://10.0.0.1:443"
    assert config["users"][0]["user"] == {"tokenFile": str(service_account / "token")}


def test_responder_outside_a_pod_keeps_the_ambient_kubeconfig(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    from sdo.agents.responder import job

    monkeypatch.setattr(job, "SERVICE_ACCOUNT_ROOT", tmp_path / "missing")
    monkeypatch.setenv("KUBECONFIG", "/home/user/.kube/config")
    result = IncidentResult.model_validate_json((FIXTURE_DIR / "incident_result.json").read_text(encoding="utf-8"))

    run_job(
        request_path=FIXTURE_DIR / "incident_request.json",
        result_name="incident-result",
        publisher=RecordingPublisher(),
        executor=lambda _request: result,
    )

    assert job.os.environ["KUBECONFIG"] == "/home/user/.kube/config"
