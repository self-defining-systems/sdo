"""Contract tests for ``sdo incident status``, the responder's verify-before-submit check."""

from __future__ import annotations

import importlib
import json
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest
from google.protobuf import json_format
from google.protobuf.any_pb2 import Any as ProtoAny

from sdo.agents.responder.incident_status import (
    EXIT_HEALTHY,
    EXIT_UNAVAILABLE,
    EXIT_UNHEALTHY,
    ControllerViewError,
    IncidentStatusReport,
    IncidentStatusState,
    incident_status,
    run_incident_status_cli,
)
from sdo.contracts import AgentTask, DriftEvent, IncidentRequest, IncidentView, parse_json, to_canonical_json

if TYPE_CHECKING:
    from collections.abc import Iterator

# The package re-exports the function under the module's name.
incident_status_module = importlib.import_module("sdo.agents.responder.incident_status")

FIXTURE_DIR = Path(__file__).resolve().parents[4] / "fixtures" / "sdo" / "contracts"


def test_controller_status_serializes_drift_events() -> None:
    event = DriftEvent(operation="Update", manager="foreign-controller", classification="unexplained")
    report = IncidentStatusReport(
        state=IncidentStatusState.UNAVAILABLE,
        burst=None,
        detail="traffic prober unavailable",
        view=IncidentView(drift_events=[event]),
    )

    assert report.to_json()["controller"]["drift_events"] == [
        {"operation": "Update", "manager": "foreign-controller", "classification": "unexplained"}
    ]


def _verdict(scenario: str, *, healthy: bool, qualified: bool = True, failure: str = "") -> dict[str, Any]:
    verdict: dict[str, Any] = {
        "scenario": scenario,
        "healthy": healthy,
        "qualified": qualified,
        "evaluated": True,
        "samples": 12,
        "errorRate": 0.0 if healthy else 1.0,
        "latencyMs": 4 if healthy else 0,
    }
    if not healthy:
        verdict["violations"] = ["error rate 1.00 > 0.50"]
        verdict["statusCounts"] = {"connection refused": 12}
        verdict["recentFailures"] = [failure or "iteration 7 step search GET /hotels: connection refused"]
    return verdict


def _burst(*verdicts: dict[str, Any]) -> dict[str, Any]:
    healthy = all(verdict["healthy"] or not verdict["qualified"] for verdict in verdicts)
    return {
        "workload": "verify",
        "healthy": healthy,
        "startedAt": "2026-09-27T10:00:00Z",
        "durationNs": 3_000_000_000,
        "verdicts": list(verdicts),
        "window": {"ignored": True},
    }


class _FakeProber:
    def __init__(self, response: dict[str, Any], status: int = 200) -> None:
        self.response = response
        self.status = status
        self.requests: list[dict[str, Any]] = []


@pytest.fixture
def prober() -> Iterator[tuple[_FakeProber, str]]:
    fake = _FakeProber(_burst(_verdict("search-hotels", healthy=True)))

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            length = int(self.headers.get("Content-Length", "0"))
            fake.requests.append({"path": self.path, "body": json.loads(self.rfile.read(length) or b"{}")})
            body = json.dumps(fake.response).encode()
            self.send_response(fake.status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: object) -> None:  # noqa: A002
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield fake, f"http://127.0.0.1:{server.server_address[1]}"
    finally:
        server.shutdown()
        server.server_close()


def _write_task(path: Path, request: IncidentRequest) -> None:
    """The file the controller mounts: an AgentTask envelope around the IncidentRequest."""

    packed = ProtoAny()
    packed.Pack(request)
    task = AgentTask(
        schema_version="sdo.dev/v1alpha1",
        task_id="task-1",
        agent_id="sdo-responder",
        task_type="incident-response",
        request=packed,
    )
    path.write_text(to_canonical_json(task), encoding="utf-8")


def _read_request(path: Path) -> IncidentRequest:
    task = json_format.Parse(path.read_text(encoding="utf-8"), AgentTask())
    request = IncidentRequest()
    assert task.request.Unpack(request)
    return request


def _traffic_request(tmp_path: Path) -> Path:
    request = parse_json((FIXTURE_DIR / "incident_request.json").read_text(encoding="utf-8"), IncidentRequest())
    finding = request.findings[0]
    finding.detector_id = "traffic-health"
    finding.rule_id = "scenario-slo.search-hotels"
    del request.findings[1:]
    path = tmp_path / "agent-task.json"
    _write_task(path, request)
    return path


def test_status_is_unavailable_without_a_prober(tmp_path: Path) -> None:
    status = incident_status(prober_url=None, request_path=tmp_path / "missing.json")

    assert status.state == IncidentStatusState.UNAVAILABLE
    assert status.exit_code == EXIT_UNAVAILABLE
    assert "own verification" in status.render()


def test_healthy_burst_passes_verification(prober: tuple[_FakeProber, str], tmp_path: Path) -> None:
    fake, url = prober

    status = incident_status(prober_url=url, request_path=_traffic_request(tmp_path))

    assert status.state == IncidentStatusState.HEALTHY
    assert status.exit_code == EXIT_HEALTHY
    assert fake.requests == [{"path": "/v1/bursts", "body": {}}]
    assert "search-hotels" in status.render()


def test_unhealthy_burst_names_failing_routes_and_incident_scenarios(
    prober: tuple[_FakeProber, str], tmp_path: Path
) -> None:
    fake, url = prober
    fake.response = _burst(_verdict("search-hotels", healthy=False), _verdict("login", healthy=True))

    status = incident_status(prober_url=url, request_path=_traffic_request(tmp_path))

    assert status.state == IncidentStatusState.UNHEALTHY
    assert status.exit_code == EXIT_UNHEALTHY
    assert status.incident_scenarios == ("search-hotels",)
    text = status.render()
    assert "UNHEALTHY" in text
    assert "GET /hotels: connection refused" in text
    assert "search-hotels (triggered this incident)" in text


def test_unqualified_scenarios_are_reported_but_do_not_block(prober: tuple[_FakeProber, str], tmp_path: Path) -> None:
    fake, url = prober
    fake.response = _burst(_verdict("search-hotels", healthy=True), _verdict("login", healthy=False, qualified=False))

    status = incident_status(prober_url=url, request_path=tmp_path / "missing.json")

    assert status.state == IncidentStatusState.HEALTHY
    assert "login" in status.render()
    assert "never passed" in status.render()


def test_scenarios_and_workload_are_forwarded(prober: tuple[_FakeProber, str], tmp_path: Path) -> None:
    fake, url = prober

    incident_status(
        prober_url=url, request_path=tmp_path / "missing.json", workload="journey", scenarios=("search-hotels",)
    )

    assert fake.requests[0]["body"] == {"workload": "journey", "scenarios": ["search-hotels"]}


def test_prober_error_is_unavailable_not_healthy(prober: tuple[_FakeProber, str], tmp_path: Path) -> None:
    fake, url = prober
    fake.status = 400
    fake.response = {"error": "no verify-burst workload is defined"}

    status = incident_status(prober_url=url, request_path=tmp_path / "missing.json")

    assert status.state == IncidentStatusState.UNAVAILABLE
    assert "no verify-burst workload is defined" in status.render()


def test_cli_prints_json_and_exits_with_the_verdict(
    prober: tuple[_FakeProber, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    fake, url = prober
    fake.response = _burst(_verdict("search-hotels", healthy=False))
    monkeypatch.setenv("SDO_PROBER_URL", url)
    monkeypatch.setenv("SDO_REQUEST_PATH", str(_traffic_request(tmp_path)))

    exit_code = run_incident_status_cli(workload=None, scenarios=(), as_json=True)

    assert exit_code == EXIT_UNHEALTHY
    payload = json.loads(capsys.readouterr().out)
    assert payload["state"] == "unhealthy"
    assert payload["incident_scenarios"] == ["search-hotels"]
    assert payload["burst"]["verdicts"][0]["recent_failures"]


def test_agent_module_is_the_status_cli(tmp_path: Path) -> None:
    completed = subprocess.run(
        [sys.executable, "-m", "sdo.agents.responder.status_cli", "--json"],
        capture_output=True,
        text=True,
        check=False,
        env={"PATH": "/usr/bin:/bin", "SDO_REQUEST_PATH": str(tmp_path / "missing.json")},
    )
    assert completed.returncode == EXIT_UNAVAILABLE
    assert json.loads(completed.stdout)["state"] == "unavailable"


def test_status_model_rejects_a_healthy_state_without_a_burst() -> None:
    with pytest.raises(ValueError, match="burst"):
        IncidentStatusReport(state=IncidentStatusState.HEALTHY, burst=None, detail="")


def _view(
    request_path: Path,
    *,
    findings: list[dict[str, Any]],
    changes: list[dict[str, Any]] | None = None,
    clearing: list[dict[str, Any]] | None = None,
) -> str:
    request = _read_request(request_path)
    view: dict[str, Any] = {
        "incident_id": request.incident_id,
        "observed_at": "2026-09-28T10:00:05Z",
        "blocking_detectors": sorted({finding["detector_id"] for finding in findings}),
        "blocking_findings": findings,
    }
    if clearing is not None:
        view["clearing_findings"] = clearing
    if changes is not None:
        view["state_changes"] = {
            "baseline_at": "2026-09-28T09:58:00Z",
            "observed_at": "2026-09-28T10:00:05Z",
            "changes": changes,
        }
    return json.dumps({"version": "sdo.controller/v1", "incident_view": view})


def _finding(detector: str, rule: str, kind: str, name: str) -> dict[str, Any]:
    return {
        "detector_id": detector,
        "rule_id": rule,
        "status": "active",
        "severity": "critical",
        "summary": f"{kind} {name} is unavailable",
        "evidence": f"deployment hotel-reservation/{name} has 0/1 available replicas",
        "primary_resource": {"api_version": "apps/v1", "kind": kind, "namespace": "hotel-reservation", "name": name},
        "fingerprint": f"{detector}/{rule}/{name}",
    }


def _reader(payload: str | None, calls: list[str] | None = None) -> Any:
    def read(location: str) -> str:
        if calls is not None:
            calls.append(location)
        if payload is None:
            raise ControllerViewError('configmaps "sdo-controller-state" is forbidden')
        return payload

    return read


def test_the_controllers_health_detectors_block_what_traffic_cannot_see(
    prober: tuple[_FakeProber, str], tmp_path: Path
) -> None:
    """A deleted ConfigMap behind an in-memory cache leaves traffic healthy, but closure still waits."""

    _, url = prober
    request_path = _traffic_request(tmp_path)
    calls: list[str] = []
    payload = _view(
        request_path, findings=[_finding("health-objective", "deployment-unavailable", "Deployment", "mongodb-geo")]
    )

    status = incident_status(
        prober_url=url,
        request_path=request_path,
        controller_state="hotel-reservation-sdo/sdo-controller-state",
        controller_reader=_reader(payload, calls),
    )

    assert calls == ["hotel-reservation-sdo/sdo-controller-state"]
    assert status.state == IncidentStatusState.UNHEALTHY
    assert status.exit_code == EXIT_UNHEALTHY
    assert status.blocking_findings[0].primary_resource.name == "mongodb-geo"
    text = status.render()
    assert "health-objective" in text
    assert "mongodb-geo" in text
    assert "will not close" in text
    assert status.to_json()["controller"]["blocking_detectors"] == ["health-objective"]


def test_findings_the_gate_confirmed_clear_do_not_block_submission(
    prober: tuple[_FakeProber, str], tmp_path: Path
) -> None:
    """After a correct fix the controller confirms the finding clear in seconds; closure's count does not gate."""

    _, url = prober
    request_path = _traffic_request(tmp_path)
    payload = _view(
        request_path,
        findings=[],
        clearing=[_finding("health-objective", "configmap-missing", "ConfigMap", "mongo-geo-script")],
    )

    status = incident_status(
        prober_url=url, request_path=request_path, controller_state="ns/cm", controller_reader=_reader(payload)
    )

    assert status.state == IncidentStatusState.HEALTHY
    assert status.exit_code == EXIT_HEALTHY
    assert status.blocking_findings == ()
    assert [finding.primary_resource.name for finding in status.clearing_findings] == ["mongo-geo-script"]
    text = status.render()
    assert "mongo-geo-script" in text
    assert "confirmed clear" in text
    assert status.to_json()["controller"]["clearing_detectors"] == ["health-objective"]


def test_a_partial_fix_still_blocks_on_the_finding_that_fires(prober: tuple[_FakeProber, str], tmp_path: Path) -> None:
    _, url = prober
    request_path = _traffic_request(tmp_path)
    payload = _view(
        request_path,
        findings=[_finding("health-objective", "configmap-missing", "ConfigMap", "mongo-rate-script")],
        clearing=[_finding("health-objective", "configmap-missing", "ConfigMap", "mongo-geo-script")],
    )

    status = incident_status(
        prober_url=url, request_path=request_path, controller_state="ns/cm", controller_reader=_reader(payload)
    )

    assert status.state == IncidentStatusState.UNHEALTHY
    assert [finding.primary_resource.name for finding in status.blocking_findings] == ["mongo-rate-script"]
    assert [finding.primary_resource.name for finding in status.clearing_findings] == ["mongo-geo-script"]


def test_traffic_findings_in_the_view_defer_to_the_fresher_burst(
    prober: tuple[_FakeProber, str], tmp_path: Path
) -> None:
    _, url = prober
    request_path = _traffic_request(tmp_path)
    payload = _view(
        request_path, findings=[_finding("traffic-health", "scenario-slo.search-hotels", "Service", "frontend")]
    )

    status = incident_status(
        prober_url=url, request_path=request_path, controller_state="ns/cm", controller_reader=_reader(payload)
    )

    assert status.state == IncidentStatusState.HEALTHY
    assert status.blocking_findings == ()


def test_a_view_of_another_incident_or_an_unreadable_one_falls_back_to_the_burst(
    prober: tuple[_FakeProber, str], tmp_path: Path
) -> None:
    _, url = prober
    request_path = _traffic_request(tmp_path)
    stale = json.loads(
        _view(request_path, findings=[_finding("health-objective", "deployment-unavailable", "Deployment", "geo")])
    )
    stale["incident_view"]["incident_id"] = "an-earlier-incident"

    other = incident_status(
        prober_url=url,
        request_path=request_path,
        controller_state="ns/cm",
        controller_reader=_reader(json.dumps(stale)),
    )
    unreadable = incident_status(
        prober_url=url, request_path=request_path, controller_state="ns/cm", controller_reader=_reader(None)
    )
    closed = incident_status(
        prober_url=url,
        request_path=request_path,
        controller_state="ns/cm",
        controller_reader=_reader(json.dumps({"version": "sdo.controller/v1"})),
    )

    for status in (other, unreadable, closed):
        assert status.state == IncidentStatusState.HEALTHY
        assert status.blocking_findings == ()
    assert "an-earlier-incident" in other.controller_detail
    assert "forbidden" in unreadable.controller_detail
    assert "no open incident" in closed.controller_detail


def test_changes_after_the_request_was_taken_are_reported(prober: tuple[_FakeProber, str], tmp_path: Path) -> None:
    """A second fault that lands after dispatch is missing from the request's diff; status shows it."""

    _, url = prober
    request_path = _traffic_request(tmp_path)
    request = json_format.MessageToDict(_read_request(request_path), preserving_proto_field_name=True)
    request["state_changes"] = {
        "baseline_at": "2026-09-28T09:58:00Z",
        "observed_at": "2026-09-28T10:00:00Z",
        "changes": [{"kind": "ConfigMap", "name": "mongo-geo-script", "change": "removed"}],
    }
    _write_task(request_path, json_format.ParseDict(request, IncidentRequest()))
    payload = _view(
        request_path,
        findings=[],
        changes=[
            {"kind": "ConfigMap", "name": "mongo-geo-script", "change": "removed"},
            {"kind": "ConfigMap", "name": "mongo-rate-script", "change": "removed"},
        ],
    )

    status = incident_status(
        prober_url=url, request_path=request_path, controller_state="ns/cm", controller_reader=_reader(payload)
    )

    assert [(change.kind, change.name) for change in status.new_state_changes] == [("ConfigMap", "mongo-rate-script")]
    assert "ConfigMap/mongo-rate-script removed" in status.render()
    assert status.to_json()["controller"]["new_state_changes"][0]["name"] == "mongo-rate-script"


def test_a_state_with_fields_outside_the_contract_is_not_trusted(
    prober: tuple[_FakeProber, str], tmp_path: Path
) -> None:
    _, url = prober
    request_path = _traffic_request(tmp_path)
    payload = json.loads(
        _view(request_path, findings=[_finding("health-objective", "deployment-unavailable", "Deployment", "geo")])
    )
    payload["IncidentView"] = payload["incident_view"]

    status = incident_status(
        prober_url=url,
        request_path=request_path,
        controller_state="ns/cm",
        controller_reader=_reader(json.dumps(payload)),
    )

    assert status.blocking_findings == ()
    assert "controller state invalid" in status.controller_detail


def test_the_cli_reads_the_controller_state_the_controller_names(
    prober: tuple[_FakeProber, str], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _, url = prober
    request_path = _traffic_request(tmp_path)
    payload = _view(
        request_path, findings=[_finding("health-objective", "deployment-unavailable", "Deployment", "mongodb-geo")]
    )
    calls: list[str] = []
    monkeypatch.setenv("SDO_PROBER_URL", url)
    monkeypatch.setenv("SDO_REQUEST_PATH", str(request_path))
    monkeypatch.setenv("SDO_CONTROLLER_STATE", "hotel-reservation-sdo/sdo-controller-state")
    monkeypatch.setattr(incident_status_module, "read_controller_state", _reader(payload, calls))

    assert run_incident_status_cli(workload=None, scenarios=(), as_json=False) == EXIT_UNHEALTHY
    assert calls == ["hotel-reservation-sdo/sdo-controller-state"]


def test_the_controller_state_is_read_with_kubectl(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[list[str]] = []

    class Completed:
        returncode = 0
        stdout = json.dumps({"data": {"runtime-state.json": '{"version": "sdo.controller/v1"}'}})
        stderr = ""

    def run(argv: list[str], **_: Any) -> Completed:
        seen.append(argv)
        return Completed()

    monkeypatch.setattr(incident_status_module.subprocess, "run", run)

    assert (
        incident_status_module.read_controller_state("ctl/sdo-controller-state") == '{"version": "sdo.controller/v1"}'
    )
    assert seen == [["kubectl", "get", "configmap", "sdo-controller-state", "-n", "ctl", "-o", "json"]]
    with pytest.raises(ControllerViewError, match="NAMESPACE/NAME"):
        incident_status_module.read_controller_state("no-slash")
