import json
from pathlib import Path
from urllib.request import Request, urlopen

from libs.agent_cli.startup import StartupTelemetry, mark_startup


def test_startup_records_actual_codex_request_start(tmp_path: Path, monkeypatch) -> None:
    target = tmp_path / "startup.json"
    monkeypatch.setenv("SDO_STARTUP_TIMING_PATH", str(target))
    mark_startup("worker_ready")
    with StartupTelemetry() as telemetry:
        telemetry.process_started()
        payload = {
            "resourceLogs": [
                {
                    "scopeLogs": [
                        {
                            "logRecords": [
                                {
                                    "timeUnixNano": "2000000000",
                                    "attributes": [
                                        {"key": "event.name", "value": {"stringValue": "codex.api_request"}},
                                        {"key": "duration_ms", "value": {"intValue": "125"}},
                                    ],
                                }
                            ]
                        }
                    ]
                }
            ]
        }
        request = Request(
            telemetry.endpoint, data=json.dumps(payload).encode(), headers={"Content-Type": "application/json"}
        )
        with urlopen(request, timeout=2) as response:
            assert response.status == 200
    record = json.loads(target.read_text())
    assert set(record) >= {
        "worker_ready",
        "agent_process_started",
        "first_model_request_sent",
        "first_model_request_source",
    }
    assert record["first_model_request_sent"] == "1970-01-01T00:00:01.875000Z"
    assert record["first_model_request_source"] == "codex.otel.api_request.timestamp-minus-duration"
    assert "prompt" not in target.read_text()
