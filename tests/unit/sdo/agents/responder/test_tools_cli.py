import json
import threading
import time

from sdo.agents.responder import tools_cli


def test_sdo_cli_exchanges_typed_request_with_fake_controller(tmp_path, monkeypatch, capsys):
    requests = tmp_path / "requests"
    responses = tmp_path / "responses"
    requests.mkdir()
    responses.mkdir()
    monkeypatch.setenv("SDO_TOOLS_DIR", str(tmp_path))
    seen = []

    def controller():
        for _ in range(100):
            files = list(requests.glob("*.json"))
            if files:
                request = json.loads(files[0].read_text())
                seen.append(request)
                (responses / files[0].name).write_text(
                    json.dumps({"command": "diagnose", "summary": "one match", "program_runs": []})
                )
                return
            time.sleep(0.01)

    worker = threading.Thread(target=controller)
    worker.start()
    try:
        assert tools_cli.main(["diagnose", "targetport", "--bind", "service=Service/app/api"]) == 0
    finally:
        worker.join(timeout=2)
    assert seen == [
        {
            "command": "diagnose",
            "program_id": "targetport",
            "bindings": {"service": {"api_version": "v1", "kind": "Service", "namespace": "app", "name": "api"}},
        }
    ]
    output = capsys.readouterr().out
    assert '"command": "diagnose"' in output
    assert "one match" in output
