"""The synthetic onboarding run's app, inputs and metrics keep their contracts."""

from __future__ import annotations

import importlib.util
import json
import threading
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer
from pathlib import Path
from types import ModuleType  # noqa: TC003

import pytest
import yaml

from benchmarks.onboarding_gate import e2e

SYNTHETIC = Path(e2e.__file__).parent / "synthetic"


def _load(path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location(path.stem, path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def notes_app() -> object:
    server_module = _load(SYNTHETIC / "app" / "server.py")
    server_module._seed()
    server = ThreadingHTTPServer(("127.0.0.1", 0), server_module.Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_address[1]}"
    server.shutdown()


def _call(base: str, method: str, path: str, body: object = None, token: str = "") -> tuple[int, object]:
    data = json.dumps(body).encode() if body is not None else None
    request = urllib.request.Request(base + path, data=data, method=method)
    if token:
        request.add_header("Authorization", f"Bearer {token}")
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            raw = response.read()
            return response.status, json.loads(raw) if raw else None
    except urllib.error.HTTPError as error:
        return error.code, json.loads(error.read() or b"null")


def test_synthetic_app_serves_token_sign_in_seeded_reads_and_write_with_cleanup(notes_app: str) -> None:
    assert _call(notes_app, "POST", "/api/login?probe=x", {"user": "user01", "password": "wrong"})[0] == 401
    status, body = _call(notes_app, "POST", "/api/login?probe=x", {"user": "user01", "password": "pass-01-synthetic"})
    assert status == 200
    token = body["token"]  # type: ignore[index]
    assert _call(notes_app, "GET", "/api/notes")[0] == 401
    status, listing = _call(notes_app, "GET", "/api/notes", token=token)
    assert status == 200
    assert {n["title"] for n in listing["notes"]} == {  # type: ignore[index]
        "welcome of user01",
        "groceries of user01",
        "ideas of user01",
    }
    status, note = _call(notes_app, "POST", "/api/notes", {"title": "t1"}, token)
    assert status == 201
    path = f"/api/notes/{note['id']}"  # type: ignore[index]
    assert _call(notes_app, "GET", path, token=token) == (200, note)
    assert _call(notes_app, "DELETE", path + "?probe=x", token=token)[0] == 204
    assert _call(notes_app, "GET", path, token=token)[0] == 404


def test_manifest_embeds_the_server_and_targets_the_namespace() -> None:
    documents = list(yaml.safe_load_all(e2e.render_manifest("gate-ns")))
    assert {d["kind"] for d in documents} == {"ConfigMap", "Deployment", "Service"}
    assert all(d["metadata"]["namespace"] == "gate-ns" for d in documents)
    config_map = next(d for d in documents if d["kind"] == "ConfigMap")
    assert config_map["data"]["server.py"] == (SYNTHETIC / "app" / "server.py").read_text()


def test_goal_lists_every_journey_with_its_id() -> None:
    goal = e2e.goal_document("gate-ns")
    front = yaml.safe_load(goal.split("---\n")[1])
    ids = [line.split("\t")[0] for line in (SYNTHETIC / "app" / "journeys.txt").read_text().splitlines()]
    assert [j["id"] for j in front["journeys"]] == ids == ["sign-in", "browse-notes", "create-note"]


def test_metrics_weight_tokens_like_the_shared_tool_and_count_validator_feedback(tmp_path: Path) -> None:
    events = [
        {
            "type": "assistant",
            "message": {
                "id": "m1",
                "content": [
                    {
                        "type": "tool_use",
                        "id": "t1",
                        "name": "Bash",
                        "input": {"command": "sdo-operator onboarding journeys --repo /work --namespace n --plan"},
                    }
                ],
            },
        },
        {
            "type": "user",
            "message": {
                "content": [
                    {
                        "type": "tool_result",
                        "tool_use_id": "t1",
                        "content": "red budget all: over\nred call-a: passes: run 1 failed\n",
                    }
                ]
            },
        },
        {"type": "assistant", "message": {"id": "m1", "content": [{"type": "text", "text": "same request"}]}},
        {"type": "assistant", "message": {"id": "m2", "content": [{"type": "text", "text": "done"}]}},
        {
            "type": "result",
            "total_cost_usd": 0.25,
            "modelUsage": {
                "m": {
                    "inputTokens": 10,
                    "cacheCreationInputTokens": 100,
                    "cacheReadInputTokens": 1000,
                    "outputTokens": 20,
                }
            },
        },
    ]
    transcript = tmp_path / "t.jsonl"
    transcript.write_text("".join(json.dumps(event) + "\n" for event in events))
    result = e2e.E2EResult("agent", "m")
    e2e.measure(transcript, result)
    assert result.requests == 2
    assert result.weighted_tokens == 10 + 1.25 * 100 + 0.05 * 1000 + 5 * 20
    assert result.cost_usd == 0.25
    assert (result.journeys_runs, result.validator_calls) == (1, 1)
    assert result.rejections == {"budget": 1, "passes": 1}


def test_scripted_agent_steps_are_named_bash_calls() -> None:
    scripted = _load(SYNTHETIC / "scripted_claude.py")
    steps = scripted.steps((SYNTHETIC / "scripted_agent.sh").read_text())
    names = [name for name, _ in steps]
    assert names[0] == "scaffold"
    assert names[-1] == "commit"
    assert "journeys" in names
    assert all(command.strip() for _, command in steps)
