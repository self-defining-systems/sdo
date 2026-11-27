"""Exercise the external-command subject at the hook boundary with a scripted agent command."""

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import pytest

from benchmarks.fuzzops import external, row_metrics, vanilla

AGENT = r"""#!/usr/bin/env python3
import datetime, json, os, pathlib, sys, time
at = lambda s: datetime.datetime.fromtimestamp(time.time() + s, datetime.timezone.utc).isoformat()
args = dict(zip(sys.argv[1::2], sys.argv[2::2]))
assert args['--phase'] == 'incident'
assert '--state-in' not in args
for name in ('AWS_SHARED_CREDENTIALS_FILE', 'BENCH_FAULT', 'BENCH_LANE_DIR', 'FUZZOPS_TRAFFIC'):
    assert name not in os.environ
assert 'secret-fault' not in json.dumps(dict(os.environ)) + json.dumps(sys.argv)
prompt = pathlib.Path(args['--prompt-file']).read_text()
assert prompt == 'Users are reporting problems with the application in namespace fg1; investigate and fix it.'
token = json.loads(pathlib.Path(args['--kubeconfig']).read_text())['users'][0]['user']['token']
cell = pathlib.Path(args['--cell-dir'])
out = cell / 'incident'
out.mkdir()
(cell / 'agent').mkdir()
(cell / 'agent' / 'memory.json').write_text('{}')
(out / 'host.txt').symlink_to('/etc/hostname')
(out / 'prompt.txt').write_text(prompt)
rows = [
    {'type': 'assistant', 'timestamp': at(20), 'message': {'content': [
        {'type': 'tool_use', 'id': 't1', 'name': 'Bash',
         'input': {'command': 'kubectl -n fg1 scale deploy/web --replicas=2'}}]}},
    {'type': 'user', 'timestamp': at(21), 'message': {'content': [
        {'type': 'tool_result', 'tool_use_id': 't1', 'content': 'deployment.apps/web scaled ' + token}]}},
]
(out / 'transcript.jsonl').write_text(''.join(json.dumps(r) + '\n' for r in rows))
(out / 'usage.json').write_text('{"input_tokens": 7, "output_tokens": 3, "total_cost_usd": 0.1}')
(out / 'agent-result.json').write_text(json.dumps({
    'schema': 'sdo.external-agent-result/v1', 'phase': 'incident', 'label': 'scripted', 'agent_version': '1',
    'started_at': at(10), 'ended_at': at(40), 'timed_out': False,
    'required_evidence': ['incident/prompt.txt'], 'problems': [], 'summary': 'scaled web'}))
"""


def lane(tmp_path: Path, agent: str) -> dict[str, str]:
    command = tmp_path / "agent-command"
    command.write_text(agent)
    command.chmod(0o755)
    auth = tmp_path / "auth.json"
    auth.write_text('{"private_key":"synthetic-vertex-key"}')
    operator = tmp_path / "operator.json"
    extra = ["CLAUDE_CODE_USE_VERTEX=1", "CLOUD_ML_REGION=global", "ANTHROPIC_VERTEX_PROJECT_ID=synthetic"]
    operator.write_text(
        json.dumps(
            {
                "settings": {
                    "auth_file": str(auth),
                    "window_seconds": 600,
                    "config": {"agent": {"extra_env": extra}},
                    "external": {"command": [str(command)], "label": "scripted arm", "model": "synthetic-model"},
                }
            }
        )
    )
    state = tmp_path / "lane" / "vanilla"
    state.mkdir(parents=True)
    (state / "kubeconfig").write_text('{"users":[{"user":{"token":"synthetic-kube-token"}}]}')
    return {
        "PATH": os.environ["PATH"],
        "AWS_SHARED_CREDENTIALS_FILE": "/synthetic/aws",
        "BENCH_OPERATOR": str(operator),
        "BENCH_OUT": str(tmp_path / "run" / "operator"),
        "BENCH_LANE_DIR": str(tmp_path / "lane"),
        "BENCH_RUN": "proof",
        "BENCH_FAULT": "secret-fault",
        "FUZZOPS_NAMESPACE": "fg1",
        "FUZZOPS_REPORT": str(tmp_path / "report.json"),
    }


def page(env: dict[str, str]) -> Path:
    target = vanilla.run_dir(env)
    target.mkdir()
    external.capture(env, target, time.time())
    return target


def test_command_gets_the_plain_agents_page_and_its_evidence_is_collected(tmp_path):
    env = lane(tmp_path, AGENT)
    assert external.main(["check"], env) == 0
    page(env)
    assert external.main(["collect"], env) == 0
    out = Path(env["BENCH_OUT"])
    manifest = json.loads((out / "evidence-manifest.json").read_text())
    assert manifest["arm"] == "external"
    assert manifest["problems"] == []
    assert json.loads((out / "done.json").read_text())["model"] == "synthetic-model"
    assert json.loads(Path(env["FUZZOPS_REPORT"]).read_text()) == {"summary": "scaled web"}
    kept = {p.relative_to(out).as_posix() for p in out.rglob("*") if p.is_file()}
    assert "session-dir/incident/agent-result.json" in kept
    assert not [p for p in kept if "host.txt" in p or "agent/memory" in p or "kubeconfig" in p]
    assert "synthetic-kube-token" not in "".join(p.read_text() for p in out.rglob("*") if p.is_file())
    row = row_metrics.measure(out.parent)
    assert row.arm == "external"
    assert row.responder_cost_usd == 0.1
    # From the page, not from the start the command reports.
    assert 20 <= (row.anchor_to_last_write_s or 0) < 30


def test_incomplete_evidence_fails_collect(tmp_path):
    env = lane(tmp_path, "#!/bin/sh\nexit 3\n")
    page(env)
    assert external.main(["collect"], env) == 1
    problems = json.loads((Path(env["BENCH_OUT"]) / "evidence-manifest.json").read_text())["problems"]
    assert any("agent-result.json missing" in p for p in problems)
    assert any("no stream-json transcript" in p for p in problems)


def test_a_command_past_the_deadline_is_killed_and_recorded(tmp_path, monkeypatch):
    monkeypatch.setattr(vanilla, "AGENT_SECONDS", 0)
    monkeypatch.setattr(external, "EVIDENCE_GRACE_SECONDS", 0)
    env = lane(tmp_path, "#!/bin/sh\nsleep 600\n")
    done = json.loads((page(env) / "done.json").read_text())
    assert done["capped"] is True
    assert any("killed" in p for p in done["problems"])
    assert datetime.fromisoformat(done["ended_at"]).tzinfo == timezone.utc


@pytest.mark.parametrize(
    "broken",
    [{}, {"command": [], "label": "x", "model": "m"}, {"command": ["relative"], "label": "x", "model": "m"}],
)
def test_settings_are_validated(tmp_path, capsys, broken):
    env = lane(tmp_path, AGENT)
    operator = Path(env["BENCH_OPERATOR"])
    document = json.loads(operator.read_text())
    document["settings"]["external"] = broken
    operator.write_text(json.dumps(document))
    assert external.main(["check"], env) == 1
    assert "settings.external" in capsys.readouterr().out
