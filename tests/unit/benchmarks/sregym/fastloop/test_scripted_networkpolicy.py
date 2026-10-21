"""Scripted NetworkPolicy learning uses the normal proposal path."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pytest

from benchmarks.sregym.adapter import scripted_worker

ROOT = Path(__file__).resolve().parents[5]


def test_scripted_policy_reflection_uses_captured_repair(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    capture = tmp_path / "capture"
    capture.mkdir()
    policy = {"metadata": {"name": "first", "namespace": "app"}, "spec": {"policyTypes": ["Ingress"]}}
    peer = {"metadata": {"name": "peer", "namespace": "app"}, "spec": {"podSelector": {"matchLabels": {"app": "peer"}}}}
    (capture / "open@detect.json").write_text(json.dumps({"networkPolicies": [policy, peer]}))
    (capture / "open@detect-recordings.json").write_text("[]")
    (capture / "fixed.json").write_text(json.dumps({"networkPolicies": [peer]}))
    # An accepted targetPort program must not suppress learning another class.
    accepted = tmp_path / ".sdo/playbooks" / scripted_worker.PROGRAM_ID
    accepted.mkdir(parents=True)
    (accepted / "diagnose.go").write_text("package targetport\n")
    monkeypatch.setenv("SDO_CAPTURED_CORPUS_DIR", str(capture))
    calls: list[list[str]] = []

    def command(argv: list[str], **_kwargs: Any) -> str:
        calls.append(argv)
        if "scaffold" in argv:
            target = Path(argv[argv.index("--out") + 1])
            target.mkdir()
            (target / "diagnose_test.go").write_text("package scaffold\n")
            assert "--mitigate" not in argv
            return ""
        assert argv[1:3] == ["memory", "check"]
        target = tmp_path / ".sdo/playbooks/networkpolicy-service-isolation"
        assert (target / "diagnose.go").read_bytes() == (
            ROOT / "benchmarks/sregym/fastloop/scripted_networkpolicy_program/diagnose.go"
        ).read_bytes()
        assert not (target / "mitigate.go").exists()
        labels = json.loads(next((tmp_path / ".sdo/corpus").glob("*/labels.json")).read_text())
        assert labels["bindings"]["policy"]["name"] == "first"
        assert labels["peer_bindings"]["policy"]["name"] == "peer"
        return ""

    monkeypatch.setattr(scripted_worker, "_command", command)
    monkeypatch.setattr(
        scripted_worker,
        "NETWORKPOLICY_PROGRAM_SOURCE",
        ROOT / "benchmarks/sregym/fastloop/scripted_networkpolicy_program",
    )
    turn = scripted_worker.ScriptedReflector().resume(
        worktree=tmp_path,
        incident_id="first-incident",
        closure=SimpleNamespace(
            request=SimpleNamespace(namespace="app", drift_events=[SimpleNamespace(investigation_verdict="bug")])
        ),
    )
    assert turn.learning_decision == "updated"
    assert turn.playbook_ids == ["networkpolicy-service-isolation"]
    assert len(calls) == 2
