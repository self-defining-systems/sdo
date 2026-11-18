"""The host evidence sink as fuzzops-bench reads it: layout, ``agents_ran``, transcript coverage, interval copy."""

from __future__ import annotations

import hashlib
import json
import os
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import pytest

from benchmarks.fuzzops import sink

if TYPE_CHECKING:
    from pathlib import Path

T0 = datetime(2026, 11, 12, 10, 0, tzinfo=UTC)
T1 = datetime(2026, 11, 12, 10, 30, tzinfo=UTC)


def _run(tmp: Path, schema: str = "sdo.evidence/v1") -> Path:
    run = tmp / "e-l-1"
    run.mkdir()
    (run / "layout.json").write_text(json.dumps({"schema": schema, "run_id": "e-l-1", "created_at": "x"}))
    return run


def _incident(run: Path, name: str, ledger: dict[str, Any] | None, rollouts: int = 0) -> Path:
    directory = run / "incidents" / name
    directory.mkdir(parents=True)
    if ledger is not None:
        (directory / "ledger.json").write_text(json.dumps(ledger))
    for i in range(rollouts):
        deep = directory / "claude" / "projects" / f"p{i}"
        deep.mkdir(parents=True)
        (deep / f"rollout-{i}.jsonl").write_text("{}\n")
    return directory


def _closure(**result: Any) -> dict[str, Any]:
    return {"detected_at": "2026-11-12T10:01:00.123456789Z", "result": result}


def test_claude_session_files_count_as_transcripts(tmp_path: Path) -> None:
    run = _run(tmp_path)
    responded = {"closure": _closure(responder_session_id="s"), "outcome_document": {"classification": "success"}}
    directory = _incident(run, "a" * 32, responded)
    session = directory / "claude" / "projects" / "-workspace-checkout"
    session.mkdir(parents=True)
    (session / "d50edf54-080a-43b2-9c16-18423070962b.jsonl").write_text("{}\n")
    assert sink.count_rollouts(directory) == 1
    assert sink.transcripts_verdict(run) == ""


# ---- environment ---------------------------------------------------------------------------------------------


def test_install_env_names_the_node_path_and_run_id() -> None:
    env = {"BENCH_EVIDENCE_NODE": "/bench-evidence", "BENCH_EVIDENCE_RUN": "exp-lane-7"}
    assert sink.install_env(env) == {"SDO_EVIDENCE_HOST_PATH": "/bench-evidence", "SDO_EVIDENCE_RUN_ID": "exp-lane-7"}


@pytest.mark.parametrize(
    ("env", "message"),
    [
        ({"BENCH_EVIDENCE_RUN": "r"}, "BENCH_EVIDENCE_NODE"),
        ({"BENCH_EVIDENCE_NODE": "/n"}, "BENCH_EVIDENCE_RUN"),
        ({"BENCH_EVIDENCE_NODE": "rel", "BENCH_EVIDENCE_RUN": "r"}, "absolute"),
        ({"BENCH_EVIDENCE_NODE": "/n", "BENCH_EVIDENCE_RUN": "../x"}, "run ID"),
    ],
)
def test_install_env_refuses_a_lane_without_a_sink(env: dict[str, str], message: str) -> None:
    with pytest.raises(sink.SinkError, match=message):
        sink.install_env(env)


def test_host_run_dir_joins_the_host_directory_and_run(tmp_path: Path) -> None:
    env = {"BENCH_EVIDENCE_HOST": str(tmp_path), "BENCH_EVIDENCE_RUN": "r1"}
    assert sink.host_run_dir(env) == tmp_path / "r1"
    with pytest.raises(sink.SinkError, match="BENCH_EVIDENCE_HOST"):
        sink.host_run_dir({"BENCH_EVIDENCE_RUN": "r1"})


# ---- agents_ran ----------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "ledger",
    [
        {"incident_id": "i"},  # no closure yet: an agent may be running
        {"closure": {"detected_at": "x"}},  # closure without result or dispatch error
        {"closure": _closure(responder_session_id="0199-abc")},
        {"closure": _closure(resolution_path="responder")},
        {"closure": _closure(usage={"llm_calls": 2})},
        {"closure": _closure(resolution_path="deterministic"), "outcome_document": {"usage": {"input_tokens": "9"}}},
        {"closure": _closure(resolution_path="deterministic"), "reflection_dispatch_at": "2026-11-12T10:09:00Z"},
        {"closure": _closure(resolution_path="deterministic"), "reflection_phase": "REFLECTION_PHASE_DISPATCH"},
        {
            "closure": _closure(resolution_path="deterministic"),
            "reflection_decision": {"learning_decision": "no_change"},
        },
        {"closure": _closure(resolution_path="deterministic"), "reflectionResults": [{"usage": {"outputTokens": 3}}]},
        {"closure": _closure(resolutionPath="responder")},
    ],
)
def test_agents_ran_unless_the_ledger_proves_otherwise(ledger: dict[str, Any]) -> None:
    assert sink.agents_ran(ledger)


@pytest.mark.parametrize(
    "ledger",
    [
        {"closure": _closure(resolution_path="deterministic", usage={})},
        {"closure": {"detected_at": "x", "dispatch_error": "no worker"}},
        {
            "closure": _closure(resolution_path="deterministic"),
            "outcome_document": {"resolution_path": "deterministic", "reflection_skip_reason": "no_session"},
            "reflection_phase": "REFLECTION_PHASE_DONE",
            "reflection_results": [{"learning_decision": "failed", "failure_reason": "session_unavailable"}],
        },
    ],
)
def test_no_agent_ran(ledger: dict[str, Any]) -> None:
    assert not sink.agents_ran(ledger)


# ---- transcripts verdict -------------------------------------------------------------------------------------


def test_verdict_needs_the_layout(tmp_path: Path) -> None:
    assert sink.transcripts_verdict(tmp_path / "absent").startswith("no sink: ")
    assert "not sdo.evidence/v1" in sink.transcripts_verdict(_run(tmp_path, schema="other"))


def test_verdict_checks_every_incident_with_a_ledger(tmp_path: Path) -> None:
    run = _run(tmp_path)
    responded = {"closure": _closure(responder_session_id="s"), "outcome_document": {"classification": "success"}}
    _incident(run, "a" * 32, responded, rollouts=2)
    _incident(run, "b" * 32, None)  # no ledger yet: not checked
    _incident(run, "c" * 32, {"closure": _closure(resolution_path="deterministic")})  # no agent ran
    assert sink.transcripts_verdict(run) == ""
    _incident(run, "d" * 32, responded)
    _incident(run, "e" * 32, {"incident_id": "open"})
    assert sink.transcripts_verdict(run) == f"transcripts missing: 2 ({'d' * 8}, {'e' * 8})"


# ---- interval copy -------------------------------------------------------------------------------------------


def _ledger(incident: str, detected: str, acknowledged: str | None = None) -> dict[str, Any]:
    ledger: dict[str, Any] = {
        "incident_id": incident,
        "closure": {"detected_at": detected, "result": {"responder_session_id": "s"}},
        "outcome_document": {"incident_id": incident},
    }
    if acknowledged:
        ledger["acknowledged_at"] = acknowledged
    return ledger


def _age(directory: Path, at: datetime) -> None:
    for path in [directory, *directory.rglob("*")]:
        os.utime(path, (at.timestamp(), at.timestamp()))


def test_copy_interval_takes_incidents_detected_in_or_open_during_the_interval(tmp_path: Path) -> None:
    run = _run(tmp_path)
    old = datetime(2026, 11, 11, tzinfo=UTC)
    inside = _incident(run, "1" * 32, _ledger("inside", "2026-11-12T10:05:00Z"), rollouts=1)
    spanning = _incident(run, "2" * 32, _ledger("spanning", "2026-11-12T09:00:00Z"), rollouts=1)
    before = _incident(run, "3" * 32, _ledger("before", "2026-11-12T09:00:00Z", "2026-11-12T09:30:00Z"), 1)
    after = _incident(run, "4" * 32, _ledger("after", "2026-11-12T11:00:00Z"), rollouts=1)
    missing = _incident(run, "5" * 32, _ledger("missing", "2026-11-12T10:10:00Z"))
    no_ledger_old = _incident(run, "6" * 32, None, rollouts=1)
    no_ledger_new = _incident(run, "7" * 32, None, rollouts=1)
    for d in (inside, spanning, before, after, missing, no_ledger_old):
        _age(d, old)
    _age(no_ledger_new, datetime(2026, 11, 12, 10, 15, tzinfo=UTC))
    task_in = run / "lifecycle" / "judge-1"
    task_out = run / "lifecycle" / "judge-0"
    task_in.mkdir(parents=True)
    task_out.mkdir(parents=True)
    (task_in / "result.json").write_text("{}")
    (task_out / "result.json").write_text("{}")
    _age(task_out, old)
    _age(task_in, old)
    records = [
        {"kind": "incident", "id": "inside", "path": "incidents/" + "1" * 32, "sealed_at": "2026-11-12T10:20:00Z"},
        {"kind": "incident", "id": "before", "path": "incidents/" + "3" * 32, "sealed_at": "2026-11-12T09:31:00Z"},
        {"kind": "task", "id": "judge-1", "path": "lifecycle/judge-1", "sealed_at": "2026-11-12T10:10:00Z"},
    ]
    (run / "index.jsonl").write_text("".join(json.dumps(r) + "\n" for r in records) + '{"torn')
    dest = tmp_path / "out" / "evidence"
    result = sink.copy_interval(run, dest, T0, T1)
    copied = sorted(p.name[:1] for p in (dest / "incidents").iterdir())
    assert copied == ["1", "2", "5", "7"]
    assert (dest / "lifecycle" / "judge-1" / "result.json").is_file()
    assert not (dest / "lifecycle" / "judge-0").exists()
    assert (dest / "layout.json").is_file()
    kept = [json.loads(line)["id"] for line in (dest / "index.jsonl").read_text().splitlines()]
    assert kept == ["inside", "judge-1"]
    summary = result.summary()
    assert summary["rollouts"] == 3
    assert summary["missing"] == ["missing"]
    by_id = {i["id"]: i for i in summary["incidents"]}
    assert by_id["inside"] == {"id": "inside", "outcome": True, "agents_ran": True, "rollouts": 1}
    assert by_id["7" * 32]["outcome"] is False


def test_copy_interval_refuses_a_missing_sink(tmp_path: Path) -> None:
    with pytest.raises(sink.SinkError, match="no sink"):
        sink.copy_interval(tmp_path / "none", tmp_path / "dest", T0, T1)


def test_parse_time_accepts_protojson_nanoseconds() -> None:
    assert sink.parse_time("2026-11-12T10:01:00.123456789Z") == datetime(2026, 11, 12, 10, 1, 0, 123456, tzinfo=UTC)
    assert sink.parse_time("bad") is None


# ---- pull mode (provider vcluster: no host mount) ------------------------------------------------------------


PULL_ENV = {"BENCH_EVIDENCE_MODE": "pull", "BENCH_EVIDENCE_RUN": "exp-lane-7"}


def test_install_env_in_pull_mode_names_the_evidence_claim_and_needs_no_node_path() -> None:
    env = sink.install_env(PULL_ENV)
    assert json.loads(env["SDO_EVIDENCE_VOLUME"]) == {"persistentVolumeClaim": {"claimName": sink.EVIDENCE_CLAIM}}
    assert env["SDO_EVIDENCE_RUN_ID"] == "exp-lane-7"
    assert "SDO_EVIDENCE_HOST_PATH" not in env
    with pytest.raises(sink.SinkError, match="BENCH_EVIDENCE_RUN"):
        sink.install_env({"BENCH_EVIDENCE_MODE": "pull"})


def test_the_pull_pod_fits_the_restricted_pod_security_level() -> None:
    pod = sink.puller_manifest("ns-sdo", "p")["spec"]
    container = pod["containers"][0]
    assert pod["securityContext"]["runAsNonRoot"] is True
    assert pod["securityContext"]["seccompProfile"] == {"type": "RuntimeDefault"}
    assert container["securityContext"] == {"allowPrivilegeEscalation": False, "capabilities": {"drop": ["ALL"]}}
    assert container["volumeMounts"][0]["readOnly"] is True
    assert "hostPath" not in json.dumps(pod)


def _fake_kubectl(
    tmp: Path, monkeypatch: pytest.MonkeyPatch, sink_dir: Path | None, *, exec_fails: bool = False
) -> Path:
    """A kubectl that logs its calls and serves ``tar`` of ``sink_dir`` for ``exec``."""
    log = tmp / "kubectl.log"
    script = tmp / "bin" / "kubectl"
    script.parent.mkdir()
    exec_body = "exit 1" if exec_fails or sink_dir is None else f'exec tar -C "{sink_dir}" -cf - .'
    script.write_text(
        '#!/bin/sh\necho "$*" >> "$CALLS"\n'
        'for a in "$@"; do [ "$a" = exec ] && { ' + exec_body + "; }; done\ncat >/dev/null 2>&1 </dev/null\nexit 0\n"
    )
    script.chmod(0o755)
    monkeypatch.setenv("PATH", f"{script.parent}:{os.environ['PATH']}")
    monkeypatch.setenv("CALLS", str(log))
    return log


def test_pull_run_dir_copies_the_claim_out_and_removes_its_pod(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    served = tmp_path / "served"
    (served / "incidents" / "a").mkdir(parents=True)
    (served / "layout.json").write_text('{"schema": "sdo.evidence/v1"}')
    (served / "incidents" / "a" / "ledger.json").write_text("{}")
    log = _fake_kubectl(tmp_path, monkeypatch, served)
    env = {**PULL_ENV, "BENCH_EVIDENCE_HOST": str(tmp_path / "host")}
    sink.pull_run_dir(env, lambda *a: ["kubectl", "--context", "kind-l", *a], "ns-sdo")
    dest = tmp_path / "host" / "exp-lane-7"
    assert (dest / "layout.json").is_file()
    assert (dest / "incidents" / "a" / "ledger.json").is_file()
    calls = log.read_text().splitlines()
    assert any("exec" in c and "/sink/exp-lane-7" in c for c in calls)
    assert "delete pod" in calls[-1]
    assert "--ignore-not-found" in calls[-1]


def test_pull_run_dir_reports_a_missing_run_directory_and_still_removes_its_pod(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    log = _fake_kubectl(tmp_path, monkeypatch, None)
    env = {**PULL_ENV, "BENCH_EVIDENCE_HOST": str(tmp_path / "host")}
    with pytest.raises(sink.SinkError, match="evidence pull"):
        sink.pull_run_dir(env, lambda *a: ["kubectl", *a], "ns-sdo")
    assert "delete pod" in log.read_text().splitlines()[-1]


# ---- pushing a tree into the evidence claim (dev mount without a node directory) -------------------------------


def test_the_push_pod_mounts_the_claim_writable_and_fits_the_restricted_level() -> None:
    pod = sink.pusher_manifest("ns-sdo", "p")["spec"]
    assert pod["securityContext"]["runAsNonRoot"] is True
    mount = pod["containers"][0]["volumeMounts"][0]
    assert mount["mountPath"] == "/sink"
    assert not mount.get("readOnly")
    assert pod["volumes"][0]["persistentVolumeClaim"] == {"claimName": sink.EVIDENCE_CLAIM}
    assert "hostPath" not in json.dumps(pod)


def _fake_pushing_kubectl(tmp: Path, monkeypatch: pytest.MonkeyPatch, landing: Path) -> Path:
    """A kubectl that logs its calls and unpacks the stdin of ``exec ... tar -xf`` into ``landing``."""
    log = tmp / "kubectl.log"
    script = tmp / "bin" / "kubectl"
    script.parent.mkdir()
    script.write_text(
        '#!/bin/sh\necho "$*" >> "$CALLS"\n'
        'case "$*" in *" -xf "*) exec tar -C "$LANDING" -xf - ;; esac\n'
        "cat >/dev/null 2>&1 </dev/null\nexit 0\n"
    )
    script.chmod(0o755)
    monkeypatch.setenv("PATH", f"{script.parent}:{os.environ['PATH']}")
    monkeypatch.setenv("CALLS", str(log))
    monkeypatch.setenv("LANDING", str(landing))
    return log


def test_push_paths_empties_the_wiped_directories_in_place_unpacks_the_tree_and_removes_its_pod(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "stage"
    (root / ".dev" / "sdo").mkdir(parents=True)
    (root / ".dev" / "sdo" / "a.py").write_text("x")
    landing = tmp_path / "claim"
    landing.mkdir()
    log = _fake_pushing_kubectl(tmp_path, monkeypatch, landing)
    sink.push_paths(lambda *a: ["kubectl", *a], "ns-sdo", root, [".dev"], wipe=[".dev/sdo"])
    assert (landing / ".dev" / "sdo" / "a.py").read_text() == "x"
    calls = log.read_text().splitlines()
    wipe = next(c for c in calls if "find" in c)
    assert "-mindepth 1 -delete" in wipe  # emptied, not removed: running pods keep their mount
    assert "/sink/.dev/sdo" in wipe
    assert "delete pod" in calls[-1]
    assert "--ignore-not-found" in calls[-1]


def test_push_paths_reports_a_failed_unpack_and_still_removes_its_pod(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "stage"
    root.mkdir()
    log = _fake_pushing_kubectl(tmp_path, monkeypatch, tmp_path / "missing")
    with pytest.raises(sink.SinkError, match="push"):
        sink.push_paths(lambda *a: ["kubectl", *a], "ns-sdo", root, ["."])
    assert "delete pod" in log.read_text().splitlines()[-1]


def test_the_evidence_claim_names_the_lane_volume_node_when_given() -> None:
    assert "annotations" not in sink.claim_manifest("ns-sdo")["metadata"]
    claim = sink.claim_manifest("ns-sdo", node="n2")
    assert claim["metadata"]["annotations"] == {"volume.kubernetes.io/selected-node": "n2"}


def test_volume_node_is_one_schedulable_ready_node_and_stable_per_lane() -> None:
    nodes = {
        "items": [
            {
                "metadata": {"name": name},
                "spec": {"unschedulable": cordoned},
                "status": {"conditions": [{"type": "Ready", "status": ready}]},
            }
            for name, cordoned, ready in (
                ("a", False, "True"),
                ("b", True, "True"),
                ("c", False, "False"),
                ("d", False, "True"),
            )
        ]
    }
    picked = {sink.volume_node(nodes, f"lane{i}") for i in range(20)}
    assert picked == {"a", "d"}
    assert sink.volume_node(nodes, "lane3") == sink.volume_node(nodes, "lane3")
    with pytest.raises(sink.SinkError, match="no schedulable"):
        sink.volume_node({"items": []}, "lane1")


def test_a_closing_turn_go_skipped_is_not_an_agent_run() -> None:
    ledger = {
        "closure": {"result": {"resolution_path": "deterministic"}},
        "outcome_document": {"resolution_path": "deterministic", "usage": {}},
        "reflection_dispatch_at": "2026-01-01T00:00:00Z",
        "reflection_phase": "REFLECTION_PHASE_DONE",
        "reflection_decision": {
            "learning_decision": "no_change",
            "no_change_reason": "accepted_programs_no_new_evidence",
        },
        "reflection_results": [
            {"learning_decision": "no_change", "no_change_reason": "closing_turn_nothing_to_script"}
        ],
    }
    assert not sink.agents_ran(ledger)
    assert not sink.closing_turn_ran(ledger)
    ledger["reflection_results"] = [{"learning_decision": "updated", "usage": {"llm_calls": "3"}}]
    assert sink.agents_ran(ledger)
    assert sink.closing_turn_ran(ledger)


def test_a_responder_less_close_with_no_session_is_not_an_agent_run() -> None:
    ledger = {
        "closure": {"result": {"resolution_path": "deterministic"}},
        "outcome_document": {"resolution_path": "deterministic", "usage": {}},
        "reflection_dispatch_at": "2026-01-01T00:00:00Z",
        "reflection_results": [{"learning_decision": "no_change", "no_change_reason": "no_session"}],
    }
    assert not sink.agents_ran(ledger)


def _identity(directory: Path, incident_id: str) -> None:
    (directory / "incident.json").write_text(json.dumps({"incident_id": incident_id}))


def test_late_writes_of_an_earlier_cells_incident_are_not_this_cells(tmp_path: Path) -> None:
    run = _run(tmp_path)
    start = datetime(2026, 11, 12, 10, 0, tzinfo=UTC)
    nanos = lambda at: int(at.timestamp() * 1e9)  # noqa: E731
    earlier = _incident(run, "a" * 32, None, rollouts=1)
    _identity(earlier, f"fg1-{nanos(datetime(2026, 11, 12, 9, 0, tzinfo=UTC))}")
    own = _incident(run, "b" * 32, None, rollouts=1)
    _identity(own, f"fg1-{nanos(datetime(2026, 11, 12, 10, 1, tzinfo=UTC))}")
    for d in (earlier, own):  # both were written during the interval
        _age(d, datetime(2026, 11, 12, 10, 5, tzinfo=UTC))
    assert sink.leftover(earlier, [], start)
    assert not sink.leftover(own, [], start)
    dest = tmp_path / "out"
    sink.copy_interval(run, dest, start, datetime(2026, 11, 12, 11, 0, tzinfo=UTC))
    assert not (dest / "incidents" / ("a" * 32)).exists()
    assert (dest / "incidents" / ("b" * 32)).exists()


def _digest(incident_id: str) -> str:
    return hashlib.sha256(incident_id.encode()).hexdigest()[:32]


def test_an_investigator_directory_follows_its_parent_incident_not_a_unix_time(tmp_path: Path) -> None:
    """The ``-inv-N`` suffix of an investigator's session ID is not a detection time: the directory is attributed to
    its parent incident, copied with it, and left out with it."""
    run = _run(tmp_path)
    start = datetime(2026, 11, 12, 10, 0, tzinfo=UTC)
    nanos = lambda at: int(at.timestamp() * 1e9)  # noqa: E731
    own_parent = f"fg1-{nanos(datetime(2026, 11, 12, 10, 1, tzinfo=UTC))}"
    old_parent = f"fg1-{nanos(datetime(2026, 11, 12, 9, 0, tzinfo=UTC))}"
    own_inv = [_incident(run, _digest(f"{own_parent}-inv-{n}"), None, rollouts=1) for n in range(2)]
    old_inv = _incident(run, _digest(f"{old_parent}-inv-0"), None, rollouts=1)
    for n, directory in enumerate(own_inv):
        _identity(directory, f"{own_parent}-inv-{n}")
    _identity(old_inv, f"{old_parent}-inv-0")
    for directory in (*own_inv, old_inv):
        _age(directory, datetime(2026, 11, 12, 10, 5, tzinfo=UTC))
    assert sink.incident_detected_at(own_inv[0]) == datetime(2026, 11, 12, 10, 1, tzinfo=UTC)
    assert not any(sink.leftover(d, [], start) for d in own_inv)
    assert sink.leftover(old_inv, [], start)
    dest = tmp_path / "out"
    result = sink.copy_interval(run, dest, start, datetime(2026, 11, 12, 11, 0, tzinfo=UTC))
    copied = {p.name for p in (dest / "incidents").iterdir()}
    assert copied == {d.name for d in own_inv}
    assert result.summary()["rollouts"] == 2


def test_an_investigator_directory_is_copied_when_its_parent_is_in_the_interval_even_if_idle(tmp_path: Path) -> None:
    """An investigator whose files were last written before the interval still belongs to a parent that was open in it."""
    run = _run(tmp_path)
    parent = "fg1-1"  # an ID that parses as 1970: only the parent's ledger places it in the interval
    parent_dir = _incident(run, _digest(parent), _ledger(parent, "2026-11-12T10:05:00Z"), rollouts=1)
    inv = _incident(run, _digest(f"{parent}-inv-0"), None, rollouts=1)
    _identity(inv, f"{parent}-inv-0")
    old = datetime(2026, 11, 11, tzinfo=UTC)
    _age(parent_dir, old)
    _age(inv, old)
    dest = tmp_path / "out"
    sink.copy_interval(run, dest, T0, T1)
    assert {p.name for p in (dest / "incidents").iterdir()} == {parent_dir.name, inv.name}
