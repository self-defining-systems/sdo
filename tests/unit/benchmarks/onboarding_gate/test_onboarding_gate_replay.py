"""The offline gate rebuilds what a recorded agent wrote and judges each validator call's files."""

from __future__ import annotations

import json
import shutil
import subprocess
from datetime import UTC, datetime
from pathlib import Path

import pytest

from benchmarks.onboarding_gate.__main__ import start_commit
from benchmarks.onboarding_gate.classify import journeys_rejections, memory_check_rejections
from benchmarks.onboarding_gate.firstread import first_read
from benchmarks.onboarding_gate.replay import Session, apply_file_call, replay
from benchmarks.onboarding_gate.transcript import read_tool_calls


def _tool_use(tool_id: str, name: str, **arguments: object) -> dict[str, object]:
    return {
        "type": "assistant",
        "message": {"content": [{"type": "tool_use", "id": tool_id, "name": name, "input": arguments}]},
    }


def _result(tool_id: str, text: str, error: bool = False) -> dict[str, object]:
    return {
        "type": "user",
        "message": {
            "content": [
                {
                    "type": "tool_result",
                    "tool_use_id": tool_id,
                    "content": [{"type": "text", "text": text}],
                    "is_error": error,
                }
            ]
        },
    }


def _write(path: Path, events: list[dict[str, object]]) -> Path:
    path.write_text("".join(json.dumps(event) + "\n" for event in events))
    return path


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(["git", "-C", str(repo), *args], capture_output=True, text=True, check=True).stdout.strip()


def test_tool_calls_keep_call_order_and_results_and_skip_subagents(tmp_path: Path) -> None:
    sidechain = {**_tool_use("s1", "Bash", command="ls"), "parent_tool_use_id": "agent-1"}
    transcript = _write(
        tmp_path / "t.jsonl",
        [
            {"type": "system", "subtype": "init"},
            {"type": "user", "message": "a plain-text message"},
            _tool_use("a", "Bash", command="sdo-operator onboarding journeys --repo /work --namespace ns --plan"),
            sidechain,
            _result("a", "red budget all: too much"),
            _tool_use("b", "Edit", file_path="/work/x", old_string="1", new_string="2"),
            _result("b", "error", error=True),
        ],
    )
    calls = read_tool_calls(transcript)
    assert [call.tool_use_id for call in calls] == ["a", "b"]
    assert calls[0].validates()
    assert calls[0].sdo_calls() == ["onboarding journeys"]
    assert calls[0].result == "red budget all: too much"
    assert calls[1].is_error
    assert not calls[1].validates()


def test_rejections_are_classed_by_rule_item_and_compile_kind() -> None:
    journeys = "\n".join(
        [
            "  1 failing a: x",
            'red compiles all: /tmp/sdo-build-1/traffic/generators/g.go: generators may not import "math/rand"',
            "red memory-check all: .sdo/arch.md:12: map.coverage: operation x has no probe",
            "red budget all: 5 of 4 requests/s",
            "red passes call-a: run 1 failed",
        ]
    )
    kinds = {r.kind for r in journeys_rejections(journeys)}
    assert kinds == {"compile:import", "map.coverage", "budget", "passes"}
    memory = "\n".join(
        [
            "2026/11/27 16:25:48 INFO memory_check_stage_started stage=x",
            "green compiles",
            "coverage_gap .sdo/arch.md: map.readers: note only",
            ".sdo/diagnostics/traffic/generators/g.go: go.format: not gofmt formatted",
            ".sdo/playbooks/health-objective/README.md:13: text.physical-path: names a path",
        ]
    )
    assert {r.kind for r in memory_check_rejections(memory)} == {"compile:format", "text.physical-path"}


def test_file_calls_apply_under_the_agent_workdir_and_report_stale_edits(tmp_path: Path) -> None:
    transcript = _write(
        tmp_path / "t.jsonl",
        [
            _tool_use("w", "Write", file_path="/work/a.go", content="x := 1\n"),
            _result("w", "ok"),
            _tool_use("e", "Edit", file_path="/work/a.go", old_string="1", new_string="2"),
            _result("e", "ok"),
            _tool_use("m", "Edit", file_path="/work/a.go", old_string="absent", new_string="3"),
            _result("m", "ok"),
            _tool_use("o", "Write", file_path="/elsewhere/b", content="no"),
            _result("o", "ok"),
        ],
    )
    kit = tmp_path / "kit" / "bin"
    kit.mkdir(parents=True)
    (kit / "sdo-operator").write_text("")
    session = Session("s", transcript, tmp_path, "HEAD", tmp_path / "kit")
    work = tmp_path / "work"
    work.mkdir()
    applied = [apply_file_call(session, work, call) for call in read_tool_calls(transcript)]
    assert applied == [True, True, False, True]
    assert (work / "a.go").read_text() == "x := 2\n"
    assert not (work / "b").exists()


def test_start_commit_is_the_newest_commit_before_the_session(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    subprocess.run(["git", "init", "-q", str(repo)], check=True)
    for message, stamp in (
        ("source", "2026-01-01T00:00:00Z"),
        ("bootstrap", "2026-01-01T01:00:00Z"),
        ("agent", "2026-01-01T03:00:00Z"),
    ):
        env = {
            "GIT_COMMITTER_DATE": stamp,
            "GIT_AUTHOR_DATE": stamp,
            "GIT_AUTHOR_NAME": "t",
            "GIT_AUTHOR_EMAIL": "t@invalid",
            "GIT_COMMITTER_NAME": "t",
            "GIT_COMMITTER_EMAIL": "t@invalid",
            "PATH": "/usr/bin:/bin",
        }
        subprocess.run(["git", "-C", str(repo), "commit", "-q", "--allow-empty", "-m", message], env=env, check=True)
    base = start_commit(repo, datetime(2026, 1, 1, 2, tzinfo=UTC))
    assert _git(repo, "log", "-1", "--format=%s", base) == "bootstrap"


def test_first_read_counts_the_skill_and_each_linked_reference_once(tmp_path: Path) -> None:
    skill = tmp_path / "skills" / "onboarding" / "SKILL.md"
    (skill.parent / "references").mkdir(parents=True)
    (skill.parent / "references" / "example.md").write_text("12345")
    skill.write_text("[a](references/example.md) [b](references/example.md#x) [c](missing.md)\n")
    read = first_read(skill)
    assert read.skill == skill.stat().st_size
    assert read.linked == {"onboarding/references/example.md": 5}


def _bwrap_works() -> bool:
    bwrap = shutil.which("bwrap")
    if bwrap is None:
        return False
    probe = subprocess.run([bwrap, "--ro-bind", "/", "/", "--unshare-net", "true"], capture_output=True, check=False)
    return probe.returncode == 0


@pytest.mark.skipif(not _bwrap_works(), reason="needs bubblewrap with user namespaces")
def test_replay_snapshots_what_each_validator_call_saw_and_measures_fidelity(tmp_path: Path) -> None:
    repo = tmp_path / "recorded"
    subprocess.run(["git", "init", "-q", str(repo)], check=True)
    _git(repo, "config", "user.name", "t")
    _git(repo, "config", "user.email", "t@invalid")
    (repo / "probe.go").write_text("v1\n")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-qm", "start")
    base = _git(repo, "rev-parse", "HEAD")
    # What the agent did: edit by shell, validate, write, commit; the recorded head is that commit.
    (repo / "probe.go").write_text("v3\n")
    _git(repo, "commit", "-qam", "agent")
    kit = tmp_path / "kit" / "bin"
    kit.mkdir(parents=True)
    (kit / "sdo-operator").write_text("#!/bin/sh\necho validated\n")
    (kit / "sdo-operator").chmod(0o755)
    transcript = _write(
        tmp_path / "t.jsonl",
        [
            _tool_use("1", "Bash", command="cd /work && sed -i s/v1/v2/ probe.go && sleep 600"),
            _result("1", ""),
            _tool_use("2", "Bash", command="sdo-operator memory check --root /work"),
            _result("2", "validated"),
            _tool_use("3", "Write", file_path="/work/probe.go", content="v3\n"),
            _result("3", "ok"),
            _tool_use("4", "Bash", command="cd /work && git commit -qam agent && curl -s http://example.invalid"),
            _result("4", ""),
        ],
    )
    replayed = replay(Session("s", transcript, repo, base, tmp_path / "kit"), tmp_path / "replay")
    work = Path(replayed.repo)
    validator, final = replayed.snapshots
    assert validator.call == 1
    assert validator.args == "memory check --root /work"
    assert _git(work, "show", f"{validator.commit}:probe.go") == "v2"
    assert final.args == "final"
    assert _git(work, "show", f"{final.commit}:probe.go") == "v3"
    assert (replayed.agent_commits, replayed.matched_commits) == (1, 1)
