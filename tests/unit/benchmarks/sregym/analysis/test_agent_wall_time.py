"""Wall-time attribution over synthetic stream-json and session-JSONL trajectories."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest

from benchmarks.sregym.analysis.agent_wall_time import MAIN, SUBAGENT_WAIT, Rule, analyze, load_rules

if TYPE_CHECKING:
    from pathlib import Path

T0 = 1_800_000_000.0


def stamp(offset: float) -> str:
    from datetime import UTC, datetime

    return datetime.fromtimestamp(T0 + offset, UTC).isoformat().replace("+00:00", "Z")


def use(offset: float, call_id: str, name: str, command: str, **extra: object) -> dict:
    block = {"type": "tool_use", "id": call_id, "name": name, "input": {"command": command}}
    return {"type": "assistant", "timestamp": stamp(offset), "message": {"content": [block]}, **extra}


def result(offset: float, call_id: str, **extra: object) -> dict:
    block = {"type": "tool_result", "tool_use_id": call_id, "content": "ok"}
    return {"type": "user", "timestamp": stamp(offset), "message": {"content": [block]}, **extra}


def text(offset: float, **extra: object) -> dict:
    return {
        "type": "assistant",
        "timestamp": stamp(offset),
        "message": {"content": [{"type": "text", "text": "x"}]},
        **extra,
    }


def write(path: Path, records: list[dict]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(r) + "\n" for r in records))
    return path


def sub_events(agent: str | None = None, **extra: object) -> list[dict]:
    tag = {"agent_id": agent} if agent else {}
    return [
        use(
            10,
            "a1",
            "Bash",
            "nohup sdo-operator onboarding check --quick --report r.txt > run.out 2>&1 &",
            **tag,
            **extra,
        ),
        result(10.5, "a1", **tag, **extra),
        use(11, "a2", "Bash", "until grep -q overall run.out; do sleep 5; done", **tag, **extra),
        result(71, "a2", **tag, **extra),
        use(75, "a3", "Bash", "kubectl -n app get pods", **tag, **extra),
        result(76, "a3", **tag, **extra),
        text(80, **tag, **extra),
    ]


def main_events() -> list[dict]:
    return [use(0, "m1", "Agent", "kubectl mentioned in prompt"), result(1, "m1"), text(2), text(100)]


def check_report(report: dict) -> None:
    sub = report["subagents"][0]
    assert sub.buckets["check --quick"] == pytest.approx(60.5)  # the polling wait counts toward the check
    assert sub.launches["check --quick"] == 1
    assert sub.buckets["kubectl reads/curl"] == pytest.approx(1)
    assert sub.wall == pytest.approx(70)
    main = report["main"]
    assert main.buckets["subagent launch"] == pytest.approx(1)
    assert main.buckets[SUBAGENT_WAIT] == pytest.approx(70)  # overlap with the running subagent only
    assert sum(main.buckets.values()) == pytest.approx(main.wall)
    assert sum(sub.buckets.values()) == pytest.approx(sub.wall)


def test_stream_json_subagent_events_are_split_by_agent_id(tmp_path: Path) -> None:
    events = [*main_events()[:3], *sub_events("sub1"), main_events()[3]]
    check_report(analyze([write(tmp_path / "t.jsonl", events)]))


def test_session_jsonl_with_subagent_files_matches_stream_json(tmp_path: Path) -> None:
    write(tmp_path / "session.jsonl", main_events())
    write(tmp_path / "session" / "subagents" / "agent-sub1.jsonl", sub_events(isSidechain=True))
    report = analyze([tmp_path])
    assert report["main"].key == MAIN
    check_report(report)


def test_custom_categories_replace_defaults(tmp_path: Path) -> None:
    rules = tmp_path / "rules.json"
    rules.write_text(json.dumps([{"name": "cluster", "pattern": "kubectl"}]))
    report = analyze([write(tmp_path / "t.jsonl", sub_events())], load_rules(rules))
    assert report["main"].buckets["cluster"] == pytest.approx(1)


def test_rule_requires_a_name() -> None:
    import re

    with pytest.raises(ValueError, match="name"):
        Rule("", re.compile("x"))
