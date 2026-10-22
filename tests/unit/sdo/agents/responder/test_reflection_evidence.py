"""Reflection browses immutable captures without printing whole Kubernetes lists."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

from sdo.agents.responder.reflection_evidence import capture_inventory, read_capture


def test_inventory_is_bounded_and_identifies_original_bytes(tmp_path: Path) -> None:
    capture = {
        "namespace": "app",
        "pods": [
            {"metadata": {"name": f"pod-{i}"}, "spec": {"containers": [{"image": "huge" * 1000}]}} for i in range(100)
        ],
    }
    raw = json.dumps(capture).encode()
    (tmp_path / "open@detect.json").write_bytes(raw)
    (tmp_path / "fixed.json").write_text('{"events":null}')
    view = capture_inventory(tmp_path)
    assert len(json.dumps(view)) < 6000
    assert view["captures"][0]["bytes"] > 0
    assert view["captures"][1]["resources"]["pods"]["count"] == 100
    assert view["captures"][1]["resources"]["pods"]["omitted"] > 0
    assert "huge" not in json.dumps(view)
    assert (tmp_path / "open@detect.json").read_bytes() == raw


def test_capture_fields_are_retrieved_on_demand_and_paginated(tmp_path: Path) -> None:
    capture = {
        "pods": [
            {
                "metadata": {"name": f"p-{i}"},
                "status": {"phase": "Pending"},
                "spec": {"containers": [{"image": "not-requested"}]},
            }
            for i in range(4)
        ],
        "events": None,
    }
    (tmp_path / "open@detect.json").write_text(json.dumps(capture))
    result = read_capture(tmp_path, "detect", "pods", name="p-1", fields=["metadata.name", "status.phase"])
    assert result["items"] == [{"metadata.name": "p-1", "status.phase": "Pending"}]
    assert "not-requested" not in json.dumps(result)
    result = read_capture(tmp_path, "detect", "pods", limit=2)
    assert len(result["items"]) == 2
    assert result["next_offset"] == 2
    assert read_capture(tmp_path, "detect", "events")["items"] == []


def test_memory_view_omits_scripts_and_bounds_repeated_memory_text() -> None:
    from sdo.agents.responder.reflection_evidence import memory_view
    from sdo.contracts import OperationalMemorySnapshot

    snapshot = OperationalMemorySnapshot(accepted_commit="accepted", architecture_markdown="architecture" * 2000)
    for index in range(100):
        book = snapshot.playbooks.add(id=f"book-{index}", markdown="large playbook" * 1000)
        book.scripts.add(name="inspect.sh", content="script body" * 1000)
    view = memory_view(snapshot)
    assert view["accepted_commit"] == "accepted"
    assert view["omitted_playbooks"] == 70
    assert "script body" not in json.dumps(view)
    assert len(json.dumps(view)) < 30_000


def test_oversized_capture_item_is_explicit_and_narrow_fields_remain_exact(tmp_path: Path) -> None:
    (tmp_path / "open@detect.json").write_text(
        json.dumps(
            {"pods": [{"metadata": {"name": "large"}, "spec": {"unused": "x" * 20000}, "status": {"phase": "Pending"}}]}
        )
    )
    broad = read_capture(tmp_path, "detect", "pods")
    assert broad["oversized_indices"] == [0]
    assert broad["items"] == []
    narrow = read_capture(tmp_path, "detect", "pods", fields=["status.phase"])
    assert narrow["items"] == [{"status.phase": "Pending"}]


def test_recording_index_finds_late_primitive_without_dumping_results(tmp_path: Path) -> None:
    from sdo.agents.responder.reflection_evidence import recording_index

    calls = [{"method": "Logs", "arguments": [], "result": "large-result" * 10000} for _ in range(27)]
    ref = {"kind": "Deployment", "namespace": "app", "name": "frontend"}
    calls.append({"phase": "fixed", "method": "DiffAgainstSource", "arguments": [ref], "error": "source unavailable"})
    raw = json.dumps(calls).encode()
    (tmp_path / "open@detect-recordings.json").write_bytes(raw)
    page = recording_index(tmp_path)
    assert page["total"] == 28
    assert page["next_offset"] is None
    assert page["items"][27] == {
        "index": 27,
        "phase_index": 0,
        "phase": "fixed",
        "method": "DiffAgainstSource",
        "bound_object": ref,
        "error": "source unavailable",
        "error_omitted_characters": 0,
    }
    selected = read_capture(
        tmp_path, "fixed", "recordings", offset=page["items"][27]["phase_index"], limit=1, fields=["method", "error"]
    )
    assert selected["items"] == [{"method": "DiffAgainstSource", "error": "source unavailable"}]
    assert page["items"][0]["phase"] == "open"
    assert "large-result" not in json.dumps(page)
    assert len(json.dumps(page)) < 8000
    assert (tmp_path / "open@detect-recordings.json").read_bytes() == raw
    inventory = capture_inventory(tmp_path)
    assert inventory["captures"][0]["recorded_methods"] == {"DiffAgainstSource": 1, "Logs": 27}


def test_recording_index_pagination_is_explicit_and_filters_whole_file(tmp_path: Path) -> None:
    from sdo.agents.responder.reflection_evidence import recording_index

    calls = [{"method": "Logs", "arguments": [], "error": "x" * 1000} for _ in range(65)]
    calls.append({"method": "DiffAgainstSource", "arguments": []})
    (tmp_path / "open@detect-recordings.json").write_text(json.dumps(calls))
    first = recording_index(tmp_path)
    assert first["next_offset"] == 64
    assert first["total"] == 66
    assert first["items"][0]["error_omitted_characters"] > 0
    filtered = recording_index(tmp_path, method="DiffAgainstSource")
    assert filtered["items"][0]["index"] == 65
    assert filtered["total"] == 1
    assert filtered["next_offset"] is None


def test_available_preapply_capture_and_recordings_remain_distinct(tmp_path: Path) -> None:
    from sdo.agents.responder.reflection_evidence import recording_index

    (tmp_path / "open@pre-apply.json").write_text('{"pods":[{"metadata":{"name":"later-fault"}}]}')
    (tmp_path / "fixed.json").write_text('{"pods":[]}')
    (tmp_path / "open@pre-apply-recordings.json").write_text(
        '[{"method":"Logs","arguments":[],"result":"before"},'
        '{"method":"Logs","phase":"fixed","arguments":[],"result":"after"}]'
    )
    names = {item["file"] for item in capture_inventory(tmp_path)["captures"]}
    assert "open@pre-apply.json" in names
    assert read_capture(tmp_path, "pre-apply", "pods")["items"][0]["metadata"]["name"] == "later-fault"
    assert recording_index(tmp_path, open_phase="pre-apply")["total"] == 2
    assert read_capture(tmp_path, "pre-apply", "recordings")["items"][0]["result"] == "before"
    assert read_capture(tmp_path, "fixed", "recordings", open_phase="pre-apply")["items"][0]["result"] == "after"
