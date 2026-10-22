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
