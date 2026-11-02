from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING

from hypothesis import given, settings
from hypothesis import strategies as st

from sdo.agents.responder import tool_output
from tests.unit.sdo.agents.responder.tool_output_cases import large_response, rendered_stdout_cases

if TYPE_CHECKING:
    from pathlib import Path


def test_every_command_has_a_bounded_default_with_complete_private_details() -> None:
    for name, output in rendered_stdout_cases().items():
        assert len(output) <= tool_output.MAX_OUTPUT_CHARS + 1, f"{name}: {len(output)} characters"


@settings(max_examples=30, deadline=None)
@given(count=st.integers(min_value=0, max_value=150), item_size=st.integers(min_value=0, max_value=4000))
def test_findings_are_bounded_per_item_and_never_grow_faster_than_linearly(count: int, item_size: int) -> None:
    response = {
        "command": "detect",
        "healthy": False,
        "findings": [
            {"fingerprint": f"finding-{i}", "summary": "界" * item_size, "evidence": "full fact" * item_size}
            for i in range(count)
        ],
    }
    summary = tool_output.summarize_response(response, "/tmp/sdo/toy/0001-detect.json")
    output = json.dumps(summary, ensure_ascii=False)
    assert len(output) <= tool_output.MAX_OUTPUT_CHARS, f"detect: {len(output)} characters"
    assert len(output) <= 700 + min(count, 4) * 400
    assert summary["finding_count"] == count
    assert len(summary["still_broken"]) + summary.get("still_broken_omitted", 0) == count
    assert all(len(item.get("summary", "")) <= 160 for item in summary["still_broken"])


def test_sequence_is_unique_across_concurrent_command_processes(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(tool_output, "OUTPUT_ROOT", tmp_path)
    with ThreadPoolExecutor(max_workers=8) as pool:
        paths = list(
            pool.map(lambda i: tool_output.write_json({"index": i}, command="detect", incident_id="toy"), range(24))
        )
    assert len(set(paths)) == 24
    assert sorted(int(path.name.split("-", 1)[0]) for path in paths) == list(range(1, 25))
    assert all(path.parent == tmp_path / "toy" for path in paths)
    assert all(path.stat().st_mode & 0o777 == 0o600 for path in paths)
    assert {json.loads(path.read_text())["index"] for path in paths} == set(range(24))


def test_new_sdk_actions_remain_in_complete_menu_without_repeated_schema_stdout() -> None:
    response = large_response("menu", 3)
    summary = tool_output.summarize_response(response, "/tmp/sdo/toy/0001-menu.json")
    assert summary["option_count"] == len(response["menu"]["options"])
    assert len(summary["options"]) + summary["options_omitted"] == summary["option_count"]
    assert "Full schema description" not in json.dumps(summary)
    assert "RunJob" in {item["option_id"] for item in response["menu"]["options"]}
