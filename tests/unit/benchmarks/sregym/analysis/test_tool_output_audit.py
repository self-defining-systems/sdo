from __future__ import annotations

import pytest

from benchmarks.sregym.analysis.tool_output_audit import audit_outputs


@pytest.mark.parametrize("custom", [False, True])
def test_audit_lists_every_large_output_and_never_its_contents(custom: bool) -> None:
    call = "custom_tool_call" if custom else "function_call"
    output = "custom_tool_call_output" if custom else "function_call_output"
    records = [
        {
            "type": "response_item",
            "payload": {
                "type": call,
                "call_id": "one",
                "name": "exec",
                "input": "sdo menu && sdo source Service/app/api",
            },
        },
        {
            "type": "response_item",
            "ordinal": 2,
            "payload": {"type": output, "call_id": "one", "output": [{"text": "界" * 11}]},
        },
        {"type": "response_item", "payload": {"type": output, "call_id": "missing", "output": "x" * 12}},
        {"type": "response_item", "payload": {"type": output, "call_id": "one", "output": "x" * 10}},
    ]
    results = list(audit_outputs(records, threshold=10))
    assert len(results) == 2
    assert results[0].ordinal == 2
    assert results[0].commands == ("menu", "source")
    assert results[0].chars == 11
    assert results[0].utf8_bytes == 33
    assert results[1].tool == "unknown"
    assert "界" not in repr(results)


def test_audit_refuses_negative_threshold() -> None:
    with pytest.raises(ValueError, match="nonnegative"):
        list(audit_outputs([], threshold=-1))
