from __future__ import annotations

import os
import re
from typing import TYPE_CHECKING

from sdo.agents.responder import page_only
from sdo.contracts import IncidentRequest

if TYPE_CHECKING:
    from pathlib import Path

    import pytest

BLOCK = [
    "Candidates: `sdo detect`; every change: `sdo changes`; signals over time: `sdo timeline`.",
    "deploy/web  readiness probe failing since 10:01:02",
    "  newest: 10:01:02 patched web; 10:00:40 scaled db  more: sdo changes",
    "+3 more candidates: sdo detect candidates",
    "clues elsewhere: 2  more: sdo detect background",
    "more: sdo map near web",
    "    more: sdo changes --component db; sdo timeline --component db",
]


def test_plain_text_keeps_the_evidence_and_drops_every_pointer_to_sdo() -> None:
    text = page_only.plain_text("\n".join(BLOCK))
    assert "deploy/web  readiness probe failing since 10:01:02" in text
    assert "newest: 10:01:02 patched web; 10:00:40 scaled db" in text
    assert "+3 more candidates" in text
    assert "clues elsewhere: 2" in text
    assert not re.search(r"\bsdo\b", text)


def test_plain_text_drops_the_passing_tests_hint() -> None:
    text = page_only.plain_text(
        "Passing: a, b. Re-check one with `sdo detect TEST`.\n- (+2 more error groups: sdo detect tests)"
    )
    assert text == "Passing: a, b.\n- (+2 more error groups)"


def test_path_without_sdo_links_other_commands_only(tmp_path: Path) -> None:
    source = tmp_path / "bin"
    source.mkdir()
    for name in ("kubectl", "sdo", "sdo-operator", "jq"):
        (source / name).write_text("#!/bin/sh\n")
    target = page_only.path_without_sdo(str(source), tmp_path / "plain")
    assert sorted(os.listdir(target)) == ["jq", "kubectl"]


def test_prompt_is_the_page_without_sdo_references() -> None:
    request = IncidentRequest(namespace="shop", recent_changes=BLOCK)
    text = page_only.prompt(request, "Answer in the `incident_result` field and leave `reflection` null.")
    assert "namespace shop" in text
    assert "readiness probe failing" in text
    assert "skills" not in text
    assert not re.search(r"\bsdo\b", text)


def test_enabled_reads_the_worker_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(page_only.ENVIRONMENT, raising=False)
    assert not page_only.enabled()
    monkeypatch.setenv(page_only.ENVIRONMENT, "1")
    assert page_only.enabled()
