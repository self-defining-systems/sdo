"""Contract of the flaky-test quarantine: data validation, deadline check and the readers CI uses."""

from __future__ import annotations

import json
import re
import subprocess
import sys
from datetime import date
from pathlib import Path

import pytest

from scripts import ci_quarantine as quarantine

ROOT = Path(__file__).resolve().parents[3]
GO = {
    "id": "sdo.dev/x/pkg TestRacy",
    "kind": "go",
    "owner": "vic-lsh",
    "reason": "synthetic race",
    "deadline": "2026-12-01",
}
PY = {
    "id": "tests/unit/test_synthetic.py::test_flaky[a]",
    "kind": "pytest",
    "owner": "vic-lsh",
    "reason": "synthetic clock",
    "deadline": "2026-12-01",
}


def _data(tmp_path: Path, *entries: dict) -> Path:
    path = tmp_path / "quarantine.json"
    path.write_text(json.dumps({"tests": list(entries)}))
    (tmp_path / "tests/unit").mkdir(parents=True, exist_ok=True)
    (tmp_path / "tests/unit/test_synthetic.py").write_text("def test_flaky():\n    pass\n")
    (tmp_path / "pkg").mkdir(exist_ok=True)
    (tmp_path / "pkg/racy_test.go").write_text("package pkg\n\nfunc TestRacy(t *testing.T) {}\n")
    return path


def test_the_shipped_quarantine_is_valid_and_within_its_deadlines() -> None:
    entries = quarantine.load()
    assert quarantine.problems(entries, date.today()) == []


def test_a_passed_deadline_fails_the_check(tmp_path: Path) -> None:
    entries = quarantine.load(_data(tmp_path, GO, PY))
    assert quarantine.problems(entries, date(2026, 12, 1), tmp_path) == []
    late = quarantine.problems(entries, date(2026, 12, 2), tmp_path)
    assert [line.split(": ")[0] for line in late] == [GO["id"], PY["id"]]
    assert all("deadline" in line for line in late)


def test_a_quarantined_test_that_no_longer_exists_fails_the_check(tmp_path: Path) -> None:
    gone = {**GO, "id": "sdo.dev/x/pkg TestGone"}
    entries = quarantine.load(_data(tmp_path, gone))
    assert "no longer exists" in quarantine.problems(entries, date(2026, 11, 1), tmp_path)[0]


@pytest.mark.parametrize(
    "bad",
    [
        {"owner": " "},
        {"reason": ""},
        {"kind": "rust"},
        {"id": "TestNoPackage"},
        {"deadline": "soon"},
    ],
)
def test_incomplete_entries_are_rejected(tmp_path: Path, bad: dict) -> None:
    with pytest.raises(ValueError, match="must|kind|go id|Invalid isoformat"):
        quarantine.load(_data(tmp_path, {**GO, **bad}))


def test_go_skip_is_one_anchored_regex_and_empty_without_go_entries(tmp_path: Path) -> None:
    assert quarantine.go_skip(quarantine.load(_data(tmp_path, GO, PY))) == "^(TestRacy)$"
    assert quarantine.go_skip(quarantine.load(_data(tmp_path, PY))) == ""
    assert re.fullmatch(quarantine.go_skip(quarantine.load(_data(tmp_path, GO))), "TestRacy")


def test_the_command_line_emits_what_the_ci_scripts_consume(tmp_path: Path) -> None:
    data = _data(tmp_path, GO, PY)

    def run(command: str) -> str:
        done = subprocess.run(
            [sys.executable, str(ROOT / "scripts/ci_quarantine.py"), command, "--data", str(data)],
            check=True,
            capture_output=True,
            text=True,
        )
        return done.stdout

    assert run("go-skip").strip() == "^(TestRacy)$"
    assert run("pytest-deselect").split("\n")[:2] == ["--deselect", PY["id"]]
    assert run("pytest-ids").strip() == PY["id"]


def test_only_the_pull_request_tier_skips_quarantined_tests() -> None:
    go = (ROOT / "scripts/test_go_ci.sh").read_text()
    nightly, _, pr = go.partition("else")
    assert "ci_quarantine" not in nightly
    assert "ci_quarantine.py" in pr
    assert "ci_quarantine.py pytest-deselect" in (ROOT / "scripts/test_python.sh").read_text()
    assert "ci_quarantine.py pytest-ids" in (ROOT / ".github/workflows/nightly.yml").read_text()
