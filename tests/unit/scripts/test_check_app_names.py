"""Contract of the application-name guard, using throwaway git repos as negative controls."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

from scripts import check_app_names as guard

RULES = {"extra_names": ["hotel reservation", "gitlab"], "exempt_apps": {"toy": "fixture"}}


def _repo(tmp_path: Path, files: dict[str, str], apps: tuple[str, ...] = ("sockshop", "toy")) -> Path:
    subprocess.run(["git", "init", "-q", str(tmp_path)], check=True)
    for app in apps:
        (tmp_path / "apps" / app).mkdir(parents=True)
    for rel, text in files.items():
        (tmp_path / rel).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / rel).write_text(text)
    subprocess.run(["git", "-C", str(tmp_path), "add", "-A"], check=True)
    return tmp_path


def _run(root: Path, allow: dict | None = None) -> list[str]:
    (root / "rules.json").write_text(json.dumps(RULES))
    (root / "allow.json").write_text(json.dumps(allow or {}))
    return guard.check(root, ("sdo",), root / "rules.json", root / "allow.json")[0]


@pytest.mark.parametrize(
    "text",
    ["deploy SockShop now", "ns = 'hotel-reservation'", "HotelReservation()", "see GitLab", "hotel_reservation"],
)
def test_names_are_caught_in_every_spelling(tmp_path: Path, text: str) -> None:
    root = _repo(tmp_path, {"sdo/a.go": text})
    assert any("sdo/a.go" in e for e in _run(root))


@pytest.mark.parametrize("text", ["sockshops", "mygitlabber", "hotels reservations", "toy story", "a toy"])
def test_word_boundaries_and_exempt_apps_pass(tmp_path: Path, text: str) -> None:
    assert _run(_repo(tmp_path, {"sdo/a.go": text})) == []


def test_files_outside_scan_roots_are_ignored(tmp_path: Path) -> None:
    assert _run(_repo(tmp_path, {"benchmarks/a.py": "sockshop", "sdo/skills/s/SKILL.md": "clean"})) == []


def test_skills_and_prompts_are_scanned(tmp_path: Path) -> None:
    root = _repo(tmp_path, {"sdo/skills/s/SKILL.md": "fix gitlab", "sdo/agents/x/prompts/p.md": "sockshop"})
    assert len(_run(root)) == 2


def test_ratchet_allows_equal_rejects_growth_and_stale_counts(tmp_path: Path) -> None:
    root = _repo(tmp_path, {"sdo/a.go": "sockshop gitlab"})
    assert _run(root, {"sdo/a.go": {"count": 2, "reason": "r"}}) == []
    assert any("exceed" in e for e in _run(root, {"sdo/a.go": {"count": 1, "reason": "r"}}))
    assert any("lower the ratchet" in e for e in _run(root, {"sdo/a.go": {"count": 3, "reason": "r"}}))
    assert any("needs a reason" in e for e in _run(root, {"sdo/a.go": {"count": 2, "reason": ""}}))


def test_denylist_follows_apps_directory() -> None:
    names = guard.denylist(Path(guard.ROOT), json.loads(guard.RULES.read_text()))
    assert {"sockshop", "teastore", "hotel reservation", "gitlab"} <= set(names)
    assert "toy" not in names


def test_repository_is_clean_against_ratchet() -> None:
    errors, _ = guard.check()
    assert errors == []
