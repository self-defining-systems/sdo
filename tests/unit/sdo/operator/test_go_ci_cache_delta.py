"""A PR hands its consumers only the entries it added to the base branch's build cache."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[4]
_spec = importlib.util.spec_from_file_location("go_ci_cache_delta", ROOT / "scripts/go_ci_cache_delta.py")
assert _spec is not None
assert _spec.loader is not None
delta = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = delta
_spec.loader.exec_module(delta)


def write(root: Path, relative: str, content: str, mode: int = 0o644) -> Path:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)
    path.chmod(mode)
    return path


def roots(base: Path) -> list:
    return [
        delta.Root.parse(f"gocache={base / 'gocache'}"),
        delta.Root.parse(f"test-bin={base / 'test-bin'}@content"),
    ]


def test_export_holds_only_new_entries_and_apply_reproduces_the_pr_build(tmp_path: Path) -> None:
    producer = tmp_path / "producer"
    write(producer, "gocache/aa/base-a", "base archive", 0o444)
    write(producer, "test-bin/setup", "old setup")
    delta.snapshot(tmp_path / "base.manifest", roots(producer))
    # The PR compiles one new archive, relinks a binary and touches an existing entry's mtime.
    write(producer, "gocache/bb/new-b", "new archive", 0o444)
    write(producer, "test-bin/setup", "new setup")
    (producer / "gocache/aa/base-a").touch()
    saved, files, size = delta.export(tmp_path / "base.manifest", tmp_path / "delta", roots(producer), 10**6)
    assert saved
    assert (files, size) == (2, len("new archive") + len("new setup"))
    assert not (tmp_path / "delta/gocache/aa/base-a").exists()

    consumer = tmp_path / "consumer"
    write(consumer, "gocache/aa/base-a", "base archive", 0o444)
    write(consumer, "test-bin/setup", "old setup")
    assert delta.apply(tmp_path / "delta", roots(consumer)) == 2
    assert (consumer / "gocache/bb/new-b").read_text() == "new archive"
    assert (consumer / "test-bin/setup").read_text() == "new setup"
    assert (consumer / "gocache/aa/base-a").read_text() == "base archive"


def test_a_delta_over_the_cap_is_not_saved(tmp_path: Path) -> None:
    producer = tmp_path / "producer"
    write(producer, "gocache/aa/base-a", "base")
    delta.snapshot(tmp_path / "base.manifest", roots(producer))
    write(producer, "gocache/bb/large", "x" * 100)
    saved, files, size = delta.export(tmp_path / "base.manifest", tmp_path / "delta", roots(producer), 99)
    assert (saved, files, size) == (False, 1, 100)
    assert not (tmp_path / "delta").exists()


def test_the_cli_reports_whether_the_delta_was_saved(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    producer = tmp_path / "producer"
    write(producer, "gocache/aa/base-a", "base")
    spec = f"gocache={producer / 'gocache'}"
    manifest, out, output = tmp_path / "m", tmp_path / "d", tmp_path / "out"
    monkeypatch.setenv("GITHUB_OUTPUT", str(output))
    assert delta.main(["snapshot", "--manifest", str(manifest), spec]) == 0
    write(producer, "gocache/bb/new", "new")
    assert delta.main(["export", "--manifest", str(manifest), "--out", str(out), "--max-bytes", "10", spec]) == 0
    assert output.read_text() == "saved=true\n"


def test_malformed_roots_and_missing_snapshots_fail_with_context(tmp_path: Path) -> None:
    with pytest.raises(delta.CacheDeltaError, match="name=path"):
        delta.Root.parse("no-separator")
    with pytest.raises(delta.CacheDeltaError, match="no snapshot"):
        delta.export(tmp_path / "missing", tmp_path / "out", [], 1)
