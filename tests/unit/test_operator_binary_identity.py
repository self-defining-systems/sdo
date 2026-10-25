"""A cached CLI must identify the exact Go inputs it was built from."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

from scripts.operator_test_binary import build_operator, source_identity, verify_operator

pytestmark = pytest.mark.go_integration


@pytest.fixture
def module(tmp_path: Path) -> Path:
    root = tmp_path / "module"
    main = root / "cmd/sdo-operator/main.go"
    main.parent.mkdir(parents=True)
    (root / "go.mod").write_text("module fixture.test/operator\n\ngo 1.24.0\n")
    main.write_text(
        'package main\nimport ("embed"; "fmt")\n'
        "var sourceHash string\n//go:embed message.txt\nvar files embed.FS\n"
        "func main() { fmt.Println(sourceHash) }\n"
    )
    (main.parent / "message.txt").write_text("main source")
    return root


def test_identity_tracks_embeds_and_local_module_replacements(module: Path) -> None:
    first = source_identity(module)
    (module / "cmd/sdo-operator/message.txt").write_text("dev source")
    assert source_identity(module) != first
    dependency = module.parent / "dependency"
    dependency.mkdir()
    (dependency / "go.mod").write_text("module fixture.test/dependency\n\ngo 1.24.0\n")
    source = dependency / "value.go"
    source.write_text('package dependency\nconst Value = "main"\n')
    with (module / "go.mod").open("a") as stream:
        stream.write("\nrequire fixture.test/dependency v0.0.0\nreplace fixture.test/dependency => ../dependency\n")
    main = module / "cmd/sdo-operator/main.go"
    main.write_text(main.read_text().replace('"embed";', '"embed"; _ "fixture.test/dependency";'))
    second = source_identity(module)
    source.write_text('package dependency\nconst Value = "dev"\n')
    assert source_identity(module) != second


def test_binary_guard_rejects_cross_source_cache_and_unstamped_binary(module: Path, tmp_path: Path) -> None:
    directory = tmp_path / "cache"
    binary = build_operator(module, directory)
    expected = source_identity(module)
    verify_operator(binary, expected)
    assert build_operator(module, directory) == binary
    stale = tmp_path / "stale"
    shutil.copy2(binary, stale)
    (module / "cmd/sdo-operator/message.txt").write_text("changed source")
    changed = source_identity(module)
    with pytest.raises(RuntimeError, match="source identity mismatch"):
        verify_operator(stale, changed)
    poisoned = directory / f"sdo-operator-{changed}"
    shutil.copy2(stale, poisoned)
    with pytest.raises(RuntimeError, match="source identity mismatch"):
        build_operator(module, directory)
    subprocess.run(["go", "build", "-o", str(stale), "./cmd/sdo-operator"], cwd=module, check=True)
    with pytest.raises(RuntimeError, match="source identity mismatch"):
        verify_operator(stale, changed)


def test_ci_pins_checkout_to_event_commit() -> None:
    import yaml

    repository = Path(__file__).resolve().parents[2]
    workflow = yaml.safe_load((repository / ".github/workflows/ci.yml").read_text())
    checkouts = [
        step
        for job in workflow["jobs"].values()
        for step in job.get("steps", [])
        if step.get("uses", "").startswith("actions/checkout@")
    ]
    assert checkouts
    assert all(step.get("with", {}).get("ref") == "${{ github.sha }}" for step in checkouts)


def test_build_settings_are_part_of_cache_identity(module: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    first = source_identity(module)
    monkeypatch.setenv("GOFLAGS", "-tags=identity_probe")
    assert source_identity(module) != first


def test_real_operator_embeds_its_input_identity(operator_binary: Path) -> None:
    repository = Path(__file__).resolve().parents[2]
    verify_operator(operator_binary, source_identity(repository / "sdo/operator"))
