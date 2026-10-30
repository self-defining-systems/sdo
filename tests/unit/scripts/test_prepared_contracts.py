"""Real race executable publication into a cold, compiler-free consumer."""

import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))
SPEC = importlib.util.spec_from_file_location("prepared_contracts", ROOT / "scripts/prepared_contracts.py")
assert SPEC is not None
assert SPEC.loader is not None
prepared = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(prepared)


@pytest.mark.go_integration
def test_real_prepared_race_binary_runs_selected_tests_without_compiling(tmp_path, monkeypatch, capsys):
    root = tmp_path / "repo"
    module = root / "sdo/operator"
    module.mkdir(parents=True)
    (module / "go.mod").write_text("module synthetic.example/contracts\n\ngo 1.24\n")
    nested = module / "nested"
    nested.mkdir()
    (nested / "fixture.txt").write_text("relative fixture")
    (nested / "selected_test.go").write_text("""//go:build race
package contracts
import ("testing"; "os")
func TestSelected(t *testing.T) {
 if _, err := os.ReadFile("fixture.txt"); err != nil { t.Fatal(err) }
 wd, _ := os.Getwd(); if os.Getenv("PWD") != wd { t.Fatalf("PWD does not match package cwd") }
 t.Run("child", func(t *testing.T) {})
}
func TestUnselected(t *testing.T) { t.Fatal("must not run") }
func TestZeroExit(t *testing.T) { os.Exit(0) }
func FuzzSeed(f *testing.F) { f.Add("seed"); f.Fuzz(func(t *testing.T, s string) {}) }
""")
    subprocess.run(["git", "init", "-q", str(root)], check=True)
    subprocess.run(
        [
            "git",
            "-C",
            str(root),
            "-c",
            "user.name=Synthetic",
            "-c",
            "user.email=synthetic@example.test",
            "commit",
            "--allow-empty",
            "-qm",
            "fixture",
        ],
        check=True,
    )
    # Distinct packages share one graph load; equal output basenames must
    # still retain their own executable and package working directory.
    extra_packages = ["other", "another/nested"]
    for relative in extra_packages:
        directory = module / relative
        directory.mkdir(parents=True)
        (directory / "selected_test.go").write_text(
            'package contracts\nimport "testing"\nfunc TestBatch(t *testing.T) {}\n'
        )
    plan = root / "plan.json"
    plan.write_text(
        json.dumps(
            {
                "bins": [["selected", "seed"], ["selected"], ["failure"], ["exit"]],
                "items": {
                    key: {
                        "kind": "go",
                        "scope": "operator",
                        "packages": ["synthetic.example/contracts/nested"],
                        "name": name,
                    }
                    for key, name in [
                        ("selected", "TestSelected"),
                        ("seed", "FuzzSeed"),
                        ("failure", "TestUnselected"),
                        ("exit", "TestZeroExit"),
                    ]
                },
            }
        )
    )
    data = json.loads(plan.read_text())
    data["items"]["batch"] = {
        "kind": "go",
        "scope": "operator",
        "name": "TestBatch",
        "packages": ["synthetic.example/contracts/" + package for package in extra_packages],
    }
    data["bins"][0].append("batch")
    plan.write_text(json.dumps(data))
    commands = []
    original_run = subprocess.run

    def record_run(command, **kwargs):
        commands.append(command)
        return original_run(command, **kwargs)

    with monkeypatch.context() as recording:
        recording.setattr(subprocess, "run", record_run)
        output = root / "prepared"
        prepared.prepare(plan, output, root=root)
    # One metadata query per module and only the necessary collision batches,
    # rather than one graph load per selected package.
    assert len([command for command in commands if command[:2] == ["go", "list"]]) == 1
    assert len([command for command in commands if command[:2] == ["go", "test"]]) == 2

    # The consumer uses the published test2json tool and cannot invoke Go.
    bindir = tmp_path / "bin"
    bindir.mkdir()
    guard = bindir / "go"
    guard.write_text("#!/bin/sh\necho unexpected-Go-command >&2\nexit 99\n")
    guard.chmod(0o755)
    monkeypatch.setenv("PATH", f"{bindir}:{os.environ['PATH']}")
    monkeypatch.setenv("GOCACHE", str(tmp_path / "empty-cache"))
    prepared.execute(plan, 0, output / "0", root=root)
    text = capsys.readouterr().out
    assert "TestSelected/child" in text
    assert "FuzzSeed/seed#0" in text
    assert "TestUnselected" not in text
    assert not (tmp_path / "empty-cache").exists()
    with pytest.raises(subprocess.CalledProcessError):
        prepared.execute(plan, 2, output / "2", root=root)
    with pytest.raises(subprocess.CalledProcessError):
        prepared.execute(plan, 3, output / "3", root=root)
    with pytest.raises(ValueError, match="source, plan or bin mismatch"):
        prepared.execute(plan, 1, output / "0", root=root)
    manifest = output / "0/manifest.json"
    data = json.loads(manifest.read_text())
    binary = output / "0" / data["entries"][0]["binary"]
    binary.write_bytes(b"corrupt")
    with pytest.raises(ValueError, match="binary identity mismatch"):
        prepared.execute(plan, 0, output / "0", root=root)
