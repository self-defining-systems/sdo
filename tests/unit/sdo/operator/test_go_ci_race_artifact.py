"""Race artifacts contain the live graph, never stale compiler payloads."""

import os
import runpy
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[4]


@pytest.mark.go_integration
def test_race_graph_runs_from_artifact_without_recompiling(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    module = tmp_path / "module"
    module.mkdir()
    (module / "go.mod").write_text("module fixture.test/artifact\n\ngo 1.26.0\n")
    (module / "artifact_test.go").write_text('package artifact\nimport "testing"\nfunc TestArtifact(t *testing.T) {}\n')
    cache = tmp_path / "producer"
    env = {**os.environ, "GOCACHE": str(cache), "GOFLAGS": "-p=2 -trimpath"}
    # Compile in a private producer, then copy only through the real artifact.
    # Unrelated large payloads model earlier source revisions.
    subprocess.run(
        ["go", "list", "-race", "-test", "-deps", "-export", "./..."],
        cwd=module,
        env=env,
        check=True,
        stdout=subprocess.DEVNULL,
    )
    stale = cache / "ab" / ("ab" + "0" * 62 + "-d")
    stale.parent.mkdir(exist_ok=True)
    stale.write_bytes(b"!<arch>\n" + b"unrelated archive" * 1000)
    executable = stale.with_name("ab" + "1" * 62 + "-d")
    executable.write_bytes(b"\x7fELF" + b"unrelated tool" * 1000)
    archive = tmp_path / "race.tar.zst"
    subprocess.run(
        ["python3", str(ROOT / "scripts/go_ci_race_artifact.py"), "--module", str(module), "--output", str(archive)],
        env=env,
        check=True,
    )
    consumer = tmp_path / "consumer"
    consumer.mkdir()
    subprocess.run(["tar", "-I", "zstd", "-xf", str(archive), "-C", str(consumer)], check=True)
    assert not (consumer / stale.relative_to(cache)).exists()
    assert not (consumer / executable.relative_to(cache)).exists()
    guard = tmp_path / "guard"
    guard.write_text(
        "#!/bin/sh\n"
        'if [ "$2" = "-V=full" ]; then exec "$@"; fi\n'
        'case "$*" in *_testmain.go*) exec "$@";; esac\n'
        'case "$1" in */compile|*/asm|*/cgo) echo "unexpected compile: $*" >&2; exit 99;; esac\n'
        'exec "$@"\n'
    )
    guard.chmod(0o755)
    result = subprocess.run(
        ["go", "test", "-race", "-toolexec", str(guard), "./..."],
        cwd=module,
        env={**env, "GOCACHE": str(consumer)},
        text=True,
        capture_output=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr

    # An oversized graph fails at its producer and cannot be published.
    producer = runpy.run_path(str(ROOT / "scripts/go_ci_race_artifact.py"))["pack"]
    monkeypatch.setenv("GOCACHE", str(cache))
    monkeypatch.setenv("GOFLAGS", env["GOFLAGS"])
    monkeypatch.setitem(producer.__globals__, "MAX_ARCHIVE_BYTES", 1)
    with pytest.raises(ValueError, match="transfer budget"):
        producer([module], cache, archive)
    assert not archive.exists()


def test_each_bucket_fetches_only_its_selected_package_graph(tmp_path: Path) -> None:
    graphs = runpy.run_path(str(ROOT / "scripts/go_ci_race_artifact.py"))["package_graphs"]
    plan = {
        "bins": [["one", "python"], ["two"], ["python"]],
        "items": {
            "one": {"kind": "go", "scope": "operator", "packages": ["fixture.test/one"]},
            "two": {"kind": "go", "scope": "runtime", "packages": ["fixture.test/two"]},
            "python": {"kind": "python"},
        },
    }
    assert graphs(plan, tmp_path) == [
        {tmp_path / "sdo/operator": ["fixture.test/one"]},
        {tmp_path / "sdo/operator/runtime": ["fixture.test/two"]},
        {tmp_path / "sdo/operator": ["testing"]},
    ]
