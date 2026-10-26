"""Python test worker defaults respect shared hosts and explicit overrides."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[3]


@pytest.mark.parametrize(
    ("cores", "override", "expected"),
    [(64, None, "4"), (2, None, "2"), (1, None, "1"), (64, "0", "0"), (64, "auto", "auto"), (64, "8", "8")],
)
def test_python_worker_selection(tmp_path: Path, cores: int, override: str | None, expected: str) -> None:
    for name, body in {
        "nproc": f"printf '%s\\n' {cores}",
        "uv": 'printf "%s\\n" "$@"',
    }.items():
        command = tmp_path / name
        command.write_text(f"#!/bin/sh\n{body}\n")
        command.chmod(0o755)
    env = {**os.environ, "PATH": f"{tmp_path}:{os.environ['PATH']}"}
    env.pop("SDO_PYTEST_WORKERS", None)
    env.pop("SDO_PYTEST_SHARD", None)
    if override is not None:
        env["SDO_PYTEST_WORKERS"] = override
    result = subprocess.run(
        ["bash", str(ROOT / "scripts/test_python.sh"), "-m", "not go_integration"],
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    args = result.stdout.splitlines()
    assert args[args.index("-n") + 1] == expected
    assert args[-3:] == ["-m", "not go_integration", "tests"]


def test_ci_explicitly_preserves_hosted_python_worker_count() -> None:
    workflow = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())
    assert workflow["env"]["SDO_PYTEST_WORKERS"] == "2"


@pytest.mark.parametrize("shard", ["core", "fastloop"])
def test_python_shards_keep_all_tests_and_defer_only_combined_coverage(tmp_path: Path, shard: str) -> None:
    command = tmp_path / "uv"
    command.write_text('#!/bin/sh\nprintf "%s\\n" "$@"\n')
    command.chmod(0o755)
    result = subprocess.run(
        ["bash", str(ROOT / "scripts/test_python.sh"), "-m", "not go_integration"],
        env={
            **os.environ,
            "PATH": f"{tmp_path}:{os.environ['PATH']}",
            "SDO_PYTEST_WORKERS": "2",
            "SDO_PYTEST_SHARD": shard,
        },
        capture_output=True,
        text=True,
        check=True,
    )
    args = result.stdout.splitlines()
    assert "--cov-fail-under=0" in args
    assert "--cov-report=" in args
    if shard == "core":
        assert "--ignore=tests/unit/benchmarks/sregym/fastloop" in args
        assert args[-1] == "tests"
    else:
        assert args[-1] == "tests/unit/benchmarks/sregym/fastloop"
        assert "tests" not in args


def test_python_shards_reject_unknown_selection(tmp_path: Path) -> None:
    command = tmp_path / "uv"
    command.write_text("#!/bin/sh\nexit 0\n")
    command.chmod(0o755)
    result = subprocess.run(
        ["bash", str(ROOT / "scripts/test_python.sh")],
        env={
            **os.environ,
            "SDO_PYTEST_SHARD": "typo",
            "SDO_PYTEST_WORKERS": "2",
            "PATH": f"{tmp_path}:{os.environ['PATH']}",
        },
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode != 0
    assert "unknown Python test shard" in result.stderr


def test_ci_reuses_existing_python_runners_and_combines_every_coverage_file() -> None:
    jobs = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())["jobs"]
    rows = jobs["python"]["strategy"]["matrix"]["include"]
    assert {row["shard"] for row in rows} == {"core", "fastloop"}
    assert all("scripts/test_python.sh" in row["run"] for row in rows)
    steps = jobs["required-pr-ci"]["steps"]
    download = next(step for step in steps if step.get("uses", "").startswith("actions/download-artifact@"))
    assert download["with"]["pattern"] == "python-coverage-*"
    report = next(step for step in steps if "coverage combine" in step.get("run", ""))
    assert "coverage report" in report["run"]
    assert "--fail-under=0" not in report["run"]


@pytest.mark.parametrize("missing", ["core", "fastloop"])
def test_combined_coverage_requires_both_shards(tmp_path: Path, missing: str) -> None:
    jobs = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())["jobs"]
    step = next(step for step in jobs["required-pr-ci"]["steps"] if "coverage combine" in step.get("run", ""))
    for shard in {"core", "fastloop"} - {missing}:
        artifact = tmp_path / f"python-coverage/python-coverage-{shard}/.coverage.{shard}"
        artifact.parent.mkdir(parents=True)
        artifact.touch()
    command = tmp_path / "uv"
    command.write_text("#!/bin/sh\nexit 0\n")
    command.chmod(0o755)
    result = subprocess.run(
        ["bash", "-e", "-c", step["run"]],
        cwd=tmp_path,
        env={**os.environ, "PATH": f"{tmp_path}:{os.environ['PATH']}"},
        check=False,
    )
    assert result.returncode != 0


def test_real_coverage_combines_disjoint_shards_before_applying_threshold(tmp_path: Path) -> None:
    (tmp_path / ".coveragerc").write_text("[run]\nsource = subject\n[report]\nfail_under = 100\n")
    (tmp_path / "subject.py").write_text("def first():\n    return 1\ndef second():\n    return 2\n")
    env = dict(os.environ)
    env.pop("COVERAGE_FILE", None)
    env.pop("COVERAGE_PROCESS_START", None)
    for shard, function in (("core", "first"), ("fastloop", "second")):
        script = tmp_path / f"{shard}.py"
        script.write_text(f"import subject\nsubject.{function}()\n")
        subprocess.run(
            [sys.executable, "-m", "coverage", "run", f"--data-file=.coverage.{shard}", str(script)],
            cwd=tmp_path,
            env=env,
            check=True,
        )
        partial = subprocess.run(
            [sys.executable, "-m", "coverage", "report", f"--data-file=.coverage.{shard}"],
            cwd=tmp_path,
            env=env,
            capture_output=True,
            check=False,
        )
        assert partial.returncode != 0
    subprocess.run([sys.executable, "-m", "coverage", "combine"], cwd=tmp_path, env=env, check=True)
    subprocess.run([sys.executable, "-m", "coverage", "report"], cwd=tmp_path, env=env, check=True)
