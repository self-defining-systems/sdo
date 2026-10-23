"""Reject broken host-driver imports before launching any stage."""

import subprocess
from collections.abc import Mapping
from pathlib import Path

from sregym_fake_host import FakeHost

from benchmarks.sregym.runner.experiment import ExperimentConfig
from benchmarks.sregym.runner.preflight import SystemHost, run_preflight

ROOT = Path(__file__).resolve().parents[5]


def test_kickoff_import_failure_blocks_launch(tmp_path: Path) -> None:
    class BrokenHost(FakeHost):
        def kickoff_import(self, command: str, workdir: Path, sregym_dir: Path, env: Mapping[str, str]) -> str | None:
            assert command == "uv run python -m benchmarks.sregym.adapter.driver"
            assert workdir == (sregym_dir / "../..").resolve()
            assert env["PYTHONUNBUFFERED"] == "1"
            assert env["PYTHONPATH"] == str(ROOT)
            return "ModuleNotFoundError: No module named 'protovalidate'"

    report = run_preflight(
        [ExperimentConfig(agent="sdo_codex")],
        project_root=ROOT,
        sregym_dir=tmp_path / "standalone" / "harness",
        env={"PYTHONPATH": str(ROOT)},
        host=BrokenHost(home=tmp_path),
    )
    check = next(c for c in report.checks if c.name == "kickoff-import")
    assert check.status == "fail"
    assert "protovalidate" in check.detail
    assert "kickoff" in check.remedy


def test_system_probe_enters_harness_uv_before_registered_kickoff(tmp_path: Path) -> None:
    calls = []

    def run(argv, **kwargs):
        calls.append((argv, kwargs))
        return subprocess.CompletedProcess(argv, 0, "usage: driver", "")

    host = SystemHost(run=run)
    assert (
        host.kickoff_import(
            "uv run python -m benchmarks.sregym.adapter.driver", tmp_path.parent, tmp_path, {"PYTHONPATH": "source"}
        )
        is None
    )
    argv, kwargs = calls[0]
    assert argv[:4] == ["uv", "run", "python", "-c"]
    assert "--help" in argv[4]
    assert "uv run python -m benchmarks.sregym.adapter.driver" in argv[4]
    assert kwargs["cwd"] == tmp_path
    assert kwargs["env"] == {"PYTHONPATH": "source"}
    assert kwargs["timeout"] <= 60
