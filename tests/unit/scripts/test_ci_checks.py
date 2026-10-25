"""Concurrent PR lanes retain failures and clean up their owned processes."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
SPEC = importlib.util.spec_from_file_location("ci_checks", ROOT / "scripts/test_ci_checks.py")
assert SPEC is not None
assert SPEC.loader is not None
checks = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = checks
SPEC.loader.exec_module(checks)


def test_failure_waits_for_the_other_real_process_to_finish(tmp_path: Path) -> None:
    failed = "from pathlib import Path; Path('failed').write_text('ran'); raise SystemExit(7)"
    passed = "import time; from pathlib import Path; time.sleep(.1); Path('passed').write_text('ran')"
    lanes = [
        checks.Lane("failing", (sys.executable, "-c", failed)),
        checks.Lane("passing", (sys.executable, "-c", passed)),
    ]
    assert checks.run_lanes(lanes, tmp_path) == 7
    assert (tmp_path / "failed").read_text() == "ran"
    assert (tmp_path / "passed").read_text() == "ran"


def test_both_successful_real_processes_are_required(tmp_path: Path) -> None:
    command = (sys.executable, "-c", "raise SystemExit(0)")
    assert checks.run_lanes([checks.Lane("go", command), checks.Lane("python", command)], tmp_path) == 0


def test_cancellation_terminates_the_real_grandchild_process_group(tmp_path: Path) -> None:
    import os
    import signal
    import subprocess
    import time

    grandchild = (
        "import os, signal, time; from pathlib import Path\n"
        "def stop(*_):\n Path('stopped').write_text('stopped'); raise SystemExit(0)\n"
        "signal.signal(signal.SIGTERM, stop)\n"
        "Path('ready').write_text(str(os.getpid()))\n"
        "while True: time.sleep(60)\n"
    )
    worker = tmp_path / "worker.py"
    worker.write_text(
        f"import os, subprocess, sys, time; from pathlib import Path\n"
        "Path('worker_pid').write_text(str(os.getpid()))\n"
        f"subprocess.Popen([sys.executable, '-c', {grandchild!r}])\n"
        "while True: time.sleep(60)\n"
    )
    # Load the repository file explicitly: an optional dependency also ships
    # a concrete `scripts` package, which takes precedence over our namespace.
    command = (
        f"import sys, runpy; ns = runpy.run_path({str(ROOT / 'scripts/test_ci_checks.py')!r}); "
        f"from pathlib import Path; raise SystemExit(ns['run_lanes']([ns['Lane']('worker', "
        f"(sys.executable, {str(worker)!r}))], Path({str(tmp_path)!r})))"
    )
    owner = subprocess.Popen([sys.executable, "-c", command])
    try:
        deadline = time.monotonic() + 10
        while not (tmp_path / "ready").exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        assert (tmp_path / "ready").exists(), "real grandchild did not start"
        owner.terminate()
        assert owner.wait(timeout=10) == 128 + signal.SIGTERM
        deadline = time.monotonic() + 10
        while not (tmp_path / "stopped").exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        assert (tmp_path / "stopped").read_text() == "stopped"
    finally:
        if owner.poll() is None:
            owner.kill()
            owner.wait()
        if (tmp_path / "worker_pid").exists():
            try:
                os.killpg(int((tmp_path / "worker_pid").read_text()), signal.SIGKILL)
            except ProcessLookupError:
                pass


def test_repository_binary_helper_wins_over_an_installed_scripts_package(tmp_path: Path) -> None:
    import subprocess

    installed = tmp_path / "site-packages"
    package = installed / "scripts"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("raise RuntimeError('unrelated installed package selected')\n")
    command = (
        f"import sys; sys.path[:0] = [{str(ROOT)!r}, {str(installed)!r}]; "
        "from scripts import operator_test_binary; "
        f"assert operator_test_binary.__file__ == {str(ROOT / 'scripts/operator_test_binary.py')!r}"
    )
    completed = subprocess.run([sys.executable, "-c", command], capture_output=True, text=True, check=False)
    assert completed.returncode == 0, completed.stderr
