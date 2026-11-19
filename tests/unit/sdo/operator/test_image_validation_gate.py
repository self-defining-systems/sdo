"""The controller image's validation smoke gate, run end to end on the built operator (nightly tier: it compiles the
fixture's playbook programs). The pull-request tier runs the same command on an empty candidate
(``test_image_python_pins.py``)."""

from __future__ import annotations

import os
import subprocess
from typing import TYPE_CHECKING

import pytest

from tests.unit.sdo.operator.image_gate import REPO_ROOT, controller_validation_gate

if TYPE_CHECKING:
    from pathlib import Path


@pytest.mark.go_integration
def test_the_controller_image_validation_gate_runs_on_the_built_operator(operator_binary: Path) -> None:
    argv, mounts = controller_validation_gate((REPO_ROOT / "scripts/build_sdo_images.sh").read_text())
    assert argv[0] == "sdo-operator"
    args = [str(mounts.get(arg, arg)) for arg in argv[1:]]
    env = {**os.environ, "SDO_OPERATOR_ROOT": str(REPO_ROOT / "sdo/operator"), "GOFLAGS": "-mod=mod"}
    proc = subprocess.run(
        [str(operator_binary), *args], env=env, capture_output=True, text=True, timeout=600, check=False
    )
    assert proc.returncode == 0, proc.stderr[-2000:]
