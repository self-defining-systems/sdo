"""The real SREGym Helm class after the bridge patch, nightly tier: PR CI ignores this file.

It runs `uv run --directory third_party/sregym` in a fresh interpreter (7 s on a hosted runner, over the
5 s PR target); the fake-helm contracts stay in test_idempotent_helm.py.
"""

import os
import subprocess
import textwrap
from pathlib import Path

import pytest

from tests.unit.benchmarks.sregym.fastloop.test_idempotent_helm import helm_state  # noqa: F401


@pytest.mark.requires_sregym
def test_the_real_sregym_helm_class_is_idempotent_after_the_bridge_patches_it(helm_state):  # noqa: F811
    """SREGym's own callers (`Helm.install(**configs)`) reach the replacement; unpatched, they fail."""
    root = Path(__file__).resolve().parents[5]
    script = textwrap.dedent(
        """\
        import sys
        from sregym.service.helm import Helm
        from benchmarks.sregym.fastloop.idempotent_helm import make_deploy_idempotent
        cfg = dict(release_name="r", chart_path="/c", namespace="n", remote_chart=True)
        Helm.install(**cfg)
        try:
            Helm.install(**cfg)
        except RuntimeError as exc:
            assert "cannot re-use a name" in str(exc), exc
        else:
            sys.exit("stock Helm.install unexpectedly succeeded twice")
        make_deploy_idempotent(Helm)
        Helm.install(**cfg)
        """
    )
    done = subprocess.run(
        ["uv", "run", "--directory", str(root / "third_party/sregym"), "python", "-c", script],
        env={**os.environ, "PYTHONPATH": str(root)},
        capture_output=True,
        text=True,
    )
    assert done.returncode == 0, done.stderr[-2000:]
