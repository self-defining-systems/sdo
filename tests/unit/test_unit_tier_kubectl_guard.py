"""The unit tier fails fast on any real kubectl call instead of waiting out a request timeout."""

from __future__ import annotations

import os
import shutil
import subprocess

import pytest


def test_kubectl_exits_at_once_with_the_fake_the_seam_message() -> None:
    result = subprocess.run(["kubectl", "get", "nodes"], capture_output=True, text=True, check=False, timeout=5)

    assert result.returncode != 0
    assert "must fake the kube seam" in result.stderr
    assert "KUBECONFIG" not in os.environ


@pytest.mark.real_kubectl
def test_opt_out_marker_leaves_the_host_path_alone() -> None:
    found = shutil.which("kubectl")

    assert found is None or "failfast-kubectl" not in found
