"""Ratchet: the SREGym worker runs in SREGym's own environment, without SDO's dependencies."""

from __future__ import annotations

from pathlib import Path

import pytest

from benchmarks.sregym.fastloop.worker_client import worker_import_error, worker_modules

ROOT = Path(__file__).resolve().parents[5]
SREGYM_READY = (ROOT / "third_party" / "sregym" / "pyproject.toml").is_file()


@pytest.mark.skipif(not SREGYM_READY, reason="SREGym submodule is not initialized")
def test_worker_modules_import_in_the_sregym_environment() -> None:
    """h9.18 astronomy deploys failed: source deployment pulled sdo.contracts (protovalidate) into the worker."""
    assert "benchmarks.sregym.fastloop.source_deployment" in worker_modules(ROOT)
    assert worker_import_error(ROOT) is None


@pytest.mark.skipif(not SREGYM_READY, reason="SREGym submodule is not initialized")
def test_a_module_that_needs_sdo_dependencies_is_named() -> None:
    error = worker_import_error(ROOT, ["benchmarks.sregym.fastloop.worker_bridge", "sdo.contracts"])
    assert error is not None
    assert "protovalidate" in error


def test_a_checkout_without_the_harness_is_named(tmp_path: Path) -> None:
    assert "not checked out" in str(worker_import_error(tmp_path))
