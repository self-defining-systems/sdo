"""Ratchet: the SREGym worker runs in SREGym's own environment, without SDO's dependencies.

Syncing that environment is too slow for a pull request, so these use a stand-in interpreter that holds only
what SREGym's lock also has. test_worker_imports_real_sregym.py runs the real one nightly.
"""

from __future__ import annotations

import importlib.metadata
import os
import re
import sys
from pathlib import Path

import pytest
import tomllib

from benchmarks.sregym.fastloop.worker_client import worker_import_error, worker_modules

ROOT = Path(__file__).resolve().parents[5]


def distribution_name(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


@pytest.fixture(scope="module")
def sregym_stand_in(tmp_path_factory: pytest.TempPathFactory) -> list[str]:
    """``python -S`` seeing the stdlib, this checkout and the installed packages SREGym's lock names."""
    lock = tomllib.loads((ROOT / "third_party/sregym/uv.lock").read_text())
    locked = {distribution_name(package["name"]) for package in lock["package"]}
    site = tmp_path_factory.mktemp("sregym-site")
    for dist in importlib.metadata.distributions():
        if distribution_name(dist.metadata["Name"]) not in locked:
            continue
        for entry in {file.parts[0] for file in dist.files or () if file.parts[0] != ".."}:
            link = site / entry
            if not link.is_symlink():
                link.symlink_to(Path(str(dist.locate_file(entry))))
    return ["env", f"PYTHONPATH={ROOT}{os.pathsep}{site}", sys.executable, "-S"]


@pytest.mark.requires_sregym
def test_worker_modules_import_without_sdo_dependencies(sregym_stand_in: list[str]) -> None:
    """h9.18 astronomy deploys failed: source deployment pulled sdo.contracts (protovalidate) into the worker."""
    assert "benchmarks.sregym.fastloop.source_deployment" in worker_modules(ROOT)
    assert worker_import_error(ROOT, python=sregym_stand_in) is None


@pytest.mark.requires_sregym
def test_a_module_that_needs_sdo_dependencies_is_named(sregym_stand_in: list[str]) -> None:
    modules = ["benchmarks.sregym.fastloop.worker_bridge", "sdo.contracts"]
    error = worker_import_error(ROOT, modules, python=sregym_stand_in)
    assert error is not None
    assert "protovalidate" in error


def test_a_checkout_without_the_harness_is_named(tmp_path: Path) -> None:
    assert "not checked out" in str(worker_import_error(tmp_path))
