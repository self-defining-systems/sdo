"""The worker's imports in SREGym's real environment, nightly tier: PR CI ignores this file.

The pull request contract is the stand-in interpreter in test_worker_imports.py; only the real environment
shows a version SREGym pins that our imports cannot use.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from benchmarks.sregym.fastloop.worker_client import worker_import_error

ROOT = Path(__file__).resolve().parents[5]

pytestmark = [pytest.mark.sregym_env, pytest.mark.requires_sregym]


def test_worker_modules_import_in_the_sregym_environment() -> None:
    assert worker_import_error(ROOT) is None
