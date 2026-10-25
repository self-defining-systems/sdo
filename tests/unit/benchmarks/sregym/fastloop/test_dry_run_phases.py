"""Cold setup has its own bound; every steady health check retains five minutes."""

import pytest

from benchmarks.sregym.fastloop import dry_run
from benchmarks.sregym.fastloop.cli import build_parser


def test_cold_setup_cannot_consume_steady_health_budget():
    assert dry_run.phase_deadline({}, 10, 900, 300) == 910
    assert dry_run.phase_deadline({"steady_started": 800}, 10, 900, 300) == 1100


def test_setup_budget_is_independently_bounded(tmp_path):
    args = build_parser().parse_args(["eval-dry-run", "--run-dir", str(tmp_path), "--setup-timeout", "900"])
    assert args.setup_timeout == 900
    assert args.timeout == 300
    with pytest.raises(SystemExit):
        build_parser().parse_args(["eval-dry-run", "--run-dir", str(tmp_path), "--setup-timeout", "901"])
