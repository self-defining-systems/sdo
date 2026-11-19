"""Live-run assembly: config tables, the no-store guard and factories."""

from __future__ import annotations

from pathlib import Path

import pytest

from benchmarks.sregym.eval.__main__ import load_config
from benchmarks.sregym.eval.live import NoStore, build_store, load_factory


def test_load_config_ignores_the_live_tables(tmp_path: Path) -> None:
    path = tmp_path / "c.toml"
    path.write_text(
        f"""
stage = "s"
runs_root = "{tmp_path}"
gate = "x:y"

[[apps]]
name = "hotel"
problems = ["a"]
sdo_lanes = ["h1"]
vanilla_lanes = ["v1"]

[row]
code_root = "/x"

[store]
root = "/y"
"""
    )
    assert load_config(path).stage == "s"


def test_without_a_store_a_snapshot_is_a_configuration_error() -> None:
    store, hook = build_store(None)
    assert isinstance(store, NoStore)
    assert hook is None
    with pytest.raises(ValueError, match=r"\[store\]"):
        store.resolve("hotel", "base")


def test_factories_are_module_attr() -> None:
    assert load_factory("json:dumps")({}) == "{}"
    with pytest.raises(ValueError, match="module:attr"):
        load_factory("json")


def _plan(**overrides: object):
    from types import SimpleNamespace

    from tests.unit.benchmarks.sregym.eval.eval_fakes import make_config

    return SimpleNamespace(config=make_config(Path("/runs"), **overrides))


def test_the_slice_memory_max_bounds_the_lanes_when_no_budget_is_configured(monkeypatch: pytest.MonkeyPatch) -> None:
    from benchmarks.sregym.eval import live

    monkeypatch.setattr(live, "slice_memory_max", lambda parent: 100 * 1024**3)
    live.assert_slice_budget("sdo-eval.slice", _plan())  # h1 h2 v1: 3 lanes x 32 GiB = 96 GiB <= 100
    with pytest.raises(ValueError, match="exceed sdo-eval.slice MemoryMax 100 GiB"):
        live.assert_slice_budget("sdo-eval.slice", _plan(lane_memory_cap_gib=40))
    live.assert_slice_budget("sdo-eval.slice", _plan(lane_budget_gib=500, lane_memory_cap_gib=40))  # configured wins
    live.assert_slice_budget(None, _plan(lane_memory_cap_gib=400))  # no slice: unchecked
