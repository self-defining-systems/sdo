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
    # An EKS lane's nodes are in its cluster: only the kind lanes count against this host's slice.
    live.assert_slice_budget("sdo-eval.slice", _plan(lane_memory_cap_gib=40), {"h1": "eks", "h2": "eks"})
    with pytest.raises(ValueError, match="exceed sdo-eval.slice MemoryMax 100 GiB"):
        live.assert_slice_budget("sdo-eval.slice", _plan(lane_memory_cap_gib=60), {"h1": "eks"})


def _proc(tmp_path: Path, *, available_gib: float, host_avg60: float, own_avg60: float) -> dict[str, Path]:
    meminfo = tmp_path / "meminfo"
    meminfo.write_text(f"MemAvailable: {int(available_gib * 1024**2)} kB\n")
    files = {"meminfo": meminfo}
    for name, avg60 in (("host", host_avg60), ("own", own_avg60)):
        files[name] = tmp_path / f"{name}.pressure"
        files[name].write_text(f"some avg10=0.00 avg60={avg60:.2f} avg300=0.00 total=1\n")
    return files


def test_an_eks_only_plan_gates_on_the_runners_own_cgroup_and_reserves_no_lane_cap(tmp_path: Path) -> None:
    """A foreign tenant stalling the host (host PSI 91) and 40 GiB free do not hold a row whose lane is a cluster."""
    from benchmarks.sregym.eval import live

    proc = _proc(tmp_path, available_gib=40, host_avg60=91, own_avg60=0)
    eks = {"h1": "eks", "h2": "eks", "v1": "eks"}
    check = live.admission_check(
        _plan(), eks, meminfo=proc["meminfo"], host_pressure=proc["host"], own_pressure=lambda: proc["own"]
    )
    assert check() is None
    proc["own"].write_text("some avg10=0.00 avg60=50.00 avg300=0.00 total=1\n")  # our own processes stall: refuse
    assert "avg60=50.00" in (check() or "")


def test_any_kind_lane_keeps_the_host_wide_gate(tmp_path: Path) -> None:
    from benchmarks.sregym.eval import live

    proc = _proc(tmp_path, available_gib=200, host_avg60=91, own_avg60=0)
    mixed = {"h1": "eks", "h2": "kind", "v1": "eks"}
    check = live.admission_check(
        _plan(), mixed, meminfo=proc["meminfo"], host_pressure=proc["host"], own_pressure=lambda: proc["own"]
    )
    assert "avg60=91.00" in (check() or "")
    proc["host"].write_text("some avg10=0.00 avg60=0.00 avg300=0.00 total=1\n")
    proc["meminfo"].write_text(f"MemAvailable: {40 * 1024**2} kB\n")  # 40 GiB < 32 GiB kind lane cap + 32 GiB reserve
    assert "lane cap" in (check() or "")


def test_lane_platforms_read_each_lane_environment_and_an_unreadable_one_is_kind(tmp_path: Path) -> None:
    from benchmarks.sregym.eval import live

    (tmp_path / "e").mkdir()
    (tmp_path / "e" / "fastloop.json").write_text('{"platform": "eks"}')
    (tmp_path / "k").mkdir()
    (tmp_path / "k" / "fastloop.json").write_text("{}")
    lanes = {"h1": tmp_path / "e", "h2": tmp_path / "k", "v1": tmp_path / "missing"}
    assert live.lane_platforms(lanes) == {"h1": "eks", "h2": "kind", "v1": "kind"}
