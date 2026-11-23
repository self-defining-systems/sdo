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


def _psi(path: Path, avg60: float) -> Path:
    path.write_text(f"some avg10=0.00 avg60={avg60:.2f} avg300=0.00 total=1\nfull avg10=0 avg60=0 avg300=0 total=0\n")
    return path


def _proc(tmp_path: Path, *, available_gib: float, host_avg60: float) -> dict[str, Path]:
    meminfo = tmp_path / "meminfo"
    meminfo.write_text(f"MemAvailable: {int(available_gib * 1024**2)} kB\n")
    own = tmp_path / "own.slice"
    own.mkdir()
    for resource in ("cpu", "io", "memory"):
        _psi(own / f"{resource}.pressure", 0)
    return {"meminfo": meminfo, "host": _psi(tmp_path / "host.pressure", host_avg60), "own": own}


def _admission(tmp_path: Path, platforms: dict[str, str], **proc: float):
    from benchmarks.sregym.eval import live

    files = _proc(tmp_path, **proc)  # type: ignore[arg-type]
    admission = live.admission(
        _plan(),
        platforms,
        meminfo=files["meminfo"],
        host_pressure=files["host"],
        own_cgroup=lambda: files["own"],
        loadavg=lambda: 6000.0,
    )
    return admission, files


def test_an_eks_only_plan_gates_on_the_runners_own_cgroup_not_on_host_load_or_pressure(tmp_path: Path) -> None:
    """Foreign tenants (host load 6000, host memory PSI 91) and 40 GiB free do not hold a row whose lane is a cluster;
    the runner's own CPU, IO or memory stalls do."""
    admission, files = _admission(tmp_path, {"h1": "eks", "h2": "eks", "v1": "eks"}, available_gib=40, host_avg60=91)
    assert admission.loadavg() == 0.0
    assert admission.host_check() is None
    for resource in ("cpu", "io", "memory"):
        _psi(files["own"] / f"{resource}.pressure", 50)
        assert f"{resource} pressure some avg60=50.00" in (admission.host_check() or "")
        _psi(files["own"] / f"{resource}.pressure", 0)
    files["meminfo"].write_text(f"MemAvailable: {16 * 1024**2} kB\n")  # under the 32 GiB reserve: refuse
    assert "reserve" in (admission.host_check() or "")


def test_any_kind_lane_keeps_the_host_load_and_host_memory_gates(tmp_path: Path) -> None:
    admission, files = _admission(tmp_path, {"h1": "eks", "h2": "kind", "v1": "eks"}, available_gib=200, host_avg60=91)
    assert admission.loadavg() == 6000.0
    assert "avg60=91.00" in (admission.host_check() or "")
    _psi(files["host"], 0)
    files["meminfo"].write_text(f"MemAvailable: {40 * 1024**2} kB\n")  # 40 GiB < 32 GiB kind lane cap + 32 GiB reserve
    assert "lane cap" in (admission.host_check() or "")


def test_an_eks_plan_with_no_readable_cgroup_refuses(tmp_path: Path) -> None:
    from benchmarks.sregym.eval import live

    def unknown() -> Path:
        raise ValueError("eval: no cgroup v2 entry in /proc/self/cgroup")

    admission = live.admission(_plan(), {"h1": "eks", "h2": "eks", "v1": "eks"}, own_cgroup=unknown)
    assert "cgroup v2" in (admission.host_check() or "")


def test_lane_platforms_read_each_lane_environment_and_an_unreadable_one_is_kind(tmp_path: Path) -> None:
    from benchmarks.sregym.eval import live

    (tmp_path / "e").mkdir()
    (tmp_path / "e" / "fastloop.json").write_text('{"platform": "eks"}')
    (tmp_path / "k").mkdir()
    (tmp_path / "k" / "fastloop.json").write_text("{}")
    lanes = {"h1": tmp_path / "e", "h2": tmp_path / "k", "v1": tmp_path / "missing"}
    assert live.lane_platforms(lanes) == {"h1": "eks", "h2": "kind", "v1": "kind"}
