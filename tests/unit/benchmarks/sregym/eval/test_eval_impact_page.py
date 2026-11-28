"""The impact page (detector-ablation arm): the runner's own prober, not SDO, decides when SDO is paged."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import pytest

from benchmarks.sregym.eval.cell import HARNESS_FALLBACK_SOURCES, ArmContext, Injection
from benchmarks.sregym.eval.page import (
    FallbackPageHook,
    ImpactPageHook,
    first_unhealthy_at,
    lane_notice,
    page_hook,
)
from tests.unit.benchmarks.sregym.eval.eval_fakes import Clock, make_config
from tests.unit.benchmarks.sregym.eval.test_eval_fastloop_row import Row
from tests.unit.benchmarks.sregym.eval.test_eval_page import (
    FakeCluster,
    make_lane,
    make_lane_for,
    sdo_cell,
    write_injection_marker_at,
)

if TYPE_CHECKING:
    from pathlib import Path

    from benchmarks.sregym.eval.cell import ArmResult

INJECTED = 1000.0
DEADLINE = 120.0


def write_probes(path: Path, samples: list[dict[str, Any]], partial: str = "") -> Path:
    path.write_text("".join(json.dumps(s) + "\n" for s in samples) + partial, encoding="utf-8")
    return path


def test_impact_is_the_first_unhealthy_sample_after_injection_and_gaps_are_not_samples(tmp_path: Path) -> None:
    probes = write_probes(
        tmp_path / "probes.jsonl",
        [
            {"at": INJECTED - 5, "healthy": False, "detail": "install"},
            {"at": INJECTED + 3, "healthy": True, "detail": ""},
            {"at": INJECTED + 6, "unavailable": "host could not ask"},
            {"at": INJECTED + 9, "healthy": False, "detail": "502"},
            {"at": INJECTED + 12, "healthy": False, "detail": "502"},
        ],
        partial='{"at": 10',
    )
    assert first_unhealthy_at(probes, INJECTED) == INJECTED + 9
    assert first_unhealthy_at(tmp_path / "missing.jsonl", INJECTED) is None


def make_hook(cluster: FakeCluster, clock: Clock, probes: Path) -> ImpactPageHook:
    return ImpactPageHook(DEADLINE, cluster, clock.time, probes=lambda cell: probes, new_id=lambda: "page-fixed")


def test_pages_once_at_impact_with_the_generic_notice(tmp_path: Path) -> None:
    clock, cluster = Clock(INJECTED + 4), FakeCluster()
    probes = write_probes(tmp_path / "probes.jsonl", [{"at": INJECTED + 3, "healthy": True}])
    hook = make_hook(cluster, clock, probes)
    assert hook.page(make_lane(), sdo_cell(), INJECTED) is None
    write_probes(probes, [{"at": INJECTED + 3, "healthy": True}, {"at": INJECTED + 6, "healthy": False}])
    clock.now = INJECTED + 7
    assert hook.page(make_lane(), sdo_cell(), INJECTED) == INJECTED + 7
    clock.now += 30
    assert hook.page(make_lane(), sdo_cell(), INJECTED) is None
    assert cluster.sent == [("page-fixed", lane_notice(make_lane()))]
    assert hook.record(sdo_cell()) == {
        "source": "impact",
        "injected_at": INJECTED,
        "impact_at": INJECTED + 6,
        "paged_at": INJECTED + 7,
        "text": lane_notice(make_lane()),
        "sdo_incident_before_page": False,
    }


def test_without_observed_impact_it_pages_at_the_deadline(tmp_path: Path) -> None:
    clock, cluster = Clock(INJECTED + DEADLINE - 1), FakeCluster(own=True)
    hook = make_hook(cluster, clock, tmp_path / "probes.jsonl")
    assert hook.page(make_lane(), sdo_cell(), INJECTED) is None
    clock.now = INJECTED + DEADLINE
    assert hook.page(make_lane(), sdo_cell(), INJECTED) == INJECTED + DEADLINE
    record = hook.record(sdo_cell())
    assert record is not None
    assert (record["source"], record["impact_at"], record["sdo_incident_before_page"]) == ("deadline", None, True)


def test_the_vanilla_arm_is_never_paged(tmp_path: Path) -> None:
    clock, cluster = Clock(INJECTED + 10 * DEADLINE), FakeCluster()
    hook = make_hook(cluster, clock, write_probes(tmp_path / "p.jsonl", [{"at": INJECTED + 1, "healthy": False}]))
    assert hook.page(make_lane(), sdo_cell("vanilla"), INJECTED) is None
    assert cluster.sent == []


def test_the_config_selects_the_hook_and_the_fallback_stays_the_default(tmp_path: Path) -> None:
    assert make_config(tmp_path).page == "fallback"
    assert isinstance(page_hook(make_config(tmp_path), FakeCluster(), Clock().time), FallbackPageHook)
    config = make_config(tmp_path, page="impact")
    hook = page_hook(config, FakeCluster(), Clock().time)
    assert isinstance(hook, ImpactPageHook)
    assert hook.deadline_seconds == config.fallback_page_seconds
    assert hook.probes(sdo_cell()) == sdo_cell().directory(config) / "probes.jsonl"
    with pytest.raises(ValueError, match="impact"):
        make_config(tmp_path, page="detector")


def impact_row(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *, unhealthy: bool) -> tuple[ArmResult, dict[str, Any]]:
    """A fastloop row paged by the impact hook; its prober sees the app unhealthy from injection, or never."""
    monkeypatch.setattr("benchmarks.sregym.eval.fastloop_row.apply_lane_cpu", lambda *args, **kwargs: None)
    fake = Row(tmp_path, monkeypatch, online_after=400, exit_after=500)
    plain_start = fake.start

    def start(command: list[str], **kwargs: Any) -> Any:
        process = plain_start(command, **kwargs)
        assert fake.results is not None
        write_injection_marker_at(fake.results, fake.time.now())
        write_probes(tmp_path / "probes.jsonl", [{"at": fake.time.now(), "healthy": not unhealthy}])
        return process

    fake.start = start  # type: ignore[method-assign]
    runner = fake.runner()
    cluster = FakeCluster()
    hook = ImpactPageHook(
        DEADLINE, cluster, fake.time.now, probes=lambda cell: tmp_path / "probes.jsonl", new_id=lambda: "page-fixed"
    )
    context = ArmContext(config=make_config(tmp_path), page_hook=hook, now=fake.time.now, sleep=fake.time.sleep)
    lane = runner.claim(sdo_cell(), "h1", None)
    result = runner.execute(lane, sdo_cell(), Injection(at=0), tmp_path, context)
    assert cluster.sent == [("page-fixed", lane_notice(make_lane_for(fake)))]
    page = json.loads((tmp_path / "page.json").read_text())
    assert page["text"] == lane_notice(make_lane_for(fake))
    assert result.paged_at == page["paged_at"]
    return result, page


def test_the_row_records_the_impact_page_and_uses_it_as_the_clock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _, page = impact_row(tmp_path, monkeypatch, unhealthy=True)
    assert page["source"] == "impact"
    assert page["impact_at"] == page["injected_at"]
    assert page["impact_at"] <= page["paged_at"] <= page["impact_at"] + 2  # fake poll step is one second


@pytest.mark.parametrize(("unhealthy", "source", "fallback"), [(True, "impact", False), (False, "deadline", True)])
def test_results_name_the_page_that_page_json_names(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, unhealthy: bool, source: str, fallback: bool
) -> None:
    result, page = impact_row(tmp_path, monkeypatch, unhealthy=unhealthy)
    assert page["source"] == result.page_source == source
    assert (result.page_source in HARNESS_FALLBACK_SOURCES) is fallback
