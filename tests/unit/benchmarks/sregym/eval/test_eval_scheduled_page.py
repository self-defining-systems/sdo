"""The scheduled page (findings-hidden arm): the harness pages at the paired cell's own page offset from injection."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import pytest

from benchmarks.sregym.eval.cell import HARNESS_FALLBACK_SOURCES, ArmContext, Injection
from benchmarks.sregym.eval.page import ScheduledPageHook, lane_notice, page_hook
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

INJECTED = 1000.0
OFFSET = 21.6
PAIRED = "synthetic-playbook-cell"


def test_pages_once_at_the_offset_with_the_generic_notice_whatever_sdo_saw() -> None:
    clock, cluster = Clock(INJECTED + OFFSET - 0.5), FakeCluster(own=True)
    hook = ScheduledPageHook(OFFSET, PAIRED, cluster, clock.time, new_id=lambda: "page-fixed")
    assert hook.page(make_lane(), sdo_cell(), INJECTED) is None
    clock.now = INJECTED + OFFSET + 0.4
    assert hook.page(make_lane(), sdo_cell(), INJECTED) == INJECTED + OFFSET + 0.4
    clock.now += 30
    assert hook.page(make_lane(), sdo_cell(), INJECTED) is None
    assert cluster.sent == [("page-fixed", lane_notice(make_lane()))]
    assert hook.record(sdo_cell()) == {
        "source": "scheduled",
        "injected_at": INJECTED,
        "offset_s": OFFSET,
        "paired_cell": PAIRED,
        "scheduled_at": INJECTED + OFFSET,
        "paged_at": INJECTED + OFFSET + 0.4,
        "text": lane_notice(make_lane()),
        "sdo_incident_before_page": True,
    }


def test_the_vanilla_arm_is_never_paged() -> None:
    clock, cluster = Clock(INJECTED + 10 * OFFSET), FakeCluster()
    assert (
        ScheduledPageHook(OFFSET, PAIRED, cluster, clock.time).page(make_lane(), sdo_cell("vanilla"), INJECTED) is None
    )
    assert cluster.sent == []


def test_the_config_selects_the_hook_only_with_an_offset_and_a_paired_cell(tmp_path: Path) -> None:
    hook = page_hook(
        make_config(tmp_path, page="scheduled", page_offset_seconds=OFFSET, page_paired_cell=PAIRED),
        FakeCluster(),
        Clock().time,
    )
    assert isinstance(hook, ScheduledPageHook)
    assert (hook.offset_seconds, hook.paired_cell) == (OFFSET, PAIRED)
    for overrides in (
        {"page": "scheduled"},
        {"page": "scheduled", "page_offset_seconds": OFFSET},
        {"page": "impact", "page_offset_seconds": OFFSET, "page_paired_cell": PAIRED},
        {"page_offset_seconds": OFFSET},
    ):
        with pytest.raises(ValueError, match="scheduled"):
            make_config(tmp_path, **overrides)
    with pytest.raises(ValueError, match="negative"):
        ScheduledPageHook(-1, PAIRED, FakeCluster(), Clock().time)


def test_the_row_pages_at_the_offset_and_uses_the_page_as_the_clock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("benchmarks.sregym.eval.fastloop_row.apply_lane_cpu", lambda *args, **kwargs: None)
    fake = Row(tmp_path, monkeypatch, online_after=400, exit_after=500)
    plain_start = fake.start

    def start(command: list[str], **kwargs: Any) -> Any:
        process = plain_start(command, **kwargs)
        assert fake.results is not None
        write_injection_marker_at(fake.results, fake.time.now())
        return process

    fake.start = start  # type: ignore[method-assign]
    runner = fake.runner()
    cluster = FakeCluster()
    hook = ScheduledPageHook(OFFSET, PAIRED, cluster, fake.time.now, new_id=lambda: "page-fixed")
    context = ArmContext(config=make_config(tmp_path), page_hook=hook, now=fake.time.now, sleep=fake.time.sleep)
    lane = runner.claim(sdo_cell(), "h1", None)
    result = runner.execute(lane, sdo_cell(), Injection(at=0), tmp_path, context)
    page = json.loads((tmp_path / "page.json").read_text())
    assert cluster.sent == [("page-fixed", lane_notice(make_lane_for(fake)))]
    assert page["source"] == result.page_source == "scheduled"
    assert result.page_source not in HARNESS_FALLBACK_SOURCES
    assert result.paged_at == page["paged_at"]
    # The fake row polls once a second: the page is within one poll of its scheduled time, well inside 3 s.
    assert page["scheduled_at"] == page["injected_at"] + OFFSET
    assert page["scheduled_at"] <= page["paged_at"] <= page["scheduled_at"] + 1.5
