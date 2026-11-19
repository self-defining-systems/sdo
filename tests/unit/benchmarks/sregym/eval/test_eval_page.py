"""The fallback page: when it fires, what it says, how the row records it (fake cluster and clock, no sleeps)."""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest

from benchmarks.sregym.adapter import persistent
from benchmarks.sregym.eval.cell import ArmContext, ArmResult, Injection, Lane, NoPageHook, run_cell
from benchmarks.sregym.eval.page import (
    FallbackPageHook,
    KubectlPageCluster,
    has_own_incident,
    incident_notice,
    lane_notice,
)
from benchmarks.sregym.eval.plan import Arm, Cell, build_plan
from benchmarks.sregym.fastloop.injection_marker import read_injection_marker, write_injection_marker
from tests.unit.benchmarks.sregym.eval.eval_fakes import Clock, FakeExecutor, make_config, make_context
from tests.unit.benchmarks.sregym.eval.test_eval_fastloop_row import Row, cell

INJECTED = 1000.0
FALLBACK = 120.0


def sdo_cell(arm: Arm = "sdo") -> Cell:
    return cell(arm)


def make_lane() -> Lane:
    return Lane(
        id="h1",
        app="hotel",
        info={
            "namespace": "hotel-reservation",
            "application": "hotel-reservation",
            "description": "A synthetic hotel application.",
            "kubeconfig": "/kube/config",
        },
    )


@dataclass
class FakeCluster:
    own: bool = False
    fail_sends: int = 0
    sent: list[tuple[str, str]] = field(default_factory=list)
    checks: int = 0

    def own_incident(self, lane: Lane) -> bool:
        self.checks += 1
        return self.own

    def send(self, lane: Lane, incident_id: str, message: str) -> None:
        if self.fail_sends:
            self.fail_sends -= 1
            raise RuntimeError("apiserver unavailable")
        self.sent.append((incident_id, message))


def make_hook(cluster: FakeCluster, clock: Clock) -> FallbackPageHook:
    ids = iter(f"page-{n}" for n in range(10))
    return FallbackPageHook(FALLBACK, cluster, clock.time, new_id=lambda: next(ids))


def test_no_page_before_the_deadline_and_none_is_even_checked() -> None:
    clock, cluster = Clock(INJECTED), FakeCluster()
    hook = make_hook(cluster, clock)
    clock.now = INJECTED + FALLBACK - 0.1
    assert hook.page(make_lane(), sdo_cell(), INJECTED) is None
    assert cluster.sent == []
    assert cluster.checks == 0


def test_pages_at_the_deadline_once_with_the_ack_time() -> None:
    clock, cluster = Clock(INJECTED), FakeCluster()
    hook = make_hook(cluster, clock)
    clock.now = INJECTED + FALLBACK
    assert hook.page(make_lane(), sdo_cell(), INJECTED) == INJECTED + FALLBACK
    clock.now += 30
    assert hook.page(make_lane(), sdo_cell(), INJECTED) is None
    assert [incident_id for incident_id, _ in cluster.sent] == ["page-0"]


def test_an_own_incident_means_no_page_now_or_later() -> None:
    clock, cluster = Clock(INJECTED), FakeCluster(own=True)
    hook = make_hook(cluster, clock)
    clock.now = INJECTED + FALLBACK + 1
    assert hook.page(make_lane(), sdo_cell(), INJECTED) is None
    cluster.own = False  # even if SDO's incident later closed and was acknowledged
    clock.now += 60
    assert hook.page(make_lane(), sdo_cell(), INJECTED) is None
    assert cluster.sent == []


def test_the_vanilla_arm_is_never_paged() -> None:
    clock, cluster = Clock(INJECTED), FakeCluster()
    hook = make_hook(cluster, clock)
    clock.now = INJECTED + 10 * FALLBACK
    assert hook.page(make_lane(), sdo_cell("vanilla"), INJECTED) is None
    assert cluster.sent == []
    assert cluster.checks == 0
    assert NoPageHook().page(make_lane(), sdo_cell(), INJECTED) is None


def test_a_failed_write_leaves_the_cell_undecided_and_the_next_poll_retries() -> None:
    clock, cluster = Clock(INJECTED), FakeCluster(fail_sends=1)
    hook = make_hook(cluster, clock)
    clock.now = INJECTED + FALLBACK
    with pytest.raises(RuntimeError):
        hook.page(make_lane(), sdo_cell(), INJECTED)
    clock.now += 1
    assert hook.page(make_lane(), sdo_cell(), INJECTED) == clock.now
    assert len(cluster.sent) == 1


def test_a_non_positive_deadline_is_rejected() -> None:
    with pytest.raises(ValueError, match="positive"):
        FallbackPageHook(0, FakeCluster(), Clock().time)


def test_the_page_text_is_the_vanilla_arms_application_block_and_names_no_fault() -> None:
    lane = make_lane()
    text = lane_notice(lane)
    assert text == ("Application: hotel-reservation\nNamespace: hotel-reservation\n\nA synthetic hotel application.")
    assert text == incident_notice("hotel-reservation", "hotel-reservation", "A synthetic hotel application.")
    clock, cluster = Clock(INJECTED), FakeCluster()
    clock.now = INJECTED + FALLBACK
    make_hook(cluster, clock).page(lane, sdo_cell(), INJECTED)
    assert cluster.sent[0][1] == text


def test_the_page_text_is_a_verbatim_slice_of_the_stock_claudecode_instruction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = Path(__file__).resolve().parents[5] / "third_party" / "sregym"
    if not (root / "clients" / "claudecode" / "driver.py").is_file():
        pytest.skip("the SREGym submodule is not checked out")
    monkeypatch.syspath_prepend(str(root))
    for name in [m for m in sys.modules if m == "clients" or m.startswith("clients.")]:
        monkeypatch.delitem(sys.modules, name)
    try:
        driver = pytest.importorskip("clients.claudecode.driver")
        app_info = {"app_name": "demo", "namespace": "demo-ns", "descriptions": "A synthetic description."}
        instruction = driver.build_instruction(app_info)
    except Exception as exc:  # SREGym's own dependencies are not part of this environment
        pytest.skip(f"SREGym build_instruction is not importable: {exc}")
    assert incident_notice("demo", "demo-ns", "A synthetic description.") in instruction


def test_the_kubectl_cluster_writes_the_external_incident_inbox_and_reads_own_incidents(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[list[str], dict[str, Any]]] = []

    def fake_kubectl(args: list[str], **kwargs: Any) -> Any:
        calls.append((args, kwargs))

        class Done:
            returncode = 0
            stdout = json.dumps(
                {"data": {"runtime-state.json": json.dumps({"incident_request": {"incident_id": "i"}})}}
            )

        return Done()

    monkeypatch.setattr(persistent, "kubectl", fake_kubectl)
    cluster, lane = KubectlPageCluster(), make_lane()
    assert cluster.own_incident(lane) is True
    assert calls[0][1]["namespace"] == "hotel-reservation-sdo"
    assert calls[0][1]["kubeconfig"] == Path("/kube/config")
    cluster.send(lane, "page-abc", "text")
    args, kwargs = calls[1]
    assert args == ["apply", "-f", "-"]
    document = json.loads(kwargs["input_text"])
    assert document["metadata"] == {"name": "sdo-external-incident", "namespace": "hotel-reservation-sdo"}
    assert json.loads(document["data"]["incident.json"]) == {
        "id": "page-abc",
        "namespace": "hotel-reservation",
        "message": "text",
    }


def test_the_inbox_refuses_an_oversized_message(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(persistent, "kubectl", lambda *a, **k: pytest.fail("must not write"))
    with pytest.raises(ValueError, match="4096"):
        persistent.apply_external_incident("c", "n", "page-1", "x" * 4097)


def test_own_incident_is_open_requested_or_awaiting_acknowledgement() -> None:
    assert has_own_incident({"incident_open": True})
    assert has_own_incident({"incident_request": {"incident_id": "i"}})
    assert has_own_incident({"pending_closure": {"request": {}}})
    assert not has_own_incident({"busy_until": "2026-11-18T12:00:00Z", "startup": {"began_at": "x"}})
    assert not has_own_incident({})


def test_the_injection_marker_round_trips_and_is_absent_before_injection(tmp_path: Path) -> None:
    from datetime import datetime, timezone

    assert read_injection_marker(tmp_path) is None
    started = datetime(2026, 11, 18, 12, 0, 5, tzinfo=timezone.utc)
    write_injection_marker(tmp_path, started)
    assert read_injection_marker(tmp_path) == started.timestamp()
    with pytest.raises(ValueError, match="timezone"):
        write_injection_marker(tmp_path, datetime(2026, 11, 18))


# The row: the hook is polled while the row runs, from the marker's injection time. -----------------------------------


def paged_row(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *, own: bool, arm: Arm = "sdo") -> tuple[Any, ...]:
    monkeypatch.setattr("benchmarks.sregym.eval.fastloop_row.apply_lane_cpu", lambda *args, **kwargs: None)
    fake = Row(tmp_path, monkeypatch, online_after=400, exit_after=500)
    plain_start = fake.start

    def start(command: list[str], **kwargs: Any) -> Any:
        process = plain_start(command, **kwargs)
        assert fake.results is not None
        # The row injected at the cell's start (fake time), before its first poll.
        write_injection_marker_at(fake.results, fake.time.now())
        return process

    fake.start = start  # type: ignore[method-assign]
    runner = fake.runner()
    cluster = FakeCluster(own=own)
    hook = FallbackPageHook(FALLBACK, cluster, fake.time.now, new_id=lambda: "page-fixed")
    context = ArmContext(config=make_config(tmp_path), page_hook=hook, now=fake.time.now, sleep=fake.time.sleep)
    the_cell = sdo_cell(arm)
    lane = runner.claim(the_cell, "h1", None)
    result = runner.execute(lane, the_cell, Injection(at=0), tmp_path, context)
    return fake, cluster, result


def write_injection_marker_at(results: Path, epoch: float) -> None:
    (results / "injection.json").write_text(json.dumps({"started_at": epoch}))


def test_a_row_without_an_own_incident_is_paged_at_the_deadline_and_the_page_is_its_clock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake, cluster, result = paged_row(tmp_path, monkeypatch, own=False)
    assert len(cluster.sent) == 1
    assert cluster.sent[0][1] == lane_notice(make_lane_for(fake))
    assert result.page_source == "fallback"
    assert result.paged_at is not None
    assert result.paged_at - 100.0 == pytest.approx(FALLBACK, abs=2.0)  # fake poll step is one second
    page = json.loads((tmp_path / "page.json").read_text())
    assert page["source"] == "fallback"
    assert page["paged_at"] == result.paged_at


def make_lane_for(fake: Any) -> Lane:
    return Lane(
        id="h1",
        app="hotel",
        info={
            "namespace": fake.env.namespace,
            "application": fake.env.application,
            "description": fake.env.description,
        },
    )


def test_a_row_with_its_own_incident_is_not_paged(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _, cluster, result = paged_row(tmp_path, monkeypatch, own=True)
    assert cluster.sent == []
    assert result.page_source == "own"  # the record's own detection inside the window
    assert not (tmp_path / "page.json").exists()


def test_the_vanilla_row_is_not_paged(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _, cluster, result = paged_row(tmp_path, monkeypatch, own=False, arm="vanilla")
    assert cluster.sent == []
    assert result.page_source == "instruction"


# The result row: fallback_page and the page time, with time to mitigate measured from the page. ----------------------


@dataclass
class PagingExecutor(FakeExecutor):
    """Waits for the deadline like a row would, asks the hook, and reports its page."""

    hook: FallbackPageHook | None = None

    def execute(self, lane: Lane, cell: Cell, injection: Injection, directory: Path, context: ArmContext) -> ArmResult:
        assert self.hook is not None
        self.clock.sleep(FALLBACK)
        paged = self.hook.page(lane, cell, injection.at)
        self.clock.sleep(self.work)
        return ArmResult(
            paged_at=paged,
            page_source="none" if paged is None else "fallback",
            agent_ended_at=self.clock.now,
            recovered_at=self.clock.now + 70,
            closed_at=self.clock.now + 80,
        )


def result_row(tmp_path: Path, *, own: bool) -> Any:
    clock = Clock()
    config = make_config(tmp_path)
    cluster = FakeCluster(own=own)
    executor = PagingExecutor(clock, hook=FallbackPageHook(FALLBACK, cluster, clock.time, notice=lambda lane: "page"))
    ctx, _ = make_context(config, clock, executor=executor, healthy_after_inject=FALLBACK + 20)
    the_cell = next(c for c in build_plan(config).cells if c.arm == "sdo")
    return run_cell(the_cell, ctx, "hotel-lane"), cluster


def test_the_row_records_the_fallback_page_and_measures_time_to_mitigate_from_it(tmp_path: Path) -> None:
    row, cluster = result_row(tmp_path, own=False)
    assert len(cluster.sent) == 1
    assert row.fallback_page is True
    assert row.page_source == "fallback"
    assert row.t_page == row.t_inject + FALLBACK
    assert row.time_to_mitigate_s == pytest.approx(row.t_healthy - row.t_page)
    assert row.inject_to_healthy_s == pytest.approx(row.t_healthy - row.t_inject)


def test_a_cell_with_its_own_incident_records_no_fallback_page(tmp_path: Path) -> None:
    row, cluster = result_row(tmp_path, own=True)
    assert cluster.sent == []
    assert row.fallback_page is False
    assert row.no_page is True
