from __future__ import annotations

from typing import TYPE_CHECKING

from benchmarks.sregym.fastloop.cli import build_parser
from benchmarks.sregym.fastloop.simultaneous import InjectBeforeResumeOps

if TYPE_CHECKING:
    from collections.abc import Callable


class _Inner:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.maintenance_calls: list[tuple[bool, bool]] = []

    def set_maintenance(
        self, control_namespace: str, *, paused: bool, generation: str, preserve_baseline: bool = False
    ) -> None:
        self.maintenance_calls.append((paused, preserve_baseline))
        self.events.append(f"{'pause' if paused else 'resume'}:{generation}")

    def inject_after_resume(
        self, control_namespace: str, generation: str, inject: Callable[[], None]
    ) -> dict[str, float]:
        self.events.append("inner-gate")
        inject()
        return {}

    def controller_pod(self, control_namespace: str) -> str:
        return "pod"


def _ops(events: list[str]) -> InjectBeforeResumeOps:
    def ack(ops: object, control: str, generation: str) -> None:
        events.append(f"ack:{generation}")

    return InjectBeforeResumeOps(_Inner(events), wait_for_pause=ack)  # type: ignore[arg-type]


def test_faults_are_injected_while_the_controller_is_paused_and_it_resumes_afterwards() -> None:
    events: list[str] = []
    ops = _ops(events)

    timings = ops.inject_after_resume("ctl", "g1", lambda: events.append("inject"))

    assert events == ["pause:g1-presim", "ack:g1-presim", "inject", "resume:g1"]
    assert timings["injection_before_resume"] == 1.0


def test_pausing_and_other_calls_pass_through() -> None:
    events: list[str] = []
    ops = _ops(events)

    ops.set_maintenance("ctl", paused=True, generation="g2")

    assert events == ["pause:g2"]
    assert ops.controller_pod("ctl") == "pod"


def test_post_injection_resume_passes_through_for_rendered_closeout() -> None:
    events: list[str] = []
    ops = _ops(events)
    ops.inject_after_resume("ctl", "first", lambda: events.append("inject:first"))
    ops.set_maintenance("ctl", paused=True, generation="after-repair")

    ops.set_maintenance("ctl", paused=False, generation="publication")

    assert events[-1] == "resume:publication"


def test_cli_flag_is_opt_in() -> None:
    parser = build_parser()
    base = ["run", "--run-dir", "x", "--agent", "sdo"]
    assert parser.parse_args(base).inject_before_resume is False
    assert parser.parse_args([*base, "--inject-before-resume"]).inject_before_resume is True


def test_healthy_baseline_cli_flag_is_opt_in_and_reaches_the_runtime_config() -> None:
    from benchmarks.sregym.fastloop.cli import _cluster_ops

    parser = build_parser()
    base = ["run", "--run-dir", "x", "--agent", "sdo"]
    off = parser.parse_args(base)
    on = parser.parse_args([*base, "--healthy-baseline"])

    assert off.healthy_baseline is False
    assert on.healthy_baseline is True
    assert type(_cluster_ops(off, "app")).__name__ == "KubectlClusterOps"
    assert type(_cluster_ops(on, "app")).__name__ == "HealthyBaselineCaptureOps"
    both = parser.parse_args([*base, "--healthy-baseline", "--inject-before-resume"])
    assert type(_cluster_ops(both, "app")).__name__ == "HealthyBaselineCaptureOps"
    assert type(_cluster_ops(both, "app")._inner).__name__ == "InjectBeforeResumeOps"  # type: ignore[attr-defined]


def test_injection_pause_preserves_the_preincident_healthy_baseline() -> None:
    events: list[str] = []
    inner = _Inner(events)
    ops = InjectBeforeResumeOps(inner, wait_for_pause=lambda *_: None)  # type: ignore[arg-type]

    def inject() -> None:
        # The pause must retain the controller's healthy capture before faults land.
        assert inner.maintenance_calls == [(True, True)]
        events.append("inject")

    ops.inject_after_resume("ctl", "incident", inject)

    assert inner.maintenance_calls == [(True, True), (False, False)]
    assert events == ["pause:incident-presim", "inject", "resume:incident"]
