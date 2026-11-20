"""The no-fault rehearsal rejects an unhealthy pod set before injection, with a named cause."""

from types import SimpleNamespace

import pytest

from benchmarks.sregym.fastloop.oracle_baseline import OracleBaselines, install_oracle_baselines
from benchmarks.sregym.fastloop.workload_readiness import sustained_pod_health


def pod(name, *, ready=True, restarts=0, waiting=None, phase="Running", uid=None):
    state = {"waiting": {"reason": waiting}} if waiting else {"running": {}}
    return {
        "metadata": {"name": name, "uid": uid or name},
        "status": {
            "phase": phase,
            "containerStatuses": [{"name": "c", "ready": ready, "restartCount": restarts, "state": state}],
        },
    }


class Clock:
    def __init__(self):
        self.now = 0.0
        self.sleeps = 0

    def sleep(self, seconds):
        self.now += seconds
        self.sleeps += 1


def run(samples, window=60.0, poll=5.0):
    clock = Clock()
    feed = iter(samples)
    last = []

    def read():
        nonlocal last
        last = next(feed, last)
        return last

    problems = sustained_pod_health(
        None, "ns", window_seconds=window, poll_seconds=poll, read_pods=read, sleep=clock.sleep, clock=lambda: clock.now
    )
    return problems, clock


def test_clean_pods_are_watched_for_the_whole_window():
    problems, clock = run([[pod("a")]])
    assert problems == []
    assert clock.now >= 60.0


def test_a_crash_looping_container_ready_at_first_sample_is_caught_by_restart_growth():
    # Ready at every instant sampled, yet the restart count grew: a single sample would pass this.
    problems, _ = run([[pod("grafana", restarts=3)], [pod("grafana", restarts=3)], [pod("grafana", restarts=4)]])
    assert problems == ["pod grafana container c restarted (3 -> 4)"]


@pytest.mark.parametrize(
    ("bad", "expected"),
    [
        (pod("a", ready=False), "pod a container c is not Ready"),
        (pod("a", waiting="CrashLoopBackOff", ready=False), "pod a container c is waiting: CrashLoopBackOff"),
        (pod("a", phase="Pending"), "pod a is Pending"),
    ],
)
def test_unready_pods_fail_a_sample(bad, expected):
    problems, _ = run([[pod("ok"), bad]])
    assert problems == [expected]


def test_finished_job_pods_are_not_unhealthy():
    problems, _ = run([[pod("job", phase="Succeeded", ready=False), pod("a")]])
    assert problems == []


def healthy_confirm(_problem):
    return {"kind": "health-check", "success": True, "details": {}}


def test_failing_pod_gate_rejects_the_rehearsal_and_is_not_cached(tmp_path):
    worker = SimpleNamespace(
        _problem_factory=lambda _: SimpleNamespace(), _grade=lambda *_: pytest.fail("graded an unhealthy app")
    )
    baselines = OracleBaselines(tmp_path / "oracles.json", "seed")
    rehearse = install_oracle_baselines(
        worker, baselines, confirm_healthy=healthy_confirm, pod_health_gate=lambda _: ["pod grafana is not Ready"]
    )
    verdict = rehearse("p")
    assert verdict["success"] is False
    assert verdict["details"]["rejection"] == {"cause": "pod_health_gate_failed", "named": ["pod grafana is not Ready"]}
    assert baselines.grades == {}
    assert not baselines.path.exists()


class MitigationOracle:
    pass


class Compound:
    def __init__(self, **members):
        self.oracles = members


class AlertOracle:
    pass


def test_red_pod_health_oracle_fails_loudly_instead_of_becoming_oracle_invalid(tmp_path):
    oracle = Compound(**{"fault_0:MitigationOracle": MitigationOracle(), "fault_1:AlertOracle": AlertOracle()})
    parts = [{"name": "fault_0:MitigationOracle", "success": False}, {"name": "fault_1:AlertOracle", "success": True}]
    grade = {"kind": "x", "success": False, "details": {"oracles": parts, "collateral_ok": True, "traffic_ok": True}}
    worker = SimpleNamespace(
        _problem_factory=lambda _: SimpleNamespace(mitigation_oracle=oracle), _grade=lambda *_: grade
    )
    baselines = OracleBaselines(tmp_path / "oracles.json", "seed")
    rehearse = install_oracle_baselines(
        worker, baselines, confirm_healthy=healthy_confirm, pod_health_gate=lambda _: []
    )
    verdict = rehearse("p")
    assert verdict["success"] is False
    assert verdict["details"]["rejection"] == {"cause": "pod_health_oracle_red", "named": ["fault_0:MitigationOracle"]}
    assert baselines.grades == {}


def test_red_alert_oracle_stays_oracle_invalid(tmp_path):
    oracle = Compound(**{"fault_0:AlertOracle": AlertOracle()})
    grade = {
        "kind": "x",
        "success": False,
        "details": {
            "oracles": [{"name": "fault_0:AlertOracle", "success": False}],
            "collateral_ok": True,
            "traffic_ok": True,
        },
    }
    worker = SimpleNamespace(
        _problem_factory=lambda _: SimpleNamespace(mitigation_oracle=oracle), _grade=lambda *_: grade
    )
    rehearse = install_oracle_baselines(
        worker, OracleBaselines(tmp_path / "oracles.json", "seed"), confirm_healthy=healthy_confirm
    )
    assert rehearse("p")["success"] is True


def test_single_red_pod_health_oracle_is_rejected(tmp_path):
    worker = SimpleNamespace(
        _problem_factory=lambda _: SimpleNamespace(mitigation_oracle=MitigationOracle()),
        _grade=lambda *_: {"kind": "x", "success": False, "details": {"success": False}},
    )
    rehearse = install_oracle_baselines(
        worker, OracleBaselines(tmp_path / "oracles.json", "seed"), confirm_healthy=healthy_confirm
    )
    verdict = rehearse("p")
    assert verdict["success"] is False
    assert verdict["details"]["rejection"]["named"] == ["MitigationOracle"]
