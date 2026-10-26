"""The batch rehearsal: problem loading, the toy vehicle map, per-problem stages that never stop each other, and the
error-class summary."""

from __future__ import annotations

import json
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

import pytest

from benchmarks.sregym.assurance import rehearse_batch as batch
from benchmarks.sregym.assurance.rehearse_harness import Check, Scenario, ScenarioResult

if TYPE_CHECKING:
    from pathlib import Path

KIND_FOOTPRINT = "FaultDraw(key='k', app='a', params=(), footprints=(Footprint(kind='{kind}', name='n', field=None),))"


def _plan(tmp_path: Path) -> tuple[Path, Path]:
    problems = [
        ("p-port", 1, ["K8STargetPortMisconfig"], ["Service"]),
        ("p-hook", 2, ["AdmissionWebhookOutage"], ["MutatingWebhookConfiguration", "Namespace"]),
        ("p-mixed", 3, ["NetworkPolicyBlock", "ResourceRequestTooLarge"], ["NetworkPolicy", "Deployment"]),
        ("p-nodraw", 4, ["IncorrectImage"], []),
    ]
    rows = [
        {"slot": slot, "problem_id": name, "app": "social_network", "fault_classes": classes, "encounter": encounter}
        for name, slot, classes, _ in problems
        for encounter in ("first", "repeat")
    ]
    plan = tmp_path / "plan.json"
    plan.write_text(json.dumps({"problems": rows}), encoding="utf-8")
    draws = tmp_path / "draws.json"
    draws.write_text(
        json.dumps(
            [{"id": n, "draws": [KIND_FOOTPRINT.format(kind=k) for k in kinds]} for n, _, _, kinds in problems if kinds]
        ),
        encoding="utf-8",
    )
    return plan, draws


def test_each_problem_loads_once_with_its_footprint_kinds(tmp_path: Path) -> None:
    problems = batch.load_problems(*_plan(tmp_path))
    assert [p.problem_id for p in problems] == ["p-port", "p-hook", "p-mixed", "p-nodraw"]
    assert problems[1].kinds == ("MutatingWebhookConfiguration", "Namespace")
    assert problems[2].vehicles == ("network_policy",)
    assert problems[2].unvehicled == ("ResourceRequestTooLarge",)
    assert problems[3].kinds == ()


def test_every_vehicle_is_a_toy_fault_the_toy_app_can_inject() -> None:
    from apps.toy.faults import fault_spec

    for vehicle in set(batch.TOY_VEHICLE.values()):
        for incident in batch.vehicle_stream(vehicle)["incidents"]:
            fault_spec(incident)


@pytest.mark.parametrize(
    ("name", "detail", "expected"),
    [
        ("incident 1 x: no error", "resume observation: context deadline exceeded", "deadline-exceeded"),
        ("incident 1 x: no error", "boom", "incident-error"),
        ("incident 1 x: detected", "no detector opened the incident", "no-detection"),
        ("incident 2 x: sdo mitigated", "None", "not-mitigated"),
        ("controller can delete pods", "no", "rbac-missing-verb"),
        ("x: receipt valid", "ValueError", "receipt-invalid"),
        ("scenario-budget", "300s > 240s", "over-budget"),
        ("scenario", "fastloop up failed (exit 1)", "cluster-up-failed"),
        ("something", "new", "other"),
    ],
)
def test_failed_checks_get_stable_error_classes(name: str, detail: str, expected: str) -> None:
    assert batch.classify(name, detail) == expected


def _can_i(denied: set[str]) -> Any:
    def command(argv: Any) -> tuple[int, str]:
        verb, resource = argv[argv.index("can-i") + 1 : argv.index("can-i") + 3]
        return 0, "no\n" if f"{verb} {resource}" in denied else "yes\n"

    return command


def test_rbac_probe_names_the_missing_verb_per_kind_and_skips_namespaces() -> None:
    missing = batch.probe_rbac(
        ["Service", "NetworkPolicy", "Namespace", "Secret"],
        _can_i({"delete networkpolicies.networking.k8s.io"}),
        namespace="app",
        kubeconfig="kc",
    )
    assert missing == {"NetworkPolicy": ["delete networkpolicies.networking.k8s.io"]}


def test_one_failing_stage_does_not_hide_the_others(tmp_path: Path) -> None:
    problems = batch.load_problems(*_plan(tmp_path))
    vehicles = {
        "target_port": batch.Stage("vehicle", "fail", "no-detection", "x: detected: none", "/ev"),
        "network_policy": batch.Stage("vehicle", "pass", evidence="/ev2"),
    }
    rows = {
        r.problem.problem_id: r
        for r in batch.build_rows(
            problems, vehicles, {"MutatingWebhookConfiguration": ["delete mutatingwebhookconfigurations"]}
        )
    }

    port = rows["p-port"]
    assert [s.status for s in port.stages] == ["pass", "pass", "fail"]
    assert port.failure is not None
    assert port.failure.error_class == "no-detection"
    assert port.stage_reached == "vehicle"

    hook = rows["p-hook"]
    assert hook.failure is not None
    assert hook.failure.error_class == "rbac-missing-verb"
    assert [s.error_class for s in hook.findings] == ["rbac-missing-verb", batch.COVERAGE_CLASS]

    mixed = rows["p-mixed"]
    assert mixed.failure is not None
    assert mixed.failure.error_class == batch.COVERAGE_CLASS
    assert "ResourceRequestTooLarge" in mixed.failure.detail
    assert [s.error_class for s in rows["p-port"].findings] == ["no-detection"]

    assert rows["p-nodraw"].failure is not None
    assert rows["p-nodraw"].failure.error_class == "plan-gap"


def test_summary_groups_problems_by_class_with_the_first_evidence(tmp_path: Path) -> None:
    problems = batch.load_problems(*_plan(tmp_path))
    vehicles = {
        "target_port": batch.Stage("vehicle", "fail", "no-detection", "d", "/ev"),
        "image": batch.Stage("vehicle", "pass"),
    }
    rows = batch.build_rows(problems, vehicles, {})
    classes = {c["error_class"]: c for c in batch.group_by_class(rows)}
    assert classes["no-detection"]["problems"] == ["p-port"]
    assert classes["no-detection"]["first_evidence"] == "/ev"
    assert classes[batch.COVERAGE_CLASS]["kind"] == "coverage"
    assert batch.group_by_class(rows)[0]["kind"] == "failure"


class FakeGate:
    """Stands in for a cluster: scenarios named in ``bad`` fail their ``detected`` check."""

    def __init__(
        self, lane: str, run_dir: Path, scenarios: tuple[Scenario, ...], bad: set[str], boom: set[str]
    ) -> None:
        self.lane, self.run_dir, self.scenarios, self.bad, self.boom = lane, run_dir, scenarios, bad, boom
        self.load_environment = lambda _: SimpleNamespace(namespace="sdo-toy", kubeconfig="kc")

    def run(self) -> dict[str, Any]:
        results = []
        for scenario in self.scenarios:
            failed = scenario.name in self.bad
            results.append(
                {
                    "name": scenario.name,
                    "covers": list(scenario.covers),
                    "status": "error" if scenario.name in self.boom else "fail" if failed else "pass",
                    "seconds": 1.0,
                    "detail": "reset failed (exit 1)" if scenario.name in self.boom else "",
                    "checks": [{"name": "incident 1 x: detected", "ok": not failed, "detail": "none"}],
                }
            )
        return {"code_sha": "abc" * 13, "scenarios": results}

    def _step(self, argv: Any, timeout: float) -> tuple[int, str]:
        return _can_i(set())(argv)


def test_the_batch_runs_every_vehicle_on_its_cluster_and_continues_after_failures(tmp_path: Path) -> None:
    problems = batch.load_problems(*_plan(tmp_path))
    gates: list[FakeGate] = []

    def make(lane: str, run_dir: Path, scenarios: tuple[Scenario, ...]) -> Any:
        gate = FakeGate(lane, run_dir, scenarios, {"vehicle-target_port"}, {"vehicle-network_policy"})
        gates.append(gate)
        return gate

    run = batch.Batch(tmp_path / "code", "t", tmp_path / "out", problems, clusters=2, make_gate=make)
    summary = run.run()

    assert len(gates) == 2
    assert sorted(s.name for g in gates for s in g.scenarios) == [
        "vehicle-image",
        "vehicle-network_policy",
        "vehicle-target_port",
    ]
    by_class = {c["error_class"]: c for c in summary["classes"]}
    assert by_class["no-detection"]["problems"] == ["p-port"]
    assert by_class["reset-failed"]["problems"] == ["p-mixed"]
    assert summary["problems"] == 4
    assert summary["clusters"] == 2
    assert (tmp_path / "out" / batch.SUMMARY_JSON).is_file()
    markdown = (tmp_path / "out" / batch.SUMMARY_MD).read_text(encoding="utf-8")
    assert "no-detection" in markdown
    assert "p-hook" in markdown


def test_a_cluster_that_will_not_start_fails_every_problem_at_rbac_without_raising(tmp_path: Path) -> None:
    problems = batch.load_problems(*_plan(tmp_path))

    def make(lane: str, run_dir: Path, scenarios: tuple[Scenario, ...]) -> Any:
        gate = FakeGate(lane, run_dir, scenarios, set(), {s.name for s in scenarios})
        gate.run = lambda: {  # type: ignore[method-assign]
            "code_sha": "x",
            "scenarios": [
                asdict_result(s, "error", "fastloop up failed (exit 1): kind create cluster") for s in scenarios
            ],
        }
        return gate

    summary = batch.Batch(tmp_path / "code", "t", tmp_path / "out", problems, make_gate=make).run()
    assert {c["error_class"] for c in summary["classes"] if c["kind"] == "failure"} == {"cluster-up-failed", "plan-gap"}


def asdict_result(scenario: Scenario, status: str, detail: str) -> dict[str, Any]:
    result = ScenarioResult(scenario.name, scenario.covers, status, 0.0, [], detail)
    return {**result.__dict__, "checks": []}


def test_clusters_must_be_positive(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="clusters"):
        batch.Batch(tmp_path, "t", tmp_path, batch.load_problems(*_plan(tmp_path)), clusters=0)


def test_a_check_round_trips_through_the_gate_summary() -> None:
    assert batch._check({"name": "n", "ok": False, "detail": "d"}) == Check("n", False, "d")
