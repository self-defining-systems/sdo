"""Three real selected rows through the runner and probes, with scripted I/O.

These are runner/probe integration tests. Native controller, broker and worker
transport are covered by test_fastloop_hermetic_run and the hermetic Go gate.
"""

from __future__ import annotations

import json
import subprocess
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from benchmarks.sregym.fastloop.fault_tracker import FaultTracker, probes_for
from benchmarks.sregym.fastloop.loop import AgentOutcome, InjectionWindow, LoopConfig, run_incidents
from benchmarks.sregym.fastloop.records import OracleVerdict, load_records
from benchmarks.sregym.faults.description import describe_sregym_problems, validate_suite
from tests.unit.benchmarks.sregym.fastloop.test_phase4_probes import deployment

if TYPE_CHECKING:
    from collections.abc import Callable

ROOT = Path(__file__).resolve().parents[5]
SELECTED = (
    "mf__hotel_reservation__n2__s1555858305__reliable__v_",
    "mf__social_network__n2__s2571351228__reliable__v_",
    "mf__astronomy_shop__n2__s3028155043__reliable__v_",
)


@pytest.fixture(scope="module")
def descriptions():
    return describe_sregym_problems(SELECTED, sregym_dir=ROOT / "third_party/sregym")


@pytest.mark.parametrize("problem", SELECTED)
def test_selected_app_pair_runs_real_probes_and_preserves_its_own_memory(tmp_path, descriptions, problem):
    description = descriptions[problem]
    capabilities = validate_suite({problem: description}, require_tracking=True, capability_mode="permissive")
    faults = description.ordered_faults()
    good = {("deployment", f.component): deployment(dnsPolicy="ClusterFirst", volumes=[]) for f in faults}
    cluster = deepcopy(good)
    ticks = [0.0]
    samples = []
    injections = []
    repository = tmp_path / "accepted"
    repository.mkdir()
    subprocess.run(["git", "init", "-q", str(repository)], check=True)

    def commit(message):
        subprocess.run(["git", "-C", str(repository), "add", "."], check=True)
        subprocess.run(
            [
                "git",
                "-C",
                str(repository),
                "-c",
                "user.name=Scripted",
                "-c",
                "user.email=s@example.invalid",
                "commit",
                "--allow-empty",
                "-qm",
                message,
            ],
            check=True,
        )

    commit("fresh app memory")

    class Driver:
        tracker = None

        def inject(self, problem_id):
            assert problem_id == problem
            assert cluster == good
            self.tracker = FaultTracker(
                probes_for(faults, lambda kind, name: cluster.get((kind, name))),
                stable_seconds=0,
                clock=lambda: ticks[0],
            )
            for fault in faults:
                pod = cluster["deployment", fault.component]["spec"]["template"]["spec"]
                if fault.kind == "sidecar_port_conflict":
                    pod["containers"].append({"name": "sidecar", "command": ["nc", "-lk", "-p", "8080"]})
                elif fault.kind == "service_port_conflict":
                    pod["containers"][0]["ports"] = [{"containerPort": 8080, "hostPort": 9100}]
                elif fault.kind == "wrong_dns_policy":
                    pod["dnsPolicy"] = "Default"
                elif fault.kind == "init_container_dependency_hang":
                    pod["initContainers"] = [{"name": "dependency-check", "command": ["sleep", "infinity"]}]
                elif fault.kind == "duplicate_pvc_mounts":
                    pod["volumes"] = [{"name": "shared", "persistentVolumeClaim": {"claimName": "shared"}}]
                else:
                    pytest.fail(f"fixture has no honest injection for {fault.kind}")
            ticks[0] += 1
            self.tracker.restart()
            assert not any(self.tracker.poll().states.values())
            injections.append(problem_id)
            now = datetime.now(timezone.utc)
            return InjectionWindow(now, now)

        def oracle(self):
            # Independently compare the repaired objects; no probe verdict or
            # oracle answer is offered to the scripted responder or learner.
            return OracleVerdict(kind="composition", success=cluster == good)

        def recover(self):
            assert cluster == good
            return 0.0

    driver = Driver()

    class ScriptedAgent:
        name = "sdo"
        model = "scripted-no-model"

        def resolve(self, index: int, problem_id: str, inject: Callable[[], InjectionWindow]) -> AgentOutcome:
            window = inject()
            assert driver.tracker is not None
            for key in good:
                # Read-only fault probes observe the actual object repair and
                # retain the unresolved peer until its own source is restored.
                cluster[key] = deepcopy(good[key])
                ticks[0] += 1
                sample = driver.tracker.poll()
                samples.append(sample.states)
            assert driver.tracker.all_green_now()
            now = datetime.now(timezone.utc)
            return AgentOutcome(
                injection=window,
                detected_at=now,
                resolved_at=now,
                responder_started_at=now,
                responder_finished_at=now,
                incident_id=f"scripted-{index}",
                warm_path=index > 0,
            )

        def learn(self, outcome):
            memory = repository / "scripted-experience.json"
            value = json.loads(memory.read_text()) if memory.exists() else []
            value.append(outcome.incident_id)
            memory.write_text(json.dumps(value))
            commit("record scripted experience")
            return outcome.with_learning(reflection_finished_at=datetime.now(timezone.utc))

        def prepare_recovery(self, outcome):
            assert outcome is not None

        def close(self):
            pass

    path = tmp_path / "results/incidents.jsonl"
    records = run_incidents(
        ScriptedAgent(),
        driver,
        LoopConfig(
            run_id="scripted-pair",
            problems=(problem,),
            incidents=2,
            stream=((problem,), (problem,)),
            results_path=path,
            accepted_repository=repository,
            capability_evidence=capabilities,
        ),
    )
    assert len(records) == len(load_records(path)) == 2
    assert injections == [problem, problem]
    assert all(r.oracle.success and r.recovery_oracle.success and r.error is None for r in records)
    assert records[1].memory_before_commit == records[0].memory_after_commit
    assert all(r.memory_before_commit != r.memory_after_commit and len(r.memory_ancestry) == 1 for r in records)
    assert samples[0] != samples[1]
    assert not all(samples[0].values())
    assert all(samples[1].values())
    assert cluster == good
