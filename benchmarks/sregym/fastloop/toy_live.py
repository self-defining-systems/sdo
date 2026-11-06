"""Kubectl-backed ``ScenarioOps`` and the live driver for the toy commit scenarios.

The assertions live in ``toy_scenarios``; this module only reads the lane (accepted repository, holds ConfigMap,
controller state, workloads) and pushes the scripted commit. Reads fail loudly: an unreadable lane is an error, not
an empty answer.
"""

from __future__ import annotations

import json
import subprocess
import time
from typing import TYPE_CHECKING, Any

from benchmarks.sregym.adapter import WaitKind, wait_for
from benchmarks.sregym.fastloop.toy import CONTROLLER_REPOSITORY
from benchmarks.sregym.fastloop.toy_scenarios import (
    ACCEPTED_LOG_ARGS,
    AcceptedCommit,
    HoldRecord,
    ScenarioResult,
    ScenarioTimings,
    Snapshot,
    parse_accepted_log,
    parse_holds,
    pod_failure_count,
    run_benign,
    run_broken,
    workload_digests,
)
from benchmarks.sregym.fastloop.toy_upstream import UpstreamLayout, forwarded_remote, replay_commit

if TYPE_CHECKING:
    from benchmarks.sregym.fastloop.environment import FastloopEnvironment

TASK_DATA_KEY = "agent-task.json"
HOLDS_CONFIGMAP = "sdo-upstream-holds"
STATE_CONFIGMAP = "sdo-controller-state"
CONTROLLER_SELECTOR = "job-name=sdo-controller-run"


class KubectlScenarioOps:
    """The lane as ``ScenarioOps`` sees it: cluster reads through kubectl, pushes through a port-forward."""

    def __init__(self, environment: FastloopEnvironment, layout: UpstreamLayout) -> None:
        self.environment = environment
        self.layout = layout
        self.control = environment.namespace + "-sdo"

    def clock(self) -> float:
        return time.monotonic()

    def sleep(self, seconds: float) -> None:
        time.sleep(seconds)

    def _kubectl(self, namespace: str, *args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["kubectl", "--kubeconfig", str(self.environment.kubeconfig), "-n", namespace, *args],
            capture_output=True,
            text=True,
            timeout=wait_for(WaitKind.COMMAND),
            check=check,
        )

    def _json(self, namespace: str, *args: str) -> dict[str, Any]:
        return json.loads(self._kubectl(namespace, *args, "-o", "json").stdout)

    def push(self, name: str) -> str:
        with forwarded_remote(self.environment.kubeconfig, self.layout) as remote:
            return replay_commit(name, remote, self.layout)

    def _controller_pods(self) -> list[dict[str, Any]]:
        return self._json(self.control, "get", "pods", "-l", CONTROLLER_SELECTOR)["items"]

    def _ready_controller(self) -> str:
        for pod in self._controller_pods():
            if pod.get("status", {}).get("phase") == "Running" and not pod["metadata"].get("deletionTimestamp"):
                return str(pod["metadata"]["name"])
        raise RuntimeError("no running controller pod")

    def _accepted(self, *git_args: str) -> str:
        pod = self._ready_controller()
        return self._kubectl(
            self.control, "exec", pod, "-c", "controller", "--", "git", "-C", CONTROLLER_REPOSITORY, *git_args
        ).stdout

    def accepted_commits(self) -> list[AcceptedCommit]:
        return parse_accepted_log(self._accepted(*ACCEPTED_LOG_ARGS))

    def holds(self) -> list[HoldRecord]:
        result = self._kubectl(self.control, "get", "configmap", HOLDS_CONFIGMAP, "-o", "json", check=False)
        if result.returncode != 0:
            if "NotFound" in result.stderr:
                return []
            raise RuntimeError(f"reading {HOLDS_CONFIGMAP} failed: {result.stderr.strip()}")
        return parse_holds(json.loads(result.stdout))

    def incident_open(self) -> bool:
        state = self._json(self.control, "get", "configmap", STATE_CONFIGMAP)["data"]["runtime-state.json"]
        return bool(json.loads(state).get("incident_open"))

    def agent_task_count(self) -> int:
        """Agent requests ever created in the control namespace (one request ConfigMap per task)."""
        items = self._json(self.control, "get", "configmap")["items"]
        return sum(1 for item in items if TASK_DATA_KEY in (item.get("data") or {}))

    def snapshot(self) -> Snapshot:
        namespace = self.environment.namespace
        deployments = self._json(namespace, "get", "deployments")["items"]
        images = {
            item["metadata"]["name"]: ",".join(c["image"] for c in item["spec"]["template"]["spec"]["containers"])
            for item in deployments
        }
        pods = self._json(namespace, "get", "pods")
        controllers = self._controller_pods()
        relaunches = len(controllers) + sum(
            c.get("restartCount", 0) for pod in controllers for c in pod.get("status", {}).get("containerStatuses", [])
        )
        return Snapshot(
            head=self._accepted("rev-parse", "HEAD").strip(),
            images=images,
            digests=workload_digests(pods),
            agent_tasks=self.agent_task_count(),
            pod_failures=pod_failure_count(pods),
            controller_relaunches=relaunches,
        )


def run_scenario(
    environment: FastloopEnvironment,
    layout: UpstreamLayout,
    commit: str,
    *,
    verdict_seconds: float | None = None,
    steady_seconds: float | None = None,
) -> int:
    """Drive one scripted commit against the live lane; print and record the result. Exit 0 only when it passed."""
    standard = ScenarioTimings.standard()
    timings = ScenarioTimings(
        verdict=verdict_seconds or standard.verdict, steady=steady_seconds or standard.steady, poll=standard.poll
    )
    ops = KubectlScenarioOps(environment, layout)
    before = ops.snapshot()
    started = time.time()
    result: ScenarioResult = (
        run_benign(ops, timings, commit) if commit == "benign" else run_broken(ops, timings, commit)
    )
    after = ops.snapshot()
    report = {
        "commit": commit,
        "pushed": result.pushed,
        "verdict": result.verdict,
        "passed": result.passed,
        "violations": result.violations,
        "seconds": round(time.time() - started, 1),
        "before": before.__dict__,
        "after": after.__dict__,
        "holds": [hold.__dict__ for hold in ops.holds()],
        "incident_open": ops.incident_open(),
    }
    (environment.run_dir / f"scenario-{commit}.json").write_text(json.dumps(report, indent=2, default=str) + "\n")
    print(json.dumps(report, indent=2, default=str))
    return 0 if result.passed else 1
