"""Always-run pre-injection evidence over narrow benchmark I/O boundaries."""

from __future__ import annotations

import ipaddress
import json
import subprocess
import time
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

import yaml

from benchmarks.sregym.adapter import (
    INCIDENT_EXECUTION_LABEL,
    POST_CLOSURE_WAIT_SECONDS,
    STANDBY_LABEL,
    ControllerStateUnavailableError,
    Deadline,
    StabilizationError,
    WaitKind,
    assert_host_disk_headroom,
    assert_lane_resources,
    assert_stable_workloads,
    controller_proof,
    idle_standby_jobs,
    incident_work_open,
    job_labels,
    kubectl,
    qualified_scenarios,
    queued_reflection_count,
    setup_lineage,
    shell_runner_proof,
    wait_for,
    wait_for_proof,
)
from benchmarks.sregym.fastloop.clean_start import assert_clean_baseline

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from benchmarks.sregym.adapter import ClusterOps, NativeInstallConfig
    from benchmarks.sregym.fastloop.environment import FastloopEnvironment
    from benchmarks.sregym.fastloop.worker_client import SregymWorker


def workload_identity(kubeconfig: Path, namespace: str) -> dict[str, Any]:
    listing = json.loads(
        kubectl(
            ["get", "deployments,statefulsets,daemonsets,pods", "-o", "json"],
            namespace=namespace,
            kubeconfig=kubeconfig,
        ).stdout
    )["items"]
    workloads = {}
    pods = [row for row in listing if row["kind"] == "Pod" and not row["metadata"].get("deletionTimestamp")]
    for row in listing:
        if row["kind"] == "Pod":
            continue
        metadata, spec = row["metadata"], row["spec"]
        selector = spec.get("selector", {}).get("matchLabels", {})
        selected = [
            pod
            for pod in pods
            if selector and all(pod["metadata"].get("labels", {}).get(key) == val for key, val in selector.items())
        ]
        status = row.get("status", {})
        desired = spec.get("replicas", 1)
        if row["kind"] == "DaemonSet":
            desired = status.get("desiredNumberScheduled", 0)
            updated, current, ready = (
                status.get("updatedNumberScheduled"),
                status.get("currentNumberScheduled"),
                status.get("numberReady", 0),
            )
        else:
            updated, current, ready = (
                status.get("updatedReplicas"),
                status.get("replicas"),
                status.get("readyReplicas", 0),
            )
        observed, generation = status.get("observedGeneration", -1), metadata.get("generation", 0)
        label = f"{row['kind']}/{metadata['name']}"
        # A rollout is a template change still in progress; an unchanged
        # template whose pods lost readiness (crash, eviction, responder
        # restart) is a different cause and is named as such.
        if (
            observed < generation
            or updated != desired
            or current != desired
            or (row["kind"] == "StatefulSet" and status.get("currentRevision") != status.get("updateRevision"))
        ):
            raise ValueError(
                f"restore invariant: rollout not complete: {label} "
                f"(generation {generation} observed {observed}; updated {updated}/{desired}; current {current})"
            )
        if ready != desired or len(selected) != desired:
            pods = ", ".join(
                f"{pod['metadata'].get('name', pod['metadata'].get('uid'))} {pod.get('status', {}).get('phase')}"
                for pod in selected
            )
            raise ValueError(f"restore invariant: workload not Ready: {label} (ready {ready}/{desired}; pods: {pods})")
        if desired == 0:
            raise ValueError(f"restore invariant: installed workload scaled to zero: {row['kind']}/{metadata['name']}")
        if not selected or any(
            not any(
                condition.get("type") == "Ready" and condition.get("status") == "True"
                for condition in pod.get("status", {}).get("conditions", [])
            )
            for pod in selected
        ):
            raise ValueError(f"restore invariant: workload not Ready: {row['kind']}/{metadata['name']}")
        workloads[f"{row['kind']}/{metadata['name']}"] = {
            "generation": metadata.get("generation"),
            "pods": sorted(
                (
                    pod["metadata"]["uid"],
                    tuple(
                        status.get("restartCount", 0) for status in pod.get("status", {}).get("containerStatuses", [])
                    ),
                )
                for pod in selected
            ),
        }
    if not workloads:
        raise ValueError("restore invariant: no installed application workloads")
    return workloads


def active_incident(state: dict[str, Any]) -> str | None:
    """Name controller incident work, so a workload refusal shows SDO acted before injection."""
    if not incident_work_open(state):
        return None
    request = state.get("incident_request")
    incident = request.get("incident_id") if isinstance(request, dict) else None
    keys = ", ".join(key for key in ("incident_open", "incident_request", "pending_closure") if state.get(key))
    return f"{incident or 'unidentified'} ({keys})"


class PriorClosureIncompleteError(ValueError):
    """A previous incident on the lane had not closed naturally; the row is not measured."""

    reason = "prior_closure_incomplete"

    def __init__(self, message: str, evidence: dict[str, Any]) -> None:
        super().__init__(message)
        self.evidence = evidence


class _IncidentWorkStarted(Exception):
    """SDO opened incident work during stabilization; admission waits for it to close, then restarts the clock."""


#: Work first seen sets the prior-closure budget; Jobs seen later may extend it to at most this multiple.
PRIOR_CLOSURE_CEILING_FACTOR = 2

#: Stabilization restarts after new SDO work at most this often; a lane that keeps opening incidents is not stable.
MAX_STABILIZATION_RESTARTS = 3


def _job_deadline(job: dict[str, Any], now: datetime) -> datetime | None:
    """When a worker Job's own ``activeDeadlineSeconds`` ends it; ``None`` when it has no deadline."""
    seconds = job.get("spec", {}).get("activeDeadlineSeconds")
    if not isinstance(seconds, int):
        return None
    started = job.get("status", {}).get("startTime")
    start = datetime.fromisoformat(started.replace("Z", "+00:00")) if isinstance(started, str) else now
    return start + timedelta(seconds=seconds)


def _accepted_head(workspace: Path) -> str | None:
    completed = subprocess.run(
        ["git", "-C", str(workspace), "rev-parse", "HEAD"], capture_output=True, text=True, check=False
    )
    return completed.stdout.strip() or None if completed.returncode == 0 else None


class LaneAdmission:
    def __init__(
        self,
        environment: FastloopEnvironment,
        worker: SregymWorker,
        ops: ClusterOps,
        directory: Path,
        *,
        timeout_seconds: float = int(wait_for(WaitKind.ROLLOUT)),
        onboarding_timeout_seconds: float = int(wait_for(WaitKind.OPERATION)),
        prior_closure_timeout_seconds: float = int(wait_for(WaitKind.RESPONSE)),
        code_path: str = "go",
        reflection_attempt_seconds: float = wait_for(WaitKind.REFLECTION_ATTEMPT),
        install_config: NativeInstallConfig | None = None,
        synchronize: Callable[[], None] | None = None,
        progress: Callable[..., None] | None = None,
        before_proof: Callable[[str, dict[str, Any]], None] | None = None,
    ) -> None:
        if code_path not in {"go", "shell"}:
            raise ValueError(f"eval: unsupported admission code path {code_path!r}")
        self.code_path = code_path
        self.environment, self.worker, self.ops = environment, worker, ops
        self.directory = directory
        self.timeout_seconds = timeout_seconds
        self.onboarding_timeout_seconds = onboarding_timeout_seconds
        self.prior_closure_timeout_seconds = prior_closure_timeout_seconds
        self.reflection_attempt_seconds = reflection_attempt_seconds
        self.install_config = install_config
        self.synchronize, self.progress = synchronize, progress
        self.before_proof = before_proof
        self.required_detectors = {"workload-health", "service-endpoints", "service-reachability", "traffic-health"}
        self.required_scenarios: set[str] = set()
        self._identity: dict[str, Any] | None = None
        self._sequence = 0

    def _policy(self) -> None:
        if self.code_path == "shell":
            self.required_detectors = {"shell.detectors"}
            self.required_scenarios = set()
            return
        root = self.environment.workspace / ".sdo/diagnostics"
        self.required_detectors.update(path.parent.name for path in (root / "detectors/health").glob("*/manifest.yaml"))
        self.required_scenarios = set()
        for path in (root / "traffic/workloads").glob("*.yaml"):
            document = yaml.safe_load(path.read_text())
            if document.get("purpose") == "health-probe":
                self.required_scenarios.update(row["id"] for row in document.get("scenarios", []))
        if not self.required_scenarios:
            raise ValueError("eval: accepted health policy declares no synthetic traffic scenarios")

    def _windows(self, control: str) -> dict[str, Any]:
        env = self.environment
        pod = json.loads(
            kubectl(["get", "pod/sdo-prober", "-o", "json"], namespace=control, kubeconfig=env.kubeconfig).stdout
        )
        address = ipaddress.ip_address(pod["status"]["podIP"])
        host = f"[{address}]" if address.version == 6 else str(address)
        code = f"import urllib.request; print(urllib.request.urlopen('http://{host}:8080/v1/windows',timeout=5).read().decode())"
        return json.loads(
            kubectl(
                ["exec", "job/sdo-controller-run", "-c", "controller", "--", "python3", "-c", code],
                namespace=control,
                kubeconfig=env.kubeconfig,
            ).stdout
        )

    def _shell_results(self, control: str) -> dict[str, Any]:
        """Read runner health and executable policy from the controller's accepted HEAD."""
        env = self.environment
        job = json.loads(
            kubectl(
                ["get", "job/sdo-controller-run", "-o", "json"], namespace=control, kubeconfig=env.kubeconfig
            ).stdout
        )
        containers = job["spec"]["template"]["spec"]["containers"]
        controller = next(row for row in containers if row["name"] == "controller")
        args = controller["args"]
        repository = args[args.index("--app") + 1]
        if not repository.startswith("/"):
            raise ValueError("eval: installed accepted repository path must be absolute")
        code = (
            "import json, subprocess, urllib.request; "
            f"root={repository!r}; "
            "git=lambda *args: subprocess.check_output(['git','-C',root,*args]); "
            "head=git('rev-parse','HEAD').decode().strip(); "
            "entries=git('ls-tree','-r','-z',head,'--','.sdo/diagnostics/detectors').split(bytes([0])); "
            "scripts=[entry.split(bytes([9]),1)[1].decode() for entry in entries "
            "if entry.startswith(b'100755 blob ') and entry.endswith(b'.sh')]; "
            "report=json.load(urllib.request.urlopen('http://sdo-shell-detectors:8080/results',timeout=5)); "
            "assert git('rev-parse','HEAD').decode().strip()==head, 'accepted HEAD changed during shell observation'; "
            "print(json.dumps({'accepted_commit':head,'scripts':scripts,'report':report}))"
        )
        return json.loads(
            kubectl(
                ["exec", "job/sdo-controller-run", "-c", "controller", "--", "python3", "-c", code],
                namespace=control,
                kubeconfig=env.kubeconfig,
            ).stdout
        )

    def _onboarding(self, control: str) -> dict[str, dict[str, Any] | None]:
        """Read the controller's pending and completed onboarding records."""
        records: dict[str, dict[str, Any] | None] = {}
        for key, name in (("complete", "sdo-namespace-onboarding"), ("pending", "sdo-namespace-onboarding-pending")):
            output = kubectl(
                ["get", f"configmap/{name}", "-o", "json", "--ignore-not-found=true"],
                namespace=control,
                kubeconfig=self.environment.kubeconfig,
            ).stdout
            records[key] = json.loads(json.loads(output)["data"]["onboarding.json"]) if output.strip() else None
        return records

    def await_onboarding(self, control: str) -> dict[str, Any]:
        """Wait, on a separate recorded budget, until the lane's SDO onboarding has finished.

        A fresh seed runs install-existing and the first health refresh after
        resume. The controller publishes the completed onboarding record only
        after that refresh and its clear first evaluation, so the record proves
        the controller is observing; only then may the stabilization clock start.
        """
        started = time.monotonic()
        self.directory.mkdir(parents=True, exist_ok=True)
        destination = self.directory / "onboarding.json"
        while True:
            try:
                records, error = self._onboarding(control), None
            except (RuntimeError, ValueError, KeyError) as exc:
                records, error = {"complete": None, "pending": None}, str(exc)
            record = records["complete"] or {}
            done = bool(record.get("acceptedCommit") and record.get("objects"))
            evidence = {
                "passed": done,
                "accepted_commit": record.get("acceptedCommit"),
                "pending": records["pending"] is not None,
                "last_error": error,
                "seconds": round(time.monotonic() - started, 3),
                "budget_seconds": self.onboarding_timeout_seconds,
            }
            if done or time.monotonic() - started >= self.onboarding_timeout_seconds:
                break
            time.sleep(min(2, max(0, started + self.onboarding_timeout_seconds - time.monotonic())))
        destination.write_text(json.dumps(evidence, indent=2) + "\n")
        if not done:
            raise ValueError(
                f"eval: SDO onboarding did not finish within {self.onboarding_timeout_seconds:g}s "
                f"(completed record: {bool(record)}, pending record: {evidence['pending']}, "
                f"last error: {error}); no injection"
            )
        if self.progress:
            self.progress("onboarding", accepted_commit=evidence["accepted_commit"], evidence=str(destination))
        return evidence

    def _active_workers(self, control: str) -> list[str]:
        """Name SDO worker Jobs (responder, reflection) that have not finished.

        The controller's warm responder standby runs until an incident claims
        it, so an unclaimed standby is idle capacity, not work.
        """
        return sorted(job["metadata"]["name"] for job in self._active_jobs(control))

    def _work_deadline(self, control: str) -> float | None:
        """Monotonic time by which every active SDO worker Job has hit its own deadline (durable Job spec)."""
        now = datetime.now(timezone.utc)
        deadlines = [deadline for job in self._active_jobs(control) if (deadline := _job_deadline(job, now))]
        if not deadlines:
            return None
        return time.monotonic() + (max(deadlines) - now).total_seconds()

    def _active_jobs(self, control: str) -> list[dict[str, Any]]:
        listing = json.loads(
            kubectl(["get", "jobs", "-o", "json"], namespace=control, kubeconfig=self.environment.kubeconfig).stdout
        )["items"]
        jobs = [
            job
            for job in listing
            if job_labels(job).get("app.kubernetes.io/name") == "sdo-worker"
            and not any(
                row.get("type") in {"Complete", "Failed"} and row.get("status") == "True"
                for row in job.get("status", {}).get("conditions") or []
            )
        ]
        idle: set[str] = set()
        if any(job_labels(job).get(STANDBY_LABEL) == "true" for job in jobs):
            executions = json.loads(
                kubectl(
                    ["get", "configmaps", "-l", INCIDENT_EXECUTION_LABEL, "-o", "json"],
                    namespace=control,
                    kubeconfig=self.environment.kubeconfig,
                ).stdout
            )["items"]
            idle = idle_standby_jobs(jobs, executions)
        return [job for job in jobs if job["metadata"]["name"] not in idle]

    def await_prior_closure(self, control: str) -> dict[str, Any]:
        """Wait, on a separate recorded budget, until every previous incident on the lane has closed naturally.

        A repeat must start on the memory the previous row's reflection
        accepted, and its admission clock must not contain that reflection.
        Closed means ``incident_work_open`` is false (no open incident or
        request, and any closure is terminal: acknowledged, permanently
        failed or retired) and no SDO worker Job is still running. The budget
        is never shorter than the work first seen: the first poll extends it to
        the latest active worker Job's own deadline plus the closure margin.
        Jobs that start later may extend it only up to twice that original
        budget, so a lane that keeps opening incidents cannot hold admission
        forever. Each queued reflection adds SDO's worst case for one
        reflection, which no ceiling cuts. The wait never aborts or reuses
        running work; when the budget ends the row is not measured.
        """
        started = time.monotonic()
        self.directory.mkdir(parents=True, exist_ok=True)
        destination = self.directory / "prior-closure.json"
        expires = started + self.prior_closure_timeout_seconds
        ceiling: float | None = None
        while True:
            try:
                state, workers, error = self.ops.runtime_state(control), self._active_workers(control), None
                work_deadline = self._work_deadline(control) if workers else None
                if not state:
                    error = "empty controller state"
            except (RuntimeError, ValueError, KeyError) as exc:
                state, workers, work_deadline, error = {}, [], None, str(exc)
            if work_deadline is not None:
                expires = max(expires, work_deadline + POST_CLOSURE_WAIT_SECONDS)
            # SDO runs queued reflections one at a time, each up to its own worst case.
            queue_budget = started + queued_reflection_count(state) * self.reflection_attempt_seconds
            expires = max(expires, queue_budget)
            if ceiling is None:
                ceiling = started + PRIOR_CLOSURE_CEILING_FACTOR * (expires - started)
            ceiling = max(ceiling, queue_budget)
            expires = min(expires, ceiling)
            closure_state = state.get("closure_state")
            done = error is None and not incident_work_open(state) and not workers
            evidence = {
                "passed": done,
                "closure_state": closure_state,
                "incident_work": active_incident(state),
                "last_acknowledged_incident_id": state.get("last_acknowledged_incident_id") or None,
                "active_workers": workers,
                "last_error": error,
                "seconds": round(time.monotonic() - started, 3),
                "budget_seconds": round(expires - started, 3),
            }
            if done or time.monotonic() >= expires:
                break
            time.sleep(min(2, max(0, expires - time.monotonic())))
        # Recorded after the wait, so a repeat names the memory its predecessor's reflection accepted.
        evidence["accepted_before"] = _accepted_head(self.environment.workspace)
        destination.write_text(json.dumps(evidence, indent=2) + "\n")
        if not done:
            failure = ControllerStateUnavailableError if error is not None else PriorClosureIncompleteError
            raise failure(
                f"eval: previous incident closure did not finish within {evidence['budget_seconds']:g}s "
                f"(closure state: {closure_state}, incident work: {evidence['incident_work']}, "
                f"active workers: {workers}, last error: {error}); row not measured, no injection",
                evidence,
            )
        if self.progress:
            self.progress("prior_closure", evidence=str(destination))
        return evidence

    def check_resources(self, stage: str) -> None:
        self.directory.mkdir(parents=True, exist_ok=True)
        try:
            evidence = assert_lane_resources(self.environment.kubeconfig, app_namespaces={self.environment.namespace})
            evidence["host_disk"] = assert_host_disk_headroom()
        except ValueError as exc:
            if self.progress:
                self.progress("lane_resources", passed=False, reason=str(exc))
            raise
        destination = self.directory / "lane-resources.json"
        destination.write_text(json.dumps(evidence, indent=2) + "\n")
        if self.progress:
            self.progress("lane_resources", evidence=str(destination))

    def prove(self, control: str, *, fresh: bool = False) -> dict[str, Any]:
        self.check_resources("entry")
        self._policy()
        # Onboarding has its own budget: the stabilization clock below must never
        # contain install-existing or the first health refresh of a fresh seed.
        onboarding = self.await_onboarding(control) if self.code_path == "go" else {}
        # A previous row's closure and reflection have their own budget too: a
        # repeat starts only on the memory that reflection accepted.
        self.await_prior_closure(control)
        if self.before_proof is not None:
            self.before_proof(control, onboarding)
        for _ in range(MAX_STABILIZATION_RESTARTS):
            try:
                return self._stabilize(control, fresh=fresh)
            except _IncidentWorkStarted:
                # Work SDO opened during stabilization gets the closure wait's derived budget,
                # never the fixed stabilization budget; the stabilization clock then restarts.
                self.await_prior_closure(control)
        try:
            return self._stabilize(control, fresh=fresh)
        except _IncidentWorkStarted as exc:
            raise StabilizationError(
                f"eval: SDO opened incident work during each of {MAX_STABILIZATION_RESTARTS + 1} stabilization "
                f"windows ({exc}); no injection"
            ) from exc

    def _stabilize(self, control: str, *, fresh: bool) -> dict[str, Any]:
        from benchmarks.sregym.fastloop.rbac import verify_rbac

        deadline = Deadline(time.monotonic() + self.timeout_seconds, time.monotonic)
        env = self.environment
        self.directory.mkdir(parents=True, exist_ok=True)
        rbac = verify_rbac(env.kubeconfig, env.namespace, self.install_config)
        (self.directory / "rbac.json").write_text(json.dumps(rbac, indent=2) + "\n")
        boundary = datetime.now(timezone.utc).isoformat()
        if self.progress:
            self.progress("rbac_equality", evidence=str(self.directory / "rbac.json"))
        self._identity = workload_identity(env.kubeconfig, env.namespace)
        self._sequence += 1
        path = self.directory / f"stabilization-{self._sequence:03d}.jsonl"

        def stable_workloads() -> None:
            try:
                assert_stable_workloads(self._identity or {}, workload_identity(env.kubeconfig, env.namespace))
            except ValueError as exc:
                incident = active_incident(self.ops.runtime_state(control))
                if incident is None:
                    raise
                raise ValueError(f"{exc}; SDO incident work active before injection: {incident}") from exc

        def sample() -> dict[str, Any]:
            assert_clean_baseline(env.kubeconfig, namespace=env.namespace)
            if self.progress:
                self.progress("probe_placement", namespace="sregym")
            stable_workloads()
            state = self.ops.runtime_state(control)
            proof = controller_proof(state, self.required_detectors, boundary)
            proof["code_path"] = self.code_path
            if proof["incident_work_pending"]:
                proof.update(required_scenarios=sorted(self.required_scenarios), suspended="incident_work_started")
                record(proof)
                raise _IncidentWorkStarted(active_incident(state) or "incident work")
            if self.code_path == "shell":
                try:
                    runner = shell_runner_proof(
                        self._shell_results(control), boundary, datetime.now(timezone.utc).isoformat()
                    )
                except (RuntimeError, ValueError, KeyError, IndexError, StopIteration) as exc:
                    runner = {"passed": False, "reason": str(exc)}
                proof["shell_runner"] = runner
                proof["passed"] = proof["passed"] and runner["passed"]
            else:
                try:
                    windows = self._windows(control)
                    proof["qualified_scenarios"] = qualified_scenarios(windows, self.required_scenarios)
                except (RuntimeError, ValueError, KeyError) as exc:
                    proof.update(qualified_scenarios=False, prober_error=str(exc))
                proof["passed"] = proof["passed"] and proof["qualified_scenarios"]
            if proof["passed"]:
                traffic = self.worker.request(
                    "baseline_oracle",
                    problem_id=env.deploy_problem,
                    verify_traffic=True,
                    timeout=deadline.remaining(),
                )
                proof["independent_traffic"] = traffic
                proof["passed"] = traffic.get("success") is True
                stable_workloads()
            proof["required_scenarios"] = sorted(self.required_scenarios)
            return proof

        def record(proof: dict[str, Any]) -> None:
            with path.open("a") as output:
                output.write(json.dumps(proof) + "\n")

        proof = wait_for_proof(
            sample, record, timeout_seconds=self.timeout_seconds, deadline=deadline, monotonic=time.monotonic
        )
        if self.progress:
            self.progress("stabilization", evidence=str(path))
        if fresh:
            if self.synchronize is None:
                raise ValueError("eval: fresh-memory proof requires synchronization from the accepted repository")
            self.synchronize()
            lineage = setup_lineage(env.workspace, env.seed_commit or "HEAD")
            destination = self.directory / "setup-lineage.json"
            destination.write_text(json.dumps(lineage, indent=2) + "\n")
            if self.progress:
                self.progress("setup_memory", evidence=str(destination))
        from benchmarks.sregym.fastloop.hotel_helpers import admit_install

        helper_evidence = admit_install(env.kubeconfig, env.namespace)
        (self.directory / "hotel-script-profile.json").write_text(json.dumps(helper_evidence, indent=2) + "\n")
        self.check_resources("injection")
        deadline.remaining()
        return proof


class EvalAdmissionOps:
    """Prove health while ACTIVE, before either ordinary or simultaneous injection."""

    def __init__(self, inner: ClusterOps, admission: LaneAdmission, *, fresh: bool = False) -> None:
        self._inner, self.admission, self._fresh = inner, admission, fresh

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    def inject_after_resume(
        self, control_namespace: str, generation: str, inject: Callable[[], None]
    ) -> dict[str, float]:
        started = time.monotonic()
        self._inner.set_maintenance(control_namespace, paused=False, generation=generation + "-proof")
        self.admission.prove(control_namespace, fresh=self._fresh)
        self._fresh = False
        seconds = time.monotonic() - started
        result = dict(self._inner.inject_after_resume(control_namespace, generation, inject))
        result["traffic_stabilization"] = seconds
        return result
