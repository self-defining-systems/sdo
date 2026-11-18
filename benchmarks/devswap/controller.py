"""The SDO controller on a lane's cluster: stop and start it, its responder standby, and its accepted repository.

Everything here talks to the control namespace through ``kubectl`` and names nothing of a benchmark: the controller Job
(``sdo-controller-run``), the standby Jobs it creates (``sdo-standby-*``) and the accepted repository it mounts.
"""

from __future__ import annotations

import json
import re
import subprocess
import time
from contextlib import contextmanager
from pathlib import Path  # noqa: TC003
from typing import TYPE_CHECKING, Any

from benchmarks.devswap.kube import APPLICATION_ROOT, CONTROLLER_CONTAINER, CONTROLLER_JOB, Kubectl, SwapError, run

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping

_COMMIT = re.compile(r"^[0-9a-f]{40}$")

#: How long a controller that was just (re)started gets to bring up its ready standby. It is a bound, not a delay: a
#: lane that is ready returns at once. 180 s was too short on a loaded host (three claim swaps failed on it with the
#: controller still starting, and the lane was then torn down).
SWAP_STANDBY_WAIT_S = 600.0


def controller_pod_ids(kube: Kubectl, control: str) -> set[str]:
    """The uids of the controller pods that run or are about to (not ended, not being deleted). A pod list kubectl could
    not read is an error: an empty answer would read as "no controller to restart"."""
    pods = kube.read_controller_pods(control)
    if pods is None:
        raise SwapError(f"could not read the controller pods in {control}")
    return {
        pod["metadata"]["uid"]
        for pod in pods
        if not (pod.get("metadata") or {}).get("deletionTimestamp")
        and (pod.get("status") or {}).get("phase") not in ("Succeeded", "Failed")
    }


def set_controller_suspended(kube: Kubectl, control: str, suspended: bool, timeout: float = 300.0) -> None:
    """Suspend or resume the controller Job. Suspending ends its pod (SIGTERM: the evidence sidecar's final pass runs),
    waits until none is left and frees the leader Lease the ended process still names."""
    run(
        kube.cmd("-n", control, "patch", "job", CONTROLLER_JOB, "-p", json.dumps({"spec": {"suspend": suspended}})),
        "suspend the controller" if suspended else "resume the controller",
        60,
    )
    if not suspended:
        return
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not kube.controller_pods(control):
            release_leader_lease(kube, control)
            return
        time.sleep(2)
    raise SwapError(f"the controller pod did not end within {timeout:.0f}s")


#: The controller's leader Lease (``controllerLeaseName`` in ``sdo/operator/runtime``), 60 s long.
CONTROLLER_LEASE = "sdo-controller"


def release_leader_lease(kube: Kubectl, control: str) -> None:
    """Free the leader Lease the stopped controller still holds. The process is killed when its pod ends, before its own
    release runs, and the controller that starts next waits out the rest of the 60 s (measured: 41-60 s of every
    restart on a lane). With no controller process left nobody holds the Lease for real; the next controller creates
    it again at once."""
    run(
        kube.cmd("-n", control, "delete", "lease", CONTROLLER_LEASE, "--ignore-not-found", "--wait=false"),
        "release the controller's leader Lease",
        60,
    )


def standby_jobs(kube: Kubectl, control: str) -> list[str]:
    proc = subprocess.run(
        kube.cmd("-n", control, "get", "jobs", "-o", "name"), capture_output=True, text=True, timeout=60, check=False
    )
    return [line for line in proc.stdout.split() if "sdo-standby-" in line]


def retire_standby(kube: Kubectl, control: str) -> None:
    for job in standby_jobs(kube, control):
        run(kube.cmd("-n", control, "delete", job, "--wait=false"), f"delete {job}", 60)


def standby_ready(pods: list[dict[str, Any]]) -> bool:
    """Whether one of ``pods`` is a responder standby that is Ready and not being deleted (a retired standby stays Ready
    until its container stops)."""
    for pod in pods:
        meta = pod.get("metadata") or {}
        if "sdo-standby-" not in meta.get("name", "") or meta.get("deletionTimestamp"):
            continue
        if any(
            c.get("type") == "Ready" and c.get("status") == "True"
            for c in (pod.get("status") or {}).get("conditions", [])
        ):
            return True
    return False


def wait_standby(kube: Kubectl, control: str, timeout: float) -> None:
    """Wait until the controller is ready and a standby Job's pod is Ready (the controller recreates the standby it
    lost)."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if standby_jobs(kube, control) and kube.controller_ready(control):
            proc = subprocess.run(
                kube.cmd("-n", control, "get", "pods", "-o", "json"),
                capture_output=True,
                text=True,
                timeout=60,
                check=False,
            )
            if standby_ready(json.loads(proc.stdout or "{}").get("items", [])):
                return
        time.sleep(2)
    raise SwapError(f"no ready responder standby within {timeout:.0f}s")


def is_ancestor(kube: Kubectl, control: str, ancestor: str, descendant: str) -> bool:
    """Whether ``ancestor`` is in the history of ``descendant`` in the controller's accepted repository."""
    script = (
        f"cd {APPLICATION_ROOT} && git -c safe.directory='*' merge-base --is-ancestor {ancestor} {descendant} "
        "&& echo yes || echo no"
    )
    return kube.exec_controller(control, script).decode().strip() == "yes"


def repository_state(kube: Kubectl, control: str) -> dict[str, str]:
    """The accepted repository as the controller holds it: head, uncommitted files and the outcome files."""
    script = (
        f"cd {APPLICATION_ROOT} && git -c safe.directory='*' rev-parse HEAD && echo -- && "
        "git -c safe.directory='*' status --porcelain && echo -- && ls .sdo/outcomes 2>/dev/null; true"
    )
    head, status, outcomes = (part.strip() for part in kube.exec_controller(control, script).decode().split("--\n"))
    return {"head": head, "status": status, "outcomes": outcomes}


def wipe_problems(before: Mapping[str, str], now: Mapping[str, str]) -> list[str]:
    """Why a lane is not what ``prepare`` left (``before``): the accepted head moved, files are uncommitted or
    outcomes were written. A swapped lane must start every cell from the same memory as a fresh one."""
    problems = []
    if now["head"] != before["head"]:
        problems.append(f"accepted head {now['head'][:12]} is not the prepared {before['head'][:12]}")
    if now["status"]:
        problems.append("uncommitted files in the accepted repository")
    if now["outcomes"] != before["outcomes"]:
        problems.append("outcomes changed since prepare")
    return problems


@contextmanager
def controller_stopped(kube: Kubectl, control: str) -> Iterator[None]:
    """The controller Job suspended for the block and resumed after it, also when the stop or the block fails: a
    stopped controller cannot be asked for anything, so no failure may leave it suspended."""
    try:
        set_controller_suspended(kube, control, True)
        yield
    finally:
        set_controller_suspended(kube, control, False)


#: Where the restore pod receives a memory bundle: its own emptyDir (the controller image has a read-only root).
BUNDLE_DIR = "/seed"


def restore_script(
    head: str, with_bundle: bool, root: str = APPLICATION_ROOT, bundle_dir: str = BUNDLE_DIR, broker_state: str = ""
) -> str:
    """The restore pod's shell: with a bundle, wait for it and unbundle its objects (whatever its refs are named), then
    put the repository at ``head`` with no other file. ``broker_state`` is the broker's worktree root: a memory with
    another history (a seed) leaves it describing the old one (a resumed proposal whose base is not an ancestor of
    the new head), so it is emptied, as on a fresh install."""
    git = "git -c safe.directory='*'"
    steps = [f"cd {root}"]
    if with_bundle:
        # the pod waits for the bundle, streamed in by exec (the image need not hold tar for a copy)
        steps.insert(0, f"until [ -e {bundle_dir}/memory.ready ]; do sleep 1; done")
        steps.append(f"{git} bundle unbundle {bundle_dir}/memory.bundle >/dev/null")
    if broker_state:
        steps.append(f"rm -rf {broker_state}/* {broker_state}/.[!.]*")
    steps += [f"{git} reset -q --hard {head}", f"{git} clean -q -fdx", f"{git} rev-parse HEAD"]
    return " && ".join(steps)


def restore_repository(kube: Kubectl, control: str, head: str, bundle: Path | None = None) -> None:
    """With the controller stopped, put the accepted repository on its volume back at ``head`` with no other file
    (outcomes and playbooks it learned go), in a short-lived pod that mounts the volume as the controller does.
    With ``bundle`` the commit comes from that git bundle (a learned memory), fetched into the repository first."""
    if not _COMMIT.match(head):
        raise SwapError(f"prepared head {head!r} is not a commit")
    job = kube.get_json("-n", control, "get", "job", CONTROLLER_JOB) or {}
    spec = ((job.get("spec") or {}).get("template") or {}).get("spec") or {}
    container = next((c for c in spec.get("containers", []) if c.get("name") == CONTROLLER_CONTAINER), None)
    if container is None:
        raise SwapError(f"no controller container in job {CONTROLLER_JOB}")
    volume = next(
        (v for v in spec.get("volumes", []) if v.get("persistentVolumeClaim") and v["name"] == "repository"), None
    )
    mount = next((m for m in container.get("volumeMounts", []) if m.get("name") == "repository"), None)
    if volume is None or mount is None:
        raise SwapError("the controller job has no repository volume")
    name = "sdo-restore-repository"
    script = restore_script(
        head, bundle is not None, broker_state=f"{mount['mountPath']}/worktrees" if bundle is not None else ""
    )
    pod = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": name, "namespace": control},
        "spec": {
            "restartPolicy": "Never",
            "securityContext": spec.get("securityContext") or {},
            "containers": [
                {
                    "name": "restore",
                    "image": container["image"],
                    "imagePullPolicy": "IfNotPresent",
                    "command": ["sh", "-c", script],
                    "securityContext": container.get("securityContext") or {},
                    "volumeMounts": [mount, {"name": "seed", "mountPath": BUNDLE_DIR}],
                }
            ],
            "volumes": [volume, {"name": "seed", "emptyDir": {}}],
        },
    }
    run(
        kube.cmd("-n", control, "delete", "pod", name, "--ignore-not-found", "--wait=true"),
        "delete old restore pod",
        120,
    )
    run(kube.cmd("apply", "-f", "-"), "restore pod", 60, input=json.dumps(pod))
    try:
        if bundle is not None:
            send_bundle(kube, control, name, bundle)
        deadline = time.monotonic() + 180
        while time.monotonic() < deadline:
            proc = subprocess.run(
                kube.cmd("-n", control, "get", "pod", name, "-o", "jsonpath={.status.phase}"),
                capture_output=True,
                text=True,
                timeout=60,
                check=False,
            )
            if proc.stdout.strip() == "Succeeded":
                logs = run(kube.cmd("-n", control, "logs", name), "restore pod log", 60).stdout.strip()
                if logs.splitlines()[-1:] != [head]:
                    raise SwapError(f"repository is at {logs[-50:]!r}, not {head}")
                return
            if proc.stdout.strip() == "Failed":
                logs = subprocess.run(
                    kube.cmd("-n", control, "logs", name), capture_output=True, text=True, check=False
                )
                raise SwapError(f"repository restore failed: {(logs.stdout + logs.stderr).strip()[-300:]}")
            time.sleep(2)
        raise SwapError("repository restore pod did not finish in 180s")
    finally:
        subprocess.run(
            kube.cmd("-n", control, "delete", "pod", name, "--ignore-not-found", "--wait=false"),
            capture_output=True,
            check=False,
        )


def send_bundle(kube: Kubectl, control: str, pod: str, bundle: Path) -> None:
    """Stream ``bundle`` into the running restore pod at BUNDLE_DIR/memory.bundle and tell it to go on."""
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        phase = subprocess.run(
            kube.cmd("-n", control, "get", "pod", pod, "-o", "jsonpath={.status.phase}"),
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        ).stdout.strip()
        if phase == "Running":
            break
        time.sleep(1)
    else:
        raise SwapError(f"restore pod {pod} did not start in 120s")
    with bundle.open("rb") as handle:
        proc = subprocess.run(
            kube.cmd(
                "-n",
                control,
                "exec",
                "-i",
                pod,
                "--",
                "sh",
                "-c",
                f"cat > {BUNDLE_DIR}/memory.bundle && touch {BUNDLE_DIR}/memory.ready",
            ),
            stdin=handle,
            capture_output=True,
            timeout=600,
            check=False,
        )
    if proc.returncode != 0:
        raise SwapError(f"sending the memory bundle failed: {proc.stderr.decode()[-300:]}")
