"""A real controller opens an incident; its deterministic worker waits for abort."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from benchmarks.sregym.adapter.native_operator import NativeInstallConfig, install_existing
from benchmarks.sregym.adapter.persistent import CleanupTarget, ControllerRecord, KubectlClusterOps, PersistentState
from benchmarks.sregym.adapter.processes import read_registry

ROOT = Path(__file__).resolve().parents[4]


def command(*args: str) -> str:
    return subprocess.run(args, capture_output=True, text=True, check=True, timeout=180).stdout


def kube(*args: str) -> str:
    return command("kubectl", "--kubeconfig", os.environ["KUBECONFIG"], *args)


def wait_for(predicate, seconds=300):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        result = predicate()
        if result:
            return result
        time.sleep(0.5)
    raise RuntimeError("scripted abort smoke timed out")


def worker(args, state_path, app, control):
    # Only these two registrations are installed; their command is sleep, with
    # no model executable or provider environment anywhere on this path.
    install_existing(
        NativeInstallConfig(
            repository=args.run_dir / "application",
            namespace=app,
            control_namespace=control,
            application="toy",
            controller_image=args.controller_image,
            responder_image=args.worker_image,
            validator_image=args.controller_image,
            repository_pvc="sdo-application-repository",
            credentials_secret="sdo-agent-credentials",
            model="scripted-no-model",
            kubeconfig=Path(os.environ["KUBECONFIG"]),
            registration_configmap="sdo-agent-registrations",
            repair_policy="recorded-actions",
            reflection_session="fresh",
            deployed_manifest_paths=("kubernetes",),
        )
    )
    ops = KubectlClusterOps()
    pod = wait_for(lambda: ops.controller_pod(control))
    state = PersistentState.load(state_path)
    state.controllers[app] = ControllerRecord(
        namespace=app,
        control_namespace=control,
        application="toy",
        lifecycle_fingerprint="abort-smoke",
        controller_pod_uid=pod.uid,
        controller_pod_name=pod.name,
        installed_by_stage="scripted",
        installed_at=datetime.now(timezone.utc),
        kubeconfig=os.environ["KUBECONFIG"],
        runtime_config={},
    )
    state.save(state_path)

    def healthy():
        observed = ops.runtime_state(control)
        return (
            observed.get("evaluation_iteration", 0) > 0
            and not observed.get("incident_open")
            and observed.get("history")
            and all(item.get("status") == "clear" for item in observed["history"][-2:])
        )

    wait_for(healthy)
    kube("-n", app, "scale", "deployment/frontend", "--replicas=0")

    def incident_running():
        observed = ops.runtime_state(control)
        pods = json.loads(kube("-n", control, "get", "pods", "-l", "app.kubernetes.io/name=sdo-worker", "-o", "json"))
        if observed.get("incident_open") and any(
            pod.get("status", {}).get("phase") == "Running" for pod in pods["items"]
        ):
            return observed.get("incident_request", {}).get("incident_id") or "open"
        return None

    incident_id = wait_for(incident_running)
    (args.run_dir / "midincident.json").write_text(json.dumps({"incident_id": incident_id}))
    time.sleep(600)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--controller-image", required=True)
    parser.add_argument("--worker-image", required=True)
    parser.add_argument("--worker", action="store_true")
    args = parser.parse_args()
    args.run_dir = args.run_dir.resolve()
    state_path = args.run_dir / "state.json"
    app = "abort-smoke-" + hashlib.sha256(str(args.run_dir).encode()).hexdigest()[:6]
    control = app + "-sdo"
    if args.worker:
        worker(args, state_path, app, control)
        return
    # Refuse to adopt any namespace belonging to a previous run or another task.
    existing = kube("get", "namespace", app, control, "--ignore-not-found=true", "-o", "name")
    if existing.strip():
        raise RuntimeError(f"fixture namespaces already exist: {existing}")
    repo = args.run_dir / "application"
    shutil.copytree(ROOT / "apps/toy/kubernetes", repo / "kubernetes")
    shutil.copytree(ROOT / "apps/toy/seed/.sdo", repo / ".sdo")
    command("git", "-C", str(repo), "init", "-q")
    command("git", "-C", str(repo), "add", "kubernetes")
    command(
        "git", "-C", str(repo), "-c", "user.name=abort-smoke", "-c", "user.email=abort@invalid", "commit", "-qm", "seed"
    )
    source = command("git", "-C", str(repo), "rev-parse", "HEAD").strip()
    arch = repo / ".sdo/arch.md"
    arch.write_text(
        arch.read_text()
        .replace("SOURCE_COMMIT", source)
        .replace("TOPOLOGY_FINGERPRINT", hashlib.sha256(b"abort").hexdigest())
    )
    command("git", "-C", str(repo), "add", ".sdo")
    command(
        "git",
        "-C",
        str(repo),
        "-c",
        "user.name=abort-smoke",
        "-c",
        "user.email=abort@invalid",
        "commit",
        "-qm",
        "memory",
    )
    accepted = command("git", "-C", str(repo), "rev-parse", "HEAD").strip()
    for namespace in (app, control):
        kube("create", "namespace", namespace)
    kube("-n", control, "create", "secret", "generic", "sdo-agent-credentials")
    kube("-n", app, "apply", "-f", str(repo / "kubernetes"))
    kube("-n", app, "rollout", "status", "deployment", "--timeout=90s")
    registrations = {}
    for path in (ROOT / "benchmarks/sregym/adapter/scripted_registrations").glob("*.json"):
        registration = json.loads(path.read_text())
        registration["kubernetes_job"].update(image=args.worker_image, command=["sh", "-c", "sleep 600"])
        registrations[path.name] = json.dumps(registration)
    config = {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "sdo-agent-registrations", "namespace": control},
        "data": registrations,
    }
    subprocess.run(
        ["kubectl", "--kubeconfig", os.environ["KUBECONFIG"], "apply", "-f", "-"],
        input=json.dumps(config),
        text=True,
        check=True,
    )
    PersistentState(
        cleanup_targets={
            app: CleanupTarget(namespace=app, control_namespace=control, kubeconfig=os.environ["KUBECONFIG"])
        }
    ).save(state_path)
    helpers = args.run_dir / "helpers.json"
    helpers.write_text(
        json.dumps(
            {
                name: [sys.executable, "-c", "import time; time.sleep(600)"]
                for name in ("credential-keeper-standin", "host-bridge")
            }
        )
    )
    launcher = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "benchmarks.sregym.adapter.launcher",
            "--state",
            str(state_path),
            "--helpers",
            str(helpers),
            "--",
            sys.executable,
            str(Path(__file__).resolve()),
            "--worker",
            "--run-dir",
            str(args.run_dir),
            "--controller-image",
            args.controller_image,
            "--worker-image",
            args.worker_image,
        ],
        cwd=ROOT,
    )
    try:
        wait_for(lambda: (args.run_dir / "midincident.json").exists(), seconds=600)
        os.kill(launcher.pid, signal.SIGKILL)
        launcher.wait(timeout=10)
        wait_for(lambda: all(not ref.alive() for ref in read_registry(state_path).processes), seconds=10)
        response = command(
            sys.executable, "-m", "benchmarks.sregym.adapter.persistent", "abort", "--state", str(state_path)
        )
        assert json.loads(response)["quiet"] is True
        assert not json.loads(kube("-n", control, "get", "jobs,pods,services,networkpolicies", "-o", "json"))["items"]
        assert kube(
            "-n", control, "get", "pvc/sdo-application-repository", "configmap/sdo-controller-state", "-o", "name"
        )
        assert command("git", "-C", str(repo), "rev-parse", "HEAD").strip() == accepted
        # Repeat through the same public CLI, after the controller and app are gone.
        assert json.loads(
            command(sys.executable, "-m", "benchmarks.sregym.adapter.persistent", "abort", "--state", str(state_path))
        )["quiet"]
        result = {
            "quiet": True,
            "live_registered_helpers": 0,
            "runtime_objects": 0,
            "accepted_commit": accepted,
            "memory_preserved": True,
            "incident": json.loads((args.run_dir / "midincident.json").read_text()),
        }
        (args.run_dir / "result.json").write_text(json.dumps(result, indent=2) + "\n")
        print(json.dumps(result, indent=2))
    finally:
        if launcher.poll() is None:
            launcher.kill()
            launcher.wait()
        subprocess.run(
            [sys.executable, "-m", "benchmarks.sregym.adapter.persistent", "abort", "--state", str(state_path)],
            cwd=ROOT,
        )


if __name__ == "__main__":
    main()
