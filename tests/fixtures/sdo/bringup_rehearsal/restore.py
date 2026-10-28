"""Synthetic harness boundary; all restore operations use the shared fake API."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace
from urllib.request import Request, urlopen

from benchmarks.sregym import adapter
from benchmarks.sregym.fastloop import cli
from benchmarks.sregym.fastloop.accepted_source import APPLICATION_MANIFEST_PATHS
from benchmarks.sregym.fastloop.baseline_recovery import ApplicationBaseline, save_application_baseline
from benchmarks.sregym.fastloop.environment import FastloopEnvironment
from benchmarks.sregym.fastloop.reset import KubectlResetCluster, reset_environment


def main() -> None:
    run_dir, kubeconfig, workspace, commit, baseline = sys.argv[1:]
    environment = FastloopEnvironment(
        run_dir=Path(run_dir),
        cluster="hermetic",
        kubeconfig=Path(kubeconfig),
        namespace="demo",
        application="demo",
        workspace=Path(workspace),
        sregym_dir=Path(run_dir),
        private_tmp=None,
        seed_commit=commit,
    )
    expected = json.loads(Path(baseline).read_text())
    environment.baseline_path.write_text("{}\n")
    save_application_baseline(environment.baseline_path, (ApplicationBaseline(namespace="demo", objects=expected),))
    cluster = KubectlResetCluster(environment.kubeconfig, "demo-sdo")

    class ScriptedHarness:
        def __init__(self) -> None:
            self.calls: list[str] = []

        def request(self, op: str, **kwargs: object) -> dict[str, object]:
            self.calls.append(op)
            if op == "clean_start":
                # The synthetic fault is a targetPort edit, restored by the real
                # baseline reconciler after this harness recovery boundary.
                return {"success": True}
            live = {(item["kind"], item["metadata"]["name"]): item for item in cluster.list_objects("demo")}
            service = live[("Service", "geo")]
            healthy = service["spec"]["ports"][0]["targetPort"] == expected[0]["spec"]["ports"][0]["targetPort"]
            if op == "health":
                return {"healthy": healthy, "unready": [] if healthy else ["geo"]}
            if op == "baseline_oracle":
                return {"success": healthy}
            raise AssertionError(f"unexpected harness operation {op}")

    # The Go fixture declares this application under deploy/, not kubernetes/.
    APPLICATION_MANIFEST_PATHS["demo"] = ("deploy/tracing.yaml",)
    worker = ScriptedHarness()
    reset_environment(environment, cluster, worker)
    assert worker.calls == ["clean_start", "health", "baseline_oracle"], worker.calls
    assert not environment.state_path.exists()
    print("HY clean restore: real reset_environment + KubectlResetCluster + baseline reconciliation passed")
    endpoint = json.loads(Path(kubeconfig).read_text())["clusters"][0]["cluster"]["server"]

    def native_install(config: object, *_args: object) -> dict[str, object]:
        # The fake cluster mounts the accepted checkout directly. Only the PVC
        # seeding transport is replaced: this endpoint runs InstallPrepared and
        # its actual onboarding hook in the Go fixture, never a canned receipt.
        assert config.namespace == "demo"
        with urlopen(Request(endpoint + "/rehearsal/install", data=b"{}", method="POST"), timeout=20) as response:
            return json.load(response)

    adapter.launch_native_lifecycle = native_install
    args = cli.build_parser().parse_args(
        [
            "run",
            "--run-dir",
            run_dir,
            "--agent",
            "sdo",
            "--no-checkpoints",
            "--agent-registrations",
            "benchmarks/sregym/adapter/scripted_registrations",
        ]
    )
    # The row-start wipe deletes the control namespace, which this fake API cannot model (test_wipe.py covers
    # it). Retire the prior install's control state through the same Go commands so the install below still
    # runs against the state a wiped lane has.
    control = adapter.control_namespace_for(environment.namespace)
    adapter.stop_runtime(control, kubeconfig=str(environment.kubeconfig))
    adapter.reset_control_state(control, kubeconfig=str(environment.kubeconfig))
    agent = cli._sdo_agent(args, environment, Path(run_dir) / "results")
    assert agent._run_lifecycle(SimpleNamespace(health_objective="Keep tracing healthy.")) is False
    print("Fresh-stream CLI: real stop -> reset-control-state, then native prepared install passed")


if __name__ == "__main__":
    main()
