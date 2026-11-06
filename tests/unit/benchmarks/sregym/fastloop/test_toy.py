"""Toy streams and reusable faults load without SREGym or a cluster."""

from pathlib import Path

import pytest

from apps.toy.faults import SERVICES, fault_spec
from benchmarks.sregym.fastloop.cli import build_parser
from benchmarks.sregym.fastloop.stream import load_stream_config
from tests.unit.benchmarks.sregym.fastloop.accepted_source_fixture import fixed_source_provider

ROOT = Path(__file__).resolve().parents[5]


def test_toy_recovery_meets_the_warm_lane_protocol(tmp_path, monkeypatch):
    import json
    from types import SimpleNamespace

    from benchmarks.sregym.fastloop.baseline_recovery import ApplicationBaseline
    from benchmarks.sregym.fastloop.fault_driver import SregymFaultDriver
    from benchmarks.sregym.fastloop.toy import ToyWorker

    deployments = [{"spec": {"replicas": 1}, "status": {"readyReplicas": 1}}] * 3
    monkeypatch.setattr("benchmarks.sregym.fastloop.toy.command", lambda *args: json.dumps({"items": deployments}))
    from tests.unit.benchmarks.sregym.fastloop.accepted_source_fixture import commit_accepted_source

    commit_accepted_source(tmp_path, namespace="toy")
    env = SimpleNamespace(kubeconfig=tmp_path / "config", namespace="toy", workspace=tmp_path)
    cluster = SimpleNamespace(list_objects=lambda namespace: [], delete=lambda *args: None, apply=lambda *args: None)
    driver = SregymFaultDriver(
        ToyWorker(env),
        namespace="toy",
        baseline=ApplicationBaseline(namespace="toy", objects=[]),
        cluster=cluster,
        require_full_cleanup=True,
        accepted_source=fixed_source_provider(ApplicationBaseline(namespace="toy", objects=[]), "toy"),
    )
    driver.recover()
    assert driver.last_full_cleanup_verified


def test_toy_latent_stream_covers_three_services_and_health_control():
    stream = load_stream_config(ROOT / "tests/fixtures/sregym/fastloop/toy_latent_repeat.json")
    specs = [fault_spec(ids[0]) for ids in stream.incidents]
    assert [spec.service for spec in specs[:3]] == list(SERVICES)
    assert all(spec.kind == "target-port" for spec in specs[:3])
    assert specs[3].kind == "image"
    assert stream.diagnosis_grading == "off"
    assert fault_spec("toy_readiness_backend").kind == "readiness"
    with pytest.raises(ValueError, match="unsupported toy fault"):
        fault_spec("toy_target_port_not-a-service")


def test_toy_up_does_not_require_sregym_seed():
    args = build_parser().parse_args(["up", "--app", "toy", "--run-dir", "/tmp/toy"])
    assert args.app == "toy"
    assert args.seed is None


def test_toy_up_accepts_live_registrations_and_credentials_hook():
    args = build_parser().parse_args(
        [
            "up",
            "--app",
            "toy",
            "--run-dir",
            "/tmp/toy",
            "--agent-registrations",
            "/tmp/live-registrations",
            "--credentials-hook",
            "/tmp/keeper",
        ]
    )
    assert args.agent_registrations == Path("/tmp/live-registrations")
    assert args.credentials_hook == Path("/tmp/keeper")


def test_scripted_source_restore_repairs_declared_readiness(monkeypatch):
    """A readiness fault must be repairable through the same accepted-source tool."""
    import json
    from types import SimpleNamespace

    from tests.unit.benchmarks.sregym.fastloop import script_harness as scripts

    probe = {"httpGet": {"path": "/ready", "port": 8080}, "periodSeconds": 1}
    source = {
        "spec": {
            "template": {"spec": {"containers": [{"name": "backend", "image": "toy:v1", "readinessProbe": probe}]}}
        }
    }
    monkeypatch.setattr(scripts, "_tool", lambda *args: {"source": source})
    calls = []
    monkeypatch.setattr(scripts, "_command", lambda args, **kwargs: calls.append(args))
    request = SimpleNamespace(namespace="toy", state_changes=None)
    scripts._patch_from_source(request, {"kind": "Deployment", "name": "backend"})
    patched = json.loads(calls[0][-1])["spec"]["template"]["spec"]["containers"][0]
    assert patched["readinessProbe"] == probe
    assert calls[1][:5] == ["kubectl", "-n", "toy", "rollout", "status"]


def test_toy_seed_has_broker_metadata():
    import yaml

    seed = ROOT / "apps/toy/seed/.sdo"
    for filename in ("goal.md", "arch.md", "playbooks/health-objective/README.md"):
        content = (seed / filename).read_text()
        assert content.startswith("---\n"), filename
        metadata = yaml.safe_load(content.split("---", 2)[1])
        assert metadata["schema_version"] == 1
    assert yaml.safe_load((seed / "goal.md").read_text().split("---", 2)[1])["owner"] == "human"


def test_readiness_uses_latest_evaluation_after_a_closed_incident():
    from benchmarks.sregym.fastloop.toy import baseline_ready

    state = {
        "evaluation_iteration": 3,
        "incident_open": False,
        "ownership_adoption": {"source_commit": "accepted"},
        "history": [
            {"detector_id": "drift/investigator", "status": "firing"},
            {"detector_id": "drift/investigator", "status": "clear"},
            {"detector_id": "toy-endpoints", "status": "clear"},
        ],
    }
    assert baseline_ready(state, "accepted")
    assert not baseline_ready(state, "different")
    state["history"].append({"detector_id": "toy-endpoints", "status": "firing"})
    assert not baseline_ready(state, "accepted")


def test_baseline_wait_ends_on_the_controllers_exported_state_not_a_fixed_two_minutes(monkeypatch):
    """A loaded host took ~2 minutes to reach an adopted, clear baseline; the old fixed 120 s killed every scenario."""
    import json
    from types import SimpleNamespace

    from benchmarks.sregym.fastloop import toy

    now = [0.0]
    monkeypatch.delenv("SDO_TOY_BASELINE_WAIT", raising=False)
    monkeypatch.setattr(toy.time, "monotonic", lambda: now[0])
    monkeypatch.setattr(toy.time, "sleep", lambda seconds: now.__setitem__(0, now[0] + max(seconds, 1.0)))
    ready_at = 150.0  # past the old fixed budget
    clear = {
        "evaluation_iteration": 3,
        "incident_open": False,
        "ownership_adoption": {"source_commit": "accepted"},
        "history": [{"detector_id": "toy-endpoints", "status": "clear", "evaluated_at": "2030-01-01T00:00:00Z"}],
    }

    def kubectl(*_args, **_kwargs):
        state = clear if now[0] >= ready_at else {**clear, "evaluation_iteration": 0}
        document = {"data": {"runtime-state.json": json.dumps(state)}}
        return SimpleNamespace(returncode=0, stdout=json.dumps(document))

    pod = {
        "metadata": {"name": "sdo-controller-run-x", "creationTimestamp": "2000-01-01T00:00:00Z"},
        "status": {"phase": "Running", "conditions": [{"type": "Ready", "status": "True"}]},
    }
    monkeypatch.setattr(toy.subprocess, "run", kubectl)
    monkeypatch.setattr(toy, "command", lambda *_args: json.dumps({"items": [pod]}))
    monkeypatch.setattr(toy, "accepted_head", lambda *_args: "accepted")
    environment = SimpleNamespace(kubeconfig=Path("/kubeconfig"), namespace="toy")

    toy.wait_for_baseline(environment, "accepted")

    assert ready_at <= now[0] < ready_at + 5, "the wait ended on the exported state"


def test_baseline_adoption_follows_the_head_the_judge_refresh_advanced(monkeypatch):
    """Install commits refreshed health detectors above the installed commit; the controller adopts that head."""
    import json
    from types import SimpleNamespace

    from benchmarks.sregym.fastloop import toy

    refreshed = "b" * 40
    calls: list[tuple[str, ...]] = []

    def kubectl(kubeconfig, namespace, *args, **_kwargs):
        calls.append(args)
        if args[0] == "get":
            return json.dumps({"items": [pod]})
        if args[0] == "exec":
            return refreshed + "\n"
        raise AssertionError(args)

    pod = {
        "metadata": {"name": "sdo-controller-run-x", "creationTimestamp": "2000-01-01T00:00:00Z"},
        "status": {"phase": "Running", "conditions": [{"type": "Ready", "status": "True"}]},
    }
    state = {
        "evaluation_iteration": 3,
        "incident_open": False,
        "ownership_adoption": {"source_commit": refreshed},
        "history": [{"detector_id": "toy-endpoints", "status": "clear", "evaluated_at": "2030-01-01T00:00:00Z"}],
    }
    document = {"data": {"runtime-state.json": json.dumps(state)}}
    monkeypatch.setattr(
        toy.subprocess, "run", lambda *a, **k: SimpleNamespace(returncode=0, stdout=json.dumps(document))
    )
    monkeypatch.setattr(toy, "command", kubectl)
    environment = SimpleNamespace(kubeconfig=Path("/kubeconfig"), namespace="toy")

    toy.wait_for_baseline(environment, "a" * 40)

    exec_call = next(args for args in calls if args[0] == "exec")
    assert "a" * 40 in exec_call, "the head must be checked to contain the installed commit"


def test_accepted_head_is_none_when_the_head_does_not_contain_the_installed_commit(monkeypatch):
    import subprocess
    from types import SimpleNamespace

    from benchmarks.sregym.fastloop import toy

    def not_ancestor(*_args, **_kwargs):
        raise subprocess.CalledProcessError(1, "git")

    monkeypatch.setattr(toy, "command", not_ancestor)
    environment = SimpleNamespace(kubeconfig=Path("/kubeconfig"), namespace="toy")

    assert toy.accepted_head(environment, "pod", "a" * 40) is None


def test_toy_kind_configuration_requires_network_policy_enforcement(tmp_path, monkeypatch):
    from subprocess import CompletedProcess

    import yaml

    from benchmarks.sregym.fastloop.dev import prepare_dev_mount

    worktree = tmp_path / "source"
    (worktree / "sdo/operator").mkdir(parents=True)
    (worktree / "sdo/operator/go.mod").write_text("module fixture\n")

    def build(*args, **kwargs):
        Path(args[0][args[0].index("-o") + 1]).write_bytes(b"synthetic operator")
        return CompletedProcess(args, 0)

    monkeypatch.setattr("benchmarks.sregym.fastloop.dev.subprocess.run", build)
    config = yaml.safe_load(prepare_dev_mount(worktree, None, tmp_path).read_text())
    assert config["networking"]["disableDefaultCNI"]
    assert config["networking"]["podSubnet"] == "192.168.0.0/16"


def test_network_policy_oracle_checks_actual_cluster_policy(monkeypatch):
    import json

    from apps.toy import faults

    spec = faults.fault_spec("toy_network_policy_backend")
    assert spec.kind == "network-policy"
    policies = [{"metadata": {"name": "isolate-backend"}}]
    calls = []

    def command(*args, **kwargs):
        calls.append((args, kwargs))
        return json.dumps({"items": policies})

    monkeypatch.setattr(faults, "command", command)
    assert not spec.oracle("config", "toy")
    policies.clear()
    assert spec.oracle("config", "toy")
    spec.inject("config", "toy")
    manifest = json.loads(calls[-1][1]["input_text"])
    assert manifest["spec"] == {
        "podSelector": {"matchLabels": {"app": "backend"}},
        "policyTypes": ["Ingress"],
        "ingress": [],
    }


def test_sweep_stream_models_an_unowned_service_without_changing_controller():
    config = load_stream_config(ROOT / "tests/fixtures/sregym/fastloop/toy_sweep.json")
    spec = fault_spec(config.incidents[0][0])
    assert spec.kind == "target-port"
    assert spec.clear_field_ownership
    with pytest.raises(ValueError, match="only target-port"):
        fault_spec("toy_readiness_unowned_backend")


@pytest.mark.parametrize("custom", [True, False])
def test_toy_controller_publishes_selected_registrations(tmp_path, monkeypatch, custom):
    from dataclasses import dataclass
    from subprocess import CompletedProcess
    from types import SimpleNamespace

    from benchmarks.sregym import adapter
    from benchmarks.sregym.fastloop import toy

    @dataclass
    class Install:
        control_namespace: str = "toy-sdo"
        credentials_secret: str = "credentials"
        registration_configmap: str = ""

    selected = tmp_path / "script-registrations" if custom else None
    config = SimpleNamespace(agent_registrations=selected)
    images = SimpleNamespace(controller="controller:test", responder="worker:test", validator="validator:test")
    environment = SimpleNamespace(
        dev_mount=tmp_path,
        kubeconfig=tmp_path / "config",
        images=images,
        apply_operator_env=lambda environ: environ.update(SDO_DEV_MOUNT="1", SDO_OPERATOR_BIN="synthetic"),
    )
    published = []
    for name in ("KUBECONFIG", "SDO_DEV_MOUNT", "SDO_OPERATOR_BIN"):
        monkeypatch.setenv(name, "synthetic")
    monkeypatch.setattr(adapter, "native_install_config", lambda _config: Install())
    monkeypatch.setattr(adapter, "install_existing", lambda _install: SimpleNamespace(accepted_commit="accepted"))
    monkeypatch.setattr(adapter, "publish_agent_registrations", lambda *args: published.append(args))
    monkeypatch.setattr(toy.subprocess, "run", lambda *args, **kwargs: CompletedProcess(args, 0))
    monkeypatch.setattr(toy, "wait_for_baseline", lambda *args: None)
    assert toy.install_controller(environment, config) is config
    assert published[0][0] == (selected or ROOT / "benchmarks/sregym/fastloop/agent_registrations")


def test_toy_recovery_writes_only_committed_source(tmp_path, monkeypatch):
    from types import SimpleNamespace

    from benchmarks.sregym.fastloop.toy import ToyWorker
    from tests.unit.benchmarks.sregym.fastloop.accepted_source_fixture import commit_accepted_source

    declared = {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "source", "namespace": "toy"},
        "data": {"value": "accepted"},
    }
    source = commit_accepted_source(tmp_path, [declared], namespace="toy")
    (tmp_path / "kubernetes/manifests.yaml").write_text("unaccepted")
    written = []
    monkeypatch.setattr("benchmarks.sregym.fastloop.toy.command", lambda *args: written.append(args))
    monkeypatch.setattr(
        "benchmarks.sregym.fastloop.baseline_recovery.KubectlApplicationCluster.apply",
        lambda self, ns, obj: written.append(obj),
    )
    ToyWorker(SimpleNamespace(kubeconfig=tmp_path / "config", namespace="toy", workspace=tmp_path)).request("recover")
    assert written == list(source.manifests)


def test_retire_control_state_clears_checkpoints_and_stale_health_refresh_records(tmp_path, monkeypatch):
    from types import SimpleNamespace

    from benchmarks.sregym import adapter
    from benchmarks.sregym.fastloop import toy

    calls = []
    monkeypatch.setattr(adapter, "stop_runtime", lambda control, **_: calls.append(("stop", control)))
    monkeypatch.setattr(adapter, "reset_control_state", lambda control, **_: calls.append(("reset", control)) or {})

    def kubectl(_config, namespace, *args):
        calls.append((namespace, *args))
        if args[0] == "get":
            return "\n".join(
                ["configmap/sdo-health-refresh-a-request", "configmap/sdo-controller-state"]
                + ["configmap/sdo-health-refresh-a-result", "configmap/sdo-external-incident"]
            )
        return ""

    monkeypatch.setattr(toy, "command", kubectl)
    toy.retire_control_state(SimpleNamespace(kubeconfig=tmp_path / "config"), "toy-sdo")
    assert calls[:2] == [("stop", "toy-sdo"), ("reset", "toy-sdo")]
    assert calls[-1] == (
        "toy-sdo",
        "delete",
        "configmap/sdo-health-refresh-a-request",
        "configmap/sdo-health-refresh-a-result",
        "configmap/sdo-external-incident",
        "--ignore-not-found=true",
    )


def test_toy_up_can_install_frozen_seed_without_judge():
    args = build_parser().parse_args(["up", "--app", "toy", "--run-dir", "/tmp/toy", "--frozen-memory"])
    assert args.frozen_memory


def test_frozen_toy_install_preserves_policy_at_native_boundary(tmp_path, monkeypatch):
    from types import SimpleNamespace

    from benchmarks.sregym import adapter
    from benchmarks.sregym.fastloop import toy

    environment = SimpleNamespace(
        dev_mount=tmp_path,
        workspace=tmp_path,
        kubeconfig=tmp_path / "kubeconfig",
        namespace="toy",
        application="toy",
        cluster="toy-private",
        images=SimpleNamespace(controller="controller:test", responder="responder:test"),
        apply_operator_env=lambda _: None,
    )
    monkeypatch.setenv("KUBECONFIG", str(environment.kubeconfig))
    monkeypatch.delenv("SREGYM_BASE_KUBECONFIG", raising=False)
    seen = []

    def install(config):
        seen.append(config)
        return SimpleNamespace(accepted_commit="a" * 40)

    monkeypatch.setattr(toy.subprocess, "run", lambda *args, **kwargs: None)
    monkeypatch.setattr(adapter, "publish_agent_registrations", lambda *args: None)
    monkeypatch.setattr(adapter, "install_existing", install)
    monkeypatch.setattr(toy, "wait_for_baseline", lambda *args: None)
    result = toy.install_controller(environment, frozen_memory=True)
    assert result.frozen_memory
    assert seen[0].frozen_memory


def _workspace(tmp_path, *, fresh):
    import subprocess

    from benchmarks.sregym.fastloop import toy

    workspace = tmp_path / "workspace"
    workspace.mkdir()
    commit = toy.prepare_workspace(workspace, "toy:test", fresh=fresh)
    log = subprocess.run(
        ["git", "-C", str(workspace), "log", "--format=%H"], check=True, capture_output=True, text=True
    ).stdout.split()
    return workspace, commit, log


def test_fresh_toy_workspace_is_source_only_so_onboarding_has_nothing_to_adopt(tmp_path):
    workspace, commit, log = _workspace(tmp_path, fresh=True)
    assert not (workspace / ".sdo").exists()
    assert log == [commit]
    assert (workspace / "main.go").is_file()
    assert (workspace / "Dockerfile").is_file()
    manifests = "".join(path.read_text() for path in (workspace / "kubernetes").glob("*.yaml"))
    assert "toy:test" in manifests


def test_seeded_toy_workspace_still_commits_the_seed_on_top_of_source(tmp_path):
    workspace, commit, log = _workspace(tmp_path, fresh=False)
    assert len(log) == 2
    assert log[0] == commit
    arch = (workspace / ".sdo/arch.md").read_text()
    assert "SOURCE_COMMIT" not in arch
    assert log[1] in arch


def test_fresh_memory_is_toy_only_live_and_names_its_goal():
    from benchmarks.sregym.fastloop import toy

    def parse(*extra):
        return build_parser().parse_args(["up", "--app", "toy", "--run-dir", "/tmp/toy", *extra])

    assert toy.fresh_objective(parse()) is None
    assert toy.fresh_objective(parse("--fresh-memory")) == toy.DEFAULT_GOAL
    assert len(toy.DEFAULT_GOAL) <= 256
    assert toy.fresh_objective(parse("--fresh-memory", "--goal", " keep it up ")) == "keep it up"
    with pytest.raises(ValueError, match="--fresh-memory"):
        toy.fresh_objective(parse("--goal", "keep it up"))
    with pytest.raises(ValueError, match="frozen"):
        toy.fresh_objective(parse("--fresh-memory", "--frozen-memory"))


def test_fresh_toy_install_runs_the_host_lifecycle_not_the_seed_install(tmp_path, monkeypatch):
    from subprocess import CompletedProcess
    from types import SimpleNamespace

    from benchmarks.sregym import adapter
    from benchmarks.sregym.fastloop import toy

    environment = SimpleNamespace(
        dev_mount=tmp_path,
        workspace=tmp_path,
        kubeconfig=tmp_path / "kubeconfig",
        namespace="toy",
        application="toy",
        cluster="toy-private",
        images=SimpleNamespace(controller="controller:test", responder="responder:test"),
        apply_operator_env=lambda _: None,
    )
    monkeypatch.setenv("KUBECONFIG", str(environment.kubeconfig))
    monkeypatch.delenv("SREGYM_BASE_KUBECONFIG", raising=False)
    launched, waited = [], []
    monkeypatch.setattr(toy.subprocess, "run", lambda *args, **kwargs: CompletedProcess(args, 0))
    monkeypatch.setattr(
        adapter, "launch_native_lifecycle", lambda *args: launched.append(args) or {"judge_commit": "j" * 40}
    )
    monkeypatch.setattr(adapter, "install_existing", lambda *_: pytest.fail("seed install used for fresh memory"))
    monkeypatch.setattr(adapter, "publish_agent_registrations", lambda *_: pytest.fail("scripted registrations"))
    monkeypatch.setattr(toy, "wait_for_baseline", lambda *args: waited.append(args))
    config = toy.install_controller(environment, objective="keep it up")
    ((launched_config, objective, api_base),) = launched
    assert launched_config is config
    assert (objective, api_base) == ("keep it up", "")
    assert not config.frozen_memory
    assert config.agent_registrations is None
    assert waited == [(environment, "j" * 40)]


def _parse_up(*extra):
    return build_parser().parse_args(["up", "--app", "toy", "--run-dir", "/tmp/toy", *extra])


def test_toy_variant_defaults_to_raw_and_non_raw_needs_fresh_memory():
    from benchmarks.sregym.fastloop import toy

    assert toy.toy_variant(_parse_up()) == "raw"
    assert toy.toy_variant(_parse_up("--fresh-memory", "--variant", "helm")) == "helm"
    with pytest.raises(ValueError, match="--fresh-memory"):
        toy.toy_variant(_parse_up("--variant", "helm"))
    with pytest.raises(SystemExit):
        _parse_up("--variant", "jsonnet")


def test_helm_toy_workspace_is_source_only_with_a_buildable_dockerfile(tmp_path):
    import subprocess

    from benchmarks.sregym.fastloop import toy

    workspace = tmp_path / "workspace"
    workspace.mkdir()
    commit = toy.prepare_workspace(workspace, "toy:test", fresh=True, variant="helm")
    assert (workspace / "chart/Chart.yaml").is_file()
    assert not (workspace / "kubernetes").exists()
    assert not (workspace / ".sdo").exists()
    assert "AS build" in (workspace / "Dockerfile").read_text()
    log = subprocess.run(
        ["git", "-C", str(workspace), "log", "--format=%H"], check=True, capture_output=True, text=True
    ).stdout.split()
    assert log == [commit]
    tracked = subprocess.run(
        ["git", "-C", str(workspace), "ls-files"], check=True, capture_output=True, text=True
    ).stdout.split()
    assert {"chart/values.yaml", "Dockerfile"} <= set(tracked)


def test_non_raw_variant_cannot_be_seeded(tmp_path):
    from benchmarks.sregym.fastloop import toy

    with pytest.raises(ValueError, match="fresh"):
        toy.prepare_workspace(tmp_path, "toy:test", fresh=False, variant="helm")


def test_non_raw_install_declares_no_manifest_paths_and_raw_declares_kubernetes(tmp_path, monkeypatch):
    from subprocess import CompletedProcess
    from types import SimpleNamespace

    from benchmarks.sregym import adapter
    from benchmarks.sregym.fastloop import toy

    environment = SimpleNamespace(
        dev_mount=tmp_path,
        workspace=tmp_path,
        kubeconfig=tmp_path / "kubeconfig",
        namespace="toy",
        application="toy",
        cluster="toy-private",
        images=SimpleNamespace(controller="controller:test", responder="responder:test"),
        apply_operator_env=lambda _: None,
    )
    monkeypatch.setenv("KUBECONFIG", str(environment.kubeconfig))
    monkeypatch.delenv("SREGYM_BASE_KUBECONFIG", raising=False)
    monkeypatch.setattr(toy.subprocess, "run", lambda *args, **kwargs: CompletedProcess(args, 0))
    monkeypatch.setattr(adapter, "launch_native_lifecycle", lambda *args: {"judge_commit": "j" * 40})
    monkeypatch.setattr(toy, "wait_for_baseline", lambda *args: None)
    helm = toy.install_controller(environment, objective="keep it up", variant="helm")
    raw = toy.install_controller(environment, objective="keep it up")
    assert helm.deployed_manifest_paths == ()
    assert raw.deployed_manifest_paths == ("kubernetes",)


def test_helm_onboarding_assertions_accept_the_expected_evidence_and_name_each_violation():
    from benchmarks.sregym.fastloop import toy

    good = toy.OnboardingEvidence(
        renderer_id="helm",
        image_builders=("buildkit", "buildkit", "buildkit"),
        controller_args=("--app", "/workspace/application", "--deployment-source", "{}"),
    )
    assert toy.onboarding_violations(good, "helm") == []
    assert toy.onboarding_violations(
        toy.OnboardingEvidence("raw", good.image_builders, good.controller_args), "helm"
    ) == ["accepted evidence plan renderer_id is 'raw', expected 'helm'"]
    prebuilt = toy.OnboardingEvidence("helm", ("buildkit", "prebuilt"), good.controller_args)
    assert toy.onboarding_violations(prebuilt, "helm") == ["images not built by buildkit: prebuilt"]
    assert toy.onboarding_violations(toy.OnboardingEvidence("helm", (), good.controller_args), "helm") == [
        "accepted evidence plan declares no images"
    ]
    bare = toy.OnboardingEvidence("helm", good.image_builders, ("--app", "/workspace/application"))
    assert toy.onboarding_violations(bare, "helm") == ["controller args carry no --deployment-source"]
    assert toy.onboarding_violations(good, "raw") == ["accepted evidence plan renderer_id is 'helm', expected 'raw'"]


class _RestartOps:
    """Scripted cluster edge for the restart check; the clock advances only through ``sleep``."""

    def __init__(self, *, new_pod_after=2, verify=True, incident_at=None, drift_at=None):
        self.now = 0.0
        self.deleted = []
        self.pod = "ctl-a"
        self.new_pod_after, self.verify_ok = new_pod_after, verify
        self.incident_at, self.drift_at = incident_at, drift_at
        self.polls = 0

    def clock(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds

    def controller_pod(self):
        self.polls += 1
        return self.pod if not self.deleted or self.polls < self.new_pod_after else "ctl-b"

    def delete_pod(self, name):
        self.deleted.append(name)

    def verify_accepted(self):
        return self.verify_ok

    def incident_open(self):
        return self.incident_at is not None and self.now >= self.incident_at

    def drift_observed(self):
        return self.drift_at is not None and self.now >= self.drift_at


def test_restart_check_passes_for_a_new_pod_with_reads_and_a_quiet_steady_window():
    from benchmarks.sregym.fastloop import toy

    ops = _RestartOps()
    result = toy.check_controller_restart(ops, steady_seconds=30, restart_timeout=60, poll=5)
    assert ops.deleted == ["ctl-a"]
    assert (result.old_pod, result.new_pod) == ("ctl-a", "ctl-b")
    assert result.violations == []


@pytest.mark.parametrize(
    ("kwargs", "expected"),
    [
        ({"verify": False}, "accepted state did not verify after the restart"),
        ({"incident_at": 10}, "an incident opened during the steady window"),
        ({"drift_at": 10}, "drift was observed during the steady window"),
        ({"new_pod_after": 10**6}, "controller pod was not replaced within 60s"),
    ],
)
def test_restart_check_reports_each_failure_instead_of_passing(kwargs, expected):
    from benchmarks.sregym.fastloop import toy

    result = toy.check_controller_restart(_RestartOps(**kwargs), steady_seconds=30, restart_timeout=60, poll=5)
    assert expected in result.violations
