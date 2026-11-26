"""Exercise the vanilla subject at the hook boundary with synthetic commands."""

import json
import os
import select
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from benchmarks.fuzzops import vanilla
from benchmarks.fuzzops.cellevidence import cell_problems


def write_executable(path: Path, content: str) -> None:
    path.write_text(content)
    path.chmod(0o755)


def test_scoped_kubeconfig_and_clean_pool(tmp_path, monkeypatch, capsys):
    calls = []

    def kube(env, config, *args, stdin=None):
        calls.append((config, args))
        if args == ("apply", "-f", "-"):
            assert stdin is not None
            rules = json.loads(stdin)["items"][1]["rules"]
            secret_rule = next(r for r in rules if r["resources"] == ["secrets"])
            assert set(secret_rule["verbs"]).isdisjoint({"get", "list", "watch"})
        if args[:2] == ("create", "token"):
            return "synthetic-kube-token"
        if args[:2] == ("config", "view"):
            return json.dumps(
                {"clusters": [{"cluster": {"server": "https://lane", "certificate-authority-data": "CA"}}]}
            )
        if args[:2] == ("auth", "can-i"):
            return "yes" if args[2:4] == ("get", "pods") and args[-1] == "fg1" else "no"
        return ""

    monkeypatch.setattr(vanilla, "kubectl", kube)
    app_calls = []
    monkeypatch.setattr(vanilla, "fuzzops_app", lambda env, verb: app_calls.append(verb) or {"match": True})
    env = {
        "BENCH_LANE_DIR": str(tmp_path),
        "BENCH_PACK": "/synthetic/pack",
        "BENCH_OUT": str(tmp_path / "receipts"),
        "FUZZOPS_NAMESPACE": "fg1",
        "KUBECONFIG": "/synthetic/admin",
    }
    for verb in ("prepare", "idle", "snapshot", "reset", "swap", "transcripts", "teardown"):
        assert vanilla.main([verb], env) == 0
    assert app_calls == ["snapshot", "reset", "verify"]
    assert "synthetic-kube-token" not in capsys.readouterr().out
    assert not (tmp_path / "vanilla" / "kubeconfig").exists()
    assert not any("synthetic-kube-token" in p.read_text() for p in (tmp_path / "receipts").rglob("*.json"))


def test_detached_onset_launcher_and_evidence(tmp_path):
    tools = tmp_path / "bin"
    tools.mkdir()
    traffic = tmp_path / "traffic.jsonl"
    ready, release = tmp_path / "agent-ready", tmp_path / "agent-release"
    os.mkfifo(ready)
    os.mkfifo(release)
    nominal_inject = datetime.now(timezone.utc)
    sample = {
        "inject_at": nominal_inject.isoformat(),
        "observed_at": datetime.now(timezone.utc).isoformat(),
        "requests": 6,
        "first_bad": None,
    }
    healthy = json.dumps(sample)
    bad_bucket = {"Start": nominal_inject.replace(microsecond=0).isoformat(), "Failed": 6}
    hurt = json.dumps({**sample, "first_bad": bad_bucket})
    traffic.write_text(healthy + "\n" + hurt[:20])
    write_executable(tools / "kubectl", "#!/bin/sh\nexit 0\n")
    write_executable(
        tools / "bwrap",
        """#!/usr/bin/python3
# Substitute just the namespace boundary, preserving the launcher and all its output plumbing.
import os, pathlib, sys
args = iter(sys.argv[1:])
env = {}
bind = {}
for flag in args:
    if flag in ('--bind', '--ro-bind'):
        src, dst = next(args), next(args)
        bind[dst] = src
    elif flag == '--setenv':
        key, value = next(args), next(args)
        env[key] = bind.get(value, value)
    elif flag == '--chdir':
        cwd = bind[next(args)]
    elif flag in ('--proc', '--dev', '--tmpfs'):
        next(args)
    elif flag == '--':
        break
cmd = next(args)
os.chdir(cwd)
os.execve(bind[cmd], [cmd, *args], env)
""",
    )
    write_executable(
        tools / "claude",
        r"""#!/usr/bin/python3
import json, os, pathlib, sys
assert not list(pathlib.Path.cwd().iterdir())
assert not list(pathlib.Path(os.environ['HOME']).iterdir())
assert 'AWS_SHARED_CREDENTIALS_FILE' not in os.environ
assert 'KUBECONFIG' in os.environ
assert sys.argv[1:] == ['-p', '--output-format', 'stream-json', '--verbose',
                        '--dangerously-skip-permissions', '--model', 'claude-opus-5-5', '--effort', 'medium']
assert sys.stdin.read().strip() == ('Users are reporting problems with the application in namespace fg1; '
                                   'investigate and fix it.')
p = pathlib.Path(os.environ['CLAUDE_CONFIG_DIR']) / 'projects'
p.mkdir()
(p / 'synthetic.jsonl').write_text('{"type":"assistant"}\n')
(p / 'agent-a0123456789abcdef.jsonl').write_text('{"type":"assistant"}\n')
with open(READY, 'wb') as signal:
    signal.write(b'R')
with open(RELEASE, 'rb') as signal:
    assert signal.read(1) == b'G'
print(json.dumps({'type': 'result', 'session_id': 'synthetic', 'usage': {'input_tokens': 7},
                  'agentId': 'a0123456789abcdef', 'total_cost_usd': 0.1,
                  'result': 'Repaired. synthetic-kube-token synthetic-vertex-key'}))
""".replace("READY", repr(str(ready))).replace("RELEASE", repr(str(release))),
    )
    auth = tmp_path / "auth.json"
    auth.write_text('{"private_key":"synthetic-vertex-key"}')
    config = tmp_path / "operator.json"
    config.write_text(
        json.dumps(
            {
                "settings": {
                    "auth_file": str(auth),
                    "window_seconds": 600,
                    "config": {
                        "agent": {
                            "extra_env": [
                                "CLAUDE_CODE_USE_VERTEX=1",
                                "CLOUD_ML_REGION=global",
                                "ANTHROPIC_VERTEX_PROJECT_ID=synthetic",
                                "GOOGLE_APPLICATION_CREDENTIALS=/unused/in/pod",
                            ]
                        }
                    },
                }
            }
        )
    )
    lane_dir = tmp_path / "lane"
    state = lane_dir / "vanilla"
    state.mkdir(parents=True)
    (state / "kubeconfig").write_text('{"users":[{"user":{"token":"synthetic-kube-token"}}]}')
    evidence = tmp_path / "evidence"
    env = {
        **os.environ,
        "PATH": f"{tools}:{os.environ['PATH']}",
        "AWS_SHARED_CREDENTIALS_FILE": "/synthetic/aws",
        "BENCH_OPERATOR": str(config),
        "BENCH_OUT": str(evidence),
        "BENCH_LANE_DIR": str(lane_dir),
        "BENCH_RUN": "proof",
        "BENCH_FAULT": "synthetic",
        "FUZZOPS_TRAFFIC": str(traffic),
        "FUZZOPS_DEADLINE": (nominal_inject + timedelta(seconds=600)).isoformat(),
        "FUZZOPS_NAMESPACE": "fg1",
    }
    ready_fd = os.open(ready, os.O_RDWR | os.O_NONBLOCK)
    release_fd = os.open(release, os.O_RDWR | os.O_NONBLOCK)
    watch = subprocess.Popen(
        [sys.executable, "-m", vanilla.__name__, "watch"],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        assert watch.stderr is not None
        assert select.select([watch.stderr], [], [], 15)[0]
        assert "waiting for harm onset" in watch.stderr.readline()
        assert not select.select([ready_fd], [], [], 0)[0]
        with traffic.open("a") as stream:
            stream.write(hurt[20:] + "\n")
        assert select.select([ready_fd], [], [], 15)[0]
        assert os.read(ready_fd, 1) == b"R"
        assert vanilla.main(["idle"], env) == 1
        watch.kill()
        stdout, _ = watch.communicate(timeout=4)
        assert stdout == ""
        os.write(release_fd, b"G")
        # The detached supervisor owns the lock through evidence publication.
        subprocess.run(
            ["flock", "-w", "15", str(state / "lock"), "true"],
            timeout=20,
            check=True,
        )
        assert vanilla.main(["idle"], env) == 0
    finally:
        os.write(release_fd, b"G")
        os.close(release_fd)
        os.close(ready_fd)
        if watch.poll() is None:
            watch.kill()
            watch.wait()
    assert vanilla.main(["collect"], env) == 0
    assert cell_problems(evidence, "vanilla") == []
    assert json.loads((evidence / "start.json").read_text())["inject_at"] == nominal_inject.isoformat()
    assert json.loads((evidence / "onset.json").read_text())["first_bad"] == bad_bucket
    assert (evidence / "onset-probes.jsonl").read_text() == healthy + "\n" + hurt + "\n"
    assert json.loads((evidence / "usage.json").read_text())["input_tokens"] == 7
    transcript = (evidence / "transcript.jsonl").read_text()
    assert "synthetic-kube-token" not in transcript
    assert "synthetic-vertex-key" not in transcript
    assert vanilla.main(["collect"], env) == 0
    assert not list(evidence.rglob("kubeconfig"))


def test_watch_without_harm_never_launches(tmp_path, monkeypatch, capsys):
    traffic = tmp_path / "traffic.jsonl"
    traffic.write_text(
        '{"inject_at":"2026-01-01T00:00:00Z","observed_at":"2026-01-01T00:00:05Z","requests":6,"first_bad":null}\n'
    )
    env = {
        "BENCH_LANE_DIR": str(tmp_path),
        "BENCH_RUN": "proof",
        "BENCH_OUT": str(tmp_path / "evidence"),
        "FUZZOPS_TRAFFIC": str(traffic),
        "FUZZOPS_DEADLINE": datetime.now(timezone.utc).isoformat(),
    }

    def unexpected_launch(*args, **kwargs):
        raise AssertionError("launched Claude without user harm")

    monkeypatch.setattr(vanilla.subprocess, "Popen", unexpected_launch)
    assert vanilla.main(["watch"], env) == 0
    assert capsys.readouterr().out == ""
    assert vanilla.main(["collect"], env) == 0
    evidence = Path(env["BENCH_OUT"])
    assert json.loads((evidence / "onset.json").read_text())["at"] is None
    assert (evidence / "onset-probes.jsonl").read_text() == traffic.read_text()


def test_watch_reports_missing_traffic_on_stderr(tmp_path, capsys):
    env = {
        "BENCH_LANE_DIR": str(tmp_path),
        "BENCH_RUN": "proof",
        "FUZZOPS_DEADLINE": datetime.now(timezone.utc).isoformat(),
    }
    assert vanilla.main(["watch"], env) == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "requires a FuzzOps pin that publishes FUZZOPS_TRAFFIC" in captured.err
    assert json.loads((tmp_path / "vanilla" / "proof" / "done.json").read_text())["error"] in captured.err


USER = {"type": "authorized_user", "client_id": "public", "client_secret": "synth-cs", "refresh_token": "synth-rt"}


@pytest.mark.parametrize(
    ("auth", "secrets"),
    [
        (USER, ["synth-cs", "synth-rt"]),
        ({"type": "impersonated_service_account", "source_credentials": USER}, ["synth-cs", "synth-rt"]),
        ({"type": "service_account", "private_key": "synth-k1\nsynth-k2\n"}, ["synth-k1", "synth-k2"]),
    ],
)
def test_every_auth_secret_is_redacted_from_kept_logs(tmp_path, auth, secrets):
    path, log, kept = (tmp_path / name for name in ("auth.json", "log", "kept"))
    path.write_text(json.dumps(auth))
    text = json.dumps({"output": path.read_text()}) + "\n" + " ".join(secrets) + "\n"
    log.write_text(text)
    found = vanilla.auth_secrets(path)
    assert found == secrets
    vanilla.copy_log(log, kept, found)
    assert not [secret for secret in secrets if secret in kept.read_text()]


#: What a full EKS lane carries before any subject runs (benchmarks/lanes/deploy/eks-lane.sh).
LANE_PLATFORM_NAMES = (
    "clusterrolebinding.rbac.authorization.k8s.io/sdo-lane-admin",
    "daemonset.apps/sdo-lane-node-agent\npod/sdo-lane-node-agent-6rxmh\nserviceaccount/sdo-lane-admin\n"
    "configmap/sdo-lane-prepull",
)


def _listing(extra_cluster: str = "", extra_workloads: str = ""):
    def kube(env, config, *args, stdin=None):
        base_cluster, base_workloads = LANE_PLATFORM_NAMES
        if "-A" in args:
            return "\n".join(x for x in ("pod/gitlab-webservice-0", base_workloads, extra_workloads) if x)
        return "\n".join(x for x in ("namespace/fg1", base_cluster, extra_cluster) if x)

    return kube


def test_full_eks_lane_platform_objects_are_not_sdo(monkeypatch):
    monkeypatch.setattr(vanilla, "kubectl", _listing())
    assert vanilla.prove_no_sdo({})["result"] == "SDO absent"


@pytest.mark.parametrize(
    ("cluster", "workloads"),
    [
        ("namespace/sdo-system", ""),
        ("", "deployment.apps/sdo-controller"),
        ("", "pod/sdo-lane-node-agent-6rxmh-extra"),
        ("", "serviceaccount/sdo-lane-admin-shadow"),
        ("", "configmap/sdo-lane-prepull-extra"),
    ],
)
def test_sdo_objects_beside_the_lane_platform_are_refused_by_name(monkeypatch, cluster, workloads):
    monkeypatch.setattr(vanilla, "kubectl", _listing(cluster, workloads))
    with pytest.raises(vanilla.HookError, match=(cluster or workloads).split("/", 1)[1]):
        vanilla.prove_no_sdo({})
