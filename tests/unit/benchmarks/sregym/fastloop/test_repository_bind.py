"""Restart schedules for the real PV producer and its node-bind consumer."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

import pytest

from benchmarks.sregym.fastloop import shared_memory as sm
from benchmarks.sregym.fastloop.environment import FastloopEnvironment

TOKEN = "a" * 32


class Nodes:
    def __init__(self, monkeypatch, tmp_path: Path):
        self.env = FastloopEnvironment(
            run_dir=tmp_path / "lane",
            cluster="synthetic",
            kubeconfig=tmp_path / "kubeconfig",
            namespace="app",
            application="app-left",
            workspace=tmp_path / "shared/accepted",
            sregym_dir=tmp_path / "sregym",
            private_tmp=None,
            parallel_root=tmp_path / "shared",
            parallel_lane="left",
            shared_accepted_id=TOKEN,
        )
        identity = self.env.workspace / ".git/sdo-shared-id"
        identity.parent.mkdir(parents=True)
        identity.write_text(TOKEN)
        self.node = "synthetic-worker"
        self.volume = sm.parallel_volume_objects(self.env, self.node)[1]
        self.claim = sm.parallel_volume_objects(self.env, self.node)[2]
        self.bound: set[str] = set()
        self.writes: list[tuple[str, ...]] = []
        self.readback_fails = False
        self.wrong_mount = False

        def kubernetes(environment, *args, **kwargs):
            if args[:2] == ("get", "pvc"):
                return self.claim
            if args[:2] == ("get", "pv"):
                return self.volume
            raise AssertionError(args)

        def command(*args):
            if args[:3] == ("kind", "get", "nodes"):
                return "synthetic-control-plane\nsynthetic-worker"
            if args[:2] == ("docker", "inspect"):
                return json.dumps(
                    [{"Type": "bind", "Destination": sm.NODE_ROOT, "Source": str(self.env.parallel_root)}]
                )
            if "mount" in args:
                self.writes.append(args)
                assert args[args.index("--privileged") + 1] == self.node
                if not self.readback_fails:
                    self.bound.add(self.node)
                return ""
            if "findmnt" in args:
                return args[-1] if self.wrong_mount else ""
            if "stat" in args:
                if args[-1] == sm.NODE_ROOT + "/accepted/.git":
                    return str((self.env.workspace / ".git").stat().st_ino)
                node = args[2]
                source = args[-1] == sm.NODE_ROOT + "/accepted"
                return "7:11" if source or node in self.bound else "7:22"
            raise AssertionError(args)

        monkeypatch.setattr(sm, "_json", kubernetes)
        monkeypatch.setattr(sm, "_command", command)


def test_repair_binds_only_pv_node_and_repeat_never_stacks(monkeypatch, tmp_path):
    nodes = Nodes(monkeypatch, tmp_path)
    first = sm.ensure_repository_bind(nodes.env)
    assert first.node == nodes.node
    assert first.repaired is True
    assert len(nodes.writes) == 1
    assert sm.ensure_repository_bind(nodes.env).repaired is False
    assert len(nodes.writes) == 1
    # Simulate a node restart: the persistent environment/PV survives, its mount does not.
    nodes.bound.clear()
    assert sm.ensure_repository_bind(nodes.env).repaired is True
    assert len(nodes.writes) == 2


@pytest.mark.parametrize("failure", ["identity", "source_identity", "pv_owner", "node", "readback", "wrong_mount"])
def test_unproven_bind_never_reaches_revision_adoption(monkeypatch, tmp_path, failure):
    nodes = Nodes(monkeypatch, tmp_path)
    if failure == "identity":
        (nodes.env.workspace / ".git/sdo-shared-id").write_text("b" * 32)
    elif failure == "source_identity":
        original = sm._command

        def command(*args):
            return "wrong-source" if args[-1] == sm.NODE_ROOT + "/accepted/.git" else original(*args)

        monkeypatch.setattr(sm, "_command", command)
    elif failure == "pv_owner":
        nodes.volume["spec"]["claimRef"]["namespace"] = "peer-sdo"
    elif failure == "node":
        nodes.volume["spec"]["nodeAffinity"]["required"]["nodeSelectorTerms"][0]["matchExpressions"][0]["values"] = [
            "foreign-worker"
        ]
    elif failure == "readback":
        nodes.readback_fails = True
    else:
        nodes.wrong_mount = True
    with pytest.raises(ValueError, match="eval:"):
        sm.ensure_repository_bind(nodes.env)
    assert len(nodes.writes) == (1 if failure == "readback" else 0)


def test_direct_shared_run_repairs_before_lifecycle_or_revision_reads(monkeypatch, tmp_path):
    from benchmarks.sregym.fastloop import cli, fault_tracker

    nodes = Nodes(monkeypatch, tmp_path)
    nodes.env.save()

    class PreflightReached(Exception):
        pass

    def preflight(environment):
        assert environment.shared_accepted_id == TOKEN
        raise PreflightReached

    monkeypatch.setattr(sm, "prepare_shared_lane", preflight)
    monkeypatch.setattr(cli, "_worker", lambda *_: pytest.fail("worker started before shared bind preflight"))
    monkeypatch.setattr(
        fault_tracker,
        "describe_sregym_problems",
        lambda *_a, **_k: pytest.fail("problem read before shared bind preflight"),
    )
    args = cli.build_parser().parse_args(["run", "--run-dir", str(nodes.env.run_dir), "--agent", "sdo"])
    with pytest.raises(PreflightReached):
        cli._run(args)


def test_repair_command_calls_the_shared_bind_owner(monkeypatch, tmp_path, capsys):
    from benchmarks.sregym.fastloop import cli

    nodes = Nodes(monkeypatch, tmp_path)
    nodes.env.save()
    assert cli.main(["repair-repository-bind", "--run-dir", str(nodes.env.run_dir)]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["node"] == nodes.node
    assert result["repaired"] is True
    assert len(nodes.writes) == 1


def test_bind_transaction_has_one_owner_even_across_callers(monkeypatch, tmp_path):
    from benchmarks.sregym.fastloop.warm_lane import WarmLaneBusy, own_lane

    nodes = Nodes(monkeypatch, tmp_path)
    lock_dir = tmp_path / "locks"
    nodes.env.worker_env["FASTLOOP_HOST_LOCK_DIR"] = str(lock_dir)
    key = f"repository-bind:{nodes.env.parallel_root}:{nodes.env.cluster}"
    with own_lane(lock_dir, key):
        with pytest.raises(WarmLaneBusy):
            sm.ensure_repository_bind(nodes.env)
    assert not nodes.writes
    assert sm.ensure_repository_bind(nodes.env).repaired is True


def test_remote_memory_verifies_native_identity_without_a_host_bind(monkeypatch, tmp_path):
    nodes = Nodes(monkeypatch, tmp_path)
    nodes.env.memory_repo = "synthetic/repository"
    nodes.env.memory_branch = "synthetic-run"
    nodes.env.parallel_root = None
    calls = []
    monkeypatch.setattr(sm, "_assert_native_shared_identity", lambda env: calls.append(env.shared_accepted_id))
    assert sm.prepare_shared_lane(nodes.env) is None
    assert calls == [TOKEN]
    assert not nodes.writes
    with pytest.raises(ValueError, match="remote"):
        sm.ensure_repository_bind(nodes.env)
