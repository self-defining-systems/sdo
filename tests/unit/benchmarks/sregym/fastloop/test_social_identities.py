"""Row start leaves the social app holding only identities its own gateway can carry.

The ids below have the live shape: user ids above 2**60 (float64 spacing 256), one
millisecond block holding a first registration (counter 0) and a third one (counter 2).
"""

from __future__ import annotations

import json
import subprocess
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

import pytest

from benchmarks.sregym.fastloop import cli, social_fixture
from benchmarks.sregym.fastloop.social_fixture import (
    GraphNode,
    Identity,
    gateway_carries,
    plan_identity_repair,
)

if TYPE_CHECKING:
    from pathlib import Path

FIRST = 0x16C4146AE5FAA000  # counter 0 in its millisecond: the gateway carries it
THIRD = FIRST + 2  # counter 2 in the same millisecond: the gateway reads it as FIRST
LATER = 0x16C4146AE5FAF000


def test_the_gateway_carries_exactly_the_ids_a_float64_holds() -> None:
    assert gateway_carries(FIRST)
    assert gateway_carries(2**53)
    assert gateway_carries(961)
    assert not gateway_carries(THIRD)
    assert not gateway_carries(2**53 + 1)


def test_an_identity_the_gateway_rounds_is_rekeyed_and_named_by_the_user_it_impersonates() -> None:
    users = [Identity("writer", FIRST), Identity("author", THIRD), Identity("reader", LATER)]
    graph = [
        GraphNode(FIRST, followers=(), followees=()),
        GraphNode(THIRD, followers=(LATER,), followees=()),
        GraphNode(LATER, followers=(), followees=(THIRD,)),
    ]
    repair = plan_identity_repair(users, graph)
    [rekey] = repair.rekeyed
    assert (rekey.username, rekey.old_id, rekey.gateway_id, rekey.impersonates) == ("author", THIRD, FIRST, "writer")
    # The new id is free, carried by the gateway, and stays in the same millisecond block.
    assert gateway_carries(rekey.new_id)
    assert rekey.new_id not in {FIRST, THIRD, LATER}
    assert rekey.new_id >> 12 == THIRD >> 12
    assert repair.orphans == ()
    assert "author" in repair.describe()
    assert "(user writer)" in repair.describe()


def test_graph_entries_no_user_holds_are_orphans() -> None:
    # A user removed from the user store (a purged identity) leaves its graph document and the
    # follow edges that point at it; a follower list read would return that dead id first.
    users = [Identity("writer", FIRST), Identity("reader", LATER)]
    graph = [GraphNode(FIRST, (), ()), GraphNode(THIRD, (LATER,), ()), GraphNode(LATER, (), (THIRD, FIRST))]
    repair = plan_identity_repair(users, graph)
    assert repair.rekeyed == ()
    assert repair.orphans == (THIRD,)


def test_a_coherent_store_needs_no_repair() -> None:
    users = [Identity("writer", FIRST), Identity("reader", LATER)]
    graph = [GraphNode(FIRST, (LATER,), ()), GraphNode(LATER, (), (FIRST,))]
    repair = plan_identity_repair(users, graph)
    assert not repair
    assert repair.to_json() == {"users": 2, "graph_nodes": 2, "rekeyed": [], "orphans": []}


def test_two_rounded_identities_never_receive_the_same_new_id() -> None:
    users = [Identity("a", FIRST), Identity("b", FIRST + 1), Identity("c", FIRST + 2)]
    repair = plan_identity_repair(users, [])
    new_ids = [row.new_id for row in repair.rekeyed]
    assert len(new_ids) == len(set(new_ids)) == 2
    assert all(gateway_carries(value) for value in new_ids)


def _reads(users: list[Identity], graph: list[GraphNode]) -> dict[str, str]:
    user_lines = "\n".join(json.dumps({"username": u.username, "user_id": str(u.user_id)}) for u in users)
    graph_lines = "\n".join(
        json.dumps(
            {
                "user_id": str(node.user_id),
                "followers": [str(v) for v in node.followers],
                "followees": [str(v) for v in node.followees],
            }
        )
        for node in graph
    )
    return {"user-mongodb": user_lines + "\n", "social-graph-mongodb": graph_lines + "\n"}


def _fake_cluster(
    monkeypatch: pytest.MonkeyPatch, reads: dict[str, str]
) -> tuple[list[list[str]], list[tuple[str, str | None]]]:
    commands: list[list[str]] = []
    scripts: list[tuple[str, str | None]] = []

    def mongo(_config: Path, script: str, stdin: str | None = None) -> str:
        scripts.append((script, stdin))
        if "SOCIAL_IDENTITIES_REPAIRED" in script:
            return "SOCIAL_IDENTITIES_REPAIRED\n"
        for service, output in reads.items():
            if f"{service}.social-network" in script:
                return output
        raise AssertionError(script)

    def command(args: list[str], **_kwargs: Any) -> subprocess.CompletedProcess[str]:
        commands.append(args)
        return subprocess.CompletedProcess(args, 0, "OK\n", "")

    monkeypatch.setattr(social_fixture, "_mongo", mongo)
    monkeypatch.setattr(social_fixture, "kubectl", command)
    return commands, scripts


def test_restore_rekeys_and_resets_the_id_caches(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    users = [Identity("writer", FIRST), Identity("author", THIRD)]
    commands, scripts = _fake_cluster(monkeypatch, _reads(users, [GraphNode(FIRST, (), ()), GraphNode(THIRD, (), ())]))
    repair = social_fixture.restore_social_identities(tmp_path / "config")
    [rekey] = repair.rekeyed
    [(_script, plan)] = [row for row in scripts if "SOCIAL_IDENTITIES_REPAIRED" in row[0]]
    assert plan is not None
    assert json.loads(plan)["rekey"] == [{"username": "author", "old": str(THIRD), "new": str(rekey.new_id)}]
    flat = [" ".join(args) for args in commands]
    assert any("social-graph-redis" in row and "FLUSHALL" in row for row in flat)
    assert any(row.startswith("delete pods") and "service=user-memcached" in row for row in flat)


def test_restore_writes_nothing_when_identities_are_coherent(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    users = [Identity("writer", FIRST)]
    commands, scripts = _fake_cluster(monkeypatch, _reads(users, [GraphNode(FIRST, (), ())]))
    assert not social_fixture.restore_social_identities(tmp_path / "config")
    assert commands == []
    assert all("SOCIAL_IDENTITIES_REPAIRED" not in script for script, _ in scripts)


@pytest.mark.parametrize(("namespace", "audited"), [("social-network", True), ("hotel-reservation", False)])
def test_row_start_audits_social_identities_and_records_them(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, namespace: str, audited: bool
) -> None:
    repair = plan_identity_repair([Identity("writer", FIRST), Identity("author", THIRD)], [])
    monkeypatch.setattr(social_fixture, "restore_social_identities", lambda _config: repair)
    environment = SimpleNamespace(namespace=namespace, kubeconfig=tmp_path / "config")
    cli.restore_lane_identities(environment, tmp_path)
    receipt = tmp_path / "social-identities.json"
    assert receipt.is_file() == audited
    if audited:
        assert json.loads(receipt.read_text())["rekeyed"][0]["username"] == "author"
