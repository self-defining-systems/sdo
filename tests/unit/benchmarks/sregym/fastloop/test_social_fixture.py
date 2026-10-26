from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest

if TYPE_CHECKING:
    import subprocess
    from pathlib import Path

from benchmarks.sregym.fastloop.social_fixture import TimelineSnapshot, fixture_posts, parse_post_ids


def test_every_timeline_store_must_resolve_all_references() -> None:
    snapshot = TimelineSnapshot(
        stored_posts=frozenset({"9007199254740993", "2"}),
        mongo_refs=frozenset({"9007199254740993"}),
        user_redis_refs=frozenset({"2"}),
        home_redis_refs=frozenset({"3"}),
    )
    with pytest.raises(ValueError, match="home-redis: 1 missing"):
        snapshot.require_coherent()


def test_high_precision_ids_and_empty_stores_are_not_silently_accepted() -> None:
    TimelineSnapshot(*(frozenset({"9007199254740993"}) for _ in range(4))).require_coherent()
    with pytest.raises(ValueError, match="empty"):
        TimelineSnapshot(*(frozenset() for _ in range(4))).require_coherent()
    with pytest.raises(ValueError, match="mongo: 1 missing"):
        TimelineSnapshot(
            frozenset({"9007199254740992"}), frozenset({"9007199254740993"}), frozenset(), frozenset()
        ).require_coherent()


def test_fixture_has_ten_posts_for_every_original_workload_user() -> None:
    posts = fixture_posts()
    assert len(posts) == 9620
    assert len({post["post_id"] for post in posts}) == len(posts)
    assert {post["creator"]["user_id"] for post in posts} == set(range(962))
    assert all(sum(p["creator"]["user_id"] == user for p in posts) == 10 for user in (0, 1, 961))
    assert posts == fixture_posts()


def test_mongo_shell_ids_are_normalized_without_precision_loss() -> None:
    assert parse_post_ids('NumberLong("9007199254740993")\nNumberLong(2)\n3\n') == frozenset(
        {"9007199254740993", "2", "3"}
    )
    with pytest.raises(ValueError, match="unexpected"):
        parse_post_ids("ERR unknown command")


def test_fixture_failure_keeps_writers_stopped(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    import json
    import subprocess

    from benchmarks.sregym.fastloop import social_fixture

    calls: list[list[str]] = []

    def command(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        calls.append(args)
        if args[:2] == ["get", "deployments"]:
            value = {
                "items": [{"metadata": {"name": name}, "spec": {"replicas": 1}} for name in social_fixture.CLIENTS]
            }
            return subprocess.CompletedProcess(args, 0, json.dumps(value), "")
        if args[0] == "exec":
            raise RuntimeError("seed write failed")
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(social_fixture, "kubectl", command)
    with pytest.raises(RuntimeError, match="seed write failed"):
        social_fixture.restore_social_fixture(tmp_path / "config")
    assert any("--replicas=0" in args for args in calls)
    assert not any("--replicas=1" in args for args in calls)
    assert not any('suspend":false' in value for args in calls for value in args)


@pytest.mark.parametrize("store", ["mongo_refs", "user_redis_refs", "home_redis_refs"])
def test_a_dangling_reference_in_any_store_fails(store: str) -> None:
    values = {name: frozenset({"1"}) for name in ("stored_posts", "mongo_refs", "user_redis_refs", "home_redis_refs")}
    values[store] = frozenset({"1", "2"})
    with pytest.raises(ValueError, match="1 missing"):
        TimelineSnapshot(**values).require_coherent()
