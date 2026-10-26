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
        if args[:2] == ["get", "pods"]:
            return subprocess.CompletedProcess(args, 0, json.dumps({"items": []}), "")
        if args[:2] == ["get", "deployments"]:
            value = {
                "items": [{"metadata": {"name": name}, "spec": {"replicas": 1}} for name in social_fixture.CLIENTS]
            }
            return subprocess.CompletedProcess(args, 0, json.dumps(value), "")
        if args[:2] == ["get", "deployment/post-storage-mongodb"]:
            value = {
                "spec": {
                    "template": {
                        "spec": {"containers": [{"name": "post-storage-mongodb", "image": "mongo:fixture-test"}]}
                    }
                }
            }
            return subprocess.CompletedProcess(args, 0, json.dumps(value), "")
        if args[0] == "exec":
            raise RuntimeError("seed write failed")
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(social_fixture, "kubectl", command)
    from benchmarks.sregym.fastloop import transient_cleanup

    monkeypatch.setattr(transient_cleanup, "kubectl", command)
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


def test_mongo_fixture_client_cannot_consume_storage_container_memory(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    import json
    import subprocess

    from benchmarks.sregym.fastloop import social_fixture

    calls: list[tuple[list[str], dict[str, Any]]] = []

    def command(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        calls.append((args, kwargs))
        if args[:2] == ["get", "deployment/post-storage-mongodb"]:
            return subprocess.CompletedProcess(
                args,
                0,
                json.dumps(
                    {
                        "spec": {
                            "template": {
                                "spec": {
                                    "containers": [{"name": "post-storage-mongodb", "image": "mongo:fixture-test"}]
                                }
                            }
                        }
                    }
                ),
                "",
            )
        if args[0] == "exec":
            assert not any(arg == "deployment/post-storage-mongodb" for arg in args), (
                "fixture client must not run in storage container"
            )
            assert kwargs["namespace"] == "sregym"
            assert "--nodb" in args
            return subprocess.CompletedProcess(args, 0, "CONTROL_OK\n", "")
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(social_fixture, "kubectl", command)
    from benchmarks.sregym.fastloop import transient_cleanup

    monkeypatch.setattr(transient_cleanup, "kubectl", command)
    assert social_fixture._mongo(tmp_path / "config", 'print("CONTROL_OK")').strip() == "CONTROL_OK"
    pods = [json.loads(kwargs["input_text"]) for args, kwargs in calls if args[:2] == ["apply", "-f"]]
    assert len(pods) == 1
    container = pods[0]["spec"]["containers"][0]
    assert container["image"] == "mongo:fixture-test"
    assert container["resources"]["limits"]["memory"] == "128Mi"
    assert any(args[0] == "delete" for args, _ in calls)


@pytest.mark.parametrize("missing_store", range(4))
def test_warm_reuse_requires_every_canonical_id_in_every_store(missing_store: int) -> None:
    from benchmarks.sregym.fastloop.social_fixture import canonical_fixture_complete

    expected = frozenset(str(row["post_id"]) for row in fixture_posts())
    stores = [expected] * 4
    assert canonical_fixture_complete(TimelineSnapshot(*stores))
    stores[missing_store] = expected - {"1"}
    if missing_store == 0:
        with pytest.raises(ValueError, match="missing referenced"):
            canonical_fixture_complete(TimelineSnapshot(*stores))
    else:
        assert not canonical_fixture_complete(TimelineSnapshot(*stores))


def test_warm_reuse_rejects_dangling_references() -> None:
    from benchmarks.sregym.fastloop.social_fixture import canonical_fixture_complete

    expected = frozenset(str(row["post_id"]) for row in fixture_posts())
    with pytest.raises(ValueError, match="missing referenced"):
        canonical_fixture_complete(TimelineSnapshot(expected, expected | {"-1"}, expected, expected))


def test_fixture_helpers_are_drained_together_before_check_returns(monkeypatch, tmp_path):
    import json
    import subprocess

    from benchmarks.sregym.fastloop import social_fixture, transient_cleanup

    calls = []

    def command(args, **kwargs):
        calls.append(args)
        if args[:2] == ["get", "deployment/post-storage-mongodb"]:
            return subprocess.CompletedProcess(
                args,
                0,
                json.dumps(
                    {
                        "spec": {
                            "template": {
                                "spec": {"containers": [{"name": "post-storage-mongodb", "image": "mongo:fixture"}]}
                            }
                        }
                    }
                ),
                "",
            )
        return subprocess.CompletedProcess(args, 0, "1\n" if args[0] == "exec" else "", "")

    monkeypatch.setattr(social_fixture, "kubectl", command)
    monkeypatch.setattr(transient_cleanup, "kubectl", command)
    social_fixture.check_social_fixture(tmp_path / "config")
    deletes = [args for args in calls if args[0] == "delete"]
    drains = [args for args in calls if "--for=delete" in args]
    assert len(deletes) == 2
    assert all("--wait=false" in args for args in deletes)
    assert len(drains) == 1
    assert sum(arg.startswith("pod/social-fixture-client-") for arg in drains[0]) == 2
    assert calls[-1] == drains[0]


def test_complete_warm_fixture_resumes_traffic_without_replacing_stores(monkeypatch, tmp_path):
    import subprocess

    from benchmarks.sregym.fastloop import social_fixture

    expected = frozenset(str(row["post_id"]) for row in fixture_posts())
    snapshot = TimelineSnapshot(*(expected for _ in range(4)))
    calls = []
    monkeypatch.setattr(social_fixture, "check_social_fixture", lambda config: snapshot)

    def command(args, **kwargs):
        calls.append(args)
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(social_fixture, "kubectl", command)
    assert social_fixture.restore_social_fixture(tmp_path / "config") is snapshot
    assert calls == [["patch", "job/wrk2-job", "--type=merge", "-p", '{"spec":{"suspend":false}}']]
