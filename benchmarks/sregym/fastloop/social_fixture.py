"""Coherent, reproducible social fixture restoration at the benchmark boundary.

Mongo container files are ephemeral in this fixture. Restoring manifests cannot
restore posts lost on a container restart, while surviving timelines retain IDs.
Reset therefore stops all post/timeline processes, replaces the entire fixture,
and checks all references before enabling writers again. No incident repair uses
this code, and neither traffic users nor read windows are changed.
"""

from __future__ import annotations

import json
import re
import time
import uuid
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from benchmarks.sregym.adapter import Deadline, kubectl
from benchmarks.sregym.fastloop.transient_cleanup import retire_pods, wait_deleted

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

NAMESPACE = "social-network"
CLIENTS = ("compose-post-service", "post-storage-service", "user-timeline-service", "home-timeline-service")
REDIS_STORES = ("user-timeline-redis", "home-timeline-redis")
HELPER_LIFETIME_SECONDS = 120
_helpers: ContextVar[list[str] | None] = ContextVar("social_fixture_helpers", default=None)


@contextmanager
def helper_cleanup(config: Path) -> Iterator[None]:
    """Batch helper retirement, then prove actual absence before returning."""
    if _helpers.get() is not None:
        yield
        return
    helpers: list[str] = []
    token = _helpers.set(helpers)
    try:
        yield
    finally:
        _helpers.reset(token)
        wait_deleted(config, "sregym", helpers, Deadline(time.monotonic() + HELPER_LIFETIME_SECONDS, time.monotonic))


@dataclass(frozen=True)
class TimelineSnapshot:
    stored_posts: frozenset[str]
    mongo_refs: frozenset[str]
    user_redis_refs: frozenset[str]
    home_redis_refs: frozenset[str]

    def require_coherent(self) -> None:
        if not self.stored_posts or not self.mongo_refs:
            raise ValueError("eval: social fixture is empty; seed posts and timelines together")
        for name, refs in (
            ("mongo", self.mongo_refs),
            ("user-redis", self.user_redis_refs),
            ("home-redis", self.home_redis_refs),
        ):
            missing = refs - self.stored_posts
            if missing:
                raise ValueError(
                    f"eval: social fixture {name}: {len(missing)} missing referenced posts; reset the fixture"
                )

    def counts(self) -> dict[str, int]:
        return {
            "posts": len(self.stored_posts),
            "mongo_refs": len(self.mongo_refs),
            "user_redis_refs": len(self.user_redis_refs),
            "home_redis_refs": len(self.home_redis_refs),
        }


def fixture_posts() -> list[dict[str, Any]]:
    """Ten deterministic posts per user in the app's original 0..961 workload."""
    return [
        {
            "post_id": user * 10 + index + 1,
            "req_id": user * 10 + index + 1,
            "timestamp": 1700000000000 + index,
            "post_type": 0,
            "creator": {"user_id": user, "username": f"username_{user}"},
            "text": f"social fixture {user}/{index}",
            "media": [],
            "urls": [],
            "user_mentions": [{"user_id": user, "username": f"username_{user}"}],
        }
        for user in range(962)
        for index in range(10)
    ]


def _exec(config: Path, deployment: str, args: list[str], stdin: str | None = None) -> str:
    return kubectl(
        ["exec", "-i", f"deployment/{deployment}", "--", *args],
        namespace=NAMESPACE,
        kubeconfig=config,
        input_text=stdin,
    ).stdout


def _mongo(config: Path, script: str, stdin: str | None = None) -> str:
    with helper_cleanup(config):
        return _mongo_client(config, script, stdin)


def _mongo_client(config: Path, script: str, stdin: str | None = None) -> str:
    """Keep the fixture client outside the storage container's memory budget."""
    from benchmarks.sregym.fastloop.clean_start import HARNESS_NAMESPACE

    deployment = json.loads(
        kubectl(
            ["get", "deployment/post-storage-mongodb", "-o", "json"],
            namespace=NAMESPACE,
            kubeconfig=config,
        ).stdout
    )
    containers = deployment["spec"]["template"]["spec"]["containers"]
    images = [value["image"] for value in containers if value["name"] == "post-storage-mongodb"]
    if len(images) != 1:
        raise ValueError("eval: social fixture requires one source-declared Mongo container")
    name = "social-fixture-client-" + uuid.uuid4().hex[:12]
    pod = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": name, "namespace": HARNESS_NAMESPACE},
        "spec": {
            "restartPolicy": "Never",
            "activeDeadlineSeconds": HELPER_LIFETIME_SECONDS,
            "automountServiceAccountToken": False,
            "containers": [
                {
                    "name": "client",
                    "image": images[0],
                    "imagePullPolicy": "IfNotPresent",
                    "command": ["sleep", str(HELPER_LIFETIME_SECONDS)],
                    "resources": {
                        "requests": {"cpu": "25m", "memory": "64Mi"},
                        "limits": {"cpu": "500m", "memory": "128Mi"},
                    },
                }
            ],
        },
    }
    kubectl(["apply", "-f", "-"], namespace=HARNESS_NAMESPACE, kubeconfig=config, input_text=json.dumps(pod))
    try:
        kubectl(
            ["wait", "--for=condition=Ready", f"pod/{name}", "--timeout=60s"],
            namespace=HARNESS_NAMESPACE,
            kubeconfig=config,
        )
        connection = 'var db=new Mongo("post-storage-mongodb.social-network.svc.cluster.local:27017").getDB("test");\n'
        return kubectl(
            ["exec", "-i", f"pod/{name}", "--", "mongo", "--nodb", "--quiet", "--norc", "--eval", connection + script],
            namespace=HARNESS_NAMESPACE,
            kubeconfig=config,
            input_text=stdin,
        ).stdout
    finally:
        helpers = _helpers.get()
        assert helpers is not None
        helpers.append(f"pod/{name}")
        kubectl(
            ["delete", f"pod/{name}", "--ignore-not-found=true", "--grace-period=0", "--wait=false"],
            namespace=HARNESS_NAMESPACE,
            kubeconfig=config,
        )


def parse_post_ids(output: str) -> frozenset[str]:
    """Normalize Mongo shell int64 syntax without converting through a float."""
    ids = set()
    for line in output.splitlines():
        if not line:
            continue
        match = re.fullmatch(r'(?:NumberLong\("?(-?\d+)"?\)|(-?\d+))', line)
        if match is None:
            raise ValueError("eval: unexpected social fixture ID output")
        ids.add(str(int(match[1] or match[2])))
    return frozenset(ids)


def _read_mongo_ids(config: Path, *, timelines: bool) -> frozenset[str]:
    if timelines:
        script = (
            'var c=new Mongo("user-timeline-mongodb.social-network.svc.cluster.local:27017").getDB("user-timeline")'
            '.getCollection("user-timeline"); c.find({}, {posts:1}).forEach(function(d){'
            "(d.posts||[]).forEach(function(p){print(p.post_id.toString());});});"
        )
    else:
        script = (
            'db.getSiblingDB("post").post.find({}, {post_id:1}).forEach(function(p){print(p.post_id.toString());});'
        )
    return parse_post_ids(_mongo(config, script))


def check_social_fixture(config: Path) -> TimelineSnapshot:
    with helper_cleanup(config):
        return _check_social_fixture(config)


def _check_social_fixture(config: Path) -> TimelineSnapshot:
    """Read every timeline ID before posts, preserving exact 64-bit identifiers.

    The app publishes posts before references, so concurrent successful writes
    cannot produce false dangling references with this read ordering.
    """
    mongo_refs = _read_mongo_ids(config, timelines=True)
    redis_refs = []
    script = (
        'local out={}; local cursor="0"; repeat local page=redis.call("SCAN",cursor); '
        "cursor=page[1]; for _,key in ipairs(page[2]) do "
        'for _,id in ipairs(redis.call("ZRANGE",key,0,-1)) do table.insert(out,id); end; end; '
        'until cursor=="0"; return out'
    )
    for name in REDIS_STORES:
        output = _exec(config, name, ["redis-cli", "--raw", "EVAL", script, "0"])
        redis_refs.append(parse_post_ids(output))
    snapshot = TimelineSnapshot(_read_mongo_ids(config, timelines=False), mongo_refs, *redis_refs)
    snapshot.require_coherent()
    return snapshot


def canonical_fixture_complete(snapshot: TimelineSnapshot) -> bool:
    snapshot.require_coherent()
    expected = frozenset(str(row["post_id"]) for row in fixture_posts())
    return all(
        expected <= ids
        for ids in (snapshot.stored_posts, snapshot.mongo_refs, snapshot.user_redis_refs, snapshot.home_redis_refs)
    )


def restore_social_fixture(config: Path) -> TimelineSnapshot:
    with helper_cleanup(config):
        snapshot: TimelineSnapshot | None = None
        try:
            snapshot = check_social_fixture(config)
            reusable = canonical_fixture_complete(snapshot)
        except (ValueError, RuntimeError):
            reusable = False
        if reusable and snapshot is not None:
            kubectl(
                ["patch", "job/wrk2-job", "--type=merge", "-p", '{"spec":{"suspend":false}}'],
                namespace="sregym",
                kubeconfig=config,
            )
            return snapshot
        return _restore_social_fixture(config)


def _restore_social_fixture(config: Path) -> TimelineSnapshot:
    """Replace all posts and timelines together, with writers held until PASS.

    Failed restoration deliberately leaves writers stopped: retrying the
    supported reset replaces the same whole fixture, never deletes selected refs.
    """
    listing = json.loads(kubectl(["get", "deployments", "-o", "json"], namespace=NAMESPACE, kubeconfig=config).stdout)[
        "items"
    ]
    replicas = {row["metadata"]["name"]: row["spec"].get("replicas", 1) for row in listing}
    if not set(CLIENTS).issubset(replicas) or any(replicas[name] != 1 for name in CLIENTS):
        raise ValueError("eval: social fixture requires its captured single-replica client topology")
    from benchmarks.sregym.fastloop.clean_start import HARNESS_NAMESPACE

    kubectl(
        ["patch", "job/wrk2-job", "--type=merge", "-p", '{"spec":{"suspend":true}}'],
        namespace=HARNESS_NAMESPACE,
        kubeconfig=config,
    )
    retirement = Deadline(time.monotonic() + HELPER_LIFETIME_SECONDS, time.monotonic)
    retire_pods(
        config, HARNESS_NAMESPACE, "job-name=wrk2-job", owner_kind="Job", owner_name="wrk2-job", deadline=retirement
    )
    kubectl(
        ["scale", *[f"deployment/{name}" for name in CLIENTS], "--replicas=0"], namespace=NAMESPACE, kubeconfig=config
    )
    retirement = Deadline(time.monotonic() + HELPER_LIFETIME_SECONDS, time.monotonic)
    for name in CLIENTS:
        retire_pods(config, NAMESPACE, f"service={name}", owner_kind="ReplicaSet", deadline=retirement, wait=False)
    for name in CLIENTS:
        wait_deleted(config, NAMESPACE, ["pods", f"--selector=service={name}"], retirement)
    posts = fixture_posts()
    # JSON is sent over stdin, not argv. Convert integer fields to BSON int64:
    # the production C++ readers require those types even for small fixture IDs.
    script = """var posts=JSON.parse(cat("/dev/stdin"));
var p=db.getSiblingDB("post").post;
var t=new Mongo("user-timeline-mongodb.social-network.svc.cluster.local:27017")
  .getDB("user-timeline").getCollection("user-timeline");
p.drop(); t.drop(); var users={};
posts.forEach(function(post){
  var user=post.creator.user_id;
  ["post_id","req_id","timestamp"].forEach(function(k){post[k]=NumberLong(post[k]);});
  post.creator.user_id=NumberLong(user);
  post.user_mentions.forEach(function(u){u.user_id=NumberLong(u.user_id);});
  if(!users[user]) users[user]={user_id:NumberLong(user),posts:[]};
  users[user].posts.unshift({post_id:post.post_id,timestamp:post.timestamp});
});
assert.eq(p.insertMany(posts).insertedIds.length, posts.length);
assert.eq(t.insertMany(Object.keys(users).map(function(k){return users[k];})).insertedIds.length, 962);
print("SOCIAL_FIXTURE_SEEDED");"""
    _mongo(config, script, json.dumps(posts))
    # Each post mentions its author, so the same ten references are valid in the
    # home feed as well as the user feed. Every cache is replaced, never merged.
    script = (
        'redis.call("FLUSHDB"); for user=0,961 do for i=0,9 do '
        'redis.call("ZADD",tostring(user),1700000000000+i,tostring(user*10+i+1)); end; end; return "OK"'
    )
    for name in REDIS_STORES:
        if _exec(config, name, ["redis-cli", "--raw", "EVAL", script, "0"]).strip() != "OK":
            raise RuntimeError(f"eval: social fixture failed to seed {name}")
    snapshot = check_social_fixture(config)
    kubectl(
        ["scale", *[f"deployment/{name}" for name in CLIENTS], "--replicas=1"], namespace=NAMESPACE, kubeconfig=config
    )
    for name in CLIENTS:
        kubectl(
            ["rollout", "status", f"deployment/{name}", "--timeout=60s"],
            namespace=NAMESPACE,
            kubeconfig=config,
        )
    kubectl(
        ["patch", "job/wrk2-job", "--type=merge", "-p", '{"spec":{"suspend":false}}'],
        namespace=HARNESS_NAMESPACE,
        kubeconfig=config,
    )
    return snapshot
