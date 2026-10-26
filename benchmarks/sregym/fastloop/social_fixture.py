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
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from benchmarks.sregym.adapter import kubectl

if TYPE_CHECKING:
    from pathlib import Path

NAMESPACE = "social-network"
CLIENTS = ("compose-post-service", "post-storage-service", "user-timeline-service", "home-timeline-service")
REDIS_STORES = ("user-timeline-redis", "home-timeline-redis")


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


def _mongo(config: Path, script: str) -> str:
    # The shell lives in the declared Mongo image; no package installation or
    # external connection is needed. Scripts contain fixture data, never auth.
    return _exec(config, "post-storage-mongodb", ["mongo", "--quiet", "--norc", "--eval", script])


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
            'var c=new Mongo("user-timeline-mongodb:27017").getDB("user-timeline")'
            '.getCollection("user-timeline"); c.find({}, {posts:1}).forEach(function(d){'
            "(d.posts||[]).forEach(function(p){print(p.post_id.toString());});});"
        )
    else:
        script = (
            'db.getSiblingDB("post").post.find({}, {post_id:1}).forEach(function(p){print(p.post_id.toString());});'
        )
    return parse_post_ids(_mongo(config, script))


def check_social_fixture(config: Path) -> TimelineSnapshot:
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


def restore_social_fixture(config: Path) -> TimelineSnapshot:
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
    kubectl(
        ["wait", "--for=delete", "pods", "--selector=job-name=wrk2-job", "--timeout=60s"],
        namespace=HARNESS_NAMESPACE,
        kubeconfig=config,
    )
    kubectl(
        ["scale", *[f"deployment/{name}" for name in CLIENTS], "--replicas=0"], namespace=NAMESPACE, kubeconfig=config
    )
    for name in CLIENTS:
        kubectl(
            ["wait", "--for=delete", "pods", f"--selector=service={name}", "--timeout=60s"],
            namespace=NAMESPACE,
            kubeconfig=config,
        )
    posts = fixture_posts()
    # JSON is sent over stdin, not argv. Convert integer fields to BSON int64:
    # the production C++ readers require those types even for small fixture IDs.
    script = """var posts=JSON.parse(cat("/dev/stdin"));
var p=db.getSiblingDB("post").post;
var t=new Mongo("user-timeline-mongodb:27017").getDB("user-timeline").getCollection("user-timeline");
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
    _exec(config, "post-storage-mongodb", ["mongo", "--quiet", "--norc", "--eval", script], json.dumps(posts))
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
