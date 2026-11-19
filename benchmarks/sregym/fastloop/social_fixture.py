"""Coherent, reproducible social fixture restoration at the benchmark boundary.

Mongo container files are ephemeral in this fixture. Restoring manifests cannot
restore posts lost on a container restart, while surviving timelines retain IDs.
Reset therefore stops all post/timeline processes, replaces the entire fixture,
and checks all references before enabling writers again. No incident repair uses
this code, and read windows are never changed.

Identities follow one invariant (`restore_social_identities`): the app holds only
user ids its own gateway can carry. The user service mints an id as
machine|millisecond|per-millisecond counter, and the nginx Lua handlers read the
id back from the login token with ``tonumber``, a float64. Above 2**53 a double
drops the low bits, so a registration that is not the first in its millisecond
(at the app's ids, above 2**60, any counter not a multiple of 256) reaches every
signed-in handler as a neighbouring id: the user composes as another user, or as
nobody. Such an identity is broken for life, and since the Mongo containers keep
no volume, every recreation re-mints the users a probe registers concurrently.
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

from benchmarks.sregym.adapter import Deadline, WaitKind, kubectl, wait_for
from benchmarks.sregym.fastloop.transient_cleanup import retire_pods, wait_deleted

if TYPE_CHECKING:
    from collections.abc import Collection, Iterator, Sequence
    from pathlib import Path

NAMESPACE = "social-network"
CLIENTS = ("compose-post-service", "post-storage-service", "user-timeline-service", "home-timeline-service")
REDIS_STORES = ("user-timeline-redis", "home-timeline-redis")
HELPER_LIFETIME_SECONDS = int(wait_for(WaitKind.HELPER_LIFETIME))
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


USER_MONGO = "user-mongodb.social-network.svc.cluster.local:27017"
GRAPH_MONGO = "social-graph-mongodb.social-network.svc.cluster.local:27017"
TIMELINE_MONGO = "user-timeline-mongodb.social-network.svc.cluster.local:27017"
# The id's low 12 bits are the user service's per-millisecond counter.
COUNTER_BITS = 12
# Field on a user document recording the id an interrupted rekey was moving it to.
PENDING_FIELD = "sdo_rekey_to"


def gateway_carries(user_id: int) -> bool:
    """Whether the gateway's Lua number (an IEEE double) holds user_id exactly."""
    return int(float(user_id)) == user_id


@dataclass(frozen=True)
class Identity:
    username: str
    user_id: int
    # The target a previous, interrupted rekey recorded on this user's document.
    pending: int | None = None


@dataclass(frozen=True)
class GraphNode:
    user_id: int
    followers: tuple[int, ...]
    followees: tuple[int, ...]


@dataclass(frozen=True)
class Rekey:
    username: str
    old_id: int
    new_id: int
    # What every signed-in handler receives for old_id, and whose id that is.
    gateway_id: int
    impersonates: str | None

    def to_json(self) -> dict[str, Any]:
        return {
            "username": self.username,
            "old_id": str(self.old_id),
            "new_id": str(self.new_id),
            "gateway_id": str(self.gateway_id),
            "impersonates": self.impersonates,
        }


@dataclass(frozen=True)
class IdentityRepair:
    users: int
    graph_nodes: int
    rekeyed: tuple[Rekey, ...]
    # Social-graph ids no user holds or is being moved to (a user removed from the user store
    # keeps its graph node and the edges pointing at it).
    orphans: tuple[int, ...]

    def __bool__(self) -> bool:
        return bool(self.rekeyed or self.orphans)

    def describe(self) -> str:
        parts = [
            f"{row.username} id {row.old_id} reaches the gateway as {row.gateway_id}"
            + (f" (user {row.impersonates})" if row.impersonates else " (no user)")
            + f", rekeyed to {row.new_id}"
            for row in self.rekeyed
        ]
        if self.orphans:
            parts.append("dropped graph ids no user holds: " + ", ".join(str(value) for value in self.orphans))
        return "; ".join(parts) or "all identities carried by the gateway"

    def to_json(self) -> dict[str, Any]:
        return {
            "users": self.users,
            "graph_nodes": self.graph_nodes,
            "rekeyed": [row.to_json() for row in self.rekeyed],
            "orphans": [str(value) for value in self.orphans],
        }


def _free_carried_id(old_id: int, taken: set[int]) -> int:
    """The highest free counter in old_id's millisecond block whose id the gateway carries.

    The user service would need thousands of registrations in one millisecond to mint
    these counters, so the new id neither collides later nor leaves its time block.
    """
    block = old_id >> COUNTER_BITS << COUNTER_BITS
    for counter in range((1 << COUNTER_BITS) - 1, 0, -1):
        candidate = block | counter
        if candidate not in taken and gateway_carries(candidate):
            return candidate
    raise ValueError(f"eval: social identity {old_id} has no free gateway-carried id in its millisecond block")


def plan_identity_repair(
    users: Sequence[Identity], graph: Sequence[GraphNode], also_taken: Collection[int] = ()
) -> IdentityRepair:
    """Rekey every user the gateway cannot carry; drop graph ids no user holds.

    A new id is never any id the stores already mention (users, pending targets, graph
    nodes, follow edges, and also_taken: ids keying posts or timelines), so it inherits no
    data and the orphan cleanup cannot reach it. A user with a recorded target resumes to
    that same target, so an interrupted run converges instead of orphaning its own work.
    """
    by_id = {user.user_id: user.username for user in users}
    if len(by_id) != len(users):
        raise ValueError("eval: social user store holds one user id twice; recreate the application")
    referenced = {node.user_id for node in graph}
    for node in graph:
        referenced.update(node.followers, node.followees)
    taken = set(by_id) | referenced | set(also_taken) | {u.pending for u in users if u.pending is not None}
    rekeyed = []
    for user in sorted(users, key=lambda row: (row.user_id, row.username)):
        if user.pending is not None:
            new_id = user.pending
            if not gateway_carries(new_id):
                raise ValueError(f"eval: social user {user.username} records a rekey target the gateway rounds")
        elif gateway_carries(user.user_id):
            continue
        else:
            new_id = _free_carried_id(user.user_id, taken)
            taken.add(new_id)
        gateway_id = int(float(user.user_id))
        impersonates = by_id.get(gateway_id) if gateway_id != user.user_id else None
        rekeyed.append(Rekey(user.username, user.user_id, new_id, gateway_id, impersonates))
    moving = {row.old_id for row in rekeyed} | {row.new_id for row in rekeyed}
    orphans = tuple(sorted(referenced - set(by_id) - moving))
    return IdentityRepair(len(users), len(graph), tuple(rekeyed), orphans)


def _json_lines(output: str) -> list[dict[str, Any]]:
    return [json.loads(line) for line in output.splitlines() if line.strip()]


_ID_JS = "function id(v){return v.toString().replace(/[^0-9-]/g,'');}"


def read_social_identities(config: Path) -> tuple[list[Identity], list[GraphNode], frozenset[int]]:
    """Read users, the follow graph and timeline owners with exact 64-bit ids (decimal strings)."""
    users_script = (
        f'var c=new Mongo("{USER_MONGO}").getDB("user").user;{_ID_JS}'
        f"c.find({{}}, {{username:1,user_id:1,{PENDING_FIELD}:1}}).forEach(function(d){{"
        f"var p=d.{PENDING_FIELD}; print(JSON.stringify({{username:d.username,user_id:id(d.user_id),"
        "pending:(p===undefined||p===null)?null:id(p)}));});"
    )
    graph_script = (
        f'var c=new Mongo("{GRAPH_MONGO}").getDB("social-graph").getCollection("social-graph");{_ID_JS}'
        "function ids(l){return (l||[]).map(function(e){return id(e.user_id);});}"
        "c.find({}).forEach(function(d){print(JSON.stringify("
        "{user_id:id(d.user_id),followers:ids(d.followers),followees:ids(d.followees)}));});"
    )
    # Every post a user composes enters that user's timeline document, so its owners cover post creators.
    timeline_script = (
        f'var c=new Mongo("{TIMELINE_MONGO}").getDB("user-timeline").getCollection("user-timeline");{_ID_JS}'
        'c.distinct("user_id").forEach(function(v){print(id(v));});'
    )
    with helper_cleanup(config):
        users = [
            Identity(row["username"], int(row["user_id"]), None if row["pending"] is None else int(row["pending"]))
            for row in _json_lines(_mongo(config, users_script))
        ]
        graph = [
            GraphNode(
                int(row["user_id"]),
                tuple(int(value) for value in row["followers"]),
                tuple(int(value) for value in row["followees"]),
            )
            for row in _json_lines(_mongo(config, graph_script))
        ]
        owners = frozenset(int(line) for line in _mongo(config, timeline_script).split())
    return users, graph, owners


# Each step is idempotent and the target is recorded on the user document before anything
# moves, so a run interrupted at any point is resumed by the next one to the same id.
_REPAIR_SCRIPT = f"""var plan=JSON.parse(cat("/dev/stdin"));
var u=new Mongo("{USER_MONGO}").getDB("user").user;
var g=new Mongo("{GRAPH_MONGO}").getDB("social-graph").getCollection("social-graph");
plan.rekey.forEach(function(r){{
  var o=NumberLong(r.old), n=NumberLong(r.new), mark={{}}; mark["{PENDING_FIELD}"]=n;
  assert.eq(u.updateOne({{username:r.username}},{{$set:mark}}).matchedCount, 1);
  if (r.old!=r.new) {{
    if (g.count({{user_id:n}})==0) g.updateOne({{user_id:o}},{{$set:{{user_id:n}}}});
    else g.deleteMany({{user_id:o}});
  }}
  if (g.count({{user_id:n}})==0) g.insertOne({{user_id:n,followers:[],followees:[]}});
  ["followers","followees"].forEach(function(k){{
    var q={{}}; q[k+".user_id"]=o; var s={{}}; s[k+".$[e].user_id"]=n;
    g.updateMany(q,{{$set:s}},{{arrayFilters:[{{"e.user_id":o}}]}});
  }});
  var done={{}}; done["{PENDING_FIELD}"]="";
  assert.eq(u.updateOne({{username:r.username}},{{$set:{{user_id:n}},$unset:done}}).matchedCount, 1);
}});
plan.orphans.forEach(function(v){{
  var o=NumberLong(v);
  g.deleteMany({{user_id:o}});
  g.updateMany({{}},{{$pull:{{followers:{{user_id:o}},followees:{{user_id:o}}}}}});
}});
print("SOCIAL_IDENTITIES_REPAIRED");"""


def _empty_id_caches(config: Path) -> None:
    """Empty the caches keyed by user id or username; their Mongo stores refill them."""
    if _exec(config, "social-graph-redis", ["redis-cli", "FLUSHALL"]).strip() != "OK":
        raise RuntimeError("eval: social identity restore could not empty social-graph-redis")
    selector = json.loads(
        kubectl(
            ["get", "deployment/user-memcached", "-o", "jsonpath={.spec.selector.matchLabels}"],
            namespace=NAMESPACE,
            kubeconfig=config,
        ).stdout
    )
    labels = ",".join(f"{key}={value}" for key, value in sorted(selector.items()))
    pods = kubectl(["get", "pods", "-l", labels, "-o", "name"], namespace=NAMESPACE, kubeconfig=config).stdout.split()
    if not pods:
        raise RuntimeError(f"eval: social identity restore found no user-memcached pod for selector {labels}")
    # Deleting the pods empties memcached without changing the Deployment the baseline captured.
    kubectl(["delete", *pods, "--wait=true"], namespace=NAMESPACE, kubeconfig=config)
    kubectl(["rollout", "status", "deployment/user-memcached", "--timeout=90s"], namespace=NAMESPACE, kubeconfig=config)


def restore_social_identities(config: Path) -> IdentityRepair:
    """Hold the identity invariant; call only while no prober runs (after the SDO namespace wipe).

    A rekeyed user keeps its username, password hash and follow edges under an id the
    gateway carries. Posts and timelines under the old id stay: nothing reads them under the
    new id, which owns none, and removing them would dangle the home-timeline references
    the post fixture keeps coherent. Session tokens carrying the old id died with the prober.
    The id caches are emptied on every run, so a run that failed after its Mongo rewrite
    cannot leave stale entries behind a store that now looks clean.
    """
    users, graph, owners = read_social_identities(config)
    repair = plan_identity_repair(users, graph, owners)
    if repair:
        plan = {
            "rekey": [
                {"username": row.username, "old": str(row.old_id), "new": str(row.new_id)} for row in repair.rekeyed
            ],
            "orphans": [str(value) for value in repair.orphans],
        }
        with helper_cleanup(config):
            if "SOCIAL_IDENTITIES_REPAIRED" not in _mongo(config, _REPAIR_SCRIPT, json.dumps(plan)):
                raise RuntimeError("eval: social identity repair did not complete")
    _empty_id_caches(config)
    return repair
