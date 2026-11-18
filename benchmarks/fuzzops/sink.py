"""SDO's run evidence sink as fuzzops-bench sees it on the host: transcript coverage and the per-run copy.

Each lane mounts one host directory (``BENCH_EVIDENCE_HOST``) on every kind node at ``BENCH_EVIDENCE_NODE``; the
``prepare`` hook installs SDO with that node path as its evidence sink and ``BENCH_EVIDENCE_RUN`` as the sink run ID, so
the controller's evidence sidecar mirrors rollouts, task records and broker ledgers into
``$BENCH_EVIDENCE_HOST/$BENCH_EVIDENCE_RUN/`` while the lane runs (layout ``sdo.evidence/v1``, ``docs/architecture.md``
"Run evidence sink"). Everything here reads only that host directory; it never touches the cluster.

An incident's transcripts are its ``rollout-*.jsonl`` files anywhere below ``incidents/<sha256(id)[:32]>/``. An
incident directory with a ``ledger.json`` must hold at least one, unless the ledger shows that no agent ran
(``agents_ran``).
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import subprocess
import zlib
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping, Sequence

LAYOUT_SCHEMA = "sdo.evidence/v1"
LAYOUT_FILE = "layout.json"
INDEX_FILE = "index.jsonl"
LEDGER_FILE = "ledger.json"
IDENTITY_FILE = "incident.json"
ROLLOUT_GLOB = "rollout-*.jsonl"
CLAUDE_PROJECTS = Path("claude") / "projects"
INCIDENTS = "incidents"
TASKS = "lifecycle"
#: Reflection failures that end the task before any agent turn starts.
_PRE_START_FAILURES = frozenset({"session_unavailable", "session_handoff_invalid", "request_too_large"})
_FRACTION = re.compile(r"(\.\d{6})\d+")
#: An investigator's session ID is ``<incident id>-inv-<n>`` (``investigatorID`` in the operator's runtime); its
#: trailing number is an index, never a detection time.
_INVESTIGATOR_SUFFIX = re.compile(r"-inv-\d+$")


class SinkError(RuntimeError):
    """The lane's evidence sink is not configured or not readable on the host."""


_RUN_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")


def _need(env: Mapping[str, str], *names: str) -> list[str]:
    if missing := [n for n in names if not env.get(n)]:
        raise SinkError(f"{', '.join(missing)} not set (every lane needs an evidence sink)")
    values = [env[n] for n in names]
    if "BENCH_EVIDENCE_RUN" in names and not _RUN_ID.match(env["BENCH_EVIDENCE_RUN"]):
        raise SinkError(f"BENCH_EVIDENCE_RUN {env['BENCH_EVIDENCE_RUN']!r} is not an evidence run ID")
    return values


#: A lane without a host mount (provider vcluster) keeps its evidence on this claim of the control namespace and the
#: hooks pull it into the host directory (``BENCH_EVIDENCE_MODE=pull``).
EVIDENCE_CLAIM = "sdo-evidence"
EVIDENCE_CLAIM_SIZE = "5Gi"
PULL_IMAGE = "busybox:1.36"
PULL_UID = 65532
PULL_READY_TIMEOUT = "180s"


def pull_mode(env: Mapping[str, str]) -> bool:
    """Whether the lane's evidence is pulled from the cluster instead of written to a mounted host directory."""
    return env.get("BENCH_EVIDENCE_MODE", "") == "pull"


def claim_manifest(control: str, node: str = "") -> dict[str, Any]:
    """The PersistentVolumeClaim that holds the sink of a pull-mode lane, bound on ``node`` when one is given."""
    metadata: dict[str, Any] = {"name": EVIDENCE_CLAIM, "namespace": control}
    if node:
        metadata["annotations"] = {"volume.kubernetes.io/selected-node": node}
    return {
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": metadata,
        "spec": {"accessModes": ["ReadWriteOnce"], "resources": {"requests": {"storage": EVIDENCE_CLAIM_SIZE}}},
    }


def volume_node(nodes: Mapping[str, Any], lane: str) -> str:
    """The node a lane's node-local volumes live on: one Ready, schedulable node of ``kubectl get nodes -o json``,
    chosen by the lane's name so that lanes spread over the nodes and a lane's choice never changes. All claims of an
    operator that mounts several (repository, evidence) must sit on one node, or its pod cannot be scheduled."""
    ready = sorted(
        item["metadata"]["name"]
        for item in nodes.get("items", [])
        if not item.get("spec", {}).get("unschedulable")
        and any(
            c.get("type") == "Ready" and c.get("status") == "True" for c in item.get("status", {}).get("conditions", [])
        )
    )
    if not ready:
        raise SinkError("no schedulable Ready node for the lane's volumes")
    return ready[zlib.crc32(lane.encode()) % len(ready)]


def puller_manifest(control: str, name: str) -> dict[str, Any]:
    """A throwaway pod that mounts the sink claim read-only (restricted pod security compatible)."""
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": name, "namespace": control},
        "spec": {
            "restartPolicy": "Never",
            "terminationGracePeriodSeconds": 0,
            "securityContext": {
                "runAsNonRoot": True,
                "runAsUser": PULL_UID,
                "seccompProfile": {"type": "RuntimeDefault"},
            },
            "containers": [
                {
                    "name": "pull",
                    "image": PULL_IMAGE,
                    "command": ["sleep", "600"],
                    "securityContext": {"allowPrivilegeEscalation": False, "capabilities": {"drop": ["ALL"]}},
                    "volumeMounts": [{"name": "sink", "mountPath": "/sink", "readOnly": True}],
                }
            ],
            "volumes": [{"name": "sink", "persistentVolumeClaim": {"claimName": EVIDENCE_CLAIM, "readOnly": True}}],
        },
    }


def pusher_manifest(control: str, name: str) -> dict[str, Any]:
    """A throwaway pod that mounts the sink claim writable (restricted pod security compatible)."""
    manifest = puller_manifest(control, name)
    container = manifest["spec"]["containers"][0]
    container["volumeMounts"] = [{"name": "sink", "mountPath": "/sink"}]
    manifest["spec"]["volumes"] = [{"name": "sink", "persistentVolumeClaim": {"claimName": EVIDENCE_CLAIM}}]
    return manifest


def push_paths(
    kube_cmd: Callable[..., list[str]], control: str, root: Path, paths: Sequence[str], wipe: Sequence[str] = ()
) -> None:
    """Unpack ``paths`` (relative to ``root``) into the lane's evidence claim, after emptying the ``wipe`` directories.

    A directory is emptied in place, never removed: a pod that already mounts it by subPath keeps seeing it. The
    claim is node-local, so the pod lands on the node the claim is bound to (or binds it). Raises ``SinkError``."""
    name = f"claim-push-{os.getpid()}"

    def kube(*args: str, **kw: Any) -> subprocess.CompletedProcess[bytes]:
        return subprocess.run(kube_cmd("-n", control, *args), capture_output=True, check=False, **kw)

    try:
        created = subprocess.run(
            kube_cmd("apply", "-f", "-"),
            input=json.dumps(pusher_manifest(control, name)).encode(),
            capture_output=True,
            check=False,
            timeout=120,
        )
        if created.returncode != 0:
            raise SinkError(f"claim push pod: {created.stderr.decode()[-200:].strip()}")
        ready = kube("wait", f"pod/{name}", "--for=condition=Ready", f"--timeout={PULL_READY_TIMEOUT}", timeout=240)
        if ready.returncode != 0:
            raise SinkError(f"claim push pod not ready: {ready.stderr.decode()[-200:].strip()}")
        script = "set -e; " + "; ".join(f"mkdir -p /sink/{d}; find /sink/{d} -mindepth 1 -delete" for d in wipe)
        if wipe:
            emptied = kube("exec", name, "-c", "pull", "--", "sh", "-c", script, timeout=300)
            if emptied.returncode != 0:
                raise SinkError(f"claim push of {', '.join(paths)}: {emptied.stderr.decode()[-200:].strip()}")
        packed = subprocess.Popen(["tar", "-C", str(root), "-cf", "-", *paths], stdout=subprocess.PIPE)
        unpacked = subprocess.run(
            kube_cmd("-n", control, "exec", "-i", name, "-c", "pull", "--", "tar", "-C", "/sink", "-xf", "-"),
            stdin=packed.stdout,
            capture_output=True,
            check=False,
            timeout=1200,
        )
        assert packed.stdout is not None
        packed.stdout.close()
        if packed.wait() != 0 or unpacked.returncode != 0:
            raise SinkError(
                f"claim push of {', '.join(paths)}: {unpacked.stderr.decode()[-200:].strip() or 'tar failed'}"
            )
    finally:
        kube("delete", "pod", name, "--ignore-not-found", "--wait=false", "--grace-period=0", "--force", timeout=120)


def pull_run_dir(env: Mapping[str, str], kube_cmd: Callable[..., list[str]], control: str) -> None:
    """Copy the sink run directory out of the lane's evidence claim into ``host_run_dir(env)``.

    A pod mounts the claim (it shares the node of the controller's pod, the claim being node-local), ``tar`` streams
    the run directory out and the pod is deleted. Safe to repeat: files are overwritten, the sink only appends.
    Raises ``SinkError`` when the claim, the pod or the run directory is not there."""
    run = _need(env, "BENCH_EVIDENCE_RUN")[0]
    dest = host_run_dir(env)
    name = f"evidence-pull-{os.getpid()}"
    manifest = json.dumps(puller_manifest(control, name))

    def kube(*args: str, **kw: Any) -> subprocess.CompletedProcess[bytes]:
        return subprocess.run(kube_cmd("-n", control, *args), capture_output=True, check=False, **kw)

    try:
        created = subprocess.run(
            kube_cmd("apply", "-f", "-"), input=manifest.encode(), capture_output=True, check=False, timeout=120
        )
        if created.returncode != 0:
            raise SinkError(f"evidence pull pod: {created.stderr.decode()[-200:].strip()}")
        ready = kube("wait", f"pod/{name}", "--for=condition=Ready", f"--timeout={PULL_READY_TIMEOUT}", timeout=240)
        if ready.returncode != 0:
            raise SinkError(f"evidence pull pod not ready: {ready.stderr.decode()[-200:].strip()}")
        dest.mkdir(parents=True, exist_ok=True)
        tar = subprocess.Popen(
            kube_cmd("-n", control, "exec", name, "-c", "pull", "--", "tar", "-C", f"/sink/{run}", "-cf", "-", "."),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        extract = subprocess.run(
            ["tar", "-C", str(dest), "-xf", "-"], stdin=tar.stdout, capture_output=True, check=False, timeout=600
        )
        assert tar.stdout is not None
        tar.stdout.close()
        err = (tar.stderr.read() if tar.stderr else b"").decode()[-200:].strip()
        if tar.wait() != 0 or extract.returncode != 0:
            raise SinkError(f"evidence pull of {run}: {err or extract.stderr.decode()[-200:].strip()}")
    finally:
        kube("delete", "pod", name, "--ignore-not-found", "--wait=false", "--grace-period=0", "--force", timeout=120)


def install_env(env: Mapping[str, str]) -> dict[str, str]:
    """The ``install-existing`` variables that point SDO's evidence sink at the lane's node mount (or, in pull mode,
    at the lane's evidence claim)."""
    if pull_mode(env):
        (run,) = _need(env, "BENCH_EVIDENCE_RUN")
        volume = json.dumps({"persistentVolumeClaim": {"claimName": EVIDENCE_CLAIM}}, separators=(",", ":"))
        return {"SDO_EVIDENCE_VOLUME": volume, "SDO_EVIDENCE_RUN_ID": run}
    node, run = _need(env, "BENCH_EVIDENCE_NODE", "BENCH_EVIDENCE_RUN")
    if not node.startswith("/"):
        raise SinkError(f"BENCH_EVIDENCE_NODE must be an absolute node path, got {node!r}")
    return {"SDO_EVIDENCE_HOST_PATH": node, "SDO_EVIDENCE_RUN_ID": run}


def host_run_dir(env: Mapping[str, str]) -> Path:
    """``$BENCH_EVIDENCE_HOST/$BENCH_EVIDENCE_RUN``: this build's sink run directory on the host."""
    host, run = _need(env, "BENCH_EVIDENCE_HOST", "BENCH_EVIDENCE_RUN")
    if not Path(host).is_absolute():
        raise SinkError(f"BENCH_EVIDENCE_HOST must be an absolute host directory, got {host!r}")
    return Path(host) / run


def layout_problem(run_dir: Path) -> str:
    """Why ``run_dir`` is not an ``sdo.evidence/v1`` run directory ("" when it is)."""
    layout = run_dir / LAYOUT_FILE
    try:
        value = json.loads(layout.read_text())
    except FileNotFoundError:
        return f"{layout} does not exist"
    except (OSError, json.JSONDecodeError) as exc:
        return f"{layout} is unreadable: {exc}"
    if not isinstance(value, dict) or value.get("schema") != LAYOUT_SCHEMA:
        schema = value.get("schema") if isinstance(value, dict) else None
        return f"{layout} has schema {schema!r}, not {LAYOUT_SCHEMA}"
    return ""


# ---- ledgers -------------------------------------------------------------------------------------------------


def _get(value: Any, name: str) -> Any:
    """A ProtoJSON field under its proto name (``outcome_document``) or its JSON name (``outcomeDocument``)."""
    if not isinstance(value, dict):
        return None
    if name in value:
        return value[name]
    head, *rest = name.split("_")
    return value.get(head + "".join(p.title() for p in rest))


def _number(value: Any) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _used(usage: Any) -> bool:
    """Whether a ``UsageMetrics`` records any model call or token."""
    names = ("llm_calls", "input_tokens", "output_tokens", "model_requests", "total_cost_usd")
    return any(_number(_get(usage, n)) > 0 for n in names)


#: ``no_change_reason`` values Go sets without starting a closing turn (``runtime/closing_turn.go``
#: ``closingSkipReason``, ``runtime/native_broker.go`` covered-repeat shortcut and ``no_session``). Such a decision
#: with no usage is a skipped turn: no session, no ``reflection-usage.jsonl``, a ``reflection_without_commit`` stamp a
#: fraction of a second after the dispatch stamp.
_TURN_SKIPPED = frozenset(
    {"closing_turn_nothing_to_script", "escalated_no_learning", "accepted_programs_no_new_evidence", "no_session"}
)


def _reflection_ran(decision: Any) -> bool:
    if not isinstance(decision, dict) or not decision:
        return False
    if _used(_get(decision, "usage")):
        return True
    if _get(decision, "no_change_reason") in _TURN_SKIPPED:
        return False
    failure = str(_get(decision, "failure_reason") or "")
    # A completed reflection dispatch with no usage and no pre-start failure may still have run a turn.
    return failure not in _PRE_START_FAILURES


def agents_ran(ledger: Mapping[str, Any]) -> bool:
    """Whether a responder or reflection agent ran (or may have run) for this broker ledger.

    Conservative: True unless the ledger proves no agent ran. It proves that only when the incident has a closure
    whose responder result is absent because dispatch failed, or is present with no responder session, a
    ``resolution_path`` other than ``responder`` and no usage; the outcome document records no responder resolution,
    usage or response loop; and no reflection was dispatched (no ``reflection_dispatch_at``, phase not ``DISPATCH``)
    and every recorded reflection result has no usage and failed before its session started. An incident without a
    closure (still open or cut off) counts as having run one.
    """
    closure = _get(ledger, "closure")
    if not isinstance(closure, dict) or not closure:
        return True
    result = _get(closure, "result")
    if isinstance(result, dict) and result:
        if _get(result, "responder_session_id") or _get(result, "resolution_path") == "responder":
            return True
        if _used(_get(result, "usage")) or _number(_get(result, "response_loops_used")) > 0:
            return True
    elif not _get(closure, "dispatch_error"):
        return True
    outcome = _get(ledger, "outcome_document")
    if isinstance(outcome, dict):
        if _get(outcome, "resolution_path") == "responder" or _used(_get(outcome, "usage")):
            return True
        if _number(_get(outcome, "response_loops_used")) > 0:
            return True
    decisions = [_get(ledger, "reflection_decision"), *(_get(ledger, "reflection_results") or [])]
    if _get(ledger, "reflection_dispatch_at") and not _only_skipped_turns(decisions):
        return True
    phase = _get(ledger, "reflection_phase")
    if phase in ("REFLECTION_PHASE_DISPATCH", 2):
        return True
    return any(_reflection_ran(d) for d in decisions)


def _only_skipped_turns(decisions: list[Any]) -> bool:
    """Whether every recorded closing decision is one Go made without a turn (and at least one exists)."""
    recorded = [d for d in decisions if isinstance(d, dict) and d]
    return bool(recorded) and all(
        not _used(_get(d, "usage")) and _get(d, "no_change_reason") in _TURN_SKIPPED for d in recorded
    )


def closing_turn_ran(ledger: Mapping[str, Any]) -> bool:
    """Whether a closing (reflection) agent turn ran for this ledger, as opposed to Go deciding without one."""
    decisions = [_get(ledger, "reflection_decision"), *(_get(ledger, "reflection_results") or [])]
    return any(_reflection_ran(d) for d in decisions)


def has_outcome(ledger: Mapping[str, Any]) -> bool:
    outcome = _get(ledger, "outcome_document")
    return isinstance(outcome, dict) and bool(outcome)


def _read_json(path: Path) -> dict[str, Any] | None:
    try:
        value = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) else None


@dataclass(frozen=True)
class IncidentCoverage:
    """One incident directory's transcripts."""

    id: str
    outcome: bool
    agents_ran: bool
    rollouts: int
    #: The directory name (``sha256(incident_id)[:32]``); not serialized.
    dir: str = ""

    @property
    def needs_rollout(self) -> bool:
        return self.agents_ran and self.rollouts == 0

    def to_json(self) -> dict[str, Any]:
        return {k: v for k, v in asdict(self).items() if k != "dir"}


def rollout_files(directory: Path) -> list[Path]:
    """Codex rollouts below ``directory``."""
    return sorted(p for p in directory.rglob(ROLLOUT_GLOB) if p.is_file())


def claude_session_files(directory: Path) -> list[Path]:
    """Claude session files, ``claude/projects/<cwd>/<session>.jsonl``."""
    return sorted(p for p in (directory / CLAUDE_PROJECTS).rglob("*.jsonl") if p.is_file())


def count_rollouts(directory: Path) -> int:
    """Transcripts of one incident: Codex rollouts plus Claude session files, once each."""
    return len({*rollout_files(directory), *claude_session_files(directory)})


def incident_coverage(directory: Path) -> IncidentCoverage:
    ledger = _read_json(directory / LEDGER_FILE) or {}
    identity = _read_json(directory / IDENTITY_FILE) or {}
    incident = str(_get(ledger, "incident_id") or _get(identity, "incident_id") or directory.name)
    return IncidentCoverage(
        id=incident,
        outcome=has_outcome(ledger),
        agents_ran=agents_ran(ledger) if ledger else True,
        rollouts=count_rollouts(directory),
        dir=directory.name,
    )


def incident_dirs(run_dir: Path) -> list[Path]:
    root = run_dir / INCIDENTS
    return sorted(p for p in root.iterdir() if p.is_dir()) if root.is_dir() else []


def short_ids(missing: Iterable[IncidentCoverage], limit: int = 5) -> str:
    ids = [m.dir[:8] or m.id[:8] for m in missing]
    return ", ".join(ids[:limit]) + (f", +{len(ids) - limit}" if len(ids) > limit else "")


def transcripts_verdict(run_dir: Path) -> str:
    """The ``transcripts`` hook's answer: "" when the sink is in place and every incident directory with a ledger
    holds a rollout unless no agent ran; else ``no sink: <why>`` or ``transcripts missing: N (<short ids>)``."""
    if why := layout_problem(run_dir):
        return f"no sink: {why}"
    missing = [
        c
        for d in incident_dirs(run_dir)
        if (d / LEDGER_FILE).is_file()
        for c in [incident_coverage(d)]
        if c.agents_ran and c.rollouts == 0
    ]
    if missing:
        return f"transcripts missing: {len(missing)} ({short_ids(missing)})"
    return ""


# ---- interval copy -------------------------------------------------------------------------------------------


def parse_time(value: Any) -> datetime | None:
    """An RFC 3339 time (nanosecond fractions truncated); None when absent or malformed."""
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(_FRACTION.sub(r"\1", value.strip()).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def read_index(run_dir: Path) -> list[dict[str, Any]]:
    """Every whole JSON record of ``index.jsonl`` (a torn line is skipped)."""
    try:
        lines = (run_dir / INDEX_FILE).read_text().splitlines()
    except OSError:
        return []
    out = []
    for line in lines:
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict) and isinstance(value.get("path"), str):
            out.append(value)
    return out


def _newest_mtime(directory: Path) -> datetime | None:
    stamps = [p.stat().st_mtime for p in directory.rglob("*") if p.is_file() and not p.is_symlink()]
    return datetime.fromtimestamp(max(stamps), UTC) if stamps else None


def incident_dir_name(incident_id: str) -> str:
    """The sink directory of an incident or investigator session: ``sha256(incident_id)[:32]``."""
    return hashlib.sha256(incident_id.encode()).hexdigest()[:32]


def parent_incident_id(incident_id: str) -> str:
    """The incident an investigator session works for: ``incident_id`` without its ``-inv-<n>`` suffix; any other ID
    unchanged."""
    return _INVESTIGATOR_SUFFIX.sub("", incident_id)


def incident_identity(directory: Path) -> str:
    """The incident ID recorded in a ledger-less incident directory (``incident.json``); empty when unknown."""
    return str(_get(_read_json(directory / IDENTITY_FILE), "incident_id") or "")


def is_investigator_dir(directory: Path) -> bool:
    """Whether an incident directory is an investigator's own session (``<incident>-inv-<n>``), not an incident."""
    return bool(_INVESTIGATOR_SUFFIX.search(incident_identity(directory)))


def incident_detected_at(directory: Path) -> datetime | None:
    """When the incident was detected, from its ID (``<namespace>-<unix nanoseconds>``); for a ledger-less directory.
    An investigator directory takes its parent incident's time."""
    suffix = parent_incident_id(incident_identity(directory)).rsplit("-", 1)[-1]
    return datetime.fromtimestamp(int(suffix) / 1e9, UTC) if suffix.isdigit() else None


def leftover(directory: Path, records: list[dict[str, Any]], start: datetime | None) -> bool:
    """Whether an incident directory is only the late writes of an incident from before ``start`` (an earlier cell's):
    no ledger, no sink index record, detected before the interval. The ready gate needs the operator idle before every
    inject, so nothing detected earlier is open at ``start`` and the directory belongs to the earlier cell."""
    if start is None or records or (directory / LEDGER_FILE).is_file():
        return False
    detected = incident_detected_at(directory)
    return detected is not None and detected < start


def _within(at: datetime | None, start: datetime, end: datetime) -> bool:
    return at is not None and start <= at <= end


def incident_in_interval(
    directory: Path, records: list[dict[str, Any]], start: datetime | None, end: datetime | None
) -> bool:
    """Whether an incident belongs to ``[start, end]``: detected in it, open during it, verified, acknowledged,
    reflected or sealed in it. Inclusive: an incident whose times are unknown is included when its files changed
    since ``start``, or always when it has none."""
    if start is None and end is None:
        return True
    lo = start or datetime.min.replace(tzinfo=UTC)
    hi = end or datetime.max.replace(tzinfo=UTC)
    ledger = _read_json(directory / LEDGER_FILE)
    closure = _get(ledger, "closure") if ledger else None
    detected = parse_time(_get(closure, "detected_at"))
    acknowledged = parse_time(_get(ledger, "acknowledged_at")) if ledger else None
    times = [
        detected,
        acknowledged,
        parse_time(_get(closure, "verified_at")),
        parse_time(_get(ledger, "reflection_dispatch_at")) if ledger else None,
        parse_time(_get(ledger, "reflection_queued_at")) if ledger else None,
    ]
    for record in records:
        detected = detected or parse_time(record.get("detected_at"))
        acknowledged = acknowledged or parse_time(record.get("acknowledged_at"))
        times += [parse_time(record.get(k)) for k in ("detected_at", "verified_at", "acknowledged_at", "sealed_at")]
    if any(_within(t, lo, hi) for t in times):
        return True
    if leftover(directory, records, start):
        return False
    if detected is not None:
        # Open during the interval: detected before its end and not acknowledged before its start.
        return detected <= hi and (acknowledged is None or acknowledged >= lo)
    newest = _newest_mtime(directory)
    return newest is None or newest >= lo


def task_in_interval(
    directory: Path, records: list[dict[str, Any]], start: datetime | None, end: datetime | None
) -> bool:
    """Whether a lifecycle task belongs to ``[start, end]``: sealed or written in it, or still unsealed and written
    since ``start``."""
    if start is None and end is None:
        return True
    lo = start or datetime.min.replace(tzinfo=UTC)
    hi = end or datetime.max.replace(tzinfo=UTC)
    sealed = [parse_time(r.get("sealed_at")) for r in records]
    if any(_within(t, lo, hi) for t in sealed):
        return True
    newest = _newest_mtime(directory)
    if newest is None:
        return False
    return _within(newest, lo, hi) or (not any(sealed) and newest >= lo)


@dataclass
class CopyResult:
    incidents: list[IncidentCoverage]
    tasks: int
    index_records: int

    def summary(self) -> dict[str, Any]:
        return {
            "incidents": [c.to_json() for c in self.incidents],
            "rollouts": sum(c.rollouts for c in self.incidents),
            "missing": [c.id for c in self.incidents if c.outcome and c.agents_ran and c.rollouts == 0],
        }


def _copy_tree(src: Path, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(src, dest, symlinks=True, dirs_exist_ok=True)


def copy_interval(run_dir: Path, dest: Path, start: datetime | None, end: datetime | None) -> CopyResult:
    """Copy the incidents and lifecycle tasks of ``[start, end]`` from the host sink run directory into ``dest``
    (relative paths kept) with their ``index.jsonl`` records; returns the copied incidents' coverage."""
    if why := layout_problem(run_dir):
        raise SinkError(f"no sink: {why}")
    records = read_index(run_dir)
    by_path: dict[str, list[dict[str, Any]]] = {}
    for record in records:
        by_path.setdefault(record["path"].strip("/"), []).append(record)
    dest.mkdir(parents=True, exist_ok=True)
    shutil.copy2(run_dir / LAYOUT_FILE, dest / LAYOUT_FILE)
    copied: set[str] = set()
    incidents = []
    directories = incident_dirs(run_dir)
    in_interval: dict[str, bool] = {}
    # Incidents first: an investigator directory follows its parent's membership (when the parent is in the sink).
    for directory in sorted(directories, key=is_investigator_dir):
        rel = f"{INCIDENTS}/{directory.name}"
        parent = incident_dir_name(parent_incident_id(incident_identity(directory)))
        if is_investigator_dir(directory) and parent in in_interval:
            in_interval[directory.name] = in_interval[parent]
        else:
            in_interval[directory.name] = incident_in_interval(directory, by_path.get(rel, []), start, end)
    for directory in directories:
        rel = f"{INCIDENTS}/{directory.name}"
        if in_interval[directory.name]:
            _copy_tree(directory, dest / rel)
            copied.add(rel)
            incidents.append(incident_coverage(dest / rel))
    tasks = 0
    task_root = run_dir / TASKS
    for directory in sorted(p for p in task_root.iterdir() if p.is_dir()) if task_root.is_dir() else []:
        rel = f"{TASKS}/{directory.name}"
        if task_in_interval(directory, by_path.get(rel, []), start, end):
            _copy_tree(directory, dest / rel)
            copied.add(rel)
            tasks += 1
    kept = [r for r in records if r["path"].strip("/") in copied]
    tmp = dest / (INDEX_FILE + ".tmp")
    tmp.write_text("".join(json.dumps(r, sort_keys=True) + "\n" for r in kept))
    os.replace(tmp, dest / INDEX_FILE)
    return CopyResult(incidents, tasks, len(kept))
