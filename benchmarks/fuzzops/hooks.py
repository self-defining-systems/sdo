"""SDO's operator hooks for fuzzops-bench: restore SDO from a baseline on a lane, tell the bench when it is idle,
watch each fault window, collect its artifacts, and remove it again.

``python -m benchmarks.fuzzops.hooks <verb>``, run by the bench with the cwd at the pinned SDO checkout. A hook only
ever touches its own lane. Verbs (environment: ``BENCH_*`` and ``FUZZOPS_*``, see the bench's hook contract):

``check``     no cluster access: the baseline manifest is well formed, names ``FUZZOPS_NAMESPACE``, its render file
              matches its digest and its bundle holds the branch at the commit.
``prepare``   on a fresh app copy: build the plain-manifest pack from the live app (the namespace's one Helm release,
              or the pack's own ``manifests/``), refuse a render that differs from the baseline's, write the attached
              pack, build ``sdo-operator``, load the operator images onto the worker nodes, provision the namespaces,
              clone the baseline memory and run production ``sdo-operator install-existing``. The last stdout line is
              ``{"pack", "operator_version", "memory"}``.
``idle``      exit 0 when the controller pod is Ready, its state ConfigMap changed within 60 s and SDO owns no incident
              work; otherwise exit 1 with the reason on the first line (``alarm: incident open <id>`` for an incident).
``watch``     the fault window's ``exec:`` command: ``benchmarks.fuzzops.observe``.
``collect``   best effort: accepted outcomes and playbooks, the controller state, controller logs since the inject and
              a runtime evidence export into ``BENCH_OUT``.
``teardown``  delete the control namespace and the cluster grant install-existing made for it.

``baseline`` (SDO only) archives a running lane's accepted memory and render into a new baseline directory.

Baseline directory: ``baseline.json`` (``Baseline``) beside the memory bundle and the render it was made on::

    {"format": 1, "namespace": "<app namespace>", "control_namespace": "<ns>-sdo" (optional),
     "application": "<ns>" (optional), "bundle": "memory.bundle", "branch": "<branch>", "commit": "<40 hex>",
     "render": "<path of the plain render>", "render_sha256": "<hex>", "objective": "<health objective>",
     "controller_flags": ["--response-timeout", "1800s", ...], "sdo_revision": "<SDO commit it was made with>"}

Paths are relative to the baseline directory. ``controller_flags`` are extra ``install-existing`` flags; the flags the
hook sets itself (``HOOK_FLAGS``) are refused there.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml

from benchmarks.fuzzops import evidence, observe
from benchmarks.fuzzops.install import (
    CREDENTIALS_SECRET,
    REPOSITORY_PVC,
    InstallConfig,
    declare_node_images,
    provision_namespaces,
)
from benchmarks.fuzzops.pack import SourcePack, images, write_attached_pack

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

BASELINE_FILE = "baseline.json"
BASELINE_FORMAT = 1
STATE_CONFIGMAP = observe.STATE_CONFIGMAP
STATE_KEY = observe.STATE_KEY
CONTROLLER_JOB = "sdo-controller-run"
CONTROLLER_CONTAINER = "controller"
APPLICATION_ROOT = "/workspace/application"
MEMORY_PATHS = (".sdo/outcomes", ".sdo/playbooks")
#: The controller state must have changed this recently for SDO to count as live.
FRESH_S = 60.0
KUBECTL_TIMEOUT_S = 25
#: ``install-existing`` flags the hook sets from the lane, the operator block and the baseline.
HOOK_FLAGS = frozenset(
    {
        "--repository",
        "--namespace",
        "--control-namespace",
        "--application",
        "--controller-image",
        "--responder-image",
        "--script-image",
        "--repository-pvc",
        "--credentials-secret",
        "--agent-provider",
        "--model",
        "--agent-effort",
        "--kubeconfig",
    }
)
#: Controller flags a new baseline records unless told otherwise (the flags SDO's fuzzops lanes ran with).
DEFAULT_CONTROLLER_FLAGS = (
    "--response-timeout",
    "1800s",
    "--verification-timeout",
    "300s",
    "--reflection-session",
    "resume",
    "--reflection-guidance",
    "baseline",
    "--max-response-loops",
    "10",
    "--repair-policy",
    "recorded-actions",
)
_DNS_LABEL = re.compile(r"^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$")
_COMMIT = re.compile(r"^[0-9a-f]{40}$")


class HookError(RuntimeError):
    """A hook cannot do its job on this lane; the message is the one-line reason the bench records."""


def _print(line: str) -> None:
    print(line, flush=True)


# ---- baseline manifest ---------------------------------------------------------------------------------------


def _relative(name: str, value: str) -> None:
    path = Path(value)
    if not value or path.is_absolute() or ".." in path.parts:
        raise ValueError(f"{name} must be a path inside the baseline directory, got {value!r}")


@dataclass(frozen=True)
class Baseline:
    """``baseline.json``: the accepted memory one SDO lane is restored from and the render it was made on."""

    namespace: str
    bundle: str
    branch: str
    commit: str
    render: str
    control_namespace: str = ""
    application: str = ""
    objective: str = ""
    controller_flags: tuple[str, ...] = ()
    render_sha256: str = ""
    sdo_revision: str = ""
    format: int = BASELINE_FORMAT
    #: The baseline directory (not serialized).
    root: Path | None = field(default=None, compare=False)

    def __post_init__(self) -> None:
        if self.format != BASELINE_FORMAT:
            raise ValueError(f"baseline format {self.format} is not {BASELINE_FORMAT}")
        if not _DNS_LABEL.match(self.namespace):
            raise ValueError(f"namespace {self.namespace!r} is not a DNS label")
        if self.control_namespace and not _DNS_LABEL.match(self.control_namespace):
            raise ValueError(f"control_namespace {self.control_namespace!r} is not a DNS label")
        if self.control == self.namespace:
            raise ValueError("the control namespace must differ from the application namespace")
        if not _COMMIT.match(self.commit):
            raise ValueError(f"commit must be 40 hex digits, got {self.commit!r}")
        if not self.branch or self.branch.startswith("-") or " " in self.branch:
            raise ValueError(f"branch {self.branch!r} is not a branch name")
        _relative("bundle", self.bundle)
        _relative("render", self.render)
        if not isinstance(self.controller_flags, tuple) or not all(isinstance(f, str) for f in self.controller_flags):
            raise TypeError("controller_flags must be a tuple of strings")
        for flag in self.controller_flags:
            if flag.split("=", 1)[0] in HOOK_FLAGS:
                raise ValueError(f"controller flag {flag} is set by the hook, not the baseline")
        if self.controller_flags and not self.controller_flags[0].startswith("--"):
            raise ValueError("controller_flags must start with a flag")

    @property
    def control(self) -> str:
        return self.control_namespace or f"{self.namespace}-sdo"

    @property
    def app_name(self) -> str:
        return self.application or self.namespace

    def path(self, relative: str) -> Path:
        if self.root is None:
            raise ValueError("a baseline read from a directory is needed to resolve its files")
        return self.root / relative

    @classmethod
    def load(cls, root: Path) -> Baseline:
        manifest = root / BASELINE_FILE
        try:
            raw = json.loads(manifest.read_text())
        except FileNotFoundError as exc:
            raise ValueError(f"{manifest} is missing") from exc
        except json.JSONDecodeError as exc:
            raise ValueError(f"{manifest} is not JSON: {exc}") from exc
        if not isinstance(raw, dict):
            raise ValueError(f"{manifest} is not a JSON object")
        known = {f for f in cls.__dataclass_fields__ if f != "root"}
        if unknown := set(raw) - known:
            raise ValueError(f"{manifest} has unknown fields {sorted(unknown)}")
        raw["controller_flags"] = tuple(raw.get("controller_flags") or ())
        try:
            return cls(**raw, root=root)
        except TypeError as exc:
            raise ValueError(f"{manifest}: {exc}") from exc

    def to_json(self) -> str:
        data = {k: v for k, v in asdict(self).items() if k != "root"}
        data["controller_flags"] = list(self.controller_flags)
        return json.dumps(data, indent=2) + "\n"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_baseline(root: Path, baseline: Baseline) -> Baseline:
    """Write ``root/baseline.json``, recording the render's sha256."""
    render = root / baseline.render
    if not render.is_file():
        raise ValueError(f"render {render} is missing")
    fields = {k: v for k, v in asdict(baseline).items() if k != "root"}
    fields["controller_flags"] = baseline.controller_flags
    fields["render_sha256"] = sha256_file(render)
    written = Baseline(**fields, root=root)
    (root / BASELINE_FILE).write_text(written.to_json())
    return written


def bundle_problem(bundle: Path, branch: str, commit: str) -> str:
    """Why the bundle does not hold ``branch`` at ``commit`` ("" when it does)."""
    if not bundle.is_file():
        return f"bundle {bundle} is missing"
    proc = subprocess.run(
        ["git", "bundle", "list-heads", str(bundle)], capture_output=True, text=True, timeout=60, check=False
    )
    if proc.returncode != 0:
        return f"bundle {bundle} is unreadable: {proc.stderr.strip()[:200]}"
    heads: dict[str, str] = {}
    for line in proc.stdout.splitlines():
        sha, _, ref = line.partition(" ")
        heads[ref.strip()] = sha
    at = heads.get(f"refs/heads/{branch}")
    if at is None:
        return f"bundle {bundle.name} has no branch {branch}"
    if at != commit:
        return f"bundle {bundle.name} has {branch} at {at[:12]}, baseline.json says {commit[:12]}"
    return ""


def baseline_problem(baseline: Baseline, namespace: str) -> str:
    """Why this baseline cannot be restored into ``namespace`` ("" when it can)."""
    if baseline.namespace != namespace:
        return f"baseline namespace {baseline.namespace} differs from FUZZOPS_NAMESPACE {namespace}"
    render = baseline.path(baseline.render)
    if not render.is_file():
        return f"render {render} is missing"
    if baseline.render_sha256 and sha256_file(render) != baseline.render_sha256:
        return f"render {baseline.render} does not match its render_sha256"
    return bundle_problem(baseline.path(baseline.bundle), baseline.branch, baseline.commit)


# ---- operator block and lane environment ---------------------------------------------------------------------


@dataclass(frozen=True)
class OperatorBlock:
    """``BENCH_OPERATOR``: the experiment's operator block, the baseline resolved to its staged copy."""

    revision: str
    baseline: Path
    model: str = ""
    effort: str = ""
    provider: str = "codex"
    image_tag: str = ""
    auth_file: Path = field(default_factory=lambda: Path.home() / ".codex" / "auth.json")

    def __post_init__(self) -> None:
        if not self.revision:
            raise ValueError("the operator block needs the SDO revision")
        if not self.baseline.is_absolute():
            raise ValueError("the operator block's baseline must be an absolute path")
        if not self.image_tag:
            object.__setattr__(self, "image_tag", f"fz-{self.revision[:8]}")

    @classmethod
    def load(cls, path: Path) -> OperatorBlock:
        try:
            raw = json.loads(path.read_text())
        except (OSError, json.JSONDecodeError) as exc:
            raise HookError(f"BENCH_OPERATOR {path} is unreadable: {exc}") from exc
        if not isinstance(raw, dict) or raw.get("hooks") != "sdo":
            raise HookError(f"BENCH_OPERATOR {path} is not an operator block for the sdo hooks")
        settings = raw.get("settings") or {}
        if not isinstance(settings, dict):
            raise HookError("BENCH_OPERATOR settings must be an object")
        try:
            return cls(
                revision=str(raw.get("revision") or ""),
                baseline=Path(str(raw.get("baseline") or "")),
                model=str(settings.get("model") or ""),
                effort=str(settings.get("effort") or ""),
                provider=str(settings.get("provider") or "codex"),
                image_tag=str(settings.get("image_tag") or ""),
                auth_file=Path(str(settings.get("auth_file") or Path.home() / ".codex" / "auth.json")).expanduser(),
            )
        except ValueError as exc:
            raise HookError(f"BENCH_OPERATOR: {exc}") from exc

    def images(self) -> dict[str, str]:
        return {
            "controller": f"sdo-controller:{self.image_tag}",
            "responder": f"sdo-responder:{self.image_tag}",
            "script": f"sdo-script-tools:{self.image_tag}",
        }


def _need(env: Mapping[str, str], name: str) -> str:
    value = env.get(name, "")
    if not value:
        raise HookError(f"{name} is not set")
    return value


def _operator(env: Mapping[str, str]) -> OperatorBlock:
    return OperatorBlock.load(Path(_need(env, "BENCH_OPERATOR")))


def _baseline(block: OperatorBlock) -> Baseline:
    try:
        return Baseline.load(block.baseline)
    except ValueError as exc:
        raise HookError(f"baseline: {exc}") from exc


def control_namespace(env: Mapping[str, str]) -> str:
    """The lane's control namespace: the baseline's, else ``<FUZZOPS_NAMESPACE>-sdo``."""
    namespace = _need(env, "FUZZOPS_NAMESPACE")
    if env.get("BENCH_OPERATOR"):
        try:
            baseline = _baseline(_operator(env))
        except HookError:
            return f"{namespace}-sdo"
        if baseline.namespace == namespace:
            return baseline.control
    return f"{namespace}-sdo"


def _kube(env: Mapping[str, str]) -> Kube:
    return Kube(env.get("FUZZOPS_KUBE_CONTEXT", ""), env.get("KUBECONFIG", ""))


def _binary(env: Mapping[str, str]) -> Path:
    return Path(_need(env, "BENCH_LANE_DIR")) / "bin" / "sdo-operator"


# ---- SDO state -----------------------------------------------------------------------------------------------


def idle_reason(state: dict[str, Any] | None) -> str:
    """Why SDO is not idle ("" when it is): an open incident, a closure in progress (not one that failed for good),
    or reflection still queued."""
    if state is None:
        return "no controller state yet"
    if state.get("incident_open") or state.get("incidentOpen"):
        return "incident open"
    failed = (state.get("closure_state") or state.get("closureState")) == "CLOSURE_STATE_FAILED"
    # A permanently failed closure stays in the state as evidence and does not block the next incident.
    if (state.get("pending_closure") or state.get("pendingClosure")) and not failed:
        return "closure pending"
    if state.get("queued_reflection_incident_ids") or state.get("queuedReflectionIncidentIds"):
        return "reflection queued"
    if state.get("queued_reflections_unknown") or state.get("queuedReflectionsUnknown"):
        return "reflection queue unknown"
    return ""


def _incident_id(state: dict[str, Any]) -> str:
    request = state.get("incident_request") or state.get("incidentRequest") or {}
    return str(request.get("incident_id") or request.get("incidentId") or "")


def idle_verdict(state: dict[str, Any] | None, updated: datetime | None, pod_ready: bool, now: datetime) -> str:
    """The ``idle`` hook's answer: "" when idle, else the reason (``alarm:`` first for an open incident)."""
    if state is not None and idle_reason(state) == "incident open":
        return f"alarm: incident open {_incident_id(state)}".rstrip()
    if not pod_ready:
        return "down: controller pod not Ready"
    if state is None:
        return "down: no controller state"
    if updated is None:
        return "stale: state update time unknown"
    age = (now - updated).total_seconds()
    if age > FRESH_S:
        return f"stale: state not updated for {age:.0f}s"
    why = idle_reason(state)
    return f"busy: {why}" if why else ""


def _parse_time(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclass
class Kube:
    """kubectl against one lane's cluster."""

    context: str
    kubeconfig: str = ""

    def cmd(self, *args: str) -> list[str]:
        out = ["kubectl"]
        if self.kubeconfig:
            out += ["--kubeconfig", self.kubeconfig]
        if self.context:
            out += ["--context", self.context]
        return [*out, *args]

    def _json(self, *args: str) -> dict[str, Any] | None:
        proc = subprocess.run(
            self.cmd(*args, "-o", "json"), capture_output=True, text=True, timeout=KUBECTL_TIMEOUT_S, check=False
        )
        if proc.returncode != 0:
            return None
        value = json.loads(proc.stdout)
        return value if isinstance(value, dict) else None

    def state_configmap(self, control: str) -> tuple[dict[str, Any] | None, datetime | None]:
        """The controller state and when the controller last changed it (the newest managedFields time).

        The controller writes the ConfigMap only when its state changed, and every step pass advances the state's
        ``step_count`` heartbeat, so the last write time is the controller's last completed step.
        """
        cm = self._json("-n", control, "get", "configmap", STATE_CONFIGMAP, "--show-managed-fields")
        if cm is None:
            return None, None
        meta = cm.get("metadata") or {}
        stamps = [_parse_time(m.get("time")) for m in meta.get("managedFields") or []]
        stamps = [s for s in stamps if s is not None] or [_parse_time(meta.get("creationTimestamp"))]
        updated = max((s for s in stamps if s is not None), default=None)
        raw = (cm.get("data") or {}).get(STATE_KEY)
        return (json.loads(raw) if raw else None), updated

    def state(self, control: str) -> dict[str, Any] | None:
        return self.state_configmap(control)[0]

    def controller_pods(self, control: str) -> list[dict[str, Any]]:
        pods = self._json("-n", control, "get", "pods", "-l", f"job-name={CONTROLLER_JOB}")
        return list((pods or {}).get("items") or [])

    def controller_ready(self, control: str) -> bool:
        for pod in self.controller_pods(control):
            if (pod.get("metadata") or {}).get("deletionTimestamp"):
                continue
            conditions = (pod.get("status") or {}).get("conditions") or []
            if any(c.get("type") == "Ready" and c.get("status") == "True" for c in conditions):
                return True
        return False

    def controller_pod(self, control: str) -> str:
        names = [
            p["metadata"]["name"]
            for p in self.controller_pods(control)
            if (p.get("status") or {}).get("phase") == "Running" and not p["metadata"].get("deletionTimestamp")
        ]
        if not names:
            raise HookError(f"no running controller pod in {control}")
        return sorted(names)[-1]

    def exec_controller(self, control: str, script: str, timeout: float = 300) -> bytes:
        pod = self.controller_pod(control)
        proc = subprocess.run(
            self.cmd("-n", control, "exec", pod, "-c", CONTROLLER_CONTAINER, "--", "sh", "-c", script),
            capture_output=True,
            timeout=timeout,
            check=False,
        )
        if proc.returncode != 0:
            raise HookError(f"exec in {pod} failed ({proc.returncode}): {proc.stderr.decode(errors='replace')[-300:]}")
        return proc.stdout

    def copy_memory(self, control: str, dest: Path) -> bool:
        """Copy the accepted outcome records and playbooks into ``dest`` (replacing an older copy)."""
        tar = f"cd {APPLICATION_ROOT} && tar cf - " + " ".join(MEMORY_PATHS) + " 2>/dev/null; true"
        try:
            data = self.exec_controller(control, tar)
        except HookError:
            return False
        if not data:
            return False
        tmp = dest.with_name(dest.name + ".tmp")
        shutil.rmtree(tmp, ignore_errors=True)
        tmp.mkdir(parents=True)
        subprocess.run(["tar", "xf", "-", "-C", str(tmp)], input=data, check=True)
        shutil.rmtree(dest, ignore_errors=True)
        tmp.rename(dest)
        return True


# ---- render --------------------------------------------------------------------------------------------------


def plain_docs(text: str) -> list[dict[str, Any]]:
    """The objects of a rendered manifest stream as a plain pack holds them: no Jobs (hooks and one-shot
    migrations), no empty documents, no ``metadata.namespace``."""
    docs = []
    for doc in yaml.safe_load_all(text):
        if not isinstance(doc, dict) or doc.get("kind") in (None, "Job"):
            continue
        (doc.get("metadata") or {}).pop("namespace", None)
        docs.append(doc)
    return docs


def _drop_nulls(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: _drop_nulls(v) for k, v in value.items() if v is not None}
    if isinstance(value, list):
        return [_drop_nulls(v) for v in value]
    return value


def _ident(doc: dict[str, Any]) -> str:
    return f"{doc.get('kind')}/{(doc.get('metadata') or {}).get('name')}"


def render_difference(live: list[dict[str, Any]], baseline: list[dict[str, Any]]) -> str:
    """Why two renders differ ("" when they are the same up to document order, key order and null-valued keys;
    ``rollingUpdate: null`` against an absent key is the known benign difference)."""

    def index(docs: list[dict[str, Any]]) -> dict[str, list[str]]:
        out: dict[str, list[str]] = {}
        for d in docs:
            out.setdefault(_ident(d), []).append(yaml.safe_dump(_drop_nulls(d), sort_keys=True))
        return {k: sorted(v) for k, v in out.items()}

    a, b = index(live), index(baseline)
    if only := sorted(set(a) - set(b)):
        return f"render differs from the baseline's: {len(only)} objects only in the live app ({', '.join(only[:3])})"
    if only := sorted(set(b) - set(a)):
        return f"render differs from the baseline's: {len(only)} objects only in the baseline ({', '.join(only[:3])})"
    changed = sorted(k for k in a if a[k] != b[k])
    if changed:
        return f"render differs from the baseline's: {len(changed)} objects changed ({', '.join(changed[:3])})"
    return ""


def plain_pack(pack: Path, out: Path, docs: list[dict[str, Any]] | None) -> SourcePack:
    """The plain-manifest pack for a lane: ``pack`` itself when it has ``manifests/``, else ``out`` with ``docs``
    under ``manifests/`` and every other file of ``pack`` beside it."""
    if (pack / "manifests").is_dir():
        return SourcePack(pack)
    if docs is None:
        raise HookError(f"{pack} has no manifests/ and no render was given")
    shutil.rmtree(out, ignore_errors=True)
    (out / "manifests").mkdir(parents=True)
    (out / "manifests" / "render.yaml").write_text(yaml.safe_dump_all(docs, sort_keys=False))
    for entry in sorted(pack.iterdir()):
        if entry.name == "manifests":
            continue
        if entry.is_dir():
            shutil.copytree(entry, out / entry.name, symlinks=True)
        else:
            shutil.copy2(entry, out / entry.name)
    return SourcePack(out)


def helm_render(env: Mapping[str, str], namespace: str) -> list[dict[str, Any]]:
    """The live manifests of the namespace's one Helm release, as plain documents."""
    base = ["helm"]
    if env.get("KUBECONFIG"):
        base += ["--kubeconfig", env["KUBECONFIG"]]
    if env.get("FUZZOPS_KUBE_CONTEXT"):
        base += ["--kube-context", env["FUZZOPS_KUBE_CONTEXT"]]
    listed = subprocess.run(
        [*base, "list", "-n", namespace, "-o", "json"], capture_output=True, text=True, timeout=120, check=False
    )
    if listed.returncode != 0:
        raise HookError(f"helm list -n {namespace} failed: {listed.stderr.strip()[-200:]}")
    releases = [r.get("name") for r in json.loads(listed.stdout or "[]")]
    if len(releases) != 1:
        raise HookError(f"namespace {namespace} has {len(releases)} Helm releases ({releases}); expected exactly one")
    manifest = subprocess.run(
        [*base, "get", "manifest", str(releases[0]), "-n", namespace],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    if manifest.returncode != 0:
        raise HookError(f"helm get manifest {releases[0]} failed: {manifest.stderr.strip()[-200:]}")
    return plain_docs(manifest.stdout)


def live_pack(env: Mapping[str, str], pack: Path, out: Path) -> SourcePack:
    """The plain-manifest pack of the live app (``pack`` when it already has plain manifests)."""
    if (pack / "manifests").is_dir():
        return SourcePack(pack)
    return plain_pack(pack, out, helm_render(env, _need(env, "FUZZOPS_NAMESPACE")))


def pack_docs(pack: SourcePack) -> list[dict[str, Any]]:
    return [d for path in pack.manifests() for d in plain_docs(path.read_text())]


# ---- install -------------------------------------------------------------------------------------------------


def install_existing_args(
    binary: Path, repository: Path, baseline: Baseline, block: OperatorBlock, kubeconfig: Path
) -> list[str]:
    """The production ``sdo-operator install-existing`` command restoring the baseline on the lane."""
    image = block.images()
    if not block.model:
        raise HookError("the operator block's settings need a model")
    args = [
        str(binary),
        "install-existing",
        "--repository",
        str(repository),
        "--namespace",
        baseline.namespace,
        "--control-namespace",
        baseline.control,
        "--application",
        baseline.app_name,
        "--controller-image",
        image["controller"],
        "--responder-image",
        image["responder"],
        "--script-image",
        image["script"],
        "--repository-pvc",
        REPOSITORY_PVC,
        "--credentials-secret",
        CREDENTIALS_SECRET,
        "--agent-provider",
        block.provider,
        "--model",
        block.model,
    ]
    if block.effort:
        args += ["--agent-effort", block.effort]
    return [*args, *baseline.controller_flags, "--kubeconfig", str(kubeconfig)]


def _run(args: list[str], what: str, timeout: float = 600, **kwargs: Any) -> subprocess.CompletedProcess[str]:
    proc = subprocess.run(args, capture_output=True, text=True, timeout=timeout, check=False, **kwargs)
    if proc.returncode != 0:
        raise HookError(f"{what} failed ({proc.returncode}): {(proc.stderr or proc.stdout).strip()[-300:]}")
    return proc


def checkout_revision() -> str:
    return _run(["git", "rev-parse", "HEAD"], "git rev-parse").stdout.strip()


def build_operator(out: Path) -> Path:
    """``go build`` the pinned checkout's ``sdo-operator`` into ``out``."""
    out.parent.mkdir(parents=True, exist_ok=True)
    _run(
        ["go", "build", "-o", str(out.resolve()), "./cmd/sdo-operator"],
        "go build sdo-operator",
        1200,
        cwd="sdo/operator",
    )
    return out


def load_images(cluster: str, wanted: list[str], log: Callable[[str], None]) -> None:
    """Load the operator images onto the cluster's worker nodes (all nodes when it has none); kind skips nodes
    that already hold the same image ID."""
    for image in wanted:
        if subprocess.run(["docker", "image", "inspect", image], capture_output=True, check=False).returncode != 0:
            raise HookError(f"image {image} does not exist on this host (build it with scripts/build_sdo_images.sh)")
    nodes = _run(["kind", "get", "nodes", "--name", cluster], "kind get nodes").stdout.split()
    workers = [n for n in nodes if "worker" in n] or nodes
    if not workers:
        raise HookError(f"kind cluster {cluster} has no nodes")
    for image in wanted:
        _run(
            ["kind", "load", "docker-image", "--name", cluster, "--nodes", ",".join(workers), image],
            f"kind load {image}",
            1200,
        )
        log(f"loaded {image} on {','.join(workers)}")


def clone_baseline(baseline: Baseline, repo: Path) -> str:
    """Clone the baseline bundle's branch into ``repo`` as ``main`` with no remote; returns HEAD."""
    shutil.rmtree(repo, ignore_errors=True)
    repo.parent.mkdir(parents=True, exist_ok=True)
    bundle = baseline.path(baseline.bundle)
    _run(["git", "clone", "-q", "-b", baseline.branch, str(bundle), str(repo)], "git clone baseline")
    _run(["git", "-C", str(repo), "checkout", "-q", "-B", "main"], "git checkout main")
    _run(["git", "-C", str(repo), "remote", "remove", "origin"], "git remote remove")
    head = _run(["git", "-C", str(repo), "rev-parse", "HEAD"], "git rev-parse").stdout.strip()
    if head != baseline.commit:
        raise HookError(f"baseline clone is at {head[:12]}, baseline.json says {baseline.commit[:12]}")
    return head


def remove_operator(kube: Kube, control: str, log: Callable[[str], None]) -> None:
    """Delete the control namespace (waiting until it is gone) and the cluster grant install-existing made for it."""
    _run(
        kube.cmd("delete", "namespace", control, "--ignore-not-found", "--wait=true", "--timeout=600s"),
        f"delete namespace {control}",
        660,
    )
    grant = f"sdo-admin-{control}"
    for kind in ("clusterrolebinding", "clusterrole"):
        _run(kube.cmd("delete", kind, grant, "--ignore-not-found"), f"delete {kind} {grant}", 120)
    log(f"removed {control} and {grant}")


# ---- verbs ---------------------------------------------------------------------------------------------------


def check(env: Mapping[str, str], out: Callable[[str], None] = _print) -> int:
    try:
        baseline = _baseline(_operator(env))
        if why := baseline_problem(baseline, _need(env, "FUZZOPS_NAMESPACE")):
            raise HookError(why)
    except HookError as exc:
        out(" ".join(str(exc).split()))
        return 1
    out(f"ok: baseline {baseline.commit[:12]} for {baseline.namespace}")
    return 0


def prepare(env: Mapping[str, str], out: Callable[[str], None] = _print) -> int:
    block = _operator(env)
    baseline = _baseline(block)
    namespace = _need(env, "FUZZOPS_NAMESPACE")
    if why := baseline_problem(baseline, namespace):
        raise HookError(why)
    if not block.model:
        raise HookError("the operator block's settings need a model")
    lane_dir = Path(_need(env, "BENCH_LANE_DIR")).resolve()
    kubeconfig = Path(_need(env, "KUBECONFIG")).resolve()
    cluster = _need(env, "FUZZOPS_KIND_CLUSTER")
    revision = checkout_revision()
    if not (revision.startswith(block.revision) or block.revision.startswith(revision)):
        raise HookError(f"the hooks checkout is at {revision[:12]}, the operator block pins {block.revision}")
    lane_dir.mkdir(parents=True, exist_ok=True)
    # (a, b) the live app's plain pack, identical to the render the baseline's memory was made on
    source = live_pack(env, Path(_need(env, "BENCH_PACK")), lane_dir / "plain-pack")
    reference = plain_docs(baseline.path(baseline.render).read_text())
    if why := render_difference(pack_docs(source), reference):
        raise HookError(why)
    out("render identical to the baseline's")
    # (c) the attached pack the runs use
    shutil.rmtree(lane_dir / "pack", ignore_errors=True)
    attached = write_attached_pack(source, lane_dir / "pack").resolve()
    # (d) the operator binary of the pinned checkout
    binary = build_operator(lane_dir / "bin" / "sdo-operator")
    # (e) the operator images on the workers
    image = block.images()
    load_images(cluster, [image["controller"], image["responder"], image["script"]], out)
    kube = _kube(env)
    if (
        subprocess.run(
            kube.cmd("get", "namespace", baseline.control), capture_output=True, timeout=60, check=False
        ).returncode
        == 0
    ):
        # A previous attempt left an operator: start over from the baseline.
        remove_operator(kube, baseline.control, out)
    # (f) namespaces, credentials by file reference, node image digest names
    cfg = InstallConfig(
        pack=source.root,
        work=lane_dir,
        namespace=namespace,
        control_namespace=baseline.control,
        objective=baseline.objective or "restored from baseline",
        controller_image=image["controller"],
        responder_image=image["responder"],
        operator_binary=binary.resolve(),
        kubeconfig=kubeconfig,
        auth_file=block.auth_file.resolve(),
        provider=block.provider,
        model=block.model,
        effort=block.effort,
        kind_cluster=cluster,
        operation_id="bench-restore",
    )
    if not cfg.auth_file.is_file():
        raise HookError(f"agent credentials file {cfg.auth_file} is missing")
    provision_namespaces(cfg)
    for line in declare_node_images(cfg, images(source.manifests())):
        out(f"declared {line}")
    # (g) the baseline memory as the accepted repository
    memory = clone_baseline(baseline, lane_dir / "app")
    # (h) production restore: install-existing, history kept, no install-time agents
    args = install_existing_args(binary.resolve(), lane_dir / "app", baseline, block, kubeconfig)
    (lane_dir / "install-existing-args.json").write_text(json.dumps(args, indent=1) + "\n")
    with (
        (lane_dir / "install-existing.json").open("w") as receipt,
        (lane_dir / "install-existing.log").open("w") as log,
    ):
        rc = subprocess.run(args, stdout=receipt, stderr=log, timeout=1500, check=False).returncode
    if env.get("BENCH_OUT"):
        bench_out = Path(env["BENCH_OUT"])
        bench_out.mkdir(parents=True, exist_ok=True)
        for name in ("install-existing-args.json", "install-existing.json", "install-existing.log"):
            shutil.copy2(lane_dir / name, bench_out / name)
    if rc != 0:
        tail = (lane_dir / "install-existing.log").read_text()[-300:].strip().replace("\n", " ")
        raise HookError(f"install-existing exited {rc}: {tail}")
    out(json.dumps({"pack": str(attached), "operator_version": revision, "memory": memory}))
    return 0


def idle(env: Mapping[str, str], out: Callable[[str], None] = _print) -> int:
    control = control_namespace(env)
    kube = _kube(env)
    try:
        ready = kube.controller_ready(control)
        state, updated = kube.state_configmap(control)
    except (subprocess.SubprocessError, json.JSONDecodeError, OSError) as exc:
        out(f"error: controller read failed: {exc}")
        return 1
    why = idle_verdict(state, updated, ready, datetime.now(UTC))
    if why:
        out(why)
        return 1
    out("idle")
    return 0


def watch(env: Mapping[str, str]) -> int:
    argv = ["--control-namespace", control_namespace(env)]
    fault = env.get("BENCH_FAULT", "none")
    if fault and fault != "none":
        argv += ["--fault", fault]
    binary = _binary(env) if env.get("BENCH_LANE_DIR") else None
    if binary is not None and binary.is_file() and env.get("BENCH_OPERATOR"):
        argv += ["--operator-binary", str(binary), "--controller-image", _operator(env).images()["controller"]]
    return observe.main(argv)


def collect(env: Mapping[str, str], out: Callable[[str], None] = _print) -> int:
    dest = Path(_need(env, "BENCH_OUT")).resolve()
    dest.mkdir(parents=True, exist_ok=True)
    control = control_namespace(env)
    kube = _kube(env)
    parts: dict[str, str] = {}

    def part(name: str, step: Callable[[], str]) -> None:
        try:
            parts[name] = step() or "ok"
        except Exception as exc:  # best effort: every part runs and records its own failure
            parts[name] = f"error: {exc}"[:400]

    def memory() -> str:
        if not kube.copy_memory(control, dest / "memory"):
            raise HookError("no accepted memory copied")
        return "ok"

    def state() -> str:
        value, updated = kube.state_configmap(control)
        if value is None:
            raise HookError("no controller state")
        (dest / "controller-state.json").write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
        return f"ok (updated {updated.isoformat() if updated else 'unknown'})"

    def logs() -> str:
        pod = kube.controller_pod(control)
        args = ["-n", control, "logs", pod, "-c", CONTROLLER_CONTAINER]
        if env.get("BENCH_INTERVAL_START"):
            args.append(f"--since-time={env['BENCH_INTERVAL_START']}")
        with (dest / "controller.log").open("w") as f:
            proc = subprocess.run(
                kube.cmd(*args), stdout=f, stderr=subprocess.PIPE, text=True, timeout=300, check=False
            )
        if proc.returncode != 0:
            raise HookError(proc.stderr.strip()[-300:])
        return "ok"

    def export() -> str:
        binary = _binary(env)
        if not binary.is_file():
            raise HookError(f"no operator binary at {binary}")
        target = evidence.EvidenceTarget(
            binary.resolve(),
            control,
            _operator(env).images()["controller"],
            Path(env["KUBECONFIG"]).resolve() if env.get("KUBECONFIG") else None,
        )
        return str(evidence.export(target, dest))

    part("memory", memory)
    part("state", state)
    part("logs", logs)
    part("evidence", export)
    summary = {
        "control_namespace": control,
        "run": env.get("BENCH_RUN", ""),
        "fault": env.get("BENCH_FAULT", ""),
        "interval": [env.get("BENCH_INTERVAL_START", ""), env.get("BENCH_INTERVAL_END", "")],
        "collected_at": _now(),
        "parts": parts,
    }
    (dest / "collect.json").write_text(json.dumps(summary, indent=2) + "\n")
    failed = sorted(k for k, v in parts.items() if v.startswith("error"))
    out(f"collect: {'failed ' + ','.join(failed) if failed else 'ok'}")
    return 1 if failed else 0


def teardown(env: Mapping[str, str], out: Callable[[str], None] = _print) -> int:
    remove_operator(_kube(env), control_namespace(env), out)
    return 0


# ---- baseline subcommand -------------------------------------------------------------------------------------


def rebranch_bundle(raw: Path, out: Path, branch: str, work: Path) -> str:
    """Rewrite a one-branch bundle so its branch is ``branch``; returns the commit."""
    shutil.rmtree(work, ignore_errors=True)
    listed = _run(["git", "bundle", "list-heads", str(raw)], "git bundle list-heads").stdout.split()
    branches = [ref.removeprefix("refs/heads/") for ref in listed if ref.startswith("refs/heads/")]
    if len(branches) != 1:
        raise HookError(f"the archived bundle has {len(branches)} branches, expected one")
    _run(["git", "clone", "-q", "-b", branches[0], str(raw), str(work)], "git clone bundle")
    _run(["git", "-C", str(work), "checkout", "-q", "-B", branch], f"git checkout {branch}")
    commit = _run(["git", "-C", str(work), "rev-parse", "HEAD"], "git rev-parse").stdout.strip()
    _run(["git", "-C", str(work), "bundle", "create", str(out.resolve()), branch], "git bundle create")
    if why := bundle_problem(out, branch, commit):
        raise HookError(why)
    return commit


def archive_baseline(a: argparse.Namespace) -> Baseline:
    """Archive a running lane's accepted memory and its live render as a new baseline directory."""
    out: Path = a.out.resolve()
    if out.exists() and any(out.iterdir()):
        raise HookError(f"{out} is not empty")
    out.mkdir(parents=True, exist_ok=True)
    control = a.control_namespace or f"{a.namespace}-sdo"
    kube = Kube(a.context, str(a.kubeconfig.resolve()))
    script = (
        f"cd {APPLICATION_ROOT} && b=$(git -c safe.directory='*' symbolic-ref --short HEAD) && "
        "git -c safe.directory='*' bundle create /tmp/sdo-baseline.bundle \"$b\" >/dev/null 2>&1 && "
        "cat /tmp/sdo-baseline.bundle; rc=$?; rm -f /tmp/sdo-baseline.bundle; exit $rc"
    )
    data = kube.exec_controller(control, script, timeout=600)
    with tempfile.TemporaryDirectory() as tmp:
        raw = Path(tmp) / "raw.bundle"
        raw.write_bytes(data)
        commit = rebranch_bundle(raw, out / "memory.bundle", a.branch, Path(tmp) / "repo")
    env = {
        "KUBECONFIG": str(a.kubeconfig.resolve()),
        "FUZZOPS_KUBE_CONTEXT": a.context,
        "FUZZOPS_NAMESPACE": a.namespace,
    }
    docs = pack_docs(SourcePack(a.pack.resolve())) if a.pack else helm_render(env, a.namespace)
    (out / "render.yaml").write_text(yaml.safe_dump_all(docs, sort_keys=False))
    flags = tuple(json.loads(a.controller_flags)) if a.controller_flags else DEFAULT_CONTROLLER_FLAGS
    return write_baseline(
        out,
        Baseline(
            namespace=a.namespace,
            control_namespace=a.control_namespace if a.control_namespace != f"{a.namespace}-sdo" else "",
            bundle="memory.bundle",
            branch=a.branch,
            commit=commit,
            render="render.yaml",
            objective=a.objective,
            controller_flags=flags,
            sdo_revision=a.sdo_revision or checkout_revision(),
        ),
    )


def baseline_main(argv: list[str]) -> int:
    p = argparse.ArgumentParser(prog="hooks baseline", description=archive_baseline.__doc__)
    p.add_argument("--kubeconfig", type=Path, required=True)
    p.add_argument("--context", required=True)
    p.add_argument("--namespace", required=True, help="the app namespace the memory operates")
    p.add_argument("--control-namespace", default="", help="default: <namespace>-sdo")
    p.add_argument("--out", type=Path, required=True, help="new baseline directory (outside the repository)")
    p.add_argument("--objective", default="", help="the health objective the memory was made for (documentation)")
    p.add_argument("--branch", default="baseline", help="branch name in the bundle")
    p.add_argument("--pack", type=Path, help="a plain-manifest pack to take the render from (default: helm)")
    p.add_argument("--controller-flags", default="", help="JSON list of install-existing flags (default: the lanes')")
    p.add_argument("--sdo-revision", default="", help="the SDO commit the lane ran (default: this checkout)")
    baseline = archive_baseline(p.parse_args(argv))
    print(json.dumps({"baseline": str(baseline.root), "commit": baseline.commit}))
    return 0


VERBS = ("check", "prepare", "idle", "watch", "collect", "teardown")


def main(argv: list[str] | None = None) -> int:
    args = sys.argv[1:] if argv is None else argv
    if not args or args[0] not in (*VERBS, "baseline"):
        print(f"usage: python -m benchmarks.fuzzops.hooks {{{','.join(VERBS)},baseline}}", file=sys.stderr)
        return 2
    verb, env = args[0], os.environ
    try:
        if verb == "baseline":
            return baseline_main(args[1:])
        if verb == "watch":
            return watch(env)
        return {"check": check, "prepare": prepare, "idle": idle, "collect": collect, "teardown": teardown}[verb](env)
    except (HookError, ValueError, OSError, subprocess.SubprocessError) as exc:
        reason = " ".join(str(exc).split()) or type(exc).__name__
        print(reason if verb == "check" else f"error: {reason}", flush=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
