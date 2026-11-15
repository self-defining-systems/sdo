"""SDO's operator hooks for fuzzops-bench: restore SDO from a baseline on a lane, tell the bench when it is idle,
watch each fault window, collect its artifacts, and remove it again.

``python -m benchmarks.fuzzops.hooks <verb>``, run by the bench with the cwd at the pinned SDO checkout. A hook only
ever touches its own lane. Verbs (environment: ``BENCH_*`` and ``FUZZOPS_*``, see the bench's hook contract):

``check``     no cluster access: the baseline manifest is well formed, names ``FUZZOPS_NAMESPACE``, its render file
              matches its digest and its bundle holds the branch at the commit.
``prepare``   on a fresh app copy: build the plain-manifest pack from the live app (the namespace's one Helm release,
              or the pack's own ``manifests/``), refuse a render that differs from the baseline's, write the attached
              pack, build ``sdo-operator``, push the operator images to the host registry (``BENCH_REGISTRY``; load
              them onto the worker nodes when there is none), provision the namespaces,
              clone the baseline memory and run production ``sdo-operator install-existing`` with the benchmark
              profile, the baseline's and the experiment's config overlays, and the lane's evidence sink
              (``BENCH_EVIDENCE_NODE``, ``BENCH_EVIDENCE_RUN``; required). The last stdout line is
              ``{"pack", "operator_version", "memory"}``. It ends with an operator reset between problems
              (``sdo-operator incidents reset``), so the lane starts with no incident.
``idle``      exit 0 when the controller pod is Ready, its state ConfigMap changed within 60 s and SDO owns no incident
              work; otherwise exit 1 with the reason on the first line (``alarm: incident open <id>`` for an incident).
``watch``     the fault window's ``exec:`` command: ``benchmarks.fuzzops.observe``.
``collect``   best effort: accepted outcomes and playbooks, the controller state, controller logs since the inject, and
              from the host sink the interval's incidents and tasks (``evidence/``) and ``transcripts.json``; then
              the dashboard view (``view.json``, ``view/transcripts/``; ``benchmarks.fuzzops.view``); last, after the
              run has settled and its evidence is copied, the operator reset between problems
              (``sdo-operator incidents reset``: any open or escalated incident closes as RESET, no learning credit,
              memory untouched), so the next run's gate starts on a clean slate (``incidents-reset.json``).
``teardown``  delete the control namespace (waiting, so the evidence sidecar's final pass runs), then the cluster grant
              install-existing made for it.
``transcripts`` read-only, the host sink only: exit 0 when ``$BENCH_EVIDENCE_HOST/$BENCH_EVIDENCE_RUN`` is an
              ``sdo.evidence/v1`` run directory and every incident with a ledger holds a rollout unless no agent ran;
              otherwise exit 1 with ``no sink: <why>`` or ``transcripts missing: N (<ids>)`` first.

``baseline`` (SDO only) archives a running lane's accepted memory and render into a new baseline directory.

Baseline directory: ``baseline.json`` (``Baseline``) beside the memory bundle and the render it was made on::

    {"format": 2, "namespace": "<app namespace>", "control_namespace": "<ns>-sdo" (optional),
     "application": "<ns>" (optional), "bundle": "memory.bundle", "branch": "<branch>", "commit": "<40 hex>",
     "render": "<path of the plain render>", "render_sha256": "<hex>", "objective": "<health objective>",
     "config": {"response": {"max_loops": 5}, ...}, "sdo_revision": "<SDO commit it was made with>"}

Paths are relative to the baseline directory. ``config`` is an operator config overlay (``docs/config.md``) passed
after the benchmark profile; the keys the hook sets from the operator block and the lane (agent provider, model and
effort, the evidence sink) are refused there. A format-1 manifest's ``controller_flags`` (pre-config
``install-existing`` flags) are translated to ``config`` when it is read; a flag without a config key is refused.
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
import time
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml

from benchmarks.fuzzops import config, devtree, observe, page, sink, view
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
BASELINE_FORMAT = 2
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
        "--kubeconfig",
    }
)
#: Config keys the hook sets from the operator block and the lane; a baseline overlay may not.
HOOK_KEYS = frozenset({("agent", "provider"), ("agent", "model"), ("agent", "effort"), ("evidence",)})
#: The overlay a new baseline records unless told otherwise. The values SDO's fuzzops lanes ran with are the
#: production defaults plus the benchmark profile (``sdo-bench.yaml``), so nothing more is recorded.
DEFAULT_CONFIG: dict[str, Any] = {}
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
    config: dict[str, Any] = field(default_factory=dict)
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
        refuse_hook_keys(self.config, "baseline config")

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
        if raw.get("format") == 1:
            raw = _translate_format_1(raw, manifest)
        known = {f for f in cls.__dataclass_fields__ if f != "root"}
        if unknown := set(raw) - known:
            raise ValueError(f"{manifest} has unknown fields {sorted(unknown)}")
        try:
            return cls(**raw, root=root)
        except TypeError as exc:
            raise ValueError(f"{manifest}: {exc}") from exc

    def to_json(self) -> str:
        data = {k: v for k, v in asdict(self).items() if k != "root"}
        return json.dumps(data, indent=2) + "\n"


def refuse_hook_keys(overlay: Any, what: str) -> None:
    """Refuse an overlay that is not a mapping or sets a key the hook sets from the operator block or the lane."""
    if not isinstance(overlay, dict):
        raise TypeError(f"{what} must be an object of config sections")
    for path in HOOK_KEYS:
        section: Any = overlay
        for key in path[:-1]:
            section = section.get(key) if isinstance(section, dict) else None
        if isinstance(section, dict) and path[-1] in section:
            raise ValueError(f"{what} sets {'.'.join(path)}, which the hook sets from the operator block or the lane")


def _translate_format_1(raw: dict[str, Any], manifest: Path) -> dict[str, Any]:
    """A format-1 manifest as format 2: its ``controller_flags`` become the equivalent ``config`` overlay."""
    flags = raw.get("controller_flags") or []
    if not isinstance(flags, list) or not all(isinstance(flag, str) for flag in flags):
        raise ValueError(f"{manifest}: controller_flags must be a list of strings")
    for flag in flags:
        if flag.split("=", 1)[0] in HOOK_FLAGS:
            raise ValueError(f"{manifest}: controller flag {flag} is set by the hook, not the baseline")
    try:
        overlay = config.translate_flags(flags)
    except ValueError as exc:
        raise ValueError(f"{manifest}: format-1 controller_flags cannot be translated: {exc}") from exc
    translated = {key: value for key, value in raw.items() if key != "controller_flags"}
    return {**translated, "format": BASELINE_FORMAT, "config": overlay}


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


#: What ``prepare`` leaves in the lane directory for ``swap``: the revision the lane runs and its memory commit.
PREPARED_RECORD = "prepared.json"
#: The projected sources under the lane's evidence directory (``devtree``).
DEV_DIRECTORY = ".dev"

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
    #: ``settings.config``: the experiment's operator config overlay, passed after the baseline's.
    config: dict[str, Any] = field(default_factory=dict)
    #: ``settings.harness_page``: the harness-paged arm (``page.PageConfig``): the benchmark opens the one SDO
    #: incident through the external-incident inbox at the inject plus ``after`` seconds; detectors only observe.
    harness_page: page.PageConfig | None = None
    #: ``settings.dev_mount``: run the pods from the base images (``image_tag``) with the checkout's sources and
    #: binary projected over them from the lane's evidence mount (``devtree``): no image build for this revision.
    dev_mount: bool = False

    def __post_init__(self) -> None:
        if not self.revision:
            raise ValueError("the operator block needs the SDO revision")
        if not self.baseline.is_absolute():
            raise ValueError("the operator block's baseline must be an absolute path")
        if self.dev_mount and not self.image_tag:
            raise ValueError("settings.dev_mount needs settings.image_tag (the base images)")
        if not self.image_tag:
            object.__setattr__(self, "image_tag", f"fz-{self.revision[:8]}")
        refuse_hook_keys(self.config, "settings.config")
        if self.harness_page is not None and "mode" in self.config:
            raise ValueError("settings.harness_page sets the operator mode; settings.config may not")

    def effective_config(self) -> dict[str, Any]:
        """The experiment overlay, with the harness-paged arm's mode: external alerts only, detectors as clues."""
        if self.harness_page is None:
            return self.config
        arm = {"mode": "external", "response": {"external_clues": True}}
        merged = {**self.config, **arm}
        merged["response"] = {**(self.config.get("response") or {}), **arm["response"]}
        return merged

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
                config=settings.get("config") or {},
                harness_page=page.PageConfig.from_settings(settings.get("harness_page")),
                dev_mount=bool(settings.get("dev_mount")),
            )
        except (TypeError, ValueError) as exc:
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


# ---- SDO state -----------------------------------------------------------------------------------------------


def _startup_holding(state: dict[str, Any]) -> bool:
    """Whether a new install's start-up period still holds its openers: begun, neither established nor past its
    bound. SDO opens no incident then, so an injected fault would only wait."""
    startup = state.get("startup") or {}
    if not (startup.get("began_at") or startup.get("beganAt")):
        return False
    ended = ("established_at", "establishedAt", "bound_reached_at", "boundReachedAt")
    return not any(startup.get(key) for key in ended)


def idle_reason(state: dict[str, Any] | None) -> str:
    """Why SDO is not idle ("" when it is): an open incident, a closure in progress (not one that failed for good),
    reflection still queued, or a new install SDO has not yet seen healthy."""
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
    if _startup_holding(state):
        return "startup baseline pending"
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


def install_existing_launch(
    binary: Path,
    repository: Path,
    baseline: Baseline,
    block: OperatorBlock,
    kubeconfig: Path,
    evidence_env: Mapping[str, str],
    config_dir: Path,
    image: Mapping[str, str] | None = None,
) -> tuple[list[str], dict[str, str]]:
    """The production ``sdo-operator install-existing`` command restoring the baseline on the lane and its
    ``SDO_*`` environment: the benchmark profile, then the baseline's and the experiment's overlays (written to
    ``config_dir``), the settings' agent and the lane's evidence sink (``sink.install_env``)."""
    if not (
        evidence_env.get("SDO_EVIDENCE_HOST_PATH") or evidence_env.get("SDO_EVIDENCE_VOLUME")
    ) or not evidence_env.get("SDO_EVIDENCE_RUN_ID"):
        raise HookError("install-existing needs the lane's evidence sink and run ID")
    image = image or block.images()
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
        "--kubeconfig",
        str(kubeconfig),
    ]
    files = [config.BENCH_PROFILE]
    config_dir.mkdir(parents=True, exist_ok=True)
    for name, overlay in (("baseline", baseline.config), ("experiment", block.effective_config())):
        if overlay:
            files.append(config.write_overlay(overlay, config_dir / f"{name}.yaml"))
    environment = config.operator_env(provider=block.provider, model=block.model, effort=block.effort)
    return [*args, *config.config_args(files)], {**environment, **evidence_env}


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


def load_images(cluster: str, wanted: list[str], log: Callable[[str], None], registry: Path | None = None) -> None:
    """Make the operator images available to the lane's nodes. With the host's registry script (``BENCH_REGISTRY``)
    each image is pushed once per host and nodes pull it on demand; without one, or when that fails, the images are
    loaded onto the worker nodes (all nodes when it has none); kind skips nodes that already hold the same image ID."""
    for image in wanted:
        if subprocess.run(["docker", "image", "inspect", image], capture_output=True, check=False).returncode != 0:
            raise HookError(f"image {image} does not exist on this host (build it with scripts/build_sdo_images.sh)")
    if registry is not None:
        try:
            _run(["bash", str(registry), "up"], "registry up", 300)
            _run(["bash", str(registry), "push", *wanted], "registry push", 1800)
            log(f"pushed {', '.join(wanted)} to the host registry")
            return
        except HookError as exc:
            log(f"host registry unavailable, falling back to loading the nodes: {exc}")
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


def lane_registry_images(registry: Path | None, wanted: list[str], log: Callable[[str], None]) -> dict[str, str]:
    """The operator images of a lane whose nodes cannot be loaded (provider vcluster): each is pushed to the shared
    cluster's registry by the pinned fuzzops checkout's ``registry.sh`` and named as every node pulls it
    (``registry.sh ref IMAGE``); returns image -> reference. There is no fallback."""
    if registry is None:
        raise HookError("a lane without nodes to load needs the host registry script (BENCH_REGISTRY)")
    for image in wanted:
        if subprocess.run(["docker", "image", "inspect", image], capture_output=True, check=False).returncode != 0:
            raise HookError(f"image {image} does not exist on this host (build it with scripts/build_sdo_images.sh)")
    _run(["bash", str(registry), "up"], "registry up", 300)
    _run(["bash", str(registry), "push", *wanted], "registry push", 1800)
    refs = {i: _run(["bash", str(registry), "ref", i], f"registry ref {i}", 60).stdout.strip() for i in wanted}
    log(f"pushed {', '.join(wanted)} to the shared registry: {', '.join(refs.values())}")
    return refs


def dev_mount_env(
    env: Mapping[str, str], block: OperatorBlock, binary: Path, log: Callable[[str], None]
) -> dict[str, str]:
    """For a ``dev_mount`` operator: project the checkout over the base images on the lane and return the ``SDO_*``
    variables that make the install mount it. Empty for an image-built operator."""
    if not block.dev_mount:
        return {}
    if sink.pull_mode(env):
        # no node directory: the tree is pushed into the evidence claim (push_dev_tree) and mounted from there
        return {"SDO_DEV_MOUNT": "1", "SDO_DEV_MOUNT_PATH": DEV_DIRECTORY, "SDO_DEV_MOUNT_CLAIM": sink.EVIDENCE_CLAIM}
    host, node = _need(env, "BENCH_EVIDENCE_HOST"), _need(env, "BENCH_EVIDENCE_NODE")
    started = time.monotonic()
    devtree.sync(Path.cwd(), Path(host) / DEV_DIRECTORY, binary)
    log(f"projected the checkout over the base images in {time.monotonic() - started:.1f}s")
    return {"SDO_DEV_MOUNT": "1", "SDO_DEV_MOUNT_PATH": f"{node}/{DEV_DIRECTORY}"}


def push_dev_tree(
    env: Mapping[str, str], kube: Kube, control: str, lane_dir: Path, binary: Path | None, log: Callable[[str], None]
) -> None:
    """Pull mode: project the checkout (and the operator binary, when given) into the lane's evidence claim, where the
    dev-mounted pods read it. The source directories are emptied in place first so running pods see the new files."""
    started = time.monotonic()
    stage = lane_dir / "dev-stage"
    shutil.rmtree(stage, ignore_errors=True)
    devtree.sync(Path.cwd(), stage / DEV_DIRECTORY, binary)
    wipe = [f"{DEV_DIRECTORY}/{rel}" for rel in devtree.PROJECTED if (stage / DEV_DIRECTORY / rel).is_dir()]
    try:
        sink.push_paths(kube.cmd, control, stage, [DEV_DIRECTORY], wipe=wipe)
    except sink.SinkError as exc:
        raise HookError(str(exc)) from exc
    log(f"pushed the checkout into the evidence claim in {time.monotonic() - started:.1f}s")


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


def pull_evidence(env: Mapping[str, str], kube: Kube, control: str) -> None:
    """Copy the lane's evidence claim into the host sink directory (pull mode only)."""
    sink.pull_run_dir(env, kube.cmd, control)


def remove_operator(kube: Kube, control: str, log: Callable[[str], None], env: Mapping[str, str] | None = None) -> None:
    """Delete the control namespace first, waiting until it is gone (its Pods get SIGTERM, so the evidence sidecar
    runs its final pass), then the cluster grant install-existing made for it. A pull-mode lane (``env``) stops the
    operator's workloads first, copies the evidence claim out and only then deletes the namespace that holds it."""
    if env is not None and sink.pull_mode(env):
        has_ns = subprocess.run(kube.cmd("get", "namespace", control), capture_output=True, timeout=60, check=False)
        if has_ns.returncode == 0:
            _run(
                kube.cmd(
                    "-n", control, "delete", "deployments,jobs,statefulsets", "--all", "--wait=true", "--timeout=600s"
                ),
                f"stop the operator in {control}",
                660,
            )
            try:
                pull_evidence(env, kube, control)
                log(f"pulled the evidence of {control} into the host sink")
            except sink.SinkError as exc:
                log(f"evidence pull failed (the namespace is deleted anyway): {exc}")
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
    try:
        evidence_env = sink.install_env(env)
    except sink.SinkError as exc:
        raise HookError(str(exc)) from exc
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
    dev_env = dev_mount_env(env, block, binary, out)
    # (e) the operator images on the workers
    image = block.images()
    registry = Path(env["BENCH_REGISTRY"]) if env.get("BENCH_REGISTRY") else None
    registry = registry if registry is not None and registry.is_file() else None
    pulled = sink.pull_mode(env)
    volume_env: dict[str, str] = {}
    if pulled:
        refs = lane_registry_images(registry, list(image.values()), out)
        image = {name: refs[tag] for name, tag in image.items()}
    else:
        load_images(cluster, [image["controller"], image["responder"], image["script"]], out, registry=registry)
    kube = _kube(env)
    if (
        subprocess.run(
            kube.cmd("get", "namespace", baseline.control), capture_output=True, timeout=60, check=False
        ).returncode
        == 0
    ):
        # A previous attempt left an operator: start over from the baseline.
        remove_operator(kube, baseline.control, out, env)
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
    if pulled:
        # no node holds the app's images by tag: they are pulled from their registries, nothing to declare
        nodes = json.loads(_run(kube.cmd("get", "nodes", "-o", "json"), "nodes", 120).stdout)
        try:
            volume_node = sink.volume_node(nodes, _need(env, "BENCH_LANE"))
        except sink.SinkError as exc:
            raise HookError(str(exc)) from exc
        volume_env["SDO_VOLUME_NODE"] = volume_node
        out(f"the lane's volumes live on {volume_node}")
        _run(
            kube.cmd("apply", "-f", "-"),
            "evidence claim",
            120,
            input=json.dumps(sink.claim_manifest(baseline.control, volume_node)),
        )
        if block.dev_mount:
            push_dev_tree(env, kube, baseline.control, lane_dir, binary, out)
    else:
        for line in declare_node_images(cfg, images(source.manifests())):
            out(f"declared {line}")
    # (g) the baseline memory as the accepted repository
    memory = clone_baseline(baseline, lane_dir / "app")
    # (h) production restore: install-existing, history kept, no install-time agents
    args, launch_env = install_existing_launch(
        binary.resolve(),
        lane_dir / "app",
        baseline,
        block,
        kubeconfig,
        {**evidence_env, **dev_env, **volume_env},
        lane_dir / "sdo-config",
        image,
    )
    (lane_dir / "install-existing-args.json").write_text(json.dumps(args, indent=1) + "\n")
    (lane_dir / "install-existing-env.json").write_text(json.dumps(launch_env, indent=1, sort_keys=True) + "\n")
    with (
        (lane_dir / "install-existing.json").open("w") as receipt,
        (lane_dir / "install-existing.log").open("w") as log,
    ):
        rc = subprocess.run(
            args, stdout=receipt, stderr=log, timeout=1500, check=False, env={**os.environ, **launch_env}
        ).returncode
    if env.get("BENCH_OUT"):
        bench_out = Path(env["BENCH_OUT"])
        bench_out.mkdir(parents=True, exist_ok=True)
        for name in (
            "install-existing-args.json",
            "install-existing-env.json",
            "install-existing.json",
            "install-existing.log",
        ):
            shutil.copy2(lane_dir / name, bench_out / name)
    if rc != 0:
        tail = (lane_dir / "install-existing.log").read_text()[-300:].strip().replace("\n", " ")
        raise HookError(f"install-existing exited {rc}: {tail}")
    # install-existing returns before the controller runs: the reset waits for it to acknowledge.
    reset = reset_when_running(binary.resolve(), baseline.control, kubeconfig)
    out(f"incidents reset: generation {reset.get('generation')}")
    (lane_dir / PREPARED_RECORD).write_text(
        json.dumps({"revision": revision, "memory": memory, "compiled": devtree.fingerprint(Path.cwd())}) + "\n"
    )
    out(json.dumps({"pack": str(attached), "operator_version": revision, "memory": memory}))
    return 0


RESET_TIMEOUT = "2m"


def _binary(env: Mapping[str, str]) -> Path:
    """The ``sdo-operator`` prepare built into the lane directory."""
    binary = Path(_need(env, "BENCH_LANE_DIR")) / "bin" / "sdo-operator"
    if not binary.is_file():
        raise HookError(f"{binary} missing: the lane was not prepared")
    return binary.resolve()


def reset_incidents(binary: Path, control: str, kubeconfig: Path | None) -> dict[str, Any]:
    """``sdo-operator incidents reset``: the operator closes any open or escalated incident as RESET (no learning
    credit, memory untouched). Returns its JSON receipt; raises unless the controller acknowledged it."""
    args = [str(binary), "incidents", "reset", "--control-namespace", control, "--timeout", RESET_TIMEOUT]
    if kubeconfig is not None:
        args += ["--kubeconfig", str(kubeconfig)]
    proc = subprocess.run(args, capture_output=True, text=True, timeout=300, check=False)
    lines = [line for line in proc.stdout.splitlines() if line.strip()]
    try:
        receipt = json.loads(lines[-1]) if lines else {}
    except json.JSONDecodeError:
        receipt = {}
    if proc.returncode != 0 or not receipt.get("acknowledged"):
        why = (proc.stderr or proc.stdout).strip()[-300:]
        raise HookError(f"incidents reset not acknowledged (exit {proc.returncode}): {why}")
    return receipt


RESET_PATIENCE_S = 15 * 60.0


def reset_when_running(
    binary: Path,
    control: str,
    kubeconfig: Path | None,
    patience: float = RESET_PATIENCE_S,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> dict[str, Any]:
    """``reset_incidents`` tried again until the controller, just installed, acknowledges (at most ``patience``)."""
    start = clock()
    while True:
        try:
            return reset_incidents(binary, control, kubeconfig)
        except HookError:
            if clock() - start > patience:
                raise
        sleep(15.0)


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
    record_prepared_state(env, kube, control)
    out("idle")
    return 0


def record_prepared_state(env: Mapping[str, str], kube: Kube, control: str) -> None:
    """The first idle after ``prepare`` is the operator finished starting: note the accepted repository as it is
    then, what ``swap`` later asserts. Best effort: without it ``swap`` refuses."""
    lane = env.get("BENCH_LANE_DIR")
    if not lane or not (Path(lane) / PREPARED_RECORD).is_file():
        return
    path = Path(lane) / PREPARED_RECORD
    try:
        record = json.loads(path.read_text())
        if "state" not in record:
            record["state"] = repository_state(kube, control)
            path.write_text(json.dumps(record) + "\n")
    except (OSError, ValueError, subprocess.SubprocessError, HookError):
        return


def watch(env: Mapping[str, str]) -> int:
    """The observer only: the evidence sink carries the transcripts, so no snapshot is sealed on the PVC."""
    argv = ["--control-namespace", control_namespace(env)]
    fault = env.get("BENCH_FAULT", "none")
    if fault and fault != "none":
        argv += ["--fault", fault]
    block = _operator(env) if env.get("BENCH_OPERATOR") else None
    if block is not None and block.harness_page is not None:
        argv += [
            "--page",
            json.dumps(asdict(block.harness_page)),
            "--page-pack",
            _need(env, "BENCH_PACK"),
            "--page-namespace",
            _need(env, "FUZZOPS_NAMESPACE"),
        ]
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

    def evidence() -> str:
        if sink.pull_mode(env):
            pull_evidence(env, kube, control)
        copied = sink.copy_interval(
            sink.host_run_dir(env),
            dest / "evidence",
            sink.parse_time(env.get("BENCH_INTERVAL_START")),
            sink.parse_time(env.get("BENCH_INTERVAL_END")),
        )
        summary = copied.summary()
        (dest / "transcripts.json").write_text(json.dumps(summary, indent=2) + "\n")
        counts = f"{len(copied.incidents)} incidents, {copied.tasks} tasks, {summary['rollouts']} rollouts"
        if summary["missing"]:
            raise HookError(f"transcripts missing: {len(summary['missing'])} ({counts})")
        return f"ok ({counts})"

    part("memory", memory)
    part("state", state)
    part("logs", logs)
    part("evidence", evidence)

    def dashboard() -> str:
        value = view.write(dest)
        return f"ok ({len(value['events'])} events, {len(value['panels'])} panels)"

    part("view", dashboard)

    def reset() -> str:
        kubeconfig = Path(env["KUBECONFIG"]).resolve() if env.get("KUBECONFIG") else None
        receipt = reset_incidents(_binary(env), control, kubeconfig)
        (dest / "incidents-reset.json").write_text(json.dumps(receipt, indent=2) + "\n")
        return f"ok (generation {receipt.get('generation')})"

    part("reset", reset)
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
    remove_operator(_kube(env), control_namespace(env), out, env)
    return 0


def repository_state(kube: Kube, control: str) -> dict[str, str]:
    """The accepted repository as the controller holds it: head, uncommitted files and the outcome files."""
    script = (
        f"cd {APPLICATION_ROOT} && git -c safe.directory='*' rev-parse HEAD && echo -- && "
        "git -c safe.directory='*' status --porcelain && echo -- && ls .sdo/outcomes 2>/dev/null; true"
    )
    head, status, outcomes = (part.strip() for part in kube.exec_controller(control, script).decode().split("--\n"))
    return {"head": head, "status": status, "outcomes": outcomes}


def wipe_problems(before: Mapping[str, str], now: Mapping[str, str]) -> list[str]:
    """Why a lane is not what ``prepare`` left (``before``): the accepted head moved, files are uncommitted or
    outcomes were written. A swapped lane must start every cell from the same memory as a fresh one."""
    problems = []
    if now["head"] != before["head"]:
        problems.append(f"accepted head {now['head'][:12]} is not the prepared {before['head'][:12]}")
    if now["status"]:
        problems.append("uncommitted files in the accepted repository")
    if now["outcomes"] != before["outcomes"]:
        problems.append("outcomes changed since prepare")
    return problems


SWAP_STANDBY_WAIT_S = 180.0


def swap(env: Mapping[str, str], out: Callable[[str], None] = _print) -> int:
    """Run the hooks checkout's Python sources on a prepared ``dev_mount`` lane without restarting the controller:
    project them over the lane's base images, retire the idle responder standby so the next one imports them, and
    assert the lane is still the baseline's (head, files, outcomes) with an idle operator. Refused when the lane
    was prepared on a revision whose compiled code differs."""
    block = _operator(env)
    if not block.dev_mount:
        raise HookError("swap needs settings.dev_mount")
    lane_dir = Path(_need(env, "BENCH_LANE_DIR")).resolve()
    record = json.loads((lane_dir / PREPARED_RECORD).read_text())
    revision = checkout_revision()
    if why := devtree.swap_problem(Path.cwd(), record.get("compiled") or {}):
        raise HookError(why)
    started = time.monotonic()
    kube, control = _kube(env), control_namespace(env)
    if sink.pull_mode(env):
        push_dev_tree(env, kube, control, lane_dir, None, lambda _m: None)
    else:
        devtree.sync(Path.cwd(), Path(_need(env, "BENCH_EVIDENCE_HOST")) / DEV_DIRECTORY, None)
    retire_standby(kube, control)
    wait_standby(kube, control, SWAP_STANDBY_WAIT_S)
    if "state" not in record:
        raise HookError("the lane has no recorded prepared state (no idle verdict since prepare)")
    if problems := wipe_problems(record["state"], repository_state(kube, control)):
        raise HookError("the lane is not the baseline's: " + "; ".join(problems))
    if why := idle_reason(kube.state(control)):
        raise HookError(f"the operator is not idle: {why}")
    record["revision"] = revision
    (lane_dir / PREPARED_RECORD).write_text(json.dumps(record) + "\n")
    out(json.dumps(swap_receipt(lane_dir, revision, record["memory"], time.monotonic() - started)))
    return 0


def swap_receipt(lane_dir: Path, revision: str, memory: str, seconds: float) -> dict[str, Any]:
    """The last stdout line of ``swap``: ``prepare``'s fields (the bench reads both alike) plus its duration."""
    return {
        "pack": str((lane_dir / "pack").resolve()),
        "operator_version": revision,
        "memory": memory,
        "swap_s": round(seconds, 1),
    }


def standby_jobs(kube: Kube, control: str) -> list[str]:
    proc = subprocess.run(
        kube.cmd("-n", control, "get", "jobs", "-o", "name"), capture_output=True, text=True, timeout=60, check=False
    )
    return [line for line in proc.stdout.split() if "sdo-standby-" in line]


def retire_standby(kube: Kube, control: str) -> None:
    for job in standby_jobs(kube, control):
        _run(kube.cmd("-n", control, "delete", job, "--wait=false"), f"delete {job}", 60)


def wait_standby(kube: Kube, control: str, timeout: float) -> None:
    """Wait until a standby Job's pod is Ready (the controller recreates the standby it lost)."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if standby_jobs(kube, control):
            proc = subprocess.run(
                kube.cmd("-n", control, "get", "pods", "-o", "json"),
                capture_output=True,
                text=True,
                timeout=60,
                check=False,
            )
            for pod in json.loads(proc.stdout or "{}").get("items", []):
                name = pod["metadata"]["name"]
                conditions = pod.get("status", {}).get("conditions", [])
                if "sdo-standby-" in name and any(c["type"] == "Ready" and c["status"] == "True" for c in conditions):
                    return
        time.sleep(2)
    raise HookError(f"no ready responder standby within {timeout:.0f}s")


# ---- reset: the lane goes back to its prepared state in place ------------------------------------------------


GOLDEN_DIRECTORY = "app-golden"
RESET_IDLE_WAIT_S = 300.0
MAINTENANCE_ACK_WAIT_S = 120.0


@dataclass(frozen=True)
class ResetSteps:
    """What ``reset_lane`` does to the lane, one callable per step (tests substitute them)."""

    reset_incidents: Callable[[], object]
    retire_standby: Callable[[], object]
    stop_controller: Callable[[], object]
    restore_repository: Callable[[str], object]
    app_reset: Callable[[], dict[str, Any]]
    start_controller: Callable[[], object]
    wait_standby: Callable[[], object]
    repository_state: Callable[[], Mapping[str, str]]
    controller_idle: Callable[[], str]

    @classmethod
    def unused(cls) -> ResetSteps:
        def refuse(*_: object) -> Any:
            raise AssertionError("step must not run")

        return cls(*([refuse] * 9))


def reset_lane(env: Mapping[str, str], steps: ResetSteps, out: Callable[[str], None] = _print) -> dict[str, Any]:
    """Return a used, prepared lane to the state ``prepare`` left, with the controller, its images and its startup
    facts kept: close any incident and end the responder session, stop the controller (its Job is suspended; the
    process caches the accepted commit, so a repository moved behind a live controller is unsafe), put the accepted
    repository back to the prepared head, return the app to its golden copy, start the controller again (a new
    process on the same repository, state and images), and assert the repository equals the prepared state and the
    operator is idle. Raises ``HookError`` unless the lane is the
    prepared one (the caller then rebuilds it)."""
    lane_dir = Path(_need(env, "BENCH_LANE_DIR")).resolve()
    record = json.loads((lane_dir / PREPARED_RECORD).read_text())
    if "state" not in record:
        raise HookError("the lane has no recorded prepared state (no idle verdict since prepare)")
    prepared = record["state"]
    started = time.monotonic()
    steps.reset_incidents()
    steps.retire_standby()
    steps.stop_controller()
    out("controller stopped")
    steps.restore_repository(prepared["head"])
    app = steps.app_reset()
    out(f"app reset: {json.dumps(app)}")
    steps.start_controller()
    steps.wait_standby()
    if problems := wipe_problems(prepared, steps.repository_state()):
        raise HookError("the lane is not the prepared one after the reset: " + "; ".join(problems))
    if why := steps.controller_idle():
        raise HookError(f"the operator is not idle after the reset: {why}")
    receipt = swap_receipt(lane_dir, record["revision"], record["memory"], time.monotonic() - started)
    receipt["reset_s"] = receipt.pop("swap_s")
    receipt["app"] = app
    return receipt


def set_maintenance(env: Mapping[str, str], paused: bool, generation: str) -> None:
    """``sdo-operator maintenance``; a pause waits for the controller's acknowledgement of the generation (a resume is
    not acknowledged)."""
    control = control_namespace(env)
    kube = _kube(env)
    args = [str(_binary(env)), "maintenance", "--control-namespace", control, "--generation", generation]
    if paused:
        args.append("--paused")
    if env.get("KUBECONFIG"):
        args += ["--kubeconfig", str(Path(env["KUBECONFIG"]).resolve())]
    proc = subprocess.run(args, capture_output=True, text=True, timeout=120, check=False)
    if proc.returncode != 0:
        raise HookError(f"maintenance failed ({proc.returncode}): {(proc.stderr or proc.stdout).strip()[-300:]}")
    if not paused:  # only a pause is acknowledged: the controller resumes by itself
        return
    deadline = time.monotonic() + MAINTENANCE_ACK_WAIT_S
    while time.monotonic() < deadline:
        done = subprocess.run(
            kube.cmd("-n", control, "get", "configmap", "sdo-controller-maintenance-ack", "-o", "jsonpath={.data}"),
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
        if generation in done.stdout:
            return
        time.sleep(1)
    raise HookError(f"the controller did not acknowledge maintenance {generation} within {MAINTENANCE_ACK_WAIT_S:.0f}s")


def set_controller_suspended(kube: Kube, control: str, suspended: bool, timeout: float = 300.0) -> None:
    """Suspend or resume the controller Job. Suspending ends its pod (SIGTERM: the evidence sidecar's final pass runs)
    and waits until none is left."""
    _run(
        kube.cmd("-n", control, "patch", "job", CONTROLLER_JOB, "-p", json.dumps({"spec": {"suspend": suspended}})),
        "suspend the controller" if suspended else "resume the controller",
        60,
    )
    if not suspended:
        return
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not kube.controller_pods(control):
            return
        time.sleep(2)
    raise HookError(f"the controller pod did not end within {timeout:.0f}s")


def restore_repository(kube: Kube, control: str, head: str) -> None:
    """With the controller stopped, put the accepted repository on its volume back at ``head`` with no other file
    (outcomes and playbooks it learned go), in a short-lived pod that mounts the volume as the controller does."""
    if not _COMMIT.match(head):
        raise HookError(f"prepared head {head!r} is not a commit")
    job = kube._json("-n", control, "get", "job", CONTROLLER_JOB) or {}
    spec = ((job.get("spec") or {}).get("template") or {}).get("spec") or {}
    container = next((c for c in spec.get("containers", []) if c.get("name") == CONTROLLER_CONTAINER), None)
    if container is None:
        raise HookError(f"no controller container in job {CONTROLLER_JOB}")
    volume = next(
        (v for v in spec.get("volumes", []) if v.get("persistentVolumeClaim") and v["name"] == "repository"), None
    )
    mount = next((m for m in container.get("volumeMounts", []) if m.get("name") == "repository"), None)
    if volume is None or mount is None:
        raise HookError("the controller job has no repository volume")
    name = "sdo-restore-repository"
    script = (
        f"cd {APPLICATION_ROOT} && git -c safe.directory='*' reset -q --hard {head} && "
        "git -c safe.directory='*' clean -q -fdx && git -c safe.directory='*' rev-parse HEAD"
    )
    pod = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": name, "namespace": control},
        "spec": {
            "restartPolicy": "Never",
            "securityContext": spec.get("securityContext") or {},
            "containers": [
                {
                    "name": "restore",
                    "image": container["image"],
                    "imagePullPolicy": "IfNotPresent",
                    "command": ["sh", "-c", script],
                    "securityContext": container.get("securityContext") or {},
                    "volumeMounts": [mount],
                }
            ],
            "volumes": [volume],
        },
    }
    _run(
        kube.cmd("-n", control, "delete", "pod", name, "--ignore-not-found", "--wait=true"),
        "delete old restore pod",
        120,
    )
    _run(kube.cmd("apply", "-f", "-"), "restore pod", 60, input=json.dumps(pod))
    try:
        deadline = time.monotonic() + 180
        while time.monotonic() < deadline:
            proc = subprocess.run(
                kube.cmd("-n", control, "get", "pod", name, "-o", "jsonpath={.status.phase}"),
                capture_output=True,
                text=True,
                timeout=60,
                check=False,
            )
            if proc.stdout.strip() == "Succeeded":
                logs = _run(kube.cmd("-n", control, "logs", name), "restore pod log", 60).stdout.strip()
                if logs.splitlines()[-1:] != [head]:
                    raise HookError(f"repository is at {logs[-50:]!r}, not {head}")
                return
            if proc.stdout.strip() == "Failed":
                logs = subprocess.run(
                    kube.cmd("-n", control, "logs", name), capture_output=True, text=True, check=False
                )
                raise HookError(f"repository restore failed: {(logs.stdout + logs.stderr).strip()[-300:]}")
            time.sleep(2)
        raise HookError("repository restore pod did not finish in 180s")
    finally:
        subprocess.run(
            kube.cmd("-n", control, "delete", "pod", name, "--ignore-not-found", "--wait=false"),
            capture_output=True,
            check=False,
        )


def fuzzops_app(env: Mapping[str, str], verb: str) -> dict[str, Any]:
    """``fuzzops app snapshot|reset`` for the lane's app against its golden directory; the receipt is its last
    stdout line. A nonzero exit (the app is not the golden copy) is a ``HookError``."""
    binary = env.get("FUZZOPS_BIN") or shutil.which("fuzzops")
    if not binary:
        raise HookError("fuzzops binary not found (FUZZOPS_BIN)")
    golden = Path(_need(env, "BENCH_LANE_DIR")).resolve() / GOLDEN_DIRECTORY
    if verb == "reset" and not (golden / "meta.json").is_file():
        raise HookError(f"no golden copy of the app in {golden}: run the snapshot verb on a prepared lane first")
    proc = subprocess.run(
        [
            binary,
            "app",
            verb,
            "--pack",
            _need(env, "BENCH_PACK"),
            "--namespace",
            _need(env, "FUZZOPS_NAMESPACE"),
            "--dir",
            str(golden),
        ],
        capture_output=True,
        text=True,
        timeout=1800,
        check=False,
    )
    lines = [line for line in proc.stdout.splitlines() if line.strip()]
    if proc.returncode != 0:
        raise HookError(f"app {verb} failed: {(proc.stderr.strip().splitlines() or ['no output'])[-1][-400:]}")
    try:
        return dict(json.loads(lines[-1]))
    except (IndexError, json.JSONDecodeError) as exc:
        raise HookError(f"app {verb}: no receipt on stdout") from exc


def wait_idle(kube: Kube, control: str, timeout: float) -> str:
    """Poll until the controller is idle as the ``idle`` verb judges it (fresh state, no incident, closure or
    reflection, startup done); the last reason otherwise. A state the controller has not written since a pause is
    stale, never idle."""
    deadline = time.monotonic() + timeout
    why = "no controller state yet"
    while time.monotonic() < deadline:
        try:
            state, updated = kube.state_configmap(control)
            why = idle_verdict(state, updated, kube.controller_ready(control), datetime.now(UTC))
        except (subprocess.SubprocessError, json.JSONDecodeError, OSError) as exc:
            why = f"controller read failed: {exc}"
        if not why:
            return ""
        time.sleep(3)
    return why


def reset(env: Mapping[str, str], out: Callable[[str], None] = _print) -> int:
    kube, control = _kube(env), control_namespace(env)
    binary, kubeconfig = _binary(env), Path(env["KUBECONFIG"]).resolve() if env.get("KUBECONFIG") else None
    steps = ResetSteps(
        reset_incidents=lambda: reset_incidents(binary, control, kubeconfig),
        retire_standby=lambda: retire_standby(kube, control),
        stop_controller=lambda: set_controller_suspended(kube, control, True),
        restore_repository=lambda head: restore_repository(kube, control, head),
        app_reset=lambda: fuzzops_app(env, "reset"),
        start_controller=lambda: set_controller_suspended(kube, control, False),
        wait_standby=lambda: wait_standby(kube, control, SWAP_STANDBY_WAIT_S),
        repository_state=lambda: repository_state(kube, control),
        controller_idle=lambda: wait_idle(kube, control, RESET_IDLE_WAIT_S),
    )
    out(json.dumps(reset_lane(env, steps, out)))
    return 0


def snapshot(env: Mapping[str, str], out: Callable[[str], None] = _print) -> int:
    """Take the lane's app golden copy while the controller is paused (the copy scales the data stores down).
    Run once on a prepared, idle lane; ``reset`` returns the lane to this state."""
    started = time.monotonic()
    generation = f"snapshot-{int(time.time())}"
    set_maintenance(env, True, generation + "-pause")
    try:
        app = fuzzops_app(env, "snapshot")
    finally:
        set_maintenance(env, False, generation + "-resume")
    kube, control = _kube(env), control_namespace(env)
    if why := wait_idle(kube, control, RESET_IDLE_WAIT_S):
        raise HookError(f"the operator is not idle after the snapshot: {why}")
    out(json.dumps({"snapshot_s": round(time.monotonic() - started, 1), "app": app}))
    return 0


def transcripts(env: Mapping[str, str], out: Callable[[str], None] = _print) -> int:
    try:
        if sink.pull_mode(env):
            control = control_namespace(env)
            kube = _kube(env)
            # after the teardown the claim is gone and the host copy it left is what counts
            if (
                subprocess.run(
                    kube.cmd("get", "namespace", control), capture_output=True, timeout=60, check=False
                ).returncode
                == 0
            ):
                pull_evidence(env, kube, control)
        why = sink.transcripts_verdict(sink.host_run_dir(env))
    except sink.SinkError as exc:
        why = f"no sink: {exc}"
    except OSError as exc:
        why = f"no sink: unreadable: {exc}"
    if why:
        out(why)
        return 1
    out("transcripts: ok")
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
    overlay = json.loads(a.config) if a.config else DEFAULT_CONFIG
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
            config=overlay,
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
    p.add_argument(
        "--config",
        default="",
        help="JSON operator config overlay the lanes ran with beyond sdo-bench.yaml (default: none)",
    )
    p.add_argument("--sdo-revision", default="", help="the SDO commit the lane ran (default: this checkout)")
    baseline = archive_baseline(p.parse_args(argv))
    print(json.dumps({"baseline": str(baseline.root), "commit": baseline.commit}))
    return 0


VERBS = ("check", "prepare", "swap", "reset", "snapshot", "idle", "watch", "collect", "teardown", "transcripts")


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
        verbs: dict[str, Callable[[Mapping[str, str]], int]] = {
            "check": check,
            "prepare": prepare,
            "swap": swap,
            "reset": reset,
            "snapshot": snapshot,
            "idle": idle,
            "collect": collect,
            "teardown": teardown,
            "transcripts": transcripts,
        }
        return verbs[verb](env)
    except (HookError, ValueError, OSError, subprocess.SubprocessError) as exc:
        reason = " ".join(str(exc).split()) or type(exc).__name__
        print(reason if verb == "check" else f"error: {reason}", flush=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
