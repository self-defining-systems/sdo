"""The command fuzzops' ``exec:sdo`` operator runs for each window: watch the SDO controller, report what it does.

SDO already runs in the cluster (it was installed once, ``benchmarks.fuzzops.install``); fuzzops neither starts
nor stops it. This watcher polls the controller state ConfigMap and

* prints applied plans and successful repairs to stdout, which fuzzops records as operator actions;
* sends lifecycle and evidence notices to stderr;
* writes ``$FUZZOPS_REPORT`` (fuzzops' diagnosis vocabulary) from the detector findings as soon as an incident
  opens, and again from the responder's confirmed root causes once it has them;
* keeps every distinct state snapshot under ``$FUZZOPS_OUT`` for later analysis;
* exits once the incident it saw is closed (or keeps watching until fuzzops ends the window).
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import threading
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from benchmarks.fuzzops.cellevidence import atomic_write_json
from benchmarks.fuzzops.evidence import EvidenceError, EvidenceTarget, snapshot, snapshot_id_for
from benchmarks.fuzzops.page import (
    DEFAULT_ENTRY,
    PageConfig,
    Pager,
    fallback_message,
    fuzzops_check,
    inbox_manifest,
    kubectl_apply,
)
from benchmarks.fuzzops.sink import incident_id_time
from benchmarks.fuzzops.window import concerns, fault_targets

STATE_CONFIGMAP = "sdo-controller-state"
STATE_KEY = "runtime-state.json"
PAGE_EVIDENCE = "sdo-page.json"


@dataclass
class PageEvidence:
    """First incident opened in this run and the harness's inbox submission, if any."""

    inject_at: str | None = None
    sdo_page_at: str | None = None
    page_source: str | None = None
    fallback_page_at: str | None = None
    fallback_id: str | None = None
    #: The harness page's kind: ``fallback`` (only when SDO opened nothing by the deadline) or ``scheduled``
    #: (always, at ``scheduled_offset_s`` after the inject, for a ``detection.pages_only`` controller).
    page_kind: str = "fallback"
    scheduled_offset_s: float | None = None
    paired_cell: str | None = None
    #: Seconds from the inject to the acknowledged harness page write.
    paged_offset_s: float | None = None
    #: Whether SDO had opened an incident of its own when the harness paged (always false under pages_only).
    sdo_incident_before_page: bool | None = None

    def observe(self, state: dict[str, Any]) -> bool:
        if self.sdo_page_at or (inject := _parse_time(self.inject_at)) is None:
            return False
        closure = _get(state, "pending_closure") or {}
        request = _get(state, "incident_request") or _get(closure, "request") or {}
        if _get(request, "external_message"):
            return False  # the explicitly configured fixed-schedule arm
        detected = _get(state, "incident_detected_at") or _get(closure, "detected_at")
        opened = _parse_time(detected)
        # A fast recovery may already have been acknowledged between two polls.
        ack = incident_id_time(str(_get(state, "last_acknowledged_incident_id") or ""))
        if ack is not None and ack >= inject and (opened is None or ack < opened):
            detected = _stamp(ack)
            opened = ack
            fallback = self.fallback_id is not None and _get(state, "last_page_id") == self.fallback_id
        else:
            fallback = bool(self.fallback_id and _get(request, "page") and not _get(request, "opening_keys"))
        if opened is None or opened < inject:
            return False
        self.sdo_page_at = str(detected)
        self.page_source = self.page_kind if fallback else "own"
        if fallback and self.fallback_page_at is None:
            # The API write can succeed even when kubectl times out waiting.
            self.fallback_page_at = self.sdo_page_at
        return True


def traffic_inject_at(path: Path) -> str | None:
    """FuzzOps puts its exact injection clock in the first live traffic sample."""
    with path.open() as traffic:
        line = traffic.readline()
    if not line.endswith("\n"):
        return None
    value = json.loads(line)["inject_at"]
    if _parse_time(value) is None:
        raise ValueError(f"invalid inject_at in {path}")
    return value


_KIND_PREFIX = {
    "Deployment": "deploy",
    "StatefulSet": "sts",
    "DaemonSet": "ds",
    "Service": "svc",
    "ConfigMap": "cm",
    "Secret": "secret",
    "Pod": "pod",
    "PersistentVolumeClaim": "pvc",
    "NetworkPolicy": "networkpolicy",
    "Job": "job",
}

# (pattern, mechanism, location, fault_type): first match wins. fuzzops' vocabulary (CONTRACT.md "Diagnosis
# report"); SDO's root causes are free text, so this is a best-effort translation recorded beside the raw text.
_MECHANISMS: tuple[tuple[str, str, str, str], ...] = (
    (
        r"imagepull|errimagepull|image (tag|reference)|cannot be pulled|nonexistent image|image .* not found",
        "image-wrong",
        "delivery",
        "unavailable",
    ),
    (r"oomkill|out of memory|memory limit", "resource-exhausted", "process", "exhausted"),
    (r"targetport|target port|port mismatch|wrong port|does not listen", "port-mismatch", "entry", "unavailable"),
    (r"selector", "selector-wrong", "entry", "wrong"),
    (r"readiness probe|liveness probe|\bprobe\b", "probe-wrong", "config", "unavailable"),
    (r"scaled (down|to zero)|replicas.{0,20}\b0\b|zero replicas", "scaled-down", "platform", "unavailable"),
    (r"networkpolicy|network policy", "network-blocked", "link", "unavailable"),
    (r"consul|registration|service discovery", "state-lost", "dependency", "unavailable"),
    (r"memcache|cache entr|cached value|garbage|corrupt", "data-corrupt", "data", "unavailable"),
    (r"read-only|readonly|write lock|fsynclock|refuses writes", "read-only", "data", "unavailable"),
    (r"index|validator|schema", "schema-changed", "data", "unavailable"),
    (r"configmap .*(deleted|missing)|missing key|key .* missing", "config-missing", "config", "unavailable"),
    (r"deleted", "object-deleted", "platform", "unavailable"),
    (r"latency|delay|slow", "network-delay", "link", "slow"),
    (r"packet loss|dropped", "network-loss", "link", "unavailable"),
    (r"cpu", "cpu-starved", "process", "slow"),
    (r"environment variable|env var|config value|address|hostname|url", "config-value-wrong", "config", "unavailable"),
    (r"crash|killed|exit code", "process-stopped", "process", "unavailable"),
)


def component(ref: dict[str, Any]) -> str:
    """``deploy/x`` style name of an ObjectRef (ProtoJSON)."""
    kind, name = ref.get("kind", ""), ref.get("name", "")
    if not name:
        return ""
    return f"{_KIND_PREFIX.get(kind, kind.lower())}/{name}"


def classify(text: str) -> tuple[str, str, str]:
    """(mechanism, location, fault_type) for a root cause text; empty strings when nothing matches."""
    low = text.lower()
    for pattern, mechanism, location, fault_type in _MECHANISMS:
        if re.search(pattern, low):
            return mechanism, location, fault_type
    return "", "", ""


def _get(obj: dict[str, Any], *names: str) -> Any:
    """A field under its snake_case or lowerCamel ProtoJSON name."""
    for name in names:
        if name in obj:
            return obj[name]
        camel = re.sub(r"_([a-z])", lambda m: m.group(1).upper(), name)
        if camel in obj:
            return obj[camel]
    return None


# SDO's typed cause classes (ConfirmedRootCause.cause_class) in fuzzops' mechanism vocabulary; a class with no
# fuzzops counterpart falls back to the keyword map over the cause text.
CAUSE_CLASS_MECHANISM = {
    "network-delay": "network-delay",
    "network-loss": "network-loss",
    "network-blocked": "network-blocked",
    "config-value-wrong": "config-value-wrong",
    "object-missing": "object-deleted",
    "admission-mutated": "admission-mutated",
    "admission-rejected": "admission-blocked",
    "permission-denied": "permission-removed",
    "credential-invalid": "credentials-revoked",
    "capacity-exhausted": "resource-exhausted",
    "process-runtime": "process-stopped",
    "state-lost": "state-lost",
    "data-corrupt": "data-corrupt",
    "data-missing": "data-missing",
    "read-only": "read-only",
    "code-defect": "code-defect",
}


def cause_report(cause: dict[str, Any]) -> dict[str, Any]:
    """One confirmed root cause in fuzzops' report vocabulary (raw SDO class and text kept)."""
    summary = str(_get(cause, "summary") or "")
    refs = _get(cause, "resources") or []
    comp = component(refs[0]) if refs else ""
    evidence = " ".join(str(_get(e, "observation") or "") for e in (_get(cause, "evidence") or []))
    mechanism, location, fault_type = classify(summary + " " + evidence)
    cause_class = str(_get(cause, "cause_class") or "")
    if CAUSE_CLASS_MECHANISM.get(cause_class):
        mechanism = CAUSE_CLASS_MECHANISM[cause_class]
    out: dict[str, Any] = {
        "component": comp,
        "location": location,
        "fault_type": fault_type,
        "mechanism": mechanism,
        "summary": "SDO responder: " + summary,
    }
    if cause_class:
        out["sdo_cause_class"] = cause_class
    reach = str(_get(cause, "reach") or "")
    if reach:
        out["reach"] = reach
    return out


def report_from_result(result: dict[str, Any]) -> dict[str, Any] | None:
    """fuzzops report from an IncidentResult: the first confirmed root cause is primary, all are in ``causes``."""
    causes = [cause_report(c) for c in (_get(result, "confirmed_root_causes") or [])]
    if not causes:
        return None
    report = dict(causes[0])
    if len(causes) > 1:
        report["causes"] = causes
    return report


def lifecycle_times(state: dict[str, Any], result: dict[str, Any]) -> dict[str, str]:
    """fuzzops lifecycle stamps (``<stage>_at``) from SDO's own incident record."""
    stamps: dict[str, str] = {}
    detected = _get(state, "incident_detected_at")
    if detected:
        stamps["detected_at"] = str(detected)
    actions = _get(result, "repair_actions") or []
    started = sorted(str(_get(a, "started_at")) for a in actions if _get(a, "started_at"))
    if started:
        stamps["mitigation_started_at"] = started[0]
    done = sorted(str(_get(a, "completed_at")) for a in actions if _get(a, "success") and _get(a, "completed_at"))
    if done:
        stamps["mitigated_at"] = done[-1]
    return stamps


def _finding_key(finding: dict[str, Any]) -> str:
    return f"{_get(finding, 'detector_id')}\x00{_get(finding, 'fingerprint')}"


def report_from_findings(findings: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Preliminary fuzzops report from the incident's detector findings (before the responder answers)."""
    for finding in findings:
        ref = _get(finding, "primary_resource") or {}
        summary = str(_get(finding, "summary") or "")
        if not ref and not summary:
            continue
        mechanism, location, fault_type = classify(summary + " " + str(_get(finding, "evidence") or ""))
        return {
            "component": component(ref),
            "location": location,
            "fault_type": fault_type,
            "mechanism": mechanism,
            "summary": f"SDO detector {_get(finding, 'detector_id')}: {summary}",
        }
    return None


@dataclass(frozen=True)
class WatchLine:
    text: str
    action: bool = False


@dataclass
class Watcher:
    """Turns successive controller states into notices, actions and reports. Pure: no I/O."""

    started: datetime
    targets: frozenset[str] = frozenset()  # resource names of the faulted component (see ``window``)
    seen: set[str] = field(default_factory=set)
    incident: str = ""
    closed: bool = False
    stamps: dict[str, str] = field(default_factory=dict)
    cause: dict[str, Any] | None = None

    def _once(self, key: str, line: str, out: list[WatchLine], *, action: bool = False) -> None:
        if key not in self.seen:
            self.seen.add(key)
            out.append(WatchLine(line, action))

    def step(self, state: dict[str, Any]) -> tuple[list[WatchLine], dict[str, Any] | None]:
        """Messages for new SDO steps, and the report to write when it changed (cause plus lifecycle stamps)."""
        lines, cause = self._step(state)
        if cause is not None:
            self.cause = cause
        if self.cause is None:
            return lines, None
        report = {**self.cause, **self.stamps}
        if cause is None and report == self._last:
            return lines, None
        self._last = report
        return lines, report

    _last: dict[str, Any] | None = None

    def _stamp(self, name: str, value: str) -> None:
        self.stamps.setdefault(name, value)

    def _step(self, state: dict[str, Any]) -> tuple[list[WatchLine], dict[str, Any] | None]:
        lines: list[WatchLine] = []
        report: dict[str, Any] | None = None
        request = _get(state, "incident_request") or {}
        result = _get(state, "incident_result") or {}
        closure = _get(state, "pending_closure") or {}
        if not result and closure:
            result = _get(closure, "result") or {}
        if not request and closure:
            request = _get(closure, "request") or {}
        detected_raw = _get(state, "incident_detected_at") or _get(closure, "detected_at")
        detected = _parse_time(detected_raw)
        incident = str(_get(request, "incident_id") or _get(result, "incident_id") or "")
        findings = _get(request, "findings") or []
        standing_keys = set(_get(request, "standing_keys") or [])
        current_findings = [f for f in findings if _finding_key(f) not in standing_keys]
        opening_keys = _get(request, "opening_keys")
        if opening_keys is None and not (_get(request, "page") or _get(request, "external_message")):
            # Older detector-opened requests only carried the findings.
            openers = current_findings
        else:
            keys = set(opening_keys or [])
            openers = [f for f in current_findings if _finding_key(f) in keys]
        # An incident SDO opened (and maybe already handed to closure) shortly
        # before this watcher started still belongs to the window: the watcher
        # starts some seconds after the inject, and a fast detection beats it.
        active = bool(_get(state, "incident_open") or closure)
        # Older than the lookback but still open now, so open at the inject: it is this window's when its findings,
        # causes or repair actions name the faulted component.
        mine = detected is not None and (
            detected >= self.started
            or concerns(
                {
                    "findings": current_findings,
                    "confirmed_root_causes": _get(result, "confirmed_root_causes") or [],
                    "repair_actions": _get(result, "repair_actions") or [],
                },
                self.targets,
            )
        )
        if active and detected is not None and mine:
            self.incident = self.incident or incident or f"detected-{detected.isoformat()}"
            display = openers or current_findings
            what = "; ".join(f"{_get(f, 'detector_id')}: {_get(f, 'summary')}" for f in display[:3])
            self._once(
                "open", f"sdo: incident opened at {detected.strftime('%H:%M:%S')} ({what or 'findings pending'})", lines
            )
            self._stamp("detected_at", str(detected_raw))
            if openers and "report:findings" not in self.seen and "report:result" not in self.seen:
                report = report_from_findings(openers)
                if report:
                    self.seen.add("report:findings")
                    if report.get("component"):
                        self._stamp("localized_at", _now())
            if _get(state, "incident_dispatched_at"):
                self._once("dispatch", "sdo: responder dispatched", lines)
        if not self.incident:
            return lines, report
        if result and (not incident or incident == self.incident or self.incident.startswith("detected-")):
            for plan in _get(result, "plan_applications") or []:
                pid = _get(plan, "playbook_id") or _get(plan, "program_id") or "plan"
                applied = bool(_get(plan, "applied")) and not (_get(plan, "dry_run") or _get(plan, "rolled_back"))
                status = "applied" if applied else "no retained application for"
                self._once(
                    f"plan:{json.dumps(plan, sort_keys=True)[:200]}",
                    f"sdo automatic stage: {status} {pid}",
                    lines,
                    action=applied,
                )
            for action in _get(result, "repair_actions") or []:
                aid = str(_get(action, "action_id") or json.dumps(action, sort_keys=True)[:120])
                success = bool(_get(action, "success"))
                ok = "ok" if success else "failed"
                self._once(
                    f"repair:{aid}",
                    f"sdo repair ({ok}): {_get(action, 'kind')} {_get(action, 'target')}: {_get(action, 'summary')}",
                    lines,
                    action=success,
                )
            for name, value in lifecycle_times(state, result).items():
                if name != "detected_at":
                    self._stamp(name, value)
            if "report:result" not in self.seen:
                report = report_from_result(result) or report
                if report and report["summary"].startswith("SDO responder"):
                    self.seen.add("report:result")
                    self._stamp("diagnosed_at", _now())
                    if report.get("reach") == "out-of-reach":
                        self._stamp("escalated_at", _now())
                    self._once("diagnosis", f"sdo diagnosis: {report['summary']}", lines)
            if _get(state, "responder_done"):
                self._once("done", f"sdo: responder done ({_get(result, 'status') or 'no status'})", lines)
        receipt = _get(state, "closure_receipt") or {}
        last_ack = str(_get(state, "last_acknowledged_incident_id") or "")
        rid = str(_get(receipt, "incident_id") or "")
        if (rid and rid == self.incident) or (last_ack and last_ack == self.incident):
            self.closed = True
            self._once("closed", f"sdo: incident {self.incident} closed", lines)
        return lines, report


_EVENTS = (
    ("detected_at", "detected"),
    ("localized_at", "localized"),
    ("diagnosed_at", "diagnosed"),
    ("mitigation_started_at", "mitigation_started"),
    ("mitigated_at", "mitigated"),
    ("escalated_at", "escalated"),
)
_CAUSE_FIELDS = ("component", "location", "fault_type", "mechanism", "summary", "reach", "causes")


def timeline_events(watcher: Watcher, emitted: set[str]) -> list[dict[str, Any]]:
    """fuzzops timeline lines for lifecycle stamps not emitted yet; cause stages carry the current cause."""
    events: list[dict[str, Any]] = []
    for stamp, event in _EVENTS:
        at = watcher.stamps.get(stamp)
        if not at or event in emitted:
            continue
        emitted.add(event)
        line: dict[str, Any] = {"event": event, "at": at}
        if event in {"localized", "diagnosed", "escalated"} and watcher.cause:
            line.update({k: watcher.cause[k] for k in _CAUSE_FIELDS if k in watcher.cause})
        if event == "escalated":
            line["reach"] = "out-of-reach"
        events.append(line)
    return events


def _now() -> str:
    return _stamp(datetime.now(UTC))


def _stamp(at: datetime) -> str:
    return at.isoformat().replace("+00:00", "Z")


def _parse_time(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def read_state(context: str, control_namespace: str, kubeconfig: str = "") -> dict[str, Any] | None:
    cmd = ["kubectl"]
    if kubeconfig:
        cmd += ["--kubeconfig", kubeconfig]
    if context:
        cmd += ["--context", context]
    cmd += ["-n", control_namespace, "get", "configmap", STATE_CONFIGMAP, "-o", "json"]
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=30, check=False)
    if proc.returncode != 0:
        return None
    raw = (json.loads(proc.stdout).get("data") or {}).get(STATE_KEY)
    return json.loads(raw) if raw else None


def seal_boundary(target: EvidenceTarget, incident: str) -> None:
    """Seal the runtime evidence at an incident boundary; a failure is reported, never fatal."""
    try:
        sealed = snapshot(target, snapshot_id_for(incident))
        print(
            f"sdo: evidence snapshot {sealed.get('snapshot_id')} sealed ({sealed.get('bytes')} bytes)",
            file=sys.stderr,
            flush=True,
        )
    except (EvidenceError, OSError, subprocess.TimeoutExpired, ValueError) as exc:
        print(f"evidence snapshot failed: {exc}", file=sys.stderr, flush=True)


def start_pager(a: argparse.Namespace, context: str, out: Path) -> threading.Thread:
    """Page SDO's responder from the harness (the harness-paged arm): the incident the watcher then follows."""
    config = PageConfig.from_settings(json.loads(a.page))
    binary = a.page_fuzzops or Path(os.environ.get("FUZZOPS_BIN") or shutil.which("fuzzops") or "")
    if config is None or not a.page_pack or not a.page_namespace or not binary.name:
        raise SystemExit("--page needs a page config, --page-pack, --page-namespace and a fuzzops binary")
    t0 = time.time()
    pager = Pager(
        config=config,
        namespace=a.page_namespace,
        control_namespace=a.control_namespace,
        pack=a.page_pack,
        incident_id=f"page-{int(t0)}",
        out=out,
        live_check=lambda: fuzzops_check(binary, a.page_pack, a.page_namespace, config),
        submit=lambda manifest: kubectl_apply(manifest, a.kubeconfig, context),
        entry=os.environ.get("BENCH_PAGE_ENTRY") or DEFAULT_ENTRY,
    )

    def run() -> None:
        try:
            pager.run(t0)
            print(f"sdo: harness page sent at +{round(time.time() - t0)}s", file=sys.stderr, flush=True)
        except Exception as exc:  # the window must go on; the missing page shows in the record
            print(f"harness page failed: {exc}", file=sys.stderr, flush=True)

    thread = threading.Thread(target=run, daemon=True, name="harness-pager")
    thread.start()
    return thread


def submit_fallback(paging: PageEvidence, receipt: Path, args: argparse.Namespace, context: str) -> None:
    """Send the generic page (the fallback, or the scheduled page: same text, same inbox)."""
    if paging.fallback_id is None:
        paging.fallback_id = f"{paging.page_kind}-{uuid.uuid4().hex}"
        paging.sdo_incident_before_page = paging.sdo_page_at is not None
        atomic_write_json(receipt, asdict(paging))
    try:
        message = fallback_message(args.page_namespace)
        kubectl_apply(
            inbox_manifest(args.control_namespace, paging.fallback_id, args.page_namespace, message),
            args.kubeconfig,
            context,
        )
    except (subprocess.SubprocessError, OSError) as exc:
        # Retry the same id if the write timed out. The controller accepts an id once.
        print(f"fallback page failed: {exc}", file=sys.stderr, flush=True)
        return
    sent = datetime.fromtimestamp(time.time(), UTC)
    paging.fallback_page_at = _stamp(sent)
    if (inject := _parse_time(paging.inject_at)) is not None:
        paging.paged_offset_s = round((sent - inject).total_seconds(), 3)
    atomic_write_json(receipt, asdict(paging))
    print(f"sdo: {paging.page_kind} page sent", file=sys.stderr, flush=True)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--control-namespace", required=True)
    p.add_argument("--kubeconfig", default=os.environ.get("KUBECONFIG", ""))
    p.add_argument("--interval", type=float, default=3.0)
    p.add_argument("--linger", type=float, default=20.0, help="seconds to keep watching after closure")
    p.add_argument(
        "--lookback",
        type=float,
        default=90.0,
        help="seconds before the watcher's start an incident may have opened and still be this window's "
        "(the watcher starts after the inject; run it only when SDO has no open incident)",
    )
    p.add_argument(
        "--fault",
        default="",
        help="the fuzzops instance ID; an incident still open from before the lookback that names its component "
        "by resource reference belongs to this window",
    )
    p.add_argument("--operator-binary", type=Path, help="seal an evidence snapshot when the incident closes")
    p.add_argument("--controller-image", default="", help="with --operator-binary: the controller image")
    p.add_argument("--page", default="", help="the harness-paged arm: PageConfig as JSON (see page.py)")
    p.add_argument("--fallback-page-after", type=float, help="seconds from inject before sending a generic page")
    p.add_argument(
        "--scheduled-page-after",
        type=float,
        help="seconds from inject at which to send the generic page whatever SDO saw (a detection.pages_only arm)",
    )
    p.add_argument("--paired-cell", default="", help="with --scheduled-page-after: the row whose page set the offset")
    p.add_argument("--page-pack", type=Path, help="with --page: the fuzzops pack (journeys.yaml)")
    p.add_argument("--page-namespace", default="", help="with --page: the application namespace")
    p.add_argument("--page-fuzzops", type=Path, help="with --page: the fuzzops binary (default: FUZZOPS_BIN or PATH)")
    a = p.parse_args(argv)
    evidence: EvidenceTarget | None = None
    if a.operator_binary is not None and a.controller_image:
        evidence = EvidenceTarget(
            a.operator_binary.resolve(),
            a.control_namespace,
            a.controller_image,
            Path(a.kubeconfig) if a.kubeconfig else None,
        )
    context = os.environ.get("FUZZOPS_KUBE_CONTEXT", "")
    report_path = os.environ.get("FUZZOPS_REPORT", "")
    timeline_path = os.environ.get("FUZZOPS_TIMELINE", "")
    emitted: set[str] = set()
    out = Path(os.environ.get("FUZZOPS_OUT") or ".")
    out.mkdir(parents=True, exist_ok=True)
    traffic = os.environ.get("FUZZOPS_TRAFFIC", "")
    if a.fallback_page_after is not None and (not traffic or not a.page_namespace):
        raise ValueError("fallback paging needs the application namespace and a FuzzOps pin with FUZZOPS_TRAFFIC")
    scheduled = a.scheduled_page_after is not None
    if scheduled and (a.fallback_page_after is not None or a.page or not traffic or not a.page_namespace):
        raise ValueError(
            "a scheduled page excludes the fallback and harness pages and needs the application namespace and "
            "FUZZOPS_TRAFFIC"
        )
    if a.page:
        start_pager(a, context, out)
    paging = PageEvidence()
    if scheduled:
        paging.page_kind, paging.scheduled_offset_s, paging.paired_cell = (
            "scheduled",
            a.scheduled_page_after,
            a.paired_cell,
        )
    receipt = out / PAGE_EVIDENCE
    deadline: float | None = None
    watcher = Watcher(started=datetime.now(UTC) - timedelta(seconds=a.lookback), targets=fault_targets(a.fault))
    last_raw = ""
    closed_at: float | None = None
    n = 0
    while closed_at is None or time.monotonic() - closed_at < a.linger:
        if traffic and paging.inject_at is None and (injected := traffic_inject_at(Path(traffic))):
            paging.inject_at = injected
            inject = datetime.fromisoformat(injected)
            watcher.started = inject
            atomic_write_json(receipt, asdict(paging))
            if a.fallback_page_after is not None:
                deadline = inject.timestamp() + a.fallback_page_after
            if scheduled:
                deadline = inject.timestamp() + a.scheduled_page_after
        read_started = time.time()
        if scheduled and deadline is not None and read_started >= deadline and paging.fallback_page_at is None:
            # Sent before the state read, so the page is not late by a read; SDO's own incident does not matter.
            submit_fallback(paging, receipt, a, context)
        try:
            state = read_state(context, a.control_namespace, a.kubeconfig)
        except (subprocess.TimeoutExpired, json.JSONDecodeError) as exc:
            print(f"state read failed: {exc}", file=sys.stderr, flush=True)
            state = None
        if state is not None:
            if paging.observe(state):
                atomic_write_json(receipt, asdict(paging))
            if (
                not scheduled
                and deadline is not None
                and read_started >= deadline
                and paging.sdo_page_at is None
                and paging.fallback_page_at is None
                and not _get(state, "incident_open")
                and not _get(state, "pending_closure")
            ):
                submit_fallback(paging, receipt, a, context)
            raw = json.dumps(state, sort_keys=True)
            if raw != last_raw:
                last_raw = raw
                n += 1
                (out / f"state-{n:04d}-{datetime.now(UTC).strftime('%H%M%S')}.json").write_text(raw)
            lines, report = watcher.step(state)
            for line in lines:
                print(" ".join(line.text.splitlines()), file=sys.stdout if line.action else sys.stderr, flush=True)
            if report and report_path:
                tmp = Path(report_path + ".tmp")
                tmp.write_text(json.dumps(report))
                tmp.replace(report_path)
                (out / "report.json").write_text(json.dumps(report, indent=2))
            events = timeline_events(watcher, emitted)
            if events:
                with (out / "timeline.jsonl").open("a") as copy:
                    copy.writelines(json.dumps(e) + "\n" for e in events)
                if timeline_path:
                    with open(timeline_path, "a") as timeline:
                        timeline.writelines(json.dumps(e) + "\n" for e in events)
            if watcher.closed and closed_at is None:
                closed_at = time.monotonic()
                if evidence is not None:
                    seal_boundary(evidence, watcher.incident)
        # Poll at the deadline even when it falls between two normal reads.
        sleep = a.interval
        if deadline is not None and read_started < deadline and (scheduled or paging.sdo_page_at is None):
            sleep = min(sleep, max(0.0, deadline - time.time()))
        time.sleep(sleep)
    return 0


if __name__ == "__main__":
    sys.exit(main())
