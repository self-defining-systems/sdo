"""The command fuzzops' ``exec:sdo`` operator runs for each window: watch the SDO controller, report what it does.

SDO already runs in the cluster (it was installed once, ``benchmarks.fuzzops.install``); fuzzops neither starts
nor stops it. This watcher polls the controller state ConfigMap and

* prints one line per SDO step (incident opened, responder dispatched, each repair, closure): fuzzops records
  each stdout line as an operator action;
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
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

STATE_CONFIGMAP = "sdo-controller-state"
STATE_KEY = "runtime-state.json"

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


def report_from_result(result: dict[str, Any]) -> dict[str, str] | None:
    """fuzzops report from an IncidentResult's first confirmed root cause."""
    causes = _get(result, "confirmed_root_causes") or []
    if not causes:
        return None
    cause = causes[0]
    summary = str(_get(cause, "summary") or "")
    refs = _get(cause, "resources") or []
    comp = component(refs[0]) if refs else ""
    evidence = " ".join(str(_get(e, "observation") or "") for e in (_get(cause, "evidence") or []))
    mechanism, location, fault_type = classify(summary + " " + evidence)
    return {
        "component": comp,
        "location": location,
        "fault_type": fault_type,
        "mechanism": mechanism,
        "summary": "SDO responder: " + summary,
    }


def report_from_findings(findings: list[dict[str, Any]]) -> dict[str, str] | None:
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


@dataclass
class Watcher:
    """Turns successive controller states into action lines and reports. Pure: no I/O."""

    started: datetime
    seen: set[str] = field(default_factory=set)
    incident: str = ""
    closed: bool = False

    def _once(self, key: str, line: str, out: list[str]) -> None:
        if key not in self.seen:
            self.seen.add(key)
            out.append(line)

    def step(self, state: dict[str, Any]) -> tuple[list[str], dict[str, str] | None]:
        lines: list[str] = []
        report: dict[str, str] | None = None
        request = _get(state, "incident_request") or {}
        result = _get(state, "incident_result") or {}
        closure = _get(state, "pending_closure") or {}
        if not result and closure:
            result = _get(closure, "result") or {}
        if not request and closure:
            request = _get(closure, "request") or {}
        detected = _parse_time(_get(state, "incident_detected_at"))
        incident = str(_get(request, "incident_id") or _get(result, "incident_id") or "")
        if _get(state, "incident_open") and detected is not None and detected >= self.started:
            self.incident = self.incident or incident or f"detected-{detected.isoformat()}"
            findings = _get(request, "findings") or []
            what = "; ".join(f"{_get(f, 'detector_id')}: {_get(f, 'summary')}" for f in findings[:3])
            self._once(
                "open", f"sdo: incident opened at {detected.strftime('%H:%M:%S')} ({what or 'findings pending'})", lines
            )
            if findings and "report:findings" not in self.seen and "report:result" not in self.seen:
                report = report_from_findings(findings)
                if report:
                    self.seen.add("report:findings")
            if _get(state, "incident_dispatched_at"):
                self._once("dispatch", "sdo: responder dispatched", lines)
        if not self.incident:
            return lines, report
        if result and (not incident or incident == self.incident or self.incident.startswith("detected-")):
            for plan in _get(result, "plan_applications") or []:
                pid = _get(plan, "playbook_id") or _get(plan, "program_id") or "plan"
                self._once(
                    f"plan:{json.dumps(plan, sort_keys=True)[:200]}", f"sdo automatic stage: applied {pid}", lines
                )
            for action in _get(result, "repair_actions") or []:
                aid = str(_get(action, "action_id") or json.dumps(action, sort_keys=True)[:120])
                ok = "ok" if _get(action, "success") else "failed"
                self._once(
                    f"repair:{aid}",
                    f"sdo repair ({ok}): {_get(action, 'kind')} {_get(action, 'target')}: {_get(action, 'summary')}",
                    lines,
                )
            if "report:result" not in self.seen:
                report = report_from_result(result) or report
                if report and report["summary"].startswith("SDO responder"):
                    self.seen.add("report:result")
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


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--control-namespace", required=True)
    p.add_argument("--kubeconfig", default=os.environ.get("KUBECONFIG", ""))
    p.add_argument("--interval", type=float, default=3.0)
    p.add_argument("--linger", type=float, default=20.0, help="seconds to keep watching after closure")
    a = p.parse_args(argv)
    context = os.environ.get("FUZZOPS_KUBE_CONTEXT", "")
    report_path = os.environ.get("FUZZOPS_REPORT", "")
    out = Path(os.environ.get("FUZZOPS_OUT") or ".")
    out.mkdir(parents=True, exist_ok=True)
    watcher = Watcher(started=datetime.now(UTC))
    last_raw = ""
    closed_at: float | None = None
    n = 0
    while closed_at is None or time.monotonic() - closed_at < a.linger:
        try:
            state = read_state(context, a.control_namespace, a.kubeconfig)
        except (subprocess.TimeoutExpired, json.JSONDecodeError) as exc:
            print(f"state read failed: {exc}", file=sys.stderr, flush=True)
            state = None
        if state is not None:
            raw = json.dumps(state, sort_keys=True)
            if raw != last_raw:
                last_raw = raw
                n += 1
                (out / f"state-{n:04d}-{datetime.now(UTC).strftime('%H%M%S')}.json").write_text(raw)
            lines, report = watcher.step(state)
            for line in lines:
                print(line, flush=True)
            if report and report_path:
                tmp = Path(report_path + ".tmp")
                tmp.write_text(json.dumps(report))
                tmp.replace(report_path)
                (out / "report.json").write_text(json.dumps(report, indent=2))
            if watcher.closed and closed_at is None:
                closed_at = time.monotonic()
        time.sleep(a.interval)
    return 0


if __name__ == "__main__":
    sys.exit(main())
