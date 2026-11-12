"""Prototype: the compact `sdo detect` text summary (users first, suspects on failing paths, background last).

Built from the detect response (findings, drift events), the app map's operation paths, the incident facts file
(playbook coverage), and bounded live reads (Pods for rollouts/restarts, logs for error counts).
Every item ends with `more:`, an exact command that expands it.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import yaml

CAP = 2048
_FAILURE = re.compile(
    r"\[(?P<at>[^ \]]+) iteration \d+\] step (?P<step>\S+) (?P<request>.*?) → (?P<label>[^:]+): (?P<error>.*?)(?=; \[|\. Replay with|$)"
)
_ERROR_LINE = re.compile(
    r"\b(error|exception|fatal|panic|denied|refused|unauthori[sz]ed|forbidden|timed? ?out|unreachable)\b", re.IGNORECASE
)
_MAP = re.compile(r"<!-- sdo:application-map:begin -->\s*```yaml\n(.*?)```", re.DOTALL)


def _time(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
    except ValueError:
        return None


def _hm(value: datetime | None) -> str:
    return value.strftime("%H:%M:%SZ") if value else "?"


def _clip(text: str, limit: int) -> str:
    text = " ".join(str(text).split())
    return text if len(text) <= limit else text[: limit - 1] + "…"


def _load_map(root: Path) -> dict[str, Any]:
    try:
        match = _MAP.search((root / ".sdo" / "arch.md").read_text(encoding="utf-8"))
        return (yaml.safe_load(match.group(1)) or {}) if match else {}
    except (OSError, yaml.YAMLError):
        return {}


def _kubectl(*args: str, timeout: float = 4.0) -> str:
    try:
        return subprocess.run(["kubectl", *args], capture_output=True, text=True, timeout=timeout, check=False).stdout
    except (OSError, subprocess.TimeoutExpired):
        return ""


class _Map:
    def __init__(self, document: dict[str, Any]) -> None:
        self.components = [item for item in document.get("components") or [] if isinstance(item, dict)]
        operations = {item.get("id"): item for item in document.get("operations") or [] if isinstance(item, dict)}
        self.paths: dict[str, list[str]] = {}
        for probe in document.get("probes") or []:
            path: list[str] = []
            for operation in probe.get("operations") or []:
                for component in (operations.get(operation) or {}).get("path") or []:
                    if component not in path:
                        path.append(component)
            self.paths[str(probe.get("scenario"))] = path
        self.names = sorted((str(item.get("name")) for item in self.components), key=len, reverse=True)

    def workload(self, component: str) -> str:
        for item in self.components:
            if item.get("name") == component:
                return str(item.get("workload") or "")
        return ""

    def resolve(self, name: str) -> str:
        """The component whose name, workload or Service the object name contains (longest wins)."""
        for item in self.components:
            workload = str(item.get("workload") or "").split("/")[-1]
            if name in (item.get("name"), workload) or name in (item.get("services") or []):
                return str(item.get("name"))
        for candidate in self.names:
            if candidate and candidate in name:
                return candidate
        return ""


def _failing_tests(findings: list[dict[str, Any]], app_map: _Map) -> list[dict[str, Any]]:
    tests = []
    for finding in findings:
        metadata = finding.get("metadata") or {}
        scenario = metadata.get("scenario")
        if not scenario:
            continue
        failures = [m.groupdict() for m in _FAILURE.finditer(finding.get("evidence", ""))]
        last = failures[-1] if failures else {}
        times = [t for t in (_time(item["at"]) for item in failures) if t]
        bad = int(metadata.get("errors") or 0) + int(metadata.get("timeouts") or 0)
        tests.append(
            {
                "scenario": scenario,
                "path": app_map.paths.get(scenario) or list(metadata.get("depends_on") or []),
                "step": last.get("step", "?"),
                "status": last.get("label", ",".join(metadata.get("violations") or [])[:40]),
                "error": last.get("error", ""),
                "onset": min(times) if times else None,
                "ratio": f"{bad}/{metadata.get('samples', '?')}",
                "fingerprint": finding.get("fingerprint", ""),
            }
        )
    return tests


def _pods(namespace: str) -> list[dict[str, Any]]:
    try:
        return json.loads(_kubectl("-n", namespace, "get", "pods", "-o", "json") or "{}").get("items", [])
    except ValueError:
        return []


def _changes(
    component: str, app_map: _Map, drift: list[dict[str, Any]], pods: list[dict[str, Any]], namespace: str
) -> list[tuple[datetime, str, str]]:
    """(when, text, more) for drift events, Pod starts and container restarts of one component."""
    changes = []
    for event in drift:
        ref = event.get("ref") or {}
        if app_map.resolve(str(ref.get("name", ""))) != component:
            continue
        when = _time(event.get("manager_at")) or _time(event.get("observed_at"))
        fields = ",".join(_clip(item.get("path", ""), 40) for item in (event.get("fields") or [])[:2])
        who = event.get("manager") or "unknown"
        kind_ref = f"{ref.get('kind')}/{ref.get('namespace') or namespace}/{ref.get('name')}"
        text = f"{ref.get('kind')}/{ref.get('name')} {fields or 'changed'} by {who}"
        if who != "sdo" and not str(who).startswith("sdo"):
            text += " (not sdo)"
        if when:
            changes.append((when, text, f"sdo source {kind_ref}"))
    workload = app_map.workload(component).split("/")[-1]
    for pod in pods:
        name = pod["metadata"]["name"]
        if not workload or not name.startswith(workload + "-"):
            continue
        started = _time(pod["metadata"].get("creationTimestamp"))
        if started:
            changes.append(
                (started, f"Pod {name} created (rollout or replacement)", f"kubectl -n {namespace} describe pod {name}")
            )
        for status in (pod.get("status") or {}).get("containerStatuses") or []:
            terminated = (status.get("lastState") or {}).get("terminated") or {}
            finished = _time(terminated.get("finishedAt"))
            if finished and status.get("restartCount"):
                changes.append(
                    (
                        finished,
                        f"container {status['name']} restarted ({status['restartCount']}x, {terminated.get('reason', '?')})",
                        f"kubectl -n {namespace} logs {name} -c {status['name']} --previous --tail=50",
                    )
                )
    return changes


def _logs(component: str, app_map: _Map, pods: list[dict[str, Any]], namespace: str, onset: datetime | None) -> str:
    workload = app_map.workload(component).split("/")[-1]
    running = [
        pod
        for pod in pods
        if workload
        and pod["metadata"]["name"].startswith(workload + "-")
        and (pod.get("status") or {}).get("phase") == "Running"
    ]
    if not running or onset is None:
        return ""
    pod = sorted(running, key=lambda item: item["metadata"].get("creationTimestamp", ""))[-1]["metadata"]["name"]
    span = min(datetime.now(UTC) - onset, timedelta(minutes=5))
    since = (onset - span).strftime("%Y-%m-%dT%H:%M:%SZ")
    text = _kubectl(
        "-n",
        namespace,
        "logs",
        pod,
        "--all-containers",
        "--timestamps",
        f"--since-time={since}",
        "--tail=4000",
        timeout=3,
    )
    before = after = 0
    sample = ""
    for line in text.splitlines():
        if not _ERROR_LINE.search(line):
            continue
        stamp = _time(line.split(" ", 1)[0][:19] + "Z")
        if stamp and stamp < onset:
            before += 1
        else:
            after += 1
            sample = sample or line.split(" ", 1)[-1]
    onset_text = onset.strftime("%Y-%m-%dT%H:%M:%SZ")
    summary = f"logs: {after} error lines since onset, {before} before"
    if sample:
        summary += f': "{_clip(sample, 70)}"'
    return (
        summary + f"\n    more: kubectl -n {namespace} logs {pod} --all-containers --since-time={onset_text} | tail -50"
    )


def _playbooks(component: str, app_map: _Map, facts: dict[str, Any]) -> list[str]:
    lines = []
    for match in (facts.get("playbook_coverage") or {}).get("matches") or []:
        bindings = match.get("bindings") or []
        names = json.dumps(bindings)
        if any(app_map.resolve(name) == component for name in re.findall(r'"name": "([^"]+)"', names)):
            command = match.get("mitigate_command") or f"sdo diagnose {match.get('playbook_id')}"
            lines.append(f"{match.get('playbook_id')} (learned; advice)\n    more: {command}")
    return lines


def render(response: dict[str, Any], *, namespace: str, root: Path, onset_text: str, facts_path: str) -> str:
    findings = [item for item in response.get("findings") or [] if item.get("status", "active") == "active"]
    drift = list(response.get("drift_events") or [])
    app_map = _Map(_load_map(root))
    facts: dict[str, Any] = {}
    if facts_path:
        try:
            facts = json.loads(Path(facts_path).read_text(encoding="utf-8"))
        except (OSError, ValueError):
            facts = {}
    onset = _time(onset_text)
    failing = _failing_tests(findings, app_map)
    failing_ids = {test["scenario"] for test in failing}
    passing = [scenario for scenario in app_map.paths if scenario not in failing_ids]
    on_passing = {component for scenario in passing for component in app_map.paths.get(scenario, [])}
    paths = [test["path"] for test in failing if test["path"]]
    suspects: list[str] = []
    if paths:
        common = [c for c in paths[0] if all(c in p for p in paths[1:])]
        rest = [c for p in paths for c in p if c not in common]
        for component in common + rest:
            if component not in suspects:
                suspects.append(component)
    other = [item for item in findings if not (item.get("metadata") or {}).get("scenario")]
    if not suspects:
        for item in other:
            component = app_map.resolve(str((item.get("primary_resource") or {}).get("name", "")))
            if component and component not in suspects:
                suspects.append(component)
    pods = _pods(namespace)
    details = {}
    for component in suspects:
        changes = _changes(component, app_map, drift, pods, namespace)
        before = [c for c in changes if onset is None or c[0] <= onset + timedelta(seconds=30)]
        latest = max(before or changes, key=lambda c: c[0]) if changes else None
        details[component] = latest

    # Suspects with a change shortly before onset first; then the order along the failing paths.
    def rank(component: str) -> tuple[int, float]:
        latest = details.get(component)
        near = (
            latest is not None
            and onset is not None
            and onset - timedelta(minutes=30) <= latest[0] <= onset + timedelta(seconds=30)
        )
        return (0 if near else 1, -(latest[0].timestamp() if near and latest else 0))

    order = {component: index for index, component in enumerate(suspects)}
    suspects.sort(key=lambda c: (rank(c), order[c]))
    out: list[str] = []
    out.append(
        f"incident {os.environ.get('SDO_INCIDENT_ID', '?')} onset {_hm(onset)} now {_hm(datetime.now(UTC))}; "
        f"{len(failing)} of {len(app_map.paths) or '?'} user tests failing"
    )
    if failing:
        out.append("\nFAILING USER TESTS")
        for test in failing[:3]:
            out.append(
                f"{test['scenario']} {'>'.join(test['path'])} step {test['step']} {test['status']} "
                f'"{_clip(test["error"], 60)}" since {_hm(test["onset"])} {test["ratio"]}'
            )
            out.append(f"  more: sdo detect --finding {test['fingerprint'][:12]}")
        if len(failing) > 3:
            out.append(f"+{len(failing) - 3} more: sdo detect --json")
    else:
        out.append("\nno failing user test: suspects come from findings")
    out.append("PASSING: " + _clip(" ".join(passing) or "none known", 200))
    used: set[str] = set()
    if suspects:
        out.append("\nSUSPECTS (on failing paths, most likely first)")
    for index, component in enumerate(suspects[:3]):
        workload = app_map.workload(component)
        out.append(f"{component} {workload}" + (" (also on passing paths)" if component in on_passing else ""))
        mine = [
            item
            for item in other
            if app_map.resolve(str((item.get("primary_resource") or {}).get("name", ""))) == component
        ]
        for item in mine[:1]:
            used.add(item.get("fingerprint", ""))
            out.append(f"  finding: {_clip(item.get('summary', ''), 110)}")
            out.append(f"    more: sdo detect --finding {item.get('fingerprint', '')[:12]}")
        for item in mine[1:]:
            used.add(item.get("fingerprint", ""))
        latest = details.get(component)
        if latest:
            when, text, more = latest
            rel = "before" if onset is None or when <= onset else "after"
            out.append(f"  change: {_clip(text, 100)} at {_hm(when)} ({rel} onset)")
            out.append(f"    more: {more}")
        else:
            out.append(
                f"  change: none seen since accepted\n    more: sdo source {workload.split('/')[0]}/{namespace}/{workload.split('/')[-1]}"
            )
        if index < 2:
            logs = _logs(component, app_map, pods, namespace, onset)
            if logs:
                out.append("  " + logs)
        for line in _playbooks(component, app_map, facts)[:1]:
            out.append("  playbooks: " + line)
    if len(suspects) > 3:
        out.append("others: " + " ".join(suspects[3:]) + "  more: sdo source Kind/ns/name")
    background = [item for item in other if item.get("fingerprint", "") not in used]
    if background:
        out.append("\nBACKGROUND (not on a failing path)")
    tail = [
        f"{_clip(item.get('summary', ''), 90)}  more: sdo detect --finding {item.get('fingerprint', '')[:12]}"
        for item in background
    ]
    text = "\n".join(out)
    shown = 0
    for line in tail:
        if len(text) + len(line) + 60 > CAP:
            break
        text += "\n" + line
        shown += 1
    if shown < len(tail):
        text += f"\n+{len(tail) - shown} more: sdo detect --json"
    if len(text.encode()) > CAP:
        text = text.encode()[: CAP - 40].decode(errors="ignore") + "\n…(cut) more: sdo detect --json"
    return text


def render_finding(response: dict[str, Any], prefix: str) -> str:
    for item in response.get("findings") or []:
        if (
            str(item.get("fingerprint", "")).startswith(prefix)
            or (item.get("metadata") or {}).get("scenario") == prefix
        ):
            ref = item.get("primary_resource") or {}
            return _clip(
                f"{item.get('summary', '')}\nresource: {ref.get('kind')}/{ref.get('namespace')}/{ref.get('name')}\n"
                f"evidence: {item.get('evidence', '')}",
                6000,
            )
    return f"no current finding matches {prefix!r}; sdo detect lists the current ones"
