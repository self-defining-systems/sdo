"""Run scripted scenarios through the fast loop and check every incident's evidence.

The harness wraps the fast loop's :class:`SdoPersistentAgent`: before each
injection it writes the incident's directive into the controller namespace,
optionally runs a chaos action, and after learning it checks the incident's
evidence (receipt, run record, broker ledger, operational memory, the scripted
CLI's turn ledger) against the scenario's expectations. One assurance record
per incident is appended to ``assurance.jsonl`` next to ``incidents.jsonl``.

Nothing here can reach a model: the images carry the scripted CLI instead of
Codex, the host lifecycle is reuse-only, and the harness process holds no
agent credentials.
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

from google.protobuf import json_format
from google.protobuf.json_format import ParseError

from benchmarks.sregym.adapter import (
    REJECTED_RECEIPT_FILENAME,
    STRICT_RECEIPT_FILENAME,
    control_namespace_for,
    kubectl,
)
from benchmarks.sregym.assurance.chaos import ChaosContext, ChaosThread
from benchmarks.sregym.assurance.scripted_codex.directive import (
    DIRECTIVE_CONFIGMAP,
    DIRECTIVE_KEY,
    SCRIPTED_LABEL,
    TURN_KEY,
)
from benchmarks.sregym.memory_cli import (
    artifact_ids,
    listing_arguments,
    outcomes_arguments,
    parse_listing,
    parse_outcomes,
    read_listing,
    read_outcomes,
)
from benchmarks.sregym.protocol import receipt_resolution
from sdo.contracts import OutcomeRecord

if TYPE_CHECKING:
    from collections.abc import Callable

    from benchmarks.sregym.assurance.scenarios import IncidentSpec
    from benchmarks.sregym.fastloop.loop import AgentOutcome, IncidentAgent, InjectionWindow
    from benchmarks.sregym.fastloop.records import AgentName

logger = logging.getLogger(__name__)

ASSURANCE_FILENAME = "assurance.jsonl"
#: The operational repository inside the controller pod.
_POD_REPOSITORY = "/workspace/application"
#: How long an abandoned (unverified) incident may take to settle after the harness recovered its fault.
SETTLE_TIMEOUT_SECONDS = 900.0


class AssuranceError(RuntimeError):
    """The harness could not set up or inspect a scripted incident."""


@dataclass(frozen=True)
class Check:
    name: str
    ok: bool
    detail: str = ""


@dataclass
class AssuranceRecord:
    index: int
    scenario: str
    problem_id: str
    incident_id: str | None
    chaos: str | None
    chaos_log: list[str] = field(default_factory=list)
    chaos_error: str | None = None
    resolve_error: str | None = None
    checks: list[Check] = field(default_factory=list)
    turns: list[dict[str, Any]] = field(default_factory=list)
    metrics: dict[str, Any] = field(default_factory=dict)

    @property
    def passed(self) -> bool:
        return all(check.ok for check in self.checks)

    def to_json(self) -> str:
        payload = asdict(self)
        payload["passed"] = self.passed
        return json.dumps(payload, sort_keys=True, default=str)


def write_directive(control_namespace: str, directive_json: str) -> None:
    document = {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": DIRECTIVE_CONFIGMAP,
            "namespace": control_namespace,
            "labels": {SCRIPTED_LABEL: "directive"},
        },
        "data": {DIRECTIVE_KEY: directive_json},
    }
    kubectl(["apply", "-f", "-"], namespace=control_namespace, input_text=json.dumps(document))


def scripted_turns(control_namespace: str) -> list[dict[str, Any]]:
    completed = kubectl(
        ["get", "configmaps", "-l", f"{SCRIPTED_LABEL}=turn", "-o", "json"], namespace=control_namespace, check=False
    )
    if completed.returncode != 0:
        raise AssuranceError(f"cannot list scripted turns: {completed.stderr.strip()}")
    turns = []
    for item in json.loads(completed.stdout).get("items", []):
        text = (item.get("data") or {}).get(TURN_KEY)
        if isinstance(text, str):
            turns.append(json.loads(text))
    return sorted(turns, key=lambda turn: str(turn.get("started_at")))


def _usage_key(usage: dict[str, Any]) -> tuple[int, int, int, int]:
    return (
        int(usage.get("input_tokens") or 0),
        int(usage.get("cached_input_tokens") or 0),
        int(usage.get("output_tokens") or 0),
        int(usage.get("reasoning_output_tokens") or 0),
    )


def _sum_usage(turns: list[dict[str, Any]]) -> tuple[int, int, int, int]:
    totals = [0, 0, 0, 0]
    for turn in turns:
        for position, value in enumerate(_usage_key(turn.get("usage") or {})):
            totals[position] += value
    return (totals[0], totals[1], totals[2], totals[3])


def _jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _manifest_detectors(workspace: Path) -> set[str]:
    return set(artifact_ids(read_listing(workspace), "detector"))


def _outcome_for(outcomes: list[OutcomeRecord], incident_id: str) -> OutcomeRecord | None:
    return next((outcome for outcome in outcomes if outcome.incident_id == incident_id), None)


def _outcome_from_cli(text: str | None, incident_id: str) -> OutcomeRecord | None:
    """One incident's outcome from ``memory outcomes --json`` output, or ``None`` when absent or malformed."""

    if text is None:
        return None
    try:
        return _outcome_for(parse_outcomes(text), incident_id)
    except ParseError:
        return None


def _evidenced_outcome(evidence: dict[str, Any], workspace: Path, incident_id: str) -> OutcomeRecord | None:
    ledger = evidence.get("native_ledger")
    document = ledger.get("outcome_document") if isinstance(ledger, dict) else None
    if isinstance(document, dict) and document.get("incident_id") == incident_id:
        return json_format.ParseDict(document, OutcomeRecord(), ignore_unknown_fields=True)
    return _outcome_for(read_outcomes(workspace), incident_id)


def _git(workspace: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(workspace), *args], check=False, capture_output=True, text=True
    ).stdout.strip()


def _evidenced_ledger(evidence: dict[str, Any], incident_id: str) -> dict[str, Any] | None:
    """The Go incident ledger the strict receipt carries (``native_ledger``) for *incident_id*."""

    ledger = evidence.get("native_ledger")
    if isinstance(ledger, dict) and ledger.get("incident_id") == incident_id:
        return ledger
    return None


def _applied_playbook_ids(ledger: dict[str, Any]) -> list[str]:
    result = (ledger.get("closure") or {}).get("result") or {}
    return [str(item.get("playbook_id")) for item in result.get("applied_playbooks") or []]


def _reflection_attempts(turns: list[dict[str, Any]]) -> int:
    """Reflection turns the scripted model ran; the receipt records no attempt count."""

    return sum(1 for turn in turns if str(turn.get("kind", "")).startswith("reflection"))


def _state_change_names(request: dict[str, Any]) -> list[str]:
    changes = (request.get("state_changes") or {}).get("changes") or []
    return [f"{item.get('kind')}/{item.get('name')}" for item in changes if isinstance(item, dict)]


#: Objects each fault class may legitimately change; anything else in the diff is a leak from another incident.
_FAULT_OBJECTS = {
    "network_policy_block": ("NetworkPolicy/",),
    "missing_configmap": ("ConfigMap/", "Deployment/", "ReplicaSet/", "Pod/"),
}


#: The incident detector each fault class's scripted reflection learns.
_FAULT_DETECTORS = {
    "network_policy_block": "deny-all-networkpolicy-isolation",
    "missing_configmap": "required-configmap-missing",
}


class ScriptedAgent:
    """An :class:`IncidentAgent` that scripts SDO's model and checks each incident's evidence."""

    def __init__(
        self,
        inner: IncidentAgent,
        specs: tuple[IncidentSpec, ...],
        *,
        namespace: str,
        cluster: str,
        workspace: Path,
        run_dir: Path,
        results_dir: Path,
        oracle: Callable[[], bool | None] | None = None,
    ) -> None:
        self._inner = inner
        self._specs = specs
        self._namespace = namespace
        self._control = control_namespace_for(namespace)
        self._cluster = cluster
        self._workspace = workspace
        self._run_dir = run_dir
        self._results = results_dir
        self._records: dict[int, AssuranceRecord] = {}
        self._unsettled: AssuranceRecord | None = None
        self._consumed_turns: set[str] = set()

    @property
    def name(self) -> AgentName:
        return "sdo"

    @property
    def model(self) -> str:
        return self._inner.model

    @property
    def records(self) -> list[AssuranceRecord]:
        return [self._records[index] for index in sorted(self._records)]

    def resolve(self, index: int, problem_id: str, inject: Callable[[], InjectionWindow]) -> AgentOutcome:
        self._settle()
        spec = self._specs[index]
        if spec.problem_id != problem_id:
            raise AssuranceError(f"incident {index}: loop problem {problem_id!r} is not the spec's {spec.problem_id!r}")
        record = AssuranceRecord(
            index=index, scenario=spec.directive.scenario, problem_id=problem_id, incident_id=None, chaos=spec.chaos
        )
        self._records[index] = record
        chaos: ChaosThread | None = None

        def scripted_inject() -> InjectionWindow:
            nonlocal chaos
            write_directive(self._control, spec.directive.to_json())
            window = inject()
            if spec.chaos:
                chaos = ChaosThread(
                    spec.chaos,
                    ChaosContext(control_namespace=self._control, namespace=self._namespace, cluster=self._cluster),
                )
                chaos.start()
            return window

        try:
            outcome = self._inner.resolve(index, problem_id, scripted_inject)
        except Exception as exc:
            record.resolve_error = f"{type(exc).__name__}: {exc}"
            self._unsettled = record
            raise
        finally:
            if chaos is not None:
                chaos.finish()
                record.chaos_log = list(chaos.ctx.log)
                record.chaos_error = chaos.error
        record.incident_id = outcome.incident_id
        return outcome

    def learn(self, outcome: AgentOutcome) -> AgentOutcome:
        record = next(r for r in self._records.values() if r.incident_id == outcome.incident_id)
        spec = self._specs[record.index]
        try:
            learned = self._inner.learn(outcome)
        except Exception as exc:
            record.resolve_error = f"learning failed: {type(exc).__name__}: {exc}"
            record.checks.append(Check("failure-is-loud", bool(str(exc).strip()), record.resolve_error))
            record.checks.append(
                Check("resolution", spec.expect.resolution == "loud-failure", f"expected {spec.expect.resolution}")
            )
            record.checks.extend(self._memory_checks(spec, outcome.incident_id))
            self._append(record)
            raise
        if learned.error and spec.expect.resolution == "loud-failure":
            record.checks.append(Check("failure-is-loud", True, learned.error))
            record.checks.extend(self._memory_checks(spec, outcome.incident_id))
            self._append(record)
            return learned
        self._check(record, spec, learned)
        self._append(record)
        return learned

    def close(self) -> None:
        try:
            self._settle()
        finally:
            self._inner.close()

    # --- evidence -----------------------------------------------------------------------------------------

    def _append(self, record: AssuranceRecord) -> None:
        path = self._results / ASSURANCE_FILENAME
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(record.to_json() + "\n")
        status = "PASS" if record.passed else "FAIL"
        logger.info("assurance %s incident %d %s: %s", status, record.index, record.scenario, record.incident_id)
        for check in record.checks:
            if not check.ok:
                logger.error("  failed check %s: %s", check.name, check.detail)

    def _turns_for(self, incident_id: str) -> list[dict[str, Any]]:
        turns = [turn for turn in scripted_turns(self._control) if turn.get("incident_id") == incident_id]
        return turns

    def _settle(self) -> None:
        """Record what became of an incident whose resolve failed, once the harness recovered its fault."""

        record, self._unsettled = self._unsettled, None
        if record is None:
            return
        spec = self._specs[record.index]
        state = self._wait_idle()
        incident_id = record.incident_id or self._abandoned_incident_id(state)
        record.incident_id = incident_id
        record.metrics["controller_state_after_settle"] = {
            key: state.get(key)
            for key in ("incident_open", "closure_state", "last_acknowledged_incident_id", "detector_review_required")
        }
        settled = not state.get("incident_open") and not state.get("pending_closure")
        record.checks.append(
            Check(
                "controller-settled",
                settled,
                "idle" if settled else f"still open after {SETTLE_TIMEOUT_SECONDS}s: {_open_summary(state)}",
            )
        )
        detector_ids = self._operational_detector_ids()
        outcome_text = self._operational_outcomes() if incident_id else None
        record.checks.append(
            Check("operational-repository-readable", detector_ids is not None and outcome_text is not None, "via pod")
        )
        record.checks.extend(self._memory_checks(spec, incident_id, detector_ids))
        if incident_id:
            turns = self._turns_for(incident_id)
            record.turns = turns
            outcome = _outcome_from_cli(outcome_text, incident_id)
            classes = [outcome.classification] if outcome is not None else []
            record.metrics["outcome_classifications"] = classes
            if spec.expect.classification is not None:
                record.checks.append(
                    Check("classification", classes == [spec.expect.classification], f"classifications {classes}")
                )
            # An abandoned incident must still reach memory exactly once; a lost one is silent.
            record.checks.append(Check("outcome-recorded", outcome is not None, f"{len(classes)} outcome record(s)"))
            if spec.expect.resolution == "not-mitigated":
                record.checks.append(
                    Check("not-recorded-as-mitigated", "success" not in classes, f"classifications {classes}")
                )
                reflections = [turn for turn in turns if str(turn.get("kind", "")).startswith("reflection")]
                learned = [turn for turn in reflections if turn.get("outcome") == "updated"]
                record.checks.append(
                    Check("no-reflection-learning", not learned, f"{len(learned)} reflection turn(s) proposed memory")
                )
        else:
            record.checks.append(Check("abandoned-incident-identified", False, "no incident id for the failed resolve"))
        record.checks.append(
            Check(
                "failure-is-loud",
                bool(record.resolve_error),
                record.resolve_error or "resolve failed without an error message",
            )
        )
        if spec.expect.resolution == "mitigated":
            record.checks.append(
                Check("resolution", False, f"expected mitigated; resolve failed: {record.resolve_error}")
            )
        self._append(record)

    def _wait_idle(self) -> dict[str, Any]:
        from benchmarks.sregym.adapter import KubectlClusterOps

        ops = KubectlClusterOps()
        deadline = time.monotonic() + SETTLE_TIMEOUT_SECONDS
        state: dict[str, Any] = {}
        while time.monotonic() < deadline:
            state = ops.runtime_state(self._control)
            closure = state.get("closure_state")
            if (
                not state.get("incident_open")
                and not state.get("pending_closure")
                and closure in (None, "", "idle", "acknowledged")
            ):
                return state
            if closure == "failed":
                return state
            time.sleep(5)
        return state

    @staticmethod
    def _abandoned_incident_id(state: dict[str, Any]) -> str | None:
        request = state.get("incident_request") or {}
        closure = (state.get("pending_closure") or {}).get("request") or {}
        for candidate in (
            request.get("incident_id"),
            closure.get("incident_id"),
            state.get("last_acknowledged_incident_id"),
        ):
            if isinstance(candidate, str) and candidate:
                return candidate
        return None

    def _controller_exec(self, command: list[str]) -> str | None:
        """Run a command in the controller pod, or ``None`` when it is absent or fails.

        The host workspace is synced only when a receipt is collected, so an
        abandoned incident's outcome and memory exist only in the pod until then.
        """

        pods = kubectl(
            ["get", "pods", "-l", "job-name=sdo-controller-run", "--field-selector=status.phase=Running", "-o", "name"],
            namespace=self._control,
            check=False,
        ).stdout.split()
        if not pods:
            return None
        completed = kubectl(["exec", pods[0], "-c", "controller", "--", *command], namespace=self._control, check=False)
        return completed.stdout if completed.returncode == 0 else None

    def _operational_outcomes(self) -> str | None:
        """Every stored outcome as the controller pod's ``sdo-operator memory outcomes`` prints it."""

        return self._controller_exec(["sdo-operator", *outcomes_arguments(_POD_REPOSITORY)])

    def _operational_detector_ids(self) -> set[str] | None:
        """Registered detector IDs as the controller pod's ``sdo-operator memory ls`` prints them."""

        text = self._controller_exec(["sdo-operator", *listing_arguments(_POD_REPOSITORY)])
        if text is None:
            return None
        try:
            ids = set(artifact_ids(parse_listing(text), "detector"))
        except ParseError:
            return None
        return ids or None

    def _memory_checks(
        self, spec: IncidentSpec, incident_id: str | None, detectors: set[str] | None = None
    ) -> list[Check]:
        del incident_id
        detectors = _manifest_detectors(self._workspace) if detectors is None else detectors
        checks = [
            Check(f"learned:{detector}", detector in detectors, "registered" if detector in detectors else "missing")
            for detector in spec.expect.learned
        ]
        checks.extend(
            Check(
                f"not-learned:{detector}",
                detector not in detectors,
                "absent" if detector not in detectors else "registered",
            )
            for detector in spec.expect.not_learned
        )
        return checks

    def _check(self, record: AssuranceRecord, spec: IncidentSpec, outcome: AgentOutcome) -> None:
        expect = spec.expect
        incident_id = str(outcome.incident_id)
        artifacts = Path(str(outcome.artifacts_dir))
        receipt_path = artifacts / STRICT_RECEIPT_FILENAME
        receipt: dict[str, Any] = json.loads(receipt_path.read_text(encoding="utf-8")) if receipt_path.is_file() else {}
        checks = record.checks
        rejected_path = artifacts / REJECTED_RECEIPT_FILENAME
        # A closure the responder did not mitigate (for example a cancelled no-op) is a valid
        # receipt whose resolution is not ``sdo_mitigated``; one that failed strict validation
        # for another reason (for example a late detector review) leaves its evidence in the rejected receipt.
        evidence = receipt
        if not receipt and expect.resolution != "mitigated" and rejected_path.is_file():
            rejected = json.loads(rejected_path.read_text(encoding="utf-8"))
            evidence = rejected.get("receipt") or {}
            checks.append(Check("strict-receipt-rejected", True, str(rejected.get("validation_error"))[:300]))
        else:
            checks.append(Check("strict-receipt", bool(receipt), outcome.error or str(receipt_path)))
        orphans = [
            str(path)
            for path in self._run_dir.rglob(STRICT_RECEIPT_FILENAME)
            if path.resolve() != receipt_path.resolve()
            and json.loads(path.read_text(encoding="utf-8")).get("incident_id") == incident_id
        ]
        checks.append(Check("no-orphaned-receipt", not orphans, ", ".join(orphans)))
        resolution = receipt_resolution(receipt) if receipt else None
        record.metrics["receipt_resolution"] = resolution
        mitigated = bool(receipt.get("completed")) and resolution == "sdo_mitigated" and outcome.resolved_at is not None
        if expect.resolution == "mitigated":
            checks.append(Check("resolution", mitigated, f"completed={receipt.get('completed')} {resolution}"))
        else:
            checks.append(Check("resolution", not mitigated, f"expected {expect.resolution}, got a verified closure"))

        outcome_document = _evidenced_outcome(evidence, self._workspace, incident_id)
        checks.append(Check("one-outcome-record", outcome_document is not None, "native incident outcome"))
        classification = outcome_document.classification if outcome_document else None
        record.metrics["classification"] = classification
        if expect.resolution == "mitigated":
            checks.append(Check("classified-success", classification == "success", str(classification)))
        if expect.classification is not None:
            checks.append(Check("classification", classification == expect.classification, str(classification)))

        memory = receipt.get("memory_reuse") or {}
        record.metrics["warm_path"] = memory.get("warm_path")
        turns = self._turns_for(incident_id)
        record.turns = turns
        responder_turns = [turn for turn in turns if str(turn.get("kind", "")).startswith("responder")]
        if expect.warm is not None:
            warm_turns = [turn for turn in responder_turns if turn.get("kind") == "responder-warm"]
            checks.append(Check("warm-path", bool(memory.get("warm_path")) == expect.warm, f"memory_reuse={memory}"))
            checks.append(
                Check(
                    "warm-prompt",
                    bool(warm_turns) == expect.warm,
                    f"responder turn kinds {[t['kind'] for t in responder_turns]}",
                )
            )
            if expect.warm:
                applied = _applied_playbook_ids(_evidenced_ledger(evidence, incident_id) or {})
                checks.append(Check("playbook-applied", bool(applied), f"applied {applied}"))
                commands = [c["command"] for turn in warm_turns for c in turn.get("commands", [])]
                checks.append(
                    Check(
                        "playbook-scripts-ran",
                        any("/scripts/repair.sh" in c for c in commands),
                        "; ".join(commands)[:400],
                    )
                )
        if expect.reflection_skipped is not None:
            skipped = receipt.get("reflection_skipped_reason")
            checks.append(Check("reflection-skipped", bool(skipped) == expect.reflection_skipped, str(skipped)))
        if expect.min_reflection_attempts is not None:
            attempts = _reflection_attempts(turns)
            checks.append(Check("reflection-attempts", attempts >= expect.min_reflection_attempts, str(attempts)))
        checks.extend(self._memory_checks(spec, incident_id))
        checks.extend(self._token_checks(evidence, outcome, turns, artifacts))
        checks.extend(self._leak_checks(spec, incident_id, evidence))
        record.metrics.update(
            {
                "responder_turns": len(responder_turns),
                "responder_requests": sum(int(t.get("requests") or 0) for t in responder_turns),
                "reflection_seconds": outcome.reflection_seconds,
                "previous_reflection_drain_seconds": outcome.previous_reflection_drain_seconds,
                "detected_to_verified_seconds": (
                    (outcome.resolved_at - outcome.detected_at).total_seconds()
                    if outcome.resolved_at and outcome.detected_at
                    else None
                ),
            }
        )
        record.metrics.update(self._resource_sample())

    def _resource_sample(self) -> dict[str, Any]:
        """Controller process memory and threads, and the size of operational memory, after this incident."""

        sample: dict[str, Any] = {}
        pods = kubectl(
            ["get", "pods", "-l", "job-name=sdo-controller-run", "-o", "jsonpath={.items[*].metadata.name}"],
            namespace=self._control,
            check=False,
        ).stdout.split()
        if pods:
            script = (
                "for p in /proc/[0-9]*; do "
                'c=$(tr "\\0" " " < $p/cmdline 2>/dev/null | cut -c1-80); [ -n "$c" ] || continue; '
                "r=$(awk '/^VmRSS/{print $2}' $p/status); t=$(awk '/^Threads/{print $2}' $p/status); "
                'echo "$r|$t|$c"; done'
            )
            completed = kubectl(
                ["exec", pods[0], "-c", "controller", "--", "sh", "-c", script], namespace=self._control, check=False
            )
            processes = []
            for line in completed.stdout.splitlines():
                rss, threads, command = (line.split("|", 2) + ["", ""])[:3]
                if rss.isdigit():
                    processes.append({"rss_kb": int(rss), "threads": int(threads or 0), "command": command.strip()})
            sample["controller_pod"] = pods[0]
            sample["controller_processes"] = processes
        sdo = self._workspace / ".sdo"
        sample["sdo_bytes"] = sum(path.stat().st_size for path in sdo.rglob("*") if path.is_file())
        sample["sdo_files"] = sum(1 for path in sdo.rglob("*") if path.is_file())
        sample["operational_commits"] = int(_git(self._workspace, "rev-list", "--count", "HEAD") or 0)
        objects = dict(
            line.split(": ", 1) for line in _git(self._workspace, "count-objects", "-v").splitlines() if ": " in line
        )
        sample["git_size_kb"] = int(objects.get("size", 0)) + int(objects.get("size-pack", 0))
        ids = [outcome.incident_id for outcome in read_outcomes(self._workspace)]
        sample["outcomes"] = len(ids)
        sample["duplicate_outcomes"] = sorted({i for i in ids if ids.count(i) > 1})
        return sample

    def _token_checks(
        self, receipt: dict[str, Any], outcome: AgentOutcome, turns: list[dict[str, Any]], artifacts: Path
    ) -> list[Check]:
        session = receipt.get("responder_session_id")
        responder = [
            t for t in turns if str(t.get("kind", "")).startswith("responder") and t.get("session_id") == session
        ]
        reflection = [
            t for t in turns if str(t.get("kind", "")).startswith("reflection") and t.get("outcome") != "crashed"
        ]
        checks = [
            Check(
                "tokens:receipt-responder",
                _usage_key(receipt.get("usage") or {}) == _sum_usage(responder),
                f"receipt {_usage_key(receipt.get('usage') or {})} vs scripted {_sum_usage(responder)}",
            ),
            Check(
                "tokens:receipt-reflection",
                _usage_key(receipt.get("reflection_usage") or {}) == _sum_usage(reflection),
                f"receipt {_usage_key(receipt.get('reflection_usage') or {})} vs scripted {_sum_usage(reflection)}",
            ),
            Check(
                "tokens:run-record",
                (outcome.responder_tokens.input_tokens, outcome.responder_tokens.output_tokens)
                == (_sum_usage(responder)[0], _sum_usage(responder)[2]),
                f"record {outcome.responder_tokens} vs scripted {_sum_usage(responder)}",
            ),
        ]
        logged: dict[str, list[dict[str, Any]]] = {}
        for path in (artifacts / "sdo_runtime").rglob("*-turns.jsonl"):
            for item in _jsonl(path):
                logged.setdefault(str(item.get("session_id")), []).append(item)
        for turn in [*responder, *reflection]:
            candidates = [
                item
                for item in logged.get(str(turn.get("session_id")), [])
                if _usage_key(item.get("usage") or {}) == _usage_key(turn.get("usage") or {})
            ]
            checks.append(
                Check(
                    f"tokens:usage-log:{turn.get('kind')}",
                    len(candidates) == 1,
                    f"{len(candidates)} matching usage-log records for session {turn.get('session_id')}",
                )
            )
        return checks

    def _leak_checks(self, spec: IncidentSpec, incident_id: str, evidence: dict[str, Any]) -> list[Check]:
        ledger = _evidenced_ledger(evidence, incident_id)
        if ledger is None:
            return [Check("broker-ledger", False, f"no broker ledger for {incident_id}")]
        request = ((ledger.get("closure") or {}).get("request")) or {}
        allowed = _FAULT_OBJECTS[spec.directive.fault]
        changes = _state_change_names(request)
        foreign = [name for name in changes if not name.startswith(allowed)]
        own = _FAULT_DETECTORS[spec.directive.fault]
        other_detectors = sorted(
            {
                str(finding.get("detector_id"))
                for finding in request.get("findings") or []
                if str(finding.get("detector_id")) in set(_FAULT_DETECTORS.values()) - {own}
            }
        )
        return [
            Check("no-state-diff-leak", not foreign, f"state changes {changes}"),
            Check("no-foreign-incident-finding", not other_detectors, f"foreign incident detectors {other_detectors}"),
        ]


def prepare_offline_environment(scratch: Path) -> dict[str, str]:
    """Strip model credentials from this process; return the original values the SREGym worker still needs.

    ``install_controller`` copies host agent credentials into the cluster when
    its Secret is missing. With ``HOME`` and ``CODEX_HOME`` pointed at an empty
    directory and a placeholder API key, the only credential that can reach the
    cluster is that placeholder, which no scripted image can use.
    """

    original = {key: os.environ[key] for key in ("HOME", "PATH") if key in os.environ}
    home = scratch / "offline-home"
    (home / ".codex").mkdir(parents=True, exist_ok=True)
    os.environ["HOME"] = str(home)
    os.environ["CODEX_HOME"] = str(home / ".codex")
    os.environ["OPENAI_API_KEY"] = "scripted-no-model"
    os.environ.pop("ANTHROPIC_API_KEY", None)
    return original


def _open_summary(state: dict[str, Any]) -> str:
    request = state.get("incident_request") or {}
    result = state.get("incident_result") or {}
    return (
        f"incident {request.get('incident_id')!r}, responder {result.get('status')!r}, "
        f"review {state.get('detector_review_reason')!r}, closure {state.get('closure_state')!r}"
    )
