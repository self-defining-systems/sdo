"""Classify every SREGym problem run as ``valid``, ``invalid_infra`` or ``agent_failure``.

Runs used to be renamed ``invalid_`` or ``sdobug_`` by hand. This checker
reads each run's own evidence and decides:

- ``invalid_infra``: the harness or host broke the measurement, so the run
  says nothing about the agent and must be excluded (with the reason):
  - no run manifest, or a manifest whose launch preflight was waived;
  - no result row (the run never finished);
  - the agent CLI failed to install, or Codex ran out of quota;
  - token totals that do not reconcile across the rollout, the usage records
    and the result files, or no token evidence at all;
  - a passed stage without judge-free TTD/TTM (time to diagnosis or
    mitigation), or a TTM earlier than the TTD, which means judge time was
    subtracted wrongly;
  - the lane isolation guard reported a mismatch or never ran, agent tool
    output showing another lane's kind nodes, or (legacy runs without the
    guard) a run that overlapped another run in time;
- ``agent_failure``: the measurement is sound and the agent did not succeed:
  an oracle failed, ``agent_error``, SDO produced no valid strict receipt, or
  responder helper objects (``sdo.dev/responder-helper=true``) were left
  behind where the evidence records them;
- ``valid``: everything above passed.

Timing uses :mod:`benchmarks.sregym.analysis.incident_cost`'s own
definitions, so the checker validates exactly the TTD and TTM that are
reported. Judge time never counts toward either.

``--legacy`` accepts runs that predate the manifest and the in-run isolation
guard (before 2026-09-28): a missing manifest becomes a note, and a missing
guard is accepted only when the run did not overlap another run.

Usage::

    uv run python -m benchmarks.sregym.analysis.run_validity third_party/sregym/logs/<run>... \\
        [--legacy] [--json out.json]
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Literal, cast

import yaml
from google.protobuf import json_format

from benchmarks.sregym.analysis.incident_cost import (
    RECEIPT_NAME,
    RESOLUTION_NAME,
    IncidentCostError,
    TokenUsage,
    Verdict,
    pipeline_stage_dirs,
    problem_results_dirs,
    read_result_row,
    read_verdict,
    receipt_responder_session_id,
    receipt_responder_usage,
    responder_rollouts,
    rollout_turn_usages,
    with_mitigation_applied,
)
from benchmarks.sregym.protocol import ProductionReceiptValidationError, validate_production_receipt
from benchmarks.sregym.protocol.native_receipt import NativeReceiptValidationError
from sdo.contracts import ReceiptFailure, RejectedProductionReceipt

Classification = Literal["valid", "invalid_infra", "agent_failure"]
CheckStatus = Literal["pass", "fail", "not_recorded"]
Blame = Literal["infra", "agent"]

MANIFEST_NAME = "run_manifest.json"
REJECTED_RECEIPT_NAME = "sdo_rejected_production_receipt.json"
HELPER_LABEL = "sdo.dev/responder-helper=true"
_MANUAL_LABEL = re.compile(r"^([a-z][a-z_]*?)_\d{8}_\d{6}")
_GUARD_OK = re.compile(r"verified: port (\d+) reaches only (\S+)")
_GUARD_BAD = re.compile(r"AgentKubeconfigMismatch|does not reach cluster")
#: kind node names of SREGym lanes, whose cluster names end in ``-w<N>``.
#: A lane's cluster is ``<prefix>-w<N>`` and its nodes ``<cluster>-control-plane`` or
#: ``<cluster>-worker<M>``; static pods (``etcd-<node>``) embed a node name after a hyphen.
# A leading "…" marks tool-output truncation, which can cut a node name mid-word.
_NODE = re.compile(r"(?<![A-Za-z0-9…])([a-z][a-z0-9]*(?:-[a-z0-9]+)*?-w\d+)-(?:control-plane|worker\d*)(?![A-Za-z0-9])")
_STATIC_POD_PREFIXES = ("etcd-", "kube-apiserver-", "kube-controller-manager-", "kube-scheduler-")
_QUOTA = re.compile(r"usage limit|usage_limit|rate limit|rate_limit|quota", re.IGNORECASE)
_HELPER_CLEANUP_FAILED = re.compile(r"clean up responder helpers:.*")
_TOKEN_FIELDS = ("input_tokens", "cached_input_tokens", "output_tokens")


@dataclass(frozen=True)
class ValidityPolicy:
    #: Accept runs from before the manifest and the in-run isolation guard existed.
    legacy: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.legacy, bool):  # pyright: ignore[reportUnnecessaryIsInstance]
            raise TypeError(f"ValidityPolicy.legacy must be a boolean, got {self.legacy!r}")


@dataclass(frozen=True)
class Check:
    name: str
    status: CheckStatus
    detail: str = ""
    blame: Blame = "infra"


@dataclass(frozen=True)
class RunValidity:
    experiment_dir: str
    results_dir: str
    problem_id: str
    agent: str | None
    classification: Classification
    reasons: tuple[str, ...]
    checks: tuple[Check, ...]
    #: The prefix a person gave the directory by hand (``invalid``, ``sdobug``, ...), if any.
    manual_label: str | None = None

    @property
    def excluded(self) -> bool:
        return self.classification == "invalid_infra"


@dataclass(frozen=True)
class _Window:
    directory: Path
    start: float
    end: float


@dataclass
class _Windows:
    """Active time windows of every experiment below one logs root, computed once."""

    cache: dict[Path, list[_Window]] = field(default_factory=dict)  # pyright: ignore[reportUnknownVariableType]

    def overlapping(self, experiment_dir: Path) -> list[Path]:
        root = _logs_root(experiment_dir)
        if root not in self.cache:
            self.cache[root] = [window for directory in _experiment_dirs(root) if (window := _window(directory))]
        own = _window(experiment_dir)
        if own is None:
            return []
        own_run = _run_root(experiment_dir)
        return [
            window.directory
            for window in self.cache[root]
            if _run_root(window.directory) != own_run and window.start < own.end and own.start < window.end
        ]


# --------------------------------------------------------------------------- layout


def _is_pipeline(directory: Path) -> bool:
    return (directory / "pipeline_state.json").is_file() or (directory / "pipeline_config.toml").is_file()


def _run_root(experiment_dir: Path) -> Path:
    """The directory a person launched: the pipeline for a stage, else the experiment itself."""

    return experiment_dir.parent if _is_pipeline(experiment_dir.parent) else experiment_dir


def _logs_root(experiment_dir: Path) -> Path:
    return _run_root(experiment_dir).parent


def _experiment_dirs(root: Path) -> list[Path]:
    directories: list[Path] = []
    for child in sorted(root.iterdir()) if root.is_dir() else []:
        if not child.is_dir():
            continue
        if _is_pipeline(child):
            try:
                directories.extend(path for _, _, path in pipeline_stage_dirs(child))
            except IncidentCostError:
                continue
        elif (child / "runs").is_dir():
            directories.append(child)
    return directories


def _window(experiment_dir: Path) -> _Window | None:
    snapshot = experiment_dir / "experiment_config.toml"
    if not snapshot.is_file():
        return None
    start = snapshot.stat().st_mtime
    ends = [path.stat().st_mtime for path in (experiment_dir / "runs").glob("*/worker_*/worker.log")]
    ends.extend(path.stat().st_mtime for path in (experiment_dir / "runs").glob("*/worker_*/results/*.csv"))
    return _Window(experiment_dir, start, max([start, *ends]))


def run_directories(root: Path) -> list[Path]:
    """The experiment and pipeline directories directly below a logs root."""

    return [
        child
        for child in sorted(root.iterdir())
        if child.is_dir() and (_is_pipeline(child) or (child / "experiment_config.toml").is_file())
    ]


def manual_label(directory: Path) -> str | None:
    match = _MANUAL_LABEL.match(_run_root(directory).name)
    return match.group(1) if match else None


def _agent(experiment_dir: Path) -> str | None:
    import tomllib

    path = experiment_dir / "experiment_config.toml"
    if not path.is_file():
        return None
    runner = tomllib.loads(path.read_text(encoding="utf-8")).get("runner") or {}
    agent = runner.get("agent")
    return str(agent) if agent else None


def _read_json(path: Path) -> dict[str, Any] | None:
    try:
        document: object = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return cast("dict[str, Any]", document) if isinstance(document, dict) else None


def _text(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="replace") if path.is_file() else ""


# --------------------------------------------------------------------------- checks


def _check_manifest(experiment_dir: Path, policy: ValidityPolicy) -> tuple[Check, dict[str, Any] | None]:
    for directory in (experiment_dir, experiment_dir.parent):
        manifest = _read_json(directory / MANIFEST_NAME)
        if manifest is None:
            continue
        preflight = cast("dict[str, Any]", manifest.get("preflight") or {})
        if preflight.get("waived") or preflight.get("ok") is False:
            failed = [
                str(check.get("name"))
                for check in cast("list[dict[str, Any]]", preflight.get("checks") or [])
                if check.get("status") == "fail"
            ]
            return Check("manifest", "fail", f"launch preflight waived (failed: {', '.join(failed) or '?'})"), manifest
        return Check("manifest", "pass", str(directory / MANIFEST_NAME)), manifest
    if policy.legacy:
        return Check("manifest", "not_recorded", "no run manifest (legacy run)"), None
    return Check("manifest", "fail", f"no {MANIFEST_NAME} in {experiment_dir} or its pipeline"), None


def _check_result(results: Path) -> tuple[Check, dict[str, str | None] | None]:
    row = read_result_row(results)
    if row is None:
        return Check("result", "fail", "no result row: the run did not finish"), None
    return Check("result", "pass"), row


def _check_install(results: Path) -> Check:
    rcs = sorted(results.rglob("install.rc"))
    if not rcs:
        return Check("agent-install", "not_recorded")
    for rc in rcs:
        code = _text(rc).strip()
        if code != "0":
            tail = " | ".join(_text(rc.parent / "install.log").strip().splitlines()[-2:])
            return Check("agent-install", "fail", f"agent CLI install exited {code or '?'}: {tail}")
    return Check("agent-install", "pass")


def _codex_events(results: Path) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for path in sorted(results.rglob("codex.txt")):
        for line in _text(path).splitlines():
            if not line.startswith("{"):
                continue
            try:
                event: object = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(event, dict):
                events.append(cast("dict[str, Any]", event))
    return events


def _check_quota(results: Path) -> Check:
    for event in _codex_events(results):
        if event.get("type") not in ("error", "turn.failed"):
            continue
        message = str(event.get("message") or (event.get("error") or {}).get("message") or "")
        if _QUOTA.search(message):
            return Check("codex-quota", "fail", f"Codex quota exhausted mid-run: {message[:160]}")
    return Check("codex-quota", "pass")


def _triple(usage: TokenUsage) -> tuple[int, int, int]:
    return usage.input_tokens, usage.cached_input_tokens, usage.output_tokens


def _reconcile(name: str, sources: dict[str, tuple[int, int, int]]) -> Check:
    if not sources:
        return Check(name, "fail", "no token evidence (no rollout, usage record or result usage)")
    distinct = set(sources.values())
    if len(distinct) > 1:
        described = "; ".join(f"{source} in/cached/out={values}" for source, values in sources.items())
        return Check(name, "fail", f"token accounting mismatch: {described}")
    return Check(name, "pass", f"{len(sources)} source(s) agree")


def _rollout_final_totals(rollouts: list[Path]) -> tuple[int, int, int] | None:
    totals = [0, 0, 0]
    seen = False
    for path in rollouts:
        last: dict[str, Any] | None = None
        for line in _text(path).splitlines():
            if '"total_token_usage"' not in line:
                continue
            info = (json.loads(line).get("payload") or {}).get("info")
            if isinstance(info, dict) and isinstance(info.get("total_token_usage"), dict):
                last = cast("dict[str, Any]", info["total_token_usage"])
        if last is not None:
            seen = True
            for index, key in enumerate(_TOKEN_FIELDS):
                totals[index] += int(last.get(key) or 0)
    return (totals[0], totals[1], totals[2]) if seen else None


def _check_codex_tokens(results: Path) -> Check:
    rollouts = sorted(results.rglob("sessions/**/rollout-*.jsonl"))
    sources: dict[str, tuple[int, int, int]] = {}
    turns = rollout_turn_usages(rollouts)
    if turns and any(turn.input_tokens for turn in turns):
        sources["rollout turns"] = _triple(sum(turns, TokenUsage()))
        final = _rollout_final_totals(rollouts)
        if final is not None:
            sources["rollout total_token_usage"] = final
    for path in sorted(results.rglob("codex_results_*.json")):
        metrics = (_read_json(path) or {}).get("usage_metrics")
        if isinstance(metrics, dict):
            sources["usage_metrics"] = _triple(TokenUsage.from_mapping(metrics))
    return _reconcile("tokens", sources)


def _check_sdo_tokens(results: Path, receipt: dict[str, Any] | None) -> Check:
    if receipt is None:
        return Check("tokens", "not_recorded", "no strict receipt to reconcile")
    session = receipt_responder_session_id(receipt)
    sources: dict[str, tuple[int, int, int]] = {}
    recorded = receipt_responder_usage(receipt)
    if recorded.input_tokens:
        sources["receipt usage"] = _triple(recorded)
    if isinstance(session, str) and session:
        rollouts = responder_rollouts(results, receipt)
        turns = rollout_turn_usages(rollouts)
        if turns and turns[0].input_tokens:
            sources["responder rollout"] = _triple(turns[0])
        for path in sorted(results.rglob("sdo_runtime/usage/responder-turns.jsonl")):
            for line in _text(path).splitlines():
                record = json.loads(line) if line.strip() else {}
                if record.get("session_id") == session and isinstance(record.get("usage"), dict):
                    sources["responder-turns.jsonl"] = _triple(TokenUsage.from_mapping(record["usage"]))
    return _reconcile("tokens", sources)


def _check_timing(verdict: Verdict, row: dict[str, str | None]) -> list[Check]:
    checks: list[Check] = []
    missing = [key for key in ("fault_injected_at", "diagnosis_submitted_at") if not (row.get(key) or "").strip()]
    if verdict.diagnosis is True and verdict.diagnosis_seconds is None:
        checks.append(Check("ttd", "fail", f"diagnosis passed but judge-free TTD is missing ({', '.join(missing)})"))
    elif verdict.diagnosis_seconds is not None and verdict.diagnosis_seconds < 0:
        checks.append(Check("ttd", "fail", f"TTD is negative ({verdict.diagnosis_seconds:.1f} s)"))
    else:
        checks.append(Check("ttd", "pass" if verdict.diagnosis_seconds is not None else "not_recorded"))
    ttm = verdict.ttm_seconds
    if verdict.mitigation is True and ttm is None:
        absent = [
            key
            for key in ("fault_injected_at", "diagnosis_submitted_at", "mitigation_submitted_at", "TTL")
            if not (row.get(key) or "").strip()
        ]
        checks.append(Check("ttm", "fail", f"mitigation passed but judge-free TTM is missing ({', '.join(absent)})"))
    elif ttm is not None and verdict.diagnosis_seconds is not None and ttm + 1e-6 < verdict.diagnosis_seconds:
        checks.append(
            Check(
                "ttm",
                "fail",
                f"judge-free TTM {ttm:.1f} s precedes TTD {verdict.diagnosis_seconds:.1f} s: "
                "the judge grading window is inconsistent with the submissions",
            )
        )
    else:
        checks.append(Check("ttm", "pass" if ttm is not None else "not_recorded"))
    return checks


def _worker_logs(results: Path) -> str:
    return re.sub(r"\s+", " ", _text(results.parent / "worker.log"))


def _lane_cluster(experiment_dir: Path, worker_dir: Path, manifest: dict[str, Any] | None) -> str | None:
    clusters = ((manifest or {}).get("kind_topology") or {}).get("clusters") or []
    if len(clusters) == 1:
        return str(clusters[0])
    kubeconfig = experiment_dir / "kubeconfigs" / f"{worker_dir.name}.kubeconfig"
    if kubeconfig.is_file():
        document = yaml.safe_load(kubeconfig.read_text(encoding="utf-8")) or {}
        current = str(document.get("current-context") or "")
        if current.startswith("kind-"):
            return current.removeprefix("kind-")
    return None


def _check_guard(
    experiment_dir: Path,
    results: Path,
    lane: str | None,
    policy: ValidityPolicy,
    windows: _Windows,
) -> tuple[Check, str | None]:
    log = _worker_logs(results)
    if _GUARD_BAD.search(log):
        return Check("isolation-guard", "fail", "the lane isolation guard reported a kubeconfig mismatch"), lane
    verified = {cluster for _, cluster in _GUARD_OK.findall(log)}
    if verified:
        if lane is not None and verified != {lane}:
            return Check(
                "isolation-guard", "fail", f"isolation guard verified {sorted(verified)}, lane is {lane}"
            ), lane
        return Check("isolation-guard", "pass", f"verified {sorted(verified)}"), lane or next(iter(verified))
    if not policy.legacy:
        return Check("isolation-guard", "fail", "the lane isolation guard never ran (no verification logged)"), lane
    overlapping = windows.overlapping(experiment_dir)
    if overlapping:
        names = ", ".join(sorted({_run_root(path).name for path in overlapping}))
        return (
            Check(
                "isolation-guard",
                "fail",
                f"no isolation guard, and the run overlapped {names} (possible shared-kubeconfig contamination)",
            ),
            lane,
        )
    return Check("isolation-guard", "pass", "legacy run without the guard; it ran alone"), lane


def _tool_outputs(results: Path) -> str:
    """What the agent's commands printed: rollout tool outputs, else Codex's ``--json`` command output."""

    chunks: list[str] = []
    rollouts = sorted(results.rglob("sessions/**/rollout-*.jsonl"))
    for path in rollouts:
        for line in _text(path).splitlines():
            if "_call_output" not in line:
                continue
            payload = json.loads(line).get("payload") or {}
            if isinstance(payload, dict) and str(payload.get("type", "")).endswith("_call_output"):
                chunks.append(str(payload.get("output", "")))
    if not rollouts:
        for event in _codex_events(results):
            item = event.get("item") or {}
            if isinstance(item, dict) and item.get("type") == "command_execution":
                chunks.append(str(item.get("aggregated_output", "")))
    return "\n".join(chunks)


def _check_foreign_nodes(results: Path, lane: str | None) -> Check:
    # Tool outputs are often JSON-encoded strings; undo their escapes so "\\nluna-w0-worker" reads as a line.
    text = (_tool_outputs(results) + "\n" + _worker_logs(results)).replace("\\n", "\n").replace("\\t", "\t")
    clusters: Counter[str] = Counter()
    for match in _NODE.findall(text):
        cluster = match
        for prefix in _STATIC_POD_PREFIXES:
            cluster = cluster.removeprefix(prefix)
        clusters[cluster] += 1
    if not clusters:
        return Check("foreign-nodes", "not_recorded", "no kind node names in the agent's output")
    if lane is None:
        if len(clusters) > 1:
            return Check("foreign-nodes", "fail", f"agent output shows nodes of several clusters {dict(clusters)}")
        return Check("foreign-nodes", "pass", f"only {next(iter(clusters))}")
    foreign = {cluster: count for cluster, count in clusters.items() if cluster != lane}
    if foreign:
        return Check("foreign-nodes", "fail", f"agent output shows nodes of other lanes {foreign} (lane {lane})")
    return Check("foreign-nodes", "pass", f"only {lane}")


def _check_receipt(experiment_dir: Path, results: Path) -> tuple[Check, dict[str, Any] | None]:
    receipts = sorted(results.rglob(RECEIPT_NAME))
    if receipts:
        receipt = _read_json(receipts[-1])
        if receipt is None:
            return Check("receipt", "fail", "strict receipt is unreadable", blame="agent"), None
        try:
            validate_production_receipt(receipt)
        except ProductionReceiptValidationError as exc:
            blame: Blame = (
                "infra"
                if isinstance(exc, NativeReceiptValidationError)
                and exc.failure.code == ReceiptFailure.CODE_CHECKER_UNAVAILABLE
                else "agent"
            )
            return Check("receipt", "fail", f"strict receipt fails production validation: {exc}", blame=blame), receipt
        return Check("receipt", "pass"), receipt
    rejected = sorted(results.rglob(REJECTED_RECEIPT_NAME))
    if rejected:
        document = _read_json(rejected[-1]) or {}
        error = document.get("validation_error")
        try:
            typed = json_format.ParseDict(document, RejectedProductionReceipt(), ignore_unknown_fields=True)
            blame = "infra" if typed.failure.code == ReceiptFailure.CODE_CHECKER_UNAVAILABLE else "agent"
        except json_format.ParseError:
            blame = "agent"
        return Check("receipt", "fail", f"SDO receipt rejected: {error}", blame=blame), None
    if any(results.rglob(RESOLUTION_NAME)) and not _pipeline_finished(experiment_dir):
        # A persistent controller's receipt is published by the next stage's drain or by teardown.
        return Check(
            "receipt", "fail", "this stage's deferred strict receipt is pending while the pipeline is still live"
        ), None
    return Check("receipt", "fail", "SDO produced no strict receipt", blame="agent"), None


def _pipeline_finished(experiment_dir: Path) -> bool:
    """Whether the stage's pipeline ran every stage to an end (completed, or failed after running)."""

    state = _read_json(experiment_dir.parent / "pipeline_state.json")
    if state is None:
        return True
    stages = cast("list[dict[str, Any]]", state.get("stages") or [])
    return all(
        stage.get("status") in ("completed", "agent_failure")
        or (stage.get("status") == "failed" and stage.get("error") != "interrupted")
        for stage in stages
    )


def _check_helpers(results: Path, receipt: dict[str, Any] | None) -> Check:
    leftovers: list[str] = []
    for key in ("leftover_helpers", "remaining_helpers"):
        value = (receipt or {}).get(key)
        if isinstance(value, list) and value:
            leftovers.extend(str(item) for item in cast("list[object]", value))
    for path in sorted(results.rglob("sdo_runtime/controller_logs/*.log")):
        leftovers.extend(match.group(0)[:200] for match in _HELPER_CLEANUP_FAILED.finditer(_text(path)))
    if leftovers:
        return Check(
            "responder-helpers",
            "fail",
            f"{HELPER_LABEL} objects may remain: {'; '.join(sorted(set(leftovers)))[:400]}",
            blame="agent",
        )
    if receipt is not None and "cleaned_helpers" in receipt:
        return Check("responder-helpers", "pass", f"cleaned {len(receipt['cleaned_helpers'] or [])}")
    return Check("responder-helpers", "not_recorded")


def _acknowledged_submissions(results: Path) -> int:
    """Stock-agent ``POST /submit`` commands the conductor acknowledged (``Submission received``)."""

    count = 0
    for event in _codex_events(results):
        item = event.get("item") or {}
        if event.get("type") != "item.completed" or not isinstance(item, dict):
            continue
        if item.get("type") == "command_execution" and "/submit" in str(item.get("command", "")):
            count += str(item.get("aggregated_output", "")).count("Submission received")
    return count


def _check_outcome(verdict: Verdict, row: dict[str, str | None], results: Path) -> Check:
    if str(row.get("agent_error") or "").strip().lower() in {"1", "true", "yes"}:
        return Check("outcome", "fail", "agent_error=true", blame="agent")
    ungraded = [stage for stage in ("Diagnosis", "Mitigation") if not (row.get(f"{stage}.success") or "").strip()]
    if ungraded:
        acknowledged = _acknowledged_submissions(results)
        if acknowledged >= (2 if "Mitigation" in ungraded else 1):
            return Check(
                "outcome",
                "fail",
                f"no {' or '.join(ungraded)} verdict although the conductor acknowledged {acknowledged} "
                "submissions: the harness dropped a submission",
            )
        return Check("outcome", "fail", f"no {' or '.join(ungraded)} verdict: the agent never submitted", blame="agent")
    failed = [
        stage
        for stage, passed in (("Diagnosis", verdict.diagnosis), ("Mitigation", verdict.mitigation))
        if passed is not True
    ]
    if failed:
        return Check("outcome", "fail", f"{' and '.join(failed)} oracle did not pass", blame="agent")
    return Check("outcome", "pass")


# --------------------------------------------------------------------------- classification


def classify_problem_run(
    experiment_dir: Path,
    problem_id: str,
    results: Path,
    *,
    policy: ValidityPolicy,
    windows: _Windows | None = None,
) -> RunValidity:
    windows = windows or _Windows()
    agent = _agent(experiment_dir)
    checks: list[Check] = []
    manifest_check, manifest = _check_manifest(experiment_dir, policy)
    checks.append(manifest_check)
    result_check, row = _check_result(results)
    checks.append(result_check)
    receipt: dict[str, Any] | None = None
    if row is not None:
        rollouts = sorted(results.rglob("sessions/**/rollout-*.jsonl"))
        verdict = read_verdict(results) or Verdict(None, None, None)
        if agent == "sdo_codex":
            receipt_check, receipt = _check_receipt(experiment_dir, results)
            checks.append(receipt_check)
            rollouts = responder_rollouts(results, receipt or {})
            checks.append(_check_sdo_tokens(results, receipt))
            checks.append(_check_helpers(results, receipt))
        else:
            checks.append(_check_install(results))
            checks.append(_check_quota(results))
            checks.append(_check_codex_tokens(results))
        verdict = with_mitigation_applied(verdict, results, rollouts)
        checks.extend(_check_timing(verdict, row))
        lane = _lane_cluster(experiment_dir, results.parent, manifest)
        guard_check, lane = _check_guard(experiment_dir, results, lane, policy, windows)
        checks.append(guard_check)
        checks.append(_check_foreign_nodes(results, lane))
        checks.append(_check_outcome(verdict, row, results))
    infra = [check.detail for check in checks if check.status == "fail" and check.blame == "infra"]
    agent_failures = [check.detail for check in checks if check.status == "fail" and check.blame == "agent"]
    classification: Classification
    if infra:
        classification, reasons = "invalid_infra", infra
    elif agent_failures:
        classification, reasons = "agent_failure", agent_failures
    else:
        classification, reasons = "valid", []
    return RunValidity(
        experiment_dir=str(experiment_dir),
        results_dir=str(results),
        problem_id=problem_id,
        agent=agent,
        classification=classification,
        reasons=tuple(reasons),
        checks=tuple(checks),
        manual_label=manual_label(experiment_dir),
    )


def classify_path(
    path: Path, *, policy: ValidityPolicy | None = None, windows: _Windows | None = None
) -> list[RunValidity]:
    """Classify every problem run below an experiment or pipeline directory.

    A pipeline without any stage directory, or an experiment without any
    problem run, yields one ``invalid_infra`` entry for the directory itself.
    """

    policy = policy or ValidityPolicy()
    windows = windows or _Windows()
    if _is_pipeline(path):
        try:
            experiment_dirs = [directory for _, _, directory in pipeline_stage_dirs(path)]
        except IncidentCostError:
            experiment_dirs = []
    else:
        experiment_dirs = [path]
    results: list[RunValidity] = []
    for experiment_dir in experiment_dirs:
        problem_runs = problem_results_dirs(experiment_dir)
        if not problem_runs:
            results.append(_empty(experiment_dir, "no problem run was recorded"))
        for problem_id, results_dir in problem_runs:
            results.append(
                classify_problem_run(experiment_dir, problem_id, results_dir, policy=policy, windows=windows)
            )
    return results or [_empty(path, "no pipeline stage was recorded")]


def _empty(directory: Path, reason: str) -> RunValidity:
    return RunValidity(
        experiment_dir=str(directory),
        results_dir="",
        problem_id="",
        agent=_agent(directory),
        classification="invalid_infra",
        reasons=(reason,),
        checks=(Check("result", "fail", reason),),
        manual_label=manual_label(directory),
    )


def validity_by_results_dir(paths: list[Path], *, policy: ValidityPolicy) -> dict[str, RunValidity]:
    """Classifications keyed by results directory, for filtering a report's runs."""

    windows = _Windows()
    return {
        result.results_dir: result
        for path in paths
        for result in classify_path(path, policy=policy, windows=windows)
        if result.results_dir
    }


def render(results: list[RunValidity]) -> str:
    lines: list[str] = []
    for result in results:
        experiment = Path(result.experiment_dir)
        run = Path(result.results_dir).parent.parent.name if result.results_dir else "-"
        where = (
            f"{_run_root(experiment).name}/{experiment.name}"
            if _run_root(experiment) != experiment
            else experiment.name
        )
        label = f" [manual: {result.manual_label}]" if result.manual_label else ""
        lines.append(f"{result.classification:<14} {where} {run}{label}")
        lines.extend(f"    - {reason}" for reason in result.reasons)
    counts = Counter(result.classification for result in results)
    lines.append(
        f"{len(results)} runs: "
        + ", ".join(f"{counts.get(name, 0)} {name}" for name in ("valid", "agent_failure", "invalid_infra"))
    )
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    parser.add_argument("paths", type=Path, nargs="*", help="experiment or pipeline directories")
    parser.add_argument("--logs-root", type=Path, help="also classify every run directory directly below this root")
    parser.add_argument("--legacy", action="store_true", help="accept runs from before the manifest and the guard")
    parser.add_argument("--json", type=Path, help="also write every classification as JSON")
    args = parser.parse_args(argv)
    policy = ValidityPolicy(legacy=args.legacy)
    windows = _Windows()
    paths = [path.resolve() for path in args.paths]
    if args.logs_root is not None:
        paths.extend(run_directories(args.logs_root.resolve()))
    if not paths:
        parser.error("give run directories or --logs-root")
    results = [result for path in paths for result in classify_path(path, policy=policy, windows=windows)]
    print(render(results))
    if args.json:
        args.json.write_text(json.dumps([asdict(result) for result in results], indent=2) + "\n", encoding="utf-8")
    return 1 if any(result.excluded for result in results) else 0


if __name__ == "__main__":
    sys.exit(main())
