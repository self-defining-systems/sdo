"""Per-stage and cumulative incident cost of an SDO pipeline against stock Codex.

Given one SDO pipeline directory (``logs/<ts>_pipeline_<name>/``) and any number
of Codex run directories (plain experiment or pipeline directories), print for
every SDO stage:

- time to diagnosis, ``ttd_s = diagnosis_submitted_at - fault_injected_at``
  (no judge time falls inside it);
- the headline time to mitigation ``ttm_s``, which excludes judge time: the
  raw time ``mitigation_submitted_at - fault_injected_at`` minus the conductor's
  diagnosis grading wait (``fault_injected_at + TTL - diagnosis_submitted_at``;
  a mitigation POST cannot land before the mitigation stage opens; zero for
  ``diagnosis_grading_deferred`` runs, whose mitigation stage opens at the
  diagnosis POST), but never
  less than when the agent's last state-changing command before its mitigation
  submission completed (``last_mut_s``, from the exported Codex rollout), since
  subtracting the whole grading wait undercounts an agent that kept repairing
  while the judge graded its diagnosis; ``ttm_s`` is unknown without the TTL.
  State changes made through scripts count: a ``kubectl`` write inside a script
  the session wrote and ran, or in inline ``python3 -``/``-c``/``bash -c`` code.
  A script file whose content the rollout never shows is assumed possibly
  mutating, and the strict receipt's last repair-action time is a cross-check;
  ``floor_source`` names the bound that set ``ttm_s`` (``judge_excluded``,
  ``rollout_mutation``, ``receipt_apply`` or ``unverified_script``);
- supplementary times: the raw ``raw_incl_judge_s`` (includes diagnosis
  grading), the judge-excluded ``no_judge_s`` alone, when the first
  state-changing command was issued (``first_mut_s``), and the receipt's
  ``incident_resolution_seconds`` (none of them includes reflection);
- oracle verdicts (diagnosis and mitigation);
- responder ("incident") tokens, reflection tokens and reflection wall time in
  separate columns (asynchronous learning is not resolution time), with the
  token breakdown, weighted totals, USD and requests in their own table;
- the receipt's ``memory_reuse.warm_path`` and whether the responder prompt
  actually contained the warm-path instructions (from exported Codex rollouts);
- operational-memory size after the stage (detectors and playbooks in ``.sdo/``).

It then prints cumulative incident tokens (responder only) and cumulative tokens
including learning (responder plus reflection) against the cumulative Codex
tokens for the same problem sequence, the one-time lifecycle tokens, and the
break-even stage for each measure. Codex is memoryless, so its cost for a
problem is the mean over every Codex run of that problem.

Tokens use agentshim's normalized breakdown for both arms (SDO and the raw
Codex or Claude Code baseline): ``input_tokens`` includes cache reads and cache
writes, ``output_tokens`` includes reasoning. A token-breakdown table reports,
per SDO responder and reflection and per baseline problem (mean over runs),
uncached input, cache reads, cache writes (and the one-hour part), output,
reasoning, the raw total ``input + output``, a cost-weighted total, the USD
cost and the model-request count. Codex counts come from the exported session
rollouts where present (they carry reasoning and requests), a Claude baseline's
from its ``stream-json`` result frames. ``--uncached`` drops cache reads from
the raw totals.

The weights and prices come from agentshim's static pricing table (sourced and
dated per entry), keyed by the provider and model in each experiment's
``experiment_config.toml``; the output names the table version and date and
each arm's weights. ``--weight CLASS=MULTIPLE`` overrides a weight (multiples of
the base input rate), ``--pricing-table`` substitutes a JSON table, and
``--sdo-model``/``--codex-model PROVIDER:MODEL`` override the detected models.
A model the table does not price gets raw totals only unless every required
weight is given.

Usage::

    uv run python -m benchmarks.sregym.analysis.incident_cost \\
        third_party/sregym/logs/<ts>_pipeline_sdo-codex-luna-sequence \\
        --codex third_party/sregym/logs/<ts>_codex third_party/sregym/logs/<ts2>_codex \\
        [--lifecycle-usage path/to/sdo_turn_usage.jsonl] [--uncached] [--json out.json] \\
        [--weight output=8 ...] [--pricing-table table.json] [--sdo-model codex:gpt-6-luna]
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import re
import sys
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime
from pathlib import Path
from typing import Any

from agentshim import ModelPricing, PricingTable, TokenWeights, cost_usd, default_pricing, price_for
from agentshim import TokenUsage as NormalizedUsage
from agentshim.providers.claude import ClaudeStreamParser

from benchmarks.sregym.detector_manifests import workspace_detector_manifests


def incident_worktree_dirname(incident_id: str) -> str:
    """Read-only name mapping for archived pre-cutover benchmark trajectories."""

    digest = hashlib.sha256(incident_id.encode()).hexdigest()[:12]
    readable = "".join(character if character.isalnum() else "-" for character in incident_id.lower()).strip("-")
    return f"{readable[:32] or 'incident'}-{digest}"


try:
    import tomllib
except ModuleNotFoundError:  # Python 3.10
    import tomli as tomllib

WARM_PROMPT_MARKER = "Warm path: validated incident memory matches this incident."
RECEIPT_NAME = "sdo_production_receipt_strict.json"
RESOLUTION_NAME = "sdo_incident_resolution.json"
LIFECYCLE_USAGE_NAME = "sdo_turn_usage.jsonl"
_RENAMED_STAGE = re.compile(r"\.\d{8}_\d{6}$")
_STAGE_DIR = re.compile(r"^stage_(\d+)_(.+)$")


class IncidentCostError(ValueError):
    """Raised when a result directory lacks the evidence this analysis needs."""


@dataclass(frozen=True)
class TokenUsage:
    """Token usage in agentshim's normalized breakdown, plus the model-request count.

    ``input_tokens`` includes cache reads (``cached_input_tokens``) and cache
    writes; ``output_tokens`` includes reasoning. ``requests`` is the number
    of model requests, ``None`` when the evidence does not say.
    """

    input_tokens: int = 0
    cached_input_tokens: int = 0
    output_tokens: int = 0
    cache_write_input_tokens: int = 0
    cache_write_1h_input_tokens: int = 0
    reasoning_output_tokens: int = 0
    requests: int | None = None

    def __post_init__(self) -> None:
        for name in _COUNT_FIELDS:
            value = getattr(self, name)
            if not isinstance(value, int) or isinstance(value, bool):
                raise TypeError(f"{name} must be an int, got {value!r}")
            if value < 0:
                raise ValueError(f"{name} must not be negative")
        if self.requests is not None and (
            not isinstance(self.requests, int) or isinstance(self.requests, bool) or self.requests < 0
        ):
            raise ValueError(f"requests must be a non-negative int or None, got {self.requests!r}")
        # The normalized value object enforces the cross-field invariants.
        self.normalized()

    @classmethod
    def from_mapping(cls, usage: object) -> TokenUsage:
        """Read a usage record: an SDO turn/receipt ``usage``, a rollout ``token_usage`` or ``usage_metrics``.

        A record without ``cache_read_input_tokens`` predates agentshim 0.7,
        whose ``cached_input_tokens`` also counted (Claude) cache writes;
        agentshim's reader subtracts them back out.
        """
        if not isinstance(usage, dict):
            return cls()
        # Native receipts use ProtoJSON, which encodes int64 counts as decimal
        # strings. agentshim's mapping reader only accepts numeric values.
        usage = dict(usage)
        for name in (*_COUNT_FIELDS, "cache_read_input_tokens", "model_requests"):
            value = usage.get(name)
            if isinstance(value, str) and re.fullmatch(r"-?[0-9]+", value):
                usage[name] = int(value)
        requests = usage.get("model_requests")
        return cls.from_normalized(
            NormalizedUsage.from_dict(usage),
            requests=requests if isinstance(requests, int) and not isinstance(requests, bool) else None,
        )

    @classmethod
    def from_normalized(cls, tokens: NormalizedUsage, *, requests: int | None = None) -> TokenUsage:
        return cls(
            input_tokens=tokens.input_tokens,
            cached_input_tokens=tokens.cache_read_input_tokens,
            output_tokens=tokens.output_tokens,
            cache_write_input_tokens=tokens.cache_write_input_tokens,
            cache_write_1h_input_tokens=tokens.cache_write_1h_input_tokens,
            reasoning_output_tokens=tokens.reasoning_output_tokens,
            requests=requests,
        )

    def normalized(self) -> NormalizedUsage:
        """The counts as agentshim's ``TokenUsage`` (raises ``ValueError`` on an impossible breakdown)."""
        return NormalizedUsage(
            input_tokens=self.input_tokens,
            output_tokens=self.output_tokens,
            cache_read_input_tokens=self.cached_input_tokens,
            cache_write_input_tokens=self.cache_write_input_tokens,
            cache_write_1h_input_tokens=self.cache_write_1h_input_tokens,
            reasoning_output_tokens=self.reasoning_output_tokens,
        )

    @property
    def uncached_input_tokens(self) -> int:
        return self.input_tokens - self.cached_input_tokens - self.cache_write_input_tokens

    def __add__(self, other: TokenUsage) -> TokenUsage:
        requests = (
            None if self.requests is None and other.requests is None else (self.requests or 0) + (other.requests or 0)
        )
        return TokenUsage(
            self.input_tokens + other.input_tokens,
            self.cached_input_tokens + other.cached_input_tokens,
            self.output_tokens + other.output_tokens,
            self.cache_write_input_tokens + other.cache_write_input_tokens,
            self.cache_write_1h_input_tokens + other.cache_write_1h_input_tokens,
            self.reasoning_output_tokens + other.reasoning_output_tokens,
            requests,
        )

    def total(self, *, uncached: bool = False) -> int:
        """Raw ``input + output``; ``uncached`` drops cache reads (cache writes are billed input)."""
        cached = self.cached_input_tokens if uncached else 0
        return self.input_tokens - cached + self.output_tokens

    def weighted(self, weights: TokenWeights) -> float:
        return self.normalized().weighted_total(weights)


_COUNT_FIELDS = (
    "input_tokens",
    "cached_input_tokens",
    "output_tokens",
    "cache_write_input_tokens",
    "cache_write_1h_input_tokens",
    "reasoning_output_tokens",
)

#: CLI weight names (multiples of the base input rate) and the ``TokenWeights`` field each sets.
WEIGHT_NAMES = {
    "uncached": "uncached_input",
    "cache_read": "cache_read_input",
    "cache_write": "cache_write_input",
    "cache_write_1h": "cache_write_1h_input",
    "output": "output",
    "reasoning": "reasoning_output",
}
_REQUIRED_WEIGHTS = ("cache_read", "cache_write", "output")


@dataclass(frozen=True)
class ArmPricing:
    """How one arm's tokens are weighted and priced.

    ``weights`` are multiples of the base input rate (``None`` when the model
    is not in the table and the CLI did not supply every weight); ``entry`` is
    the table's USD price, ``None`` when the model is not priced.
    """

    provider: str
    model: str | None
    entry: ModelPricing | None
    weights: TokenWeights | None
    overridden: tuple[str, ...] = ()

    @property
    def label(self) -> str:
        return f"{self.provider}/{self.model or '?'}"


@dataclass(frozen=True)
class PricingContext:
    """The price table and CLI weight overrides a report is computed with."""

    table: PricingTable
    table_source: str = "agentshim pricing table"
    weight_overrides: dict[str, float] = field(default_factory=dict[str, float])

    def arm(self, provider: str | None, model: str | None) -> ArmPricing:
        provider = provider or "codex"
        entry = price_for(provider, model, self.table)
        overrides = {WEIGHT_NAMES[name]: value for name, value in self.weight_overrides.items()}
        weights: TokenWeights | None = entry.relative_weights() if entry is not None else None
        if weights is not None and overrides:
            weights = replace(weights, **overrides)
        elif weights is None and all(name in self.weight_overrides for name in _REQUIRED_WEIGHTS):
            weights = TokenWeights(
                uncached_input=overrides.get("uncached_input", 1.0),
                cache_read_input=overrides["cache_read_input"],
                cache_write_input=overrides["cache_write_input"],
                output=overrides["output"],
                cache_write_1h_input=overrides.get("cache_write_1h_input"),
                reasoning_output=overrides.get("reasoning_output"),
            )
        return ArmPricing(provider, model, entry, weights, tuple(sorted(self.weight_overrides)))


@dataclass(frozen=True)
class UsageSummary:
    """One usage's token classes, raw and weighted totals, USD cost and request count.

    For a Codex problem these are means over its runs.
    """

    uncached_input_tokens: float
    cache_read_input_tokens: float
    cache_write_input_tokens: float
    cache_write_1h_input_tokens: float
    output_tokens: float
    reasoning_output_tokens: float
    raw_tokens: float
    weighted_tokens: float | None
    usd: float | None
    requests: float | None

    @classmethod
    def of(cls, usage: TokenUsage, pricing: ArmPricing) -> UsageSummary:
        tokens = usage.normalized()
        return cls(
            uncached_input_tokens=usage.uncached_input_tokens,
            cache_read_input_tokens=usage.cached_input_tokens,
            cache_write_input_tokens=usage.cache_write_input_tokens,
            cache_write_1h_input_tokens=usage.cache_write_1h_input_tokens,
            output_tokens=usage.output_tokens,
            reasoning_output_tokens=usage.reasoning_output_tokens,
            raw_tokens=usage.total(),
            weighted_tokens=tokens.weighted_total(pricing.weights) if pricing.weights is not None else None,
            usd=cost_usd(tokens, pricing.entry) if pricing.entry is not None else None,
            requests=usage.requests,
        )

    @classmethod
    def mean(cls, summaries: list[UsageSummary]) -> UsageSummary:
        def avg(values: list[float | None]) -> float | None:
            if not values or any(value is None for value in values):
                return None
            return sum(value for value in values if value is not None) / len(values)

        def avg_known(values: list[float | None]) -> float | None:
            return _mean(values)

        return cls(
            uncached_input_tokens=avg([s.uncached_input_tokens for s in summaries]) or 0.0,
            cache_read_input_tokens=avg([s.cache_read_input_tokens for s in summaries]) or 0.0,
            cache_write_input_tokens=avg([s.cache_write_input_tokens for s in summaries]) or 0.0,
            cache_write_1h_input_tokens=avg([s.cache_write_1h_input_tokens for s in summaries]) or 0.0,
            output_tokens=avg([s.output_tokens for s in summaries]) or 0.0,
            reasoning_output_tokens=avg([s.reasoning_output_tokens for s in summaries]) or 0.0,
            raw_tokens=avg([s.raw_tokens for s in summaries]) or 0.0,
            weighted_tokens=avg([s.weighted_tokens for s in summaries]),
            usd=avg([s.usd for s in summaries]),
            requests=avg_known([s.requests for s in summaries]),
        )


@dataclass(frozen=True)
class Verdict:
    """Harness grading of one problem run and its agent-neutral timing."""

    diagnosis: bool | None
    mitigation: bool | None
    #: ``mitigation_submitted_at - fault_injected_at``; includes the diagnosis grading wait.
    raw_incl_judge_seconds: float | None
    #: Diagnosis POST until the conductor opened the mitigation stage (judge grading).
    grading_wait_seconds: float | None = None
    #: Injection until the agent issued its first state-changing command.
    mitigation_applied_seconds: float | None = None
    #: Time to diagnosis: ``diagnosis_submitted_at - fault_injected_at``.
    diagnosis_seconds: float | None = None
    #: Injection until the agent's last state-changing command before its mitigation POST completed.
    last_mitigation_seconds: float | None = None
    #: Injection until the last script whose content the rollout never shows finished (may have mutated).
    unverified_script_seconds: float | None = None
    #: Injection until the strict receipt's last recorded repair action completed (independent cross-check).
    receipt_applied_seconds: float | None = None

    @property
    def passed(self) -> bool:
        return self.diagnosis is True and self.mitigation is True

    @property
    def judge_excluded_seconds(self) -> float | None:
        """Raw time without the diagnosis grading wait the mitigation POST sat behind."""

        if self.raw_incl_judge_seconds is None or self.grading_wait_seconds is None:
            return None
        return self.raw_incl_judge_seconds - self.grading_wait_seconds

    @property
    def ttm_seconds(self) -> float | None:
        """Headline time to mitigation, excluding judge time.

        The judge-excluded time, but never less than when the last mitigating
        command completed (a kubectl write, directly or inside a script the
        rollout shows, a script it cannot show, or the receipt's last repair
        action): work done while the judge graded the diagnosis is agent time,
        and subtracting the whole grading wait would drop it.
        """

        bound = self._ttm_bound()
        return bound[0] if bound is not None else None

    @property
    def floor_source(self) -> str | None:
        """Which bound set the headline TTM: ``judge_excluded`` (no floor applied), ``rollout_mutation``,
        ``receipt_apply`` or ``unverified_script``; ``None`` when TTM is unknown."""

        bound = self._ttm_bound()
        return bound[1] if bound is not None else None

    def _ttm_bound(self) -> tuple[float, str] | None:
        excluded = self.judge_excluded_seconds
        if excluded is None:
            return None
        best = (excluded, "judge_excluded")
        for seconds, source in (
            (self.last_mitigation_seconds, "rollout_mutation"),
            (self.receipt_applied_seconds, "receipt_apply"),
            (self.unverified_script_seconds, "unverified_script"),
        ):
            if seconds is not None and seconds > best[0]:
                best = (seconds, source)
        return best


@dataclass(frozen=True)
class MemorySize:
    detectors: int
    incident_detectors: int
    playbooks: int


@dataclass(frozen=True)
class SdoStage:
    index: int
    name: str
    problem_id: str
    verdict: Verdict
    incident_resolution_seconds: float | None
    responder: TokenUsage
    reflection: TokenUsage
    reflection_attempts: int | None
    reflection_skipped_reason: str | None
    learning_seconds: float | None
    reflection_turn_seconds: float | None
    warm_path: bool | None
    match_reasons: tuple[str, ...]
    warm_prompt: bool | None
    memory: MemorySize | None
    lifecycle: TokenUsage
    provider: str | None = None
    model: str | None = None
    responder_summary: UsageSummary | None = None
    reflection_summary: UsageSummary | None = None
    #: The problem run's results directory (what the validity checker classifies).
    source: str = ""


@dataclass(frozen=True)
class CodexRun:
    """One run of a raw baseline agent (stock Codex or Claude Code) on one problem."""

    problem_id: str
    verdict: Verdict
    tokens: TokenUsage
    source: str
    provider: str = "codex"
    model: str | None = None


@dataclass(frozen=True)
class CodexProblemCost:
    problem_id: str
    runs: int
    passed: int
    mean_tokens: float
    mean_raw_incl_judge_seconds: float | None
    mean_judge_excluded_seconds: float | None = None
    mean_mitigation_applied_seconds: float | None = None
    mean_ttd_seconds: float | None = None
    mean_ttm_seconds: float | None = None
    mean_last_mitigation_seconds: float | None = None
    mean_summary: UsageSummary | None = None
    pricing: str | None = None


@dataclass(frozen=True)
class BreakEven:
    measure: str
    includes_lifecycle: bool
    stage: int | None
    final_gap: float
    projected_extra_repeats: int | None


@dataclass(frozen=True)
class CumulativeRow:
    index: int
    sdo_incident_tokens: int
    sdo_tokens_with_learning: int
    codex_tokens: float | None
    sdo_ttm_seconds: float | None
    codex_ttm_seconds: float | None
    sdo_incident_weighted: float | None = None
    sdo_weighted_with_learning: float | None = None
    codex_weighted: float | None = None
    sdo_incident_usd: float | None = None
    codex_usd: float | None = None


@dataclass(frozen=True)
class PricingInfo:
    """Which prices and weights a report used, for its output and JSON."""

    table_source: str
    table_version: str
    table_last_updated: str
    weight_overrides: dict[str, float]
    arms: dict[str, str]


@dataclass
class Report:
    stages: list[SdoStage]
    codex: dict[str, CodexProblemCost]
    cumulative: list[CumulativeRow]
    lifecycle_tokens: int
    break_even: list[BreakEven] = field(default_factory=list)
    uncached: bool = False
    pricing: PricingInfo | None = None
    lifecycle_summary: UsageSummary | None = None
    #: Runs left out as ``invalid_infra``, each with its reasons.
    excluded: list[str] = field(default_factory=list)  # pyright: ignore[reportUnknownVariableType]


# --------------------------------------------------------------------------- loading


def _truthy(value: object) -> bool | None:
    if value is None:
        return None
    text = str(value).strip().lower()
    if text in {"true", "1", "yes"}:
        return True
    if text in {"false", "0", "no"}:
        return False
    return None


def _float(value: object) -> float | None:
    try:
        number = float(str(value))
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def read_result_row(results_dir: Path) -> dict[str, str | None] | None:
    """The last row of the problem run's ``*_ALL_results.csv`` (or any results CSV), ``None`` when absent."""

    candidates = sorted(results_dir.glob("*_ALL_results.csv")) or sorted(results_dir.rglob("*_results.csv"))
    for path in candidates:
        with path.open(encoding="utf-8", newline="") as stream:
            rows = list(csv.DictReader(stream))
        if rows:
            return rows[-1]
    return None


def read_verdict(results_dir: Path) -> Verdict | None:
    """Read the last row of the problem run's ``*_ALL_results.csv`` (or any results CSV)."""

    row = read_result_row(results_dir)
    if row is None:
        return None
    injected = _float(row.get("fault_injected_at"))
    submitted = _float(row.get("mitigation_submitted_at"))
    raw = submitted - injected if injected is not None and submitted is not None else None
    # The conductor records TTL when the diagnosis verdict completes, right before it opens the
    # mitigation stage, on a clock that starts when the fault is injected.
    diagnosed = _float(row.get("diagnosis_submitted_at"))
    ttl = _float(row.get("TTL"))
    grading_wait = (
        max(0.0, injected + ttl - diagnosed)
        if injected is not None and diagnosed is not None and ttl is not None
        else None
    )
    if _truthy(row.get("diagnosis_grading_deferred")) is True:
        # defer_diagnosis_grading opens the mitigation stage at the diagnosis POST and grades
        # it in the background, so the mitigation POST never waits on the judge.
        grading_wait = 0.0
    return Verdict(
        diagnosis=_truthy(row.get("Diagnosis.success")),
        mitigation=_truthy(row.get("Mitigation.success")),
        raw_incl_judge_seconds=raw,
        grading_wait_seconds=grading_wait,
        diagnosis_seconds=diagnosed - injected if injected is not None and diagnosed is not None else None,
    )


_KUBECTL_WRITES = frozenset(
    {"apply", "create", "patch", "replace", "delete", "rollout", "set", "scale", "edit", "label", "annotate"}
)
_KUBECTL_ROLLOUT_WRITES = frozenset({"restart", "undo", "pause", "resume"})
_KUBECTL_VALUE_FLAGS = frozenset({"-n", "--namespace", "--context", "--kubeconfig", "-l", "--selector"})
_CODE_MODE_CMD = re.compile(r'\bcmd\s*:\s*("(?:[^"\\]|\\.)*")')
_COMMAND_SPLIT = re.compile(r"&&|\|\||[;|\n]")
_STATEMENT_SPLIT = re.compile(r"&&|\|\||[;\n]")

#: A script's classification: it changes cluster state, or it ran but its content is not in the rollout.
_MUTATION = "mutation"
_UNVERIFIED = "unverified_script"

_KUBECTL_WORD = re.compile(r"(?<![\w-])kubectl\b")
_PYTHON_ARGV_LIST = re.compile(r"""['"]\s*,""")  # ``['kubectl', ...`` right after the program name
_ARGV_END = re.compile(r"[\]\)]")
_SHELL_WORD_END = re.compile(r"[\n;|&)]")
_ARGV_SEPARATORS = re.compile(r"""[\s,\[\](){}+*%]+""")
_HEREDOC = re.compile(r"(?<!<)<<(?!<)-?\s*(['\"]?)(\w+)\1")
_FEEDS_INTERPRETER = re.compile(r"\b(?:python[\d.]*|bash|sh|zsh)(?:\s+-[\w-]*)*\s*<<")
_RUNS_INLINE_CODE = re.compile(r"\b(?:python[\d.]*|bash|sh|zsh)\b[^\n]*?\s-[A-Za-z]*c\b")
_WRITE_TARGET = re.compile(r"""(?:(?<![\d&<])>>?|\btee(?:\s+-a)?)\s*("[^"]+"|'[^']+'|[^\s;|&<>]+)""")
_INTERPRETER = re.compile(r"python[\d.]*|bash|sh|zsh")
_INLINE_OPTION = re.compile(r"-[A-Za-z]*c[A-Za-z]*")
_DIRECT_SCRIPT = re.compile(r"""^["']?[^\s"'(){}<>]+\.(?:sh|bash|py)["']?$""")
_SCRIPT_SUFFIXES = (".sh", ".bash", ".py")


def _rollout_commands(payload: dict[str, Any]) -> list[str]:
    """Shell commands of one Codex tool call (plain ``exec_command`` or code-mode ``exec``)."""

    if payload.get("type") == "function_call":
        try:
            arguments = json.loads(str(payload.get("arguments") or "{}"))
        except json.JSONDecodeError:
            return []
        command = arguments.get("cmd") or arguments.get("command") if isinstance(arguments, dict) else None
        if isinstance(command, list):
            command = " ".join(str(part) for part in command)
        return [command] if isinstance(command, str) else []
    if payload.get("type") == "custom_tool_call":
        commands = []
        for literal in _CODE_MODE_CMD.findall(str(payload.get("input") or "")):
            try:
                commands.append(json.loads(literal))
            except json.JSONDecodeError:
                continue
        return commands
    return []


def _kubectl_mutates(args: list[str]) -> bool:
    """Whether ``kubectl <args>`` writes cluster state (flags before the verb skipped; ``--dry-run`` never does)."""

    if any(arg.startswith("--dry-run") for arg in args):
        return False
    rest = iter(args)
    for token in rest:
        if token in _KUBECTL_VALUE_FLAGS:
            next(rest, None)
        elif not token.startswith("-"):
            if token == "rollout":
                action = next((item for item in rest if not item.startswith("-")), "")
                return action in _KUBECTL_ROLLOUT_WRITES
            return token in _KUBECTL_WRITES
    return False


def _text_mutates(text: str) -> bool:
    """Whether script or inline interpreter source invokes a state-changing ``kubectl``.

    Finds each ``kubectl`` word and reads its arguments as a shell word list or as
    a Python argv list (``["kubectl", "-n", ns, "patch", ...]``, possibly spanning
    lines). Arguments held in variables or built by a wrapper are not resolved.
    """

    text = text.replace("\\\n", " ")
    for match in _KUBECTL_WORD.finditer(text):
        rest = text[match.end() :]
        argv_list = _PYTHON_ARGV_LIST.match(rest) is not None
        end = (_ARGV_END if argv_list else _SHELL_WORD_END).search(rest)
        window = rest[: end.start()] if end else rest
        tokens = [token.strip("'\"") for token in _ARGV_SEPARATORS.split(window)]
        if _kubectl_mutates([token for token in tokens if token]):
            return True
    return False


def _split_heredocs(command: str) -> tuple[str, list[tuple[str, str]]]:
    """The command without its heredoc bodies, and each body with the statement that introduced it.

    The statement is the text on the heredoc's own command line around its ``<<`` marker, cut at
    ``;``/``&&``/``||``, so earlier commands on the same line cannot be mistaken for its redirects.
    """

    lines = command.split("\n")
    kept: list[str] = []
    bodies: list[tuple[str, str]] = []
    index = 0
    while index < len(lines):
        line = lines[index]
        kept.append(line)
        index += 1
        for marker in _HEREDOC.finditer(line):
            delimiter = marker.group(2)
            header = (
                _STATEMENT_SPLIT.split(line[: marker.start()])[-1]
                + "<<"
                + _STATEMENT_SPLIT.split(line[marker.end() :])[0]
            )
            body: list[str] = []
            while index < len(lines) and lines[index].strip() != delimiter:
                body.append(lines[index])
                index += 1
            index += 1
            bodies.append((header, "\n".join(body)))
    return "\n".join(kept), bodies


class _CommandClassifier:
    """Classifies the shell commands of one agent session in order, remembering the scripts it writes.

    A command is a ``mutation`` when it runs a state-changing ``kubectl`` directly,
    in inline interpreter code (``python3 - <<EOF``, ``python3 -c``, ``bash -c``),
    in a script file the session wrote earlier, or a playbook ``repair*`` script. A
    script file whose content the rollout never shows is ``unverified_script``: it
    may have mutated, which the floor cannot rule out.
    """

    def __init__(self) -> None:
        self._by_path: dict[str, str] = {}
        self._by_name: dict[str, str] = {}

    def classify(self, command: str) -> str | None:
        stripped, bodies = _split_heredocs(command)
        for header, body in bodies:
            if _FEEDS_INTERPRETER.search(header):
                if _text_mutates(body):
                    return _MUTATION
            elif (target := _WRITE_TARGET.search(header)) is not None:
                path = target.group(1).strip("\"'")
                self._by_path[path] = self._by_name[Path(path).name] = body
        if _RUNS_INLINE_CODE.search(stripped) and _text_mutates(stripped):
            return _MUTATION
        found: str | None = None
        for segment in _COMMAND_SPLIT.split(stripped):
            kind = self._classify_segment(segment.split())
            if kind == _MUTATION:
                return kind
            found = kind or found
        return found

    def _classify_segment(self, tokens: list[str]) -> str | None:
        while tokens and ("=" in tokens[0] and not tokens[0].startswith("-") or tokens[0] in {"env", "sudo"}):
            tokens = tokens[1:]
        if not tokens:
            return None
        name = Path(tokens[0]).name
        if name == "kubectl":
            return _MUTATION if _kubectl_mutates(tokens[1:]) else None
        if name in {"source", "."}:
            target = next((token for token in tokens[1:] if not token.startswith("-")), None)
        elif _INTERPRETER.fullmatch(name):
            if any(token.startswith("-n") for token in tokens[1:2]):
                return None  # ``bash -n`` only checks syntax
            target = _script_argument(tokens[1:])
        elif _DIRECT_SCRIPT.match(tokens[0]):
            target = tokens[0]
        else:
            return None
        if target is None:
            return None
        target = target.strip("\"'")
        if Path(target).parent.name == "scripts" and Path(target).name.startswith("repair"):
            return _MUTATION
        content = self._by_path.get(target, self._by_name.get(Path(target).name))
        if content is not None:
            return _MUTATION if _text_mutates(content) else None
        return _UNVERIFIED if target.endswith(_SCRIPT_SUFFIXES) else None


def _script_argument(args: list[str]) -> str | None:
    """The script file an interpreter runs: its first non-option argument (none for ``-m``, ``-c`` or stdin)."""

    for arg in args:
        if arg in {"-m", "-"} or _INLINE_OPTION.fullmatch(arg):
            return None
        if not arg.startswith("-"):
            return arg
    return None


def _timestamp(value: object) -> float | None:
    text = re.sub(r"(\.\d{6})\d+", r"\1", str(value).replace("Z", "+00:00"))
    try:
        return datetime.fromisoformat(text).timestamp()
    except ValueError:
        return None


_TOOL_OUTPUTS = frozenset({"function_call_output", "custom_tool_call_output"})


@dataclass(frozen=True)
class RolloutMutations:
    """State-changing activity found in Codex rollouts, as epochs."""

    #: Issue time of the first state-changing call at or after the window start.
    first: float | None = None
    #: Completion of the last state-changing call issued in the window (capped at its end).
    last_done: float | None = None
    #: Completion of the last script call in the window whose content the rollout never shows.
    last_unverified_done: float | None = None


def scan_rollout_mutations(rollouts: list[Path], *, after: float, before: float | None = None) -> RolloutMutations:
    """Find the state-changing tool calls of Codex rollouts, including those made through scripts.

    Calls are classified in order across *rollouts* so a script written by one call is
    recognized when a later call runs it, even when the write happened before *after*.
    Completion is the call's output record (paired by ``call_id``), capped at *before*
    (the mitigation POST); a call without a recorded output counts at its issue time.
    """

    classifier = _CommandClassifier()
    done_at: dict[str, float] = {}
    pending: list[tuple[str, float, str]] = []
    first: float | None = None
    last: dict[str, float] = {}

    def settle(kind: str, stamp: float) -> None:
        last[kind] = max(stamp, last.get(kind, stamp))

    for path in rollouts:
        for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
            if '"call_id"' not in line and '"custom_tool_call"' not in line and '"function_call"' not in line:
                continue
            record = json.loads(line)
            payload = record.get("payload")
            stamp = _timestamp(record.get("timestamp"))
            if not isinstance(payload, dict) or stamp is None:
                continue
            call_id = str(payload.get("call_id") or "")
            if payload.get("type") in _TOOL_OUTPUTS:
                if call_id:
                    done_at[call_id] = stamp
                continue
            kinds = {classifier.classify(command) for command in _rollout_commands(payload)} - {None}
            kind = _MUTATION if _MUTATION in kinds else _UNVERIFIED if kinds else None
            if kind is None or stamp < after:
                continue
            if kind == _MUTATION and (first is None or stamp < first):
                first = stamp
            if before is not None and stamp > before:
                continue
            if call_id:
                pending.append((call_id, stamp, kind))
            else:
                settle(kind, stamp)
    for call_id, issued, kind in pending:
        done = done_at.get(call_id, issued)
        settle(kind, min(done, before) if before is not None else done)
    return RolloutMutations(first, last.get(_MUTATION), last.get(_UNVERIFIED))


def first_mutation_at(rollouts: list[Path], *, after: float) -> float | None:
    """Epoch of the first state-changing tool call issued at or after *after* in Codex rollouts."""

    return scan_rollout_mutations(rollouts, after=after).first


def last_mutation_done_at(rollouts: list[Path], *, after: float, before: float) -> float | None:
    """Epoch at which the last state-changing tool call issued in ``[after, before]`` completed."""

    return scan_rollout_mutations(rollouts, after=after, before=before).last_done


def receipt_repair_applied_at(receipt: dict[str, Any], *, after: float, before: float) -> float | None:
    """Epoch of the last repair action the strict receipt's incident ledger records in ``[after, before]``.

    An independent cross-check on the rollout: the actions are reported by the repair itself,
    so they are seen even when the rollout hides the mutation inside a script.
    """

    ledger = receipt.get("native_ledger")
    if not isinstance(ledger, dict):
        return None
    outcome = ledger.get("outcome_document")
    closure = ledger.get("closure")
    result = closure.get("result") if isinstance(closure, dict) else None
    actions = (outcome.get("repair_actions") if isinstance(outcome, dict) else None) or (
        result.get("repair_actions") if isinstance(result, dict) else None
    )
    stamps = [
        _timestamp(action.get("completed_at") or action.get("started_at"))
        for action in actions or []
        if isinstance(action, dict)
    ]
    return max((stamp for stamp in stamps if stamp is not None and after <= stamp <= before), default=None)


def with_mitigation_applied(
    verdict: Verdict, results_dir: Path, rollouts: list[Path], *, receipt: dict[str, Any] | None = None
) -> Verdict:
    row = read_result_row(results_dir) or {}
    injected = _float(row.get("fault_injected_at"))
    submitted = _float(row.get("mitigation_submitted_at"))
    if injected is None or not (rollouts or receipt):
        return verdict
    found = scan_rollout_mutations(rollouts, after=injected, before=submitted) if rollouts else RolloutMutations()
    receipt_applied = (
        receipt_repair_applied_at(receipt, after=injected, before=submitted)
        if receipt is not None and submitted is not None
        else None
    )

    def since_injection(epoch: float | None) -> float | None:
        return epoch - injected if epoch is not None else None

    if not rollouts:
        return replace(verdict, receipt_applied_seconds=since_injection(receipt_applied))
    return replace(
        verdict,
        mitigation_applied_seconds=since_injection(found.first),
        last_mitigation_seconds=since_injection(found.last_done) if submitted is not None else None,
        unverified_script_seconds=since_injection(found.last_unverified_done) if submitted is not None else None,
        receipt_applied_seconds=since_injection(receipt_applied),
    )


def problem_results_dirs(experiment_dir: Path) -> list[tuple[str, Path]]:
    """Return ``(problem_id, results_dir)`` in sequence order for one experiment directory."""

    return [
        (run_dir.name.split("_", 1)[1], results)
        for run_dir in sorted((experiment_dir / "runs").glob("*_*"))
        for results in sorted(run_dir.glob("worker_*/results"))
    ]


def _sum_usage_jsonl(paths: list[Path], *, incident_id: str | None = None) -> tuple[TokenUsage, float | None]:
    """Sum per-turn usage records, optionally only the turns run in one incident's worktree.

    A persistent controller's usage log accumulates every incident it served,
    so a stage filters by its receipt's incident; the broker runs reflection in
    the incident worktree, whose directory name is derived from the incident ID.
    """

    worktree = incident_worktree_dirname(incident_id) if incident_id else None
    usage = TokenUsage()
    seconds: float | None = None
    for path in paths:
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            record = json.loads(line)
            if worktree is not None and Path(str(record.get("cwd") or "")).name != worktree:
                continue
            usage = usage + TokenUsage.from_mapping(record.get("usage"))
            duration = _float(record.get("duration_seconds"))
            if duration is not None:
                seconds = (seconds or 0.0) + duration
    return usage, seconds


def rollout_turn_usages(rollouts: list[Path]) -> list[TokenUsage]:
    """Per-turn token usage of Codex session rollouts, in turn order.

    Each model response writes a ``token_count`` event whose ``last_token_usage``
    is that response's own usage (with reasoning, which Codex's ``--json``
    stream did not surface before agentshim 0.7); a ``task_started`` event
    opens each turn. Each turn's ``requests`` counts its responses.
    """

    turns: list[TokenUsage] = []
    for path in rollouts:
        for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
            if '"task_started"' not in line and '"token_count"' not in line:
                continue
            payload = json.loads(line).get("payload")
            if not isinstance(payload, dict):
                continue
            if payload.get("type") == "task_started":
                turns.append(TokenUsage(requests=0))
            elif payload.get("type") == "token_count" and isinstance(payload.get("info"), dict) and turns:
                last = TokenUsage.from_mapping(payload["info"].get("last_token_usage"))
                turns[-1] = turns[-1] + replace(last, requests=1)
    return turns


def _reflection_usage(
    receipt: dict[str, Any], responder_rollouts: list[Path], own_rollouts: list[Path] | None = None
) -> TokenUsage:
    """The reflection's own tokens, from rollouts where they were exported.

    A reflection that resumed the responder's session was recorded with
    ``codex exec resume``'s session-cumulative usage, which includes the
    responder turn; its own turns are read back from the shared rollout.
    Reflection turns in sessions of their own (fresh mode, fresh retries) are
    read from those sessions' rollouts (*own_rollouts*), which also carry the
    reasoning and request counts older receipts lack. Without rollouts, the
    receipt's ``reflection_usage`` is used.
    """

    ledger = receipt.get("native_ledger")
    decision = ledger.get("reflection_decision") if isinstance(ledger, dict) else None
    native_usage = decision.get("usage") if isinstance(decision, dict) else None
    recorded = TokenUsage.from_mapping(receipt.get("reflection_usage") or native_usage)
    own = [turn for turn in rollout_turn_usages(own_rollouts or []) if turn.input_tokens]
    if receipt.get("reflection_session_mode") == "resume":
        shared = rollout_turn_usages(responder_rollouts)
        if len(shared) < 2:
            return recorded
        return sum([*shared[1:], *own], TokenUsage())
    if own:
        return sum(own, TokenUsage())
    return recorded


def _usage_session_ids(paths: list[Path], *, incident_id: str | None = None) -> list[str]:
    """Session IDs of the usage-log turns run in one incident's worktree (all turns when no incident)."""

    worktree = incident_worktree_dirname(incident_id) if incident_id else None
    sessions: list[str] = []
    for path in paths:
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            record = json.loads(line)
            if worktree is not None and Path(str(record.get("cwd") or "")).name != worktree:
                continue
            session = record.get("session_id")
            if isinstance(session, str) and session and session not in sessions:
                sessions.append(session)
    return sessions


def _receipt_responder_result(receipt: dict[str, Any]) -> dict[str, Any]:
    ledger = receipt.get("native_ledger")
    closure = ledger.get("closure") if isinstance(ledger, dict) else None
    result = closure.get("result") if isinstance(closure, dict) else None
    return result if isinstance(result, dict) else {}


def receipt_responder_session_id(receipt: dict[str, Any]) -> str | None:
    """The responder session in a legacy receipt or native closure result."""

    session = receipt.get("responder_session_id") or _receipt_responder_result(receipt).get("responder_session_id")
    return session if isinstance(session, str) and session else None


def sdo_rollouts(results: Path) -> list[Path]:
    """Codex exports in both the legacy shared and current per-session layout."""

    return sorted(results.rglob("sdo_runtime/**/codex/sessions/**/rollout-*.jsonl"))


def responder_rollouts(results: Path, receipt: dict[str, Any]) -> list[Path]:
    """Only rollouts bound to this receipt's responder, excluding other incidents."""

    session = receipt_responder_session_id(receipt)
    return [path for path in sdo_rollouts(results) if session and session in path.name]


def receipt_responder_usage(receipt: dict[str, Any]) -> TokenUsage:
    """The responder's recorded usage: the legacy receipt's ``usage``, else the native ledger's closure result."""

    legacy = receipt.get("usage")
    if isinstance(legacy, dict):
        return TokenUsage.from_mapping(legacy)
    return TokenUsage.from_mapping(_receipt_responder_result(receipt).get("usage"))


def _responder_usage(receipt: dict[str, Any], responder_rollouts: list[Path]) -> TokenUsage:
    """The responder turn's usage: its rollout's first turn when exported, else the receipt's.

    The rollout carries what receipts written before agentshim 0.7 lack:
    reasoning tokens and the model-request count.
    """

    recorded = receipt_responder_usage(receipt)
    turns = rollout_turn_usages(responder_rollouts)
    if turns and turns[0].input_tokens:
        return turns[0]
    return recorded


def experiment_model(experiment_dir: Path) -> tuple[str | None, str | None]:
    """``(provider, model)`` an experiment ran, from its ``experiment_config.toml``.

    SDO arms name their provider under ``[agent.<agent>]``; a raw baseline's
    agent is the provider (``claudecode`` runs Claude Code).
    """

    path = experiment_dir / "experiment_config.toml"
    if not path.is_file():
        return None, None
    config = tomllib.loads(path.read_text(encoding="utf-8"))
    runner = config.get("runner") or {}
    agent = str(runner.get("agent") or "")
    section = (config.get("agent") or {}).get(agent) or {}
    model = section.get("model") or runner.get("model")
    provider = section.get("provider") or _BASELINE_PROVIDERS.get(agent)
    return (str(provider) if provider else None, str(model) if model else None)


_BASELINE_PROVIDERS = {"codex": "codex", "claudecode": "claude"}


def warm_prompt_fired(results_dir: Path, responder_session_id: str | None) -> bool | None:
    """Whether a responder rollout's user prompt carried the warm-path instructions.

    Returns ``None`` when no Codex rollout was exported.
    """

    rollouts = sdo_rollouts(results_dir)
    if responder_session_id:
        own = [path for path in rollouts if responder_session_id in path.name]
        rollouts = own or rollouts
    if not rollouts:
        return None
    for path in rollouts:
        for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
            if WARM_PROMPT_MARKER not in line:
                continue
            record = json.loads(line)
            payload = record.get("payload")
            if isinstance(payload, dict) and payload.get("type") == "message" and payload.get("role") == "user":
                return True
    return False


def memory_size(workspace: Path) -> MemorySize | None:
    """Count registered detectors and playbooks in a workspace's ``.sdo/``."""

    sdo = workspace / ".sdo"
    if not sdo.is_dir():
        return None
    detectors = workspace_detector_manifests(workspace)
    playbooks = list((sdo / "playbooks").glob("*/README.md"))
    return MemorySize(
        detectors=len(detectors),
        incident_detectors=sum(1 for item in detectors if item.get("class") == "incident"),
        playbooks=len(playbooks),
    )


def pipeline_stage_dirs(pipeline_dir: Path) -> list[tuple[int, str, Path]]:
    """Ordered ``(index, name, dir)`` of a pipeline's current stages, skipping renamed aborted attempts."""

    state_path = pipeline_dir / "pipeline_state.json"
    stages: list[tuple[int, str, Path]] = []
    if state_path.is_file():
        state = json.loads(state_path.read_text(encoding="utf-8"))
        for stage in state.get("stages", []):
            if not stage.get("experiment_dir"):
                continue  # a stage that never started
            recorded = Path(str(stage.get("experiment_dir", "")))
            local = pipeline_dir / recorded.name
            directory = local if local.is_dir() else recorded
            if directory.is_dir():
                stages.append((int(stage["index"]), str(stage["name"]), directory))
    else:
        for directory in pipeline_dir.iterdir():
            match = _STAGE_DIR.match(directory.name)
            if match and directory.is_dir() and not _RENAMED_STAGE.search(directory.name):
                stages.append((int(match.group(1)), match.group(2), directory))
    if not stages:
        raise IncidentCostError(f"{pipeline_dir} has no pipeline stages")
    return sorted(stages)


def _stage_end_record(results: Path) -> dict[str, Any]:
    """The resolution record of a stage whose strict receipt never came (a pipeline stopped before its drain).

    It names the incident and the responder session, so the stage-end
    ``sdo_runtime/`` snapshot still yields the responder's tokens. Reflection
    usage is unknown without the receipt.
    """

    records = sorted(results.rglob(RESOLUTION_NAME))
    if not records:
        return {}
    record = json.loads(records[-1].read_text(encoding="utf-8"))
    keys = ("incident_id", "responder_session_id", "incident_resolution_seconds")
    return {key: record[key] for key in keys if key in record} if isinstance(record, dict) else {}


def load_sdo_pipeline(pipeline_dir: Path) -> list[SdoStage]:
    stages: list[SdoStage] = []
    for index, name, stage_dir in pipeline_stage_dirs(pipeline_dir):
        provider, model = experiment_model(stage_dir)
        memory = memory_size(stage_dir / "application_workspace")
        for problem_id, results in problem_results_dirs(stage_dir):
            receipts = sorted(results.rglob(RECEIPT_NAME))
            receipt: dict[str, Any] = (
                json.loads(receipts[-1].read_text(encoding="utf-8")) if receipts else _stage_end_record(results)
            )
            reuse = receipt.get("memory_reuse") if isinstance(receipt.get("memory_reuse"), dict) else {}
            phases = receipt.get("phase_timings_seconds") or {}
            incident_id = receipt.get("incident_id")
            controller_turns = sorted(results.rglob("sdo_runtime/usage/controller-turns.jsonl"))
            scoped_incident = incident_id if isinstance(incident_id, str) and incident_id else None
            _, reflection_seconds = _sum_usage_jsonl(controller_turns, incident_id=scoped_incident)
            lifecycle, _ = _sum_usage_jsonl(sorted(results.rglob(LIFECYCLE_USAGE_NAME)))
            session = receipt_responder_session_id(receipt)
            own_responder_rollouts = responder_rollouts(results, receipt)
            all_rollouts = sdo_rollouts(results)
            reflection_sessions = [
                own for own in _usage_session_ids(controller_turns, incident_id=scoped_incident) if own != session
            ]
            reflection_rollouts = [
                path for path in all_rollouts if any(own in path.name for own in reflection_sessions)
            ]
            verdict = with_mitigation_applied(
                read_verdict(results) or Verdict(None, None, None),
                results,
                own_responder_rollouts,
                receipt=receipt,
            )
            stages.append(
                SdoStage(
                    index=index,
                    name=name,
                    problem_id=problem_id,
                    verdict=verdict,
                    incident_resolution_seconds=_float(receipt.get("incident_resolution_seconds")),
                    responder=_responder_usage(receipt, own_responder_rollouts),
                    reflection=_reflection_usage(receipt, own_responder_rollouts, reflection_rollouts),
                    reflection_attempts=receipt.get("reflection_attempts"),
                    reflection_skipped_reason=receipt.get("reflection_skipped_reason"),
                    learning_seconds=_float(phases.get("post_recovery_learning_and_receipt")),
                    reflection_turn_seconds=reflection_seconds,
                    warm_path=reuse.get("warm_path") if reuse else None,
                    match_reasons=tuple(reuse.get("match_reasons") or ()) if reuse else (),
                    warm_prompt=warm_prompt_fired(results, session if isinstance(session, str) else None),
                    memory=memory,
                    lifecycle=lifecycle,
                    provider=provider,
                    model=model,
                    source=str(results),
                )
            )
    return stages


def _codex_tokens(results_dir: Path) -> TokenUsage:
    """A stock Codex run's usage: its session rollout when exported, else its result files.

    The rollout gives the full breakdown (reasoning included) and the request
    count; ``usage_metrics`` and ``trajectory.json`` have totals only.
    """

    rollouts = sorted(results_dir.rglob("sessions/**/rollout-*.jsonl"))
    turns = rollout_turn_usages(rollouts)
    if turns and any(turn.input_tokens for turn in turns):
        return sum(turns, TokenUsage(requests=0))
    for path in sorted(results_dir.rglob("codex_results_*.json")):
        document = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(document.get("usage_metrics"), dict):
            return TokenUsage.from_mapping(document["usage_metrics"])
    for path in sorted(results_dir.rglob("trajectory.json")):
        metrics = json.loads(path.read_text(encoding="utf-8")).get("final_metrics") or {}
        extra = metrics.get("extra") or {}
        return TokenUsage(
            input_tokens=int(metrics.get("total_prompt_tokens") or 0),
            cached_input_tokens=int(metrics.get("total_cached_tokens") or 0),
            output_tokens=int(metrics.get("total_completion_tokens") or 0),
            reasoning_output_tokens=int(extra.get("reasoning_output_tokens") or 0),
        )
    return TokenUsage()


def _claude_tokens(results_dir: Path) -> TokenUsage:
    """A stock Claude Code run's usage, from the ``result`` frames of its ``stream-json`` output.

    Each frame is replayed through agentshim's Claude parser, so cache reads
    and writes (by TTL) are normalized as for SDO's Claude turns; requests are
    the frames' ``num_turns``. SREGym's ``usage_metrics`` is the fallback: it
    excludes cache reads from ``input_tokens`` and drops cache writes, so only
    the reads are added back.
    """

    usage: TokenUsage | None = None
    for path in sorted(results_dir.rglob("*")):
        if not path.is_file() or path.suffix not in {".txt", ".log", ".jsonl"}:
            continue
        for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
            if '"result"' not in line or '"usage"' not in line:
                continue
            parser = ClaudeStreamParser(lambda _event: None)
            parser.feed_stdout(line)
            parsed = parser.finish()
            if parsed.usage.raw is None:
                continue
            turn = TokenUsage.from_normalized(parsed.usage.tokens, requests=parsed.usage.tokens.turns)
            usage = turn if usage is None else usage + turn
    if usage is not None:
        return usage
    for path in sorted(results_dir.rglob("claudecode_results_*.json")):
        metrics = json.loads(path.read_text(encoding="utf-8")).get("usage_metrics")
        if isinstance(metrics, dict):
            reads = int(metrics.get("cached_input_tokens") or 0)
            return TokenUsage(
                input_tokens=int(metrics.get("input_tokens") or 0) + reads,
                cached_input_tokens=reads,
                output_tokens=int(metrics.get("output_tokens") or 0),
            )
    return TokenUsage()


def load_codex_runs(directory: Path) -> list[CodexRun]:
    """Load every raw-baseline (Codex or Claude Code) run below a plain experiment or pipeline directory."""

    if (directory / "pipeline_state.json").is_file() or not (directory / "runs").is_dir():
        experiment_dirs = [path for _, _, path in pipeline_stage_dirs(directory)]
    else:
        experiment_dirs = [directory]
    runs: list[CodexRun] = []
    for experiment_dir in experiment_dirs:
        provider, model = experiment_model(experiment_dir)
        for problem_id, results in problem_results_dirs(experiment_dir):
            verdict = read_verdict(results)
            if verdict is None:
                continue
            rollouts = sorted(results.rglob("sessions/**/rollout-*.jsonl"))
            verdict = with_mitigation_applied(verdict, results, rollouts)
            claude = provider == "claude" or any(results.rglob("claudecode_results_*.json"))
            tokens = _claude_tokens(results) if claude else _codex_tokens(results)
            runs.append(
                CodexRun(
                    problem_id,
                    verdict,
                    tokens,
                    str(results),
                    provider="claude" if claude else (provider or "codex"),
                    model=model,
                )
            )
    return runs


# --------------------------------------------------------------------------- analysis


def _mean(values: list[float | None]) -> float | None:
    known = [value for value in values if value is not None]
    return sum(known) / len(known) if known else None


def codex_costs(
    runs: list[CodexRun],
    *,
    uncached: bool,
    pricing: PricingContext | None = None,
    model_override: tuple[str, str] | None = None,
) -> dict[str, CodexProblemCost]:
    pricing = pricing or PricingContext(default_pricing())
    by_problem: dict[str, list[CodexRun]] = {}
    for run in runs:
        by_problem.setdefault(run.problem_id, []).append(run)
    costs: dict[str, CodexProblemCost] = {}
    for problem_id, group in by_problem.items():
        arms = [pricing.arm(*(model_override or (run.provider, run.model))) for run in group]
        costs[problem_id] = CodexProblemCost(
            problem_id=problem_id,
            runs=len(group),
            passed=sum(run.verdict.passed for run in group),
            mean_tokens=sum(run.tokens.total(uncached=uncached) for run in group) / len(group),
            mean_raw_incl_judge_seconds=_mean([run.verdict.raw_incl_judge_seconds for run in group]),
            mean_judge_excluded_seconds=_mean([run.verdict.judge_excluded_seconds for run in group]),
            mean_mitigation_applied_seconds=_mean([run.verdict.mitigation_applied_seconds for run in group]),
            mean_ttd_seconds=_mean([run.verdict.diagnosis_seconds for run in group]),
            mean_ttm_seconds=_mean([run.verdict.ttm_seconds for run in group]),
            mean_last_mitigation_seconds=_mean([run.verdict.last_mitigation_seconds for run in group]),
            mean_summary=UsageSummary.mean(
                [UsageSummary.of(run.tokens, arm) for run, arm in zip(group, arms, strict=True)]
            ),
            pricing=", ".join(sorted({arm.label for arm in arms})),
        )
    return costs


def _break_even(
    measure: str,
    sdo: list[int] | list[float | None],
    codex: list[float | None],
    offset: float,
    stages: list[SdoStage],
) -> BreakEven:
    """First stage whose cumulative SDO tokens (plus ``offset``) are at most Codex's."""

    crossing: int | None = None
    gap = 0.0
    for position, (sdo_total, codex_total) in enumerate(zip(sdo, codex, strict=True)):
        if codex_total is None or sdo_total is None:
            break
        gap = sdo_total + offset - codex_total
        if crossing is None and gap <= 0:
            crossing = stages[position].index
    projected: int | None = None
    if crossing is None and gap > 0 and codex and codex[-1] is not None:
        # Project with the mean saving of stages whose problem was already seen earlier.
        seen: set[str] = set()
        savings: list[float] = []
        previous_sdo, previous_codex = 0, 0.0
        for stage, sdo_total, codex_total in zip(stages, sdo, codex, strict=True):
            if codex_total is None or sdo_total is None:
                break
            if stage.problem_id in seen:
                savings.append((codex_total - previous_codex) - (sdo_total - previous_sdo))
            seen.add(stage.problem_id)
            previous_sdo, previous_codex = sdo_total, codex_total
        mean_saving = sum(savings) / len(savings) if savings else 0.0
        if mean_saving > 0:
            projected = math.ceil(gap / mean_saving)
    return BreakEven(measure, offset > 0, crossing, gap, projected)


def _running(total: float | None, value: float | None) -> float | None:
    return total + value if total is not None and value is not None else None


def build_report(
    stages: list[SdoStage],
    codex_runs: list[CodexRun],
    *,
    lifecycle_override: TokenUsage | None = None,
    uncached: bool = False,
    pricing: PricingContext | None = None,
    sdo_model: tuple[str, str] | None = None,
    codex_model: tuple[str, str] | None = None,
) -> Report:
    pricing = pricing or PricingContext(default_pricing())
    costs = codex_costs(codex_runs, uncached=uncached, pricing=pricing, model_override=codex_model)
    lifecycle = lifecycle_override or sum((stage.lifecycle for stage in stages), TokenUsage())
    lifecycle_tokens = lifecycle.total(uncached=uncached)

    arms: dict[str, str] = {}
    priced: list[SdoStage] = []
    for stage in stages:
        arm = pricing.arm(*(sdo_model or (stage.provider, stage.model)))
        arms[f"sdo {arm.label}"] = _describe_arm(arm)
        priced.append(
            replace(
                stage,
                responder_summary=UsageSummary.of(stage.responder, arm),
                reflection_summary=UsageSummary.of(stage.reflection, arm),
            )
        )
    stages = priced
    for run in codex_runs:
        arm = pricing.arm(*(codex_model or (run.provider, run.model)))
        arms[f"baseline {arm.label}"] = _describe_arm(arm)
    lifecycle_arm = pricing.arm(*(sdo_model or ((stages[0].provider, stages[0].model) if stages else (None, None))))
    lifecycle_summary = UsageSummary.of(lifecycle, lifecycle_arm)

    cumulative: list[CumulativeRow] = []
    incident = learning = 0
    codex_total: float | None = 0.0
    sdo_time: float | None = 0.0
    codex_time: float | None = 0.0
    incident_weighted: float | None = 0.0
    learning_weighted: float | None = 0.0
    codex_weighted: float | None = 0.0
    incident_usd: float | None = 0.0
    codex_usd: float | None = 0.0
    for stage in stages:
        responder = stage.responder_summary
        reflection = stage.reflection_summary
        if responder is None or reflection is None:
            raise IncidentCostError(f"stage {stage.index} was not priced")
        incident += stage.responder.total(uncached=uncached)
        learning += stage.responder.total(uncached=uncached) + stage.reflection.total(uncached=uncached)
        cost = costs.get(stage.problem_id)
        summary = cost.mean_summary if cost else None
        codex_total = codex_total + cost.mean_tokens if codex_total is not None and cost else None
        sdo_time = _running(sdo_time, stage.verdict.ttm_seconds)
        codex_time = _running(codex_time, cost.mean_ttm_seconds if cost else None)
        incident_weighted = _running(incident_weighted, responder.weighted_tokens)
        learning_weighted = _running(_running(learning_weighted, responder.weighted_tokens), reflection.weighted_tokens)
        codex_weighted = _running(codex_weighted, summary.weighted_tokens if summary else None)
        incident_usd = _running(incident_usd, responder.usd)
        codex_usd = _running(codex_usd, summary.usd if summary else None)
        cumulative.append(
            CumulativeRow(
                stage.index,
                incident,
                learning,
                codex_total,
                sdo_time,
                codex_time,
                sdo_incident_weighted=incident_weighted,
                sdo_weighted_with_learning=learning_weighted,
                codex_weighted=codex_weighted,
                sdo_incident_usd=incident_usd,
                codex_usd=codex_usd,
            )
        )

    codex_series = [row.codex_tokens for row in cumulative]
    incident_series = [row.sdo_incident_tokens for row in cumulative]
    learning_series = [row.sdo_tokens_with_learning for row in cumulative]
    break_even = [
        _break_even("incident tokens (responder)", incident_series, codex_series, 0, stages),
        _break_even("total tokens incl. learning", learning_series, codex_series, 0, stages),
    ]
    if lifecycle_tokens:
        break_even += [
            _break_even("incident tokens (responder)", incident_series, codex_series, lifecycle_tokens, stages),
            _break_even("total tokens incl. learning", learning_series, codex_series, lifecycle_tokens, stages),
        ]
    weighted_incident = [row.sdo_incident_weighted for row in cumulative]
    weighted_learning = [row.sdo_weighted_with_learning for row in cumulative]
    if cumulative and all(value is not None for value in weighted_incident + weighted_learning):
        weighted_codex = [row.codex_weighted for row in cumulative]
        break_even += [
            _break_even("weighted incident tokens", weighted_incident, weighted_codex, 0, stages),
            _break_even("weighted tokens incl. learning", weighted_learning, weighted_codex, 0, stages),
        ]
        lifecycle_weighted = lifecycle_summary.weighted_tokens
        if lifecycle_tokens and lifecycle_weighted:
            break_even += [
                _break_even("weighted incident tokens", weighted_incident, weighted_codex, lifecycle_weighted, stages),
                _break_even(
                    "weighted tokens incl. learning", weighted_learning, weighted_codex, lifecycle_weighted, stages
                ),
            ]
    info = PricingInfo(
        table_source=pricing.table_source,
        table_version=pricing.table.version,
        table_last_updated=pricing.table.last_updated.isoformat(),
        weight_overrides=dict(pricing.weight_overrides),
        arms=arms,
    )
    return Report(stages, costs, cumulative, lifecycle_tokens, break_even, uncached, info, lifecycle_summary)


def _fmt_weight(value: float | None) -> str:
    return "-" if value is None else f"{value:g}"


def _describe_arm(arm: ArmPricing) -> str:
    """One line naming an arm's weights (x base input) and USD source."""

    if arm.weights is None:
        missing = ", ".join(f"{name}=..." for name in _REQUIRED_WEIGHTS)
        return f"no price for {arm.label} in the table; raw tokens only (pass --weight {missing} to weight them)"
    weights = arm.weights
    write_1h = "= cache_write" if weights.cache_write_1h_input is None else _fmt_weight(weights.cache_write_1h_input)
    reasoning = "= output" if weights.reasoning_output is None else _fmt_weight(weights.reasoning_output)
    text = (
        f"weights x base input: uncached {_fmt_weight(weights.uncached_input)}, "
        f"cache_read {_fmt_weight(weights.cache_read_input)}, "
        f"cache_write {_fmt_weight(weights.cache_write_input)}, "
        f"cache_write_1h {write_1h}, "
        f"output {_fmt_weight(weights.output)}, "
        f"reasoning {reasoning}"
    )
    if arm.overridden:
        text += f" (CLI override: {', '.join(arm.overridden)})"
    if arm.entry is None:
        return text + f"; no USD price for {arm.label}"
    entry = arm.entry
    estimated = " [estimated]" if entry.estimated else ""
    return (
        text + f"; USD/MTok input {entry.input_usd:g} ({entry.vendor}/{entry.model}{estimated}, "
        f"{entry.source}, checked {entry.checked.isoformat()})"
    )


# --------------------------------------------------------------------------- rendering


def _fmt_num(value: float | None, digits: int = 0) -> str:
    if value is None:
        return "-"
    return f"{value:,.{digits}f}"


def _fmt_bool(value: bool | None) -> str:
    return "-" if value is None else ("yes" if value else "no")


def _fmt_verdict(verdict: Verdict) -> str:
    if verdict.diagnosis is None and verdict.mitigation is None:
        return "-"
    letters = "".join(
        f"{label}{'+' if value else '-' if value is False else '?'}"
        for label, value in (("D", verdict.diagnosis), ("M", verdict.mitigation))
    )
    return f"{'PASS' if verdict.passed else 'FAIL'} {letters}"


def _table(headers: list[str], rows: list[list[str]]) -> str:
    widths = [
        max(len(header), *(len(row[i]) for row in rows)) if rows else len(header) for i, header in enumerate(headers)
    ]
    lines = ["  ".join(header.ljust(width) for header, width in zip(headers, widths, strict=True))]
    lines.append("  ".join("-" * width for width in widths))
    lines += ["  ".join(cell.ljust(width) for cell, width in zip(row, widths, strict=True)) for row in rows]
    return "\n".join(lines)


def _breakdown_row(label: list[str], summary: UsageSummary | None) -> list[str]:
    if summary is None:
        return [*label, *["-"] * 10]
    return [
        *label,
        _fmt_num(summary.uncached_input_tokens),
        _fmt_num(summary.cache_read_input_tokens),
        _fmt_num(summary.cache_write_input_tokens),
        _fmt_num(summary.output_tokens),
        _fmt_num(summary.reasoning_output_tokens),
        _fmt_num(summary.raw_tokens),
        _fmt_num(summary.weighted_tokens),
        "-" if summary.usd is None else f"{summary.usd:.4f}",
        _fmt_num(summary.requests, 1 if summary.requests is not None and summary.requests % 1 else 0),
        _fmt_num(summary.cache_write_1h_input_tokens),
    ]


_BREAKDOWN_HEADERS = [
    "uncached_in",
    "cache_read",
    "cache_write",
    "output",
    "reasoning",
    "raw_tok",
    "weighted_tok",
    "usd",
    "requests",
    "cache_write_1h",
]


def render_pricing(info: PricingInfo | None) -> list[str]:
    if info is None:
        return []
    lines = [f"prices: {info.table_source} {info.table_version}, last updated {info.table_last_updated}"]
    if info.weight_overrides:
        overrides = ", ".join(f"{name}={value:g}" for name, value in sorted(info.weight_overrides.items()))
        lines.append(f"weight override (x base input, from the CLI): {overrides}; USD still uses the table")
    lines += [f"  {arm}: {text}" for arm, text in sorted(info.arms.items())]
    lines.append(
        "  weighted_tok = sum of each token class x its weight, in base-input-token units (an assumption about"
        " relative prices, not a bill); raw_tok = input + output"
    )
    return lines


def render_breakdown(report: Report) -> str:
    rows = []
    for stage in report.stages:
        rows.append(_breakdown_row(["sdo", str(stage.index), stage.problem_id, "responder"], stage.responder_summary))
        rows.append(_breakdown_row(["sdo", str(stage.index), stage.problem_id, "reflection"], stage.reflection_summary))
    rows.extend(
        _breakdown_row(["baseline", "-", cost.problem_id, f"mean of {cost.runs}"], cost.mean_summary)
        for cost in sorted(report.codex.values(), key=lambda item: item.problem_id)
    )
    if report.lifecycle_tokens and report.lifecycle_summary is not None:
        rows.append(_breakdown_row(["sdo", "-", "-", "lifecycle (one-time)"], report.lifecycle_summary))
    return _table(["arm", "#", "problem", "part", *_BREAKDOWN_HEADERS], rows)


def render_excluded(excluded: list[str]) -> str:
    if not excluded:
        return ""
    lines = [f"Excluded runs (invalid_infra, {len(excluded)}):"]
    lines.extend(f"  {line}" for line in excluded)
    return "\n".join(lines) + "\n\n"


def render(report: Report) -> str:
    uncached = report.uncached
    unit = "uncached tokens" if uncached else "tokens"
    stage_rows = []
    for stage in report.stages:
        memory = stage.memory
        stage_rows.append(
            [
                str(stage.index),
                stage.name,
                stage.problem_id,
                _fmt_verdict(stage.verdict),
                _fmt_num(stage.verdict.diagnosis_seconds, 1),
                _fmt_num(stage.verdict.ttm_seconds, 1),
                _fmt_num(stage.verdict.raw_incl_judge_seconds, 1),
                _fmt_num(stage.verdict.judge_excluded_seconds, 1),
                _fmt_num(stage.verdict.mitigation_applied_seconds, 1),
                _fmt_num(stage.verdict.last_mitigation_seconds, 1),
                stage.verdict.floor_source or "-",
                _fmt_num(stage.incident_resolution_seconds, 1),
                _fmt_num(stage.responder.total(uncached=uncached)),
                _fmt_num(stage.reflection.total(uncached=uncached)),
                "skipped" if stage.reflection_skipped_reason else _fmt_num(stage.reflection_attempts),
                _fmt_num(stage.learning_seconds, 1),
                _fmt_num(stage.reflection_turn_seconds, 1),
                _fmt_bool(stage.warm_path),
                _fmt_bool(stage.warm_prompt),
                f"{memory.detectors}({memory.incident_detectors})/{memory.playbooks}" if memory else "-",
            ]
        )
    sections = [
        *render_pricing(report.pricing),
        "",
        f"SDO stages ({unit}; ttm_s is the headline and excludes judge time; no time includes reflection)",
        _table(
            [
                "#",
                "stage",
                "problem",
                "oracles",
                "ttd_s",
                "ttm_s",
                "raw_incl_judge_s",
                "no_judge_s",
                "first_mut_s",
                "last_mut_s",
                "floor_source",
                "resolution_s",
                "responder_tok",
                "reflection_tok",
                "refl",
                "learning_s",
                "refl_turn_s",
                "warm_path",
                "warm_prompt",
                "det(inc)/pb",
            ],
            stage_rows,
        ),
        "",
        "Codex baseline (mean per problem)",
        _table(
            [
                "problem",
                "runs",
                "passed",
                "mean_tok",
                "mean_ttd_s",
                "mean_ttm_s",
                "mean_raw_incl_judge_s",
                "mean_no_judge_s",
                "mean_first_mut_s",
                "mean_last_mut_s",
            ],
            [
                [
                    cost.problem_id,
                    str(cost.runs),
                    str(cost.passed),
                    _fmt_num(cost.mean_tokens),
                    _fmt_num(cost.mean_ttd_seconds, 1),
                    _fmt_num(cost.mean_ttm_seconds, 1),
                    _fmt_num(cost.mean_raw_incl_judge_seconds, 1),
                    _fmt_num(cost.mean_judge_excluded_seconds, 1),
                    _fmt_num(cost.mean_mitigation_applied_seconds, 1),
                    _fmt_num(cost.mean_last_mitigation_seconds, 1),
                ]
                for cost in sorted(report.codex.values(), key=lambda item: item.problem_id)
            ],
        ),
        "",
        "Token breakdown (cache reads and writes are part of input; reasoning is part of output; requests are model"
        " requests; baseline rows are means per run)",
        render_breakdown(report),
        "",
        "Cumulative",
        _table(
            [
                "#",
                "sdo_incident_tok",
                "sdo_total_tok_incl_learning",
                "codex_tok",
                "sdo_incident_wtok",
                "sdo_total_wtok_incl_learning",
                "codex_wtok",
                "sdo_incident_usd",
                "codex_usd",
                "sdo_ttm_s",
                "codex_ttm_s",
            ],
            [
                [
                    str(row.index),
                    _fmt_num(row.sdo_incident_tokens),
                    _fmt_num(row.sdo_tokens_with_learning),
                    _fmt_num(row.codex_tokens),
                    _fmt_num(row.sdo_incident_weighted),
                    _fmt_num(row.sdo_weighted_with_learning),
                    _fmt_num(row.codex_weighted),
                    "-" if row.sdo_incident_usd is None else f"{row.sdo_incident_usd:.4f}",
                    "-" if row.codex_usd is None else f"{row.codex_usd:.4f}",
                    _fmt_num(row.sdo_ttm_seconds, 1),
                    _fmt_num(row.codex_ttm_seconds, 1),
                ]
                for row in report.cumulative
            ],
        ),
        f"One-time SDO lifecycle: {_fmt_num(report.lifecycle_tokens)} {unit}"
        + ("" if report.lifecycle_tokens else " (no sdo_turn_usage.jsonl found; pass --lifecycle-usage)"),
        "",
        "Break-even against Codex (first stage where cumulative SDO <= cumulative Codex)",
    ]
    for item in report.break_even:
        scope = "with lifecycle" if item.includes_lifecycle else "without lifecycle"
        if item.stage is not None:
            outcome = f"stage {item.stage}"
        else:
            outcome = f"not reached (gap {_fmt_num(item.final_gap)})"
            if item.projected_extra_repeats is not None:
                outcome += f"; ~{item.projected_extra_repeats} more repeat incidents at the observed repeat saving"
        sections.append(f"  {item.measure}, {scope}: {outcome}")
    return "\n".join(sections)


def _report_json(report: Report) -> dict[str, Any]:
    document = asdict(report)
    for stage, row in zip(report.stages, document["stages"], strict=True):
        row["verdict"]["judge_excluded_seconds"] = stage.verdict.judge_excluded_seconds
        row["verdict"]["ttm_seconds"] = stage.verdict.ttm_seconds
        row["verdict"]["floor_source"] = stage.verdict.floor_source
    return document


def _model_arg(value: str) -> tuple[str, str]:
    provider, sep, model = value.partition(":")
    if not sep or not provider or not model:
        raise argparse.ArgumentTypeError(f"expected PROVIDER:MODEL, got {value!r}")
    return provider, model


def _pricing_context(table_path: Path | None, weights: list[str]) -> PricingContext:
    """The price table (agentshim's, or a JSON file) and parsed ``--weight`` overrides."""

    overrides: dict[str, float] = {}
    for item in weights:
        name, sep, raw = item.partition("=")
        if not sep or name not in WEIGHT_NAMES:
            raise IncidentCostError(
                f"--weight expects CLASS=MULTIPLE with CLASS in {sorted(WEIGHT_NAMES)}, got {item!r}"
            )
        value = float(raw)
        if not math.isfinite(value) or value < 0:
            raise IncidentCostError(f"--weight {name} must be finite and non-negative")
        overrides[name] = value
    if table_path is None:
        return PricingContext(default_pricing(), weight_overrides=overrides)
    table = PricingTable.from_dict(json.loads(table_path.read_text(encoding="utf-8")))
    return PricingContext(table, table_source=f"pricing table {table_path}", weight_overrides=overrides)


def exclude_invalid_runs(
    sdo_pipeline: Path,
    codex_dirs: list[Path],
    stages: list[SdoStage],
    codex_runs: list[CodexRun],
    *,
    legacy: bool,
) -> tuple[list[SdoStage], list[CodexRun], list[str]]:
    """Drop the stages and baseline runs the validity checker classifies ``invalid_infra``.

    ``agent_failure`` runs stay: they are real outcomes and count as failures.
    Returns the kept stages and runs, and one line per excluded run with its reasons.
    """

    from benchmarks.sregym.analysis.run_validity import ValidityPolicy, validity_by_results_dir

    validity = validity_by_results_dir([sdo_pipeline, *codex_dirs], policy=ValidityPolicy(legacy=legacy))
    excluded: list[str] = []

    def keep(source: str, label: str) -> bool:
        result = validity.get(source)
        if result is None or not result.excluded:
            return True
        excluded.append(f"{label} ({source}): {'; '.join(result.reasons)}")
        return False

    kept_stages = [stage for stage in stages if keep(stage.source, f"SDO stage {stage.index} {stage.name}")]
    kept_runs = [run for run in codex_runs if keep(run.source, f"baseline {run.problem_id}")]
    return kept_stages, kept_runs, excluded


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    parser.add_argument("sdo_pipeline", type=Path, help="SDO pipeline log directory")
    parser.add_argument("--codex", type=Path, nargs="*", default=[], help="Codex run directories")
    parser.add_argument(
        "--lifecycle-usage",
        type=Path,
        nargs="*",
        default=None,
        help="sdo_turn_usage.jsonl files for the one-time lifecycle (overrides files found in the pipeline)",
    )
    parser.add_argument("--uncached", action="store_true", help="exclude cache reads from the raw token totals")
    parser.add_argument(
        "--pricing-table",
        type=Path,
        help="JSON pricing table (agentshim PricingTable.to_dict() shape) to use instead of agentshim's",
    )
    parser.add_argument(
        "--weight",
        action="append",
        default=[],
        metavar="CLASS=MULTIPLE",
        help=(
            "override one token-class weight, as a multiple of the base input rate "
            f"(classes: {', '.join(WEIGHT_NAMES)}); repeatable. USD costs still use the table"
        ),
    )
    parser.add_argument(
        "--sdo-model", type=_model_arg, help="PROVIDER:MODEL the SDO arm ran (default: its experiment_config.toml)"
    )
    parser.add_argument(
        "--codex-model",
        type=_model_arg,
        help="PROVIDER:MODEL the baseline arm ran (default: its experiment_config.toml)",
    )
    parser.add_argument("--json", type=Path, help="also write the report as JSON")
    parser.add_argument(
        "--legacy-runs",
        action="store_true",
        help="accept runs from before the run manifest and the lane isolation guard (run_validity --legacy)",
    )
    parser.add_argument(
        "--include-invalid",
        action="store_true",
        help="report invalid_infra runs too instead of excluding them (the default excludes them and says why)",
    )
    args = parser.parse_args(argv)

    try:
        pricing = _pricing_context(args.pricing_table, args.weight)
        stages = load_sdo_pipeline(args.sdo_pipeline)
        codex_runs = [run for directory in args.codex for run in load_codex_runs(directory)]
    except (IncidentCostError, ValueError, TypeError, OSError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    excluded: list[str] = []
    if not args.include_invalid:
        stages, codex_runs, excluded = exclude_invalid_runs(
            args.sdo_pipeline, args.codex, stages, codex_runs, legacy=args.legacy_runs
        )
        for line in excluded:
            print(f"excluded: {line}", file=sys.stderr)
        if not stages:
            print(
                "error: every SDO stage is invalid_infra (see the exclusions above); "
                "pass --legacy-runs for runs from before 2026-09-28, or --include-invalid to report them anyway",
                file=sys.stderr,
            )
            return 2
    lifecycle = _sum_usage_jsonl(args.lifecycle_usage)[0] if args.lifecycle_usage else None
    report = build_report(
        stages,
        codex_runs,
        lifecycle_override=lifecycle,
        uncached=args.uncached,
        pricing=pricing,
        sdo_model=args.sdo_model,
        codex_model=args.codex_model,
    )
    report.excluded = excluded
    print(render_excluded(excluded) + render(report))
    if args.json:
        args.json.write_text(json.dumps(_report_json(report), indent=2), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
