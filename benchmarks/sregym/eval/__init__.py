"""SREGym evaluation runner: SDO against a vanilla agent, cell by cell (benchmark harness only).

Production code never imports this package. It reaches SDO and SREGym only through the interfaces it defines.

A cell's row ends when its outcome is committed and graded (``row.json``). The same process then drains the closing
turn, the end-of-row fault recovery and the exit on the still-held lane and writes ``closing.json`` (offline tokens and
USD, ``closing_s``, ``drain_s``, ``lane_occupancy_s``, the learning delta, the closing outcome); the lane is released
only after that, and a merge barrier reads ``closing.json`` (``closing.pending_closings``). Both arms run the same
end-of-row recovery (``--end-of-row-recovery``), and the table carries ``row_wall_s``, ``lane_occupancy_s``,
``closing_s``, ``recovery_s`` and the offline tokens beside the online ones.
"""

from benchmarks.sregym.eval.cell import (
    Accountant,
    Accounting,
    ArmContext,
    ArmExecutor,
    ArmResult,
    CellContext,
    ClosingDrain,
    EvidenceGate,
    GateVerdict,
    Injection,
    Lane,
    LaneBackend,
    NoPageHook,
    PageHook,
    Verdict,
    run_cell,
)
from benchmarks.sregym.eval.closing import ClosingResult, closing_is_terminal, pending_closings, read_closing
from benchmarks.sregym.eval.health import (
    DEFAULT_HOLD_SECONDS,
    HealthAssessment,
    Prober,
    ProbeResult,
    ProbeSample,
    assess_health,
    http_probe,
)
from benchmarks.sregym.eval.plan import AppConfig, Cell, EvalConfig, Plan, build_plan
from benchmarks.sregym.eval.results import ResultRow, ResultsTable, TokenRow, verify_manifest, write_manifest
from benchmarks.sregym.eval.scheduler import LoadGate, Scheduler, SnapshotStore
from benchmarks.sregym.eval.timings import PHASES, TIMINGS_SCHEMA, PhaseTimings

__all__ = [
    "Accountant",
    "Accounting",
    "AppConfig",
    "ArmContext",
    "ArmExecutor",
    "ArmResult",
    "Cell",
    "CellContext",
    "ClosingDrain",
    "ClosingResult",
    "EvalConfig",
    "EvidenceGate",
    "GateVerdict",
    "Injection",
    "Lane",
    "LaneBackend",
    "LoadGate",
    "NoPageHook",
    "PageHook",
    "Plan",
    "ResultRow",
    "ResultsTable",
    "Scheduler",
    "SnapshotStore",
    "TokenRow",
    "Verdict",
    "build_plan",
    "closing_is_terminal",
    "pending_closings",
    "read_closing",
    "run_cell",
    "verify_manifest",
    "write_manifest",
    "DEFAULT_HOLD_SECONDS",
    "PHASES",
    "TIMINGS_SCHEMA",
    "HealthAssessment",
    "PhaseTimings",
    "ProbeResult",
    "ProbeSample",
    "Prober",
    "assess_health",
    "http_probe",
]
