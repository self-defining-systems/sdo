"""SREGym evaluation runner: SDO against a vanilla agent, cell by cell (benchmark harness only).

Production code never imports this package. It reaches SDO and SREGym only through the interfaces it defines.
"""

from benchmarks.sregym.eval.cell import (
    Accountant,
    Accounting,
    ArmContext,
    ArmExecutor,
    ArmResult,
    CellContext,
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
