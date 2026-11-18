"""SREGym evaluation runner: SDO against a vanilla agent, cell by cell (benchmark harness only).

Production code never imports this package. It reaches SDO and SREGym only through the interfaces it defines.
"""

from benchmarks.sregym.eval.health import (
    DEFAULT_HOLD_SECONDS,
    HealthAssessment,
    Prober,
    ProbeResult,
    ProbeSample,
    assess_health,
    http_probe,
)
from benchmarks.sregym.eval.timings import PHASES, TIMINGS_SCHEMA, PhaseTimings

__all__ = [
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
