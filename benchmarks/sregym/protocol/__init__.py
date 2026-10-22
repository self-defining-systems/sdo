"""SREGym conductor and submission protocol helpers."""

from benchmarks.sregym.protocol._http import request_with_retry
from benchmarks.sregym.protocol.benchmark import BenchmarkResult, Oracle, Stage
from benchmarks.sregym.protocol.conductor import (
    get_api_base,
    get_app_info,
    get_current_stage,
    get_current_stage_sync,
    get_planned_stages,
    get_problem_id,
    poll_stage,
    poll_stage_sync,
    signal_cleanup,
    wait_for_stages_or_last_seen,
    wait_for_stages_or_last_seen_sync,
)
from benchmarks.sregym.protocol.mcp_client import submit_to_benchmark
from benchmarks.sregym.protocol.receipt import (
    RECEIPT_RESOLUTIONS,
    ProductionReceiptValidationError,
    receipt_resolution,
    validate_production_receipt,
)
from benchmarks.sregym.protocol.schema import MAX_DIAGNOSIS_CANDIDATES, READY_STAGES, TERMINAL_STAGES

__all__ = [
    "MAX_DIAGNOSIS_CANDIDATES",
    "READY_STAGES",
    "RECEIPT_RESOLUTIONS",
    "TERMINAL_STAGES",
    "BenchmarkResult",
    "Oracle",
    "ProductionReceiptValidationError",
    "Stage",
    "get_api_base",
    "get_app_info",
    "get_current_stage",
    "get_current_stage_sync",
    "get_planned_stages",
    "get_problem_id",
    "poll_stage",
    "poll_stage_sync",
    "receipt_resolution",
    "request_with_retry",
    "signal_cleanup",
    "submit_to_benchmark",
    "validate_production_receipt",
    "wait_for_stages_or_last_seen",
    "wait_for_stages_or_last_seen_sync",
]
