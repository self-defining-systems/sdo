"""Per-incident table for a detector-generalization pipeline (SDO arm).

Joins :func:`incident_cost.load_sdo_pipeline` with the learned detectors and
playbooks present in each stage's chained workspace, for example::

    uv run python -m benchmarks.sregym.analysis.detgen_table third_party/sregym/logs/<pipeline>
"""

from __future__ import annotations

import sys
from pathlib import Path

from benchmarks.sregym.analysis.incident_cost import load_sdo_pipeline, pipeline_stage_dirs


def _count(workspace: Path, pattern: str) -> int:
    return len(list(workspace.glob(pattern)))


def main(argv: list[str] | None = None) -> int:
    pipeline = Path((argv or sys.argv[1:])[0])
    dirs = {index: path for index, _name, path in pipeline_stage_dirs(pipeline)}
    print(
        "idx problem solved ttd_s ttm_s resp_in resp_out refl_in refl_out refl_attempts warm det_count(next stage seed)"
    )
    for stage in load_sdo_pipeline(pipeline):
        workspace = dirs[stage.index] / "application_workspace" / ".sdo"
        nxt = dirs.get(stage.index + 1)
        seed = (nxt / "application_workspace" / ".sdo") if nxt else workspace
        print(
            stage.index,
            stage.problem_id,
            stage.verdict.passed,
            stage.verdict.diagnosis_seconds and round(stage.verdict.diagnosis_seconds),
            stage.verdict.ttm_seconds and round(stage.verdict.ttm_seconds),
            stage.responder.input_tokens,
            stage.responder.output_tokens,
            stage.reflection.input_tokens,
            stage.reflection.output_tokens,
            stage.reflection_attempts,
            stage.warm_path,
            _count(seed, "diagnostics/detectors/learned/*"),
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
