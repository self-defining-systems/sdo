#!/bin/bash
# Run an SREGym experiment.
#
# Usage:
#   bash scripts/run_sregym.sh                                     # new run (default: the SDO example config)
#   bash scripts/run_sregym.sh benchmarks/sregym/experiments/sdo_example.toml            # SDO example
#   bash scripts/run_sregym.sh benchmarks/sregym/experiments/codex_baseline_example.toml # Codex baseline example
#   bash scripts/run_sregym.sh third_party/sregym/logs/<exp-dir>/  # resume
#
# Environment variable overrides (e.g. MODEL=foo) are still supported.

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

if [ $# -eq 0 ]; then
  exec uv run python -m benchmarks.sregym.adapter.launcher -- uv run python -m benchmarks.sregym.run benchmarks/sregym/experiments/sdo_example.toml
else
  exec uv run python -m benchmarks.sregym.adapter.launcher -- uv run python -m benchmarks.sregym.run "$@"
fi
