#!/bin/sh
# The lanes adapter command: `run.sh workload <verb>` or `run.sh subject <verb>`, run in the pinned SDO checkout.
# The checkout's own virtualenv runs the adapter (SREGYM_LANES_PYTHON overrides it).
set -eu
role=$1
shift
cd "$(dirname "$0")/../../.."
exec "${SREGYM_LANES_PYTHON:-.venv/bin/python}" -m "benchmarks.sregym.lanes.$role" "$@"
