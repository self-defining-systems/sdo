#!/bin/sh
# The lanes host's cluster_up for SREGym lanes: `cluster.sh up CLUSTER KUBECONFIG [EVIDENCE]`.
# It runs the workload's `app-up up` (fastloop up: the kind cluster, the app of $SREGYM_LANES_PACK and SDO) with
# the lane docker shim first in PATH, so the kind nodes start under the lane's cgroup slice, cpuset and memory
# (FUZZOPS_LANE_* from the platform). A later `app-up up` of the build finds the lane and reuses it.
set -eu
[ "${1:-}" = up ] || { echo "usage: cluster.sh up CLUSTER KUBECONFIG [EVIDENCE]" >&2; exit 2; }
cd "$(dirname "$0")/../../.."
root=$(pwd)
PATH=$root/benchmarks/lanes/deploy/lane-docker-shim:$PATH
BENCH_LANE=$2
KUBECONFIG=$3
BENCH_EVIDENCE_HOST=${4:-}
BENCH_PACK=$root/${SREGYM_LANES_PACK:?SREGym lanes need SREGYM_LANES_PACK (an app pack path in the checkout)}
WORKLOAD_ACTION=up
WORKLOAD_ARGS='[]'
export PATH BENCH_LANE KUBECONFIG BENCH_EVIDENCE_HOST BENCH_PACK WORKLOAD_ACTION WORKLOAD_ARGS
exec "${SREGYM_LANES_PYTHON:-.venv/bin/python}" -m benchmarks.sregym.lanes.workload app-up
