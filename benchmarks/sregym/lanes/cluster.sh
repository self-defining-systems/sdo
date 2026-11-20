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
# SREGYM_LANES_PACKS maps lanes to app packs ("sgl1=benchmarks/.../hotel_reservation sgl3=..."): one host serves lanes
# of several apps; SREGYM_LANES_PACK is the pack of every other lane.
pack=${SREGYM_LANES_PACK:-}
for entry in ${SREGYM_LANES_PACKS:-}; do
  [ "${entry%%=*}" = "$2" ] && pack=${entry#*=}
done
BENCH_PACK=$root/${pack:?SREGym lanes need SREGYM_LANES_PACKS or SREGYM_LANES_PACK (an app pack path in the checkout)}
# SREGYM_LANES_NODE_SLICE overrides the nodes' cgroup parent. On a reserved host the step itself runs in the lane's
# slice, and cgroup v2 refuses a docker scope beneath a cgroup that holds processes ("Couldn't move process ...
# Operation not supported"), so such a host names lanes.slice here; the shim still pins the nodes to the lane's cores.
FUZZOPS_LANE_SLICE=${SREGYM_LANES_NODE_SLICE:-${FUZZOPS_LANE_SLICE:-}}
export FUZZOPS_LANE_SLICE
WORKLOAD_ACTION=up
WORKLOAD_ARGS='[]'
SREGYM_LANES_CLUSTER_UP=1
export PATH BENCH_LANE KUBECONFIG BENCH_EVIDENCE_HOST BENCH_PACK WORKLOAD_ACTION WORKLOAD_ARGS SREGYM_LANES_CLUSTER_UP
exec "${SREGYM_LANES_PYTHON:-.venv/bin/python}" -m benchmarks.sregym.lanes.workload app-up
