#!/usr/bin/env bash
# Per-lane CPU and memory limits of a kind lane's node containers (docs/bench.md, "Lane limits").
#
#   FUZZOPS_LANE_CPUSET=8-11 FUZZOPS_LANE_MEMORY=BYTES deploy/lane-limits.sh apply NAME   # docker update every node of lane NAME (no restart)
#   FUZZOPS_LANE_CPUSET=8-11 FUZZOPS_LANE_MEMORY=BYTES deploy/lane-limits.sh flags NODE   # the docker run/update flags for one node
#
# The bench sets the variables (its allocator picked the cores); with neither set both verbs do nothing.
# FUZZOPS_LANE_MEMORY is the lane's memory in TOTAL, split over its nodes so that the shares add up to it: the control-plane
# node(s) 15 % together, the workers the other 85 % between them (a lane without workers: the control-plane gets all);
# a node that is neither (a load balancer) gets no memory limit. FUZZOPS_LANE_WORKERS and FUZZOPS_LANE_CONTROL_PLANES
# (default 2 and 1; `apply` counts the real nodes) say how many there are. Swap is the same as memory, i.e. none. A node
# that hits its limit has its biggest process killed by the kernel (the bench counts those: memory.events oom_kill, and
# records them in the run), so the limits are ceilings that should not be reached, not throttles. docker update changes a
# running container's limits in place and keeps them across a restart; `kind create` gets them from
# lane-docker-shim/docker, together with the parent slice, which can only be chosen when a container is created.
set -euo pipefail

HELPER=${FUZZOPS_EVAL_HELPER:-/usr/local/sbin/sdo-eval-reserve}   # by its absolute path: the one the sudoers rule names (a non-login ssh PATH has no sbin)

flags() {
  local node=$1 m= workers=${FUZZOPS_LANE_WORKERS:-2} cps=${FUZZOPS_LANE_CONTROL_PLANES:-1}
  [ -z "${FUZZOPS_LANE_CPUSET:-}" ] || printf -- '--cpuset-cpus=%s ' "$FUZZOPS_LANE_CPUSET"
  if [ -n "${FUZZOPS_LANE_MEMORY:-}" ]; then
    case $node in
    *-control-plane | *-control-plane[0-9]*)
      if [ "$workers" -gt 0 ]; then m=$((FUZZOPS_LANE_MEMORY * 15 / 100 / cps)); else m=$((FUZZOPS_LANE_MEMORY / cps)); fi ;;
    *-worker | *-worker[0-9]*) m=$((FUZZOPS_LANE_MEMORY * 85 / 100 / workers)) ;;
    esac
    if [ -n "$m" ]; then
      m=$((m / 4096 * 4096))   # whole pages: what the cgroup file holds
      printf -- '--memory=%s --memory-swap=%s' "$m" "$m"
    fi
  fi
  echo
}

# pin NODE: make systemd's AllowedCPUs of the node's scope agree with the cpuset docker just set. runc 1.1.x's systemd
# driver encodes that property with the wrong byte order (cpus 51-52 become 3-4, 38-42 become 0-2,14-15): docker's own
# files are right, until the next `systemctl daemon-reload` (an apt upgrade, a `systemctl enable`) makes systemd write the
# property into cpuset.cpus and the lane lands on other lanes' cores. Setting the property with systemd's encoder fixes
# it; that needs root, so it goes through the optional helper (`sudo -n $HELPER pin`), and without it we say so.
pin() {
  local id unit have
  id=$(docker inspect -f '{{.Id}}' "$1" 2>/dev/null) || return 0
  [[ $id =~ ^[0-9a-f]{64}$ ]] || return 0
  unit=docker-$id.scope
  have=$(systemctl show "$unit" -p AllowedCPUs --value 2>/dev/null | tr ' ' ',') || return 0
  [ -n "$have" ] && [ "$have" != "$FUZZOPS_LANE_CPUSET" ] || return 0
  if ! [ -x "$HELPER" ] || ! sudo -n "$HELPER" pin "$id" "$FUZZOPS_LANE_CPUSET" >/dev/null 2>&1; then
    echo "lane-limits: $1: systemd has AllowedCPUs=$have for the node (docker/runc encodes it wrongly), not $FUZZOPS_LANE_CPUSET: a daemon-reload on this host would move the lane; install deploy/host/sdo-eval-reserve ($HELPER) or run apply again after one" >&2
  fi
}

case "${1:-}" in
flags) flags "${2:?node}" ;;
apply)
  name=${2:?lane name}
  [ -n "${FUZZOPS_LANE_CPUSET:-}${FUZZOPS_LANE_MEMORY:-}" ] || exit 0
  nodes=$(kind get nodes --name "$name")
  FUZZOPS_LANE_WORKERS=$(grep -c -- '-worker' <<<"$nodes" || true)   # the real node counts, not the defaults
  FUZZOPS_LANE_CONTROL_PLANES=$(grep -c -- '-control-plane' <<<"$nodes" || true)
  export FUZZOPS_LANE_WORKERS FUZZOPS_LANE_CONTROL_PLANES
  for n in $nodes; do
    # shellcheck disable=SC2046
    docker update $(flags "$n") "$n" >/dev/null
    [ -z "${FUZZOPS_LANE_CPUSET:-}" ] || pin "$n"
  done
  echo "lane $name: limits applied (cpus ${FUZZOPS_LANE_CPUSET:-unchanged}, memory ${FUZZOPS_LANE_MEMORY:-unchanged})"
  ;;
*) echo "usage: lane-limits.sh apply NAME | flags NODE" >&2; exit 2 ;;
esac
