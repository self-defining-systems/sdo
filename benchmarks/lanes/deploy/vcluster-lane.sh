#!/usr/bin/env bash
# One lane as a virtual cluster (vcluster) on a shared host cluster; the provider `vcluster` of fuzzops-bench.
#
#   deploy/vcluster-lane.sh up NAME KUBECONFIG [EVIDENCE_DIR]   # create (idempotent: an existing lane gets its kubeconfig rewritten)
#   deploy/vcluster-lane.sh down NAME [KUBECONFIG]              # delete the virtual cluster and its host namespace
#   deploy/vcluster-lane.sh evidence-pull NAME NAMESPACE SELECTOR CONTAINER SRC DEST
#                                                               # copy a directory out of a pod of the lane (no hostPath)
#
# What `up` makes (each line is a failure of the first prototype, see docs/bench.md "Lane providers"):
#  - the host namespace NAME holds the virtual cluster's control plane (a StatefulSet pinned to a Ready node that is
#    not under pressure) and every pod of the lane; its API server is a NodePort Service on the shared cluster, and
#    KUBECONFIG is written from `vcluster connect --print --server https://ENTRY:PORT` with the context named
#    kind-NAME (fuzzops derives the context from the lane name). ENTRY is $FUZZOPS_ENTRY_HOST.
#  - a ResourceQuota of the lane with room for VCLUSTER_NODEPORTS (default 10) NodePorts and no limits.* keys, and a
#    LimitRange with defaultRequest only: a default limit OOM-kills or throttles the app (and a quota on limits.*
#    would reject pods without one).
#  - a host NetworkPolicy "allow-node-entry" letting each node's flannel address in: with isolation on, NodePort
#    traffic that crosses nodes arrives from that address and would be dropped (the entry flapped by a third).
#  - the vcluster's host ClusterRole may read nodes/proxy: an operator reads node usage (kubelet stats) through the
#    virtual API, which forwards it with that identity; without it a resource detector fails and holds an install.
#  - the virtual cluster admits pods at the pod security level VCLUSTER_POD_SECURITY (default privileged): an SDO
#    operator starts its agent workers with an unconfined seccomp profile, which `baseline` refuses (the worker Job
#    then never gets a pod and the incident stays open). Lanes run on shared nodes: use `baseline` for an operator
#    that needs no such pod.
#  - no hostPath: the lane has no node mounts. An operator keeps its evidence on a volume of its own and the
#    hooks copy it out with `evidence-pull` (BENCH_EVIDENCE_MODE=pull).
# Settings: VCLUSTER_HOST_KUBECONFIG, FUZZOPS_ENTRY_HOST (required); VCLUSTER_HOST_GROUP (docker), VCLUSTER_NODE (pin the
# control plane), VCLUSTER_AVOID_NODES (space-separated), VCLUSTER_VERSION (0.37.7), VCLUSTER_NODEPORTS (10),
# VCLUSTER_POD_SECURITY (privileged), VCLUSTER_QUOTA_CPU (14), VCLUSTER_QUOTA_MEMORY (30Gi), VCLUSTER_QUOTA_PODS (120), VCLUSTER_QUOTA_STORAGE (100Gi).
set -euo pipefail
cd "$(dirname "$0")"
. ./vcluster-lib.sh

VER=${VCLUSTER_VERSION:-0.37.7}
log() { echo "[vcluster-lane] $*" >&2; }

# pick_node: the node for the control plane: Ready for at least VCLUSTER_NODE_STABLE_MIN minutes (default 120: a node
# that flaps under someone else's load has just changed state), no memory/disk/pid pressure, not in
# VCLUSTER_AVOID_NODES, the fewest CPU millicores requested (no metrics server on the shared cluster).
# VCLUSTER_NODE wins.
pick_node() {
  if [ -n "${VCLUSTER_NODE:-}" ]; then echo "$VCLUSTER_NODE"; return; fi
  hk get nodes -o json | python3 -c '
import datetime, json, os, subprocess, sys
avoid = set(os.environ.get("VCLUSTER_AVOID_NODES", "").split())
best = None
for n in json.load(sys.stdin)["items"]:
    name = n["metadata"]["name"]
    c = {x["type"]: x["status"] for x in n["status"]["conditions"]}
    if name in avoid or c.get("Ready") != "True" or n["spec"].get("unschedulable"):
        continue
    since = next(x["lastTransitionTime"] for x in n["status"]["conditions"] if x["type"] == "Ready")
    age = (datetime.datetime.now(datetime.timezone.utc) - datetime.datetime.fromisoformat(since.replace("Z", "+00:00"))).total_seconds()
    if age < 60 * float(os.environ.get("VCLUSTER_NODE_STABLE_MIN", "120")):
        continue
    if any(c.get(k) == "True" for k in ("MemoryPressure", "DiskPressure", "PIDPressure")):
        continue
    if any(t.get("effect") in ("NoSchedule", "NoExecute") for t in n["spec"].get("taints") or []):
        continue
    out = subprocess.run(["kubectl", "--kubeconfig", os.environ["HK"], "get", "pods", "-A", "--field-selector", "spec.nodeName=" + name,
                          "-o", "jsonpath={range .items[*]}{range .spec.containers[*]}{.resources.requests.cpu}{\"\\n\"}{end}{end}"],
                         capture_output=True, text=True).stdout
    milli = sum(int(l[:-1]) if l.endswith("m") else int(float(l) * 1000) for l in out.split() if l)
    if best is None or milli < best[0]:
        best = (milli, name)
print(best[1] if best else "")'
}

# flannel_addresses: the address each node's pods network is reached from (the .0 of its podCIDR, flannel.1).
flannel_addresses() {
  hk get nodes -o jsonpath='{range .items[*]}{.spec.podCIDR}{"\n"}{end}' | awk -F'[./]' 'NF>=5 {printf "%s.%s.%s.%s\n", $1,$2,$3,$4}'
}

write_values() {
  local node=$1 f=$2
  cat >"$f" <<YAML
controlPlane:
  statefulSet:
    scheduling:
      nodeSelector:
        kubernetes.io/hostname: $node
  service:
    spec:
      type: NodePort
  proxy:
    extraSANs: [$FUZZOPS_ENTRY_HOST]
sync:
  fromHost:
    storageClasses:
      enabled: true
rbac:
  clusterRole:
    extraRules:
    - apiGroups: [""]
      resources: ["nodes/proxy", "nodes/stats"]
      verbs: ["get"]
policies:
  podSecurityStandard: ${VCLUSTER_POD_SECURITY:-privileged}
  networkPolicy:
    enabled: true
  resourceQuota:
    enabled: true
    quota:
      requests.cpu: "${VCLUSTER_QUOTA_CPU:-14}"
      requests.memory: ${VCLUSTER_QUOTA_MEMORY:-30Gi}
      requests.storage: ${VCLUSTER_QUOTA_STORAGE:-100Gi}
      requests.ephemeral-storage: null
      limits.cpu: null
      limits.memory: null
      limits.ephemeral-storage: null
      count/pods: "${VCLUSTER_QUOTA_PODS:-120}"
      count/services: "100"
      count/endpoints: "100"
      count/secrets: "300"
      count/configmaps: "300"
      count/persistentvolumeclaims: "40"
      services.nodeports: "${VCLUSTER_NODEPORTS:-10}"
  limitRange:
    enabled: true
    default: null
    defaultRequest:
      cpu: 50m
      memory: 64Mi
      ephemeral-storage: null   # the chart's 3Gi per container is counted by the scheduler against the lane's own nodes
YAML
}

# connect_server NAME: https://ENTRY:NODEPORT of the lane's API server.
connect_server() {
  local port
  port=$(hk -n "$1" get svc "$1" -o jsonpath='{.spec.ports[?(@.name=="https")].nodePort}')
  [ -n "$port" ] || { echo "service $1 in namespace $1 has no https nodePort" >&2; return 1; }
  echo "https://$FUZZOPS_ENTRY_HOST:$port"
}

write_kubeconfig() { # NAME OUT
  local name=$1 out=$2 server tmp
  server=$(connect_server "$name")
  mkdir -p "$(dirname "$out")"
  tmp="$out.tmp.$$"
  KUBECONFIG=$HK vcluster connect "$name" -n "$name" --print --server "$server" --background-proxy=false \
    --kube-config-context-name "kind-$name" >"$tmp"
  chmod 600 "$tmp" && mv "$tmp" "$out"
}

lane_ready() { # NAME KUBECONFIG: the virtual API server answers
  kubectl --kubeconfig "$2" --context "kind-$1" --request-timeout=10s get ns >/dev/null 2>&1
}

allow_node_entry() { # NAME
  local ips ip blocks=""
  ips=$(flannel_addresses)
  for ip in $ips; do blocks="$blocks
    - ipBlock: {cidr: $ip/32}"; done
  [ -n "$blocks" ] || { log "no node podCIDR found: node entry policy not written"; return 0; }
  hk apply -f - >/dev/null <<YAML
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata: {name: allow-node-entry, namespace: $1}
spec:
  podSelector: {}
  policyTypes: [Ingress]
  ingress:
  - from:$blocks
YAML
}

case "${1:-}" in
up)
  name=${2:?name} kc=${3:?kubeconfig}
  : "${FUZZOPS_ENTRY_HOST:?FUZZOPS_ENTRY_HOST (a node address of the shared cluster) is not set}"
  vc_host_kubeconfig
  if [ -n "${4:-}" ]; then mkdir -p "$4" && chmod 0777 "$4"; fi
  t0=$(date +%s)
  if hk -n "$name" get statefulset "$name" >/dev/null 2>&1; then
    log "lane $name exists: rewriting its kubeconfig"
  else
    node=$(pick_node)
    [ -n "$node" ] || { echo "no Ready node without pressure on the shared cluster" >&2; exit 1; }
    log "creating $name (control plane on $node)"
    vals=$(mktemp) && write_values "$node" "$vals"
    KUBECONFIG=$HK vcluster create "$name" -n "$name" --chart-version "$VER" --values "$vals" --connect=false \
      --background-proxy=false >&2
    rm -f "$vals"
  fi
  allow_node_entry "$name"
  for i in $(seq 1 90); do
    if write_kubeconfig "$name" "$kc" 2>/dev/null && lane_ready "$name" "$kc"; then break; fi
    [ "$i" = 90 ] && { echo "lane $name: API server not ready after 7.5 minutes" >&2; exit 1; }
    sleep 5
  done
  echo "lane cluster $name up on the shared cluster ($(( $(date +%s) - t0 )) s; entry $FUZZOPS_ENTRY_HOST)"
  ;;
down)
  name=${2:?name}
  vc_host_kubeconfig
  KUBECONFIG=$HK vcluster delete "$name" -n "$name" --delete-namespace >&2 || true
  hk delete ns "$name" --ignore-not-found --wait=true --timeout=300s >&2 || true
  [ -z "${3:-}" ] || rm -f "$3"
  ;;
evidence-pull)
  name=${2:?name} ns=${3:?namespace} sel=${4:?selector} container=${5:?container} src=${6:?src dir} dest=${7:?dest dir}
  kc=${KUBECONFIG:?KUBECONFIG of the lane}
  pod=$(kubectl --kubeconfig "$kc" --context "kind-$name" -n "$ns" get pods -l "$sel" --field-selector=status.phase=Running \
    -o jsonpath='{.items[0].metadata.name}')
  [ -n "$pod" ] || { echo "no running pod matches $sel in $ns" >&2; exit 1; }
  mkdir -p "$dest"
  kubectl --kubeconfig "$kc" --context "kind-$name" -n "$ns" exec "$pod" -c "$container" -- tar -C "$src" -cf - . | tar -C "$dest" -xf -
  ;;
*) echo "usage: vcluster-lane.sh up NAME KUBECONFIG [EVIDENCE_DIR] | down NAME [KUBECONFIG] | evidence-pull NAME NAMESPACE SELECTOR CONTAINER SRC DEST" >&2; exit 2 ;;
esac
