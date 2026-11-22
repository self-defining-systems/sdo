#!/usr/bin/env bash
# The local-kind variant of eks-vcluster.sh (EKS_LOCAL_KIND=1; host option eks.local_kind). Sourced by eks-vcluster.sh after its
# own functions: it redefines the ones that need AWS, so `up`, `down`, `exists` and the rest run unchanged on top of them.
#
# The host cluster is a kind cluster EKS_HOST_CLUSTER on this machine: one control plane and EKS_LOCAL_WORKERS (default 14)
# workers. The first worker is the system node (label sdo-role=system, tainted like the EKS system node group) that holds
# every vcluster's control plane. A lane claims EKS_NODES free workers (label and taint sdo-lane=<lane>, the labels and
# taint Karpenter's NodePool gives its nodes) and gives them back on `down`; the vcluster is the one eks-vcluster.sh makes
# (it syncs those nodes, the host's NetworkPolicy enforcement and the local-path StorageClass).
# Images: a registry container EKS_HOST_CLUSTER-registry (published on localhost:EKS_LOCAL_REGISTRY_PORT, default 5611) takes
# what the adapter pushes; every node's containerd resolves each upstream (docker.io, ghcr.io, quay.io, registry.k8s.io) to
# <registry>/v2/sdo-mirror/<upstream> first and falls back to the upstream, like the node agent does on EKS; docker.io pulls
# also go through a pull-through cache (EKS_HOST_CLUSTER-hubcache) so the lanes of one machine pull a public image once.
#
# What it cannot rehearse: ECR, the node agent, Karpenter, EBS and the VPC CNI policy agent. Everything of the lane itself
# (the vcluster, its facts, NetworkPolicy sync, node-bound claims, the app and the subject) is the same.
# Settings: EKS_LOCAL_WORKERS (14), EKS_LOCAL_REGISTRY_PORT (5611), KIND_NODE_IMAGE, EKS_LOCAL_UPSTREAMS (docker.io ghcr.io
# quay.io registry.k8s.io), EKS_LOCAL_NODE_CPUS and EKS_LOCAL_NODE_MEMORY (a docker limit on every node of the host cluster, for a
# machine others share: `--cpus` and `--memory` values; unset: none).

LOCAL_WORKERS=${EKS_LOCAL_WORKERS:-14}
REG_PORT=${EKS_LOCAL_REGISTRY_PORT:-5611}
REG_NAME=$HOST-registry
CACHE_NAME=$HOST-hubcache
UPSTREAMS=${EKS_LOCAL_UPSTREAMS:-${EKS_MIRROR_UPSTREAMS:-docker.io ghcr.io quay.io registry.k8s.io}}
MIRROR_PREFIX=${EKS_MIRROR_PREFIX:-sdo-mirror}
KIND_NET=kind

account() { echo local-kind; }

# host_state: 0 the host cluster exists, 1 it does not (kind lists it or not; a kind that cannot list says it cannot tell).
host_state() {
  local out
  out=$(kind get clusters 2>&1) || { log "cannot tell whether host cluster $HOST exists: ${out: -200}"; return 2; }
  if printf '%s\n' "$out" | grep -qx "$HOST"; then return 0; fi
  return 1
}
host_exists() { host_state; }

# a lane's nodes are workers of the host cluster: released synchronously (release_lane_nodes), no node pool to wait for
lane_pool_absent() { return 0; }

refresh_host_kubeconfig() {
  mkdir -p "$(dirname "$HK")"
  kind get kubeconfig --name "$HOST" >"$HK.tmp.$$" && chmod 600 "$HK.tmp.$$" && mv "$HK.tmp.$$" "$HK"
}

# The system node and a lane's nodes are kind workers: the manager reaches them on their docker network address.
node_address() {
  hk get nodes -l "$1" -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}'
}

scale_system() { :; } # the host cluster is always whole: nothing bills

wait_system() {
  local end=$((SECONDS + 300))
  until [ "$(hk get nodes -l sdo-role=system --no-headers 2>/dev/null | grep -c ' Ready')" -ge 1 ]; do
    [ $SECONDS -lt $end ] || die "no Ready system node after 5 minutes"
    sleep 3
  done
}

# lane_nodepool LANE: claim EKS_NODES free workers for the lane (what Karpenter's NodePool does on EKS).
lane_nodepool() {
  local lane=$1 have need free n
  mkdir -p "$(dirname "$HK")"
  exec 8>"$(dirname "$HK")/.claim.lock"
  flock 8
  have=$(hk get nodes -l "sdo-lane=$lane" --no-headers 2>/dev/null | grep -c . || true)
  need=$((NODES - have))
  if [ "$need" -gt 0 ]; then
    free=$(hk get nodes -l '!sdo-lane,!sdo-role,!node-role.kubernetes.io/control-plane' -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' | awk -v n="$need" 'NR <= n')
    [ "$(echo "$free" | grep -c .)" -ge "$need" ] || die "host $HOST has too few free worker nodes for lane $lane ($need needed): raise EKS_LOCAL_WORKERS"
    for n in $free; do
      hk label node "$n" "sdo-lane=$lane" --overwrite >/dev/null
      hk taint node "$n" "sdo-lane=$lane:NoSchedule" --overwrite >/dev/null
    done
  fi
  flock -u 8
}

# release_lane_nodes LANE: the lane's workers go back to the free pool, clean.
release_lane_nodes() {
  local n
  for n in $(hk get nodes -l "sdo-lane=$1" -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null); do
    hk taint node "$n" sdo-lane- >/dev/null 2>&1 || true
    hk label node "$n" sdo-lane- node-role.kubernetes.io/control-plane- node-role.kubernetes.io/worker- >/dev/null 2>&1 || true
  done
}

kind_nodes() { docker ps --filter "label=io.x-k8s.kind.cluster=$HOST" --format '{{.Names}}'; }

start_registry() { # NAME [ENV...]: a registry:2 container on the kind network (idempotent)
  local name=$1 publish=$2; shift 2
  if [ "$(docker inspect -f '{{.State.Running}}' "$name" 2>/dev/null)" = true ]; then return 0; fi
  docker rm -f "$name" >/dev/null 2>&1 || true
  docker run -d --restart=always --network "$KIND_NET" --name "$name" $publish "$@" registry:2 >/dev/null
}

# mirror_node NODE: containerd of the node resolves each upstream to the local registry first, then to the upstream.
mirror_node() {
  local node=$1 up server hosts
  for up in $UPSTREAMS; do
    case $up in docker.io) server=https://registry-1.docker.io;; *) server=https://$up;; esac
    hosts="server = \"$server\"

[host.\"http://$REG_NAME:5000/v2/$MIRROR_PREFIX/$up\"]
  capabilities = [\"pull\", \"resolve\"]
  override_path = true
"
    if [ "$up" = docker.io ]; then
      hosts="$hosts
[host.\"http://$CACHE_NAME:5000\"]
  capabilities = [\"pull\", \"resolve\"]
"
    fi
    printf '%s' "$hosts" | docker exec -i "$node" sh -c "mkdir -p /etc/containerd/certs.d/$up && cat > /etc/containerd/certs.d/$up/hosts.toml"
  done
  # the registry's own name (the pinned lane images are pulled as localhost:PORT/...)
  printf 'server = "http://%s:5000"\n\n[host."http://%s:5000"]\n  capabilities = ["pull", "resolve"]\n' "$REG_NAME" "$REG_NAME" |
    docker exec -i "$node" sh -c "mkdir -p /etc/containerd/certs.d/localhost:$REG_PORT && cat > /etc/containerd/certs.d/localhost:$REG_PORT/hosts.toml"
}

cmd_host_up() {
  need_host
  mkdir -p "$(dirname "$HK")"
  STAGE_FILE="$HK.stages"; : >"$STAGE_FILE"
  local created=0
  if host_exists; then
    log "host cluster $HOST exists: attaching"
  else
    local cfg i
    cfg=$(mktemp)
    {
      printf 'kind: Cluster\napiVersion: kind.x-k8s.io/v1alpha4\n'
      printf 'containerdConfigPatches:\n- |-\n  [plugins."io.containerd.grpc.v1.cri".registry]\n    config_path = "/etc/containerd/certs.d"\n'
      printf 'nodes:\n- role: control-plane\n'
      for i in $(seq 1 "$LOCAL_WORKERS"); do printf -- '- role: worker\n'; done
    } >"$cfg"
    log "creating kind host cluster $HOST ($LOCAL_WORKERS workers)"
    kind create cluster --name "$HOST" --config "$cfg" ${KIND_NODE_IMAGE:+--image "$KIND_NODE_IMAGE"} --wait 300s >&2 ||
      { rm -f "$cfg"; kind delete cluster --name "$HOST" >&2 || true; die "kind create cluster failed: $HOST"; }
    rm -f "$cfg"; created=1
    stage host_cluster_created
  fi
  refresh_host_kubeconfig
  hk wait --for=condition=Ready node --all --timeout=300s >&2
  # the system node: the first worker, labeled and tainted as the EKS system node group is
  local sys
  sys=$(hk get nodes -l sdo-role=system -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
  if [ -z "$sys" ]; then
    sys=$(hk get nodes -l '!node-role.kubernetes.io/control-plane' -o jsonpath='{.items[0].metadata.name}')
    hk label node "$sys" sdo-role=system --overwrite >/dev/null
    hk taint node "$sys" sdo-role=system:NoSchedule --overwrite >/dev/null
  fi
  stage system_node_ready
  # storage: local-path as the default class, as on an EKS host (kind's own `standard` stops being the default)
  sed -e '/- key: node.kubernetes.io\/disk-pressure/,/effect: NoSchedule/c\        - operator: Exists' "$here/eks/local-path-storage.yaml" | hk apply -f - >/dev/null
  hk -n local-path-storage patch deployment local-path-provisioner --type merge -p '{"spec":{"template":{"spec":{"tolerations":[{"operator":"Exists"}]}}}}' >/dev/null
  hk -n local-path-storage rollout status deployment/local-path-provisioner --timeout=180s >/dev/null || die "the local-path provisioner is not ready"
  hk annotate storageclass standard storageclass.kubernetes.io/is-default-class=false --overwrite >/dev/null 2>&1 || true
  # coredns and the vcluster add-ons may run on the system node as well
  hk -n kube-system patch deployment coredns --type merge -p '{"spec":{"template":{"spec":{"tolerations":[{"operator":"Exists"}]}}}}' >/dev/null 2>&1 || true
  stage storage_ready
  start_registry "$REG_NAME" "-p 127.0.0.1:$REG_PORT:5000"
  start_registry "$CACHE_NAME" "" -e REGISTRY_PROXY_REMOTEURL=https://registry-1.docker.io -v "$CACHE_NAME:/var/lib/registry"
  local node
  for node in $(kind_nodes); do
    mirror_node "$node"
    if [ -n "${EKS_LOCAL_NODE_CPUS:-}${EKS_LOCAL_NODE_MEMORY:-}" ]; then
      docker update ${EKS_LOCAL_NODE_CPUS:+--cpus "$EKS_LOCAL_NODE_CPUS"} ${EKS_LOCAL_NODE_MEMORY:+--memory "$EKS_LOCAL_NODE_MEMORY" --memory-swap "$EKS_LOCAL_NODE_MEMORY"} "$node" >/dev/null
    fi
  done
  stage registry_ready
  stage host_up_total
  echo "EKS_HOST_READY cluster=$HOST created=$created"
}

cmd_host_down() {
  need_host
  kind delete cluster --name "$HOST" >&2 || true
  docker rm -f "$REG_NAME" "$CACHE_NAME" >/dev/null 2>&1 || true
  rm -f "$HK" "$HK.stages"
  echo "EKS_HOST_DELETED cluster=$HOST"
}

cmd_prepull() { echo "EKS_PREPULL $(($# - 1)) images (local kind host: images are pulled on demand through the local registry and cache)"; }

cmd_nodes() { die "a local kind host has no node agent: a virtual lane cannot run node scripts"; }
