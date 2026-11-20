#!/usr/bin/env bash
# One lane's kind cluster whose nodes pull images through the host's registry (registry.sh).
#
#   deploy/kind-lane.sh up NAME KUBECONFIG EVIDENCE_DIR   # create (idempotent; an existing cluster gets KUBECONFIG rewritten from kind)
#   (an adopted cluster keeps the evidence mount it was created with: EVIDENCE_DIR is made a link to that source)
#   deploy/kind-lane.sh down NAME                         # delete (EVIDENCE_DIR is kept)
#
# EVIDENCE_DIR (a host directory) is mounted on every node at /bench-evidence, where an operator's
# evidence sink writes, so its evidence is on the host while it runs and outlives the cluster.
# For fuzzops-bench hosts.yaml:  cluster_up: "deploy/kind-lane.sh up {cluster} {kubeconfig} {evidence}"
#                                cluster_down: "deploy/kind-lane.sh down {cluster}"
# Settings: KIND_WORKERS (default 2), KIND_NODE_IMAGE (default: kind's own), and those of registry.sh.
# Before it does anything else for a new cluster it checks, with a throwaway container of the node image (KIND_NODE_IMAGE, else kind's
# default), that the host kernel gives a node's iptables the mangle table and the comment match that kube-proxy needs, and refuses naming
# the one that is missing; a probe that fails for another reason (no image, docker down, a timeout of KIND_KERNEL_CHECK_TIMEOUT, default
# 60 s) only warns. KIND_SKIP_KERNEL_CHECK=1 skips it.
# KIND_NODE_PRELOAD=PACKDIR (needs KIND_NODE_IMAGE): the workers start with the pack's images
# (`PACKDIR/images.sh --list`) already in containerd, so their pods do not pull them. The node image is
# unchanged: each worker's /var/lib/containerd is a host directory cloned (hard links, seconds) from a template
# that node-preload.sh builds once per host, base image and image list. KIND_NODE_PRELOAD_BUILD=off: use only a
# template that exists. No template, or a failed build or clone: today's path, the workers pull. What was used is
# recorded in KUBECONFIG.node-image (JSON). `down` removes the lane's directories.
# FUZZOPS_LANE_CPUSET / FUZZOPS_LANE_MEMORY / FUZZOPS_LANE_SLICE (the bench sets them from hosts.yaml `lanes:` and the
# spec's `lane:`): the nodes are created under that parent cgroup, on those cores, within that memory
# (lane-docker-shim/docker, lane-limits.sh), and an existing cluster gets the same limits by docker update.
set -euo pipefail
ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)
# a relative KIND_NODE_PRELOAD is a pack directory under the fuzzops root, whatever the caller's cwd (this script runs from deploy/)
case "${KIND_NODE_PRELOAD:-}" in ""|/*) ;; *) KIND_NODE_PRELOAD="$ROOT_DIR/$KIND_NODE_PRELOAD" ;; esac
cd "$(dirname "$0")"
REG_NAME=${FUZZOPS_REGISTRY_NAME:-fuzzops-registry}
# preload_dirs: print "KEY DIR..." (one containerd directory per worker) of KIND_NODE_PRELOAD for lane $1, building
# the template when allowed; fail (so that the lane is made without) on anything missing; status 2 (an unusable root) stops the lane up.
preload_dirs() {
  local lane=$1 base=${KIND_NODE_IMAGE:-} imgs key i dirs=
  [ -n "$base" ] || { echo "KIND_NODE_PRELOAD needs KIND_NODE_IMAGE: no preload" >&2; return 1; }
  [ -x "$KIND_NODE_PRELOAD/images.sh" ] || { echo "preload: KIND_NODE_PRELOAD=$KIND_NODE_PRELOAD has no executable images.sh: no preload" >&2; return 1; }
  imgs=$("$KIND_NODE_PRELOAD/images.sh" --list) || { echo "preload: $KIND_NODE_PRELOAD/images.sh --list failed (exit $?): no preload" >&2; return 1; }
  # a root that cannot hold overlayfs layers breaks every pod on the lane: refuse it (status 2: the lane up stops)
  ./node-preload.sh check "$base" >&2 || return 2
  # shellcheck disable=SC2086
  key=$(./node-preload.sh key "$base" $imgs) || { echo "preload: no key: no preload" >&2; return 1; }
  if ! [ -d "${FUZZOPS_PRELOAD_ROOT:-${FUZZOPS_CACHE:-$HOME/.cache/fuzzops}/node-preload}/templates/$key/containerd" ]; then
    [ "${KIND_NODE_PRELOAD_BUILD:-on}" = off ] && { echo "preload: template $key absent, build off: no preload" >&2; return 1; }
    echo "preload: building template $key" >&2
    # shellcheck disable=SC2086
    ./registry.sh push $imgs >&2 && ./node-preload.sh ensure "$base" $imgs >/dev/null || { echo "preload: build failed: no preload" >&2; return 1; }
  fi
  for i in $(seq "$workers"); do
    dirs="$dirs $(./node-preload.sh clone "$base" "$key" "$lane" "$i")" || { ./node-preload.sh drop "$lane" || true; echo "preload: clone failed: no preload" >&2; return 1; }
  done
  echo "$key$dirs"
}

# record_images NAME FILE [KEY]: what the lane's nodes were made from (ids, not secrets), for the run's record.
record_images() {
  local n id rec=
  for n in $(kind get nodes --name "$1"); do
    id=$(docker inspect -f '{{.Config.Image}} {{.Image}}' "$n")
    rec="$rec{\"node\":\"$n\",\"image\":\"${id% *}\",\"image_id\":\"${id#* }\"},"
  done
  printf '{"preload_template":"%s","nodes":[%s]}\n' "${3:-}" "${rec%,}" > "$2"
}

# adopt_evidence NAME EV: a cluster's evidence mount source is fixed when it is created, so one adopted under a new
# bench root still writes to the old root's directory. Make EV (where the bench reads) point at the actual source;
# fail only when the cluster has no evidence mount or its nodes disagree.
adopt_evidence() {
  local n src first= want
  for n in $(kind get nodes --name "$1"); do
    src=$(docker inspect -f '{{range .Mounts}}{{if eq .Destination "/bench-evidence"}}{{.Source}}{{end}}{{end}}' "$n")
    [ -n "$src" ] || { echo "node $n has no /bench-evidence mount: not a lane (delete and recreate it)" >&2; return 1; }
    [ -z "$first" ] || [ "$src" = "$first" ] || { echo "nodes of $1 mount different evidence directories ($first, $src)" >&2; return 1; }
    first=$src
  done
  [ -n "$first" ] || return 0
  want=$(readlink -f "$2")
  [ "$want" = "$(readlink -f "$first")" ] && return 0
  if [ -L "$2" ]; then rm -f "$2"
  elif [ -d "$2" ]; then rmdir "$2" 2>/dev/null || { echo "evidence dir $2 is not empty but cluster $1 writes to $first: move it away" >&2; return 1; }
  fi
  ln -s "$first" "$2"
  echo "cluster $1 writes evidence to $first: $2 now links to it" >&2
}

# kernel_node_image: the image to probe with: KIND_NODE_IMAGE, else kind's own default (the reference embedded in the kind binary;
# what `kind create cluster` would pull), else a local kindest/node image. Empty when none can be found.
kernel_node_image() {
  [ -z "${KIND_NODE_IMAGE:-}" ] || { printf '%s\n' "$KIND_NODE_IMAGE"; return 0; }
  local img
  img=$(grep -ao 'kindest/node:v[0-9.]*@sha256:[0-9a-f]*' "$(command -v kind)" 2>/dev/null | head -1) || true
  [ -n "$img" ] || img=$(docker images --format '{{.Repository}}:{{.Tag}}' 2>/dev/null | grep -E '^kindest/node:' | head -1) || true
  printf '%s\n' "$img"
}

# kernel_check: refuse a host kernel that cannot run kube-proxy. A node's iptables (either flavour) must have the mangle table and
# accept a rule with the comment match in it; a custom kernel without CONFIG_IP_NF_MANGLE / CONFIG_NETFILTER_XT_MATCH_COMMENT makes
# kindnet and kube-proxy crash-loop and every in-cluster call to the API Service time out, which otherwise shows only as a
# helm timeout 15 minutes later. The probe runs in a throwaway container with its own network namespace (the host's rules are untouched)
# and exits 42 when the mangle table is rejected, 43 when the comment match is. Only those two are a verdict on the kernel: any other
# failure (no image, a dead or hung docker, no iptables in the image) is inconclusive: its output is printed, the lane goes on and
# kind gives its own error if it is real. KIND_KERNEL_CHECK_TIMEOUT (default 60 s) bounds the probe.
kernel_check() {
  [ "${KIND_SKIP_KERNEL_CHECK:-}" = 1 ] && return 0
  local img out rc=0 tmo=${KIND_KERNEL_CHECK_TIMEOUT:-60} run=()
  img=$(kernel_node_image)
  [ -n "$img" ] || { echo "kind-lane: kernel check inconclusive: no node image to probe with (KIND_NODE_IMAGE unset and no kindest/node found); going on" >&2; return 0; }
  command -v timeout >/dev/null 2>&1 && run=(timeout "$tmo")
  out=$(${run[@]+"${run[@]}"} docker run --rm --privileged --name "fzkernelcheck-$$" --entrypoint sh "$img" -c '
    seen=0 mangle=0
    for ipt in iptables-legacy iptables-nft; do
      command -v "$ipt" >/dev/null 2>&1 || continue
      seen=1
      "$ipt" -t mangle -N FZCHK || continue
      mangle=1
      "$ipt" -t mangle -A FZCHK -m comment --comment fzcheck -j RETURN && exit 0
    done
    [ "$seen" = 1 ] || { echo "no iptables in the node image" >&2; exit 44; }
    [ "$mangle" = 1 ] && exit 43
    exit 42' 2>&1) || rc=$?
  [ "$rc" -eq 0 ] && return 0
  case "$rc" in
  42|43)
    local what=("the mangle table" CONFIG_IP_NF_MANGLE iptable_mangle) ; [ "$rc" -eq 43 ] && what=("the comment match" CONFIG_NETFILTER_XT_MATCH_COMMENT xt_comment)
    echo "kind-lane: the host kernel ($(uname -r)) cannot run kube-proxy: a node's iptables rejects ${what[0]} (needs ${what[1]}, module ${what[2]})." >&2
    echo "kind-lane: probe output: $out" >&2
    echo "kind-lane: boot a kernel that has it, or set KIND_SKIP_KERNEL_CHECK=1 to make the lane anyway." >&2
    return 1 ;;
  124) rc="timed out after ${tmo}s"; docker rm -f "fzkernelcheck-$$" >/dev/null 2>&1 || true ;;
  esac
  echo "kind-lane: kernel check inconclusive (probe with $img: exit $rc); going on. Probe output: $out" >&2
  return 0
}

case "${1:-}" in
up)
  name=${2:?name} kc=${3:?kubeconfig} ev=${4:?evidence dir} workers=${KIND_WORKERS:-2}
  # a new cluster on a kernel that cannot run kube-proxy is refused before the registry or any preload staging is touched
  kind get clusters | grep -x "$name" >/dev/null || kernel_check || exit 1
  mkdir -p "$ev" && chmod 0777 "$ev"   # the operator's sink may write as another uid
  ./registry.sh up
  preload_key= preload=()
  if [ -n "${KIND_NODE_PRELOAD:-}" ] && ! kind get clusters | grep -x "$name" >/dev/null; then
    out=$(preload_dirs "$name") && rc=0 || rc=$?
    [ "$rc" -eq 2 ] && { echo "kind-lane: KIND_NODE_PRELOAD is set but its root is unusable: not making the lane" >&2; exit 1; }
    if [ "$rc" -eq 0 ]; then read -ra parts <<<"$out"; preload_key=${parts[0]}; preload=("${parts[@]:1}"); fi
  fi
  if ! kind get clusters | grep -x "$name" >/dev/null; then
    cfg=$(mktemp)
    {
      printf 'kind: Cluster\napiVersion: kind.x-k8s.io/v1alpha4\n'
      printf 'containerdConfigPatches:\n- |-\n  [plugins."io.containerd.grpc.v1.cri".registry]\n    config_path = "/etc/containerd/certs.d"\n'
      mount="  extraMounts:\n  - {hostPath: $ev, containerPath: /bench-evidence}\n"
      printf 'nodes:\n- role: control-plane\n'"$mount"
      for i in $(seq "$workers"); do
        printf -- '- role: worker\n'"$mount"
        if [ "${#preload[@]}" -gt 0 ]; then printf '  - {hostPath: %s, containerPath: /var/lib/containerd}\n' "${preload[$((i-1))]}"; fi
      done
    } > "$cfg"
    mkdir -p "$(dirname "$kc")"
    if [ -n "${FUZZOPS_LANE_CPUSET:-}${FUZZOPS_LANE_MEMORY:-}" ]; then
      export FUZZOPS_LANE_WORKERS=$workers FUZZOPS_LANE_CONTROL_PLANES=1
      # the shim is first in PATH for this command only, and finds the real docker now
      FUZZOPS_REAL_DOCKER=$(command -v docker) PATH="$PWD/lane-docker-shim:$PATH" \
        kind create cluster --name "$name" ${KIND_NODE_IMAGE:+--image "$KIND_NODE_IMAGE"} --config "$cfg" --kubeconfig "$kc.tmp.$$" --wait 5m
    else
      kind create cluster --name "$name" ${KIND_NODE_IMAGE:+--image "$KIND_NODE_IMAGE"} --config "$cfg" --kubeconfig "$kc.tmp.$$" --wait 5m
    fi
    rm -f "$cfg"
    if [ -n "${FUZZOPS_LANE_SLICE:-}" ]; then
      # kind passes the node's `docker run` through the shim; if a kind version words its arguments differently the shim
      # passes them untouched and the lane would run outside the slice (and outside the host reservation): fail, loudly
      for n in $(kind get nodes --name "$name"); do
        parent=$(docker inspect -f '{{.HostConfig.CgroupParent}}' "$n")
        [ "$parent" = "$FUZZOPS_LANE_SLICE" ] || {
          echo "kind-lane: node $n is under cgroup parent '$parent', not '$FUZZOPS_LANE_SLICE': this kind ($(kind version 2>/dev/null)) was not passed through lane-docker-shim (tested with kind v0.31); deleting the lane" >&2
          kind delete cluster --name "$name" >&2 || true
          exit 1
        }
      done
    fi
    mv "$kc.tmp.$$" "$kc"
    record_images "$name" "$kc.node-image" "$preload_key"
  else
    # an adopted cluster (a new bench root, a wiped kube directory): the kubeconfig under this root is rewritten
    # from kind's, never assumed to exist
    mkdir -p "$(dirname "$kc")"
    kind get kubeconfig --name "$name" > "$kc.tmp.$$"
    mv "$kc.tmp.$$" "$kc"
    [ -e "$kc.node-image" ] || record_images "$name" "$kc.node-image" ""
  fi
  adopt_evidence "$name" "$ev"
  ./lane-limits.sh apply "$name"   # no-op without FUZZOPS_LANE_*; sets an adopted cluster's nodes to the lane's share
  for n in $(kind get nodes --name "$name"); do
    docker exec "$n" test -d /bench-evidence || { echo "node $n has no /bench-evidence mount: not a lane (delete and recreate it)" >&2; exit 1; }
    docker exec "$n" mkdir -p /etc/containerd/certs.d/_default
    printf '[host."http://%s:5000"]\n  capabilities = ["pull", "resolve"]\n' "$REG_NAME" |
      docker exec -i "$n" sh -c 'cat > /etc/containerd/certs.d/_default/hosts.toml'
  done
  docker network connect kind "$REG_NAME" 2>/dev/null || true
  kubectl --kubeconfig "$kc" --context "kind-$name" apply -f - >/dev/null <<YAML
apiVersion: v1
kind: ConfigMap
metadata: {name: local-registry-hosting, namespace: kube-public}
data:
  localRegistryHosting.v1: |
    host: "localhost:${FUZZOPS_REGISTRY_PORT:-5001}"
YAML
  echo "lane cluster $name up (images through $REG_NAME)"
  ;;
down)
  kind delete cluster --name "${2:?name}"
  ./node-preload.sh drop "$2"
  ;;
*) echo "usage: kind-lane.sh up NAME KUBECONFIG EVIDENCE_DIR | down NAME" >&2; exit 2 ;;
esac
