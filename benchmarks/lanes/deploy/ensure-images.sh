#!/usr/bin/env bash
# Make sure every schedulable node of a kind cluster holds every image (idempotent), on a host that
# other people's clusters share:
#
#   deploy/ensure-images.sh CLUSTER IMAGE...      # IMAGE: repo:tag or repo@sha256:digest
#   source deploy/ensure-images.sh; ensure_node_images CLUSTER IMAGE...
#
# A node is skipped only when it holds the very image: a digest reference the node has; or, for an
# image the host's docker has, the same image ID or repo digest as docker's copy (so a rebuilt
# local tag such as fuzzops/hotel:dev is loaded again, as `kind load` would); or, for an image only
# the node knows, the tag. So a second `up` loads nothing.
# The nodes that lack it get docker's copy with `kind load docker-image --nodes`, after a
# `docker pull` when the host has no copy. On a shared host that path fails for reasons that are not
# the image's (docker's containerd image store: "snapshot ... already exists" on a pull, "content
# digest ... not found" on a load), so whatever it did not deliver is pulled by the node's own
# containerd with `crictl pull` instead (an image that only exists in the host's docker, a local
# build, has no registry to come from: that is an error naming it).
set -uo pipefail

# containerd's own name for an image: the registry and, on Docker Hub, library/ spelled out
_ei_fqin() {
  case "$1" in
    */*) case "${1%%/*}" in *.*|*:*|localhost) echo "$1" ;; *) echo "docker.io/$1" ;; esac ;;
    *) echo "docker.io/library/$1" ;;
  esac
}

# the image ID and repo digests of docker's copy, space separated ("" when the host has none)
_ei_local() { docker image inspect -f '{{.Id}} {{range .RepoDigests}}{{.}} {{end}}' "$1" 2>/dev/null; }

# _ei_holds NODE REF IMAGE LOCAL: the node holds the image the host means
_ei_holds() {
  local n=$1 ref=$2 local_ids=$4 have t u
  docker exec "$n" crictl inspecti "$ref" >/dev/null 2>&1 || return 1
  case "$ref" in *@sha256:*) return 0 ;; esac   # a digest names exactly one image
  [ -n "$local_ids" ] || return 0               # only the node knows it: the tag is all there is
  have=$(docker exec "$n" crictl inspecti -o go-template --template '{{.status.id}} {{range .status.repoDigests}}{{.}} {{end}}' "$ref" 2>/dev/null) || return 1
  for t in $have; do
    for u in $local_ids; do [ "$t" = "$u" ] && return 0; done
  done
  return 1
}

ensure_node_images() {
  local cluster=${1:-} img n ref fail=0
  [ -n "$cluster" ] || { echo "ensure-images: usage: ensure-images.sh CLUSTER IMAGE..." >&2; return 2; }
  shift
  [ "$#" -gt 0 ] || { echo "ensure-images: no images given for kind cluster $cluster (an empty list is a mistake of the caller, not a cluster that has everything)" >&2; return 1; }
  local nodes
  nodes=$(kind get nodes --name "$cluster" | grep -Ev -- '-control-plane[0-9]*$' || true)
  [ -n "$nodes" ] || nodes=$(kind get nodes --name "$cluster")
  [ -n "$nodes" ] || { echo "ensure-images: no nodes in kind cluster $cluster" >&2; return 1; }
  for img in "$@"; do
    ref=$(_ei_fqin "$img")
    local ids missing=()
    ids=$(_ei_local "$img")
    for n in $nodes; do
      _ei_holds "$n" "$ref" "$img" "$ids" || missing+=("$n")
    done
    [ "${#missing[@]}" -gt 0 ] || continue
    local try
    if [ -z "$ids" ]; then
      for try in 1 2; do
        docker pull -q "$img" >/dev/null 2>&1 && break
      done
      ids=$(_ei_local "$img")
    fi
    if [ -n "$ids" ]; then
      kind load docker-image --name "$cluster" --nodes "$(IFS=,; echo "${missing[*]}")" "$img" >/dev/null 2>&1 || true
      local still=()
      for n in "${missing[@]}"; do
        _ei_holds "$n" "$ref" "$img" "$ids" || still+=("$n")
      done
      missing=("${still[@]+"${still[@]}"}")
    fi
    for n in ${missing[@]+"${missing[@]}"}; do
      echo "ensure-images: $n pulls $ref itself (docker could not deliver it)" >&2
      for try in 1 2; do
        docker exec "$n" crictl pull "$ref" >/dev/null 2>&1 && break
      done
      docker exec "$n" crictl inspecti "$ref" >/dev/null 2>&1 \
        || { echo "ensure-images: node $n cannot get $img (docker pull, kind load and crictl pull all failed)" >&2; fail=1; }
    done
  done
  return $fail
}

if [ "${BASH_SOURCE[0]}" = "$0" ]; then
  ensure_node_images "$@"
fi
