#!/usr/bin/env bash
# Load images into every node of a kind cluster (idempotent), keeping digest references; for a
# cluster made by kind-lane.sh, push them to the host's registry instead (registry.sh).
#
#   deploy/load-images.sh CLUSTER IMAGE...     # IMAGE: repo:tag or repo@sha256:digest
#
# `kind load docker-image` cannot load an image a manifest names by digest (the node
# then pulls it anyway). Here each image is pulled into the host's docker once, saved,
# imported into each node's containerd that lacks the reference, and digest references
# are added as names of the imported image, which is how the kubelet looks them up.
set -euo pipefail
CLUSTER=${1:?usage: load-images.sh CLUSTER IMAGE...}
shift
norm() { # the containerd names of a reference: repo[:tag]@digest also as repo@digest
  local r=$1
  case "$r" in
    */*) case "${r%%/*}" in *.*|*:*|localhost) ;; *) r="docker.io/$r" ;; esac ;;
    *) r="docker.io/library/$r" ;;
  esac
  case "$r" in
    *@*) echo "$r"; local repo=${r%@*}; repo=${repo%:*}; [ "$repo@${r#*@}" != "$r" ] && echo "$repo@${r#*@}" ;;
    *:*) echo "$r" ;;
    *) echo "$r:latest" ;;
  esac
  return 0
}
# A cluster made by kind-lane.sh pulls through the host's registry: put the images there once
# instead of copying them into every node.
first=$(kind get nodes --name "$CLUSTER" | head -1)
if [ -n "$first" ] && docker exec "$first" test -f /etc/containerd/certs.d/_default/hosts.toml 2>/dev/null; then
  exec "$(dirname "$0")/registry.sh" push "$@"
fi
nodes=$(kind get nodes --name "$CLUSTER" | grep -v control-plane || true)
[ -n "$nodes" ] || nodes=$(kind get nodes --name "$CLUSTER")
for img in "$@"; do
  refs=$(norm "$img")
  need=""
  for n in $nodes; do
    have=$(docker exec "$n" ctr -n k8s.io images ls -q)
    for ref in $refs; do grep -qxF "$ref" <<<"$have" || { need="$need $n"; break; }; done
  done
  [ -z "$need" ] && continue
  docker image inspect "$img" >/dev/null 2>&1 || docker pull -q "$img" >/dev/null
  id=$(docker image inspect -f '{{.Id}}' "$img")
  local_tag="fuzzops-preload/${id#sha256:}"
  local_tag="${local_tag:0:60}:img"
  docker tag "$id" "$local_tag"
  for n in $need; do
    docker save "$local_tag" | docker exec -i "$n" ctr -n k8s.io images import - >/dev/null
    for ref in $refs; do docker exec "$n" ctr -n k8s.io images tag --force "docker.io/$local_tag" "$ref" >/dev/null; done
    echo "$n: $ref"
  done &
done
wait
