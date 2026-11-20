#!/usr/bin/env bash
# The host's local image registry (the kind local-registry pattern), shared by every lane cluster
# made with kind-lane.sh: an image is pushed here once per host and nodes pull it on demand.
#
#   deploy/registry.sh up                 # start it (idempotent)
#   deploy/registry.sh ref IMAGE          # the name IMAGE has in it (localhost:PORT/<path>)
#   deploy/registry.sh push IMAGE...      # put images in it (skips what it holds)
#
# Nodes find an image under its usual name: their containerd asks the registry first for every
# upstream (a `_default` mirror), with the image's path minus its registry host, then the upstream.
# Settings: FUZZOPS_REGISTRY_NAME (default fuzzops-registry), FUZZOPS_REGISTRY_PORT (5001).
# On a host whose lanes are virtual clusters on a shared cluster (FUZZOPS_LANE_PROVIDER=vcluster) the registry is
# the shared cluster's (shared-registry.sh): the same localhost:PORT name works on every node of it, but nothing
# mirrors upstream names, so a lane's pod must name its image by `ref`.
set -euo pipefail
REG_NAME=${FUZZOPS_REGISTRY_NAME:-fuzzops-registry}
REG_PORT=${FUZZOPS_REGISTRY_PORT:-5001}
SHARED=0
if [ "${FUZZOPS_LANE_PROVIDER:-}" = vcluster ]; then
  SHARED=1
  REG_PORT=${FUZZOPS_SHARED_REGISTRY_PORT:-30500}
fi

up() {
  if [ "$SHARED" = 1 ]; then "$(dirname "$0")/shared-registry.sh" up; return; fi
  if [ "$(docker inspect -f '{{.State.Running}}' "$REG_NAME" 2>/dev/null)" != true ]; then
    docker rm -f "$REG_NAME" >/dev/null 2>&1 || true
    docker run -d --restart=always -p "127.0.0.1:$REG_PORT:5000" --name "$REG_NAME" \
      -e REGISTRY_STORAGE_DELETE_ENABLED=true registry:2 >/dev/null
  fi
  docker network inspect kind >/dev/null 2>&1 && { docker network connect kind "$REG_NAME" 2>/dev/null || true; }
  echo "registry $REG_NAME on 127.0.0.1:$REG_PORT"
}

# path: an image reference without its registry host (docker.io/library/x -> library/x).
path() {
  local r=$1 first=${1%%/*}
  case "$r" in */*) ;; *) echo "library/$r"; return ;; esac
  case "$first" in
    *.*|*:*|localhost) r=${r#*/} ;;
  esac
  case "$r" in */*) echo "$r" ;; *) echo "library/$r" ;; esac
}

ref() { echo "localhost:$REG_PORT/$(path "$1")"; }

has() { # repo ref -> 0 when the registry holds it
  local p=${1#localhost:$REG_PORT/} repo tag
  case "$p" in *@*) repo=${p%@*}; repo=${repo%:*}; tag=${p#*@} ;; *) repo=${p%:*}; tag=${p##*:} ;; esac
  curl -sf -o /dev/null -H 'Accept: application/vnd.oci.image.index.v1+json, application/vnd.docker.distribution.manifest.list.v2+json, application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json' \
    "http://127.0.0.1:$REG_PORT/v2/$repo/manifests/$tag"
}

push() {
  local img dst
  for img in "$@"; do
    dst=$(ref "$img")
    has "$dst" && continue
    case "$img" in
      *@sha256:*)
        # A digest-pinned upstream image: copy its manifest unchanged, so the node's pull by digest hits.
        docker buildx imagetools create --tag "${dst%@*}" "$img" >/dev/null 2>&1 && { echo "copied $img"; continue; } ;;
    esac
    docker image inspect "$img" >/dev/null 2>&1 || docker pull -q "$img" >/dev/null
    local tagged=${dst%@*}
    case "$tagged" in *:*) ;; *) tagged="$tagged:latest" ;; esac
    docker tag "$img" "$tagged" && docker push -q "$tagged" >/dev/null && echo "pushed $img"
  done
}

case "${1:-}" in
  up) up ;;
  ref) ref "$2" ;;
  push) shift; push "$@" ;;
  *) echo "usage: registry.sh up | ref IMAGE | push IMAGE..." >&2; exit 2 ;;
esac
