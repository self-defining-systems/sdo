#!/usr/bin/env bash
# A pack's images already in containerd on a lane's worker nodes, so their pods do not pull them (a GitLab lane
# spends 80-170 s pulling and unpacking 3-4 GB images). The node image stays the protocol's, unchanged.
#
#   deploy/node-preload.sh key      BASE IMAGE...   # the template's key (no build)
#   deploy/node-preload.sh check    BASE           # fail unless the root can hold overlayfs layers (see below)
#   deploy/node-preload.sh ensure   BASE IMAGE...   # build the template when this host lacks it; print its key
#   deploy/node-preload.sh clone    BASE KEY LANE N # LANE's worker N gets a containerd directory from the template;
#                                                   # print its path (kind-lane.sh mounts it at /var/lib/containerd)
#   deploy/node-preload.sh drop     LANE            # remove LANE's directories
#
# Template: the containerd directory of a throwaway node from BASE (so it keeps BASE's own images) after IMAGE...
# were pulled through the host registry's `_default` mirror exactly as a node's kubelet would (registry.sh puts
# them there); at most FUZZOPS_PRELOAD_PARALLEL (default 3) pulls at a time. Its key is a hash of a format version,
# BASE's identity (its digest when pinned, else its image id: the containerd store is tied to the node image's
# containerd) and the sorted image list, so another base or pack image list is another template. Keep images that
# change per build (the SDO controller, responder, script-tools) out of IMAGE...: they come through the registry.
# A lane's directory is a clone: the layer files and the image content (immutable, never written in place) are
# hard links to the template's, everything else (the metadata databases) is copied, so a clone takes seconds and
# almost no disk. A failed build leaves no template.
# The root is mounted into the workers as /var/lib/containerd, where containerd's overlayfs snapshotter mounts
# layers with upperdir/workdir inside it. A filesystem that cannot hold an overlayfs upper layer (NFS, most FUSE)
# mounts fine but every pod then fails with "invalid argument", so `check` really mounts an overlay there first;
# ensure runs it, and kind-lane.sh refuses the lane when it fails. Use a local disk (ext4, xfs with d_type).
# Settings: FUZZOPS_PRELOAD_PARALLEL, FUZZOPS_PRELOAD_ROOT (default $FUZZOPS_CACHE/node-preload; templates and
# lane directories must share one filesystem), FUZZOPS_REGISTRY_NAME (as registry.sh).
set -euo pipefail
REG_NAME=${FUZZOPS_REGISTRY_NAME:-fuzzops-registry}
FORMAT=1
ROOT=${FUZZOPS_PRELOAD_ROOT:-${FUZZOPS_CACHE:-$HOME/.cache/fuzzops}/node-preload}
PARALLEL=${FUZZOPS_PRELOAD_PARALLEL:-3}

base_id() { # the base's identity: its digest when pinned, else the local image id
  local base=$1
  case "$base" in
    *@sha256:*) echo "sha256:${base##*@sha256:}" ;;
    *) docker image inspect -f '{{.Id}}' "$base" 2>/dev/null || { docker pull -q "$base" >/dev/null && docker image inspect -f '{{.Id}}' "$base"; } ;;
  esac
}

key() { # BASE IMAGE...
  local base=$1; shift
  { echo "format $FORMAT"; echo "base $(base_id "$base")"; printf 'image %s\n' "$@" | LC_ALL=C sort -u; } |
    sha256sum | cut -c1-16
}

canonical() { # the name containerd lists an image under (docker.io/library/ for bare names)
  local r=$1
  case "$r" in
    */*) case "${r%%/*}" in *.*|*:*|localhost) ;; *) r="docker.io/$r" ;; esac ;;
    *) r="docker.io/library/$r" ;;
  esac
  case "$r" in *@*|*:*) echo "$r" ;; *) echo "$r:latest" ;; esac
}

root_run() { # BASE ARGS...: a privileged root shell of BASE with ROOT at /w (files are root's, with xattrs)
  local base=$1; shift
  docker run --rm --privileged -v "$ROOT:/w" --entrypoint sh "$base" -c "$@"
}

build() ( # KEY BASE IMAGE...   (a subshell: its EXIT trap cleans up whatever happens)
  key=$1 base=$2; shift 2
  docker image inspect "$base" >/dev/null 2>&1 || docker pull -q "$base" >/dev/null
  mkdir -p "$ROOT/templates"
  tmp=.tmp-$$-$key
  cn=fzpreload-$$
  trap 'docker rm -f "$cn" >/dev/null 2>&1 || true; [ -e "$ROOT/templates/$tmp" ] && root_run "$base" "rm -rf /w/templates/$tmp" >/dev/null 2>&1 || true' EXIT
  mkdir "$ROOT/templates/$tmp"
  # --volume /var as kind does: the node's own images are copied up into it, and the pulls go on top
  docker run -d --name "$cn" --privileged --network kind --volume /var -v "$ROOT/templates/$tmp:/out" \
    --entrypoint sleep "$base" infinity >/dev/null
  docker exec "$cn" sh -c 'mkdir -p /etc/containerd/certs.d/_default && printf "[host.\"http://%s:5000\"]\n  capabilities = [\"pull\", \"resolve\"]\n" "$1" > /etc/containerd/certs.d/_default/hosts.toml' sh "$REG_NAME"
  docker exec -d "$cn" sh -c 'containerd >/var/log/containerd-build.log 2>&1'
  for i in $(seq 60); do docker exec "$cn" ctr version >/dev/null 2>&1 && break; sleep 1; done
  docker exec "$cn" ctr version >/dev/null
  for img in "$@"; do canonical "$img"; done | xargs -P "$PARALLEL" -I{} docker exec "$cn" sh -c '
    set -e
    ctr -n k8s.io images pull --hosts-dir /etc/containerd/certs.d "$1" >/dev/null
    ctr -n k8s.io images label "$1" io.cri-containerd.image=managed >/dev/null' sh {}
  # every image must be there under the name the kubelet asks for
  have=$(docker exec "$cn" ctr -n k8s.io images ls -q)
  for img in "$@"; do
    grep -qxF "$(canonical "$img")" <<<"$have" || { echo "node-preload: $img not in containerd after the pull" >&2; exit 1; }
  done
  docker exec "$cn" sh -c 'pkill containerd; sleep 2; sync; cp -a /var/lib/containerd /out/containerd'
  docker rm -f "$cn" >/dev/null
  mv "$ROOT/templates/$tmp" "$ROOT/templates/$key"   # whole template or none
)

check() { # BASE: a real overlay mount with its upper and work directories in ROOT, from inside a privileged node
  local base=$1 out
  mkdir -p "$ROOT"
  out=$(root_run "$base" '
    set -e
    p=/w/.probe-$$; m=$p/m
    trap "umount $m 2>/dev/null || :; rm -rf $p" EXIT
    mkdir -p $p/l $p/u $p/w $m
    mount -t overlay overlay -o lowerdir=$p/l,upperdir=$p/u,workdir=$p/w $m' 2>&1) || {
    echo "node-preload: $ROOT cannot hold overlayfs layers (the overlay mount failed: ${out##*$'\n'})." >&2
    echo "  Containerd in the workers would fail every pod with 'invalid argument'. Set FUZZOPS_PRELOAD_ROOT (or FUZZOPS_CACHE)" >&2
    echo "  to a directory on a local disk (ext4, or xfs with d_type), not NFS." >&2
    return 1
  }
}

clone() { # BASE KEY LANE N
  local base=$1 key=$2 lane=$3 n=$4
  root_run "$base" '
    set -e
    t=/w/templates/$1/containerd; d=/w/lanes/$2-w$3
    [ -d "$t" ] || { echo "no template $1" >&2; exit 1; }
    rm -rf "$d"; mkdir -p /w/lanes
    cp -al "$t" "$d"
    # whatever containerd writes in place gets its own copy; layer files and image content are never written
    cd "$t"
    find . -type f ! -path "./io.containerd.content.v1.content/blobs/*" ! -path "./io.containerd.snapshotter.v1.overlayfs/snapshots/*" |
      while read -r f; do rm "$d/$f"; cp -a "$f" "$d/$f"; done' sh "$key" "$lane" "$n"
  echo "$ROOT/lanes/$lane-w$n"
}

case "${1:-}" in
  key) shift; key "$@" ;;
  _build) shift; build "$@" ;;   # ensure runs it as its own process, so `set -e` holds inside
  ensure)
    shift
    k=$(key "$@")
    check "$1"   # before the lock and before any build: no template on a root that cannot serve a lane
    exec 8>"$ROOT/.lock"
    flock 8   # one build per host at a time; a second caller waits and then finds the template
    [ -d "$ROOT/templates/$k/containerd" ] || bash "$0" _build "$k" "$@" >&2
    echo "$k"
    ;;
  check) shift; check "$1" ;;
  clone) shift; clone "$@" ;;
  drop)
    lane=${2:?lane}
    if [ -n "$(ls -d "$ROOT"/lanes/"$lane"-w* 2>/dev/null)" ]; then
      b=${KIND_NODE_IMAGE:-$(docker images --format '{{.Repository}}:{{.Tag}}' | grep -E '^kindest/node:' | head -1)}
      root_run "${b:?no node image on this host to remove the lane directories with}" "rm -rf /w/lanes/$lane-w*"
    fi
    ;;
  *) echo "usage: node-preload.sh key|check|ensure BASE IMAGE... | clone BASE KEY LANE N | drop LANE" >&2; exit 2 ;;
esac
