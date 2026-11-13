#!/usr/bin/env bash
# Build SDO's benchmark images for one commit and put them on the experiment hosts.
#
#   scripts/build_push_bench_images.sh <sha> <host>...
#
# Produces sdo-controller, sdo-responder and sdo-script-tools tagged fz-<sha8> (the tag
# benchmarks/fuzzops/hooks.py derives from the revision) from a worktree at <sha>, and
# leaves them in each host's Docker daemon and in its fuzzops-registry (the registry
# kind lane nodes pull from).
#
# Why this is fast:
#   * the build reuses the BuildKit layer cache of the Dockerfile (Go modules, the Go
#     build-cache seed, Python packages and the model CLIs sit in layers that a source
#     edit leaves alone);
#   * hosts receive images through the registry, so only layers the host's registry does
#     not hold move (a code change moves a few hundred MB, not the whole 3+ GB image, which
#     streaming an image archive to the host moves every layer every time);
#   * the slow runtime smoke gates run while the images are being distributed;
#   * every step is skipped when its result already exists: the build when the images
#     for <sha> are in the local daemon, a host's copy when its image ID matches.
#
# Hosts: `chelan5` (this machine), `spokane1`, `chelan4` (via the `dock` jump host) are
# known. Any other `name[=jump][:registry_port]` is an ssh host with a registry on
# 127.0.0.1:5001 (override with :port). The local host is `local` or this machine's
# short hostname.
#
# Environment:
#   SDO_BENCH_WORKTREE_ROOT  where bench-sdo-<sha8> worktrees live (default: the repo's parent directory)
#   SDO_BENCH_LOG_DIR        build and smoke-gate logs (default: $TMPDIR or /tmp)
#   SDO_BENCH_PARALLEL       image pushes and pulls per host at once (default and maximum 3)
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
images=(sdo-controller sdo-responder sdo-script-tools)
targets="controller responder script-tools"
parallel="${SDO_BENCH_PARALLEL:-3}"
((parallel >= 1 && parallel <= 3)) || { echo "SDO_BENCH_PARALLEL must be 1..3" >&2; exit 2; }

if (($# < 2)); then
  echo "usage: $0 <sha> <host>..." >&2
  exit 2
fi
sha_arg="$1"
shift
hosts=("$@")

log() { printf '%s [%s] %s\n' "$(date +%H:%M:%S)" "${LOG_PREFIX:-build}" "$*"; }
# timed LABEL CMD...: run CMD, print its wall-clock time.
timed() {
  local label="$1" start=${SECONDS} rc=0
  shift
  "$@" || rc=$?
  log "timing: ${label}: $((SECONDS - start))s (rc=${rc})"
  return "${rc}"
}

if ! full_sha="$(git -C "${repo_root}" rev-parse --verify --quiet "${sha_arg}^{commit}")"; then
  git -C "${repo_root}" fetch --quiet origin
  full_sha="$(git -C "${repo_root}" rev-parse --verify "${sha_arg}^{commit}")"
fi
tag="fz-${full_sha:0:8}"
worktree="${SDO_BENCH_WORKTREE_ROOT:-$(dirname "${repo_root}")}/bench-sdo-${full_sha:0:8}"
total_start=${SECONDS}
log "revision ${full_sha} tag ${tag} hosts: ${hosts[*]}"

# The identity of an image's content: the hash of its layer chain. (An image ID differs between
# daemons that keep images in different stores, a layer chain does not.)
local_image_id() { docker image inspect --format '{{json .RootFS.Layers}}' "$1:${tag}" 2>/dev/null | sha256sum | cut -c1-12; }
local_image_present() { docker image inspect "$1:${tag}" >/dev/null 2>&1; }

built() {
  local image revision
  for image in "${images[@]}"; do
    local_image_present "${image}" || return 1
  done
  for image in sdo-controller sdo-responder; do
    revision="$(docker image inspect --format '{{index .Config.Labels "org.opencontainers.image.revision"}}' "${image}:${tag}")"
    [[ "${revision}" == "${full_sha}" ]] || return 1
  done
}

# ---- build -----------------------------------------------------------------------
ensure_worktree() {
  if [[ ! -d "${worktree}" ]]; then
    git -C "${repo_root}" worktree add --quiet --detach "${worktree}" "${full_sha}"
  fi
  [[ "$(git -C "${worktree}" rev-parse HEAD)" == "${full_sha}" ]] || { echo "${worktree} is not at ${full_sha}" >&2; return 1; }
}

# Commits that predate SDO_IMAGE_SMOKE run their gates inside the build script.
gates_split() { grep -q SDO_IMAGE_SMOKE "${worktree}/scripts/build_sdo_images.sh"; }

build_step() {
  local smoke=1
  gates_split && smoke=0
  (
    cd "${worktree}"
    export PATH="${HOME}/.local/bin:${HOME}/.local/go/bin:${PATH}"
    SDO_IMAGE_TAG="${tag}" SDO_IMAGES="${targets}" SDO_IMAGE_SMOKE="${smoke}" bash scripts/build_sdo_images.sh
  )
}

log_dir="${SDO_BENCH_LOG_DIR:-${TMPDIR:-/tmp}}"
ensure_worktree
smoke_marker="$(git -C "${worktree}" rev-parse --absolute-git-dir)/sdo-image-smoke-${tag}"
if built; then
  log "images ${tag} already built locally: skipping build"
else
  build_log="${log_dir}/build-${tag}.log"
  log "building (full output: ${build_log})"
  timed "build ${targets}" build_step >"${build_log}" 2>&1 || { tail -40 "${build_log}"; exit 1; }
  grep -h '^timing:' "${build_log}" | sed 's/^/  /' || true
  built || { echo "build finished but ${tag} images are missing or carry another revision" >&2; exit 1; }
  gates_split || : >"${smoke_marker}"
fi

# ---- smoke gates (in parallel with distribution) ------------------------------------
gate_log="${log_dir}/smoke-${tag}.log"
gate_pid=""
if [[ -e "${smoke_marker}" ]]; then
  log "smoke gates already passed for ${tag}"
else
  (
    cd "${worktree}"
    export PATH="${HOME}/.local/bin:${HOME}/.local/go/bin:${PATH}"
    LOG_PREFIX=gates timed "smoke gates" env SDO_IMAGE_TAG="${tag}" SDO_IMAGES="${targets}" SDO_SKIP_IMAGE_BUILD=1 \
      bash scripts/build_sdo_images.sh
  ) >"${gate_log}" 2>&1 &
  gate_pid=$!
fi

# ---- distribution ------------------------------------------------------------------
# registry_port of a host, and how to reach it.
host_ssh_opts=() host_ssh=() host_name="" host_port="" is_local=0
resolve_host() {
  local spec="$1" name jump="" port=5001
  name="${spec%%[=:]*}"
  [[ "${spec}" == *=* ]] && { jump="${spec#*=}"; jump="${jump%%:*}"; }
  [[ "${spec}" == *:* ]] && port="${spec##*:}"
  case "${name}" in
  chelan5 | local) is_local=1 ;;
  chelan4)
    [[ -z "${jump}" && "${spec}" != *=* ]] && jump=dock
    [[ "${spec}" == *:* ]] || port=5301
    ;;
  *) is_local=0 ;;
  esac
  [[ "${name}" == "$(hostname -s)" ]] && is_local=1
  host_name="${name}"
  host_port="${port}"
  host_ssh_opts=(-o BatchMode=yes -o ControlPath=none)
  [[ -n "${jump}" ]] && host_ssh_opts+=(-J "${jump}")
  host_ssh=(ssh "${host_ssh_opts[@]}" "${name}")
}

free_port() { python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])'; }

# Push the images in $need through registry 127.0.0.1:$1 under library/<name>:<tag>.
push_images() {
  local port="$1" image
  shift
  printf '%s\n' "$@" | xargs -P "${parallel}" -I{} bash -c '
    set -e
    ref="127.0.0.1:'"${port}"'/library/{}:'"${tag}"'"
    docker tag "{}:'"${tag}"'" "$ref"
    docker push --quiet "$ref" >/dev/null
    docker rmi "$ref" >/dev/null
  '
}

distribute() {
  local spec="$1" image remote_id local_id
  local -a need=()
  resolve_host "${spec}"
  LOG_PREFIX="${host_name}"
  if ((is_local)); then
    curl -sf "http://127.0.0.1:${host_port}/v2/" >/dev/null || { log "no registry on 127.0.0.1:${host_port}"; return 1; }
    timed "push to local registry" push_images "${host_port}" "${images[@]}"
    return
  fi
  for image in "${images[@]}"; do
    local_id="$(local_image_id "${image}")"
    remote_id="$("${host_ssh[@]}" "docker image inspect --format '{{json .RootFS.Layers}}' ${image}:${tag} 2>/dev/null | sha256sum | cut -c1-12")"
    if [[ "${remote_id}" == "${local_id}" ]]; then
      log "${image}:${tag} already present (${local_id}): skipping"
    else
      need+=("${image}")
    fi
  done
  if ((${#need[@]} == 0)); then
    return 0
  fi
  "${host_ssh[@]}" "curl -sf http://127.0.0.1:${host_port}/v2/ >/dev/null" \
    || { log "no registry on the host's 127.0.0.1:${host_port} (deploy/registry.sh up)"; return 1; }
  # An ssh tunnel to the host's registry; Docker pushes only the layers the registry lacks.
  local lport tunnel_pid
  lport="$(free_port)"
  ssh "${host_ssh_opts[@]}" -N -o ExitOnForwardFailure=yes -L "127.0.0.1:${lport}:127.0.0.1:${host_port}" "${host_name}" &
  tunnel_pid=$!
  local waited=0
  until curl -sf "http://127.0.0.1:${lport}/v2/" >/dev/null; do
    sleep 0.3
    ((++waited < 100)) || { log "tunnel to registry did not come up"; kill "${tunnel_pid}" 2>/dev/null || true; return 1; }
  done
  timed "push ${need[*]} to registry (tunnel)" push_images "${lport}" "${need[@]}" || { kill "${tunnel_pid}" 2>/dev/null || true; return 1; }
  # The host's daemon pulls from its own registry (hooks.py needs the images in the daemon too).
  timed "pull ${need[*]} into the daemon" "${host_ssh[@]}" "printf '%s\n' ${need[*]} | xargs -P ${parallel} -I{} sh -c \
    'docker pull -q 127.0.0.1:${host_port}/library/{}:${tag} >/dev/null && docker tag 127.0.0.1:${host_port}/library/{}:${tag} {}:${tag}'"
  kill "${tunnel_pid}" 2>/dev/null || true
  for image in "${need[@]}"; do
    remote_id="$("${host_ssh[@]}" "docker image inspect --format '{{json .RootFS.Layers}}' ${image}:${tag} | sha256sum | cut -c1-12")"
    [[ "${remote_id}" == "$(local_image_id "${image}")" ]] || { log "${image}:${tag} on the host differs from the local image (${remote_id} != $(local_image_id "${image}"))"; return 1; }
  done
}

dist_dir="$(mktemp -d)"
declare -A dist_pid=()
for spec in "${hosts[@]}"; do
  ( distribute "${spec}" ) >"${dist_dir}/${spec//[^A-Za-z0-9]/_}.log" 2>&1 &
  dist_pid["${spec}"]=$!
done
status=0
for spec in "${hosts[@]}"; do
  wait "${dist_pid[${spec}]}" || { log "host ${spec}: FAILED"; status=1; }
  cat "${dist_dir}/${spec//[^A-Za-z0-9]/_}.log"
done

if [[ -n "${gate_pid}" ]]; then
  if wait "${gate_pid}"; then
    log "smoke gates passed"
    : >"${smoke_marker}"
  else
    log "smoke gates FAILED: do not use ${tag}"
    status=1
  fi
  grep -h 'timing:' "${gate_log}" | sed 's/^/  /' || true
  [[ ${status} == 0 ]] || tail -30 "${gate_log}"
else
  log "smoke gates: ${gates_skipped_reason}"
fi
rm -rf "${dist_dir}"
log "timing: total: $((SECONDS - total_start))s (status=${status})"
exit "${status}"
