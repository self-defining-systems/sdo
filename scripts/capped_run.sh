#!/usr/bin/env bash
# Run a build or test command with bounded CPU fan-out and, when the host allows it, a memory cap.
#
#   scripts/capped_run.sh [--memory 16G] -- <command> [args...]
#
# Sets GOFLAGS=-p=4 and GOMAXPROCS=8 (our own limits; an existing GOFLAGS is kept, ours is last so it wins).
# If `systemd-run --user --scope` can apply MemoryMax here (verified through the scope's own memory.max), the command
# runs in that scope with MemoryMax=<memory> (default 16G, or SDO_CAPPED_MEMORY_MAX); otherwise it runs under
# nice/ionice. Needs no root. Which mode ran is printed on stderr.
set -u

memory="${SDO_CAPPED_MEMORY_MAX:-16G}"
while [ $# -gt 0 ]; do
    case "$1" in
        --memory) memory="${2:?--memory needs a value}"; shift 2 ;;
        --) shift; break ;;
        *) break ;;
    esac
done
if [ $# -eq 0 ]; then
    echo "usage: $0 [--memory SIZE] -- command [args...]" >&2
    exit 2
fi

export GOFLAGS="${GOFLAGS:+$GOFLAGS }-p=4"
export GOMAXPROCS=8

scope_applies_memory_max() {
    command -v systemd-run >/dev/null 2>&1 || return 1
    local limit
    limit="$(systemd-run --user --scope -q -p "MemoryMax=$memory" \
        sh -c 'cat "/sys/fs/cgroup$(cut -d: -f3 /proc/self/cgroup)/memory.max"' 2>/dev/null)" || return 1
    [ -n "$limit" ] && [ "$limit" != "max" ]
}

if scope_applies_memory_max; then
    echo "capped_run: systemd scope MemoryMax=$memory, GOFLAGS=$GOFLAGS GOMAXPROCS=$GOMAXPROCS" >&2
    exec systemd-run --user --scope -q -p "MemoryMax=$memory" -- "$@"
fi

echo "capped_run: no memory cap available (systemd --user scope unusable); nice/ionice, GOFLAGS=$GOFLAGS GOMAXPROCS=$GOMAXPROCS" >&2
if command -v ionice >/dev/null 2>&1; then
    exec nice -n 10 ionice -c 3 "$@"
fi
exec nice -n 10 "$@"
