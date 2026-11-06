#!/usr/bin/env bash
# Keep a saved Go cache to the entries one run used. Go refreshes the mtime of a compiler entry it reads only
# when that mtime is over an hour old, so a bare age cutoff cannot tell "used in this run" from "used in the
# last two hours", and on a busy main the two-hour window holds several generations of every package a merge
# changed. `mark` (right after the restore) ages every entry to three hours; the run then refreshes exactly the
# entries it reads; `trim` (right before the save) deletes whatever is still older than two hours and the module
# zip archives (a module is extracted beside its zip, and the zip is read only to extract it; zips do not compress).
# usage: ci_go_cache.sh mark | trim   (GOCACHE / GOMODCACHE default to the CI runner's paths)
set -euo pipefail
build="${GOCACHE:-$HOME/.cache/go-build}"
modules="${GOMODCACHE:-$HOME/go/pkg/mod}"
case "${1:-}" in
    mark)
        [[ -d "$build" ]] && find "$build" -type f -exec touch -d '3 hours ago' {} +
        ;;
    trim)
        [[ -d "$build" ]] && find "$build" -type f -mmin +120 -delete
        [[ -d "$modules/cache/download" ]] && find "$modules/cache/download" -type f -name '*.zip' -delete
        ;;
    *)
        echo "usage: ci_go_cache.sh mark | trim" >&2
        exit 2
        ;;
esac
exit 0
