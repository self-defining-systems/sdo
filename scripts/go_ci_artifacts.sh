#!/usr/bin/env bash
# Transfer complete prepared builds without relying on cache retention or ref scope.
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
action="${1:?pack or unpack required}"
mode="${2:?normal, python or race required}"
archive="${3:?artifact directory required}"
case "$action" in pack|unpack) ;; *) echo "unknown artifact action: $action" >&2; exit 1 ;; esac
names=(compiler)
paths=("${GOCACHE:?GOCACHE must select the compiler cache for this mode}")
case "$mode" in
  normal)
    names+=(modules tools)
    paths+=("${GOMODCACHE:-$HOME/go/pkg/mod}" "$root/.fastloop/test-bin")
    ;;
  python)
    names=(modules tools)
    paths=("${GOMODCACHE:-$HOME/go/pkg/mod}" "$root/.fastloop/test-bin")
    ;;
  race)
    if [[ "$action" == pack ]]; then
      mkdir -p "$archive"
      exec python3 "$root/scripts/go_ci_race_artifact.py" --plan "$root/.fastloop/test-bin/pr-test-plan.json" --output "$archive"
    fi
    ;;
  *) echo "unknown artifact mode: $mode" >&2; exit 1 ;;
esac
mkdir -p "$archive"
for index in "${!names[@]}"; do
  file="$archive/${names[$index]}.tar.zst"
  directory="${paths[$index]}"
  if [[ "$action" == pack ]]; then
    tar -I 'zstd -T2 -3' -cf "$file" -C "$directory" .
  else
    mkdir -p "$directory"
    tar -I zstd -xf "$file" -C "$directory"
  fi
done
