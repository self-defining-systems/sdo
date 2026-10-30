#!/usr/bin/env bash
# Transfer compiler-free Python inputs without relying on cache retention or ref scope.
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
action="${1:?pack or unpack required}"
mode="${2:?python required}"
archive="${3:?artifact directory required}"
case "$action" in pack|unpack) ;; *) echo "unknown artifact action: $action" >&2; exit 1 ;; esac
if [[ "$mode" != python ]]; then
  echo "only compiler-free Python artifacts are supported: $mode" >&2
  exit 1
fi
names=(modules tools)
paths=("${GOMODCACHE:-$HOME/go/pkg/mod}" "$root/.fastloop/test-bin")
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
