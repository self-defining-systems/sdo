#!/usr/bin/env bash
# Share linked binaries between Go invocations (see scripts/linkcache/main.go).
# CI appends the output to $GITHUB_ENV; locally: eval "$(bash scripts/link_cache.sh --shell)".
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
tool="$root/.fastloop/test-bin/linkcache"
if [[ ! -x "$tool" ]]; then
  mkdir -p "$(dirname "$tool")"
  go build -o "$tool" "$root/scripts/linkcache/main.go"
fi
flags="${GOFLAGS:-}"
case " $flags " in *" -toolexec="*) ;; *) flags="$flags -toolexec=$tool" ;; esac
cache="${SDO_LINK_CACHE:-${RUNNER_TEMP:-${TMPDIR:-/tmp}}/sdo-link-cache}"
if [[ "${1:-}" == "--shell" ]]; then
  printf 'export GOFLAGS=%q\nexport SDO_LINK_CACHE=%q\n' "$flags" "$cache"
else
  printf 'GOFLAGS=%s\nSDO_LINK_CACHE=%s\n' "$flags" "$cache"
fi
