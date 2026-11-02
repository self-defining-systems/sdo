#!/usr/bin/env bash
# Compilation, fuzzing and shutdown together have a five-minute wall cap.
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root/sdo/operator/runtime"
exec timeout --signal=TERM --kill-after=5s 295s \
    go test -run '^$' -fuzz '^FuzzSimIncidentSchedule$' -fuzztime=4m -parallel=2 -timeout=285s .
