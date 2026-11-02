#!/usr/bin/env bash
# Compilation, all fuzz targets, minimization and shutdown share one wall cap.
set -euo pipefail
if [[ "${1:-}" != --within-cap ]]; then
    exec timeout --signal=TERM --kill-after=5s 295s bash "$0" --within-cap
fi
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root/sdo/operator"
go test -run '^$' -fuzz '^FuzzHistoryStorageSchedule$' -fuzztime=60s \
    -fuzzminimizetime=5s -parallel=2 -timeout=90s ./apihistory
go test -run '^$' -fuzz '^FuzzProxyHistorySchedule$' -fuzztime=60s \
    -fuzzminimizetime=5s -parallel=2 -timeout=90s ./kubeproxy
go test -run '^$' -fuzz '^FuzzEvidenceHistorySchedule$' -fuzztime=60s \
    -fuzzminimizetime=5s -parallel=2 -timeout=90s ./operatorcli
