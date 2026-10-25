#!/usr/bin/env bash
# Seeds run under -race in test_go_ci.sh; mutate briefly on PRs, 30s nightly.
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
fuzz_time="${1:-1s}"
run_fuzz() {
  (cd "$repo_root/$1" && timeout 120s go test -run '^$' -fuzz "^$2$" -fuzzminimizetime=1s -fuzztime="$fuzz_time" -parallel=2 -timeout=2m "${3:-.}")
}
run_fuzz sdo/operator/sdk FuzzObjectRefParseFormat
run_fuzz sdo/operator/sdk FuzzObjectRefTypedRoundTrip
run_fuzz sdo/operator FuzzStateChangeRefMatchesObservedLabel ./outcome_derivation
run_fuzz sdo/operator FuzzControllerObservationBoundary ./outcome_derivation
run_fuzz benchmarks/sregym/protocol/native_receipt_go FuzzNativeReceiptBoundary
run_fuzz benchmarks/sregym/protocol/submission_go FuzzSubmissionPhaseBoundary
