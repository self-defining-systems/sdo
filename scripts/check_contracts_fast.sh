#!/usr/bin/env bash
# Fast cross-language contract parity: Go constants, Python models and
# protos must agree. These are the hosted-CI contract tests that run in
# seconds, so a stale parity source fails before push, not 20 min later.
#
# Selection is by module and naming convention, so new tests join without
# editing this script:
#   - every test in the small Go contract modules
#   - runtime tests named *MatchProto, *MapsToProto, Proto*, TrafficProto*,
#     *ClosedEnums or *MatchesGenerated*
#   - every Python test under tests/unit/sdo/contracts
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

for module in sdo/operator/contracts \
    benchmarks/sregym/protocol/native_receipt_go \
    benchmarks/sregym/protocol/submission_go; do
    echo "==> contract parity: $module"
    (cd "$root/$module" && go test -count=1 -timeout=60s ./...)
done

echo "==> contract parity: runtime proto mirrors"
(
    cd "$root/sdo/operator/runtime"
    go test . -count=1 -timeout=60s \
        -run '^Test([A-Za-z0-9]*(MatchProto|MapsToProto|ClosedEnums)|(Traffic)?Proto[A-Z][A-Za-z0-9]*|[A-Za-z0-9]*MatchesGenerated[A-Za-z0-9]*)$'
)

echo "==> contract parity: Python contracts"
cd "$root"
uv run --extra test python -m pytest -q -p no:tach -p no:cacheprovider tests/unit/sdo/contracts
