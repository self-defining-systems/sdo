#!/usr/bin/env bash
# Keep every package/test, isolating the expensive CLI/broker parity matrix.
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root/sdo/operator"
flags=(-race -parallel=2 -count=1 -timeout=4m)
case "${1:?select acceptance, acceptance-second, broker, compiler or remaining}" in
  acceptance) go test "${flags[@]}" -run '^TestCLIAndBrokerAgreeOnAcceptance$' ./broker ;;
  acceptance-second) go test "${flags[@]}" -run '^TestCLIAndBrokerAgreeOnAcceptanceSecondShard$' ./broker ;;
  compiler) go test "${flags[@]}" ./builder ./memory/validate ;;
  broker) go test "${flags[@]}" -skip '^TestCLIAndBrokerAgreeOnAcceptance' ./broker ;;
  remaining)
    inventory="$(go list ./...)"
    packages=()
    while IFS= read -r package; do
      if [[ "$package" != sdo.dev/operator/broker && "$package" != sdo.dev/operator/builder && "$package" != sdo.dev/operator/memory/validate ]]; then packages+=("$package"); fi
    done <<< "$inventory"
    test "${#packages[@]}" -gt 0
    go test "${flags[@]}" "${packages[@]}"
    ;;
  *) echo 'select acceptance, acceptance-second, broker, compiler or remaining' >&2; exit 2 ;;
esac
