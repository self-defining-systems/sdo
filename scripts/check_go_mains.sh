#!/usr/bin/env bash
# Compile every Go program under benchmarks/ against the current operator API.
# Those programs sit outside the operator tests, so a runtime API change can
# break them without failing `go test`; this catches it in seconds.
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# A package's tests can accidentally supply declarations missing from production.
# Compile without test sources before any generated-workspace or integration
# suite. `go list -export` compiles every package, main ones included, and
# skips the link step that `go build ./...` repeats for each command.
for module in "$root/sdo/operator" "$root/sdo/operator/runtime"; do
    (cd "$module" && go list -export ./... > /dev/null)
done

# Standalone modules (own go.mod, fixtures and generated seeds excluded).
while IFS= read -r mod; do
    (cd "$(dirname "$mod")" && go vet ./...)
done < <(find "$root/benchmarks" -name go.mod -not -path '*/seeds/*' -not -path '*/operational_memory/*' | sort)

# Programs with no module of their own run via `go run` inside the operator
# runtime module, so vet them in that context.
while IFS= read -r program; do
    dir="$(dirname "$program")"
    while [[ "$dir" != "$root" && ! -f "$dir/go.mod" ]]; do dir="$(dirname "$dir")"; done
    [[ "$dir" != "$root" ]] && continue
    (cd "$root/sdo/operator/runtime" && go vet "$program")
done < <(grep -rl --include='*.go' '^package main' "$root/benchmarks" | sort)

# Broker replay builds replay_go in a private workspace that *replaces* the
# operator modules instead of `use`-ing them (benchmarks/sregym/fastloop/replay.py).
# Replaced modules contribute their own requirements, so a stale transitive
# requirement (the monolithic google.golang.org/genproto) can make an import
# ambiguous there while every other build of the module is fine. Build it the
# way replay does.
replay_workspace="$(mktemp -d)"
trap 'rm -rf "$replay_workspace"' EXIT
{
    echo "go 1.26.8"
    echo "use $root/benchmarks/sregym/fastloop/replay_go"
    echo "replace ("
    for name in "" /core /sdk /runtime /contracts; do
        echo " sdo.dev/operator$name => $root/sdo/operator$name"
    done
    echo ")"
} > "$replay_workspace/go.work"
(cd "$root/benchmarks/sregym/fastloop/replay_go" && GOWORK="$replay_workspace/go.work" go list -export ./... > /dev/null)
