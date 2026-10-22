#!/usr/bin/env bash
# Fast architecture checks, shared by static analysis and architecture CI.
set -euo pipefail
if [[ "${SDO_CORE_CHECK_WITH_DEADLINE:-}" != 1 ]]; then
    exec timeout 55s env SDO_CORE_CHECK_WITH_DEADLINE=1 bash "$0"
fi
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
linter="$("$root/scripts/install_go_lint.sh")"
# One bounded invocation per module. The registry is deliberately small.
while read -r module package; do
    [[ -z "$module" || "$module" == \#* ]] && continue
    (
        cd "$root/$module"
        "$linter" run --config "$root/.golangci.yml" "$package"
    )
done < "$root/scripts/go_pure_packages.txt"
(
    cd "$root/sdo/operator"
    go test ./internal/purity -count=1 -timeout=45s
)

# Prove the committed linter configuration is active, including import aliases.
negative="$(mktemp)"
trap 'rm -f "$negative"' EXIT
if (cd "$root/sdo/operator" && "$linter" run --config "$root/.golangci.yml" ./internal/purity/testdata/outcome) >"$negative" 2>&1; then
    echo "Impure linter fixture unexpectedly passed" >&2
    exit 1
fi
for expected in depguard forbidigo 'clock.Now' 'random.Int'; do
    if ! grep -Fq "$expected" "$negative"; then
        cat "$negative" >&2
        echo "Missing expected architecture diagnostic: $expected" >&2
        exit 1
    fi
done
