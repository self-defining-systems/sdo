#!/usr/bin/env bash
# Download every module in the build list of the workspace's Go modules into the module cache.
#
# The controller end-to-end sandbox builds generated detector modules with GOPROXY=off and the
# host's module cache mounted read-only, so the cache must already hold the whole module graph
# (including go.mod files of transitive modules). A hosted runner's cache restored from an older
# run did not, and the nightly failed with "module lookup disabled by GOPROXY=off". `go mod download`
# alone is not enough: `go mod tidy` also loads the tests of dependencies (a gonum package that a
# grpc test imports), so the whole build list is fetched (`all`, about 1 GB). Already-cached modules
# cost nothing; the module list is the one the CI selection rules use.
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
mapfile -t modules < <(python3 -c '
import json, sys
# Detector modules depend on the operator modules only; the benchmark modules have local-only
# requirements that `go mod download all` cannot resolve.
print("\n".join(m for m in json.load(open(sys.argv[1]))["go_workspace_modules"] if m.startswith("sdo/operator")))
' "$root/.github/ci_selection.json")
for module in "${modules[@]}"; do
    (cd "$root/$module" && go mod download all)
done
