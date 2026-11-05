#!/usr/bin/env bash
# Generate/check read-only client facades from the SDK's pinned client-go module.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
# `go list -m` prints no directory for a module the cache lacks (a cold CI
# runner), which the generator would read as an empty facade.
go -C sdo/operator/sdk mod download k8s.io/client-go
module_dir="$(go -C sdo/operator/sdk list -m -f '{{.Dir}}' k8s.io/client-go)"
go run scripts/gen_kube_read_clients.go "$module_dir" "$@"
