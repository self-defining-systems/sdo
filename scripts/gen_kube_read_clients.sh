#!/usr/bin/env bash
# Generate/check read-only client facades from the SDK's pinned client-go module.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
# "go list -m" prints an empty directory, without failing, for a module that is
# not in the module cache yet (a fresh CI runner); download it first.
go -C sdo/operator/sdk mod download k8s.io/client-go
module_dir="$(go -C sdo/operator/sdk list -m -f '{{.Dir}}' k8s.io/client-go)"
[ -n "$module_dir" ] || { echo "k8s.io/client-go is not in the module cache" >&2; exit 1; }
go run scripts/gen_kube_read_clients.go "$module_dir" "$@"
