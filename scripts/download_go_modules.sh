#!/usr/bin/env bash
# Warm the full graph without rewriting source-bound go.mod/go.sum inputs.
set -euo pipefail
cd "${1:?module directory required}"
alternate="$(mktemp .ci-download-XXXXXX.mod)"
sums="${alternate%.mod}.sum"
trap 'rm -f -- "$alternate" "$sums"' EXIT
cp go.mod "$alternate"
if [[ -f go.sum ]]; then cp go.sum "$sums"; fi
# Keep the alternate beside go.mod so relative replacements retain their owner.
go mod download -modfile "$alternate" all
