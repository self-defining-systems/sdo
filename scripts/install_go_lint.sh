#!/usr/bin/env bash
# Pin the binary independently of the project's Go module dependencies.
set -euo pipefail
version=2.11.4
cache="${SDO_GO_TOOL_CACHE:-${XDG_CACHE_HOME:-$HOME/.cache}/sdo-tools}"
binary="${cache}/golangci-lint-${version}/golangci-lint"
if [[ ! -x "$binary" ]]; then
    platform="$(uname -s | tr '[:upper:]' '[:lower:]')"
    case "$(uname -m)" in
        x86_64) arch=amd64 ;;
        aarch64|arm64) arch=arm64 ;;
        *) echo 'Unsupported golangci-lint architecture' >&2; exit 1 ;;
    esac
    scratch="$(mktemp -d)"
    trap 'rm -rf "$scratch"' EXIT
    archive="golangci-lint-${version}-${platform}-${arch}.tar.gz"
    release="https://github.com/golangci/golangci-lint/releases/download/v${version}"
    curl --fail --silent --show-error --location --connect-timeout 10 --max-time 60 "$release/$archive" -o "$scratch/$archive"
    curl --fail --silent --show-error --location --connect-timeout 10 --max-time 60 "$release/golangci-lint-${version}-checksums.txt" -o "$scratch/checksums"
    (
        cd "$scratch"
        # Only check the requested archive, not other release assets.
        awk -v asset="$archive" '$2 == asset { print }' checksums > selected-checksum
        test -s selected-checksum
        if command -v sha256sum >/dev/null; then
            sha256sum --check selected-checksum >&2
        else
            shasum -a 256 --check selected-checksum >&2
        fi
    )
    tar -xzf "$scratch/$archive" -C "$scratch"
    mkdir -p "$(dirname "$binary")"
    install -m 755 "$scratch/golangci-lint-${version}-${platform}-${arch}/golangci-lint" "$binary"
fi
if ! "$binary" version | grep -Fq "version $version "; then
    echo "Expected golangci-lint $version at $binary" >&2
    exit 1
fi
printf '%s\n' "$binary"
