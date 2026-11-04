#!/usr/bin/env bash
# Installs the pinned kube-apiserver and etcd (envtest) once, from the
# controller-tools GitHub release, verified against a pinned SHA-512, into the
# directory envtestio.Assets looks in. Idempotent. Prints the directory; CI
# exports it as KUBEBUILDER_ASSETS so no test downloads anything.
#   bash scripts/install_envtest.sh            # print the assets directory
# Version: keep in step with envtestio.KubernetesVersion (a unit test checks).
set -euo pipefail
version="1.34.0"
case "$(uname -s)-$(uname -m)" in
    Linux-x86_64) platform=linux-amd64
        sha512=445b4dd543963744925fffd5225413d852821442b244db710057abc3a98cf5a1e139a43b4b97949010360a19a8ffab4971db97daba60d72ddf1cc260cbfe3844 ;;
    Linux-aarch64 | Linux-arm64) platform=linux-arm64
        sha512=06f3b72b133595f463849d8cde7c36307e2e51e1709bded1659694c1bd39491d3133d534d33a7c38df1136c314d1e1ad483684c519c0694f505b703ed2e44ef9 ;;
    Darwin-arm64) platform=darwin-arm64
        sha512=b841e5ffa351b2ea1748093b612bc63e23a36362c90304cec498f1ee29c2858b63c146012a562f39d92031dba68a819a381d71c348b567c9de986c069edfce04 ;;
    Darwin-x86_64) platform=darwin-amd64
        sha512=e92e41a183e8504d3763bd5486418167ce4f6204556d3a53c7abc2e538da639db940f8a1764030d8bd5c5658cbc3eca35e1c1749290b74fc1653da6e517fc995 ;;
    *) echo "no pinned envtest archive for $(uname -s)-$(uname -m)" >&2; exit 1 ;;
esac
cache="${SDO_ENVTEST_CACHE:-${XDG_CACHE_HOME:-$HOME/.cache}/sdo-envtest}"
dest="$cache/bin/k8s/$version-$platform"
if [[ -x "$dest/kube-apiserver" && -x "$dest/etcd" ]]; then
    echo "$dest"
    exit 0
fi
url="https://github.com/kubernetes-sigs/controller-tools/releases/download/envtest-v$version/envtest-v$version-$platform.tar.gz"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
curl --fail --silent --show-error --location --retry 5 --retry-delay 3 --retry-all-errors \
    --connect-timeout 20 --max-time 300 --output "$work/envtest.tar.gz" "$url" >&2
echo "$sha512  $work/envtest.tar.gz" | sha512sum --check --status 2>/dev/null \
    || echo "$sha512  $work/envtest.tar.gz" | shasum -a 512 --check --status \
    || { echo "envtest archive checksum mismatch for $url" >&2; exit 1; }
mkdir -p "$work/x"
tar -xzf "$work/envtest.tar.gz" -C "$work/x" --strip-components=2
mkdir -p "$(dirname "$dest")"
rm -rf "$dest.tmp"
mv "$work/x" "$dest.tmp"
mv "$dest.tmp" "$dest"
echo "$dest"
