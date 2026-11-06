#!/usr/bin/env bash
# Measure the compressed size of the paths a main-only cache save will upload. Writes size_mb and oversize
# to $GITHUB_OUTPUT so the save step can be skipped; never fails itself (the job's
# final step fails when oversize=true, so the warning cannot be missed and the
# previous good cache stays in place). Usage: ci_cache_guard.sh <limit_mb> <path>...
set -euo pipefail
limit_mb="$1"
shift
# The limit is on the compressed archive, which is what actions/cache uploads and
# restores (raw Go build caches compress about 4:1).
existing=()
for path in "$@"; do
    expanded="${path/#\~/$HOME}"
    [[ -e "$expanded" ]] && existing+=("$expanded")
done
size_mb=0
if [[ ${#existing[@]} -gt 0 ]]; then
    compress=(zstd -1 -T0 -c)
    command -v zstd > /dev/null || compress=(gzip -1 -c)
    bytes=$(tar -cP "${existing[@]}" 2> /dev/null | "${compress[@]}" | wc -c)
    size_mb=$(((bytes + 1048575) / 1048576))
fi
oversize=false
if ((size_mb > limit_mb)); then
    oversize=true
    echo "::error::Go cache archive is ${size_mb} MB, over the ${limit_mb} MB limit; not saving it (prune the cache or raise cache_limit_mb in .github/ci_budgets.json)"
fi
echo "cache size: ${size_mb} MB (limit ${limit_mb} MB)"
if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
    echo "size_mb=${size_mb}" >> "$GITHUB_OUTPUT"
    echo "oversize=${oversize}" >> "$GITHUB_OUTPUT"
fi
