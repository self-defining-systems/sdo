#!/usr/bin/env bash
# Measure the paths a main-only cache save will upload. Writes size_mb and oversize
# to $GITHUB_OUTPUT so the save step can be skipped; never fails itself (the job's
# final step fails when oversize=true, so the warning cannot be missed and the
# previous good cache stays in place). Usage: ci_cache_guard.sh <limit_mb> <path>...
set -euo pipefail
limit_mb="$1"
shift
size_mb=0
for path in "$@"; do
    expanded="${path/#\~/$HOME}"
    [[ -e "$expanded" ]] || continue
    size_mb=$((size_mb + $(du -sm "$expanded" | cut -f1)))
done
oversize=false
if ((size_mb > limit_mb)); then
    oversize=true
    level=warning
    [[ "${GITHUB_REF:-}" == refs/heads/main ]] && level=error
    echo "::${level}::Go cache is ${size_mb} MB, over the ${limit_mb} MB limit; not saving it (prune the cache or raise cache_limit_mb in .github/ci_budgets.json)"
fi
echo "cache size: ${size_mb} MB (limit ${limit_mb} MB)"
if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
    echo "size_mb=${size_mb}" >> "$GITHUB_OUTPUT"
    echo "oversize=${oversize}" >> "$GITHUB_OUTPUT"
fi
