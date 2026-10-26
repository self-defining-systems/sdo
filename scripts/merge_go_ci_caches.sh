#!/usr/bin/env bash
# Union immutable Go compiler entries after restoring each archive once.
set -euo pipefail
normal_cache="$1"
race_cache="$2"
test -d "$normal_cache"
test -d "$race_cache"
# Both paths are under runner.temp. Hard links avoid a second large cache copy;
# existing content-addressed entries stay untouched. Module downloads are owned
# solely by the normal archive and never participate in this merge.
cp -aln "$race_cache/." "$normal_cache/"
