#!/bin/sh
# A conformance hook for tests: every verb logs itself to $HOOK_LOG and obeys
# the files in $HOOK_STATE (busy: idle says busy with its content; fail-<verb>: that verb fails).
verb=$1
[ -n "$HOOK_LOG" ] && echo "$verb $BENCH_LANE $BENCH_EXPERIMENT" >> "$HOOK_LOG"
st=${HOOK_STATE:-/nonexistent}
if [ -f "$st/fail-$verb" ]; then cat "$st/fail-$verb"; exit 1; fi
case $verb in
  check) grep -q '"baseline"' "$BENCH_OPERATOR" || { echo "no baseline"; exit 1; } ;;
  prepare) [ -d "$st" ] && echo "$BENCH_REGISTRY" > "$st/registry"; [ -d "$st" ] && touch "$st/prepared"; [ -d "$st" ] && echo "$FUZZOPS_REGISTRY_PORT" > "$st/regport"; [ -d "$st" ] && echo "$BENCH_LANE_PROVIDER $FUZZOPS_LANE_PROVIDER $BENCH_EVIDENCE_MODE $FUZZOPS_ENTRY_HOST $VCLUSTER_HOST_KUBECONFIG" > "$st/provider"; echo "preparing"; echo "{\"pack\": \"$BENCH_PACK\", \"operator_version\": \"test\", \"memory\": \"m1\"}" ;;
  swap) [ -f "$st/prepared" ] || { echo "no prepared operator"; exit 1; }; echo "{\"pack\": \"$BENCH_PACK\", \"operator_version\": \"swapped\", \"memory\": \"m1\"}" ;;
  snapshot) [ -f "$st/prepared" ] || { echo "no prepared operator"; exit 1; }
    # snapshot-kept: the copy is whole and kept, only the operator was not idle afterwards (the hook says so and fails);
    # snapshot-kept-busy: and the operator stays busy
    if [ -f "$st/snapshot-kept" ] || [ -f "$st/snapshot-kept-busy" ]; then touch "$st/golden"; [ -f "$st/snapshot-kept-busy" ] && echo "alarm: incident open" > "$st/busy"
      rm -f "$st/snapshot-kept"; echo "{\"golden\": \"kept\", \"not_idle\": \"idle for less than the quiet period\"}"; exit 1; fi
    touch "$st/golden"; echo "{\"golden\": true}" ;;
  reset) [ -f "$st/golden" ] && [ -f "$st/prepared" ] || { echo "error: no golden copy"; exit 1; }; echo "{\"pack\": \"$BENCH_PACK\", \"operator_version\": \"reset\", \"memory\": \"m1\"}" ;;
  idle) if [ -f "$st/busy" ]; then cat "$st/busy"; exit 1; fi
    if [ -f "$st/starting" ]; then n=$(cat "$st/starting"); [ "$n" -gt 0 ] && { echo $((n-1)) > "$st/starting"; echo "down: controller pod not Ready"; exit 1; }; fi ;;
  watch) echo "watching $BENCH_FAULT"; sleep 3600 ;;
  collect) echo "collected $BENCH_RUN" > "$BENCH_OUT/collected.txt"
    [ -f "$st/incomplete-evidence" ] && echo '{"problems": ["controller.log missing (operator/controller.log)"]}' > "$BENCH_OUT/evidence-manifest.json" ;;
  reset-incidents) echo reset >> "$st/incident-resets" ;;
  teardown) rm -f "$st/prepared" "$st/golden" ;;
  transcripts) [ -n "$BENCH_EVIDENCE_HOST" ] && [ -n "$BENCH_EVIDENCE_RUN" ] || { echo "no sink: no evidence directory"; exit 1; } ;;
esac
exit 0
