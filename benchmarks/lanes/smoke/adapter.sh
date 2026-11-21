#!/bin/sh
# The smoke test's adapter (docs/smoke.md): `adapter.sh workload VERB` and `adapter.sh subject VERB`, run in a throwaway
# git checkout the smoke makes on each host. The app is two pause pods; the "fault" scales them to 0 and `run` holds it for
# SMOKE_RUN_SECONDS. Nothing here is a real workload: it is the smallest one that exercises every platform seam.
set -eu
role=$1 verb=$2
ns=${FUZZOPS_NAMESPACE:-smk}
k() { kubectl --context "kind-$BENCH_LANE" "$@"; }
dir=$(cd "$(dirname "$0")" && pwd)
seed_of() { sed -n 's/.*"seed" *: *"\([^"]*\)".*/\1/p' "${BENCH_OPERATOR:-/dev/null}"; }
case $role in
subject)
  case $verb in
  check | idle | teardown | collect | snapshot | reset | reset-incidents | watch | transcripts) echo '{"ok":true}' ;;
  prepare | swap) printf '{"pack":"%s","operator_version":"smoke","memory":"%s"}\n' "${BENCH_PACK:-}" "$(seed_of)" ;;
  preflight) echo '{"checks":[{"check":"smoke-subject","ok":true,"detail":"adapter reachable"}]}' ;;
  *) echo '{"unsupported":true}'; exit 64 ;;
  esac ;;
workload)
  act=${WORKLOAD_ACTION:-}
  case $verb in
  identity)
    mod=false
    [ -z "$(git -C "$dir" status --porcelain)" ] || mod=true
    printf '{"revision":"%s","modified":%s}\n' "$(git -C "$dir" rev-parse HEAD)" "$mod" ;;
  app-up)
    case $act in
    check-pack) test -f "$dir/pack/app.json" && echo '{"ok":true}' ;;
    up)
      k create namespace "$ns" --dry-run=client -o yaml | k apply -f - >/dev/null
      k -n "$ns" apply -f "$dir/pack/app.yaml" >/dev/null
      k -n "$ns" rollout status deploy/tiny --timeout=120s >&2
      echo '{"ok":true}' ;;
    down) k delete namespace "$ns" --ignore-not-found --timeout=120s >/dev/null; echo '{"ok":true}' ;;
    *) echo '{"unsupported":true}'; exit 64 ;;
    esac ;;
  app-snapshot | footprint) echo '{"unsupported":true}'; exit 64 ;;
  app-reset) # undo a fault a dead run left; nothing to undo when the app is not there yet
    k -n "$ns" get deploy/tiny >/dev/null 2>&1 && k -n "$ns" scale deploy/tiny --replicas=2 >/dev/null
    echo '{"ok":true}' ;;
  gate)
    ready=$(k -n "$ns" get deploy tiny -o jsonpath='{.status.readyReplicas}')
    [ "${ready:-0}" = 2 ] || { echo "tiny has ${ready:-0}/2 ready" >&2; exit 1; }
    echo '{"ok":true}' ;;
  run)
    case $act in
    run)
      seeded=false
      seed=$(seed_of)
      [ -n "$seed" ] && [ -s "$seed/artifact.txt" ] && seeded=true
      k -n "$ns" scale deploy/tiny --replicas=0 >/dev/null
      mkdir -p "$FUZZOPS_EXEC_OUT/memory"
      echo "learned-by-$BENCH_EXPERIMENT" > "$FUZZOPS_EXEC_OUT/memory/artifact.txt"
      sleep "${SMOKE_RUN_SECONDS:-20}"
      k -n "$ns" scale deploy/tiny --replicas=2 >/dev/null
      k -n "$ns" rollout status deploy/tiny --timeout=120s >&2
      printf '{"valid":true,"operator":"smoke","seeded":%s,"lifecycle":{"inject_at":"%s"}}\n' "$seeded" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" ;;
    active) n=$(k -n "$ns" get deploy tiny -o jsonpath='{.spec.replicas}'); [ "$n" = 0 ] && echo '{"active":true}' || echo '{"active":false}' ;;
    undo) k -n "$ns" scale deploy/tiny --replicas=2 >/dev/null; echo '{"active":false}' ;;
    esac ;;
  grade) echo '{"valid":true}' > "${WORKLOAD_RESULTS:?}/grades.json"; echo '{"ok":true}' ;;
  *) echo '{"unsupported":true}'; exit 64 ;;
  esac ;;
esac
