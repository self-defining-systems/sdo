#!/usr/bin/env bash
# Real-host smoke test of the lane platform (docs/smoke.md). Builds one small lane on one reserved host through the real
# manager, runs a two-node plan on it, kills and restarts things, and checks the host's cgroups and ledger. Everything it
# makes is named smk-*; everything else on the host is foreign and is never touched.
#
#   benchmarks/lanes/smoke/smoke.sh --host spokane1 --root /scratch/smoke [--host-dir DIR] [--sudo-password-file F]
#
# PASS or FAIL per step with the reason; exit 1 on any FAIL. Cleans up (manager, plan, cluster, ledger entry, directories)
# on any exit.
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
LANES_DIR=$(cd "$HERE/.." && pwd)
HOST= ROOT= HDIR= PWFILE= RUN_SECONDS=${SMOKE_RUN_SECONDS:-45} WAIT_CORES=${SMOKE_WAIT_CORES:-600} KEEP=0
while [ $# -gt 0 ]; do
  case $1 in
  --host) HOST=$2; shift 2 ;;
  --root) ROOT=$2; shift 2 ;;
  --host-dir) HDIR=$2; shift 2 ;;
  --sudo-password-file) PWFILE=$2; shift 2 ;; # only read when the root helper has to be installed; never printed
  --run-seconds) RUN_SECONDS=$2; shift 2 ;;
  --wait-cores) WAIT_CORES=$2; shift 2 ;;
  --keep) KEEP=1; shift ;;
  *) echo "usage: smoke.sh --host H --root DIR [--host-dir DIR] [--sudo-password-file F] [--run-seconds N] [--wait-cores SECONDS] [--keep]" >&2; exit 2 ;;
  esac
done
[ -n "$HOST" ] && [ -n "$ROOT" ] || { echo "smoke.sh: --host and --root are required" >&2; exit 2; }
case $HOST in *[!a-z0-9.-]*) echo "smoke.sh: bad host name" >&2; exit 2 ;; esac
if [ -z "$HDIR" ]; then
  case $HOST in
  spokane1) HDIR=/mnt/nvme1/shli/lanes-smoke ;;
  chelan4 | chelan3) HDIR=/mnt/data/shli/lanes-smoke ;;
  *) echo "smoke.sh: --host-dir is required for host $HOST" >&2; exit 2 ;;
  esac
fi
mkdir -p "$ROOT" || exit 2
ROOT=$(cd "$ROOT" && pwd)
if [ -n "$(ls -A "$ROOT")" ] && [ ! -e "$ROOT/.smoke-root" ]; then echo "smoke.sh: $ROOT is not empty and not a smoke root" >&2; exit 2; fi
touch "$ROOT/.smoke-root"

LANE=smk-1 PLAN=smk-plan NS=smk
BIN=$ROOT/bin/lanes
SSH=(ssh -o BatchMode=yes -o ConnectTimeout=10 "$HOST")
H() { "${SSH[@]}" "$@"; }
fails=0 started=$(date +%s) MGR_PID= COMPLETED=0
say() { printf '%s\n' "$*"; }
pass() { printf 'PASS  %-34s %s\n' "$1" "${2:-}"; }
fail() { printf 'FAIL  %-34s %s\n' "$1" "${2:-}"; fails=$((fails + 1)); }
note() { printf '      %s\n' "$*"; }
LOG=$ROOT/smoke.log
: > "$LOG"
L() { "$BIN" "$@" --root "$ROOT"; }
elapsed() { echo $(($(date +%s) - started)); }
# until_true SECONDS CMD...: poll CMD every 2 s
until_true() { local end=$((SECONDS + $1)); shift; while [ $SECONDS -lt $end ]; do "$@" >/dev/null 2>&1 && return 0; sleep 2; done; return 1; }

mgr_start() {
  cd "$ROOT" || return 1
  nohup "$BIN" serve --root "$ROOT" >> "$ROOT/manager.log" 2>&1 &
  MGR_PID=$!
  cd "$OLDPWD" || true
}
# mgr_stop: SIGTERM, and SIGKILL if the manager is still there after 20 s (said in the log: serve should stop on SIGTERM)
mgr_stop() {
  [ -n "$MGR_PID" ] || return 0
  kill "$MGR_PID" 2>/dev/null
  until_true 20 sh -c "! kill -0 $MGR_PID 2>/dev/null" || { echo "manager $MGR_PID ignored SIGTERM for 20s: killed" >> "$LOG"; MGR_STUCK=1; kill -KILL "$MGR_PID" 2>/dev/null; }
  MGR_PID=
}

cleanup() {
  local rc=$?
  trap - EXIT INT TERM
  say "-- cleanup (${SECONDS}s)"
  mgr_stop
  # a plan and its experiments, and the lane's cluster: only smk-* names are ever passed on
  [ -x "$BIN" ] && "$BIN" plan remove "$PLAN" --root "$ROOT" >> "$LOG" 2>&1
  H "pkill -KILL -f '[s]taging/$LANE/' ; kind get clusters 2>/dev/null | grep -x '$LANE' && kind delete cluster --name '$LANE'; true" >> "$LOG" 2>&1
  if [ -x "$BIN" ]; then
    H "$HDIR/staging/lanes-*/bin/lanes ledger show 2>/dev/null | grep -q '^$LANE ' && $HDIR/staging/lanes-*/bin/lanes ledger prune; true" >> "$LOG" 2>&1
  fi
  if [ "$KEEP" = 0 ]; then
    H "rm -rf '$HDIR'" >> "$LOG" 2>&1
    find "$ROOT" -mindepth 1 -maxdepth 1 ! -name .smoke-root ! -name smoke.log ! -name manager.log -exec rm -rf {} + 2>/dev/null
  fi
  [ "$COMPLETED" = 1 ] || { fails=$((fails + 1)); say "FAIL  incomplete                       the smoke stopped before its last step (interrupted or exited early)"; }
  say "smoke: $([ "$fails" = 0 ] && [ "$rc" = 0 ] && echo PASS || echo FAIL) in $(elapsed)s (log $LOG, manager $ROOT/manager.log)"
  [ "$fails" = 0 ] && [ "$rc" = 0 ] && exit 0
  exit 1
}
trap cleanup EXIT INT TERM

# ---------------------------------------------------------------- 0: build, inventory, workload checkout
step=setup
export GOFLAGS="${GOFLAGS:-} -buildvcs=false"
mkdir -p "$ROOT/bin"
if ! (cd "$LANES_DIR" && go build -o "$BIN" ./cmd/lanes) >> "$LOG" 2>&1; then fail setup "go build of the lanes binary failed (see $LOG)"; exit 1; fi
rm -rf "$ROOT/wl"
mkdir -p "$ROOT/wl"
cp -r "$HERE/adapter.sh" "$HERE/pack" "$ROOT/wl/"
(cd "$ROOT/wl" && git init -q && git add -A && git -c user.name=smoke -c user.email=smoke@localhost commit -qm smoke) >> "$LOG" 2>&1
REV=$(git -C "$ROOT/wl" rev-parse HEAD)
if ! H "mkdir -p '$HDIR/wl' && cd '$HDIR/wl' && rm -rf ./* ./.git" >> "$LOG" 2>&1 || ! tar -C "$ROOT/wl" -cf - . | H "tar -C '$HDIR/wl' -xf -" >> "$LOG" 2>&1; then
  fail setup "cannot reach $HOST over ssh or copy the workload checkout"; exit 1
fi
BLOCK=$(H "cat /sys/fs/cgroup/lanes.slice/cpuset.cpus 2>/dev/null")
if [ -z "$BLOCK" ]; then fail setup "$HOST has no lanes.slice (no reserved block)"; exit 1; fi
LOCAL=$(hostname -s)
cat > "$ROOT/hosts.yaml" <<EOF
# generated by benchmarks/lanes/smoke/smoke.sh
hosts:
  $LOCAL:
    local: true
    staging: $ROOT/staging
    max_builds: 1
    max_load5: 100000
    kubeconfigs: $ROOT/kube
    evidence: $ROOT/evidence
    clusters: []
    kernel_minimums: {fs.inotify.max_user_instances: 1, fs.inotify.max_user_watches: 1, vm.max_map_count: 1}   # the manager's own host runs no lane
  $HOST:
    ssh: $HOST
    staging: $HDIR/staging
    max_builds: 1
    max_load5: 100000
    min_free: 20G
    kubeconfigs: $HDIR/kube
    evidence: $HDIR/evidence
    clusters: [$LANE]
    cluster_up: "\$LANES_INSTALL/deploy/kind-lane.sh up {cluster} {kubeconfig} {evidence}"
    cluster_down: "\$LANES_INSTALL/deploy/kind-lane.sh down {cluster}"
    reserve: {cpus: "$BLOCK"}
    env:
      PATH: "/usr/local/bin:/usr/bin:/bin"
      KIND_NODE_IMAGE: "kindest/node:v1.35.0"
      KIND_WORKERS: "1"
      SMOKE_RUN_SECONDS: "$RUN_SECONDS"
checkouts:
  fuzzops:
    ${REV:0:12}: {$LOCAL: $ROOT/wl, $HOST: $HDIR/wl}
  hooks:
    smoke:
      ${REV:0:12}: {$LOCAL: $ROOT/wl, $HOST: $HDIR/wl}
hook_command: {smoke: "./adapter.sh subject"}
EOF
mkdir -p "$ROOT/plan/specs"
echo '{"ids": ["smk-fault"]}' > "$ROOT/plan/instances.json"
mkdir -p "$ROOT/plan/baseline"
echo baseline > "$ROOT/plan/baseline/base.txt"
for arm in p1 p2; do
  seed=', "baseline": "baseline"'
  [ $arm = p2 ] && seed=', "baseline": "baseline", "seed": "${artifact.memory}"'
  cat > "$ROOT/plan/specs/$arm.json" <<EOF
{
  "app": "pack",
  "workload": "./adapter.sh workload",
  "operator": {"hooks": "smoke"$seed},
  "namespace": "$NS",
  "faults": {"instances": "instances.json", "ids": ["\${item.fault}"]},
  "sequences": [[1]],
  "run": {},
  "ready_for": "5s",
  "ready_timeout": "5m",
  "cap": "5m",
  "lanes": 1,
  "hosts": ["$HOST"],
  "lane": {"cpus": 2, "memory": "6Gi"},
  "keep_lanes": false
}
EOF
done
cat > "$ROOT/plan/plan.yaml" <<EOF
plan: $PLAN
pins: {workload: smoke@${REV:0:12}, subject: smoke@${REV:0:12}}
arms:
  - {name: p1, spec: specs/p1.json}
  - {name: p2, spec: specs/p2.json}
items:
  - {id: a, vars: {fault: smk-fault}}
reps: 1
parallel: 1
artifacts: {memory: {path: "\${run}/exec/memory", verify: "cmd:test -s \\"\$ARTIFACT_PATH/artifact.txt\\""}}
edges: [{from: p1, to: p2, artifact: memory}]
EOF

# ---------------------------------------------------------------- 1: host status and preflight
step=host
out=$(L host adopt "$HOST" 2>&1); rc=$?   # records the block that exists in this root; changes nothing on the host
echo "$out" >> "$LOG"
[ $rc = 0 ] || fail host-adopt "$(echo "$out" | tail -2 | tr '\n' ' ')"
out=$(L host status "$HOST" 2>&1); rc=$?
echo "$out" >> "$LOG"
if [ $rc != 0 ]; then fail host-status "lanes host status $HOST exited $rc: $(echo "$out" | tail -2 | tr '\n' ' ')"; else
  if echo "$out" | grep -qiE 'unhealthy|drift|overlap'; then fail host-status "$(echo "$out" | grep -iE 'unhealthy|drift|overlap' | head -2 | tr '\n' ' ')"; else pass host-status "$(echo "$out" | head -1)"; fi
fi
# a standalone spec (never added) so preflight has something concrete to check
mkdir -p "$ROOT/preflight"
sed -e 's/\${item.fault}/smk-fault/' "$ROOT/plan/specs/p1.json" |
  python3 -c 'import json,sys; s=json.load(sys.stdin); s["name"]="smk-pre"; s["fuzzops"]=sys.argv[1]; s["operator"]["revision"]=sys.argv[1]; s["operator"].pop("baseline",None);s["faults"]["instances"]=sys.argv[2]+"/plan/instances.json"; print(json.dumps(s))' "${REV:0:12}" "$ROOT" > "$ROOT/preflight/smk-pre.json"
out=$(L preflight --spec "$ROOT/preflight/smk-pre.json" 2>&1); rc=$?
echo "$out" >> "$LOG"
if [ $rc != 0 ]; then fail preflight "$(echo "$out" | grep -E 'FAIL' | head -3 | tr '\n' ' ')"; else pass preflight "$(echo "$out" | grep -c PASS) checks"; fi
if ! out=$(L plan add "$ROOT/plan/plan.yaml" 2>&1); then fail plan-add "$out"; echo "$out" >> "$LOG"; exit 1; fi
echo "$out" >> "$LOG"
pass plan-add "$(echo "$out" | tail -1)"

# ---------------------------------------------------------------- 2: the lane is built and p1 runs
INST=
mgr_start
t0=$SECONDS
state() { L plan status "$PLAN" --json 2>>"$LOG" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(json.dumps(d[0] if isinstance(d,list) else d))'; }
nodes() { state | python3 -c 'import json,sys; print(" ".join("%s done=%d inflight=%d failed=%d queued=%d" % (a["arm"],a["done"],a["inflight"],a["failed"],a["queued"]) for a in json.load(sys.stdin)["arms"]))'; }
run_step_pid() { H "pgrep -u \$(id -u) -f '[l]anes step $HDIR/staging/$LANE/run-' | head -1"; }
have_run_step() { [ -n "$(run_step_pid)" ]; }
# a lane that goes down (a failed build) will never start a step: fail at once with its reason
run_or_down() { have_run_step || { L status 2>/dev/null | grep -E "/$LANE +(down|unreachable)" >/dev/null && return 0; return 1; }; }
if until_true "$WAIT_CORES" run_or_down && have_run_step; then
  pass lane-build "kind lane $LANE built and p1's run step started in $((SECONDS - t0))s"
else
  fail lane-build "no run step after $((SECONDS - t0))s: $(L status 2>&1 | grep -iE "$LANE|wait|core|fail" | head -4 | tr '\n' ' ')"
  exit 1
fi
INST=$(H "ls -td $HDIR/staging/lanes-*/bin/lanes 2>/dev/null | head -1")   # the platform install the first step put on the host

# ---------------------------------------------------------------- 3: the lane's cgroup (while the run step is live)
PID=$(run_step_pid)
cg=$(H "cat /proc/$PID/cgroup 2>/dev/null | sed 's/^0:://'")
slice=$(H "ls -d /sys/fs/cgroup/lanes.slice/lanes-smk*1.slice 2>/dev/null | head -1")
ledger=$(H "$INST ledger show 2>/dev/null | grep '^$LANE '")
lcpus=$(echo "$ledger" | sed -n 's/^[^ ]* *cpus \([^ ]*\) .*/\1/p')
problems=
case $cg in */lanes.slice/lanes-smk*1.slice/step) ;; *) problems="$problems step process is in cgroup '$cg', not a leaf of the lane slice;" ;; esac
[ -n "$slice" ] || problems="$problems no lanes-smk-1 slice under lanes.slice;"
if [ -n "$slice" ]; then
  [ -z "$(H "cat $slice/cgroup.procs")" ] || problems="$problems the lane slice itself holds processes (cgroup v2 no-internal-process rule);"
  [ "$(H "grep -c . $slice/step/cgroup.procs")" -ge 1 ] || problems="$problems the step leaf holds no process;"
fi
[ -n "$lcpus" ] || problems="$problems the ledger has no entry for $LANE;"
expand() { python3 -c 'import sys; r=set(); [r.update(range(int(a.split("-")[0]),int(a.split("-")[-1])+1)) for a in sys.argv[1].split(",") if a]; print(" ".join(map(str,sorted(r))))' "$1"; }
inblock=$(python3 -c 'import sys; a=set(sys.argv[1].split()); b=set(sys.argv[2].split()); print("yes" if a and a<=b else "no")' "$(expand "$lcpus")" "$(expand "$BLOCK")")
[ "$inblock" = yes ] || problems="$problems cores '$lcpus' are not inside the block $BLOCK;"
nodes_info=$(H "for n in \$(kind get nodes --name $LANE); do docker inspect -f '{{.Name}} {{.HostConfig.Memory}} {{.HostConfig.CgroupParent}} {{.HostConfig.CpusetCpus}}' \$n; done")
echo "$nodes_info" | grep -q . || problems="$problems no node containers for $LANE;"
while read -r n m p c; do
  [ -n "$n" ] || continue
  case $n in *load-balancer*) continue ;; esac
  [ "${m:-0}" -gt 0 ] 2>/dev/null || problems="$problems node $n has no memory limit;"
  case $p in *lanes-smk*1.slice*) ;; *) problems="$problems node $n cgroup parent is '$p';" ;; esac
  sub=$(python3 -c 'import sys; a=set(sys.argv[1].split()); b=set(sys.argv[2].split()); print("yes" if a and a<=b else "no")' "$(expand "$c")" "$(expand "$lcpus")")
  [ "$sub" = yes ] || problems="$problems node $n cpuset '$c' is not within the lane's cores '$lcpus';"
done <<< "$nodes_info"
if [ -z "$problems" ]; then pass lane-cgroup "step in $cg, slice cores $lcpus inside block, memory.max 6Gi, nodes limited"; else fail lane-cgroup "$problems"; fi

# ---------------------------------------------------------------- 4: kill p1's run step once: retried, recovered in place
CP_BEFORE=$(H "docker inspect -f '{{.Id}}' $LANE-control-plane 2>/dev/null")
H "kill -KILL -\$(ps -o pgid= -p $PID | tr -d ' ')" >> "$LOG" 2>&1
t1=$SECONDS
retried() { grep -q "smk-plan-p1-a-r1-x1" "$ROOT/manager.log" 2>/dev/null; }
if until_true 180 retried; then
  pass step-kill-retry "killed run step $PID; attempt x1 was released after $((SECONDS - t1))s"
else
  fail step-kill-retry "attempt x1 of p1 not released within 180s; plan: $(nodes); manager: $(grep -iE 'died|recover|lane lost' "$ROOT/manager.log" | tail -3 | tr '\n' ' ')"
fi
if until_true 300 have_run_step; then
  CP_AFTER=$(H "docker inspect -f '{{.Id}}' $LANE-control-plane 2>/dev/null")
  if grep -q "step .* died" "$ROOT/manager.log" && grep -qE "recovered after" "$ROOT/manager.log"; then
    pass step-recover "the dead step was cleaned up and the lane recovered; retried run started ($([ "$CP_BEFORE" = "$CP_AFTER" ] && echo 'same cluster' || echo 'cluster rebuilt'))"
  else
    fail step-recover "no 'died' and 'recovered after' in the manager log: $(grep -iE "lane $LANE" "$ROOT/manager.log" | tail -4 | tr '\n' ' ')"
  fi
else
  fail step-recover "the retried run did not start within 300s; $(nodes)"
fi

# ---------------------------------------------------------------- 5: artifact edge, then restart the manager mid-run of p2
p2_pid() { H "pgrep -u \$(id -u) -f '[l]anes step $HDIR/staging/$LANE/run-' | head -1"; }
p1_done() { [ "$(state | python3 -c 'import json,sys; print([a for a in json.load(sys.stdin)["arms"] if a["arm"]=="p1"][0]["done"])')" -ge 1 ]; }
# p1's retried run is live now; wait for it to end and p2 to start (a different run step pid after p1 is done)
p2_started() { p1_done && have_run_step; }
if until_true 300 p2_started; then
  P2PID=$(run_step_pid)
  pass artifact-edge "p1 done and verified; p2 released (run step $P2PID)"
  mgr_stop
  sleep 5
  mgr_start
  alive() { H "kill -0 $P2PID 2>/dev/null"; }
  alive && alive_after=yes || alive_after=no
  if [ "$alive_after" = no ]; then note "step $P2PID ended during the restart gap"; fi
  adopted() { grep -qE "lane $LANE: .*(finished ok=true|-> free)" "$ROOT/manager.log" && state | python3 -c 'import json,sys; sys.exit(0 if json.load(sys.stdin)["complete"] else 1)'; }
  if until_true 300 adopted; then
    pass manager-restart "manager restarted mid-run; the step ($P2PID) was adopted and the plan completed"
  else
    fail manager-restart "after restarting the manager the plan did not complete in 300s: $(nodes); $(grep -iE "lane $LANE" "$ROOT/manager.log" | tail -3 | tr '\n' ' ')"
  fi
else
  fail artifact-edge "p2 did not start within 300s of p1; $(nodes)"
fi
seeded=$(grep -rl '"seeded":true' "$ROOT"/smk-plan-p2-* 2>/dev/null | head -1)
if [ -n "$seeded" ]; then pass artifact-seeded "p2's run saw p1's artifact as its seed ($seeded)"; else fail artifact-seeded "no p2 run record with \"seeded\":true under $ROOT/smk-plan-p2-*"; fi

# ---------------------------------------------------------------- 6: remove the plan: ledger entries and clusters are released
mgr_stop
L plan remove "$PLAN" >> "$LOG" 2>&1 || fail plan-remove "lanes plan remove failed"
mgr_start
released() { ! H "kind get clusters | grep -x $LANE" && ! H "$INST ledger show | grep '^$LANE '"; }
if until_true 180 released; then pass release "cluster $LANE deleted and its ledger entry released"; else
  fail release "after plan remove: clusters: $(H "kind get clusters | grep smk" | tr '\n' ' ') ledger: $(H "$INST ledger show | grep smk" | tr '\n' ' ')"
fi

COMPLETED=1
