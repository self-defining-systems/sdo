# Restartable lanes and application conversion

## Restartable private-memory sequences

`python -m benchmarks.sregym.fastloop.sequence --plan PLAN` runs adjacent first
and exact-repeat pairs through the ordinary guarded fastloop CLI. Each lane owns
one accepted history; cross-lane transfer is absent. `sequence.json` freezes the
code SHA, lane, environment and row order. Per-row `manifest.json` records the
initial seed and the pre-reset workspace head; `finished.json` adds the observed final commit,
ancestry, native incident/result/receipt identity and independent oracle verdict.
For private native lanes, `results/RUN_ID/reset-generation.json`
(`sdo.fastloop-reset-generation/v1`) is published atomically by the successful
row-start reset before setup or injection. It binds a unique generation and run
identity to the actual post-reset `accepted_before`, planned commit, workspace,
and separate `pre_reset_head`. The sequence consumes that exact attempt's receipt
and verifies its start against the seed or predecessor's accepted final commit;
a missing, foreign or inconsistent generation holds without reinjection. It
never substitutes the requested seed or the journal's pre-reset head for an
observed reset result. The original ancestry check remains mandatory, so a real
rewind after reset still blocks advancement.

These bounds describe observed memory, not causal attribution. Continue to use
the native request and broker receipt for exact incident inputs.

On restart, completed rows are skipped. A terminal native result written before
the journal commit is adopted after checking recovery and acknowledged learning.
An incomplete row holds the lane for native recovery and is never reinjected.
`complete.json` means all rows passed the advance gate, not that every repair
passed its official oracle. `program_trust_threshold: 3` records experiment intent
only: automatic trust gating is retired and the value grants no authority.

## Warm-lane application retirement

`fastloop retire-app` requires the old environment, a distinct next run directory,
next namespace/problem/seed/images, and the current lane lease. Its fixed target
set is the old app namespace plus that app's SDO control namespace. Pending
native closure/reflection or active worker Jobs block retirement. Every namespace
delete checks the lease, waits for deletion, and never targets infrastructure or
unrelated app namespaces. The next app deploys through `up --reuse-cluster-only`
using the same fenced `KUBECONFIG`; failed reuse admission never falls back to
kind creation. `app-retirement-started.json` preserves intent; the completed
`app-retirement.json` (`sdo.fastloop-app-retirement/v1`) records the exact retired
set and the next app's passing `lane_resources` evidence. Missing completion
means conversion is incomplete; it grants no injection or learning credit.

Single-row SDO results with `cleanup_skipped_reason: row_start_tier2` advance
without a recovery timestamp/oracle or a verified cleanup pass. Their independent
grade is unchanged; the next invocation's attempt marker requires a tier-2 app
recreate. Acknowledged, correlated learning is still required by the sequence.

Baseline observation exhaustion before injection is a safe retry, identified by
`injection: {state: not_injected, stage: pre_injection}` together with
`infra_error: never_injected_retry`. The sequence writes `never-injected-N.json`
with the original native-result path and `retry.json` with `next_attempt`.
`attempt-N.json` is durable before execution, so interruption of a retry still
holds uncertain native work. Restarting a proven never-injected failure executes the row in a fresh `RUN_ID-INDEX-retry-N` result directory;
the row's slot and memory lineage stay unchanged. Earlier result directories
remain evidence, not injected incidents. An absent result, an injection-stage
failure or uncertain native work does not receive retry permission.

`results/RUN_ID/lane-plan.json` records offline admission before any row wipe or
setup. `not_runnable_on_lane` names unsatisfied worker-count/cluster-feature or
injector Secret-read requirements. These rows have no injection/grade/learning
credit. `check-plan --lane-environment RUN_DIR` exposes the same decisions and
filtered `runnable_problem_ids` for a sweep planner.
