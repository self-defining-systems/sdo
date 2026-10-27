# Restartable lanes and application conversion

## Restartable private-memory sequences

`python -m benchmarks.sregym.fastloop.sequence --plan PLAN` runs adjacent first
and exact-repeat pairs through the ordinary guarded fastloop CLI. Each lane owns
one accepted history; cross-lane transfer is absent. `sequence.json` freezes the
code SHA, lane, environment and row order. Per-row `manifest.json` records the
initial seed and accepted commit; `finished.json` adds the observed final commit,
ancestry, native incident/result/receipt identity and independent oracle verdict.
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
