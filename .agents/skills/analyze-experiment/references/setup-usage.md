# Setup and responder accounting

Native exports retain health-judge turn logs separately at
`sdo_runtime/setup/<sha256(canonical AgentTask)>/setup-usage.jsonl`.
The writable setup PVC mount and usage destination are provisioned before
worker launch, so a failed result publication cannot discard completed-turn
accounting, including model turns rejected for invalid structured output. This is setup cost, never incident responder or reflection cost.
Each JSONL record retains provider/model, session ID, model requests, token
breakdown and `cost_usd`. `cost_source=provider` means reported dollars;
`agentshim-pricing` means the installed agentshim table priced observed tokens.
`pricing-unavailable` explicitly records a missing model price as null, never
zero; add the model to the pricing table before claiming a total. A reused
complete result starts no worker and adds no turn. Old aborted attempts without
these logs remain unavailable; the change cannot reconstruct their usage.


## Interrupted responders

Responder accounting survives interrupted workers through worker-owned neutral
checkpoints in the incident session's `usage/<turn_id>.json` files. Each snapshot
is cumulative for one process turn, bound to incident, task and `responder`
phase. Runtime replaces the incident aggregate from the latest file for each
turn at completion or retirement; never add these snapshots to final result
usage or to each other over time. Distinct continuation and restart turns are
summed once. `usage.partial: true` means at least one included turn did not
finish accounting; totals are observed lower bounds. Missing checkpoints are
unknown, not measured zero. Optional costs and request counts remain unknown
when any included turn lacks them. Setup and reflection accounting are separate;
a resumed provider transcript's whole-session totals can include both phases.
