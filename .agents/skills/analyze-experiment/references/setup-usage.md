# Setup judge accounting

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

