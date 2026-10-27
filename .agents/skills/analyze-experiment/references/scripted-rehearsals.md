# Scripted repair rehearsals

When `--require-faulty-oracle` is enabled, `pre-repair-oracle.json` records the
aggregate official oracle while the controller is paused, before repair. A passing
oracle or any nested oracle `error`/`infra_error` rejects injection as unproven;
inspect the invalid-injection row and full cleanup rather than counting a repair.
This file contains benchmark evidence and must never enter a production request.
RH3 single-slot runs retain their hand-written directive and generated stream and
registrations beside the usual result files. Action receipts from `sdo act` are
not strict native incident receipts. Require the strict native receipt parser,
broker source/outcome commits, the accepted lock's zero-gap evidence, and verified
full seed recovery before reporting a slot as passed. The replay's no-change
reflection and zero model usage establish no learned-program trust credit.

Credential-slot rehearsals request `--cleanup-passes 2` on the same live driver.
`cleanup_passes_verified` counts full seed-compatible recovery, seed reconciliation,
client refresh, healthy completed rollouts and a passing official recovery oracle
for each pass. A second-pass failure stops reuse even with continue-on-error;
`full_cleanup_verified` is false for any recovery failure. Require two passes on
credential slots, followed by a healthy start of the next slot. Legacy records
default the count to zero and do not establish repeated-cleanup evidence.

Native Go broker seeds may lack the historical Python lifecycle-provenance file.
The adapter routes a current-schema goal, architecture and health bundle through
the Go seeded installer, which independently validates it. Do not add fabricated
legacy provenance to make a native seed look like a historical lifecycle run.

Source advances also require the native health-refresh task. A scripted refresh
may retain the complete accepted generic health bundle only with the same goal
and the same selected deployed resource identities as the accepted lock. It must
reject missing health detectors, new topology or a changed objective. Require its
typed result and the independent Go broker validation; it provides no model or
learning credit. Large non-manifest assets remain part of the source fingerprint
through streaming, while YAML parsing retains its bounded input budget.

The native health-refresh request carries the human objective extracted from the accepted goal document, without its YAML front matter or `# Health objective` heading. A scripted callback must compare that extracted objective and hash it for the typed result; comparing the complete Markdown document rejects a valid native refresh. Use a complete accepted goal document in callback transport regression fixtures.

When source repairs require health adoption, the restarted controller resumes the open investigation, the pending closure and the final ACK of an already committed native closure; nothing is retired for a moved repository head (STALE3 removed that class). Large astronomy healthy snapshots may exceed 1 MiB each; the writer preserves complete captures within its unchanged 8 MiB aggregate bound.

The complete native receipt may reach the broker ledger before the controller persists its receipt state. After health adoption, the controller resumes that frozen closure’s idempotent broker effect, returning the existing receipt without publishing again. An absent/incomplete native receipt still fails closed. The restart fixture must cover both sides of the controller receipt persistence boundary.

A complete authenticated native receipt is recovered as an existing decision. Its replay does not re-run the independent closure decision after adoption. The durable effect identity still applies; incomplete receipts use the ordinary live recheck, which runs under the detectors now in force (a changed observation scope never supersedes a closure).

## Harness rehearsal gate

`python -m benchmarks.sregym.assurance.rehearse_harness --code-root <freeze> --lane <id>` is the gate a new harness passes on one lane before any live row. It builds one private toy kind cluster (`reh<lane>-`) from the freeze's source mount and runs three scenarios serially with scripted workers and no model: `baseline` (learn then reuse, RH3 machinery), `p51-restart-in-pause` (controller Pod replaced inside the injection pause), and `p52-source-repair-refresh` (the incident's own source repair, then a judge refresh that rewrites a health detector, then closure). It writes `rehearse-harness-summary.json` (per-scenario checks, `passed`, wall `seconds` and `up_seconds`) beside the run directory and exits non-zero on any failed check. Time is reported, never a verdict: bring-up, reset and each stream end on their owner's state, with the owner's own deadline as the only outer cap. Each scenario asserts strict native receipts and a broker outcome commit that is an ancestor of the accepted checkout; `p51` also needs `restart-in-pause.jsonl` (old and new controller UID) and no `resume observation ... context deadline exceeded` controller log; `p52` needs a health-detector path changed between the receipt's `proposal_commit` and `outcome_commit`. A scenario whose trigger did not fire fails instead of passing vacuously.

## Batch rehearsal on real apps

`python -m benchmarks.sregym.assurance.rehearse_batch --real-apps --code-root <freeze> --tag <t> --image-tag <t> [--lane APP=ENV_DIR[:LEASE]] [--only-slot N]` runs every planned Phase 4 problem on its app's lane with scripted workers and no model (static catalog, controller RBAC and responder-write stages first). Per problem: admission, SREGym injection, detection, scripted responder (restores Service, Deployment, ConfigMap, Ingress and HPA from accepted source, deletes fault-added quotas, policies and webhooks, through the responder identity), closure, harness recovery, a cluster-object diff (`fault-leftover`) and a guarded reset. It runs on a private copy of the lane environment (own workspace clone and results), so recorded lane data is never written; after it the lane holds the scripted controller and needs `--prepare` before live use. Image source is checked against the code root before the lane is touched. Output: `real-<tag>/summary.{json,md}`, one row per problem with stages, wall seconds and an error class (`no-scripted-repair`, `closure-failed`, `fault-leftover`, `cluster-leftover` on teardown, ...), then one row per class. Apps without a given lane are the coverage class `no-lane`.
