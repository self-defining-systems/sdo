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
