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

When source repairs require health adoption, distinguish a stale investigation from the final ACK of an already committed native closure. RH3 found that rejecting both prevents strict receipt completion even after verified mitigation and a broker outcome. The narrow ACK continuation requires the exact durable native ledger/receipt, broker-attributed proposal and outcome commits, unchanged human objective and health-policy bytes, and current source inputs identical to that incident’s accepted repair. Unrelated advances and uncommitted investigations remain stale. Large astronomy healthy snapshots may exceed 1 MiB each; the writer preserves complete captures within its unchanged 8 MiB aggregate bound.

The complete native receipt may reach the broker ledger before the controller persists its receipt state. After health adoption, the same ownership proof may resume that frozen closure’s idempotent broker effect, returning the existing receipt without publishing again. An absent/incomplete native receipt still fails closed. The restart fixture must cover both sides of the controller receipt persistence boundary.

A complete authenticated native receipt is recovered as an existing decision. Its replay does not re-run the pre-commit observation-generation check or independent closure decision after adoption, because source publication itself changes that generation. The health guard and durable effect identity still apply; incomplete receipts use the ordinary fresh-observation path.

After an acknowledged source repair, receipt drain waits for the controller-owned rendered lock to match the synchronized accepted application source. ACK may precede the next lock reconciliation; stopping immediately can retain a zero-gap but stale seed lock. Once closure is acknowledged and full recovery has completed, a stale lock requires briefly resuming the paused controller; closeout always pauses it again before returning, including on failure. The bounded wait never writes lock evidence and fails if publication remains stale. Check post-repair lock continuity before cleanup resets the workspace.

Rendered closeout follows resume/baseline log progress and current-lock publication, with a separate ten-minute hang bound. A new controller `reconcile rendered lock:` error fails immediately with that reason; a timeout retains the last reported reason and progress phase. Controller logs are exported again after this resume, because stage-end exports predate it. A slow resume is not itself a failed reconciliation, and neither a warning nor elapsed time establishes a published lock.
