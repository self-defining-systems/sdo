# Checkpoint recording evidence

Fastloop stage checkpoints live under `results/<run-id>/checkpoints/<incident-number>/<stage>/`. `request.json` and `result.json` are the exact worker ProtoJSON; `checkpoint.json` holds capture digests, a digest of the direct file-edit bundle, and the settled native broker ledger. `file_edits.json` is recorded beside each exported worker request/result before publication and stored content-addressed by the checkpoint. Its version-1 edits carry repository-relative paths, base64 file bytes (null for deletion), and executable modes, bounded to 48 MiB encoded. Recording never blocks publication: an edit set it cannot record (over 32 MiB raw, a symlink, an unreadable base) leaves `file_edits_unrecorded.json` with the reason, the manifest carries it as `unrecorded_file_edits`, and replay reports the checkpoint as not replayable. Replay applies them to a fresh checkout and runs the real Go broker. Old proposal-only recordings fail with an explicit error; no proposal shim is kept. Corpus files live under `checkpoints/blobs/<sha256>` and are joined by digest, not by their original session path. Corpus evidence is capped at 4 MiB per file and 32 MiB per stage; omitted files have name-to-reason entries in the optional version-1 manifest field `unrecorded_captures`. Exact results, broker ledger and bounded captures remain available; every replay entrypoint reports the omitted evidence as `not replayable`. A failed optional archive/checkpoint export writes `checkpoints/<incident-number>/unrecorded.json` with a reason, which blocks replay of partial or older stages until recapture succeeds. These evidence failures never discard completed resolution or learning metrics; distinguish them from native closure/reflection failures. A checkpoint is replay evidence, not an accepted commit or a benchmark oracle verdict. Default Codex fastloop registrations record provider CLI turns under the exported incident session's `model_io/<request-digest>/<prompt-digest>.json` (each streamed line once in `events`, at most 64 MiB per turn, and a larger turn keeps only an `unrecorded` reason that replay reports; older records also carry joined `stdout`/`stderr`); those are model I/O for deterministic plumbing replay, not additional agent decisions. Compare replay's broker acceptance with the recorded ledger and accepted commit before attributing a learning change.

## Offline Claude reflection scenarios

`benchmarks.sregym.skilleval` scenarios pin `provider`, `model`, `effort` and an
optional explicit `controller_image` (CLI overlay tags need not have a matching
controller tag). Claude Code scenarios retain only the original responder
conversation through the first reflection enqueue/user boundary. CLI indexes,
side conversations, old skills, credentials and reflection usage are removed.
A private copied session receives its own released responder ownership through
the native `SessionOwnership` producer; live ownership is never changed.
The runner installs the evaluated skills into `CLAUDE_CONFIG_DIR` and mounts
`.credentials.json` only for the worker, then removes that credential copy.

Claude phase accounting uses new unique assistant `message.id` values compared
with the frozen prefix, deduplicating streaming revisions and copied projects.
It retains Claude `num_turns` as `model_calls`, and exposes deduplicated API
requests separately as `unique_requests`. It reports raw input plus output, cache reads/writes including 1h writes,
weighted base-input tokens and USD from the versioned agentshim pricing table.
A resumed CLI's cumulative `total_cost_usd` must never be summed as reflection
USD. `usage.accounting_source` and `usage.pricing_version` state these sources.
When configured, host-side `SDO_OPUS_POOL_DIR` admission wraps the entire offline
container for recorded h9.22t images predating native provider-capacity support; use the
same provisioned pool and capacity as the live lanes. The runner retains its
three-run cap, nice 10, Go concurrency cap and load1 < 40 start gate.

A source exported without context/tools/API history is incomplete. Scenario
provenance must say so; reconstructed native-ledger requests must be identified
and their accepted-memory snapshot checked against the outcome commit. Such
runs measure the explicitly frozen inputs and must not be presented as exact
live cost reproduction.

`reflection_seconds` measures the native reflection worker, including its validation.
`wall_seconds` also includes host admission and independent post-check/replay; the
table uses reflection minutes when available. Per-run `run-config.json` pins the
actual provider/model/effort, including a model override.

For replay-on/off comparisons, use [replay policy](replay-policy.md); missing corpus or replay observations in an off row are expected.
