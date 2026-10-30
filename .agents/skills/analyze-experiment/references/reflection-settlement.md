# Confirmed lifecycle repairs and reflection settlement

An old deterministic stage can report `rollout_failed` after a deletion succeeded
because its informer never observed the write. That transport failure alone does
not show that the saved repair needs rewriting.

The existing stale-playbook check recognizes settlement only when the same
application and plan contain every applied action and confirmed lifecycle effect,
the same bound program revision returns NoMatch, its own findings clear, and a
later independent successful closure has only clear final health detectors.
Application identity, action reports, timestamps and exact effect targets must
agree. Missing or ambiguous proof remains stale. Later same-resource hand repairs
and unresolved own findings still require learning. This does not grant early
repair credit or change automatic program execution.

For reflection completeness, count a ledger of canonical repaired resources and
receipt IDs mapped to concrete program actions. A program may cover several
repairs; duplicate target strings may identify the same object. A changed-program
count divided by raw target strings is not coverage. Review action parameters as
well as presence: `EnsureDatabaseUser` replaces the complete direct-role set,
while generic `SetField` paths use JSON-pointer spelling outside its dotted
vocabulary. Compilation alone does not establish the intended effect.

The built-in worker probes that same native check before resolving a model. A
passed `no_change` probe records reason `native_memory_check_no_change` and uses
zero model calls/tokens. Rejection or timeout continues with the normal session;
it does not suppress learning. A resumed native completion still performs the
permanent responder-to-reflection session ownership transfer. No provider writer
is started, and the broker independently checks the returned decision.

### Composite post-ACK idle tails

A composite report with `stop_reason=no_further_incident` and red `final_green`
probes can include the adapter's follow-up incident wait after native learning
already finished. Correlate the current attempt's `reflection_drain` timestamps
with `controller_state_timeline` before attributing a near-600-second addition to
reflection. Native ACK/idle now ends follow-up collection; benchmark fault
probes remain reporting evidence. This applies equally to model-driven,
model-free and no-change closures. Existing native reflection/rollout drain
and strict receipt validation still precede that boundary. Ordinary export,
probe stability and collection overhead is separate from this idle timeout.


Terminal validation exhaustion records `reflection_rejected=true`,
`reflection_processed=true`, and `reflection_decision.learning_decision=failed`
with an `error` naming the final validator diagnostics. The closure completes
without a reflection commit; `reflection_results` retains the original worker
decisions and `reflection_rejections` retains validator evidence. Older ledgers
can omit the final decision after exhaustion: that is lost terminal evidence,
not successful learning, and strict receipts reject it.
