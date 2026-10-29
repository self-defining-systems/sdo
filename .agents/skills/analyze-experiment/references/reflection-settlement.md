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
