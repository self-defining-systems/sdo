# Warm-lane preparation evidence

Opt-in private-lane `fastloop run --warm-lane` writes
`results/<run-id>/warm-transaction.json` (`sdo.warm-lane/v1`). `reused` means
changed-resource reset, accepted-revision equality and fresh admission all passed
without replacing the controller or standby. `identity` contains controller and
standby Pod UIDs plus the full accepted Git revision. `baseline_not_before` is the
post-reset/adoption boundary. `phase_seconds` splits inspect, pause, reset, adopt,
prove, confirmation and (on fallback) full reset. Ordinary injection admission
remains required after preparation; this receipt is not injection/closure proof.

A failed preparation records boundary and exception type without raw command
errors. `reused: false` with a fallback reason means full reset was selected;
`full_reset_failed` or `prior_incident_open` prevents proceeding. Missing receipts
on legacy rows say nothing about whether a controller was retained. Export remains
synchronous in this initial implementation; do not infer asynchronous export or
sub-30-second overhead from reuse alone.

Private reuse additionally compares cluster configuration hashes against the
sealed no-fault setup receipt, not a names-only inventory. Legacy setup receipts
missing the expanded cluster kinds cannot establish this proof.

`repair-repository-bind` prints `{node, volume_path, identity, repaired}`. `identity`
is node-local device/inode evidence, not a Git revision. Repaired bind evidence
alone grants no controller or incident admission: native shared-repository
identity and the ordinary fresh baseline checks remain required. Attachment
recovery rechecks the mount even when its journal already says `all-bound`.
