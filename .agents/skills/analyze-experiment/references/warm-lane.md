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
on legacy rows say nothing about whether a controller was retained. Do not infer
sub-30-second overhead from reuse alone; measure row-start to injection-ready.

Private reuse compares cluster and retained nonsecret infrastructure configuration
hashes against the sealed no-fault setup receipt, including objects in namespaces
outside the application and its controller. Application and controller lifecycle
objects can advance. Legacy receipts missing the expanded inventory cannot
establish this proof.

`repair-repository-bind` prints `{node, volume_path, identity, repaired}`. `identity`
is node-local device/inode evidence, not a Git revision. Repaired bind evidence
alone grants no controller or incident admission: native shared-repository
identity and the ordinary fresh baseline checks remain required. Attachment
recovery rechecks the mount even when its journal already says `all-bound`.

The learner reuses a completed post-reflection export from the native drain only
when its receipt names this incident and its existing `sdo_runtime` directory.
Failed, missing or foreign export evidence retains the last-chance export.

## Sealed evidence and queued delivery

In `--warm-lane` runs the drain seals the incident's whitelisted runtime evidence
into one immutable archive on the lane PVC (`sdo-operator runtime-artifacts --mode
snapshot`) and delivery runs off the critical path. Then
`reflection_drain.runtime_artifacts` has `delivery: queued`, `snapshot_id`,
`sha256` and `bytes`, and `phase_seconds.runtime_artifacts` times only the seal.
The row's `runtime_artifacts_export.json` (`sdo.evidence-export/v1`) gives the
delivery `state`: `queued`, `delivered` or `failed`, with `attempts` and `error`.
`<run-dir>/evidence-exports/` holds the lane's job files; a later row, `fastloop
down` or a control-namespace wipe delivers queued jobs first.

- A row's `sdo_runtime/` is complete only when its state is `delivered`; the tree
  then carries `.sdo-snapshot` (`<snapshot_id> <sha256>`). Before that, the tree is
  the stage-end export, taken before reflection.
- Delivered evidence is exactly the closure-time archive. It never contains a later
  row's sessions or usage lines, so cumulative streams end at that boundary.
- `failed` means the evidence was lost or not copied; never treat that row's
  reflection usage or rollouts as complete. Delivery never grants repair authority.
- Replay checkpoints of a queued row are captured after delivery from ConfigMaps
  read at the boundary; `unrecorded.json` records a failed capture.
