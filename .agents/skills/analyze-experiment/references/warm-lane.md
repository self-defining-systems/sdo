# Warm-lane preparation evidence

Opt-in private-lane `fastloop run --warm-lane` writes
`results/<run-id>/warm-transaction.json` (`sdo.warm-lane/v1`). `reused` means
changed-resource reset, the in-place reseed, accepted-revision equality and fresh
admission all passed on the lane's own install (no namespace deletion, no new volume).
`identity` contains controller and standby Pod UIDs plus the full accepted Git revision. `baseline_not_before` is the
boundary taken after the restore's writes and before the new controller's install. After the pause the application
restore and the reseed run concurrently (the reseed installs only once the restore's writes are done; the restore's
oracle overlaps the controller's start), so `reset` and `reseed` are per-branch durations that overlap and
`reset_and_reseed` is their wall time; `reset.restore`/`reset.verify` split the restore at that signal and
`reseed.await_application` is the reseed's wait for it (the critical path is the restore's writes when it is large).
`phase_seconds` also holds inspect, pause, pre-reseed identity, reidentify, adopt, prove, confirmation and (on
fallback) full reset; never sum all its values. `warm/reseed-phases.json` splits the reseed into settle_evidence,
read_head, stop_controller, retire_control_state, fresh_observability, await_application, install_controller,
wait_started, record_controller and await_observability. The controller and standby identities in `identity` are those of the
controller the reseed installed (the process is replaced by design); `reused` means that new process proved equal to a
fresh install on the planned memory. A fallback at `reseed:<phase>:<error type>` means the controller may have been
stopped and the full reset (which deletes the control namespace) followed. Ordinary injection admission
remains required after preparation; this receipt is not injection/closure proof.

A failed preparation records boundary and exception type without raw command
errors. When the lane was not reused because it differs from a fresh install
(`fallback_reason` `prove:ResidentStartMismatch`), `violations` lists every item that
differed (planned memory, carried-over incident or closure state, stale history,
startup period, onboarding, session ConfigMaps, configuration digest); it is empty
for every other fallback. `reused: false` with a fallback reason means full reset was selected;
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

## Lanes workload rows: `reseed`

On an SREGym lanes host with `SREGYM_LANES_RESEED=1`, `run` passes `--warm-lane --deliver-evidence-before-exit` to
`fastloop run` when the lane's `lane-install.json` (application, namespace, images, agent provider/model/effort and a digest
of the config overlay) equals the row's. The row record has `reseed`: `{attempted, reused, reason, violations, phase_seconds}`
(from `results/<run-id>/warm-transaction.json`). `attempted: false` rows installed fresh and `reason` says why (`reseed is not
enabled for this host`, `no earlier install on this lane`, `install identity differs: <fields>`); `attempted: true, reused:
false` is a fallback to the fresh install and `reason` is fastloop's `<phase>:<error type>` (`no warm-transaction receipt`
when fastloop left none). The grade has `reseed` = `reseeded | fallback | fresh` and `reseed_reason`: count them to see how
many rows skipped the reinstall. `--deliver-evidence-before-exit` makes `fastloop run` finish delivering the sealed evidence
into `results/` before it returns (the workload copies results out at that moment).

`<lane run dir>/fresh-onboarding/<commit>.json` and `latest.json` are written by a FRESH install's admission after its
onboarding: `{accepted_commit, objects: [apiVersion/kind/namespace/name, ...]}`. A reseeded controller's onboarding must be
for the planned commit and name the same objects as the reference for that commit (else `latest.json`); timestamps, UIDs and
digests are install-bound and never compared. No reference (a lane whose first row has not run fresh yet) is a fallback.
