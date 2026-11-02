# Fresh memory control state

A legacy finalized record without `reachability-reference.json` grants no port
exclusions. Startup emits a typed `reference_absent` note identifying the control
namespace, ConfigMap and missing key; ordinary reachability findings remain active.
Every non-toy SDO row starts with the tier-1 wipe (`fastloop/wipe.py`): `<app>-sdo` is deleted with all of
its ConfigMaps, Jobs and checkpoints, harness leftover namespaces and cluster objects are removed, credentials
are re-provisioned by the lane keeper's hook, and the accepted repository is reset to the row's planned commit.
`wipe-receipt.json` in the results directory records `tier1_seconds`, `tier2_ran`, `tier2_seconds`,
`tier2_trigger` (`current_row_data_faults`, `lane_last_injection_data_faults`, `lane_injection_unknown`,
`unclean_attempt`, `previous_row_unrecovered`, `restore_invariant` or `pods_unhealthy`),
`current_row_data_faults` and `previous_row_data_faults` (the triggering incoming or last lane-injected row's
catalog `DATA_LEVEL_CLASSES` faults). The last injection is recorded by the real fault driver beside the
lane kubeconfig as `<kubeconfig>.<namespace>.last-injection.json`, independently of journals and run directories.
Missing, malformed or pending injection state forces tier 2; a seeded first-try record is never lane history.
The receipt also records
`tier2_reason`, `tier2_namespaces` (deleted with their PVCs before the redeploy) and `force_finalized`
(`namespace/kind/name` of objects whose finalizers the wipe cleared so a Terminating namespace could finish). Toy lanes still stop the runtime and run
`sdo-operator reset-control-state --control-namespace <app>-sdo` before install; it deletes only the three
SDO-managed onboarding ConfigMaps and the controller checkpoint and never reads Secrets.
The next confirmed healthy install evaluation must create a new reference;
reset itself supplies no health authority.

The legacy controller checkpoint is recognized by its exact name,
`sdo.dev/state-revision` annotation and `runtime-state.json` key. Retiring it
prevents restored controller state from resurrecting old onboarding authority.

Setup detector exclusions: `ControllerState.disabled_setup_detectors`,
`NamespaceOnboarding.disabled_setup_detectors` (the immutable setup receipt), `IncidentRequest.disabled_setup_detectors`, and
`OutcomeRecord.disabled_setup_detectors` retain detector ID, accepted revision,
finding fingerprints, disabled time and the reason "fired on the confirmed-healthy
app at setup". Only responder-owned learned incident detectors qualify; health
judge detectors remain authoritative. Evaluation is skipped only at the exact
accepted revision and resumes on a different revision. Reflection sees the
outcome records to repair or delete the detector; exclusion is not incident
recovery or repair credit. `setup_evaluated_revision` prevents later steady-state
findings being mistaken for setup false positives.
