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
(`namespace/kind/name` of objects whose finalizers the wipe cleared so a Terminating namespace could finish) and `helpers_removed` (`kind/name` of foreign helper pods and workloads a prior row left in `sregym-injector`, deleted at row start). Toy lanes still stop the runtime and run
`sdo-operator reset-control-state --control-namespace <app>-sdo` before install; it deletes only the three
SDO-managed onboarding ConfigMaps and the controller checkpoint and never reads Secrets.
The next confirmed healthy install evaluation must create a new reference;
reset itself supplies no health authority.

The legacy controller checkpoint is recognized by its exact name,
`sdo.dev/state-revision` annotation and `runtime-state.json` key. Retiring it
prevents restored controller state from resurrecting old onboarding authority.

Setup and learned detectors: only health-class detectors and user tests decide whether an install or
start-up is healthy and accepted. A learned detector (whoever wrote it) never affects
acceptance and is never disabled at setup. When one fires or errors while the app is judged healthy it stays
enabled and is recorded as noisy evidence: a log line and `NamespaceOnboarding.setup_noisy` (detector ID,
accepted revision, `errored` or `fired`, the error or the fired rule IDs). The earlier quarantine is gone:
`disabled_setup_detectors` (controller state, onboarding record, incident request, outcome record) is retired and
stored documents that still carry it decode with the field dropped. Read `setup_noisy` to see which clues were
noisy at install; a detector that is always noisy needs repair by reflection, and the install is unaffected.
`setup_evaluated_revision` prevents later steady-state findings being mistaken for setup noise.
