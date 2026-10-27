# Fresh memory control state

A legacy finalized record without `reachability-reference.json` grants no port
exclusions. Startup emits a typed `reference_absent` note identifying the control
namespace, ConfigMap and missing key; ordinary reachability findings remain active.
Every non-toy SDO row starts with the tier-1 wipe (`fastloop/wipe.py`): `<app>-sdo` is deleted with all of
its ConfigMaps, Jobs and checkpoints, harness leftover namespaces and cluster objects are removed, credentials
are re-provisioned by the lane keeper's hook, and the accepted repository is reset to the row's planned commit.
`wipe-receipt.json` in the results directory records `tier1_seconds`, `tier2_ran`, `tier2_seconds`,
`tier2_trigger` (`unclean_attempt`, `previous_row_unrecovered`, `previous_row_data_faults`, `restore_invariant` or
`pods_unhealthy`), `previous_row_data_faults` (the previous row's injected faults whose state lives in app data,
from the fault catalog's `DATA_LEVEL_CLASSES`; they recreate the app even when the row recovered),
`tier2_reason`, `tier2_namespaces` (deleted with their PVCs before the redeploy) and `force_finalized`
(`namespace/kind/name` of objects whose finalizers the wipe cleared so a Terminating namespace could finish). Toy lanes still stop the runtime and run
`sdo-operator reset-control-state --control-namespace <app>-sdo` before install; it deletes only the three
SDO-managed onboarding ConfigMaps and the controller checkpoint and never reads Secrets.
The next confirmed healthy install evaluation must create a new reference;
reset itself supplies no health authority.

The legacy controller checkpoint is recognized by its exact name,
`sdo.dev/state-revision` annotation and `runtime-state.json` key. Retiring it
prevents restored controller state from resurrecting old onboarding authority.
