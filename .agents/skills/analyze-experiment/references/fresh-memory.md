# Fresh memory control state

A legacy finalized record without `reachability-reference.json` grants no port
exclusions. Startup emits a typed `reference_absent` note identifying the control
namespace, ConfigMap and missing key; ordinary reachability findings remain active.
Every non-toy SDO row starts with the tier-1 wipe (`fastloop/wipe.py`): `<app>-sdo` is deleted with all of
its ConfigMaps, Jobs and checkpoints, harness leftover namespaces and cluster objects are removed, credentials
are re-provisioned by the lane keeper's hook, and the accepted repository is reset to the row's planned commit.
`wipe-receipt.json` in the results directory records `tier1_seconds`, `tier2_ran`, `tier2_seconds` and
`tier2_reason`. Toy lanes still stop the runtime and run
`sdo-operator reset-control-state --control-namespace <app>-sdo` before install; it deletes only the three
SDO-managed onboarding ConfigMaps and the controller checkpoint and never reads Secrets.
The next confirmed healthy install evaluation must create a new reference;
reset itself supplies no health authority.

The legacy controller checkpoint is recognized by its exact name,
`sdo.dev/state-revision` annotation and `runtime-state.json` key. Retiring it
prevents restored controller state from resurrecting old onboarding authority.
